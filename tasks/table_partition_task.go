package tasks

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/alekLukanen/ChapterhouseDB/operations"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	taskpackets "github.com/alekLukanen/ChapterhouseDB/taskPackets"
	"github.com/alekLukanen/ChapterhouseDB/tasker"
	arrowops "github.com/alekLukanen/arrow-ops"
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type TablePartitionTask struct {
	logger          *slog.Logger
	keyStorage      storage.IKeyStorage
	objectStorage   storage.IObjectStorage
	manifestStorage storage.IManifestStorage
	allocator       *memory.GoAllocator

	tableRegistry operations.ITableRegistry
	inserter      operations.IInserter
}

func NewTablePartitionTask(
	logger *slog.Logger,
	keyStorage storage.IKeyStorage,
	objectStorage storage.IObjectStorage,
	manifestStorage storage.IManifestStorage,
	allocator *memory.GoAllocator,
	tableRegistry operations.ITableRegistry,
	inserter operations.IInserter) *TablePartitionTask {
	return &TablePartitionTask{
		logger:          logger,
		keyStorage:      keyStorage,
		objectStorage:   objectStorage,
		manifestStorage: manifestStorage,
		allocator:       allocator,
		tableRegistry:   tableRegistry,
		inserter:        inserter,
	}
}

func (obj *TablePartitionTask) Name() string {
	return taskpackets.TablePartitionTaskName
}
func (obj *TablePartitionTask) NewPacket() tasker.ITaskPacket {
	return new(taskpackets.TablePartitionTaskPacket)
}

/*
Used to process the next partition for a table in the warehouse.
Steps:
 1. Get a partition for a table subscription
 2. Pass the partition data arrow Record to the subscriptions
    transformer function. This will pass back an unorder record
    which will then need to be ordered.
 3. Request all existing files from S3
 4. Once the records ascending order is found get the partition's
    tables from object storage and merge the new/updated/deleted records into existing data.
    The merge rows function will scan the partition's parquet
    files for items effected by the tuples. The merge process will
    be performed on just the partition keys and
    will compare all other columns to see if this unique
    row has changed. This process is essentially 2-way merge sort.
    For now pull down all parquet files for the partition and then perform
    the merge. Any row not effected by the tuples will be
    written back to a new parquet file. Any row that is effected
    by the tuples will be compared against the new version of
    the row, if it exists. Only write the new rows and if the
    row hasn't changed then only mark its _processed_ts
    not its _updated_ts. Include a _processed_count column
    to keep track of how many times a row has been processed and
    a _updated_count column to indicate how many times it has been
    updated.
 5. Push the new parquet files to the object storage. The file name
    should include an incremented version count and a total file
    count so the system can handle crash recovery. If the total
    files count for that version does not match the number of file
    in object storage then there has been a failure and the system
    will need to be manually recovered. In the future this recovery
    process will be automated by using a KeyDB list of in process
    items for the partition. On crash and then restart the system
    will check that list for any items that have not been removed
    and process those again.
 6. Delete the old parquet files for the partition.
 7. Release the lock on the partition
 8. Release the arrow record from the memory allocator
*/
func (obj *TablePartitionTask) Process(ctx context.Context, packet tasker.ITaskPacket) (tasker.Result, error) {
	tptPacket, ok := packet.(*taskpackets.TablePartitionTaskPacket)
	if !ok {
		return tasker.Result{}, errs.NewStackError(ErrInvalidPacketType)
	}

	partition := tptPacket.Partition
	tbl, err := obj.tableRegistry.GetTable(partition.TableName)
	if err != nil {
		return tasker.Result{}, errs.Wrap(err)
	}
	tblSub, err := tbl.GetSubscriptionBySourceName(partition.SubscriptionSourceName)
	if err != nil {
		err = errs.Wrap(err, fmt.Errorf("unabled to get subscription for partition source name"))
		return tasker.Result{}, err
	}
	tblOpts := tbl.Options()

	lock, record, err := obj.inserter.GetPartitionBatch(ctx, partition)
	if err != nil {
		return tasker.Result{}, errs.Wrap(err)
	}
	defer record.Release()
	defer func() {
		_, unlockErr := lock.UnlockContext(ctx)
		if unlockErr != nil && err != nil {
			err = errs.Wrap(err, fmt.Errorf("%w| while handling previous error another occurred", unlockErr))
		} else if unlockErr != nil {
			err = unlockErr
		}
	}()

	obj.logger.Info(
		"processing partition",
		slog.String("partition.Key", partition.Key),
		slog.Int64("record.NumRows()", record.NumRows()),
		slog.Any("lock.Name()", lock.Name()),
	)

	// 2. Transform the partition data basec on the subscription
	transformedData, err := tblSub.Transformer()(ctx, obj.allocator, obj.logger, record)
	if err != nil {
		err = errs.Wrap(err, fmt.Errorf("unable to transform data"))
		return tasker.Result{}, err
	}
	defer transformedData.Release()
	obj.logger.Info("transformed data", slog.Int64("numrows", transformedData.NumRows()))

	// 3. Sort the record in ascending order
	// The order will be based on the combination of the
	// partition keys for the table:
	//   ("partition_column1", "partition_column2",...)
	columnPartitions := tbl.ColumnPartitions()
	partitionColumnNames := make([]string, len(columnPartitions))
	for i, col := range columnPartitions {
		partitionColumnNames[i] = col.Name()
	}
	obj.logger.Debug(
		"partition columns used in sorting",
		slog.String("table", tbl.TableName()),
		slog.Any("columns", partitionColumnNames))

	sortedRecord, err := arrowops.SortRecord(obj.allocator, transformedData, partitionColumnNames)
	if err != nil {
		err = errs.Wrap(err, fmt.Errorf("failed to sort transformed data using columns %v", partitionColumnNames))
		return tasker.Result{}, err
	}
	defer sortedRecord.Release()

	// 4-8. Merge the new record into the existing partition records using
	// the manifest storage implementation.
	tableColumns := tbl.Columns()
	allColumnNames := make([]string, len(tableColumns))
	for i, col := range tableColumns {
		allColumnNames[i] = col.Name
	}
	obj.logger.Debug(
		"all columns used in merging",
		slog.String("table", tbl.TableName()),
		slog.Any("columns", allColumnNames))

	processedKeyRecord, err := arrowops.TakeRecordColumns(sortedRecord, partitionColumnNames)
	if err != nil {
		err = errs.Wrap(err, fmt.Errorf("failed to take record columns"))
		return tasker.Result{}, err
	}

	processedKeyRecord, err = arrowops.DeduplicateRecord(obj.allocator, processedKeyRecord, partitionColumnNames, true)
	if err != nil {
		err = errs.Wrap(err, fmt.Errorf("failed to deduplicate record"))
		return tasker.Result{}, err
	}

	err = obj.manifestStorage.MergePartitionRecordIntoManifest(
		ctx,
		partition,
		processedKeyRecord,
		sortedRecord,
		partitionColumnNames,
		allColumnNames,
		storage.PartitionManifestOptions{
			MaxObjectRows: tblOpts.MaxObjectSize,
		},
	)
	if err != nil {
		err = errs.Wrap(err, fmt.Errorf("failed to merge the new record into the manifest"))
		return tasker.Result{}, err
	}

	obj.logger.Info("finished processing partition", slog.String("partition.Key", partition.Key))

	return tasker.Result{}, nil
}
