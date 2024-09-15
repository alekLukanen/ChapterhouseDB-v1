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

	return tasker.Result{Requeue: true}, nil
}
