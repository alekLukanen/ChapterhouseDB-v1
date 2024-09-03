package warehouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/alekLukanen/ChapterhouseDB/operations"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/arrow-ops"
	"github.com/alekLukanen/errs"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type Warehouse struct {
	logger          *slog.Logger
	keyStorage      storage.IKeyStorage
	objectStorage   storage.IObjectStorage
	manifestStorage storage.IManifestStorage
	allocator       *memory.GoAllocator

	name          string
	tableRegistry operations.ITableRegistry
	inserter      operations.IInserter
}

func NewWarehouse(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	tableRegistry *operations.TableRegistry,
	keyStorageOptions storage.KeyStorageOptions,
	objectStorageOptions storage.ObjectStorageOptions,
	manifestStorageOptions storage.ManifestStorageOptions,
) (*Warehouse, error) {
	keyStorage, err := storage.NewKeyStorage(ctx, logger, keyStorageOptions)
	if err != nil {
		return nil, err
	}

	objectStorage, err := storage.NewObjectStorage(ctx, logger, objectStorageOptions)
	if err != nil {
		return nil, err
	}

	allocator := memory.NewGoAllocator()
	inserter := operations.NewInserter(
		logger,
		tableRegistry,
		keyStorage,
		allocator,
		operations.InserterOptions{
			PartitionLockDuration: 1 * time.Minute,
		},
	)

	manifestStorage := storage.NewManifestStorage(
		ctx, logger, allocator, objectStorage, manifestStorageOptions,
	)

	warehouse := &Warehouse{
		logger:          logger,
		keyStorage:      keyStorage,
		objectStorage:   objectStorage,
		manifestStorage: manifestStorage,
		allocator:       allocator,
		name:            name,
		tableRegistry:   tableRegistry,
		inserter:        inserter,
	}
	return warehouse, nil
}

func (obj *Warehouse) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			processedAPartition, err := obj.ProcessNextTablePartition(ctx)
			if err != nil {
				obj.logger.Error(
					"failed to process next table partition",
					slog.String("error", errs.ErrorWithStack(err)))
			}
			if !processedAPartition {
				time.Sleep(5 * time.Second)
			}
		}
	}
}

/*
* Used to process the next partition for a table in the warehouse.
* Steps:
* 1. Get a partition for a table subscription
* 2. Pass the partition data arrow Record to the subscriptions
*    transformer function. This will pass back an unorder record
*    which will then need to be ordered.
* 3. Request all existing files from S3
* 4. Once the records ascending order is found get the partition's
*    tables from object storage and merge the new/updated/deleted records into existing data.
*    The merge rows function will scan the partition's parquet
*    files for items effected by the tuples. The merge process will
*    be performed on just the partition keys and
*    will compare all other columns to see if this unique
*    row has changed. This process is essentially 2-way merge sort.
*    For now pull down all parquet files for the partition and then perform
*    the merge. Any row not effected by the tuples will be
*    written back to a new parquet file. Any row that is effected
*    by the tuples will be compared against the new version of
*    the row, if it exists. Only write the new rows and if the
*    row hasn't changed then only mark its _processed_ts
*    not its _updated_ts. Include a _processed_count column
*    to keep track of how many times a row has been processed and
*    a _updated_count column to indicate how many times it has been
*    updated.
* 5. Push the new parquet files to the object storage. The file name
*    should include an incremented version count and a total file
*    count so the system can handle crash recovery. If the total
*    files count for that version does not match the number of file
*    in object storage then there has been a failure and the system
*    will need to be manually recovered. In the future this recovery
*    process will be automated by using a KeyDB list of in process
*    items for the partition. On crash and then restart the system
*    will check that list for any items that have not been removed
*    and process those again.
* 6. Delete the old parquet files for the partition.
* 7. Release the lock on the partition
* 8. Release the arrow record from the memory allocator
 */
func (obj *Warehouse) ProcessNextTablePartition(ctx context.Context) (bool, error) {
	var partition elements.Partition
	var err error
	var lock storage.ILock
	var record arrow.Record
	var table *elements.Table
  var tableOptions elements.TableOptions
	var foundPartition bool

	// 1. Get a partition for a table subscription
	for idx, tab := range obj.tableRegistry.Tables() {

		obj.logger.Debug("Processing table", slog.Any("table", tab.TableName()), slog.Int("index", idx))

		tableOptions = table.Options()
		partition, lock, record, err = obj.inserter.GetPartition(
			ctx, "table1", tableOptions.BatchProcessingSize, tableOptions.BatchProcessingDelay,
		)
		if errors.Is(err, operations.ErrNoPartitionsAvailable) {
			continue
		} else if err != nil {
			return false, errs.Wrap(
				err,
				fmt.Errorf("unable to read partition items for table %s", tab.TableName()))
		} else {
			table = tab
			foundPartition = true
			break
		}

	}
	if !foundPartition {
		return false, nil
	}

	obj.logger.Info(
		"processing partition",
		slog.String("partition.Key", partition.Key),
		slog.Int64("record.NumRows()", record.NumRows()),
		slog.Any("lock.Name()", lock.Name()),
	)

	subscription, err := table.GetSubscriptionBySourceName(partition.SubscriptionSourceName)
	if err != nil {
		return false, errs.Wrap(err, fmt.Errorf("unabled to get subscription for partition source name"))
	}

	// 2. Transform the partition data basec on the subscription
	transformedData, err := subscription.Transformer()(ctx, obj.allocator, record)
	if err != nil {
		return false, errs.Wrap(err, fmt.Errorf("unable to transform data"))
	}
	defer transformedData.Release()
	obj.logger.Debug("transformed data", slog.Int64("numrows", transformedData.NumRows()))

	// 3. Sort the record in ascending order
	// The order will be based on the combination of the
	// partition keys for the table:
	//   ("partition_column1", "partition_column2",...)
	columnPartitions := table.ColumnPartitions()
	partitionColumnNames := make([]string, len(columnPartitions))
	for i, col := range columnPartitions {
		partitionColumnNames[i] = col.Name()
	}
	obj.logger.Debug(
		"partition columns used in sorting",
		slog.String("table", table.TableName()),
		slog.Any("columns", partitionColumnNames))

	sortedRecord, err := arrowops.SortRecord(obj.allocator, transformedData, partitionColumnNames)
	if err != nil {
		return false, errs.Wrap(
			err,
			fmt.Errorf("failed to sort transformed data using columns %v", partitionColumnNames))
	}
	defer sortedRecord.Release()

	// 4-8. Merge the new record into the existing partition records using
	// the manifest storage implementation.
	tableColumns := table.Columns()
	allColumnNames := make([]string, len(tableColumns))
	for i, col := range tableColumns {
		allColumnNames[i] = col.Name
	}
	obj.logger.Debug(
		"all columns used in merging",
		slog.String("table", table.TableName()),
		slog.Any("columns", allColumnNames))

	processedKeyRecord, err := arrowops.TakeRecordColumns(sortedRecord, partitionColumnNames)
	if err != nil {
		return false, errs.Wrap(err, fmt.Errorf("failed to take record columns"))
	}

	processedKeyRecord, err = arrowops.DeduplicateRecord(obj.allocator, processedKeyRecord, partitionColumnNames, true)
	if err != nil {
		return false, errs.Wrap(err, fmt.Errorf("failed to deduplicate record"))
	}

	err = obj.manifestStorage.MergePartitionRecordIntoManifest(
		ctx,
		partition,
		processedKeyRecord,
		sortedRecord,
		partitionColumnNames,
		allColumnNames,
		storage.PartitionManifestOptions{
      MaxObjectRows: tableOptions.MaxObjectSize,
    },
	)
	if err != nil {
		return false, errs.Wrap(
			err,
			fmt.Errorf("failed to merge the new record into the manifest"))
	}

	return true, nil
}
