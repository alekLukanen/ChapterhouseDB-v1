package warehouse

import (
	"context"
	"log/slog"
	"time"

	arrowops "github.com/alekLukanen/chapterhouseDB/arrowOps"
	"github.com/alekLukanen/chapterhouseDB/elements"
	"github.com/alekLukanen/chapterhouseDB/operations"
	"github.com/alekLukanen/chapterhouseDB/storage"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

type Warehouse struct {
	logger        *slog.Logger
	keyStorage    storage.IKeyStorage
	objectStorage storage.IObjectStorage
	allocator     *memory.GoAllocator

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
) (*Warehouse, error) {
	keyStorage, err := storage.NewKeyStorage(ctx, logger, keyStorageOptions)
	if err != nil {
		return nil, err
	}

	objectStorage, err := storage.NewObjectStorage(ctx, logger, objectStorageOptions)

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

	warehouse := &Warehouse{
		logger:        logger,
		keyStorage:    keyStorage,
		objectStorage: objectStorage,
		allocator:     allocator,
		name:          name,
		tableRegistry: tableRegistry,
		inserter:      inserter,
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
				obj.logger.Error("unable to process partition", slog.Any("error", err))
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
* 3. Sort the record in ascending order.
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
* 7. For any row that has changed signal to all subscribed
*    tables that the row has changed by batching the row
*    in that dependent tables partition batches with the
*    requested partition keys.
* 8. Release the lock on the partition
* 9. Release the arrow record from the memory allocator
 */
func (obj *Warehouse) ProcessNextTablePartition(ctx context.Context) (bool, error) {
	var partition elements.Partition
	var err error
	var lock storage.ILock
	var record arrow.Record
	var table *elements.Table

	// 1. Get a partition for a table subscription
	for idx, tab := range obj.tableRegistry.Tables() {

		obj.logger.Info("Processing table", slog.Any("table", tab.TableName()), slog.Int("index", idx))

		tableOptions := table.Options()
		partition, lock, record, err = obj.inserter.GetPartition(
			ctx, "table1", tableOptions.BatchProcessingSize, tableOptions.BatchProcessingDelay,
		)
		if err != nil {
			obj.logger.Error("unable to read items", slog.Any("error", err))
			return false, err
		}

		table = tab

		break

	}

	obj.logger.Info(
		"processing partition",
		slog.Any("partition", partition),
		slog.Any("error", err),
		slog.Any("lock", lock),
		slog.Any("record", record),
	)

	subscription, err := table.GetSubscriptionBySourceName(partition.SubscriptionSourceName)
	if err != nil {
		return false, err
	}

	// 2. Transform the partition data basec on the subscription
	transformedData, err := subscription.Transformer()(ctx, obj.allocator, record)
	if err != nil {
		return false, err
	}
	obj.logger.Info("transformed data", slog.Any("numrows", transformedData.NumRows()))

	// 3. Sort the record in ascending order
	// The order will be based on the combination of the
	// partition keys for the table:
	//   ("partition_column1", "partition_column2",...)
	columnPartitions := table.ColumnPartitions()
	columnNames := make([]string, len(columnPartitions))
	for i, col := range columnPartitions {
		columnNames[i] = col.Name()
	}
	sortedData, err := arrowops.SortRecord(obj.allocator, transformedData, columnNames)
	if err != nil {
		return false, err
	}
	obj.logger.Info("sortedData", slog.Any("numrows", sortedData.NumRows()))

	// 4. Merge the new data with the existing

	return true, nil
}
