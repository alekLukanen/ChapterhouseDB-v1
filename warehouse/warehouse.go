package warehouse

import (
	"context"
	"log/slog"
	"time"

	"github.com/alekLukanen/chapterhouseDB/elements"
	"github.com/alekLukanen/chapterhouseDB/operations"
	"github.com/alekLukanen/chapterhouseDB/storage"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

type Warehouse struct {
	logger        *slog.Logger
	keyStorage    *storage.KeyStorage
	objectStorage *storage.ObjectStorage
	allocator     *memory.GoAllocator

	name          string
	tableRegistry *operations.TableRegistry
	inserter      *operations.Inserter
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
* Used to process the next partitionn for a table in the warehouse.
* Steps:
* 1. Get a partition for a table
* 2. Get the current and new rows using the partition
*    tuple record as input. The current rows function
*    will scan the partition's parquet files for items
*    effected by the tuples. The new rows function will
*    run some kind of query and produce an arrow record
*    or a parquet file. If the table is a source table a diff
*    merge will be performed on just the partition keys and
*    will compare all other columns to see if this unique
*    row has changed.
*    For now pull down all parquet files for the partition and then perform
*    the merge. Any row not effected by the tuples will be
*    written back to a new parquet file. Any row that is effected
*    by the tuples will be compared against the new version of
*    the row, if it exists. Only write the new rows and if the
*    row hasn't changed then only mark its _processed_ts
*    not its _updated_ts. Include a _processed_count column
*    to keep track of how many times a row has been processed.
* 3. Push the new parquet files to the object storage. The file name
*    should include an incremented version count and a total file
*    count so the system can handle crash recovery.
* 4. Delete the old parquet files for the partition.
* 5. For any row that has changed signal to all subscribed
*    tables that the row has changed by batching the row
*    in that dependent tables partition batches with the
*    requested partition keys.
* 6. Release the lock on the partition
* 7. Release the arrow record from the memory allocator
 */
func (obj *Warehouse) ProcessNextTablePartition(ctx context.Context) (bool, error) {
	var partition elements.Partition
	var err error
	var lock storage.ILock
	var record arrow.Record

	for idx, table := range obj.tableRegistry.Tables() {

		obj.logger.Info("Processing table", slog.Any("table", table.TableName()), slog.Int("index", idx))

		tableOptions := table.Options()
		partition, lock, record, err = obj.inserter.GetPartition(
			ctx, "table1", tableOptions.BatchProcessingSize, tableOptions.BatchProcessingDelay,
		)
		if err != nil {
			obj.logger.Error("unable to read items", slog.Any("error", err))
			return false, err
		}

		break

	}

	obj.logger.Info(
		"processing partition",
		slog.Any("partition", partition),
		slog.Any("error", err),
		slog.Any("lock", lock),
		slog.Any("record", record),
	)

	return true, nil
}
