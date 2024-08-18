package operations

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type IInserter interface {
	GetPartition(ctx context.Context, tableName string, batchCount int, batchDelay time.Duration) (elements.Partition, storage.ILock, arrow.Record, error)
	InsertTuples(ctx context.Context, tableName, sourceName string, tuples arrow.Record) error
}

type InserterOptions struct {
	PartitionLockDuration time.Duration
}

type Inserter struct {
	logger *slog.Logger

	tableRegistry ITableRegistry
	keyStorage    IKeyStorage
	allocator     *memory.GoAllocator

	options InserterOptions
}

func NewInserter(
	logger *slog.Logger,
	tableRegistry ITableRegistry,
	keyStorage IKeyStorage,
	allocator *memory.GoAllocator,
	options InserterOptions,
) *Inserter {
	return &Inserter{
		logger:        logger,
		tableRegistry: tableRegistry,
		keyStorage:    keyStorage,
		allocator:     allocator,
		options:       options,
	}
}

func (obj *Inserter) GetPartition(ctx context.Context, tableName string, batchCount int, batchDelay time.Duration) (elements.Partition, storage.ILock, arrow.Record, error) {
	obj.logger.Info("getting tuples")

	var pageCursor uint64
	var pageCount int64 = 25
	var err error
	var lock storage.ILock
	defer func() {
		if lock != nil && err != nil {
			_, lockErr := lock.UnlockContext(ctx)
			if lockErr != nil {
				err = fmt.Errorf("while handling '%w' another error occurred: '%v'", err, lockErr)
			}
		}
	}()

	for {
		// for each available partition, get the nunmber of tuples in the set
		partitions, err := obj.keyStorage.GetTablePartitions(ctx, tableName, pageCursor, pageCount)
		if err != nil {
			return elements.Partition{}, nil, nil, err
		}

		if len(partitions) == 0 {
			break
		}

		for _, part := range partitions {
			// check if the partition has been batched long enough
			partTs, err := obj.keyStorage.GetTablePartitionTimestamp(ctx, part)
			if err != nil {
				return elements.Partition{}, nil, nil, err
			}

			obj.logger.Info("partition ts", slog.Time("ts", partTs))
			if time.Since(partTs) < batchDelay {
				continue
			}

			// check if the set has partition items
			items, err := obj.keyStorage.GetTablePartitionItems(ctx, part, batchCount)
			if err != nil {
				return elements.Partition{}, nil, nil, err
			}

			if len(items) == 0 {
				continue
			}

			// if it does then lock the partition set and get the items
			// if the partition set is already locked then try the next partition set
			lock, err = obj.keyStorage.ClaimPartition(ctx, part, obj.options.PartitionLockDuration)
			if errors.Is(err, storage.ErrLockFailed) {
				continue
			} else if err != nil {
				return elements.Partition{}, nil, nil, err
			}

			// to convert the items to an arrow record we need to know the schema
			// of the table partition. Pass the table definition to the avro to arrow
			// converter function. The converter will return an arrow record with the
			// correct schema and the items will be converted to the correct format.
			// The tuples are defined by the partition columns so only those columns
			// will be present in the record.

			// convert the items to an arrow record
			table, err := obj.tableRegistry.GetTable(tableName)
			if err != nil {
				return elements.Partition{}, nil, nil, err
			}

			tuples, err := AvroToArrow(obj.allocator, table, part.SubscriptionSourceName, items)
			if err != nil {
				return elements.Partition{}, nil, nil, err
			}

			return part, lock, tuples, nil
		}

		pageCursor += uint64(pageCount)
	}

	return elements.Partition{}, nil, nil, ErrNoPartitionsAvailable
}

func (obj *Inserter) InsertTuples(ctx context.Context, tableName, sourceName string, tuples arrow.Record) error {
	obj.logger.Info("inserting new tuples")

	table, err := obj.tableRegistry.GetTable(tableName)
	if err != nil {
		return errs.Wrap(err, fmt.Errorf("unable to get table %s from the table registry", tableName))
	}

	subscription, err := table.GetSubscriptionBySourceName(sourceName)
	if err != nil {
		return errs.Wrap(err, fmt.Errorf("unable to get subscription %s for table %s", sourceName, tableName))
	}

	// validate that the tuples are the correct format
	columns := subscription.Columns()
	if len(columns) != int(tuples.NumCols()) {
		return errs.NewStackError(
			fmt.Errorf(
				"%w| record has %d columns while subscription has %d columns",
				ErrTupleColumnsDifferentThanSubscription,
				int(tuples.NumCols()),
				len(columns),
			))
	}
	for _, column := range columns {
		if !tuples.Schema().HasField(column.Name) {
			return errs.NewStackError(
				fmt.Errorf(
					"%w| column %s missing from record",
					ErrTupleColumnsDifferentThanSubscription,
					column.Name,
				))
		}
	}

	// convert the tuples to avro format
	avroData, err := ArrowToAvro(tuples)
	if err != nil {
		return err
	}

	// get the partition for each tuple as an arrow array
	partitionKeyArr, err := PartitionKeys(obj.allocator, tuples, table.ColumnPartitions())
	if err != nil {
		return err
	}
	defer partitionKeyArr.Release()

	// add the tuples to the batch map so the storage can
	// be called fewer times
	batchMap := make(map[string]map[int]struct{})
	for idx := 0; idx < partitionKeyArr.Len(); idx++ {
		partitionKey := partitionKeyArr.Value(idx)
		if _, ok := batchMap[partitionKey]; !ok {
			batchMap[partitionKey] = make(map[int]struct{})
		}
		batchMap[partitionKey][idx] = struct{}{}
	}

	// insert each batch of tuples into the tables partition sets
	for partitionKey, idxSet := range batchMap {
		partition := elements.Partition{TableName: tableName, SubscriptionSourceName: subscription.SourceName(), Key: partitionKey}

		// add the items to the partition
		dataItems := make([][]byte, len(idxSet))
		var i int
		for idx := range idxSet {
			dataItems[i] = avroData[idx]
			i++
		}
		_, err = obj.keyStorage.AddItemsToTablePartition(
			ctx,
			partition,
			dataItems,
		)
		if err != nil {
			return err
		}

		// set the timestamp for the partition
		_, err := obj.keyStorage.SetTablePartitionTimestamp(ctx, partition)
		if err != nil {
			return err
		}

	}

	return nil
}
