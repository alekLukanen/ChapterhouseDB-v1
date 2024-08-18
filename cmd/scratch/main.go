package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/alekLukanen/ChapterhouseDB/operations"
	"github.com/alekLukanen/ChapterhouseDB/partitionFuncs"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/errs"
)

func main() {

	BuildSampleRecord()

}

func BuildSampleRecord() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("Running ChapterhouseDB Scratch")

	ctx := context.Background()
	tableRegistery := operations.NewTableRegistry(ctx, logger)
	table1 := elements.NewTable("table1").
		AddColumns(
			elements.NewColumn("column1", &arrow.Int32Type{}),
			elements.NewColumn("column2", &arrow.BooleanType{}),
			elements.NewColumn("column3", &arrow.Float64Type{}),
		).
		AddColumnPartitions(
			elements.NewColumnPartition(
				"column1",
				partitionFuncs.NewIntegerRangePartitionOptions(10, 10),
			),
		).
		AddSubscriptionGroups(
			elements.NewSubscriptionGroup(
				"group1",
			).
				AddSubscriptions(
					elements.NewExternalSubscription(
						"externalTable1",
						nil,
						[]elements.Column{
							elements.NewColumn("column1", &arrow.Int32Type{}),
							elements.NewColumn("column2", &arrow.BooleanType{}),
							elements.NewColumn("column3", &arrow.Float64Type{}),
							elements.NewColumn("eventName", &arrow.StringType{}),
						},
					),
				),
		)

	err := tableRegistery.AddTables(table1)
	if err != nil {
		logger.Error("failed to add table to registery", slog.String("error", errs.ErrorWithStack(err)))
	}

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "column1", Type: &arrow.Int32Type{}},
			{Name: "column2", Type: &arrow.BooleanType{}},
			{Name: "column3", Type: &arrow.Float64Type{}},
			{Name: "eventName", Type: &arrow.StringType{}},
		}, nil,
	)
	recBuilder := array.NewRecordBuilder(pool, schema)
	defer recBuilder.Release()

	recBuilder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 10, 20, 29, 35, 36, 37}, nil)
	recBuilder.Field(1).(*array.BooleanBuilder).AppendValues([]bool{true, true, true, false, true, false, true, false, true, false}, nil)
	recBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{1., 2., 3., 4., 10., 20., 29., 35., 36., 37.}, nil)
	recBuilder.Field(3).(*array.StringBuilder).AppendValues([]string{"ev1", "ev1", "ev1", "ev1", "ev1", "ev1", "ev2", "ev2", "ev2", "ev2"}, nil)

	rec := recBuilder.NewRecord()
	defer rec.Release()

	logger.Info("record schema", slog.Any("schema", rec.Schema().String()), slog.Int("numFields", rec.Schema().NumFields()))
	logger.Info("record data", slog.Any("record", rec))

}

func InsertTuplesIntoKeyStorage() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("Running ChapterhouseDB Scratch")

	ctx := context.Background()

	keyStorage, err := storage.NewKeyStorage(ctx, logger, storage.KeyStorageOptions{
		Address:   "localhost:6379",
		Password:  "",
		KeyPrefix: "chapterhouseDB",
	})
	if err != nil {
		logger.Error("unable to start storage", slog.String("error", errs.ErrorWithStack(err)))
		return
	}

	tableRegistery := operations.NewTableRegistry(ctx, logger)

	table1 := elements.NewTable("table1").
		AddColumns(
			elements.NewColumn("column1", &arrow.Int32Type{}),
			elements.NewColumn("column2", &arrow.BooleanType{}),
			elements.NewColumn("column3", &arrow.Float64Type{}),
		).
		AddColumnPartitions(
			elements.NewColumnPartition(
				"column1",
				partitionFuncs.NewIntegerRangePartitionOptions(10, 10),
			),
		).
		AddSubscriptionGroups(
			elements.NewSubscriptionGroup(
				"group1",
			).
				AddSubscriptions(
					elements.NewExternalSubscription(
						"externalTable1",
						nil,
						[]elements.Column{
							elements.NewColumn("column1", &arrow.Int32Type{}),
							elements.NewColumn("column2", &arrow.BooleanType{}),
							elements.NewColumn("column3", &arrow.Float64Type{}),
							elements.NewColumn("eventName", &arrow.StringType{}),
						},
					),
				),
		)

	tableRegistery.AddTables(table1)

	pool := memory.NewGoAllocator()
	inserter := operations.NewInserter(
		logger,
		tableRegistery,
		keyStorage,
		pool,
		operations.InserterOptions{PartitionLockDuration: 15 * time.Second},
	)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "column1", Type: &arrow.Int32Type{}},
			{Name: "column2", Type: &arrow.BooleanType{}},
			{Name: "column3", Type: &arrow.Float64Type{}},
			{Name: "eventName", Type: &arrow.StringType{}},
		}, nil,
	)
	recBuilder := array.NewRecordBuilder(pool, schema)
	defer recBuilder.Release()

	recBuilder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 10, 20, 29, 35, 36, 37}, nil)
	recBuilder.Field(1).(*array.BooleanBuilder).AppendValues([]bool{true, true, true, false, true, false, true, false, true, false}, nil)
	recBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{1., 2., 3., 4., 10., 20., 29., 35., 36., 37.}, nil)
	recBuilder.Field(3).(*array.StringBuilder).AppendValues([]string{"ev1", "ev1", "ev1", "ev1", "ev1", "ev1", "ev2", "ev2", "ev2", "ev2"}, nil)

	rec := recBuilder.NewRecord()
	defer rec.Release()

	logger.Info("record schema", slog.Any("schema", rec.Schema().String()), slog.Int("numFields", rec.Schema().NumFields()))

	err = inserter.InsertTuples(ctx, table1.TableName(), "external.externalTable1", rec)
	if err != nil {
		logger.Error("unable to insert tuples", slog.String("error", errs.ErrorWithStack(err)))
		return
	}

	// Read partitions
	////////////////////////////////////////////////////
	/*
		  partitions, err := keyStorage.GetTablePartitions(ctx, "table1", 0, 100)
			if err != nil {
				logger.Error("unable to get partitions", slog.Any("error", err))
				return
			}

			for _, part := range partitions {

				logger.Info("partition", slog.String("key", part.Key))

				items, err := keyStorage.GetTablePartitionItems(ctx, elements.Partition{TableName: "table1", Key: part.Key}, 100)
				if err != nil {
					logger.Error("unable to get items", err)
					return
				}

				for idx, item := range items {
					logger.Info("item in key storage", slog.Int("idx", idx), slog.String("item", item))
				}
			}
	*/

	// Read record from partition and claim the lock
	//////////////////////////////////////////////////////

	logger.Info("attempt to read at most 10 partitions")
	for i := 0; i < 10; i++ {
		logger.Info("attempt to read an entire record", slog.Int("attempt", i))

		part, lock, record, err := inserter.GetPartition(ctx, "table1", 100, 5*time.Second)
		if err != nil {
			logger.Error("unable to read additional items", slog.String("error", errs.ErrorWithStack(err)))
			break
		}

		_, err = keyStorage.DeleteTablePartitionTimestamp(ctx, part)
		if err != nil {
			logger.Error("unable to delete partition timestamp", slog.String("error", errs.ErrorWithStack(err)))
			break
		}

		_, err = lock.UnlockContext(ctx)
		if err != nil {
			logger.Error("unable to unlock partition", slog.String("error", errs.ErrorWithStack(err)))
		}

		logger.Info("partition", slog.String("key", part.Key), slog.String("subscription", part.SubscriptionSourceName))
		logger.Info("record", slog.Any("record", record))
		logger.Info("record length", slog.Int64("length", record.NumRows()))
		logger.Info("recrod schema", slog.Any("schema", record.Schema().String()), slog.Int("numFields", record.Schema().NumFields()))
	}

	logger.Info("ChapterhouseDB Scratch Complete")
}
