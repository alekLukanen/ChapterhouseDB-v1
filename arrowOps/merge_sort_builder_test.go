package arrowops

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func BenchmarkValidateSampleRecord(b *testing.B) {
	mem := memory.NewGoAllocator()

	for _, size := range TEST_SIZES {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			for idx := 0; idx < b.N; idx++ {
				b.StopTimer()
				data := mockData(mem, size, "ascending", false)

				schema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
				}, nil)

				keyBldr := array.NewRecordBuilder(mem, schema)
				defer keyBldr.Release()
				for i := 0; i < size; i++ {
					keyBldr.Field(0).(*array.Uint32Builder).Append(uint32(i))
				}
				keyRec := keyBldr.NewRecord()
				defer keyRec.Release()
				b.StartTimer()

				err := ValidateSampleRecord(keyRec, data, []string{"a"})
				if err != nil {
					b.Errorf("unexpected error: %s", err)
				}

			}
		})
	}

}

/*
func TestParquetRecordMergeSortBuilderOnMultipleFiles(t *testing.T) {

	ctx := context.Background()
	mem := memory.NewGoAllocator()
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	workingDir, err := os.MkdirTemp("", "test-parquet-builder")
	if err != nil {
		t.Errorf("fail to create temp dir with error '%s'", err)
		return
	}
	defer os.RemoveAll(workingDir)

	allData := mockData(mem, 100, "ascending", true)

}
*/

func TestParquetRecordMergeSortBuilder(t *testing.T) {

	ctx := context.Background()
	mem := memory.NewGoAllocator()
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	workingDir, err := os.MkdirTemp("", "test-parquet-builder")
	if err != nil {
		t.Errorf("failed to create temp dir with error '%s'", err)
		return
	}
	defer os.RemoveAll(workingDir)

	// define the schema ////////////////////////////
	dataSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Uint32, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"0"})},
		{Name: "b", Type: arrow.PrimitiveTypes.Float32, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"})},
		{Name: "c", Type: arrow.BinaryTypes.String, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"2"})},
		{Name: "_updated_ts", Type: arrow.FixedWidthTypes.Timestamp_ms, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"3"})},
		{Name: "_created_ts", Type: arrow.FixedWidthTypes.Timestamp_ms, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"4"})},
		{Name: "_processed_ts", Type: arrow.FixedWidthTypes.Timestamp_ms, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"5"})},
	}, nil)
	keySchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Uint32, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"0"})},
	}, nil)

	currentTimestamp, err := arrow.TimestampFromTime(time.Now().UTC(), arrow.Millisecond)
	if err != nil {
		t.Errorf("failed to create timestamp: %s", err)
	}
	///////////////////////////////////////////////////

	// build data records /////////////////////////////
	bldr1 := array.NewRecordBuilder(mem, dataSchema)
	defer bldr1.Release()
	bldr1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2, 3, 10}, nil)
	bldr1.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1., 2., 3., 10.}, nil)
	bldr1.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1", "s2", "s3", "s10"}, nil)
	bldr1.Field(3).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp, currentTimestamp, currentTimestamp, currentTimestamp,
		},
		nil)
	bldr1.Field(4).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp, currentTimestamp, currentTimestamp, currentTimestamp,
		}, nil)
	bldr1.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp, currentTimestamp, currentTimestamp, currentTimestamp,
		}, nil)

	bldr2 := array.NewRecordBuilder(mem, dataSchema)
	defer bldr2.Release()
	bldr2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{3, 4}, nil)
	bldr2.Field(1).(*array.Float32Builder).AppendValues([]float32{3., 4.}, nil)
	bldr2.Field(2).(*array.StringBuilder).AppendValues([]string{"s3-modified", "s4"}, nil)
	bldr2.Field(3).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		},
		nil)
	bldr2.Field(4).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)
	bldr2.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)

	rec1 := bldr1.NewRecord()
	rec2 := bldr2.NewRecord()
	defer rec1.Release()
	defer rec2.Release()
	////////////////////////////////////////////////////////

	// build key record ////////////////////////////////////
	bldr3 := array.NewRecordBuilder(mem, keySchema)
	defer bldr3.Release()
	bldr3.Field(0).(*array.Uint32Builder).AppendValues([]uint32{2, 3, 4}, nil)

	keyRec := bldr3.NewRecord()
	defer keyRec.Release()
	////////////////////////////////////////////////////////

	// write rec1 to a parquet file ////////////////////////
	file1 := fmt.Sprintf("%s/parquet1.parquet", workingDir)
	err = WriteRecordToParquetFile(ctx, mem, rec1, file1)
	if err != nil {
		t.Errorf("failed to write record to parquet file: %s", err)
	}
	////////////////////////////////////////////////////////

	builder, err := NewParquetRecordMergeSortBuilder(
		logger,
		mem,
		keyRec,
		rec2,
		workingDir,
		[]string{"a"},
		[]string{"a", "b", "c"},
		2,
	)
	if err != nil {
		t.Errorf("failed to construct record merge sort builder with error '%s'", err)
	}
	defer builder.Release()

	// build first record //////////////////////////////////
	firstParquetFiles, err := builder.BuildNextFiles(ctx, file1)
	if err != nil {
		t.Fatalf("failed to build next with error '%s'", err)
	}
	if len(firstParquetFiles) != 2 {
		t.Fatalf("expected 2 parquet file, got %d", len(firstParquetFiles))
	}
	/////////////////////////////////////////////////////////

	par1, err := ReadParquetFile(ctx, mem, firstParquetFiles[0].FilePath)
	if err != nil {
		t.Fatalf("failed to read parquet file with error '%s'", err)
	}
	if len(par1) != 1 {
		t.Fatalf("expected 1 record, got %d", len(par1))
	}

	par2, err := ReadParquetFile(ctx, mem, firstParquetFiles[1].FilePath)
	if err != nil {
		t.Fatalf("failed to read parquet file with error '%s'", err)
	}
	if len(par2) != 1 {
		t.Fatalf("expected 1 record, got %d", len(par2))
	}

	par1Rec := par1[0]
	par2Rec := par2[0]

	// build expected records for first files ///////////////
	bldrPar1 := array.NewRecordBuilder(mem, dataSchema)
	defer bldrPar1.Release()
	bldrPar1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1}, nil)
	bldrPar1.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1.}, nil)
	bldrPar1.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1"}, nil)
	bldrPar1.Field(3).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		},
		nil)
	bldrPar1.Field(4).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)
	bldrPar1.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)

	bldrPar2 := array.NewRecordBuilder(mem, dataSchema)
	defer bldrPar2.Release()
	bldrPar2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{3, 4}, nil)
	bldrPar2.Field(1).(*array.Float32Builder).AppendValues([]float32{3., 4.}, nil)
	bldrPar2.Field(2).(*array.StringBuilder).AppendValues([]string{"s3-modified", "s4"}, nil)
	bldrPar2.Field(3).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		},
		nil)
	bldrPar2.Field(4).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)
	bldrPar2.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)

	expectedPar1 := bldrPar1.NewRecord()
	expectedPar2 := bldrPar2.NewRecord()
	defer expectedPar1.Release()
	defer expectedPar2.Release()
	/////////////////////////////////////////////////////////

	if !RecordsEqual(par1Rec, expectedPar1, "a", "b", "c") {
		t.Log("par1Rec: ", par1Rec)
		t.Log("expectedPar1: ", expectedPar1)
		t.Errorf("expected records to be equal")
		return
	}
	if !RecordsEqual(par2Rec, expectedPar2, "a", "b", "c") {
		t.Log("par2Rec: ", par2Rec)
		t.Log("expectedPar2: ", expectedPar2)
		t.Errorf("expected records to be equal")
		return
	}

	// build second record //////////////////////////////////
	lastParquetFiles, err := builder.BuildLastFiles(ctx)
	if err != nil {
		t.Fatalf("failed to build last with error '%s'", err)
	}
	if len(lastParquetFiles) != 1 {
		t.Fatalf("expected 1 parquet file, got %d", len(lastParquetFiles))
	}
	//////////////////////////////////////////////////////////

	par3, err := ReadParquetFile(ctx, mem, lastParquetFiles[0].FilePath)
	if err != nil {
		t.Fatalf("failed to read parquet file with error '%s'", err)
	}
	if len(par3) != 1 {
		t.Fatalf("expected 1 record, got %d", len(par3))
	}

	par3Rec := par3[0]

	// build expected records for first files ///////////////
	bldrPar3 := array.NewRecordBuilder(mem, dataSchema)
	defer bldrPar3.Release()
	bldrPar3.Field(0).(*array.Uint32Builder).AppendValues([]uint32{10}, nil)
	bldrPar3.Field(1).(*array.Float32Builder).AppendValues([]float32{10.}, nil)
	bldrPar3.Field(2).(*array.StringBuilder).AppendValues([]string{"s10"}, nil)
	bldrPar3.Field(3).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp,
		},
		nil)
	bldrPar3.Field(4).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp,
		}, nil)
	bldrPar3.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp,
		}, nil)

	expectedPar3 := bldrPar3.NewRecord()
	defer expectedPar3.Release()
	/////////////////////////////////////////////////////////

	if !RecordsEqual(par3Rec, expectedPar3, "a", "b", "c") {
		t.Log("par3Rec: ", par3Rec)
		t.Log("expectedPar3: ", expectedPar3)
		t.Errorf("expected records to be equal")
		return
	}

}

func TestMergeSortBuilder(t *testing.T) {

	mem := memory.NewGoAllocator()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dataSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "b", Type: arrow.PrimitiveTypes.Float32},
		{Name: "c", Type: arrow.BinaryTypes.String},
		{Name: "_updated_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
		{Name: "_created_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
		{Name: "_processed_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
	}, nil)
	keySchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
	}, nil)

	currentTimestamp, err := arrow.TimestampFromTime(time.Now().UTC(), arrow.Millisecond)
	if err != nil {
		t.Errorf("failed to create timestamp: %s", err)
	}

	// build data records /////////////////////////////
	bldr1 := array.NewRecordBuilder(mem, dataSchema)
	defer bldr1.Release()
	bldr1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
	bldr1.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1., 2.}, nil)
	bldr1.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1", "s2"}, nil)
	bldr1.Field(3).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp, currentTimestamp,
		},
		nil)
	bldr1.Field(4).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp, currentTimestamp,
		}, nil)
	bldr1.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp, currentTimestamp,
		}, nil)

	bldr2 := array.NewRecordBuilder(mem, dataSchema)
	defer bldr2.Release()
	bldr2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{3, 4}, nil)
	bldr2.Field(1).(*array.Float32Builder).AppendValues([]float32{3., 4.}, nil)
	bldr2.Field(2).(*array.StringBuilder).AppendValues([]string{"s3", "s4"}, nil)
	bldr2.Field(3).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		},
		nil)
	bldr2.Field(4).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)
	bldr2.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)

	rec1 := bldr1.NewRecord()
	rec2 := bldr2.NewRecord()
	defer rec1.Release()
	defer rec2.Release()
	////////////////////////////////////////////////////////

	// build key record ////////////////////////////////////
	bldr3 := array.NewRecordBuilder(mem, keySchema)
	defer bldr3.Release()
	bldr3.Field(0).(*array.Uint32Builder).AppendValues([]uint32{2, 3, 4}, nil)

	keyRec := bldr3.NewRecord()
	defer keyRec.Release()
	////////////////////////////////////////////////////////

	// build expected records //////////////////////////////
	bldr4 := array.NewRecordBuilder(mem, dataSchema)
	defer bldr4.Release()
	bldr4.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1}, nil)
	bldr4.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1.}, nil)
	bldr4.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1"}, nil)
	bldr4.Field(3).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		},
		nil)
	bldr4.Field(4).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)
	bldr4.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)

	bldr5 := array.NewRecordBuilder(mem, dataSchema)
	defer bldr5.Release()
	bldr5.Field(0).(*array.Uint32Builder).AppendValues([]uint32{3, 4}, nil)
	bldr5.Field(1).(*array.Float32Builder).AppendValues([]float32{3., 4.}, nil)
	bldr5.Field(2).(*array.StringBuilder).AppendValues([]string{"s3", "s4"}, nil)
	bldr5.Field(3).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		},
		nil)
	bldr5.Field(4).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)
	bldr5.Field(5).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			currentTimestamp, currentTimestamp,
		}, nil)

	expectedRec1 := bldr4.NewRecord()
	expectedRec2 := bldr5.NewRecord()
	defer expectedRec1.Release()
	defer expectedRec2.Release()
	//////////////////////////////////////////////////////////

	// Create a MergeSortBuilder
	builder, err := NewRecordMergeSortBuilder(logger, mem, keyRec, rec2, []string{"a"}, []string{"a", "b", "c"}, 2)
	if err != nil {
		t.Errorf("failed to construct record merge sort builder")
	}
	defer builder.Release()

	err = builder.AddMainLineRecords([]arrow.Record{rec1})
	if err != nil {
		t.Errorf("failed to add main line records: %s", err)
	}

	// build first record //////////////////////////////////
	newRec1, err := builder.BuildNextRecord()
	if err != nil {
		t.Fatalf("failed to build next record: %s", err)
	}
	if !RecordsEqual(newRec1, expectedRec1, "a", "b", "c") {
		t.Log("newRecord: ", newRec1)
		t.Log("expectedRec1: ", expectedRec1)
		t.Errorf("expected records to be equal")
	}
	///////////////////////////////////////////////////////

	// build second record ////////////////////////////////
	newRec2, err := builder.BuildNextRecord()
	if err != nil {
		t.Fatalf("unexpected error '%s'", err)
	}
	if !RecordsEqual(newRec2, expectedRec2, "a", "b", "c") {
		t.Log("newRecord: ", newRec2)
		t.Log("expectedRec2: ", expectedRec2)
		t.Errorf("expected records to be equal")
	}
	///////////////////////////////////////////////////////

	// build last record //////////////////////
	newRec3, err := builder.BuildLastRecord()
	if !errors.Is(err, ErrNoMoreRecords) {
		t.Fatalf("unexpected error '%s'", err)
	}
	if newRec3 != nil {
		t.Log("newRecord: ", newRec3)
		t.Errorf("expected records to be nil")
	}

}

func TestValidateSampleRecord(t *testing.T) {

	mem := memory.NewGoAllocator()

	currentTimestamp, err := arrow.TimestampFromTime(time.Now().UTC(), arrow.Millisecond)
	if err != nil {
		t.Errorf("failed to create timestamp: %s", err)
	}

	testCases := []struct {
		name              string
		processingKeyFunc func() arrow.Record
		recordFunc        func() arrow.Record
		primaryColumns    []string
		expectedErr       error
	}{
		{
			name: "valid_simple_record",
			processingKeyFunc: func() arrow.Record {
				keySchema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
				}, nil)
				bldr := array.NewRecordBuilder(mem, keySchema)
				defer bldr.Release()
				bldr.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2, 3}, nil)
				return bldr.NewRecord()
			},
			recordFunc: func() arrow.Record {
				dataSchema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
					{Name: "b", Type: arrow.PrimitiveTypes.Float32},
					{Name: "c", Type: arrow.BinaryTypes.String},
					{Name: "_updated_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
					{Name: "_created_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
					{Name: "_processed_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
				}, nil)
				bldr := array.NewRecordBuilder(mem, dataSchema)
				defer bldr.Release()
				bldr.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
				bldr.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1., 2.}, nil)
				bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1", "s2"}, nil)
				bldr.Field(3).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{
						currentTimestamp, currentTimestamp, currentTimestamp,
					},
					nil)
				bldr.Field(4).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{
						currentTimestamp, currentTimestamp, currentTimestamp,
					}, nil)
				bldr.Field(5).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{
						currentTimestamp, currentTimestamp, currentTimestamp,
					}, nil)

				return bldr.NewRecord()
			},
			primaryColumns: []string{"a"},
			expectedErr:    nil,
		},
		{
			name: "simple_record_with_duplicates",
			processingKeyFunc: func() arrow.Record {
				keySchema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
				}, nil)
				bldr := array.NewRecordBuilder(mem, keySchema)
				defer bldr.Release()
				bldr.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2, 3}, nil)
				return bldr.NewRecord()
			},
			recordFunc: func() arrow.Record {
				dataSchema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
					{Name: "b", Type: arrow.PrimitiveTypes.Float32},
					{Name: "c", Type: arrow.BinaryTypes.String},
					{Name: "_updated_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
					{Name: "_created_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
					{Name: "_processed_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
				}, nil)
				bldr := array.NewRecordBuilder(mem, dataSchema)
				defer bldr.Release()
				bldr.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 1, 2}, nil)
				bldr.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1., 1., 2.}, nil)
				bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1", "s1", "s2"}, nil)
				bldr.Field(3).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{
						currentTimestamp, currentTimestamp, currentTimestamp, currentTimestamp,
					},
					nil)
				bldr.Field(4).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{
						currentTimestamp, currentTimestamp, currentTimestamp, currentTimestamp,
					}, nil)
				bldr.Field(5).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{
						currentTimestamp, currentTimestamp, currentTimestamp, currentTimestamp,
					}, nil)

				return bldr.NewRecord()
			},
			primaryColumns: []string{"a"},
			expectedErr:    ErrRecordHasDuplicateRows,
		},
		{
			name: "simple_record_with_key_not_in_key_record",
			processingKeyFunc: func() arrow.Record {
				keySchema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
				}, nil)
				bldr := array.NewRecordBuilder(mem, keySchema)
				defer bldr.Release()
				bldr.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 2, 3}, nil)
				return bldr.NewRecord()
			},
			recordFunc: func() arrow.Record {
				dataSchema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
					{Name: "b", Type: arrow.PrimitiveTypes.Float32},
					{Name: "c", Type: arrow.BinaryTypes.String},
					{Name: "_updated_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
					{Name: "_created_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
					{Name: "_processed_ts", Type: arrow.FixedWidthTypes.Timestamp_ms},
				}, nil)
				bldr := array.NewRecordBuilder(mem, dataSchema)
				defer bldr.Release()
				bldr.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
				bldr.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1., 2.}, nil)
				bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1", "s2"}, nil)
				bldr.Field(3).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{
						currentTimestamp, currentTimestamp, currentTimestamp,
					},
					nil)
				bldr.Field(4).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{
						currentTimestamp, currentTimestamp, currentTimestamp,
					}, nil)
				bldr.Field(5).(*array.TimestampBuilder).AppendValues(
					[]arrow.Timestamp{
						currentTimestamp, currentTimestamp, currentTimestamp,
					}, nil)

				return bldr.NewRecord()
			},
			primaryColumns: []string{"a"},
			expectedErr:    ErrRecordContainsRowsNotInProcessedKey,
		},
	}

	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("%d_%s", idx, tc.name), func(t *testing.T) {
			err := ValidateSampleRecord(tc.processingKeyFunc(), tc.recordFunc(), tc.primaryColumns)
			if err != tc.expectedErr {
				t.Errorf("expected error: %s, got: %s", tc.expectedErr, err)
			}
		})
	}

}
