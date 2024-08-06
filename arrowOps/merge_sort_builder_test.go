package arrowops

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

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

}

func TestValidateSampleRecord(t *testing.T) {

}
