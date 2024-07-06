package arrowops

import (
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func mockData(mem *memory.GoAllocator, size int) arrow.Record {
	rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		},
		nil,
	))
	defer rb1.Release()

	aValues := make([]uint32, size)
	bValues := make([]float32, size)
	cValues := make([]string, size)
	for i := 0; i < size; i++ {
		aValues[i] = uint32(i)
		bValues[i] = float32(i)
		cValues[i] = strconv.Itoa(i)
	}

	rb1.Field(0).(*array.Uint32Builder).AppendValues(aValues, nil)
	rb1.Field(1).(*array.Float32Builder).AppendValues(bValues, nil)
	rb1.Field(2).(*array.StringBuilder).AppendValues(cValues, nil)

	return rb1.NewRecord()
}

func BenchmarkRecordsEqual(b *testing.B) {
	mem := memory.NewGoAllocator()

	// create large records to compare
	r1 := mockData(mem, 1_000_000)
	defer r1.Release()

	r2 := mockData(mem, 1_000_000)
	defer r2.Release()

	b.ResetTimer()

	if equal, ifErr := RecordsEqual(r1, r2); ifErr != nil {
		b.Fatalf("received error while comparing records: %s", ifErr)
	} else if !equal {
		b.Fatalf("expected records to be equal")
	}

}

func TestRecordsEqual(t *testing.T) {
	mem := memory.NewGoAllocator()
	// record to test
	rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		},
		nil,
	))
	defer rb1.Release()
	rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3}, nil)
	rb1.Field(1).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0}, nil)
	rb1.Field(2).(*array.StringBuilder).AppendValues([]string{"s1", "s2", "s3"}, nil)
	record := rb1.NewRecord()
	defer record.Release()
	// expected record
	rb2 := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		},
		nil,
	))
	defer rb2.Release()
	rb2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3}, nil)
	rb2.Field(1).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0}, nil)
	rb2.Field(2).(*array.StringBuilder).AppendValues([]string{"s1", "s2", "s3"}, nil)
	expectedRecord := rb2.NewRecord()
	defer expectedRecord.Release()
	// compare records
	equal, err := RecordsEqual(record, expectedRecord)
	if err != nil {
		t.Errorf("RecordsEqual() error = %v, wantErr %v", err, nil)
		return
	}
	if !equal {
		t.Logf("record: %v", record)
		t.Logf("expectedRecord: %v", expectedRecord)
		t.Errorf("expected records to be equal")
	}
}
