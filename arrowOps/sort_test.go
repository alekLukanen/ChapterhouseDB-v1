package arrowops

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func BenchmarkSortRecordWithSingleColumnAndDescendingData(b *testing.B) {
	for _, size := range TEST_SIZES {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				mem := memory.NewGoAllocator()
				// create large records to compare
				r1 := mockData(mem, size, "descending")
				defer r1.Release()
				b.StartTimer()
				if val, ifErr := SortRecord(mem, r1, []string{"a"}); ifErr != nil {
					b.Fatalf("received error while sorting record '%s'", ifErr)
				} else if val == nil || val.NumRows() != int64(size) {
					b.Fatalf("expected sorted record to have %d rows", size)
				} else {
					val.Release()
					r1.Release()
				}
			}
		})
	}
}

func BenchmarkSortRecordWithSingleColumnAndRandomData(b *testing.B) {
	for _, size := range TEST_SIZES {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				mem := memory.NewGoAllocator()
				// create large records to compare
				r1 := mockData(mem, size, "random")
				defer r1.Release()
				b.StartTimer()
				if val, ifErr := SortRecord(mem, r1, []string{"a"}); ifErr != nil {
					b.Fatalf("received error while sorting record '%s'", ifErr)
				} else if val == nil || val.NumRows() != int64(size) {
					b.Fatalf("expected sorted record to have %d rows", size)
				} else {
					val.Release()
					r1.Release()
				}
			}
		})
	}
}

func TestSortRecordWithSingleColumnAndDescendingData(t *testing.T) {
	mem := memory.NewGoAllocator()
	size := 1_000_000
	// create large records to compare
	r1 := mockData(mem, size, "descending")
	defer r1.Release()
	r2 := mockData(mem, size, "ascending")
	defer r2.Release()

	sortedRecord, err := SortRecord(mem, r1, []string{"a"})
	if err != nil {
		t.Fatalf("received error while sorting record '%s'", err)
	}
	defer sortedRecord.Release()

	if !array.RecordEqual(r2, sortedRecord) {
		t.Fatalf("expected records to be equal")
	}
}

func TestSortRecord(t *testing.T) {

	mem := memory.NewGoAllocator()

	rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		}, nil))
	defer rb1.Release()

	rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{4, 4, 3, 2, 1}, nil)
	rb1.Field(1).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0, 2.0, 1.0}, nil)
	rb1.Field(2).(*array.StringBuilder).AppendValues([]string{"s1", "s2", "s3", "s4", "s5"}, nil)

	r1 := rb1.NewRecord()
	defer r1.Release()

	rb2 := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		}, nil))
	defer rb2.Release()

	rb2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3, 4, 4}, nil)
	rb2.Field(1).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0, 1.0, 2.0}, nil)
	rb2.Field(2).(*array.StringBuilder).AppendValues([]string{"s5", "s4", "s3", "s1", "s2"}, nil)

	expectedRecord := rb2.NewRecord()
	defer expectedRecord.Release()

	sortedRecord, err := SortRecord(mem, r1, []string{"a", "b"})
	if err != nil {
		t.Fatalf("received error while sorting record '%s'", err)
	}
	defer sortedRecord.Release()

	if !array.RecordEqual(expectedRecord, sortedRecord) {
		t.Log("record: ", r1)
		t.Log("expectedRecord: ", expectedRecord)
		t.Log("sortedRecord: ", sortedRecord)
		t.Fatalf("expected records to be equal")
	}

}
