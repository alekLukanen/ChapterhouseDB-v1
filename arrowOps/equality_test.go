package arrowops

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func mockData(mem *memory.GoAllocator, size int, method string, addProcessingColumns bool) arrow.Record {
	fields := make([]arrow.Field, 0)
	fields = append(fields, arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Uint32})
	fields = append(fields, arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Float32})
	fields = append(fields, arrow.Field{Name: "c", Type: arrow.BinaryTypes.String})
	if addProcessingColumns {
		fields = append(fields, arrow.Field{Name: "_updated_ts", Type: arrow.FixedWidthTypes.Timestamp_ms})
		fields = append(fields, arrow.Field{Name: "_created_ts", Type: arrow.FixedWidthTypes.Timestamp_ms})
		fields = append(fields, arrow.Field{Name: "_processed_ts", Type: arrow.FixedWidthTypes.Timestamp_ms})
	}

	rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(fields, nil))
	defer rb1.Release()

	currentTime := time.Now().UTC()

	aValues := make([]uint32, size)
	bValues := make([]float32, size)
	cValues := make([]string, size)

	updatedValues := make([]arrow.Timestamp, size)
	createdValues := make([]arrow.Timestamp, size)
	processedValues := make([]arrow.Timestamp, size)

	if method == "ascending" {
		for i := 0; i < size; i++ {
			aValues[i] = uint32(i)
			bValues[i] = float32(i)
			cValues[i] = strconv.Itoa(i)

			nextTs, err := arrow.TimestampFromTime(currentTime.Add(time.Duration(i)*time.Second), arrow.Millisecond)
			if err != nil {
				panic(err)
			}
			createdValues[i] = nextTs
			updatedValues[i] = nextTs
			processedValues[i] = nextTs
		}
	} else if method == "descending" {
		for i := 0; i < size; i++ {
			aValues[i] = uint32(size - 1 - i)
			bValues[i] = float32(size - 1 - i)
			cValues[i] = strconv.Itoa(size - 1 - i)

			nextTs, err := arrow.TimestampFromTime(currentTime.Add(-1*time.Duration(i)*time.Second), arrow.Millisecond)
			if err != nil {
				panic(err)
			}
			createdValues[i] = nextTs
			updatedValues[i] = nextTs
			processedValues[i] = nextTs
		}
	} else if method == "random" {
		for i := 0; i < size; i++ {
			val := rand.Intn(size)
			aValues[i] = uint32(val)
			bValues[i] = float32(val)
			cValues[i] = strconv.Itoa(val)

			nextTs, err := arrow.TimestampFromTime(
				currentTime.Add(-1*time.Duration(size)).Add(time.Duration(val)*time.Second),
				arrow.Millisecond,
			)
			if err != nil {
				panic(err)
			}
			createdValues[i] = nextTs
			updatedValues[i] = nextTs
			processedValues[i] = nextTs
		}
	} else {
		panic("invalid method")
	}

	rb1.Field(0).(*array.Uint32Builder).AppendValues(aValues, nil)
	rb1.Field(1).(*array.Float32Builder).AppendValues(bValues, nil)
	rb1.Field(2).(*array.StringBuilder).AppendValues(cValues, nil)

	if addProcessingColumns {
		rb1.Field(3).(*array.TimestampBuilder).AppendValues(updatedValues, nil)
		rb1.Field(4).(*array.TimestampBuilder).AppendValues(createdValues, nil)
		rb1.Field(5).(*array.TimestampBuilder).AppendValues(processedValues, nil)
	}

	return rb1.NewRecord()
}

func BenchmarkRecordsEqual(b *testing.B) {
	for _, size := range TEST_SIZES {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mem := memory.NewGoAllocator()
				b.StopTimer()
				// create large records to compare
				r1 := mockData(mem, 1_000_000, "ascending", false)
				defer r1.Release()

				r2 := mockData(mem, 1_000_000, "ascending", false)
				defer r2.Release()

				b.StartTimer()

				if !array.RecordEqual(r1, r2) {
					b.Fatalf("expected records to be equal")
				} else {
					r1.Release()
					r2.Release()
				}
			}
		})
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
	if !array.RecordEqual(record, expectedRecord) {
		t.Logf("record: %v", record)
		t.Logf("expectedRecord: %v", expectedRecord)
		t.Errorf("expected records to be equal")
	}
}
