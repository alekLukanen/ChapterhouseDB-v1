package arrowops

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func BenchmarkDeduplicateRecordUsingSingleColumnRandomData(b *testing.B) {
	for _, size := range TEST_SIZES {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				mem := memory.NewGoAllocator()
				// create large records to compare
				r1 := mockData(mem, size, "random", false)
				defer r1.Release()
				b.StartTimer()
				if val, ifErr := DeduplicateRecord(mem, r1, []string{"a"}, false); ifErr != nil {
					b.Fatalf("received error while sorting record '%s'", ifErr)
				} else if val == nil || val.NumRows() > int64(size) {
					b.Fatalf(
            "expected sorted record to have less than %d rows but had %d rows instead", 
            size, 
            val.NumRows(),
          )
				} else {
					val.Release()
					r1.Release()
				}
			}
		})
	}
}

func TestDeduplicateRecord(t *testing.T) {

  mem := memory.NewGoAllocator()

  testCases := []struct{
    caseName string
    recordBldr func() arrow.Record
    columns []string
    presortedByColumnsNames bool
    expectedRecordBldr func() arrow.Record
    expectedErr error
  }{
    {
      caseName: "no_duplicates",
      recordBldr: func() arrow.Record {
        return mockData(mem, 10, "ascending", false)
      },
      columns: []string{"a", "b", "c"},
      presortedByColumnsNames: false,
      expectedRecordBldr: func() arrow.Record {
        return mockData(mem, 10, "ascending", false)
      },
      expectedErr: nil,
    },
    {
      caseName: "all_duplicates",
      recordBldr: func() arrow.Record {
				recBuilder := array.NewRecordBuilder(
					mem, arrow.NewSchema(
						[]arrow.Field{
							{Name: "a", Type: arrow.PrimitiveTypes.Int64},
							{Name: "b", Type: arrow.BinaryTypes.String},
							{Name: "c", Type: arrow.PrimitiveTypes.Float64},
						}, nil),
				)
				defer recBuilder.Release()

				recBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{3, 3, 3}, nil)
				recBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"c", "c", "c"}, nil)
				recBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{3.3, 3.3, 3.3}, nil)
				return recBuilder.NewRecord()
			},
      columns: []string{"a", "b", "c"},
      presortedByColumnsNames: false,
      expectedRecordBldr:  func() arrow.Record {
				recBuilder := array.NewRecordBuilder(
					mem, arrow.NewSchema(
						[]arrow.Field{
							{Name: "a", Type: arrow.PrimitiveTypes.Int64},
							{Name: "b", Type: arrow.BinaryTypes.String},
							{Name: "c", Type: arrow.PrimitiveTypes.Float64},
						}, nil),
				)
				defer recBuilder.Release()

				recBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{3,}, nil)
				recBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"c",}, nil)
				recBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{3.3,}, nil)
				return recBuilder.NewRecord()
			},
      expectedErr: nil,
    },
  }

  for idx, tc := range testCases {
    t.Run(fmt.Sprintf("case_%d:%s", idx, tc.caseName), func(t *testing.T) {
      
      record := tc.recordBldr()
      defer record.Release()
      
      expectedRecord := tc.expectedRecordBldr()
      defer expectedRecord.Release()
      
      actualRecord, err := DeduplicateRecord(mem, record, tc.columns, tc.presortedByColumnsNames)
      if !errors.Is(err, tc.expectedErr) {
        t.Errorf("expected error: %v, got: %v", tc.expectedErr, err)
        return
      }
      
      if err != nil {
        defer actualRecord.Release()
      }
      
      if !array.RecordEqual(expectedRecord, actualRecord) {
        t.Errorf("expected record: %v, got: %v", expectedRecord, actualRecord)
        return
      }

    })
  }

}
