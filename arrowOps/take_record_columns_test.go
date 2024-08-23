package arrowops

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func TestTakeRecordColumns(t *testing.T) {

  mem := memory.NewGoAllocator()

  testCases := []struct{
    caseName string
    recordBldr func() arrow.Record
    columns []string
    expectedRecordBldr func() arrow.Record
    expectedErr error
  }{
    {
      caseName: "allColumns",
      recordBldr: func() arrow.Record {
        return mockData(mem, 10, "ascending", false)
      },
      columns: []string{"a", "b", "c"},
      expectedRecordBldr: func() arrow.Record {
        return mockData(mem, 10, "ascending", false)
      },
      expectedErr: nil,
    },
    {
      caseName: "singleColumn",
      recordBldr: func() arrow.Record {
        return mockData(mem, 10, "ascending", false)
              },
      columns: []string{"a"},
      expectedRecordBldr: func() arrow.Record {
        recordBldr := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}}, nil))
        defer recordBldr.Release()

        recordBldr.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
        return recordBldr.NewRecord()
      },
      expectedErr: nil,
    },
    {
      caseName: "emptyColumnSlice",
      recordBldr: func() arrow.Record {
        return mockData(mem, 10, "ascending", false)
              },
      columns: []string{},
      expectedRecordBldr: func() arrow.Record {
        return nil
      },
      expectedErr: ErrNoColumnsProvided,
    },
    {
      caseName: "columnNotInRecord",
      recordBldr: func() arrow.Record {
        return mockData(mem, 10, "ascending", false)
              },
      columns: []string{"zap"},
      expectedRecordBldr: func() arrow.Record {
        return nil
      },
      expectedErr: ErrColumnNotFound,
    },

  }

  for _, testCase := range testCases {
    t.Run(fmt.Sprintf("case_%s", testCase.caseName), func(t *testing.T) {
      record := testCase.recordBldr()
      defer record.Release()
      newRecord, err := TakeRecordColumns(record, testCase.columns)
      if !errors.Is(err, testCase.expectedErr) {
        t.Errorf("expected error:\n%v\ngot\n%v", testCase.expectedErr, err)
      }
      if err == nil {
        defer newRecord.Release()

        expectedRecord := testCase.expectedRecordBldr()
        defer expectedRecord.Release()

        if !RecordsEqual(expectedRecord, newRecord) {
          t.Errorf("expected record: %v, got: %v", expectedRecord, newRecord)
        }
      }
    })
  }
}
