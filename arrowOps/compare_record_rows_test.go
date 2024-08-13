package arrowops

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func BenchmarkCompareRecordRowsOnAllColumns(b *testing.B) {

	for _, size := range TEST_SIZES {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			for idx := 0; idx < b.N; idx++ {
				mem := memory.NewGoAllocator()
				b.StopTimer()
				record1 := mockData(mem, size, "ascending")
				record2 := mockData(mem, size, "ascending")
				defer record1.Release()
				defer record2.Release()
				b.StartTimer()
				for i := 0; i < size; i++ {
					val, err := CompareRecordRows(record1, record2, i, i)
					if err != nil {
						b.Errorf("received unexpected error: %s", err)
					}
					if val != 0 {
						b.Errorf("expected 0, got %d", val)
					}
				}
			}
		})
	}

}

func BenchmarkCompareRecordRowsWithColumnSubset(b *testing.B) {

	columnNames := []string{"a", "b", "c"}

	for _, size := range TEST_SIZES {
		for _, column := range columnNames {
			b.Run(fmt.Sprintf("size_%d|column_%s", size, column), func(b *testing.B) {
				for idx := 0; idx < b.N; idx++ {
					mem := memory.NewGoAllocator()
					b.StopTimer()
					record1 := mockData(mem, size, "ascending")
					record2 := mockData(mem, size, "ascending")
					defer record1.Release()
					defer record2.Release()
					b.StartTimer()
					for i := 0; i < size; i++ {
						val, err := CompareRecordRows(record1, record2, i, i, column)
						if err != nil {
							b.Errorf("received unexpected error: %s", err)
						}
						if val != 0 {
							b.Errorf("expected 0, got %d", val)
						}
					}
				}
			})
		}
	}

}

func TestCompareRecordRows(t *testing.T) {

	mem := memory.NewGoAllocator()

	testCases := []struct {
		caseName    string
		record1     func() arrow.Record
		record2     func() arrow.Record
		index1Vals  []int
		index2Vals  []int
		fields      []string
		expectedVal []int
		expectedErr error
	}{
		{
			caseName: "basic_record",
			record1: func() arrow.Record {
				recBuilder := array.NewRecordBuilder(
					mem, arrow.NewSchema(
						[]arrow.Field{
							{Name: "a", Type: arrow.PrimitiveTypes.Int64},
							{Name: "b", Type: arrow.BinaryTypes.String},
							{Name: "c", Type: arrow.PrimitiveTypes.Float64},
						}, nil),
				)
				defer recBuilder.Release()

				recBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
				recBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
				recBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
				return recBuilder.NewRecord()
			},
			record2: func() arrow.Record {
				recBuilder := array.NewRecordBuilder(
					mem, arrow.NewSchema(
						[]arrow.Field{
							{Name: "a", Type: arrow.PrimitiveTypes.Int64},
							{Name: "b", Type: arrow.BinaryTypes.String},
							{Name: "c", Type: arrow.PrimitiveTypes.Float64},
						}, nil),
				)
				defer recBuilder.Release()

				recBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 77, 0}, nil)
				recBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "hh", "c"}, nil)
				recBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 77.77, 3.3}, nil)
				return recBuilder.NewRecord()

			},
			index1Vals:  []int{0, 1, 2},
			index2Vals:  []int{0, 1, 2},
			fields:      []string{"a", "b"},
			expectedVal: []int{0, -1, 1},
			expectedErr: nil,
		},
		{
			caseName: "record_with_null_values",
			record1: func() arrow.Record {
				recBuilder := array.NewRecordBuilder(
					mem, arrow.NewSchema(
						[]arrow.Field{
							{Name: "a", Type: arrow.PrimitiveTypes.Int64},
							{Name: "b", Type: arrow.BinaryTypes.String},
							{Name: "c", Type: arrow.PrimitiveTypes.Float64},
						}, nil),
				)
				defer recBuilder.Release()

				recBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, []bool{false, true, true})
				recBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
				recBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, []bool{true, true, false})
				return recBuilder.NewRecord()
			},
			record2: func() arrow.Record {
				recBuilder := array.NewRecordBuilder(
					mem, arrow.NewSchema(
						[]arrow.Field{
							{Name: "a", Type: arrow.PrimitiveTypes.Int64},
							{Name: "b", Type: arrow.BinaryTypes.String},
							{Name: "c", Type: arrow.PrimitiveTypes.Float64},
						}, nil),
				)
				defer recBuilder.Release()

				recBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 77, 3}, nil)
				recBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "hh", "c"}, []bool{true, false, false})
				recBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 77.77, 11.1}, []bool{true, true, false})
				return recBuilder.NewRecord()

			},
			index1Vals:  []int{0, 1, 2},
			index2Vals:  []int{0, 1, 2},
			fields:      []string{"a", "b", "c"},
			expectedVal: []int{-1, -1, 1},
			expectedErr: nil,
		},
	}

	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d:%s", idx, testCase.caseName), func(t *testing.T) {
			record1 := testCase.record1()
			record2 := testCase.record2()
			defer record1.Release()
			defer record2.Release()
			for i := 0; i < len(testCase.index1Vals); i++ {
				val, err := CompareRecordRows(record1, record2, testCase.index1Vals[i], testCase.index2Vals[i], testCase.fields...)
				if val != testCase.expectedVal[i] {
					t.Errorf("[%d] expected value %d, got %d", i, testCase.expectedVal[i], val)
				}
				if err != testCase.expectedErr {
					t.Errorf("[%d] expected error %v, got %v", i, testCase.expectedErr, err)
				}
			}
		})
	}

}
