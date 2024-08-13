package arrowops

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func BenchmarkConcatenateRecords(b *testing.B) {

	numRecs := []int{10, 100}
	recSizes := []int{100, 1_000, 10_000}
	for _, recVal := range numRecs {
		for _, sizeVal := range recSizes {
			b.Run(fmt.Sprintf("records=%d|size=%d", recVal, sizeVal), func(b *testing.B) {
				for idx := 0; idx < b.N; idx++ {
					mem := memory.NewGoAllocator()
					b.StopTimer()
					records := make([]arrow.Record, recVal)
					for recIdx := range recVal {
						records[recIdx] = mockData(mem, sizeVal, "ascending")
					}
					b.StartTimer()
					result, err := ConcatenateRecords(mem, records...)
					if err != nil {
						b.Errorf("received unexpected error: %s", err)
					}
					if result.NumRows() != int64(recVal*sizeVal) {
						b.Errorf("expected a record with '%d' rows but received '%d'", recVal*sizeVal, result.NumRows())
					}
				}
			})
		}
	}

}

func TestConcatenateRecords(t *testing.T) {

	mem := memory.NewGoAllocator()

	testCases := []struct {
		records        []arrow.Record
		expectedRecord arrow.Record
		expectedErr    error
	}{
		{
			records:        []arrow.Record{mockData(mem, 10, "ascending")},
			expectedRecord: mockData(mem, 10, "ascending"),
			expectedErr:    nil,
		},
		{
			records: func() []arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "b", Type: arrow.PrimitiveTypes.Float32},
						{Name: "c", Type: arrow.BinaryTypes.String},
					}, nil))
				defer rb1.Release()
				rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
				rb1.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1., 2.}, nil)
				rb1.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1", "s2"}, nil)
				rb2 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "b", Type: arrow.PrimitiveTypes.Float32},
						{Name: "c", Type: arrow.BinaryTypes.String},
					}, nil))
				defer rb2.Release()
				rb2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
				rb2.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1., 2.}, nil)
				rb2.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1", "s2"}, nil)
				return []arrow.Record{rb1.NewRecord(), rb2.NewRecord()}
			}(),
			expectedRecord: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "b", Type: arrow.PrimitiveTypes.Float32},
						{Name: "c", Type: arrow.BinaryTypes.String},
					}, nil))
				defer rb1.Release()
				rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2, 0, 1, 2}, nil)
				rb1.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 1., 2., 0., 1., 2.}, nil)
				rb1.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s1", "s2", "s0", "s1", "s2"}, nil)
				return rb1.NewRecord()
			}(),
		},
		{
			records: func() []arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "b", Type: arrow.PrimitiveTypes.Float32},
						{Name: "c", Type: arrow.BinaryTypes.String},
					}, nil))
				defer rb1.Release()
				rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 0, 2}, []bool{true, false, true})
				rb1.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 0., 2.}, []bool{true, false, true})
				rb1.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "", "s2"}, []bool{true, false, true})
				rb2 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "b", Type: arrow.PrimitiveTypes.Float32},
						{Name: "c", Type: arrow.BinaryTypes.String},
					}, nil))
				defer rb2.Release()
				rb2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 0, 2}, []bool{true, false, true})
				rb2.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 0., 2.}, []bool{true, false, true})
				rb2.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "", "s2"}, []bool{true, false, true})
				return []arrow.Record{rb1.NewRecord(), rb2.NewRecord()}
			}(),
			expectedRecord: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "b", Type: arrow.PrimitiveTypes.Float32},
						{Name: "c", Type: arrow.BinaryTypes.String},
					}, nil))
				defer rb1.Release()
				rb1.Field(0).(*array.Uint32Builder).AppendValues(
					[]uint32{0, 1, 2, 0, 1, 2}, []bool{true, false, true, true, false, true},
				)
				rb1.Field(1).(*array.Float32Builder).AppendValues(
					[]float32{0., 1., 2., 0., 1., 2.}, []bool{true, false, true, true, false, true},
				)
				rb1.Field(2).(*array.StringBuilder).AppendValues(
					[]string{"s0", "s1", "s2", "s0", "s1", "s2"}, []bool{true, false, true, true, false, true},
				)
				return rb1.NewRecord()
			}(),
		},
	}

	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", idx), func(t *testing.T) {

			result, err := ConcatenateRecords(mem, testCase.records...)
			if !errors.Is(err, testCase.expectedErr) {
				t.Errorf("expected error '%s' but received '%s'", testCase.expectedErr, err)
			}
			defer result.Release()
			if !array.RecordEqual(testCase.expectedRecord, result) {
				t.Log(result)
				t.Error("result record does not match the expected record")
			}

		})
	}

}
