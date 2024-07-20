package arrowops

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func TestConcatenateRecords(t *testing.T) {

	mem := &memory.GoAllocator{}

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
