package arrowops

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func TestTakeMultipleRecords(t *testing.T) {

	mem := memory.NewGoAllocator()

	testCases := []struct {
		records        []arrow.Record
		takeIndices    arrow.Record
		expectedRecord arrow.Record
		expectedErr    error
	}{
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
				rb2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{3, 4, 5}, nil)
				rb2.Field(1).(*array.Float32Builder).AppendValues([]float32{3., 4., 5.}, nil)
				rb2.Field(2).(*array.StringBuilder).AppendValues([]string{"s3", "s4", "s5"}, nil)
				return []arrow.Record{rb1.NewRecord(), rb2.NewRecord()}
			}(),
			takeIndices: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "record", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "recordIdx", Type: arrow.PrimitiveTypes.Uint32},
					}, nil))
				defer rb1.Release()
				rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 1}, nil)
				rb1.Field(1).(*array.Uint32Builder).AppendValues([]uint32{0, 0, 2}, nil)
				return rb1.NewRecord()
			}(),
			expectedRecord: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "b", Type: arrow.PrimitiveTypes.Float32},
						{Name: "c", Type: arrow.BinaryTypes.String},
					}, nil))
				defer rb1.Release()
				rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 3, 5}, nil)
				rb1.Field(1).(*array.Float32Builder).AppendValues([]float32{0., 3., 5.}, nil)
				rb1.Field(2).(*array.StringBuilder).AppendValues([]string{"s0", "s3", "s5"}, nil)
				return rb1.NewRecord()
			}(),
			expectedErr: nil,
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
				rb2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{3, 4, 5}, nil)
				rb2.Field(1).(*array.Float32Builder).AppendValues([]float32{3., 4., 5.}, nil)
				rb2.Field(2).(*array.StringBuilder).AppendValues([]string{"s3", "s4", "s5"}, nil)
				return []arrow.Record{rb1.NewRecord(), rb2.NewRecord()}
			}(),
			takeIndices: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "record", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "recordIdx", Type: arrow.PrimitiveTypes.Uint32},
					}, nil))
				defer rb1.Release()
				rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 2}, nil)
				rb1.Field(1).(*array.Uint32Builder).AppendValues([]uint32{0, 0, 2}, nil)
				return rb1.NewRecord()
			}(),
			expectedRecord: nil,
			expectedErr:    ErrIndexOutOfBounds,
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
				rb2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{3, 4, 5}, nil)
				rb2.Field(1).(*array.Float32Builder).AppendValues([]float32{3., 4., 5.}, nil)
				rb2.Field(2).(*array.StringBuilder).AppendValues([]string{"s3", "s4", "s5"}, nil)
				return []arrow.Record{rb1.NewRecord(), rb2.NewRecord()}
			}(),
			takeIndices: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "record", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "recordIdx", Type: arrow.PrimitiveTypes.Uint32},
					}, nil))
				defer rb1.Release()
				rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 1, 1}, nil)
				rb1.Field(1).(*array.Uint32Builder).AppendValues([]uint32{0, 0, 8}, nil)
				return rb1.NewRecord()
			}(),
			expectedRecord: nil,
			expectedErr:    ErrIndexOutOfBounds,
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
				return []arrow.Record{rb1.NewRecord()}
			}(),
			takeIndices: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "record", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "recordIdx", Type: arrow.PrimitiveTypes.Uint32},
					}, nil))
				defer rb1.Release()
				return rb1.NewRecord()
			}(),
			expectedRecord: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "b", Type: arrow.PrimitiveTypes.Float32},
						{Name: "c", Type: arrow.BinaryTypes.String},
					}, nil))
				defer rb1.Release()
				return rb1.NewRecord()
			}(),
			expectedErr: nil,
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
				return []arrow.Record{rb1.NewRecord()}
			}(),
			takeIndices: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "record", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "recordIdx", Type: arrow.PrimitiveTypes.Uint32},
					}, nil))
				defer rb1.Release()
				rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{0, 0, 0}, []bool{true, false, true})
				rb1.Field(1).(*array.Uint32Builder).AppendValues([]uint32{0, 0, 1}, []bool{true, false, true})
				return rb1.NewRecord()
			}(),
			expectedRecord: func() arrow.Record {
				rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
						{Name: "b", Type: arrow.PrimitiveTypes.Float32},
						{Name: "c", Type: arrow.BinaryTypes.String},
					}, nil))
				defer rb1.Release()
				return rb1.NewRecord()
			}(),
			expectedErr: ErrNullValuesNotAllowed,
		},
	}

	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", idx), func(t *testing.T) {

			result, err := TakeMultipleRecords(mem, testCase.records, testCase.takeIndices)
			if !errors.Is(err, testCase.expectedErr) {
				t.Fatalf("expected error '%s' but received '%s'", testCase.expectedErr, err)
			}
			if (testCase.expectedRecord == nil && result != nil) ||
				(result != nil && !array.RecordEqual(testCase.expectedRecord, result)) {
				t.Log(result)
				t.Error("result record does not match the expected record")
			}

		})
	}

}
