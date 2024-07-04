package arrowops

import (
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func TestTakeRecord(t *testing.T) {
	mem := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		},
		nil,
	))
	defer recordBuilder.Release()
	recordBuilder.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3}, nil)
	recordBuilder.Field(1).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0}, nil)
	recordBuilder.Field(2).(*array.StringBuilder).AppendValues([]string{"s1", "s2", "s3"}, nil)

	record := recordBuilder.NewRecord()
	defer record.Release()

	indicesBuilder := array.NewUint32Builder(mem)
	defer indicesBuilder.Release()
	indicesBuilder.AppendValues([]uint32{2, 0}, nil)

	indices := indicesBuilder.NewUint32Array()
	defer indices.Release()

	takenRecord, err := TakeRecord(mem, record, indices)
	if err != nil {
		t.Errorf("TakeRecord() error = %v, wantErr %v", err, nil)
		return
	}
	defer takenRecord.Release()

	t.Log(takenRecord)

}
