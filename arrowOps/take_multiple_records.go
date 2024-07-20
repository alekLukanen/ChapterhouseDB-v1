package arrowops

import (
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func TakeMultipleRecords(mem *memory.GoAllocator, records []arrow.Record, indices arrow.Record) (arrow.Record, error) {
	for _, record := range records {
		record.Retain()
	}
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()

	// validate the indices record
	if indices.NumCols() != 2 {
		return nil, fmt.Errorf(
			"%w| indices must have 2 columns, got %d", 
			ErrColumnNotFound, 
			indices.NumCols(),
		)
	}
	for idx := int64(0); idx < indices.NumCols(); idx++ {
		column := indices.Column(int(idx))
		if column.DataType().ID() != arrow.UINT32 {
			return nil, fmt.Errorf(
				"%w| expected UINT32 column, got %s for column index %d", 
				ErrUnsupportedDataType, 
				column.DataType().Name(), 
				idx,
			)
		}
	}

	recordSliceIndices := indices.Column(0).(*array.Uint32)
	recordIndices := indices.Column(1).(*array.Uint32)

	takenRecords := make([]arrow.Record, len(records))
	for recordIdx, record := range records {
		recordTakeIndicesBuilder := array.NewUint32Builder(mem)
		for i := 0; i < recordSliceIndices.Len(); i++ {
			if recordSliceIndices.Value(i) == uint32(recordIdx) {
				recordTakeIndicesBuilder.Append(recordIndices.Value(i))
			}
		}
		recordTakeIndices := recordTakeIndicesBuilder.NewUint32Array()
		recordTakeIndicesBuilder.Release()

		takenRecord, err := TakeRecord(mem, record, recordTakeIndices)
		if err != nil {
			return nil, err
		}
		takenRecords[recordIdx] = takenRecord
	}

	resultRecord, err := ConcatenateRecords(mem, records...)
	if err != nil {
		return nil, fmt.Errorf("%w| failed to concatenate taken records", err)
	}
	return resultRecord, nil
}
