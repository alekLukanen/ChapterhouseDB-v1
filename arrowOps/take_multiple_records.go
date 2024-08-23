package arrowops

import (
	"fmt"

	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

/*
* Take all rows from the input record based on the input indices record. The indices record should contain two UINT32 columns.
* The first column should contain the index of the record in the "records" slice and the second column should contain
* the index of the row in that record. The record returned contains data copied from the original record.
*/
func TakeMultipleRecords(mem *memory.GoAllocator, records []arrow.Record, indices arrow.Record) (arrow.Record, error) {
	for _, record := range records {
		record.Retain()
	}
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()

	if len(records) == 0 {
		return nil, errs.NewStackError(fmt.Errorf("%w| empty records slice", ErrNoDataSupplied))
	}

	// validate the indices record
	if indices.NumCols() != 2 {
		return nil, errs.NewStackError(fmt.Errorf(
			"%w| indices must have 2 columns, got %d",
			ErrColumnNotFound,
			indices.NumCols(),
		))
	}
	for idx := int64(0); idx < indices.NumCols(); idx++ {
		column := indices.Column(int(idx))
		if column.DataType().ID() != arrow.UINT32 {
			return nil, errs.NewStackError(fmt.Errorf(
				"%w| expected UINT32 column, got %s for column index %d",
				ErrUnsupportedDataType,
				column.DataType().Name(),
				idx,
			))
		}
	}
	recordSliceIndices := indices.Column(0).(*array.Uint32)
	recordIndices := indices.Column(1).(*array.Uint32)
	for idx := range indices.NumRows() {
		if int(recordSliceIndices.Value(int(idx))) >= len(records) {
			return nil, errs.NewStackError(fmt.Errorf("%w| record slice index out of bounds", ErrIndexOutOfBounds))
		}
	}
	if recordSliceIndices.NullN() > 0 || recordIndices.NullN() > 0 {
		return nil, errs.NewStackError(fmt.Errorf("%w| null values are not allowed in the indices record", ErrNullValuesNotAllowed))
	}

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

	resultRecord, err := ConcatenateRecords(mem, takenRecords...)
	if err != nil {
		return nil, errs.NewStackError(fmt.Errorf("%w| failed to concatenate taken records", err))
	}
	return resultRecord, nil
}
