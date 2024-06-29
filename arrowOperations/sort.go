package arrowops

import (
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
)

func SortRecord(record arrow.Record, columns []string) (sortOrder []int, err error) {

	data := make([][3]interface{}, record.NumRows())
	for i := 0; i < record.NumRows(); i++ {
		data[i] = [3]interface{}{i, nil, nil}
	}

	return sortOrder, err
}

func compareRows(record arrow.Record, columnIndex int, row1Index int, row2Index int) bool {
	switch record.Column(columnIndex).DataType().ID() {
	case arrow.BOOL:
		row1Value := record.Column(columnIndex).(*array.Boolean).Value(row1Index)
		row2Value := record.Column(columnIndex).(*array.Boolean).Value(row2Index)
		if !row1Value && row2Value {
			return true
		}
	case arrow.INT8:
		row1Value := record.Column(columnIndex).(*array.Int8).Value(row1Index)
		row2Value := record.Column(columnIndex).(*array.Int8).Value(row2Index)
		if row1Value < row2Value {
			return true
		}
	case arrow.INT16:
		row1Value := record.Column(columnIndex).(*array.Int16).Value(row1Index)
		row2Value := record.Column(columnIndex).(*array.Int16).Value(row2Index)
		if row1Value < row2Value {
			return true
		}
	case arrow.INT32:
		row1Value := record.Column(columnIndex).(*array.Int32).Value(row1Index)
		row2Value := record.Column(columnIndex).(*array.Int32).Value(row2Index)
		if row1Value < row2Value {
			return true
		}
	case arrow.INT64:
		row1Value := record.Column(columnIndex).(*array.Int64).Value(row1Index)
		row2Value := record.Column(columnIndex).(*array.Int64).Value(row2Index)
		if row1Value < row2Value {
			return true
		}
	default:
		return false
	}
	return false
}
