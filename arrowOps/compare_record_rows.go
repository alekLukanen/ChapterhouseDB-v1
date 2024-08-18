package arrowops

import (
	"bytes"
	"cmp"
	"fmt"

	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

/*
* Determines if the row at index1 in record1 is less/equal/greater than
* the row at index2 in record2. If the column list is empty
* than all rows will be compared else only the columns in the
* list will be compared. It's assumed that each record only has
* one column with the same name or that each column with the same name
* has the same values.
 */
func CompareRecordRows(record1, record2 arrow.Record, index1, index2 int, fields ...string) (int, error) {

	if record1.NumRows() <= int64(index1) {
		return 0, errs.NewStackError(fmt.Errorf("%w| index1 value of %d out of bounds %d", ErrIndexOutOfBounds, index1, record1.NumRows()))
	}
	if record2.NumRows() <= int64(index2) {
		return 0, errs.NewStackError(fmt.Errorf("%w| index2 value of %d out of bounds %d", ErrIndexOutOfBounds, index2, record2.NumRows()))
	}
	if len(fields) == 0 {
		return compareRecordRowsUsingAllFields(record1, record2, index1, index2)
	} else {
		return compareRecordRowsUsingSubset(record1, record2, index1, index2, fields...)
	}

}

func compareRecordRowsUsingSubset(record1, record2 arrow.Record, index1, index2 int, fields ...string) (int, error) {
	if !RecordSchemasEqual(record1, record2, fields...) {
		return 0, errs.NewStackError(FErrSchemasNotEqual(record1, record2, fields...))
	}
	for _, field := range fields {
		column1Idxs := record1.Schema().FieldIndices(field)
		column2Idxs := record2.Schema().FieldIndices(field)
		for _, column1Idx := range column1Idxs {
			for _, column2Idx := range column2Idxs {
				column1 := record1.Column(column1Idx)
				column2 := record2.Column(column2Idx)
				compareValue, err := compareArrayValues(column1, column2, index1, index2)
				if err != nil {
					return 0, err
				}
				if compareValue != 0 {
					return compareValue, nil
				}
			}
		}
	}
	return 0, nil
}

/*
* Determines if the row at index1 in record1 is less/equal/greater than
* the row at index2 in record2. All columns are used in the comparison
* and the order of comparison is based on the column order in the first record.
 */
func compareRecordRowsUsingAllFields(record1, record2 arrow.Record, index1, index2 int) (int, error) {
	if !RecordSchemasEqual(record1, record2) {
		return 0, errs.NewStackError(fmt.Errorf("%w| records have different number of columns", ErrSchemasNotEqual))
	}
	for i := 0; i < int(record1.NumCols()); i++ {
		column1 := record1.Column(i)
		record2ColumnIdxs := record2.Schema().FieldIndices(record1.ColumnName(i))
		for _, record2ColumnIdx := range record2ColumnIdxs {
			column2 := record2.Column(record2ColumnIdx)
			compareValue, err := compareArrayValues(column1, column2, index1, index2)
			if err != nil {
				return 0, err
			}
			if compareValue != 0 {
				return compareValue, nil
			}
		}
	}
	return 0, nil
}

func compareArrayValues(a1, a2 arrow.Array, i1, i2 int) (int, error) {
	if a1.DataType().ID() != a2.DataType().ID() {
		return 0, nil
	}

	if a1.IsNull(i1) && a2.IsNull(i2) {
		return 0, nil
	} else if a1.IsNull(i1) {
		return -1, nil
	} else if a2.IsNull(i2) {
		return 1, nil
	}

	switch a1.DataType().ID() {
	case arrow.BOOL:
		return booleanArrayValuesEqual(a1.(*array.Boolean), a2.(*array.Boolean), i1, i2), nil
	case arrow.INT8:
		return nativeArrayValuesEqual[int8, *array.Int8](a1.(*array.Int8), a2.(*array.Int8), i1, i2), nil
	case arrow.INT16:
		return nativeArrayValuesEqual[int16, *array.Int16](a1.(*array.Int16), a2.(*array.Int16), i1, i2), nil
	case arrow.INT32:
		return nativeArrayValuesEqual[int32, *array.Int32](a1.(*array.Int32), a2.(*array.Int32), i1, i2), nil
	case arrow.INT64:
		return nativeArrayValuesEqual[int64, *array.Int64](a1.(*array.Int64), a2.(*array.Int64), i1, i2), nil
	case arrow.UINT8:
		return nativeArrayValuesEqual[uint8, *array.Uint8](a1.(*array.Uint8), a2.(*array.Uint8), i1, i2), nil
	case arrow.UINT16:
		return nativeArrayValuesEqual[uint16, *array.Uint16](a1.(*array.Uint16), a2.(*array.Uint16), i1, i2), nil
	case arrow.UINT32:
		return nativeArrayValuesEqual[uint32, *array.Uint32](a1.(*array.Uint32), a2.(*array.Uint32), i1, i2), nil
	case arrow.UINT64:
		return nativeArrayValuesEqual[uint64, *array.Uint64](a1.(*array.Uint64), a2.(*array.Uint64), i1, i2), nil
	case arrow.FLOAT16:
		return float16ArrayValuesEqual(a1.(*array.Float16), a2.(*array.Float16), i1, i2), nil
	case arrow.FLOAT32:
		return nativeArrayValuesEqual[float32, *array.Float32](a1.(*array.Float32), a2.(*array.Float32), i1, i2), nil
	case arrow.FLOAT64:
		return nativeArrayValuesEqual[float64, *array.Float64](a1.(*array.Float64), a2.(*array.Float64), i1, i2), nil
	case arrow.STRING:
		return nativeArrayValuesEqual[string, *array.String](a1.(*array.String), a2.(*array.String), i1, i2), nil
	case arrow.BINARY:
		return binaryArrayEqual(a1.(*array.Binary), a2.(*array.Binary), i1, i2), nil
	case arrow.DATE32:
		return nativeArrayValuesEqual[arrow.Date32, *array.Date32](a1.(*array.Date32), a2.(*array.Date32), i1, i2), nil
	case arrow.DATE64:
		return nativeArrayValuesEqual[arrow.Date64, *array.Date64](a1.(*array.Date64), a2.(*array.Date64), i1, i2), nil
	case arrow.TIMESTAMP:
		return nativeArrayValuesEqual[arrow.Timestamp, *array.Timestamp](a1.(*array.Timestamp), a2.(*array.Timestamp), i1, i2), nil
	case arrow.TIME32:
		return nativeArrayValuesEqual[arrow.Time32, *array.Time32](a1.(*array.Time32), a2.(*array.Time32), i1, i2), nil
	case arrow.TIME64:
		return nativeArrayValuesEqual[arrow.Time64, *array.Time64](a1.(*array.Time64), a2.(*array.Time64), i1, i2), nil
	case arrow.DURATION:
		return nativeArrayValuesEqual[arrow.Duration, *array.Duration](a1.(*array.Duration), a2.(*array.Duration), i1, i2), nil
	default:
		return 0, errs.NewStackError(ErrUnsupportedDataType)
	}
}

func nativeArrayValuesEqual[T cmp.Ordered, E valueArray[T]](a1, a2 E, i1, i2 int) int {
	return cmp.Compare(a1.Value(i1), a2.Value(i2))
}

func float16ArrayValuesEqual(a1, a2 *array.Float16, i1, i2 int) int {
	return a1.Value(i1).Cmp(a2.Value(i2))
}

func booleanArrayValuesEqual(a1, a2 *array.Boolean, i1, i2 int) int {
	if a1.Value(i1) == a2.Value(i2) {
		return 0
	} else if a1.Value(i1) {
		return 1
	} else {
		return -1
	}
}

func binaryArrayEqual(a1, a2 *array.Binary, i1, i2 int) int {
	return bytes.Compare(a1.Value(i1), a2.Value(i2))
}
