package arrowops

import (
	"fmt"

	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/float16"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

/*
* Take all rows from the input record based on the input indices array. 
* The resulting record contains data copied from the original record.
*/
func TakeRecord(mem *memory.GoAllocator, record arrow.Record, indices *array.Uint32) (arrow.Record, error) {
	record.Retain()
	defer record.Release()

	if indices.NullN() > 0 {
		return nil, errs.NewStackError(fmt.Errorf("%w| null values are not allowed in the indices array", ErrNullValuesNotAllowed))
	}

	fields := make([]arrow.Array, record.NumCols())
	for i := int64(0); i < record.NumCols(); i++ {
		fields[i] = record.Column(int(i))
	}
	takenFields := make([]arrow.Array, record.NumCols())
	for i := 0; i < int(record.NumCols()); i++ {
		takenRows, err := TakeArray(mem, fields[i], indices)
		if err != nil {
			return nil, err
		}
		takenFields[i] = takenRows
	}
	return array.NewRecord(record.Schema(), takenFields, int64(indices.Len())), nil
}

func TakeArray(mem *memory.GoAllocator, arr arrow.Array, indices *array.Uint32) (arrow.Array, error) {
	switch arr.DataType().ID() {
	case arrow.BOOL:
		return takeBoolArray(mem, arr.(*array.Boolean), indices)
	case arrow.INT8:
		return takeNativeArray[int8, *array.Int8](array.NewInt8Builder(mem), arr.(*array.Int8), indices)
	case arrow.INT16:
		return takeNativeArray[int16, *array.Int16](array.NewInt16Builder(mem), arr.(*array.Int16), indices)
	case arrow.INT32:
		return takeNativeArray[int32, *array.Int32](array.NewInt32Builder(mem), arr.(*array.Int32), indices)
	case arrow.INT64:
		return takeNativeArray[int64, *array.Int64](array.NewInt64Builder(mem), arr.(*array.Int64), indices)
	case arrow.UINT8:
		return takeNativeArray[uint8, *array.Uint8](array.NewUint8Builder(mem), arr.(*array.Uint8), indices)
	case arrow.UINT16:
		return takeNativeArray[uint16, *array.Uint16](array.NewUint16Builder(mem), arr.(*array.Uint16), indices)
	case arrow.UINT32:
		return takeNativeArray[uint32, *array.Uint32](array.NewUint32Builder(mem), arr.(*array.Uint32), indices)
	case arrow.UINT64:
		return takeNativeArray[uint64, *array.Uint64](array.NewUint64Builder(mem), arr.(*array.Uint64), indices)
	case arrow.FLOAT16:
		return takeNativeArray[float16.Num, *array.Float16](array.NewFloat16Builder(mem), arr.(*array.Float16), indices)
	case arrow.FLOAT32:
		return takeNativeArray[float32, *array.Float32](array.NewFloat32Builder(mem), arr.(*array.Float32), indices)
	case arrow.FLOAT64:
		return takeNativeArray[float64, *array.Float64](array.NewFloat64Builder(mem), arr.(*array.Float64), indices)
	case arrow.STRING:
		return takeNativeArray[string, *array.String](array.NewStringBuilder(mem), arr.(*array.String), indices)
	case arrow.BINARY:
		return takeBinaryArray(mem, arr.(*array.Binary), indices)
	case arrow.DATE32:
		return takeNativeArray[arrow.Date32, *array.Date32](array.NewDate32Builder(mem), arr.(*array.Date32), indices)
	case arrow.DATE64:
		return takeNativeArray[arrow.Date64, *array.Date64](array.NewDate64Builder(mem), arr.(*array.Date64), indices)
	case arrow.TIMESTAMP:
		return takeNativeArray[arrow.Timestamp, *array.Timestamp](
			array.NewTimestampBuilder(mem, arr.DataType().(*arrow.TimestampType)), arr.(*array.Timestamp), indices,
		)
	case arrow.TIME32:
		return takeNativeArray[arrow.Time32, *array.Time32](
			array.NewTime32Builder(mem, arr.DataType().(*arrow.Time32Type)), arr.(*array.Time32), indices,
		)
	case arrow.TIME64:
		return takeNativeArray[arrow.Time64, *array.Time64](
			array.NewTime64Builder(mem, arr.DataType().(*arrow.Time64Type)), arr.(*array.Time64), indices,
		)
	case arrow.DURATION:
		return takeNativeArray[arrow.Duration, *array.Duration](
			array.NewDurationBuilder(mem, arr.DataType().(*arrow.DurationType)), arr.(*array.Duration), indices,
		)
	default:
		return nil, errs.NewStackError(ErrUnsupportedDataType)
	}
}

func takeBoolArray(mem *memory.GoAllocator, arr *array.Boolean, indices *array.Uint32) (*array.Boolean, error) {
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	arrLen := arr.Len()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		idx := int(indices.Value(i))
		if idx >= arrLen || idx < 0 {
			return nil, ErrIndexOutOfBounds
		}
		b.Append(arr.Value(idx))
	}
	return b.NewBooleanArray(), nil
}

func takeNativeArray[T comparable, E valueArray[T]](b arrayBuilder[T], arr E, indices *array.Uint32) (E, error) {
	defer b.Release()
	arrLen := arr.Len()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		idx := int(indices.Value(i))
		if idx >= arrLen || idx < 0 {
			return *new(E), fmt.Errorf("%w| record index out of bounds", ErrIndexOutOfBounds)
		}
		b.Append(arr.Value(idx))
	}
	return b.NewArray().(E), nil
}

func takeBinaryArray(mem *memory.GoAllocator, arr *array.Binary, indices *array.Uint32) (*array.Binary, error) {
	b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer b.Release()
	arrLen := arr.Len()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		idx := int(indices.Value(i))
		if idx >= arrLen || idx < 0 {
			return nil, ErrIndexOutOfBounds
		}
		b.Append(arr.Value(idx))
	}
	return b.NewBinaryArray(), nil
}
