package arrowops

import (
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/float16"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

/*
* Returns a concatenated array from the provided homogeneous arrays.
* If only one array is provided the resulting array will still be a
* net new array.
 */
func ConcatenateArrays(mem *memory.GoAllocator, arrays ...arrow.Array) (arrow.Array, error) {
	// reference couting
	for _, arr := range arrays {
		arr.Retain()
	}
	defer func() {
		for _, arr := range arrays {
			arr.Release()
		}
	}()

	// validate the arrays
	if len(arrays) == 0 {
		return nil, fmt.Errorf("%w: expected at least one array but received 0", ErrNoDataSupplied)
	}
	arrayDataType := arrays[0].DataType()
	if len(arrays) > 1 {
		for _, arr := range arrays[1:] {
			if arrayDataType.ID() != arr.DataType().ID() {
				return nil, ErrDataTypesNotEqual
			}
		}
	}

	// concatenate the arrays
	switch arrayDataType.ID() {
	case arrow.BOOL:
		boolArrays, err := CastArraysToBaseDataType[*array.Boolean](arrays...)
		if err != nil {
			return nil, err
		}
		return concatBoolArrays(mem, boolArrays), nil
	case arrow.INT8:
		castArrays, err := CastArraysToBaseDataType[*array.Int8](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[int8, *array.Int8](mem, array.NewInt8Builder(mem), castArrays), nil
	case arrow.INT16:
		castArrays, err := CastArraysToBaseDataType[*array.Int16](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[int16, *array.Int16](mem, array.NewInt16Builder(mem), castArrays), nil
	case arrow.INT32:
		castArrays, err := CastArraysToBaseDataType[*array.Int32](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[int32, *array.Int32](mem, array.NewInt32Builder(mem), castArrays), nil
	case arrow.INT64:
		castArrays, err := CastArraysToBaseDataType[*array.Int64](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[int64, *array.Int64](mem, array.NewInt64Builder(mem), castArrays), nil
	case arrow.UINT8:
		castArrays, err := CastArraysToBaseDataType[*array.Uint8](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[uint8, *array.Uint8](mem, array.NewUint8Builder(mem), castArrays), nil
	case arrow.UINT16:
		castArrays, err := CastArraysToBaseDataType[*array.Uint16](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[uint16, *array.Uint16](mem, array.NewUint16Builder(mem), castArrays), nil
	case arrow.UINT32:
		castArrays, err := CastArraysToBaseDataType[*array.Uint32](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[uint32, *array.Uint32](mem, array.NewUint32Builder(mem), castArrays), nil
	case arrow.UINT64:
		castArrays, err := CastArraysToBaseDataType[*array.Uint64](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[uint64, *array.Uint64](mem, array.NewUint64Builder(mem), castArrays), nil
	case arrow.FLOAT16:
		castArrays, err := CastArraysToBaseDataType[*array.Float16](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[float16.Num, *array.Float16](mem, array.NewFloat16Builder(mem), castArrays), nil
	case arrow.FLOAT32:
		castArrays, err := CastArraysToBaseDataType[*array.Float32](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[float32, *array.Float32](mem, array.NewFloat32Builder(mem), castArrays), nil
	case arrow.FLOAT64:
		castArrays, err := CastArraysToBaseDataType[*array.Float64](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[float64, *array.Float64](mem, array.NewFloat64Builder(mem), castArrays), nil
	case arrow.STRING:
		castArrays, err := CastArraysToBaseDataType[*array.String](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[string, *array.String](mem, array.NewStringBuilder(mem), castArrays), nil
	case arrow.BINARY:
		castArrays, err := CastArraysToBaseDataType[*array.Binary](arrays...)
		if err != nil {
			return nil, err
		}
		return conacatBinaryArrays(mem, castArrays), nil
	case arrow.DATE32:
		castArrays, err := CastArraysToBaseDataType[*array.Date32](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[arrow.Date32, *array.Date32](mem, array.NewDate32Builder(mem), castArrays), nil
	case arrow.DATE64:
		castArrays, err := CastArraysToBaseDataType[*array.Date64](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[arrow.Date64, *array.Date64](mem, array.NewDate64Builder(mem), castArrays), nil
	case arrow.TIMESTAMP:
		castArrays, err := CastArraysToBaseDataType[*array.Timestamp](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[arrow.Timestamp, *array.Timestamp](mem, array.NewTimestampBuilder(mem, arrayDataType.(*arrow.TimestampType)), castArrays), nil
	case arrow.TIME32:
		castArrays, err := CastArraysToBaseDataType[*array.Time32](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[arrow.Time32, *array.Time32](mem, array.NewTime32Builder(mem, arrayDataType.(*arrow.Time32Type)), castArrays), nil
	case arrow.TIME64:
		castArrays, err := CastArraysToBaseDataType[*array.Time64](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[arrow.Time64, *array.Time64](mem, array.NewTime64Builder(mem, arrayDataType.(*arrow.Time64Type)), castArrays), nil
	case arrow.DURATION:
		castArrays, err := CastArraysToBaseDataType[*array.Duration](arrays...)
		if err != nil {
			return nil, err
		}
		return concatNativeArray[arrow.Duration, *array.Duration](mem, array.NewDurationBuilder(mem, arrayDataType.(*arrow.DurationType)), castArrays), nil
	default:
		return nil, fmt.Errorf("%w: unsupported data type %s", ErrUnsupportedDataType, arrayDataType.Name())
	}

}

func conacatBinaryArrays(mem *memory.GoAllocator, arrays []*array.Binary) *array.Binary {
	// get the total number of rows in the concatenated array
	var numRows uint32
	for _, arr := range arrays {
		numRows += uint32(arr.Len())
	}
	// create the concatenated array
	concatenatedArray := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer concatenatedArray.Release()
	concatenatedArray.Reserve(int(numRows))
	// concatenate the arrays
	for _, arr := range arrays {
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsValid(idx) {
				concatenatedArray.Append(arr.Value(idx))
			} else {
				concatenatedArray.AppendNull()
			}
		}
	}
	return concatenatedArray.NewBinaryArray()
}

func concatBoolArrays(mem *memory.GoAllocator, arrays []*array.Boolean) *array.Boolean {
	// get the total number of rows in the concatenated array
	var numRows uint32
	for _, arr := range arrays {
		numRows += uint32(arr.Len())
	}
	// create the concatenated array
	concatenatedArray := array.NewBooleanBuilder(mem)
	defer concatenatedArray.Release()
	concatenatedArray.Reserve(int(numRows))
	// concatenate the arrays
	for _, arr := range arrays {
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsValid(idx) {
				concatenatedArray.Append(arr.Value(idx))
			} else {
				concatenatedArray.AppendNull()
			}
		}
	}
	return concatenatedArray.NewBooleanArray()
}

func concatNativeArray[T comparable, E valueArray[T]](mem *memory.GoAllocator, builder arrayBuilder[T], arrays []E) E {
	// get the total number of rows in the concatenated array
	var numRows uint32
	for _, arr := range arrays {
		numRows += uint32(arr.Len())
	}
	// create the concatenated array
	builder.Reserve(int(numRows))
	// concatenate the arrays
	for _, arr := range arrays {
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsNull(idx) {
				builder.Append(arr.Value(idx))
			} else {
				builder.AppendNull()
			}
		}
	}
	return builder.NewArray().(E)
}
