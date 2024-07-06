package arrowops

import (
	"bytes"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/float16"
)

func RecordsEqual(a, b arrow.Record) (bool, error) {
	if !a.Schema().Equal(b.Schema()) {
		return false, nil
	}

	if a.NumRows() != b.NumRows() {
		return false, nil
	}

	for i := 0; i < int(a.NumCols()); i++ {
		if val, err := ArraysEqual(a.Column(int(i)), b.Column(int(i))); err != nil {
			return false, err
		} else if !val {
			return false, nil
		}
	}

	return true, nil
}

func ArraysEqual(a, b arrow.Array) (bool, error) {
	if a.DataType().ID() != b.DataType().ID() {
		return false, nil
	}

	if a.Len() != b.Len() {
		return false, nil
	}

	if a.NullN() != b.NullN() {
		return false, nil
	} else {
		aNulls := a.NullBitmapBytes()
		bNulls := b.NullBitmapBytes()
		if !bytes.Equal(aNulls, bNulls) {
			return false, nil
		}
	}

	switch a.DataType().ID() {
	case arrow.BOOL:
		return nativeArrayEqual[bool, *array.Boolean](a.(*array.Boolean), b.(*array.Boolean)), nil
	case arrow.INT8:
		return nativeArrayEqual[int8, *array.Int8](a.(*array.Int8), b.(*array.Int8)), nil
	case arrow.INT16:
		return nativeArrayEqual[int16, *array.Int16](a.(*array.Int16), b.(*array.Int16)), nil
	case arrow.INT32:
		return nativeArrayEqual[int32, *array.Int32](a.(*array.Int32), b.(*array.Int32)), nil
	case arrow.INT64:
		return nativeArrayEqual[int64, *array.Int64](a.(*array.Int64), b.(*array.Int64)), nil
	case arrow.UINT8:
		return nativeArrayEqual[uint8, *array.Uint8](a.(*array.Uint8), b.(*array.Uint8)), nil
	case arrow.UINT16:
		return nativeArrayEqual[uint16, *array.Uint16](a.(*array.Uint16), b.(*array.Uint16)), nil
	case arrow.UINT32:
		return nativeArrayEqual[uint32, *array.Uint32](a.(*array.Uint32), b.(*array.Uint32)), nil
	case arrow.UINT64:
		return nativeArrayEqual[uint64, *array.Uint64](a.(*array.Uint64), b.(*array.Uint64)), nil
	case arrow.FLOAT16:
		return nativeArrayEqual[float16.Num, *array.Float16](a.(*array.Float16), b.(*array.Float16)), nil
	case arrow.FLOAT32:
		return nativeArrayEqual[float32, *array.Float32](a.(*array.Float32), b.(*array.Float32)), nil
	case arrow.FLOAT64:
		return nativeArrayEqual[float64, *array.Float64](a.(*array.Float64), b.(*array.Float64)), nil
	case arrow.STRING:
		return nativeArrayEqual[string, *array.String](a.(*array.String), b.(*array.String)), nil
	case arrow.BINARY:
		return binaryArrayEqual(a.(*array.Binary), b.(*array.Binary)), nil
	case arrow.DATE32:
		return nativeArrayEqual[arrow.Date32, *array.Date32](a.(*array.Date32), b.(*array.Date32)), nil
	case arrow.DATE64:
		return nativeArrayEqual[arrow.Date64, *array.Date64](a.(*array.Date64), b.(*array.Date64)), nil
	case arrow.TIMESTAMP:
		return nativeArrayEqual[arrow.Timestamp, *array.Timestamp](a.(*array.Timestamp), b.(*array.Timestamp)), nil
	case arrow.TIME32:
		return nativeArrayEqual[arrow.Time32, *array.Time32](a.(*array.Time32), b.(*array.Time32)), nil
	case arrow.TIME64:
		return nativeArrayEqual[arrow.Time64, *array.Time64](a.(*array.Time64), b.(*array.Time64)), nil
	case arrow.DURATION:
		return nativeArrayEqual[arrow.Duration, *array.Duration](a.(*array.Duration), b.(*array.Duration)), nil
	default:
		return false, ErrUnsupportedDataType
	}
}

type valueArray[T comparable] interface {
	IsNull(i int) bool
	Value(i int) T
	Len() int
}

func nativeArrayEqual[T comparable, E valueArray[T]](a, b E) bool {
	for i := 0; i < a.Len(); i++ {
		if a.IsNull(i) != b.IsNull(i) {
			return false
		}
		if a.Value(i) != b.Value(i) {
			return false
		}
	}
	return true
}

func binaryArrayEqual(a, b *array.Binary) bool {
	for i := 0; i < a.Len(); i++ {
		if a.IsNull(i) != b.IsNull(i) {
			return false
		}
		if !bytes.Equal(a.Value(i), b.Value(i)) {
			return false
		}
	}
	return true
}
