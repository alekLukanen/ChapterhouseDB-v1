package arrowops

import (
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func TakeRecord(mem *memory.GoAllocator, record arrow.Record, indices *array.Uint32) (arrow.Record, error) {
	record.Retain()
	defer record.Release()

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
		return takeBoolArray(mem, arr.(*array.Boolean), indices), nil
	case arrow.INT8:
		return takeInt8Array(mem, arr.(*array.Int8), indices), nil
	case arrow.INT16:
		return takeInt16Array(mem, arr.(*array.Int16), indices), nil
	case arrow.INT32:
		return takeInt32Array(mem, arr.(*array.Int32), indices), nil
	case arrow.INT64:
		return takeInt64Array(mem, arr.(*array.Int64), indices), nil
	case arrow.UINT8:
		return takeUint8Array(mem, arr.(*array.Uint8), indices), nil
	case arrow.UINT16:
		return takeUint16Array(mem, arr.(*array.Uint16), indices), nil
	case arrow.UINT32:
		return takeUint32Array(mem, arr.(*array.Uint32), indices), nil
	case arrow.UINT64:
		return takeUint64Array(mem, arr.(*array.Uint64), indices), nil
	case arrow.FLOAT16:
		return takeFloat16Array(mem, arr.(*array.Float16), indices), nil
	case arrow.FLOAT32:
		return takeFloat32Array(mem, arr.(*array.Float32), indices), nil
	case arrow.FLOAT64:
		return takeFloat64Array(mem, arr.(*array.Float64), indices), nil
	case arrow.STRING:
		return takeStringArray(mem, arr.(*array.String), indices), nil
	case arrow.BINARY:
		return takeBinaryArray(mem, arr.(*array.Binary), indices), nil
	case arrow.DATE32:
		return takeDate32Array(mem, arr.(*array.Date32), indices), nil
	case arrow.DATE64:
		return takeDate64Array(mem, arr.(*array.Date64), indices), nil
	case arrow.TIMESTAMP:
		return takeTimestampArray(mem, arr.(*array.Timestamp), indices), nil
	case arrow.TIME32:
		return takeTime32Array(mem, arr.(*array.Time32), indices), nil
	case arrow.TIME64:
		return takeTime64Array(mem, arr.(*array.Time64), indices), nil
	case arrow.DURATION:
		return takeDurationArray(mem, arr.(*array.Duration), indices), nil
	default:
		return nil, ErrUnsupportedDataType
	}
}

func takeBoolArray(mem *memory.GoAllocator, arr *array.Boolean, indices *array.Uint32) *array.Boolean {
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewBooleanArray()
}

func takeInt8Array(mem *memory.GoAllocator, arr *array.Int8, indices *array.Uint32) *array.Int8 {
	b := array.NewInt8Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewInt8Array()
}

func takeInt16Array(mem *memory.GoAllocator, arr *array.Int16, indices *array.Uint32) *array.Int16 {
	b := array.NewInt16Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewInt16Array()
}

func takeInt32Array(mem *memory.GoAllocator, arr *array.Int32, indices *array.Uint32) *array.Int32 {
	b := array.NewInt32Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewInt32Array()
}

func takeInt64Array(mem *memory.GoAllocator, arr *array.Int64, indices *array.Uint32) *array.Int64 {
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewInt64Array()
}

func takeUint8Array(mem *memory.GoAllocator, arr *array.Uint8, indices *array.Uint32) *array.Uint8 {
	b := array.NewUint8Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewUint8Array()
}

func takeUint16Array(mem *memory.GoAllocator, arr *array.Uint16, indices *array.Uint32) *array.Uint16 {
	b := array.NewUint16Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewUint16Array()
}

func takeUint32Array(mem *memory.GoAllocator, arr *array.Uint32, indices *array.Uint32) *array.Uint32 {
	b := array.NewUint32Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewUint32Array()
}

func takeUint64Array(mem *memory.GoAllocator, arr *array.Uint64, indices *array.Uint32) *array.Uint64 {
	b := array.NewUint64Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewUint64Array()
}

func takeFloat16Array(mem *memory.GoAllocator, arr *array.Float16, indices *array.Uint32) *array.Float16 {
	b := array.NewFloat16Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewFloat16Array()
}

func takeFloat32Array(mem *memory.GoAllocator, arr *array.Float32, indices *array.Uint32) *array.Float32 {
	b := array.NewFloat32Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewFloat32Array()
}

func takeFloat64Array(mem *memory.GoAllocator, arr *array.Float64, indices *array.Uint32) *array.Float64 {
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewFloat64Array()
}

func takeStringArray(mem *memory.GoAllocator, arr *array.String, indices *array.Uint32) *array.String {
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewStringArray()
}

func takeBinaryArray(mem *memory.GoAllocator, arr *array.Binary, indices *array.Uint32) *array.Binary {
	b := array.NewBinaryBuilder(mem, &arrow.BinaryType{})
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewBinaryArray()
}

func takeDate32Array(mem *memory.GoAllocator, arr *array.Date32, indices *array.Uint32) *array.Date32 {
	b := array.NewDate32Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewDate32Array()
}

func takeDate64Array(mem *memory.GoAllocator, arr *array.Date64, indices *array.Uint32) *array.Date64 {
	b := array.NewDate64Builder(mem)
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewDate64Array()
}

func takeTimestampArray(mem *memory.GoAllocator, arr *array.Timestamp, indices *array.Uint32) *array.Timestamp {
	b := array.NewTimestampBuilder(mem, &arrow.TimestampType{})
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewTimestampArray()
}

func takeTime32Array(mem *memory.GoAllocator, arr *array.Time32, indices *array.Uint32) *array.Time32 {
	b := array.NewTime32Builder(mem, &arrow.Time32Type{})
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewTime32Array()
}

func takeTime64Array(mem *memory.GoAllocator, arr *array.Time64, indices *array.Uint32) *array.Time64 {
	b := array.NewTime64Builder(mem, &arrow.Time64Type{})
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewTime64Array()
}

func takeDurationArray(mem *memory.GoAllocator, arr *array.Duration, indices *array.Uint32) *array.Duration {
	b := array.NewDurationBuilder(mem, &arrow.DurationType{})
	defer b.Release()
	b.Reserve(indices.Len())
	for i := 0; i < indices.Len(); i++ {
		b.Append(arr.Value(int(indices.Value(i))))
	}
	return b.NewDurationArray()
}
