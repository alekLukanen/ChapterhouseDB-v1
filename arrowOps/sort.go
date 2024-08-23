package arrowops

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/float16"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type sortItem[E comparable] struct {
	Rank  uint32
	Index uint32
	Value E
}

func SortRecord(mem *memory.GoAllocator, record arrow.Record, columns []string) (arrow.Record, error) {

	var scratchRecord arrow.Record
	freeMemory := func() {
		if scratchRecord != nil {
			scratchRecord.Release()
		}
	}

	for idx, column := range columns {
		columnIndexes := record.Schema().FieldIndices(column)
		if len(columnIndexes) == 0 {
			freeMemory()
			return nil, errs.NewStackError(ErrColumnNotFound)
		}
		columnIndex := columnIndexes[0]

		var currentRecord arrow.Record
		if idx == 0 {
			currentRecord = record
		} else {
			currentRecord = scratchRecord
		}

		var previousArray arrow.Array
		if idx > 0 {
			previousArray = currentRecord.Column(currentRecord.Schema().FieldIndices(columns[idx-1])[0])
		}

		sortedIndices, err := RankedSort(mem, previousArray, currentRecord.Column(columnIndex))
		if err != nil {
			freeMemory()
			return nil, errs.Wrap(err, fmt.Errorf("failed to sort column %s", column))
		}
		defer sortedIndices.Release()

		proposedRecord, err := TakeRecord(mem, currentRecord, sortedIndices)
		if err != nil {
			freeMemory()
			return nil, errs.Wrap(err, fmt.Errorf("failed to take record for column %s", column))
		}
		scratchRecord = proposedRecord

	}

	return scratchRecord, nil
}

func RankedSort(mem *memory.GoAllocator, previousArray, currentArray arrow.Array) (*array.Uint32, error) {
	var ranks *array.Uint32
	if previousArray != nil {
		r, err := RankArray(mem, previousArray)
		if err != nil {
			return nil, err
		}
		ranks = r
	} else {
		ranks = ZeroUint32Array(mem, currentArray.Len())
	}
	defer ranks.Release()

	indicesBuilder := array.NewUint32Builder(mem)
	defer indicesBuilder.Release()
	indicesBuilder.Resize(currentArray.Len())

	// handle native types differently than arrow types
	switch currentArray.DataType().ID() {
	case arrow.INT8:
		sortItems[uint8, *array.Uint8](indicesBuilder, ranks, currentArray.(*array.Uint8))
	case arrow.INT16:
		sortItems[int16, *array.Int16](indicesBuilder, ranks, currentArray.(*array.Int16))
	case arrow.INT32:
		sortItems[int32, *array.Int32](indicesBuilder, ranks, currentArray.(*array.Int32))
	case arrow.INT64:
		sortItems[int64, *array.Int64](indicesBuilder, ranks, currentArray.(*array.Int64))
	case arrow.UINT8:
		sortItems[uint8, *array.Uint8](indicesBuilder, ranks, currentArray.(*array.Uint8))
	case arrow.UINT16:
		sortItems[uint16, *array.Uint16](indicesBuilder, ranks, currentArray.(*array.Uint16))
	case arrow.UINT32:
		sortItems[uint32, *array.Uint32](indicesBuilder, ranks, currentArray.(*array.Uint32))
	case arrow.UINT64:
		sortItems[uint64, *array.Uint64](indicesBuilder, ranks, currentArray.(*array.Uint64))
	case arrow.FLOAT16:
		return nil, ErrUnsupportedDataType
	case arrow.FLOAT32:
		sortItems[float32, *array.Float32](indicesBuilder, ranks, currentArray.(*array.Float32))
	case arrow.FLOAT64:
		sortItems[float64, *array.Float64](indicesBuilder, ranks, currentArray.(*array.Float64))
	case arrow.STRING:
		sortItems[string, *array.String](indicesBuilder, ranks, currentArray.(*array.String))
	case arrow.BINARY:
    return nil, ErrUnsupportedDataType
	case arrow.BOOL:
		return nil, ErrUnsupportedDataType
	case arrow.DATE32:
		sortItems[arrow.Date32, *array.Date32](indicesBuilder, ranks, currentArray.(*array.Date32))
	case arrow.DATE64:
		sortItems[arrow.Date64, *array.Date64](indicesBuilder, ranks, currentArray.(*array.Date64))
  case arrow.TIMESTAMP:
		sortItems[arrow.Timestamp, *array.Timestamp](indicesBuilder, ranks, currentArray.(*array.Timestamp))
	case arrow.TIME32:
		sortItems[arrow.Time32, *array.Time32](indicesBuilder, ranks, currentArray.(*array.Time32))
	case arrow.TIME64:
		sortItems[arrow.Time64, *array.Time64](indicesBuilder, ranks, currentArray.(*array.Time64))
	case arrow.DURATION:
		sortItems[arrow.Duration, *array.Duration](indicesBuilder, ranks, currentArray.(*array.Duration))
	default:
		return nil, ErrUnsupportedDataType
	}
	return indicesBuilder.NewUint32Array(), nil
}

func sortItems[E cmp.Ordered, T orderableArray[E]](indicesBuilder *array.Uint32Builder, ranks *array.Uint32, arr T) {
	sortItems := make([]sortItem[E], arr.Len())
	for i := 0; i < arr.Len(); i++ {
		sortItems[i] = sortItem[E]{
			Rank:  ranks.Value(i),
			Index: uint32(i),
			Value: arr.Value(i),
		}
	}
	slices.SortFunc(sortItems, func(item1, item2 sortItem[E]) int {
		if n := cmp.Compare(item1.Rank, item2.Rank); n != 0 {
			return n
		}
		return cmp.Compare(item1.Value, item2.Value)
	})
	indicesBuilder.Resize(arr.Len())
	indicesBuilder.AppendValues(sortItemsToIndexes(sortItems), nil)
}

func sortItemsToIndexes[E comparable](sortItems []sortItem[E]) []uint32 {
	indicies := make([]uint32, len(sortItems))
	for i, item := range sortItems {
		indicies[i] = item.Index
	}
	return indicies
}

func RankArray(mem *memory.GoAllocator, arr arrow.Array) (*array.Uint32, error) {
	switch arr.DataType().ID() {
	case arrow.INT8:
		return nativeRankArray[int8, *array.Int8](mem, arr.(*array.Int8))
	case arrow.INT16:
		return nativeRankArray[int16, *array.Int16](mem, arr.(*array.Int16))
	case arrow.INT32:
		return nativeRankArray[int32, *array.Int32](mem, arr.(*array.Int32))
	case arrow.INT64:
		return nativeRankArray[int64, *array.Int64](mem, arr.(*array.Int64))
	case arrow.UINT8:
		return nativeRankArray[uint8, *array.Uint8](mem, arr.(*array.Uint8))
	case arrow.UINT16:
		return nativeRankArray[uint16, *array.Uint16](mem, arr.(*array.Uint16))
	case arrow.UINT32:
		return nativeRankArray[uint32, *array.Uint32](mem, arr.(*array.Uint32))
	case arrow.UINT64:
		return nativeRankArray[uint64, *array.Uint64](mem, arr.(*array.Uint64))
	case arrow.FLOAT16:
		return nativeRankArray[float16.Num, *array.Float16](mem, arr.(*array.Float16))
	case arrow.FLOAT32:
		return nativeRankArray[float32, *array.Float32](mem, arr.(*array.Float32))
	case arrow.FLOAT64:
		return nativeRankArray[float64, *array.Float64](mem, arr.(*array.Float64))
	case arrow.STRING:
		return nativeRankArray[string, *array.String](mem, arr.(*array.String))
	case arrow.BINARY:
		return binaryRankArray(mem, arr.(*array.Binary))
	case arrow.BOOL:
		return nativeRankArray[bool, *array.Boolean](mem, arr.(*array.Boolean))
	case arrow.DATE32:
		return nativeRankArray[arrow.Date32, *array.Date32](mem, arr.(*array.Date32))
	case arrow.DATE64:
		return nativeRankArray[arrow.Date64, *array.Date64](mem, arr.(*array.Date64))
	case arrow.TIMESTAMP:
		return nativeRankArray[arrow.Timestamp, *array.Timestamp](mem, arr.(*array.Timestamp))
	case arrow.TIME32:
		return nativeRankArray[arrow.Time32, *array.Time32](mem, arr.(*array.Time32))
	case arrow.TIME64:
		return nativeRankArray[arrow.Time64, *array.Time64](mem, arr.(*array.Time64))
	case arrow.DURATION:
		return nativeRankArray[arrow.Duration, *array.Duration](mem, arr.(*array.Duration))
	default:
		return nil, errs.NewStackError(ErrUnsupportedDataType)
	}
}

func binaryRankArray(mem *memory.GoAllocator, arr *array.Binary) (*array.Uint32, error) {
	ranks := make([]uint32, arr.Len())
	ranks[0] = 0
	var currentRank uint32
	previousValue := arr.Value(0)
	for i := 0; i < arr.Len(); i++ {
		if !bytes.Equal(arr.Value(i), previousValue) {
			currentRank++
		}
		ranks[i] = uint32(currentRank)
	}
	builder := array.NewUint32Builder(mem)
	builder.AppendValues(ranks, nil)
	return builder.NewUint32Array(), nil
}

func nativeRankArray[E comparable, T valueArray[E]](mem *memory.GoAllocator, arr T) (*array.Uint32, error) {
	ranks := make([]uint32, arr.Len())
	ranks[0] = 0
	var currentRank uint32
	previousValue := arr.Value(0)
	for i := 0; i < arr.Len(); i++ {
		if arr.Value(i) != previousValue {
			currentRank++
		}
		ranks[i] = uint32(currentRank)
		previousValue = arr.Value(i)
	}
	builder := array.NewUint32Builder(mem)
	builder.AppendValues(ranks, nil)
	return builder.NewUint32Array(), nil
}
