package arrowops

import (
	"cmp"
	"slices"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

type SortItem[E comparable] struct {
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

	for _, column := range columns {
		columnIndexes := record.Schema().FieldIndices(column)
		if len(columnIndexes) == 0 {
			freeMemory()
			return nil, ErrColumnNotFound
		}
		columnIndex := columnIndexes[0]

		sortedIndices, err := RankedSort(mem, nil, record.Column(columnIndex))
		if err != nil {
			freeMemory()
			return nil, err
		}
		defer sortedIndices.Release()

		proposedRecord, err := TakeRecord(mem, record, sortedIndices)
		if err != nil {
			freeMemory()
			return nil, err
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
		SortItems[uint8, *array.Uint8](indicesBuilder, ranks, currentArray.(*array.Uint8))
	case arrow.INT16:
		SortItems[int16, *array.Int16](indicesBuilder, ranks, currentArray.(*array.Int16))
	case arrow.INT32:
		SortItems[int32, *array.Int32](indicesBuilder, ranks, currentArray.(*array.Int32))
	case arrow.INT64:
		SortItems[int64, *array.Int64](indicesBuilder, ranks, currentArray.(*array.Int64))
	case arrow.UINT8:
		SortItems[uint8, *array.Uint8](indicesBuilder, ranks, currentArray.(*array.Uint8))
	case arrow.UINT16:
		SortItems[uint16, *array.Uint16](indicesBuilder, ranks, currentArray.(*array.Uint16))
	case arrow.UINT32:
		SortItems[uint32, *array.Uint32](indicesBuilder, ranks, currentArray.(*array.Uint32))
	case arrow.UINT64:
		SortItems[uint64, *array.Uint64](indicesBuilder, ranks, currentArray.(*array.Uint64))
	case arrow.FLOAT16:
		return nil, ErrUnsupportedDataType
	case arrow.FLOAT32:
		SortItems[float32, *array.Float32](indicesBuilder, ranks, currentArray.(*array.Float32))
	case arrow.FLOAT64:
		SortItems[float64, *array.Float64](indicesBuilder, ranks, currentArray.(*array.Float64))
	case arrow.STRING:
		SortItems[string, *array.String](indicesBuilder, ranks, currentArray.(*array.String))
	case arrow.BINARY:
		return nil, ErrUnsupportedDataType
	case arrow.BOOL:
		return nil, ErrUnsupportedDataType
	case arrow.DATE32:
		return nil, ErrUnsupportedDataType
	case arrow.DATE64:
		return nil, ErrUnsupportedDataType
	case arrow.TIMESTAMP:
		return nil, ErrUnsupportedDataType
	case arrow.TIME32:
		return nil, ErrUnsupportedDataType
	case arrow.TIME64:
		return nil, ErrUnsupportedDataType
	case arrow.DURATION:
		return nil, ErrUnsupportedDataType
	default:
		return nil, ErrUnsupportedDataType
	}
	return indicesBuilder.NewUint32Array(), nil
}

type ValueArray[E cmp.Ordered] interface {
	Value(i int) E
	Len() int
}

func SortItems[E cmp.Ordered, T ValueArray[E]](indicesBuilder *array.Uint32Builder, ranks *array.Uint32, arr T) {
	sortItems := make([]SortItem[E], arr.Len())
	for i := 0; i < arr.Len(); i++ {
		sortItems[i] = SortItem[E]{
			Rank:  ranks.Value(i),
			Index: uint32(i),
			Value: arr.Value(i),
		}
	}
	slices.SortFunc(sortItems, func(item1, item2 SortItem[E]) int {
		if n := cmp.Compare(item1.Rank, item2.Rank); n != 0 {
			return n
		}
		return cmp.Compare(item1.Value, item2.Value)
	})
	indicesBuilder.Resize(arr.Len())
	indicesBuilder.AppendValues(SortItemsToIndexes(sortItems), nil)
}

func SortItemsToIndexes[E comparable](sortItems []SortItem[E]) []uint32 {
	indicies := make([]uint32, len(sortItems))
	for i, item := range sortItems {
		indicies[i] = item.Index
	}
	return indicies
}

func RankArray(mem *memory.GoAllocator, arr arrow.Array) (*array.Uint32, error) {
	ranks := make([]uint32, arr.Len())
	ranks[0] = 0
	var currentRank uint32
	switch arr.DataType().ID() {
	case arrow.INT8:
		previousValue := arr.(*array.Int8).Value(0)
		for i := 0; i < arr.Len(); i++ {
			if arr.(*array.Int8).Value(i) != previousValue {
				currentRank++
			}
			ranks[i] = uint32(currentRank)
		}
	default:
		return nil, ErrUnsupportedDataType
	}
	builder := array.NewUint32Builder(mem)
	builder.AppendValues(ranks, nil)
	return builder.NewUint32Array(), nil
}
