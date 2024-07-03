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

func SortRecord(mem *memory.GoAllocator, record arrow.Record, columns []string) (sortOrder []int, err error) {

	return sortOrder, err
}

func RankedSort(mem *memory.GoAllocator, previousArray, currentArray arrow.Array) (arrow.Array, error) {
	ranks, err := RankArray(mem, previousArray)
	if err != nil {
		return nil, err
	}
	defer ranks.Release()

	indiciesBuilder := array.NewUint32Builder(mem)

	switch currentArray.DataType().ID() {
	case arrow.INT8:
		sortItems := make([]SortItem[int8], currentArray.Len())
		for i := 0; i < currentArray.Len(); i++ {
			sortItems[i] = SortItem[int8]{
				Rank:  ranks.(*array.Uint32).Value(i),
				Index: uint32(i),
				Value: currentArray.(*array.Int8).Value(i),
			}
		}
		slices.SortFunc(sortItems, func(item1, item2 SortItem[int8]) int {
			if n := cmp.Compare(item1.Rank, item2.Rank); n != 0 {
				return n
			}
			return cmp.Compare(item1.Value, item2.Value)
		})
		indiciesBuilder.AppendValues(SortItemsToIndexes(sortItems), nil)
	default:
		return nil, ErrUnsupportedDataType
	}
	return indiciesBuilder.NewArray(), nil
}

func SortItemsToIndexes[E comparable](sortItems []SortItem[E]) []uint32 {
	indicies := make([]uint32, len(sortItems))
	for i, item := range sortItems {
		indicies[i] = item.Index
	}
	return indicies
}

func RankArray(mem *memory.GoAllocator, arr arrow.Array) (arrow.Array, error) {
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
	return builder.NewArray(), nil

}
