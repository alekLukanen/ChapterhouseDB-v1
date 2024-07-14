package partitionFuncs

import (
	"fmt"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

type IntegerRangePartitionOptions struct {
	Width         int
	MaxPartitions int
}

func NewIntegerRangePartitionOptions(width, maxPartitions int) *IntegerRangePartitionOptions {
	return &IntegerRangePartitionOptions{
		Width:         width,
		MaxPartitions: maxPartitions,
	}
}

func (obj *IntegerRangePartitionOptions) PartitionType() string {
	return "integer_range"
}

func (obj *IntegerRangePartitionOptions) PartitionMetaData() map[string]string {
	return map[string]string{
		"width":          fmt.Sprint(obj.Width),
		"max_partitions": fmt.Sprint(obj.MaxPartitions),
	}
}

func (obj *IntegerRangePartitionOptions) PartitionFunc() elements.PartitionFunc {
	return IntegerRangePartition
}

/*
* Partition the rows by an integer range. The returned array represents the
* partition index for each row for the particular column name.
 */
func IntegerRangePartition(allocator *memory.GoAllocator, record arrow.Record, column string, options elements.IPartitionOptions) (arrow.Array, error) {
	intOptions, ok := options.(*IntegerRangePartitionOptions)
	if !ok {
		return nil, ErrInvalidPartitionOptions
	}

	arrayBuilder := array.NewUint32Builder(allocator)
	defer arrayBuilder.Release()

	schema := record.Schema()
	columnIdxs := schema.FieldIndices(column)
	if columnIdxs == nil || len(columnIdxs) == 0 {
		return nil, ErrColumnNotFound
	} else if len(columnIdxs) > 1 {
		return nil, ErrMultipleColumnsFound
	}

	columnIdx := columnIdxs[0]

	arr := record.Column(columnIdx)
	arrData := make([]uint32, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		partIdx, err := IntegerRangeCastCall(arr, i, intOptions)
		if err != nil {
			return nil, err
		}
		arrData[i] = partIdx
	}

	arrayBuilder.AppendValues(arrData, nil)

	partArr := arrayBuilder.NewArray()

	return partArr, nil
}

func IntegerRangeCastCall(arr arrow.Array, idx int, options *IntegerRangePartitionOptions) (uint32, error) {
	switch arr.DataType().ID() {
	case arrow.INT8:
		value := arr.(*array.Int8).Value(idx)
		part := value / int8(options.Width)
		return uint32(part), nil

	case arrow.INT16:
		value := arr.(*array.Int16).Value(idx)
		part := value / int16(options.Width)
		return uint32(part), nil

	case arrow.INT32:
		value := arr.(*array.Int32).Value(idx)
		part := value / int32(options.Width)
		return uint32(part), nil

	case arrow.INT64:
		value := arr.(*array.Int64).Value(idx)
		part := value / int64(options.Width)
		return uint32(part), nil

	case arrow.UINT8:
		value := arr.(*array.Uint8).Value(idx)
		part := value / uint8(options.Width)
		return uint32(part), nil

	case arrow.UINT16:
		value := arr.(*array.Uint16).Value(idx)
		part := value / uint16(options.Width)
		return uint32(part), nil

	case arrow.UINT32:
		value := arr.(*array.Uint32).Value(idx)
		part := value / uint32(options.Width)
		return uint32(part), nil

	case arrow.UINT64:
		value := arr.(*array.Uint64).Value(idx)
		part := value / uint64(options.Width)
		return uint32(part), nil

	default:
		return 0, ErrIntegerRangeTypeNotImplemented
	}
}
