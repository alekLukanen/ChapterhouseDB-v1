package partitionFuncs

import (
	"fmt"

	"github.com/alekLukanen/ChapterhouseDB-v1/elements"
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type IntegerRangePartitionOptions struct {
	Width int
}

func NewIntegerRangePartitionOptions(width int) *IntegerRangePartitionOptions {
	return &IntegerRangePartitionOptions{
		Width: width,
	}
}

func (obj *IntegerRangePartitionOptions) PartitionType() string {
	return "integer_range"
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
		return nil, errs.NewStackError(ErrInvalidPartitionOptions)
	}

	arrayBuilder := array.NewUint32Builder(allocator)
	defer arrayBuilder.Release()

	schema := record.Schema()
	columnIdxs := schema.FieldIndices(column)
	if len(columnIdxs) == 0 {
		return nil, errs.NewStackError(ErrColumnNotFound)
	} else if len(columnIdxs) > 1 {
		return nil, errs.NewStackError(ErrMultipleColumnsFound)
	}

	columnIdx := columnIdxs[0]

	arr := record.Column(columnIdx)
	arrData := make([]uint32, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		partIdx, err := IntegerRangeCastCall(arr, i, intOptions)
		if err != nil {
			return nil, errs.Wrap(err, fmt.Errorf("column: %s, array index: %d", column, i))
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
		return 0, errs.NewStackError(ErrIntegerRangeTypeNotImplemented)
	}
}
