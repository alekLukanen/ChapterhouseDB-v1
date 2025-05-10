package partitionFuncs

import (
	"fmt"
	"hash"
	"hash/fnv"
	"math"

	"github.com/alekLukanen/ChapterhouseDB-v1/elements"
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type StringHashMethod string

const (
	MethodFNVHash StringHashMethod = "fnv-hash"
)

type StringHashPartitonOptions struct {
	PartitionCount int
	Method         StringHashMethod
}

func NewStringHashPartitionOptions(partitionCount int, method StringHashMethod) *StringHashPartitonOptions {
	return &StringHashPartitonOptions{
		PartitionCount: partitionCount,
		Method:         method,
	}
}

func (obj *StringHashPartitonOptions) PartitionType() string {
	return "string_hash"
}

func (obj *StringHashPartitonOptions) PartitionFunc() elements.PartitionFunc {
	switch obj.Method {
	case MethodFNVHash:
		return StringFNVHashPartition
	default:
		return nil
	}
}

func (obj *StringHashPartitonOptions) Validate() error {
	if obj.PartitionCount < 1 || obj.PartitionCount > int(math.MaxUint32) {
		return errs.NewStackError(
			fmt.Errorf(
				"%w: partition count of %d is out of bounds [1,%d]",
				ErrValidation,
				obj.PartitionCount,
				int(math.MaxUint32),
			),
		)
	}
	if obj.PartitionFunc() == nil {
		return errs.NewStackError(
			fmt.Errorf("%w: method %s not implemented", ErrValidation, obj.Method),
		)
	}

	return nil
}

func StringFNVHashPartition(mem *memory.GoAllocator, record arrow.Record, column string, options elements.IPartitionOptions) (arrow.Array, error) {

	strOptions, ok := options.(*StringHashPartitonOptions)
	if !ok {
		return nil, errs.NewStackError(ErrInvalidPartitionOptions)
	}

	hasher := fnv.New32()

	arrayBuilder := array.NewUint32Builder(mem)
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
		partIdx, err := FNVHash(arr, i, hasher, strOptions)
		if err != nil {
			return nil, errs.Wrap(err, fmt.Errorf("column: %s, array index: %d", column, i))
		}
		hasher.Reset()
		arrData[i] = partIdx
	}

	arrayBuilder.AppendValues(arrData, nil)

	partArr := arrayBuilder.NewArray()

	return partArr, nil

}

func FNVHash(arr arrow.Array, idx int, hasher hash.Hash32, options *StringHashPartitonOptions) (uint32, error) {

	switch arr.DataType().ID() {
	case arrow.STRING:
		value := arr.(*array.String).Value(idx)
		valueBytes := []byte(value)
		rn, err := hasher.Write(valueBytes)
		if err != nil {
			return 0, errs.NewStackError(err)
		} else if rn != len(valueBytes) {
			return 0, errs.NewStackError(fmt.Errorf("assert issue: not all data written in hash"))
		}
		part := findInterval(uint32(options.PartitionCount), hasher.Sum32())
		return part, nil
	default:
		return 0, errs.NewStackError(ErrStringHashTypeNotImplemented)
	}

}

func findInterval(n uint32, x uint32) uint32 {

	maxVal := uint32(math.MaxUint32)
	step := maxVal / n
	intervalID := uint32(x) / step

	if intervalID > n-1 {
		intervalID = n - 1
	}

	return intervalID

}
