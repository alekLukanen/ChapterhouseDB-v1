package operations

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/alekLukanen/ChapterhouseDB-v1/elements"
	"github.com/alekLukanen/errs"
)

/*
* Returns an arry of partition arrays for each partitioned column. They are in the order
* in which the columns were passed in.
 */
func PartitionColumns(allocator *memory.GoAllocator, tuples arrow.Record, columns []elements.ColumnPartition) ([]arrow.Array, error) {
	partitionedColumns := make([]arrow.Array, len(columns))
	for idx, column := range columns {
		partitionFunc := column.Options().PartitionFunc()
		partitionedColumn, err := partitionFunc(allocator, tuples, column.Name(), column.Options())
		if err != nil {
			return nil, errs.NewStackError(err)
		}
		partitionedColumns[idx] = partitionedColumn
	}

	return partitionedColumns, nil
}

func PartitionKeys(allocator *memory.GoAllocator, tuples arrow.Record, columns []elements.ColumnPartition) (*array.String, error) {
	partitionedColumns, err := PartitionColumns(allocator, tuples, columns)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	if len(partitionedColumns) < 1 {
		return nil, errs.NewStackError(ErrPartitionColumnsEmpty)
	}

	keys := make([]string, tuples.NumRows())
	for idx := int64(0); idx < tuples.NumRows(); idx++ {
		var key string
		key += partitionedColumns[0].ValueStr(int(idx))
		if len(partitionedColumns) > 1 {
			for _, colParts := range partitionedColumns[1:] {
				key += fmt.Sprintf("-%s", colParts.ValueStr(int(idx)))
			}
		}
		keys[idx] = key
	}

	arrBuilder := array.NewStringBuilder(allocator)
	defer arrBuilder.Release()
	arrBuilder.AppendValues(keys, nil)

	arr := arrBuilder.NewArray().(*array.String)

	// release the partition arrays
	for _, partArr := range partitionedColumns {
		partArr.Release()
	}

	return arr, nil
}
