package operations

import (
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"

	"github.com/alekLukanen/chapterhouseDB/elements"
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
			return nil, err
		}
		partitionedColumns[idx] = partitionedColumn
	}

	return partitionedColumns, nil
}

func PartitionKeys(allocator *memory.GoAllocator, tuples arrow.Record, columns []elements.ColumnPartition) (*array.String, error) {
	partitionedColumns, err := PartitionColumns(allocator, tuples, columns)
	if err != nil {
		return nil, err
	}

	keys := make([]string, tuples.NumRows())
	for idx := int64(0); idx < tuples.NumRows(); idx++ {
		var key string
		for _, colParts := range partitionedColumns {
			key += colParts.ValueStr(int(idx))
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
