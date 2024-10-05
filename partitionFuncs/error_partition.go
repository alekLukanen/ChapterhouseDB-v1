package partitionFuncs

import (
	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func ErrorPartition(mem *memory.GoAllocator, record arrow.Record, column string, options elements.IPartitionOptions) (arrow.Array, error) {

	return nil, errs.NewStackError(ErrMethodNotFound)

}
