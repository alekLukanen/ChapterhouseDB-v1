package elements

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type PartitionFunc func(*memory.GoAllocator, arrow.Record, string, IPartitionOptions) (arrow.Array, error)

type Transformer func(ctx context.Context, allocator *memory.GoAllocator, tuples arrow.Record) (arrow.Record, error)
