package elements

import (
	"context"
	"log/slog"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type PartitionFunc func(
  *memory.GoAllocator, 
  arrow.Record, 
  string, 
  IPartitionOptions,
) (arrow.Array, error)

type Transformer func(
  context.Context, 
  *memory.GoAllocator,
  *slog.Logger,
  arrow.Record,
) (arrow.Record, error)
