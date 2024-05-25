package storage

import (
	"context"
	"log/slog"

	"github.com/apache/arrow/go/v16/arrow"
)

type ParquetStorage struct {
	logger slog.Logger
}

func (obj *ParquetStorage) Write(ctx context.Context, schema *arrow.Schema, data []arrow.Record) error {
	obj.logger.Info("Writing data to Parquet")
	return nil
}
