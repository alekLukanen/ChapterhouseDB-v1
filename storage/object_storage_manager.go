package storage

import (
	"log/slog"

	"context"
)

type ObjectStorageManagerOptions struct {
	MaxFiles    int
	MaxSizeInMB int
}

type ObjectStorageManager struct {
	logger *slog.Logger

	maxFiles    int
	maxSizeInMB int
}

func NewObjectStorageManager(
	ctx context.Context,
	logger *slog.Logger,
	options ObjectStorageManagerOptions,
) *ObjectStorageManager {
	return &ObjectStorageManager{
		logger:      logger,
		maxFiles:    options.MaxFiles,
		maxSizeInMB: options.MaxSizeInMB,
	}
}
