package storage

import (
	"context"
	"log/slog"

	arrowops "github.com/alekLukanen/arrow-ops"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/mock"
)

type MockManifestStorageExternalFuncs struct {
	mock.Mock
}

func (obj *MockManifestStorageExternalFuncs) newParquetRecordMergeSortBuilder(
	logger *slog.Logger,
	mem *memory.GoAllocator,
	processedKeyRecord arrow.Record,
	newRecord arrow.Record,
	tmpDir string,
	primaryColumns []string,
	compareColumns []string,
	maxObjectRows int) (iParquetMergeSortBuilder, error) {
	args := obj.Called(
		logger,
		mem,
		processedKeyRecord,
		newRecord,
		tmpDir,
		primaryColumns,
		compareColumns,
		maxObjectRows)
	return args.Get(0).(iParquetMergeSortBuilder), args.Error(1)
}

type MockParquetMergeSortBuilder struct {
  mock.Mock
}

func (obj *MockParquetMergeSortBuilder) BuildNextFiles(ctx context.Context, tmpDir string) ([]arrowops.ParquetFile, error) {
  args := obj.Called(ctx, tmpDir)
  return args.Get(0).([]arrowops.ParquetFile), args.Error(1)
}

func (obj *MockParquetMergeSortBuilder) BuildLastFiles(ctx context.Context) ([]arrowops.ParquetFile, error) {
  args := obj.Called(ctx)
  return args.Get(0).([]arrowops.ParquetFile), args.Error(1)
}
