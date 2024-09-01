package storage

import (
	"log/slog"

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
