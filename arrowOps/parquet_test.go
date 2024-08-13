package arrowops

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func TestWritingAndReadingParquetFile(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewGoAllocator()

	data := mockData(mem, 10, "ascending")
	workingDir, err := os.MkdirTemp("", "arrowops")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(workingDir)

	filePath := filepath.Join(workingDir, "test.parquet")

	err = WriteRecordToParquetFile(ctx, mem, data, filePath)
	if err != nil {
		t.Fatalf("WriteRecordToParquetFile failed: %v", err)
	}

	for i, col := range data.Columns() {
		t.Logf("data.column[%d] %q: %v\n", i, data.ColumnName(i), col)
	}

	readRecords, err := ReadParquetFile(ctx, mem, filePath)
	if err != nil {
		t.Fatalf("ReadParquetFile failed: %v", err)
	}
	if len(readRecords) != 1 {
		t.Fatalf("ReadParquetFile failed: expected 1 record, got %d", len(readRecords))
	}

	readRecord := readRecords[0]
	if readRecord.NumRows() != 10 {
		t.Fatalf("ReadParquetFile failed: expected 10 rows, got %d", readRecords[0].NumRows())
	}

	for i, col := range readRecord.Columns() {
		t.Logf("readRecord.column[%d] %q: %v\n", i, readRecord.ColumnName(i), col)
	}

	t.Log("readRecord.Schema", readRecord.Schema().Fields())

	if !array.Equal(readRecord.Column(0), data.Column(0)) {
		t.Fatalf("ReadParquetFile failed: column 0 not equal")
	}

	if !RecordsEqual(data, readRecord) {
		t.Log("Expected:", data)
		t.Log("Got:", readRecord)
		t.Errorf("ReadParquetFile failed: records are not equal")
		return
	}

}
