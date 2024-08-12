package arrowops

import (
	"context"
	"os"
	"testing"

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

	filePath := workingDir + "/test.parquet"

	err = WriteRecordToParquetFile(ctx, mem, data, filePath)
	if err != nil {
		t.Fatalf("WriteRecordToParquetFile failed: %v", err)
	}

	readRecords, err := ReadParquetFile(ctx, mem, filePath)
	if err != nil {
		t.Fatalf("ReadParquetFile failed: %v", err)
	}
	if len(readRecords) != 1 {
		t.Fatalf("ReadParquetFile failed: expected 1 record, got %d", len(readRecords))
	}
	if readRecords[0].NumRows() != 10 {
		t.Fatalf("ReadParquetFile failed: expected 10 rows, got %d", readRecords[0].NumRows())
	}

	if !RecordsEqual(data, readRecords[0]) {
		t.Log("Expected:", data)
		t.Log("Got:", readRecords[0])
		t.Errorf("ReadParquetFile failed: records are not equal")
		return
	}

}
