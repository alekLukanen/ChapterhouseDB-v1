package arrowops

import (
	"context"
	"os"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet"
	parquetFileUtils "github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
)

type ParquetFile struct {
	FilePath string
	NumRows int64
}

func WriteRecordToParquetFile(ctx context.Context, mem *memory.GoAllocator, record arrow.Record, filePath string) error {

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	parquetWriteProps := parquet.NewWriterProperties(parquet.WithStats(true))
	arrowWriteProps := pqarrow.NewArrowWriterProperties()
	parquetFileWriter, err := pqarrow.NewFileWriter(record.Schema(), file, parquetWriteProps, arrowWriteProps)
	if err != nil {
		return err
	}
	defer parquetFileWriter.Close()

	err = parquetFileWriter.Write(record)
	if err != nil {
		return err
	}
	return nil
}

func ReadParquetFile(ctx context.Context, mem *memory.GoAllocator, filePath string) ([]arrow.Record, error) {

	parquetFileReader, err := parquetFileUtils.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, err
	}

	parquetReadProps := pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 1 << 20, // 1MB
	}
	parquetReadProps.SetReadDict(0, true)
	parquetReadProps.SetReadDict(1, true)
	arrowFileReader, err := pqarrow.NewFileReader(parquetFileReader, parquetReadProps, mem)
	if err != nil {
		return nil, err
	}

	recordReader, err := arrowFileReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return nil, err
	}

	records := make([]arrow.Record, 0)
	for recordReader.Next() {
		records = append(records, recordReader.Record())
	}

	return records, nil
}
