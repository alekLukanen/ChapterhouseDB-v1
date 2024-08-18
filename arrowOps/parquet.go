package arrowops

import (
	"context"
	"os"

	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	parquetFileUtils "github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

type ParquetFile struct {
	FilePath string
	NumRows  int64
}

func WriteRecordToParquetFile(ctx context.Context, mem *memory.GoAllocator, record arrow.Record, filePath string) error {

	file, err := os.Create(filePath)
	if err != nil {
		return errs.NewStackError(err)
	}
	defer file.Close()

	parquetWriteProps := parquet.NewWriterProperties(
		parquet.WithStats(true),
	)
	arrowWriteProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	parquetFileWriter, err := pqarrow.NewFileWriter(
		record.Schema(),
		file,
		parquetWriteProps,
		arrowWriteProps,
	)
	if err != nil {
		return errs.NewStackError(err)
	}
	defer parquetFileWriter.Close()

	err = parquetFileWriter.Write(record)
	if err != nil {
		return errs.NewStackError(err)
	}
	return parquetFileWriter.Close()
}

func ReadParquetFile(ctx context.Context, mem *memory.GoAllocator, filePath string) ([]arrow.Record, error) {

	parquetFileReader, err := parquetFileUtils.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, errs.NewStackError(err)
	}
	defer parquetFileReader.Close()

	parquetReadProps := pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 1 << 20, // 1MB
	}
	arrowFileReader, err := pqarrow.NewFileReader(parquetFileReader, parquetReadProps, mem)
	if err != nil {
		return nil, errs.NewStackError(err)
	}

	recordReader, err := arrowFileReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return nil, errs.NewStackError(err)
	}
	defer recordReader.Release()

	records := make([]arrow.Record, 0)
	for recordReader.Next() {
		rec := recordReader.Record()
		rec.Retain()
		records = append(records, rec)
	}

	return records, nil
}
