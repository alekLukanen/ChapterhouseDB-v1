package arrowops

import (
	"context"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

type progressRecord struct {
	record   arrow.Record
	retained bool
	index    int32
}

func newProgressRecord(record arrow.Record, retain bool) *progressRecord {
	if retain {
		record.Retain()
	}
	return &progressRecord{record: record, retained: retain}
}
func (obj *progressRecord) Release() {
	obj.record.Release()
}

type ParquetRecordMergeSortBuilder struct {
	mem                    *memory.GoAllocator
	recordMergeSortBuilder *RecordMergeSortBuilder
	filePaths              []string
}

func NewParquetRecordMergeSortBuilder(record arrow.Record, filePaths []string, maxRowsPerRecord int) *ParquetRecordMergeSortBuilder {
	return &ParquetRecordMergeSortBuilder{
		recordMergeSortBuilder: NewRecordMergeSortBuilder(record, maxRowsPerRecord),
	}
}
func (obj *ParquetRecordMergeSortBuilder) Build(ctx context.Context) ([]string, error) {

	for _, filePath := range obj.filePaths {

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		records, err := ReadParquetFile(ctx, obj.mem, filePath)
		if err != nil {
			return nil, err
		}

		obj.recordMergeSortBuilder.AddExistingRecords(records)

	}

	return nil, nil

}

type RecordMergeSortBuilder struct {
	sampleRecord    progressRecord
	progressRecords []*progressRecord

	maxRowsPerRecord int
}

func NewRecordMergeSortBuilder(record arrow.Record, maxRowsPerRecord int) *RecordMergeSortBuilder {
	return &RecordMergeSortBuilder{
		sampleRecord:     *newProgressRecord(record, true),
		progressRecords:  make([]*progressRecord, 0),
		maxRowsPerRecord: maxRowsPerRecord,
	}
}

func (obj *RecordMergeSortBuilder) AddExistingRecords(records []arrow.Record) {
	for _, record := range records {
		obj.progressRecords = append(obj.progressRecords, newProgressRecord(record, false))
	}
}

/*
* Builds the next full record using the records provided.
 */
func (obj *RecordMergeSortBuilder) BuildNextRecord() (arrow.Record, error) {
	return nil, nil
}

func (obj *RecordMergeSortBuilder) BuildLastRecord() (arrow.Record, error) {
	return nil, nil
}

func (obj *RecordMergeSortBuilder) HasRecords() bool {
	return false
}
