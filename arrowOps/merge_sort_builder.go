package arrowops

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

type progressRecord struct {
	record   arrow.Record
	retained bool
	index    uint32
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
	logger                 *slog.Logger
	mem                    *memory.GoAllocator
	recordMergeSortBuilder *RecordMergeSortBuilder
	filePaths              []string
	workingDir             string

	maxRowsPerRecord int
}

func NewParquetRecordMergeSortBuilder(logger *slog.Logger, mem *memory.GoAllocator, record arrow.Record, filePaths []string, workingDir string, primaryColumns []string, compareColumns []string, maxRowsPerRecord int) *ParquetRecordMergeSortBuilder {
	return &ParquetRecordMergeSortBuilder{
		logger:                 logger,
		mem:                    mem,
		recordMergeSortBuilder: NewRecordMergeSortBuilder(logger, record, primaryColumns, compareColumns, maxRowsPerRecord),
		filePaths:              filePaths,
		workingDir:             workingDir,
		maxRowsPerRecord:       maxRowsPerRecord,
	}
}
func (obj *ParquetRecordMergeSortBuilder) Build(ctx context.Context) ([]string, error) {

	newFilePaths := make([]string, 0)
	var fileIndexId int

	addFile := func(record arrow.Record) error {
		filePath := path.Join(obj.workingDir, fmt.Sprintf("file_%d.parquet", fileIndexId))
		err := WriteRecordToParquetFile(ctx, obj.mem, record, filePath)
		if err != nil {
			return err
		}
		newFilePaths = append(newFilePaths, filePath)
		fileIndexId++
		return nil
	}

	for _, filePath := range obj.filePaths {

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		records, err := ReadParquetFile(ctx, obj.mem, filePath)
		if err != nil {
			return nil, err
		}

		obj.recordMergeSortBuilder.AddExistingRecords(records)

		for {
			nextRecord, err := obj.recordMergeSortBuilder.BuildNextRecord()
			if errors.Is(err, ErrRecordNotComplete) {
				break
			} else if err != nil {
				return nil, err
			}

			err = addFile(nextRecord)
			if err != nil {
				nextRecord.Release()
				return nil, err
			}

			nextRecord.Release()
		}

	}

	lastRecord, err := obj.recordMergeSortBuilder.BuildLastRecord()
	if errors.Is(err, ErrNoDataLeft) {
		return newFilePaths, nil
	} else if err != nil {
		return nil, err
	}

	addFile(lastRecord)
	lastRecord.Release()

	return newFilePaths, nil

}

type RecordMergeSortBuilder struct {
	logger *slog.Logger

	sampleRecord    progressRecord
	progressRecords []*progressRecord

	takeOrder [][2]uint32
	takeIndex int

	maxRowsPerRecord int
	primaryColumns   []string
	compareColumns   []string
}

func NewRecordMergeSortBuilder(logger *slog.Logger, record arrow.Record, primaryColumns []string, compareColumns []string, maxRowsPerRecord int) *RecordMergeSortBuilder {
	return &RecordMergeSortBuilder{
		logger:           logger,
		sampleRecord:     *newProgressRecord(record, true),
		progressRecords:  make([]*progressRecord, 0),
		takeOrder:        make([][2]uint32, maxRowsPerRecord),
		takeIndex:        0,
		maxRowsPerRecord: maxRowsPerRecord,
		primaryColumns:   primaryColumns,
		compareColumns:   compareColumns,
	}
}

func (obj *RecordMergeSortBuilder) AddExistingRecords(records []arrow.Record) error {
	for _, record := range records {
		if !RecordSchemasEqual(obj.sampleRecord.record, record) {
			obj.progressRecords = append(obj.progressRecords, newProgressRecord(record, false))
		}
	}
	return nil
}

/*
* Builds the next full record using the records provided.
 */
func (obj *RecordMergeSortBuilder) BuildNextRecord() (arrow.Record, error) {

	if obj.takeIndex == obj.maxRowsPerRecord-1 {
		// TODO: build the record since we are out of room
	}

	if len(obj.progressRecords) == 0 {
		return nil, ErrRecordNotComplete
	}

	for idx, pRecord := range obj.progressRecords {
		obj.logger.Info("Processing progress record", slog.Int("index", idx), slog.Any("record", pRecord.record), slog.Any("rowIndex", pRecord.index))

		for int64(pRecord.index) < pRecord.record.NumRows() {

			cmpRowDirection, err := CompareRecordRows(
				pRecord.record,
				obj.sampleRecord.record,
				pRecord.index,
				obj.sampleRecord.index,
				obj.primaryColumns...,
			)
			if err != nil {
				return nil, err
			}

			if cmpRowDirection == 0 {
				cmpRowDirection, err = CompareRecordRows(
					pRecord.record,
					obj.sampleRecord.record,
					pRecord.index,
					obj.sampleRecord.index,
					obj.compareColumns...,
				)
				if err != nil {
					return nil, err
				}

			} else if cmpRowDirection < 0 {
				// Add the record to the sample order
				obj.takeOrder[obj.takeIndex] = [2]uint32{uint32(idx), pRecord.index}
				obj.takeIndex++
				pRecord.index++
			} else {
				obj.takeOrder[obj.takeIndex] = [2]uint32{uint32(idx), obj.sampleRecord.index}
				obj.takeIndex++
				obj.sampleRecord.index++
			}

			if obj.takeIndex == obj.maxRowsPerRecord {
				// TODO: build the record since we are out of space
				return nil, nil
			}

		}

	}
	return nil, nil
}

func (obj *RecordMergeSortBuilder) BuildLastRecord() (arrow.Record, error) {
	return nil, nil
}

func (obj *RecordMergeSortBuilder) HasRecords() bool {
	return false
}

/*
* Using the current records and the take order, build the record.
* Once the record has been built then reset the take order
* and take index.
 */
func (obj *RecordMergeSortBuilder) TakeRecord() (arrow.Record, error) {
	return nil, nil
}
