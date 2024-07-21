package arrowops

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
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
		recordMergeSortBuilder: NewRecordMergeSortBuilder(logger, mem, record, primaryColumns, compareColumns, maxRowsPerRecord),
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

/*
* --- IMPORTANT ---
* This merge sort builder assumes that each row is unique by it
* primary key columns. So no other row will have the same value
* for the given compound primary key. It is assumed that the user
* will perform deduplication of there records. 
 */
type RecordMergeSortBuilder struct {
	logger *slog.Logger
	mem    *memory.GoAllocator

	sampleRecord        progressRecord
	progressRecords     []*progressRecord
	progressRecordIndex int

	takeOrder [][2]uint32
	takeIndex int

	maxRowsPerRecord int
	primaryColumns   []string
	compareColumns   []string
}

func NewRecordMergeSortBuilder(logger *slog.Logger, mem *memory.GoAllocator, record arrow.Record, primaryColumns []string, compareColumns []string, maxRowsPerRecord int) *RecordMergeSortBuilder {
	return &RecordMergeSortBuilder{
		logger:              logger,
		mem:                 mem,
		sampleRecord:        *newProgressRecord(record, true),
		progressRecords:     make([]*progressRecord, 0),
		progressRecordIndex: 0,
		takeOrder:           make([][2]uint32, maxRowsPerRecord),
		takeIndex:           0,
		maxRowsPerRecord:    maxRowsPerRecord,
		primaryColumns:      primaryColumns,
		compareColumns:      compareColumns,
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
		return obj.TakeRecord()
	}

	if len(obj.progressRecords) == 0 {
		return nil, ErrRecordNotComplete
	}

	for idx, pRecord := range obj.progressRecords[obj.progressRecordIndex:] {
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
				// if the records are equal by key then compare them
				// when equal then take the old row
				// when not equal then take the new row
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
				if cmpRowDirection == 0 {
					obj.takeOrder[obj.takeIndex] = [2]uint32{uint32(idx), pRecord.index}
					obj.takeIndex++
					pRecord.index++
				} else {
					obj.takeOrder[obj.takeIndex] = [2]uint32{uint32(idx), pRecord.index}
					obj.takeIndex++
					pRecord.index++
				}

			} else if cmpRowDirection < 0 {
				obj.takeOrder[obj.takeIndex] = [2]uint32{uint32(idx), pRecord.index}
				obj.takeIndex++
				pRecord.index++
			} else {
				obj.takeOrder[obj.takeIndex] = [2]uint32{uint32(idx), obj.sampleRecord.index}
				obj.takeIndex++
				obj.sampleRecord.index++
			}

			if obj.takeIndex == obj.maxRowsPerRecord {
				return obj.TakeRecord()
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
	rb := array.NewRecordBuilder(obj.mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "record", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "recordIdx", Type: arrow.PrimitiveTypes.Uint32},
		}, nil))
	defer rb.Release()
	for idx := range len(obj.takeOrder) {
		rb.Field(0).(*array.Uint32Builder).Append(obj.takeOrder[idx][0])
		rb.Field(1).(*array.Uint32Builder).Append(obj.takeOrder[idx][1])
	}
	indices := rb.NewRecord()

	takenRecord, err := TakeMultipleRecords(obj.mem, obj.ProgressRecordsToRecords(), indices)
	if err != nil {
		return nil, err
	}

	// remove any progress records which no longer need to
	// be kept around.
	newProgressRecords := make([]*progressRecord, 0)
	for idx, pRec := range obj.progressRecords {
		if idx < obj.progressRecordIndex {
			pRec.Release()
		} else {
			newProgressRecords = append(newProgressRecords, pRec)
		}
	}
	obj.progressRecords = newProgressRecords
	obj.progressRecordIndex = 0

	return takenRecord, nil
}

func (obj *RecordMergeSortBuilder) ProgressRecordsToRecords() []arrow.Record {
	records := make([]arrow.Record, len(obj.progressRecords))
	for idx, pRec := range obj.progressRecords {
		records[idx] = pRec.record
	}
	return records
}
