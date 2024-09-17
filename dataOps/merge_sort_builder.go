package dataops

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"

	"github.com/alekLukanen/arrow-ops"
	"github.com/alekLukanen/errs"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type progressRecord struct {
	record   arrow.Record
	retained bool
	index    uint32
	done     bool
}

func newProgressRecord(record arrow.Record, retain bool) *progressRecord {
	if retain {
		record.Retain()
	}
	return &progressRecord{record: record, retained: retain}
}
func (obj *progressRecord) release() {
	obj.record.Release()
}
func (obj *progressRecord) increment() {
	obj.index++
	if int64(obj.index) >= obj.record.NumRows() {
		obj.done = true
	}
}

type ParquetRecordMergeSortBuilder struct {
	logger                 *slog.Logger
	mem                    *memory.GoAllocator
	recordMergeSortBuilder *RecordMergeSortBuilder
	workingDir             string

	fileIndexId int

	maxRowsPerRecord int
}

func NewParquetRecordMergeSortBuilder(
	logger *slog.Logger,
	mem *memory.GoAllocator,
	processedKeyRecord arrow.Record,
	newRecord arrow.Record,
	workingDir string,
	primaryColumns []string,
	compareColumns []string,
	maxRowsPerRecord int,
) (*ParquetRecordMergeSortBuilder, error) {
	processedKeyRecord.Retain()
	defer processedKeyRecord.Release()
	newRecord.Retain()
	defer newRecord.Release()

	sortedProcessedKeyRecord, err := arrowops.SortRecord(mem, processedKeyRecord, primaryColumns)
	if err != nil {
		return nil, fmt.Errorf("%w| failed to sort procssed key record", err)
	}
	defer sortedProcessedKeyRecord.Release()

	sortedNewRecord, err := arrowops.SortRecord(mem, newRecord, primaryColumns)
	if err != nil {
		return nil, fmt.Errorf("%w| failed to sort the new record", err)
	}
	defer sortedNewRecord.Release()

	sortedNewRecordWithParquetFieldIds := arrowops.AddParquetFieldIds(sortedNewRecord)
	defer sortedNewRecordWithParquetFieldIds.Release()

	sortedProcessedKeyRecordWithParquetFieldsIds := arrowops.AddParquetFieldIds(sortedProcessedKeyRecord)
	defer sortedProcessedKeyRecordWithParquetFieldsIds.Release()

	recordMergeSortBuilder, err := NewRecordMergeSortBuilder(
		logger,
		mem,
		sortedProcessedKeyRecordWithParquetFieldsIds,
		sortedNewRecordWithParquetFieldIds,
		primaryColumns,
		compareColumns,
		maxRowsPerRecord)
	if err != nil {
		return nil, fmt.Errorf("%w| failed to create record merge sort builder", err)
	}

	return &ParquetRecordMergeSortBuilder{
		logger:                 logger,
		mem:                    mem,
		recordMergeSortBuilder: recordMergeSortBuilder,
		workingDir:             workingDir,
		fileIndexId:            0,
		maxRowsPerRecord:       maxRowsPerRecord,
	}, nil
}

func (obj *ParquetRecordMergeSortBuilder) Release() {
	if obj.recordMergeSortBuilder != nil {
		obj.recordMergeSortBuilder.Release()
	}
}

func (obj *ParquetRecordMergeSortBuilder) addNewFile(ctx context.Context, record arrow.Record) (arrowops.ParquetFile, error) {
	record.Retain()
	defer record.Release()

	filePath := path.Join(obj.workingDir, fmt.Sprintf("file_%d.parquet", obj.fileIndexId))
	err := arrowops.WriteRecordToParquetFile(ctx, obj.mem, record, filePath)
	if err != nil {
		return arrowops.ParquetFile{}, err
	}
	obj.fileIndexId++
	return arrowops.ParquetFile{
		FilePath: filePath,
		NumRows:  record.NumRows(),
	}, nil
}

func (obj *ParquetRecordMergeSortBuilder) BuildNextFiles(ctx context.Context, filePath string) ([]arrowops.ParquetFile, error) {

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	records, err := arrowops.ReadParquetFile(ctx, obj.mem, filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()

	err = obj.recordMergeSortBuilder.AddMainLineRecords(records)
	if err != nil {
		return nil, err
	}

	files := make([]arrowops.ParquetFile, 0)
	for {
		nextRecord, err := obj.recordMergeSortBuilder.BuildNextRecord()
		if errors.Is(err, ErrRecordNotComplete) {
			break
		} else if err != nil {
			return nil, err
		}

		f, err := obj.addNewFile(ctx, nextRecord)
		if err != nil {
			nextRecord.Release()
			return nil, err
		}
		nextRecord.Release()

		files = append(files, f)
	}

	obj.recordMergeSortBuilder.Debug()

	return files, nil
}

func (obj *ParquetRecordMergeSortBuilder) BuildLastFiles(ctx context.Context) ([]arrowops.ParquetFile, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	files := make([]arrowops.ParquetFile, 0)
	for {
		lastRecord, err := obj.recordMergeSortBuilder.BuildLastRecord()
		if errors.Is(err, ErrNoMoreRecords) {
			break
		} else if err != nil {
			return nil, err
		}

		pqf, err := obj.addNewFile(ctx, lastRecord)
		if err != nil {
			return nil, err
		}
		lastRecord.Release()

		files = append(files, pqf)
	}

	return files, nil
}

type takeInfo struct {
	recordIndex    uint32
	recordRowIndex uint32
	updated        bool
	isSampleRecord bool
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

	processedKeyRecordForMainLine *progressRecord

	sampleRecord        *progressRecord
	mainLineRecords     []*progressRecord
	mainLineRecordIndex int

	takeInfo  []*takeInfo
	takeIndex int

	maxRowsPerRecord int
	primaryColumns   []string
	compareColumns   []string
}

func NewRecordMergeSortBuilder(
	logger *slog.Logger,
	mem *memory.GoAllocator,
	processedKeyRecord arrow.Record,
	newRecord arrow.Record,
	primaryColumns []string,
	compareColumns []string,
	maxRowsPerRecord int,
) (*RecordMergeSortBuilder, error) {
	newRecord.Retain()
	processedKeyRecord.Retain()

	err := ValidateSampleRecord(processedKeyRecord, newRecord, primaryColumns)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return &RecordMergeSortBuilder{
		logger:                        logger,
		mem:                           mem,
		processedKeyRecordForMainLine: newProgressRecord(processedKeyRecord, true),
		sampleRecord:                  newProgressRecord(newRecord, true),
		mainLineRecords:               make([]*progressRecord, 0),
		mainLineRecordIndex:           0,
		takeInfo:                      make([]*takeInfo, maxRowsPerRecord),
		takeIndex:                     0,
		maxRowsPerRecord:              maxRowsPerRecord,
		primaryColumns:                primaryColumns,
		compareColumns:                compareColumns,
	}, nil

}

func (obj *RecordMergeSortBuilder) Debug() {
	obj.logger.Debug("RecordMergeSortBuilder",
		slog.Int("maxRowsPerRecord", obj.maxRowsPerRecord),
		slog.Int("takeIndex", obj.takeIndex),
		slog.Int("mainLineRecordIndex", obj.mainLineRecordIndex),
		slog.Any("primaryColumns", obj.primaryColumns),
		slog.Any("compareColumns", obj.compareColumns),
	)
}

func (obj *RecordMergeSortBuilder) Release() {
	obj.processedKeyRecordForMainLine.release()
	obj.sampleRecord.release()
}

func (obj *RecordMergeSortBuilder) AddMainLineRecords(records []arrow.Record) error {
	for _, record := range records {
		if arrowops.RecordSchemasEqual(obj.sampleRecord.record, record) {
			obj.mainLineRecords = append(obj.mainLineRecords, newProgressRecord(record, true))
		} else {
			return ErrSchemasNotEqual
		}
	}
	return nil
}

/*
* Builds the next full record using the records provided.
 */
func (obj *RecordMergeSortBuilder) BuildNextRecord() (arrow.Record, error) {

	if obj.takeIndex == obj.maxRowsPerRecord {
		return obj.TakeRecord()
	}

	obj.logger.Info("mainLineRecord count: ", slog.Int("count", len(obj.mainLineRecords)))

	if len(obj.mainLineRecords) == 0 {
		return nil, errs.NewStackError(ErrRecordNotComplete)
	}

	for idx := obj.mainLineRecordIndex; idx < len(obj.mainLineRecords); idx++ {
		pRecord := obj.mainLineRecords[idx]

		for !pRecord.done {

			if obj.takeIndex == obj.maxRowsPerRecord {
				return obj.TakeRecord()
			}

			// the sample record doesn't have anything left so we can
			// just pass the rest of the main line file rows
			if obj.sampleRecord.done {
				obj.takeRecordRow(idx, pRecord, true)
				continue
			}

			rowProcessed := false
			if !obj.processedKeyRecordForMainLine.done {
				obj.SeekProcessingKeyRecordForMainLine()
				if !obj.processedKeyRecordForMainLine.done {
					cmpRowProcessed, err := arrowops.CompareRecordRows(
						obj.processedKeyRecordForMainLine.record,
						pRecord.record,
						int(obj.processedKeyRecordForMainLine.index),
						int(pRecord.index),
						obj.primaryColumns...,
					)
					if err != nil {
						return nil, err
					}

					if cmpRowProcessed == 0 {
						obj.processedKeyRecordForMainLine.increment()
						rowProcessed = true
					}
				}
			} else {
				rowProcessed = false
			}

			cmpRowDirection, err := arrowops.CompareRecordRows(
				pRecord.record,
				obj.sampleRecord.record,
				int(pRecord.index),
				int(obj.sampleRecord.index),
				obj.primaryColumns...,
			)
			if err != nil {
				return nil, err
			}

			if !rowProcessed && cmpRowDirection < 0 {
				// ROW NOT PROCESSED AND LESS THAN ///////////////////

				obj.takeRecordRow(idx, pRecord, false)

			} else if cmpRowDirection == 0 {
				// ROWS ARE EQUAL //////////////////////////////
				// if the records are equal by key then compare them
				// when equal then take the old row
				// when not equal then take the new row
				cmpRowDirection, err = arrowops.CompareRecordRows(
					pRecord.record,
					obj.sampleRecord.record,
					int(pRecord.index),
					int(obj.sampleRecord.index),
					obj.compareColumns...,
				)
				if err != nil {
					return nil, err
				}
				if cmpRowDirection == 0 {
					// ROWS WAS NOT UPDATED ////////////////////////
					obj.takeRecordRow(idx, pRecord, false)
					obj.sampleRecord.increment()
				} else {
					// ROW WAS UPDATED ////////////////////////////
					obj.takeRecordRow(0, obj.sampleRecord, true)
					pRecord.increment()
				}

			} else if cmpRowDirection < 0 {
				// ROW WAS PROCESSED BUT WAS DELETED /////////////////////
				pRecord.increment()
			} else {
				// ROW IS NET NEW ////////////////////////////////////////
				obj.takeRecordRow(0, obj.sampleRecord, false)
			}

			if obj.takeIndex == obj.maxRowsPerRecord {
				return obj.TakeRecord()
			}

		}

		obj.mainLineRecordIndex = idx
	}

	if obj.takeIndex == obj.maxRowsPerRecord {
		return obj.TakeRecord()
	}

	return nil, ErrRecordNotComplete
}

func (obj *RecordMergeSortBuilder) takeRecordRow(idx int, record *progressRecord, updated bool) {
	obj.takeInfo[obj.takeIndex] = &takeInfo{
		recordIndex:    uint32(idx),
		recordRowIndex: record.index,
		updated:        updated,
		isSampleRecord: record == obj.sampleRecord,
	}
	obj.takeIndex++
	record.increment()
}

func (obj *RecordMergeSortBuilder) BuildLastRecord() (arrow.Record, error) {
	return obj.buildNextRecordFromSampleRecord(true)
}

func (obj *RecordMergeSortBuilder) buildNextRecordFromSampleRecord(allowPartialFill bool) (arrow.Record, error) {
	for !obj.sampleRecord.done {

		obj.takeRecordRow(0, obj.sampleRecord, true)

		if obj.takeIndex == obj.maxRowsPerRecord {
			return obj.TakeRecord()
		}

	}

	if allowPartialFill {
		return obj.TakeRecord()
	} else {
		return nil, ErrRecordNotComplete
	}
}

func (obj *RecordMergeSortBuilder) HasRecords() bool {
	return false
}

func (obj *RecordMergeSortBuilder) SeekProcessingKeyRecordForMainLine() {
	if len(obj.mainLineRecords) == 0 {
		return
	}

	progressRecord := obj.mainLineRecords[obj.mainLineRecordIndex]
	for int64(obj.processedKeyRecordForMainLine.index) < obj.processedKeyRecordForMainLine.record.NumRows() {
		cmpRowDirection, err := arrowops.CompareRecordRows(
			obj.processedKeyRecordForMainLine.record,
			progressRecord.record,
			int(obj.processedKeyRecordForMainLine.index),
			int(progressRecord.index),
			obj.primaryColumns...,
		)
		if err != nil {
			return
		}
		if cmpRowDirection == 0 || cmpRowDirection > 0 {
			return
		}
		obj.processedKeyRecordForMainLine.increment()
	}
}

func (obj *RecordMergeSortBuilder) currentTakenRecord() (arrow.Record, error) {
	if obj.takeIndex == 0 {
		return nil, errs.NewStackError(fmt.Errorf("%w| there aren't any record items to take", ErrNoMoreRecords))
	}

	rb := array.NewRecordBuilder(obj.mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "record", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "recordIdx", Type: arrow.PrimitiveTypes.Uint32},
		}, nil))
	defer rb.Release()

	for _, tf := range obj.takeInfo[:obj.takeIndex] {
		if tf.isSampleRecord {
			rb.Field(0).(*array.Uint32Builder).Append(0)
		} else {
			rb.Field(0).(*array.Uint32Builder).Append(tf.recordIndex + 1)
		}
		rb.Field(1).(*array.Uint32Builder).Append(tf.recordRowIndex)
	}
	indices := rb.NewRecord()
	defer indices.Release()

	records := obj.ProgressRecordsToRecords()
	takenRecord, err := arrowops.TakeMultipleRecords(obj.mem, records, indices)
	if err != nil {
		return nil, err
	}

	return takenRecord, nil
}

/*
* Using the current records and the take order, build the record.
* Once the record has been built then reset the take order
* and take index. The zeroth record will always be the sample
* record and the rest will be the progress records.
 */
func (obj *RecordMergeSortBuilder) TakeRecord() (arrow.Record, error) {

	takenRecord, err := obj.currentTakenRecord()
	if err != nil {
		return nil, err
	}

	// remove any progress records which no longer need to
	// be kept around.
	newProgressRecords := make([]*progressRecord, 0)
	for idx, pRec := range obj.mainLineRecords {
		if idx < obj.mainLineRecordIndex {
			pRec.release()
		} else {
			newProgressRecords = append(newProgressRecords, pRec)
		}
	}
	obj.mainLineRecords = newProgressRecords
	obj.mainLineRecordIndex = 0

	// reset the take info
	obj.takeInfo = make([]*takeInfo, obj.maxRowsPerRecord)
	obj.takeIndex = 0

	return takenRecord, nil
}

func (obj *RecordMergeSortBuilder) ProgressRecordsToRecords() []arrow.Record {
	records := make([]arrow.Record, len(obj.mainLineRecords)+1)
	records[0] = obj.sampleRecord.record
	for idx, pRec := range obj.mainLineRecords {
		records[idx+1] = pRec.record
	}
	return records
}

func ValidateSampleRecord(processedKeyRecord arrow.Record, sampleRecord arrow.Record, primaryColumns []string) error {

	// check if the sample record has any duplicate rows
	for i := int64(0); i < sampleRecord.NumRows()-1; i++ {
		cmpRowDirection, err := arrowops.CompareRecordRows(
			sampleRecord,
			sampleRecord,
			int(i),
			int(i)+1,
			primaryColumns...,
		)
		if err != nil {
			return errs.Wrap(err, fmt.Errorf("failed to compare sample record rows %d and %d", i, i+1))
		}
		if cmpRowDirection == 0 {
			return errs.NewStackError(ErrRecordHasDuplicateRows)
		}
	}

	// check if the sample record has any rows not in the processing key set
	pRecord := newProgressRecord(processedKeyRecord, true)
	for i := int64(0); i < sampleRecord.NumRows(); i++ {
		if pRecord.done {
			return errs.NewStackError(ErrRecordContainsRowsNotInProcessedKey)
		}
		for !pRecord.done {
			cmpRowDirection, err := arrowops.CompareRecordRows(
				pRecord.record,
				sampleRecord,
				int(pRecord.index),
				int(i),
				primaryColumns...,
			)
			if err != nil {
				return errs.Wrap(err, fmt.Errorf("failed to compare processed key record row %d and sample record row %d", pRecord.index, i))
			}

			if cmpRowDirection == 0 {
				pRecord.increment()
				break
			} else if cmpRowDirection > 0 {
				return errs.NewStackError(ErrRecordContainsRowsNotInProcessedKey)
			} else {
				pRecord.increment()
			}

		}
	}

	return nil
}
