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

	fileIndexId  int
	newFilePaths []string

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

	sortedProcssedKeyRecord, err := SortRecord(mem, processedKeyRecord, primaryColumns)
	if err != nil {
		return nil, fmt.Errorf("%w| failed to sort procssed key record", err)
	}
	sortedNewRecord, err := SortRecord(mem, newRecord, primaryColumns)
	if err != nil {
		return nil, fmt.Errorf("%w| failed to sort the new record", err)
	}

	return &ParquetRecordMergeSortBuilder{
		logger:                 logger,
		mem:                    mem,
		recordMergeSortBuilder: NewRecordMergeSortBuilder(logger, mem, sortedProcssedKeyRecord, sortedNewRecord, primaryColumns, compareColumns, maxRowsPerRecord),
		workingDir:             workingDir,
		fileIndexId:            0,
		newFilePaths:           make([]string, 0),
		maxRowsPerRecord:       maxRowsPerRecord,
	}, nil
}

func (obj *ParquetRecordMergeSortBuilder) addNewFile(ctx context.Context, record arrow.Record) (ParquetFile, error) {
	record.Retain()
	defer record.Release()
	filePath := path.Join(obj.workingDir, fmt.Sprintf("file_%d.parquet", obj.fileIndexId))
	err := WriteRecordToParquetFile(ctx, obj.mem, record, filePath)
	if err != nil {
		return ParquetFile{}, err
	}
	obj.fileIndexId++
	return ParquetFile{
		FilePath: filePath,
		NumRows:  record.NumRows(),
	}, nil
}

func (obj *ParquetRecordMergeSortBuilder) BuildNext(ctx context.Context, filePath string) ([]ParquetFile, error) {

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	records, err := ReadParquetFile(ctx, obj.mem, filePath)
	if err != nil {
		return nil, err
	}
	obj.recordMergeSortBuilder.AddMainLineRecords(records)

	files := make([]ParquetFile, 0)
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

	return files, nil
}

func (obj *ParquetRecordMergeSortBuilder) BuildLast(ctx context.Context) (ParquetFile, error) {
	if ctx.Err() != nil {
		return ParquetFile{}, ctx.Err()
	}

	lastRecord, err := obj.recordMergeSortBuilder.BuildLastRecord()
	if errors.Is(err, ErrNoDataLeft) {
		return ParquetFile{}, nil
	} else if err != nil {
		return ParquetFile{}, err
	}

	pqf, err := obj.addNewFile(ctx, lastRecord)
	if err != nil {
		return ParquetFile{}, err
	}
	lastRecord.Release()

	return pqf, nil
}

type takeInfo struct {
	recordIndex    uint32
	recordRowIndex uint32
	updated        bool
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

	processedKeyRecord  *progressRecord
	sampleRecord        *progressRecord
	mainLineRecords     []*progressRecord
	mainLineRecordIndex int

	takeInfo  []takeInfo
	takeIndex int

	maxRowsPerRecord int
	primaryColumns   []string
	compareColumns   []string
}

func NewRecordMergeSortBuilder(logger *slog.Logger, mem *memory.GoAllocator, processedKeyRecord arrow.Record, newRecord arrow.Record, primaryColumns []string, compareColumns []string, maxRowsPerRecord int) *RecordMergeSortBuilder {
	return &RecordMergeSortBuilder{
		logger:              logger,
		mem:                 mem,
		processedKeyRecord:  newProgressRecord(processedKeyRecord, true),
		sampleRecord:        newProgressRecord(newRecord, true),
		mainLineRecords:     make([]*progressRecord, 0),
		mainLineRecordIndex: 0,
		takeInfo:            make([]takeInfo, maxRowsPerRecord),
		takeIndex:           0,
		maxRowsPerRecord:    maxRowsPerRecord,
		primaryColumns:      primaryColumns,
		compareColumns:      compareColumns,
	}
}

func (obj *RecordMergeSortBuilder) Debug() {
	obj.logger.Debug("RecordMergeSortBuilder",
		slog.Int("maxRowsPerRecord", obj.maxRowsPerRecord),
		slog.Int("takeIndex", obj.takeIndex),
		slog.Int("mainLineRecordIndex", obj.mainLineRecordIndex),
		slog.Any("takeInfo", obj.takeInfo),
		slog.Any("primaryColumns", obj.primaryColumns),
		slog.Any("compareColumns", obj.compareColumns),
	)
}

func (obj *RecordMergeSortBuilder) Release() {
	obj.processedKeyRecord.release()
	obj.sampleRecord.release()
}

func (obj *RecordMergeSortBuilder) AddMainLineRecords(records []arrow.Record) error {
	for _, record := range records {
		if RecordSchemasEqual(obj.sampleRecord.record, record) {
			obj.mainLineRecords = append(obj.mainLineRecords, newProgressRecord(record, false))
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

	if obj.takeIndex == obj.maxRowsPerRecord-1 {
		return obj.TakeRecord()
	}

	if len(obj.mainLineRecords) == 0 {
		return nil, ErrRecordNotComplete
	}

	for idx, pRecord := range obj.mainLineRecords[obj.mainLineRecordIndex:] {
		obj.logger.Info("Processing progress record", slog.Int("index", idx), slog.Any("rowIndex", pRecord.index))

		for !pRecord.done {

			rowProcessed := false
			if !obj.processedKeyRecord.done {
				obj.SeekProcessingKeyRecord()
				cmpRowProcessed, err := CompareRecordRows(
					obj.processedKeyRecord.record,
					pRecord.record,
					int(obj.processedKeyRecord.index),
					int(pRecord.index),
					obj.primaryColumns...,
				)
				if err != nil {
					return nil, err
				}

				if cmpRowProcessed == 0 {
					obj.processedKeyRecord.increment()
					rowProcessed = true
				}
			} else {
				rowProcessed = true
			}

			if !rowProcessed {
				// ROW NOT PROCESSED ///////////////////

				obj.takeInfo[obj.takeIndex] = takeInfo{
					recordIndex:    uint32(idx),
					recordRowIndex: pRecord.index,
					updated:        true,
				}
				obj.takeIndex++
				pRecord.increment()

			} else {
				// ROW PROCESSED //////////////////////

				cmpRowDirection, err := CompareRecordRows(
					pRecord.record,
					obj.sampleRecord.record,
					int(pRecord.index),
					int(obj.sampleRecord.index),
					obj.primaryColumns...,
				)
				if err != nil {
					return nil, err
				}

				if cmpRowDirection == 0 {
					// ROWS ARE EQUAL //////////////////////////////
					// if the records are equal by key then compare them
					// when equal then take the old row
					// when not equal then take the new row
					cmpRowDirection, err = CompareRecordRows(
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
						obj.takeInfo[obj.takeIndex] = takeInfo{
							recordIndex:    uint32(idx),
							recordRowIndex: pRecord.index,
							updated:        false,
						}
						obj.takeIndex++
						pRecord.increment()
						obj.sampleRecord.increment()
					} else {
						// ROW WAS UPDATED ////////////////////////////
						obj.takeInfo[obj.takeIndex] = takeInfo{
							recordIndex:    uint32(idx),
							recordRowIndex: obj.sampleRecord.index,
							updated:        true,
						}
						obj.takeIndex++
						obj.sampleRecord.increment()
					}

				} else if cmpRowDirection < 0 {
					// ROW WAS PROCESSED BUT WAS DELETED /////////////////////
					pRecord.increment()
				} else {
					// ROW IS NET NEW ////////////////////////////////////////
					obj.takeInfo[obj.takeIndex] = takeInfo{
						recordIndex:    uint32(idx),
						recordRowIndex: obj.sampleRecord.index,
						updated:        true,
					}
					obj.takeIndex++
					obj.sampleRecord.increment()
				}

			}

			if obj.takeIndex == obj.maxRowsPerRecord {
				return obj.TakeRecord()
			}

		}

		obj.mainLineRecordIndex = idx
	}
	return nil, ErrRecordNotComplete
}

func (obj *RecordMergeSortBuilder) BuildLastRecord() (arrow.Record, error) {

	for !obj.processedKeyRecord.done {

		obj.takeInfo[obj.takeIndex] = takeInfo{
			recordIndex:    0,
			recordRowIndex: obj.processedKeyRecord.index,
			updated:        false,
		}
		obj.takeIndex++
		obj.processedKeyRecord.increment()

		if obj.takeIndex == obj.maxRowsPerRecord {
			return obj.TakeRecord()
		}

	}

	return nil, ErrNoDataLeft
}

func (obj *RecordMergeSortBuilder) HasRecords() bool {
	return false
}

func (obj *RecordMergeSortBuilder) SeekProcessingKeyRecord() {
	if len(obj.mainLineRecords) == 0 {
		return
	}

	progressRecord := obj.mainLineRecords[0]
	for int64(obj.processedKeyRecord.index) < obj.processedKeyRecord.record.NumRows() {
		cmpRowDirection, err := CompareRecordRows(
			obj.processedKeyRecord.record,
			progressRecord.record,
			int(obj.processedKeyRecord.index),
			int(progressRecord.index),
			obj.primaryColumns...,
		)
		if err != nil {
			return
		}
		if cmpRowDirection == 0 || cmpRowDirection > 0 {
			return
		}
		obj.processedKeyRecord.increment()
	}
}

/*
* Using the current records and the take order, build the record.
* Once the record has been built then reset the take order
* and take index. The zeroth record will always be the sample
* record and the rest will be the progress records.
 */
func (obj *RecordMergeSortBuilder) TakeRecord() (arrow.Record, error) {
	rb := array.NewRecordBuilder(obj.mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "record", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "recordIdx", Type: arrow.PrimitiveTypes.Uint32},
		}, nil))
	defer rb.Release()
	for _, tf := range obj.takeInfo {
		rb.Field(0).(*array.Uint32Builder).Append(tf.recordIndex + 1)
		rb.Field(1).(*array.Uint32Builder).Append(tf.recordRowIndex)
	}
	indices := rb.NewRecord()

	takenRecord, err := TakeMultipleRecords(obj.mem, obj.ProgressRecordsToRecords(), indices)
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
	obj.takeInfo = make([]takeInfo, obj.maxRowsPerRecord)
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
