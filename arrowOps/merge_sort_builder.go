package arrowops

import (
	"github.com/apache/arrow/go/v16/arrow"
)

type RecordMergeSortBuilder struct {
	record        arrow.Record
	recordIndexes []uint32

	maxRowsPerRecord int
}

func NewRecordMergeSortBuilder(record arrow.Record, maxRowsPerRecord int) *RecordMergeSortBuilder {
	return &RecordMergeSortBuilder{
		record: record,

		maxRowsPerRecord: maxRowsPerRecord,
	}
}
