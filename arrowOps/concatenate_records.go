package arrowops

import (
	"fmt"

	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func ConcatenateRecords(mem *memory.GoAllocator, records ...arrow.Record) (arrow.Record, error) {
	for _, record := range records {
		record.Retain()
	}
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()

	// validate the records
	if len(records) == 0 {
		return nil, errs.NewStackError(fmt.Errorf("%w| records not provided", ErrNoDataSupplied))
	}
	schema := records[0].Schema()
	for _, record := range records {
		if !schema.Equal(record.Schema()) {
			return nil, errs.NewStackError(fmt.Errorf("%w| records schemas not all equal", ErrSchemasNotEqual))
		}
	}

	// group all of the columns from each record together
	// so that we can concatenate them together
	fields := make([][]arrow.Array, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		fields[i] = make([]arrow.Array, len(records))
	}
	for recordIdx, record := range records {
		for i := 0; i < schema.NumFields(); i++ {
			fields[i][recordIdx] = record.Column(i)
		}
	}

	// concatenate the columns of the same index together
	concatenatedFields := make([]arrow.Array, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		concatenatedField, err := array.Concatenate(fields[i], mem)
		if err != nil {
			return nil, errs.NewStackError(fmt.Errorf("%w| concatenation failed", err))
		}
		concatenatedFields[i] = concatenatedField
	}

	// get the total number of rows in the concatenated record
	var numRows uint32
	for _, record := range records {
		numRows += uint32(record.NumRows())
	}
	return array.NewRecord(schema, concatenatedFields, int64(numRows)), nil
}
