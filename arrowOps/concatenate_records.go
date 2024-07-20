package arrowops

import (
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
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
		return nil, ErrNoDataLeft
	}
	schema := records[0].Schema()
	for _, record := range records {
		if !schema.Equal(record.Schema()) {
			return nil, ErrSchemasNotEqual
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
			return nil, err
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
