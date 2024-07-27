package arrowops

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
)

var (
	ErrUnsupportedDataType  = errors.New("unsupported data type")
	ErrColumnNotFound       = errors.New("column not found")
	ErrRecordNotComplete    = errors.New("record not complete")
	ErrNoDataLeft           = errors.New("no data left")
	ErrSchemasNotEqual      = errors.New("schemas not equal")
	ErrDataTypesNotEqual    = errors.New("data types not equal")
	ErrNoDataSupplied       = errors.New("no data supplied")
	ErrIndexOutOfBounds     = errors.New("index out of bounds")
	ErrNullValuesNotAllowed = errors.New("null values not allowed")
)

func FErrSchemasNotEqual(record1, record2 arrow.Record, fields ...string) error {
	return fmt.Errorf(
		"%w|\n record1.schema: %s\n record2.schema: %s\n fields: %v\n",
		ErrSchemasNotEqual,
		record1.Schema(),
		record2.Schema(),
		fields)
}
