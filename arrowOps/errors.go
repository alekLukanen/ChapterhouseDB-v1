package arrowops

import "errors"

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
