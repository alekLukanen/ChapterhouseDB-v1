package arrowops

import "errors"

var (
	ErrUnsupportedDataType = errors.New("unsupported data type")
	ErrColumnNotFound      = errors.New("column not found")
)
