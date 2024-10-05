package partitionFuncs

import "errors"

var (
	ErrColumnNotFound                 = errors.New("column not found")
	ErrMultipleColumnsFound           = errors.New("multiple columns found")
	ErrIntegerRangeTypeNotImplemented = errors.New("integer range type not implemented")
	ErrInvalidPartitionOptions        = errors.New("invalid partition options")
	ErrStringHashTypeNotImplemented   = errors.New("string hash type not implemented")
	ErrMethodNotFound                 = errors.New("method not found")
	ErrValidation                     = errors.New("validation error")
)
