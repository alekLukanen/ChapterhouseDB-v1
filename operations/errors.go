package operations

import "errors"

var (
	ErrTableAlreadyAddedToRegistry           = errors.New("table already added to registry")
	ErrTableNotFound                         = errors.New("table not found")
	ErrSubscriptionNotFound                  = errors.New("subscription not found")
	ErrColumnNotFound                        = errors.New("column not found")
	ErrTupleColumnsDifferentThanSubscription = errors.New("tuple columns different than subscription")
	ErrUnsupportedArrowToAvroTypeConversion  = errors.New("unsupported arrow to avro type conversion")
	ErrMultipleColumnsFound                  = errors.New("multiple columns found")
	ErrIntegerRangeTypeNotImplemented        = errors.New("integer range type not implemented")
	ErrNoPartitionsAvailable                 = errors.New("no partitions available")
	ErrPartitionTuplesEmpty                  = errors.New("partition tuples empty")
	ErrPartitionColumnsEmpty                 = errors.New("partition columns empty")
)
