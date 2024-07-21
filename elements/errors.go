package elements

import "errors"

var (
	ErrTableAlreadyAddedToRegistry           = errors.New("table already added to registry")
	ErrTableNotFound                         = errors.New("table not found")
	ErrTableInvalid                          = errors.New("table invalid")
	ErrSubscriptionNotFound                  = errors.New("subscription not found")
	ErrColumnNotFound                        = errors.New("column not found")
	ErrTupleColumnsDifferentThanSubscription = errors.New("tuple columns different than subscription")
	ErrUnsupportedArrowToAvroTypeConversion  = errors.New("unsupported arrow to avro type conversion")
)
