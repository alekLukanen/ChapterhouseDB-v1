package tasker

import (
	"errors"
)

var (
	ErrTaskNotFoundInRegistry       = errors.New("task not found in registry")
	ErrQueueNotFoundInRegistry      = errors.New("queue not found in registry")
	ErrQueueTypeInvalidForOperation = errors.New("queue type invalid for operation")
	ErrQueueRegistryIsEmpty         = errors.New("queue registry is empty")
	ErrKeyDBResponseInvalid         = errors.New("key db response invalid")
	ErrTaskNotAvailable             = errors.New("task not available")
)
