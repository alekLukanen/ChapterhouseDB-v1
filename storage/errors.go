package storage

import (
	"github.com/go-redsync/redsync/v4"
)

var (
	ErrLockFailed         = redsync.ErrFailed
	ErrLockAlreadyExpired = redsync.ErrLockAlreadyExpired
)
