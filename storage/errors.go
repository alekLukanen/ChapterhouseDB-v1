package storage

import (
	"errors"
	"github.com/go-redsync/redsync/v4"
)

var (
	ErrLockFailed         = redsync.ErrFailed
	ErrLockAlreadyExpired = redsync.ErrLockAlreadyExpired
	ErrManifestInvalid    = errors.New("manifest is invalid")
)
