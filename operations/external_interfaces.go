package operations

import (
	"context"
	"time"

	"github.com/alekLukanen/ChapterhouseDB-v1/elements"
	"github.com/alekLukanen/ChapterhouseDB-v1/storage"
)

type IKeyStorage interface {
	AddItemsToTablePartition(context.Context, elements.Partition, [][]byte) (int64, error)

	GetTablePartitionItems(context.Context, elements.Partition, int) ([]string, error)
	GetTablePartitions(context.Context, string, uint64, int64) ([]elements.Partition, error)

	GetTablePartitionTimestamp(context.Context, elements.Partition) (time.Time, error)
	SetTablePartitionTimestamp(context.Context, elements.Partition) (bool, error)
	DeleteTablePartitionTimestamp(context.Context, elements.Partition) (bool, error)

	ClaimPartition(context.Context, elements.Partition, time.Duration) (storage.ILock, error)
	ReleasePartitionLock(context.Context, storage.ILock) (bool, error)
}
