package warehouse

import (
	"context"
	"log/slog"
	"time"

	"github.com/alekLukanen/ChapterhouseDB/operations"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/ChapterhouseDB/tasker"
	"github.com/alekLukanen/ChapterhouseDB/tasks"
	"github.com/alekLukanen/errs"

	"github.com/apache/arrow/go/v17/arrow/memory"
)

type Warehouse struct {
	logger          *slog.Logger
	keyStorage      storage.IKeyStorage
	objectStorage   storage.IObjectStorage
	manifestStorage storage.IManifestStorage
	allocator       *memory.GoAllocator

	name          string
	TableRegistry operations.ITableRegistry
	Inserter      operations.IInserter
	Tasker        *tasker.Tasker
}

func NewWarehouse(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	tableRegistry *operations.TableRegistry,
	keyStorageOptions storage.KeyStorageOptions,
	objectStorageOptions storage.ObjectStorageOptions,
	manifestStorageOptions storage.ManifestStorageOptions,
	taskerOptions tasker.Options,
) (*Warehouse, error) {
	keyStorage, err := storage.NewKeyStorage(ctx, logger, keyStorageOptions)
	if err != nil {
		return nil, err
	}

	objectStorage, err := storage.NewObjectStorage(ctx, logger, objectStorageOptions)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	tr, err := operations.BuildTasker(ctx, logger, taskerOptions)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	allocator := memory.NewGoAllocator()
	inserter := operations.NewInserter(
		logger,
		tableRegistry,
		keyStorage,
		tr,
		allocator,
		operations.InserterOptions{
			PartitionLockDuration: 1 * time.Minute,
		},
	)

	manifestStorage := storage.NewManifestStorage(
		ctx, logger, allocator, objectStorage, manifestStorageOptions,
	)

	warehouse := &Warehouse{
		logger:          logger,
		keyStorage:      keyStorage,
		objectStorage:   objectStorage,
		manifestStorage: manifestStorage,
		allocator:       allocator,
		name:            name,
		TableRegistry:   tableRegistry,
		Inserter:        inserter,
		Tasker:          tr,
	}
	warehouse.registerTasks()
	return warehouse, nil
}

func (obj *Warehouse) registerTasks() {
	obj.Tasker.RegisterTask(
		tasks.NewTablePartitionTask(
			obj.logger,
			obj.keyStorage,
			obj.objectStorage,
			obj.manifestStorage,
			obj.allocator,
			obj.TableRegistry,
			obj.Inserter,
		),
	)
}

func (obj *Warehouse) Run(ctx context.Context) error {
	err := obj.Tasker.DelayedTaskLoop(ctx)
	if err != nil {
		return errs.Wrap(err)
	}
	return nil
}
