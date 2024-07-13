package storage

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/alekLukanen/chapterhouseDB/elements"
)

type ObjectStorageManagerOptions struct {
	MaxFiles    int
	MaxSizeInMB int
	BucketName  string
	KeyPrefix   string
}

type ObjectStorageManager struct {
	logger *slog.Logger

	IObjectStorage

	maxFiles    int
	maxSizeInMB int
	bucketName  string
	keyPrefix   string
}

func NewObjectStorageManager(
	ctx context.Context,
	logger *slog.Logger,
	objectStorage IObjectStorage,
	options ObjectStorageManagerOptions,
) *ObjectStorageManager {
	return &ObjectStorageManager{
		logger:         logger,
		IObjectStorage: objectStorage,
		maxFiles:       options.MaxFiles,
		maxSizeInMB:    options.MaxSizeInMB,
		bucketName:     options.BucketName,
		keyPrefix:      options.KeyPrefix,
	}
}

func (obj *ObjectStorageManager) GetPartitionManifest(ctx context.Context, partition elements.Partition) (*PartitionManifest, error) {

	// get the json manifest file
	manifestData, err := obj.Download(ctx, obj.bucketName, fmt.Sprintf("%s/table-state/part-data/%s/%s/manifest.json", obj.keyPrefix, partition.TableName, partition.Key))
	if err != nil {
		return nil, err
	}

	manifest, err := NewManifestFromBytes(manifestData)
	if err != nil {
		return nil, err
	}

	return manifest, nil
}

func (obj *ObjectStorageManager) GetPartitionManifestFile(ctx context.Context, manifest *PartitionManifest, index int, filePath string) error {

	manifestObject := manifest.Objects[index]
	err := obj.DownloadFile(ctx, obj.bucketName, fmt.Sprintf("%s/%s", obj.keyPrefix, manifestObject.Key), filePath)
	if err != nil {
		return err
	}

	return nil
}
