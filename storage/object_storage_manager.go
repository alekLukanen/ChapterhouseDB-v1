package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/alekLukanen/chapterhouseDB/elements"
	"github.com/apache/arrow/go/v16/arrow"
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

func (obj *ObjectStorageManager) ReplacePartitionManifest(ctx context.Context, partition elements.Partition, previousManifest *PartitionManifest, manifest *PartitionManifest, filePaths []string) error {
	// upload the individual files
	for i, filePath := range filePaths {
		err := obj.UploadFile(ctx, obj.bucketName, fmt.Sprintf("%s/%s", obj.keyPrefix, manifest.Objects[i].Key), filePath)
		if err != nil {
			return err
		}
	}

	// upload the manifest file
	manifestData, err := manifest.ToBytes()
	if err != nil {
		return err
	}

	err = obj.Upload(
		ctx,
		obj.bucketName,
		fmt.Sprintf("%s/table-state/part-data/%s/%s/manifest_%s.json", obj.keyPrefix, partition.TableName, partition.Key, manifest.Id),
		manifestData,
	)
	if err != nil {
		return err
	}

	// delete the previous manifest file objects
	for _, manifestObj := range previousManifest.Objects {
		err = obj.Delete(ctx, obj.bucketName, fmt.Sprintf("%s/%s", obj.keyPrefix, manifestObj.Key))
		if err != nil {
			return err
		}
	}

	// delete the previous manifest file
	err = obj.Delete(
		ctx,
		obj.bucketName,
		fmt.Sprintf(
			"%s/table-state/part-data/%s/%s/manifest_%s.json",
			obj.keyPrefix,
			partition.TableName,
			partition.Key,
			previousManifest.Id,
		),
	)
	if err != nil {
		return err
	}

	return nil
}

func (obj *ObjectStorageManager) MergePartitionRecordIntoManifest(ctx context.Context, partition elements.Partition, record arrow.Record) error {
	// get the manifest
	hasManifest := true
	manifest, err := obj.GetPartitionManifest(ctx, partition)
	if err != nil {
		var notFoundErr *types.NoSuchKey
		if ok := errors.As(err, &notFoundErr); ok {
			hasManifest = false
		} else {
			return err
		}
	}

	var manifestObjects []ManifestObject
	if hasManifest {
		manifestObjects = manifest.Objects
	} else {
		manifestObjects = make([]ManifestObject, 0)
	}

	// iterate over each manifest object, request the file and
	// attempt to merge the record into the existing files if any.
	tmpDir, err := os.MkdirTemp("", "merge-part-rec")
	if err != nil {
		return err
	}
	manifestBuilder := NewPartitionManifestBuilder(partition.TableName, partition.Key, manifest.Version+1)
	for idx, manifestObj := range manifestObjects {
		// download the file

	}
}
