package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	arrowops "github.com/alekLukanen/ChapterhouseDB/arrowOps"
	"github.com/alekLukanen/ChapterhouseDB/elements"
)

type ManifestStorageOptions struct {
	MaxFiles    int
	MaxSizeInMB int
	BucketName  string
	KeyPrefix   string
}

type ManifestStorage struct {
	logger *slog.Logger
	mem    *memory.GoAllocator

	IObjectStorage

	maxFiles    int
	maxSizeInMB int
	bucketName  string
	keyPrefix   string
}

func NewManifestStorage(
	ctx context.Context,
	logger *slog.Logger,
	mem *memory.GoAllocator,
	objectStorage IObjectStorage,
	options ManifestStorageOptions,
) *ManifestStorage {
	return &ManifestStorage{
		logger:         logger,
		mem:            mem,
		IObjectStorage: objectStorage,
		maxFiles:       options.MaxFiles,
		maxSizeInMB:    options.MaxSizeInMB,
		bucketName:     options.BucketName,
		keyPrefix:      options.KeyPrefix,
	}
}

func (obj *ManifestStorage) GetPartitionManifest(ctx context.Context, partition elements.Partition) (*PartitionManifest, error) {

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

func (obj *ManifestStorage) GetPartitionManifestFile(ctx context.Context, manifest *PartitionManifest, index int, filePath string) error {

	manifestObject := manifest.Objects[index]
	err := obj.DownloadFile(ctx, obj.bucketName, fmt.Sprintf("%s/%s", obj.keyPrefix, manifestObject.Key), filePath)
	if err != nil {
		return err
	}

	return nil
}

func (obj *ManifestStorage) ReplacePartitionManifest(
	ctx context.Context,
	partition elements.Partition,
	previousManifest *PartitionManifest,
	manifest *PartitionManifest,
	filePaths []string,
) error {
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

func (obj *ManifestStorage) MergePartitionRecordIntoManifest(
	ctx context.Context,
	partition elements.Partition,
	record arrow.Record,
	options PartitionManifestOptions,
) error {
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
	_ = NewPartitionManifestBuilder(partition.TableName, partition.Key, manifest.Version+1)
	for idx, manifestObj := range manifestObjects {
		// download the file
		filePath := fmt.Sprintf("%s/%d", tmpDir, idx)
		err = obj.DownloadFile(ctx, obj.bucketName, fmt.Sprintf("%s/%s", obj.keyPrefix, manifestObj.Key), filePath)
		if err != nil {
			return err
		}

		// load the parquet file into a record
		_, err := arrowops.ReadParquetFile(ctx, obj.mem, filePath)
		if err != nil {
			return err
		}
	}

	return nil
}
