package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/alekLukanen/ChapterhouseDB/dataOps"
	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/alekLukanen/errs"
)

type IManifestStorage interface {
	GetPartitionManifest(context.Context, elements.Partition) (*PartitionManifest, error)
	GetPartitionManifestFile(context.Context, *PartitionManifest, int, string) error
	ReplacePartitionManifest(
		context.Context,
		elements.Partition,
		*PartitionManifest,
		*PartitionManifest, []string) error
	MergePartitionRecordIntoManifest(
		context.Context,
		elements.Partition,
		arrow.Record,
		arrow.Record,
		[]string,
		[]string,
		PartitionManifestOptions) error
}

type ManifestStorageOptions struct {
	MaxFiles    int
	BucketName  string
	KeyPrefix   string
}

type ManifestStorage struct {
	logger *slog.Logger
	mem    *memory.GoAllocator

	IObjectStorage

	maxFiles    int
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
		bucketName:     options.BucketName,
		keyPrefix:      options.KeyPrefix,
	}
}

func (obj *ManifestStorage) GetPartitionManifest(
  ctx context.Context, 
  partition elements.Partition,
) (*PartitionManifest, error) {

  manifestPrefix := fmt.Sprintf(
    "%s/table-state/part-data/%s/%s/manifest_", 
    obj.keyPrefix, 
    partition.TableName, 
    partition.Key)

  // get all manifests for the partition
  manifestKeys, err := obj.ListObjects(
    ctx, obj.bucketName, manifestPrefix,
  )
  if err != nil {
    return nil, errs.Wrap(
      err, 
      fmt.Errorf("failed getting manifests for table %s partition %s", partition.TableName, partition.Key),
    )
  }

  // parse the manifest id from the keys
  var newestManifestVersion int
  for _, key := range manifestKeys {
    cleanedKey := strings.TrimPrefix(key, manifestPrefix)
    cleanedKey = strings.TrimSuffix(cleanedKey, ".json")
    manifestVersion, err := strconv.Atoi(cleanedKey)
    if err != nil {
      continue
    }
    if manifestVersion > newestManifestVersion {
      newestManifestVersion = manifestVersion
    }
  }
    
	// get the json manifest file
	manifestData, err := obj.Download(
    ctx, 
    obj.bucketName, 
    fmt.Sprintf(
      "%s%d.json", manifestPrefix, newestManifestVersion,
    ),
  )
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
	processedKeyRecord arrow.Record,
	newRecord arrow.Record,
	primaryColumns []string,
	compareColumns []string,
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
  defer os.RemoveAll(tmpDir)

	parquetMergeSortBuilder, err := dataops.NewParquetRecordMergeSortBuilder(
		obj.logger,
		obj.mem,
		processedKeyRecord,
		newRecord,
		tmpDir,
		primaryColumns,
		compareColumns,
		options.MaxObjectRows,
	)
	manifestBuilder := NewPartitionManifestBuilder(partition.TableName, partition.Key, manifest.Version+1)
	for idx, manifestObj := range manifestObjects {
		// download the file
		filePath := fmt.Sprintf("%s/%d", tmpDir, idx)
		err = obj.DownloadFile(ctx, obj.bucketName, fmt.Sprintf("%s/%s", obj.keyPrefix, manifestObj.Key), filePath)
		if err != nil {
			return errs.Wrap(err, fmt.Errorf("failed downloading manifest object key: %s", manifestObj.Key))
		}
		// add the parquet file to the merge sort builder
		files, err := parquetMergeSortBuilder.BuildNextFiles(ctx, filePath)
		if err != nil {
			return errs.Wrap(err, fmt.Errorf("failed merging manifest object key: %s", manifestObj.Key))
		}
		for _, pqf := range files {
			manifestBuilder.AddFile(pqf)
		}
	}

	lastPqfs, err := parquetMergeSortBuilder.BuildLastFiles(ctx)
	if err != nil {
		return errs.Wrap(err, fmt.Errorf("failed building last file for partition key %s", partition.Key))
	}
	for _, pqf := range lastPqfs {
		manifestBuilder.AddFile(pqf)
	}

	err = obj.ReplacePartitionManifest(
		ctx,
		partition,
		manifest,
		manifestBuilder.Manifest(),
		manifestBuilder.Files(),
	)
	if err != nil {
		return errs.Wrap(err, fmt.Errorf("failed replacing manifest files for partition key %s", partition.Key))
	}

	return nil
}
