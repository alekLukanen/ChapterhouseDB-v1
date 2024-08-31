package storage

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestManifestStorage_ReplacePartitionManifest_emptyPreviousManifest(t *testing.T) {

	ctx := context.Background()
	mem := memory.NewGoAllocator()
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)
	objectStorage := new(MockObjectStorage)
	options := ManifestStorageOptions{
		MaxFiles:   5,
		BucketName: "bucket",
		KeyPrefix:  "prefix",
	}

	manifestStorage := NewManifestStorage(ctx, logger, mem, objectStorage, options)

	// partition
	partition := elements.Partition{
		TableName: "table-a",
		Key:       "23",
	}

	// previous empty manifest
	previousManifest := PartitionManifest{
		Objects: []ManifestObject{},
	}

	// manifest to upload
	manifest := PartitionManifest{
		Id:           "part-1",
		TableName:    "table-a",
		PartitionKey: "23",
		Version:      1,
		Objects: []ManifestObject{
			{
				Key:     "table-state/part-data/table-a/23/d_1_0.parquet",
				Index:   0,
				NumRows: 100,
			},
		},
	}
	filePaths := []string{"./example/d_1_0.parquet"}
	manifestData, err := manifest.ToBytes()
	if !assert.Nil(t, err) {
		return
	}

	// mock object storage calls
	objectStorage.On(
		"UploadFile",
		mock.Anything,
		"bucket",
		"prefix/table-state/part-data/table-a/23/d_1_0.parquet",
		"./example/d_1_0.parquet",
	).Return(nil).Once()

	objectStorage.On(
		"Upload",
		mock.Anything,
		"bucket",
		"prefix/table-state/part-data/table-a/23/manifest_1.json",
		manifestData,
	).Return(nil).Once()

	err = manifestStorage.ReplacePartitionManifest(
		ctx, partition, &previousManifest, &manifest, filePaths,
	)
	if !assert.Nil(t, err, "expected a nil error") {
		return
	}

}

func TestManifestStorage_GetPartitionManifest(t *testing.T) {

	ctx := context.Background()
	mem := memory.NewGoAllocator()
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)
	objectStorage := new(MockObjectStorage)
	options := ManifestStorageOptions{
		MaxFiles:   5,
		BucketName: "bucket",
		KeyPrefix:  "prefix",
	}

	manifestStorage := NewManifestStorage(ctx, logger, mem, objectStorage, options)

	// define the mock object storage expectations
	objectStorage.On(
		"ListObjects",
		ctx,
		"bucket",
		"prefix/table-state/part-data/table-a/23/manifest_",
	).Return([]string{
		"prefix/table-state/part-data/table-a/23/manifest_1.json",
		"prefix/table-state/part-data/table-a/23/manifest_3.json",
		"prefix/table-state/part-data/table-a/23/manifest_2.json",
		"prefix/table-state/part-data/table-a/23/manifest_abc.json",
	}, nil).Once()

	manifest := PartitionManifest{
		Id:           "part-1",
		TableName:    "table-a",
		PartitionKey: "23",
		Version:      1,
		Objects: []ManifestObject{
			{
				Key:     "table-state/part-data/table-a/23/d_1_0.parquet",
				Index:   0,
				NumRows: 100,
			},
		},
	}
	manifestData, err := manifest.ToBytes()
	if !assert.Nil(t, err) {
		return
	}
	objectStorage.On(
		"Download",
		ctx,
		"bucket",
		"prefix/table-state/part-data/table-a/23/manifest_3.json",
	).Return(manifestData, nil).Once()

	// test the function
	partition := elements.Partition{
		TableName: "table-a",
		Key:       "23",
	}
	result, err := manifestStorage.GetPartitionManifest(ctx, partition)
	if !assert.Nil(t, err, "expected a nil error") {
		return
	}
	if !assert.Equal(t, *result, manifest, "expected the manifests to be equal") {
		return
	}

}
