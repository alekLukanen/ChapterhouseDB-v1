package storage

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	arrowops "github.com/alekLukanen/arrow-ops"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestManifestStorage_MergePartitionRecordIntoManifest(t *testing.T) {

	ctx := context.Background()
	mem := memory.NewGoAllocator()
	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
	options := ManifestStorageOptions{
		BucketName: "bucket",
		KeyPrefix:  "prefix",
	}

	testCases := []struct {
		caseName       string
		partition      elements.Partition
		buildPKRecord  func() (arrow.Record, error)
		buildNewRecord func() (arrow.Record, error)
		primaryColumns []string
		compareColumns []string
		options        PartitionManifestOptions
		// mocks
		buildObjectStorage func() (*MockObjectStorage, error)
		buildExternalFuncs func() *MockManifestStorageExternalFuncs
	}{
		{
			caseName: "merge-records-into-empty-manifest",
			partition: elements.Partition{
				TableName: "table-a",
				Key:       "23",
			},
			buildPKRecord: func() (arrow.Record, error) {
				rec := arrowops.MockData(mem, 1, "ascending")
				return rec, nil
			},
			buildNewRecord: func() (arrow.Record, error) {
				rec := arrowops.MockData(mem, 1, "ascending")
				return rec, nil
			},
			primaryColumns: []string{"a"},
			compareColumns: []string{"a", "b", "c"},
			options: PartitionManifestOptions{
				MaxObjects:    10,
				MaxObjectRows: 100,
			},
			buildObjectStorage: func() (*MockObjectStorage, error) {
				objectStorage := new(MockObjectStorage)

        // mocks for getting the manifest file
				objectStorage.On(
					"ListObjects",
					ctx,
					"bucket",
					"prefix/table-state/part-data/table-a/23/manifest_",
				).Return([]string{
					"prefix/table-state/part-data/table-a/23/manifest_3.json",
				}, nil).Once()

        manifest := PartitionManifest{
          Id:           "3",
          TableName:    "table-a",
          PartitionKey: "23",
          Version:      3,
          Objects: []ManifestObject{
            {
              Key:     "table-state/part-data/table-a/23/d_3_0.parquet",
              Index:   0,
              NumRows: 100,
            },
          },
        }
        manifestData, err := manifest.ToBytes()
        if err != nil {
          return nil, err
        }
        objectStorage.On(
          "Download",
          mock.Anything,
          "bucket",
          "prefix/table-state/part-data/table-a/23/manifest_3.json",
        ).Return(manifestData, nil).Once()

        // mocks for downloading the partition data file(s)
        objectStorage.On(
          "DownloadFile",
          mock.Anything,
          "bucket",
          "prefix/table-state/part-data/table-a/23/d_3_0.parquet",
          mock.MatchedBy(func (s string) bool {
            return strings.HasSuffix(s, "/0")
          }),
        ).Return(nil).Once()

        // mocks for replacing the partition manifest and data files
        objectStorage.On(
          "UploadFile",
          mock.Anything,
          "bucket",
          "prefix/table-state/part-data/table-a/23/d_4_0.parquet",
          "./example/d_4_0.parquet",
        ).Return(nil)

        expectedNewManifest := PartitionManifest{
          Id:           "4",
          TableName:    "table-a",
          PartitionKey: "23",
          Version:      4,
          Objects: []ManifestObject{
            {
              Key:     "table-state/part-data/table-a/23/d_4_0.parquet",
              Index:   0,
              NumRows: 100,
            },
          },
        }
        newManifestData, err := expectedNewManifest.ToBytes()
        if err != nil {
          return nil, err
        }
        objectStorage.On(
          "Upload",
          mock.Anything,
          "bucket",
          "prefix/table-state/part-data/table-a/23/manifest_4.json",
          newManifestData,
        ).Return(nil).Once()

        objectStorage.On(
          "Delete",
          mock.Anything,
          "bucket",
          "prefix/table-state/part-data/table-a/23/d_3_0.parquet",
        ).Return(nil).Once()

        objectStorage.On(
          "Delete",
          mock.Anything,
          "bucket",
          "prefix/table-state/part-data/table-a/23/manifest_3.json",
        ).Return(nil).Once()

				return objectStorage, nil
			},
			buildExternalFuncs: func() *MockManifestStorageExternalFuncs {
				externalFuncs := new(MockManifestStorageExternalFuncs)
        mockParquetMergeSortBuilder := new(MockParquetMergeSortBuilder)

        // mocks for the partition data file
        externalFuncs.On(
          "newParquetRecordMergeSortBuilder",
          mock.Anything,
          mock.Anything,
          mock.Anything,
          mock.Anything,
          mock.MatchedBy(func (s string) bool { return len(s) > 0 }),
          []string{"a"},
          []string{"a", "b", "c"},
          100,
        ).Return(mockParquetMergeSortBuilder, nil)

        mockParquetMergeSortBuilder.On(
          "BuildNextFiles",
          mock.Anything,
          mock.MatchedBy(func (s string) bool { return strings.HasSuffix(s, "/0") }),
        ).Return([]arrowops.ParquetFile{
          {
            FilePath: "./example/d_4_0.parquet",
            NumRows: 100,
          },
        }, nil).Once()

        mockParquetMergeSortBuilder.On(
          "BuildLastFiles",
          mock.Anything,
        ).Return([]arrowops.ParquetFile{}, nil).Once()

				return externalFuncs
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			objectStorage, err := tc.buildObjectStorage()
      if !assert.Nil(t, err, "failed to create object storage mocks") {
        return
      }
			externalFuncs := tc.buildExternalFuncs()

			manifestStorage := ManifestStorage{
				logger:                        logger,
				mem:                           mem,
				IObjectStorage:                objectStorage,
				bucketName:                    options.BucketName,
				keyPrefix:                     options.KeyPrefix,
				iManifestStorageExternalFuncs: externalFuncs,
			}

			pkRecord, err := tc.buildPKRecord()
			if !assert.Nil(t, err) {
				return
			}
			newRecord, err := tc.buildNewRecord()
			if !assert.Nil(t, err) {
				return
			}
			defer pkRecord.Release()
			defer newRecord.Release()

			err = manifestStorage.MergePartitionRecordIntoManifest(
				ctx,
				tc.partition,
				pkRecord,
				newRecord,
				tc.primaryColumns,
				tc.compareColumns,
				tc.options,
			)
			if !assert.Nil(t, err, "expected a nil error") {
				return
			}
      if !objectStorage.AssertExpectations(t) {
        t.Log("mock expectations were not met")
        return
      }
      if !externalFuncs.AssertExpectations(t) {
        t.Log("mock expectations were not met")
        return
      }

		})
	}

}

func TestManifestStorage_ReplacePartitionManifest_emptyPreviousManifest(t *testing.T) {

	ctx := context.Background()
	mem := memory.NewGoAllocator()
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)
	options := ManifestStorageOptions{
		BucketName: "bucket",
		KeyPrefix:  "prefix",
	}

	testCases := []struct {
		caseName           string
		partition          elements.Partition
		previousManifest   PartitionManifest
		manifest           PartitionManifest
		filePaths          []string
		buildObjectStorage func(manifest PartitionManifest) (*MockObjectStorage, error)
	}{
		{
			caseName: "empty-previous-manifest-and-non-empty-new-manifest",
			partition: elements.Partition{
				TableName: "table-a",
				Key:       "23",
			},
			previousManifest: PartitionManifest{
				Objects: []ManifestObject{},
			},
			manifest: PartitionManifest{
				Id:           "1",
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
			},
			filePaths: []string{"./example/d_1_0.parquet"},
			buildObjectStorage: func(manifest PartitionManifest) (*MockObjectStorage, error) {
				manifestData, err := manifest.ToBytes()
				if err != nil {
					return nil, err
				}

				objectStorage := new(MockObjectStorage)

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

				return objectStorage, nil
			},
		},
		{
			caseName: "replace-manifest-with-same-partition-in-previous-and-new-manifests",
			partition: elements.Partition{
				TableName: "table-a",
				Key:       "23",
			},
			previousManifest: PartitionManifest{
				Id:           "0",
				TableName:    "table-a",
				PartitionKey: "23",
				Version:      0,
				Objects: []ManifestObject{
					{
						Key:     "table-state/part-data/table-a/23/d_0_0.parquet",
						Index:   0,
						NumRows: 100,
					},
				},
			},
			manifest: PartitionManifest{
				Id:           "1",
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
			},
			filePaths: []string{"./example/d_1_0.parquet"},
			buildObjectStorage: func(manifest PartitionManifest) (*MockObjectStorage, error) {
				manifestData, err := manifest.ToBytes()
				if err != nil {
					return nil, err
				}

				objectStorage := new(MockObjectStorage)

				// upload new manifest files
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

				// delete previous manifest files
				objectStorage.On(
					"Delete",
					mock.Anything,
					"bucket",
					"prefix/table-state/part-data/table-a/23/d_0_0.parquet",
				).Return(nil).Once()
				objectStorage.On(
					"Delete",
					mock.Anything,
					"bucket",
					"prefix/table-state/part-data/table-a/23/manifest_0.json",
				).Return(nil).Once()

				return objectStorage, nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {

			objectStorage, err := tc.buildObjectStorage(tc.manifest)
			if !assert.Nil(t, err) {
				return
			}

			manifestStorage := NewManifestStorage(ctx, logger, mem, objectStorage, options)

			err = manifestStorage.ReplacePartitionManifest(
				ctx, tc.partition, &tc.previousManifest, &tc.manifest, tc.filePaths,
			)
			if !assert.Nil(t, err, "expected a nil error") {
				return
			}
			if !objectStorage.AssertExpectations(t) {
				t.Log("mock expectations were not met")
				return
			}

		})
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
