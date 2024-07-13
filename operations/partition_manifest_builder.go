package operations

import (
	"fmt"

	"github.com/alekLukanen/chapterhouseDB/storage"
)

type PartitionManifestFile struct {
	filePath string
	storage.ManifestObject
}

type PartitionManifestBuilder struct {
	manifest *storage.PartitionManifest
	files    []string
}

func NewPartitionManifestBuilder(tableName string, partitionKey string, version int) *PartitionManifestBuilder {
	return &PartitionManifestBuilder{
		manifest: &storage.PartitionManifest{
			TableName:    tableName,
			PartitionKey: partitionKey,
			Version:      version,
			Objects:      []storage.ManifestObject{},
		},
		files: []string{},
	}
}

func (obj *PartitionManifestBuilder) AddFile(filePath string, index int, size int) {
	key := fmt.Sprintf("/table-state/part-data/%s/%s/d_%s_%s.parquet", obj.manifest.TableName, obj.manifest.PartitionKey, obj.manifest.Version, index)
	obj.manifest.Objects = append(obj.manifest.Objects, storage.ManifestObject{
		Key:   key,
		Index: index,
		Size:  size,
	})
	obj.files = append(obj.files, filePath)
}

func (obj *PartitionManifestBuilder) Manifest() *storage.PartitionManifest {
	return obj.manifest
}

func (obj *PartitionManifestBuilder) Files() []string {
	return obj.files
}
