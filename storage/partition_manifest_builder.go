package storage

import (
	"fmt"

	arrowops "github.com/alekLukanen/ChapterhouseDB/arrowOps"
)

type PartitionManifestBuilder struct {
	manifest *PartitionManifest
	files    []string
	index    int
}

func NewPartitionManifestBuilder(tableName string, partitionKey string, version int) *PartitionManifestBuilder {
	return &PartitionManifestBuilder{
		manifest: &PartitionManifest{
			Id:           fmt.Sprintf("part-%d", version),
			TableName:    tableName,
			PartitionKey: partitionKey,
			Version:      version,
			Objects:      []ManifestObject{},
		},
		files: []string{},
	}
}

func (obj *PartitionManifestBuilder) AddFile(pqf arrowops.ParquetFile) {
	key := fmt.Sprintf(
		"table-state/part-data/%s/%s/d_%d_%d.parquet", 
		obj.manifest.TableName, 
		obj.manifest.PartitionKey,
		obj.manifest.Version, 
		obj.index,
	)
	obj.manifest.Objects = append(obj.manifest.Objects, ManifestObject{
		Key:     key,
		Index:   obj.index,
		NumRows: pqf.NumRows,
	})
	obj.files = append(obj.files, pqf.FilePath)
	obj.index++
}

func (obj *PartitionManifestBuilder) Manifest() *PartitionManifest {
	return obj.manifest
}

func (obj *PartitionManifestBuilder) Files() []string {
	return obj.files
}
