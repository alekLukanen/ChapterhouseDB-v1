package storage

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/alekLukanen/errs"
)

type PartitionManifestOptions struct {
	MaxObjects    int
	MaxObjectRows int
}

type ManifestObject struct {
	Key     string `json:"key"`
	Index   int    `json:"index"`
	NumRows int64  `json:"num_rows"`
}

func (obj *ManifestObject) Validate() error {
	if obj.Key == "" {
		return fmt.Errorf("%w| key is required", ErrManifestInvalid)
	}
	if obj.Index < 0 {
		return fmt.Errorf("%w| index must be positive", ErrManifestInvalid)
	}
	if obj.NumRows < 0 {
		return fmt.Errorf("%w| number of rows must be positive", ErrManifestInvalid)
	}
	return nil
}

type PartitionManifest struct {
	Id           string           `json:"id"`
	TableName    string           `json:"table_name"`
	PartitionKey string           `json:"partition_key"`
	Version      int              `json:"version"`
	Objects      []ManifestObject `json:"objects"`
}

func NewManifestFromBytes(data []byte) (*PartitionManifest, error) {
	manifest := &PartitionManifest{}
	err := json.Unmarshal(data, manifest)
	if err != nil {
		return nil, errs.NewStackError(err)
	}

	manifest.SortObjects()
	if ifErr := manifest.Validate(); ifErr != nil {
		return nil, ifErr
	}

	return manifest, nil
}

func (obj *PartitionManifest) ToBytes() ([]byte, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, errs.NewStackError(err)
	}
	return data, nil
}

func (obj *PartitionManifest) SortObjects() {
	slices.SortFunc(obj.Objects, func(a, b ManifestObject) int {
		return cmp.Compare(a.Index, b.Index)
	})
}

func (obj *PartitionManifest) Validate() error {
	if obj.Id == "" {
		return fmt.Errorf("%w| id is required", ErrManifestInvalid)
	}
	if obj.TableName == "" {
		return fmt.Errorf("%w| table name is required", ErrManifestInvalid)
	}
	if obj.PartitionKey == "" {
		return fmt.Errorf("%w| partition key is required", ErrManifestInvalid)
	}
	if obj.Version < 0 {
		return fmt.Errorf("%w| version must be positive", ErrManifestInvalid)
	}

	for idx, obj := range obj.Objects {
		if ifErr := obj.Validate(); ifErr != nil {
			return fmt.Errorf("%w| object at index %d is invalid: %v", ErrManifestInvalid, idx, ifErr)
		}
		if idx != obj.Index {
			return fmt.Errorf("%w| object at index %d has invalid index %d", ErrManifestInvalid, idx, obj.Index)
		}
	}

	return nil
}
