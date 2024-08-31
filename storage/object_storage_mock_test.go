package storage

import (
	"context"

	"github.com/stretchr/testify/mock"
)


type MockObjectStorage struct {
  mock.Mock
}

func (obj *MockObjectStorage) Upload(ctx context.Context, bucket, key string, data []byte) error {
  ret := obj.Called(ctx, bucket, key, data)
  return ret.Error(0)
}

func (obj *MockObjectStorage) UploadFile(ctx context.Context, bucket, key, filepath string) error {
  ret := obj.Called(ctx, bucket, key, filepath)
  return ret.Error(0)
}

func (obj *MockObjectStorage) Download(ctx context.Context, bucket, key string) ([]byte, error) {
  ret := obj.Called(ctx, bucket, key)
  return ret.Get(0).([]byte), ret.Error(1)
}

func (obj *MockObjectStorage) DownloadFile(ctx context.Context, bucket, key, filepath string) error {
  ret := obj.Called(ctx, bucket, key, filepath)
  return ret.Error(0)
}

func (obj *MockObjectStorage) Delete(ctx context.Context, bucket, key string) error {
  ret := obj.Called(ctx, bucket, key)
  return ret.Error(0)
}

func (obj *MockObjectStorage) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
  ret := obj.Called(ctx, bucket, prefix)
  return ret.Get(0).([]string), ret.Error(1)
}
