package storage

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	ObjectStorageAuthTypeStatic = "static"
)

type ObjectStorageOptions struct {
	Endpoint     string
	Region       string
	AuthKey      string
	AuthSecret   string
	UsePathStyle bool
	AuthType     string
}

func NewObjectStorageOptionsFromStaticCredentials(
	endpoint string,
	region string,
	authKey string,
	authSecret string,
	usePathStyle bool,
) *ObjectStorageOptions {
	return &ObjectStorageOptions{
		Endpoint:     endpoint,
		Region:       region,
		AuthKey:      authKey,
		AuthSecret:   authSecret,
		UsePathStyle: usePathStyle,
		AuthType:     ObjectStorageAuthTypeStatic,
	}
}

type ObjectStorage struct {
	logger *slog.Logger

	client *s3.Client
}

func NewObjectStorage(
	ctx context.Context,
	logger *slog.Logger,
	options ObjectStorageOptions,
) (*ObjectStorage, error) {

	configFuncs := make([]func(*config.LoadOptions) error, 0)
	configFuncs = append(configFuncs, config.WithRegion(options.Region))

	if options.AuthType == ObjectStorageAuthTypeStatic {
		creds := credentials.NewStaticCredentialsProvider(options.AuthKey, options.AuthSecret, "")
		configFuncs = append(configFuncs, config.WithCredentialsProvider(creds))
	}

	s3Config, err := config.LoadDefaultConfig(
		ctx,
		configFuncs...,
	)
	if err != nil {
		return nil, err
	}

	newSession := s3.NewFromConfig(s3Config, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(options.Endpoint)
		o.UsePathStyle = options.UsePathStyle
	})

	return &ObjectStorage{
		logger: logger,
		client: newSession,
	}, nil
}

func (obj *ObjectStorage) Upload(ctx context.Context, bucket, key string, body []byte) error {
	obj.logger.Info(
		"uploading object", slog.String("bucket", bucket), slog.String("key", key), slog.Int("numBytes", len(body)),
	)

	uploader := manager.NewUploader(obj.client)
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(body),
	})
	return err
}

func (obj *ObjectStorage) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	obj.logger.Info("downloading object", slog.String("bucket", bucket), slog.String("key", key))

	downloader := manager.NewDownloader(obj.client)
	buf := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(ctx, buf, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	return buf.Bytes(), err
}

func (obj *ObjectStorage) Delete(ctx context.Context, bucket, key string) error {
	obj.logger.Info("deleting object", slog.String("bucket", bucket), slog.String("key", key))

	_, err := obj.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	return err
}

func (obj *ObjectStorage) ListObjects(ctx context.Context, bucket string, prefix string) ([]string, error) {
	obj.logger.Info("listing objects", slog.String("bucket", bucket))

	maxKeys := int32(10_000)
	listObjectsOutput, err := obj.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  &bucket,
		Prefix:  aws.String(prefix),
		MaxKeys: &maxKeys,
	})
	if err != nil {
		return nil, err
	}

	keys := make([]string, len(listObjectsOutput.Contents))
	for i, obj := range listObjectsOutput.Contents {
		keys[i] = *obj.Key
	}
	return keys, nil
}
