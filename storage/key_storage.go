package storage

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	goredislib "github.com/redis/go-redis/v9"

	"github.com/alekLukanen/ChapterhouseDB-v1/elements"
	"github.com/alekLukanen/errs"
)

type ILock interface {
	TryLockContext(context.Context) error
	UnlockContext(context.Context) (bool, error)
	Name() string
}

type IKeyStorage interface {
	AddItemsToTablePartition(context.Context, elements.Partition, [][]byte) (int64, error)

	GetTablePartitionItems(context.Context, elements.Partition, int) ([]string, error)
	GetTablePartitions(context.Context, string, uint64, int64) ([]elements.Partition, error)

	GetTablePartitionTimestamp(context.Context, elements.Partition) (time.Time, error)
	SetTablePartitionTimestamp(context.Context, elements.Partition) (bool, error)
	DeleteTablePartitionTimestamp(context.Context, elements.Partition) (bool, error)

	ClaimPartition(context.Context, elements.Partition, time.Duration) (ILock, error)
	ReleasePartitionLock(context.Context, ILock) (bool, error)
}

type KeyStorageOptions struct {
	Address   string
	Password  string
	KeyPrefix string
}

type KeyStorage struct {
	logger *slog.Logger
	client *goredislib.Client
	pool   redsyncredis.Pool
	sync   *redsync.Redsync

	KeyPrefix string
}

func NewKeyStorage(
	ctx context.Context,
	logger *slog.Logger,
	options KeyStorageOptions,
) (*KeyStorage, error) {
	client := goredislib.NewClient(&goredislib.Options{
		Addr:     options.Address,
		Password: options.Password, // no password set
		DB:       0,                // use default DB
	})

	redisPool := goredis.NewPool(client)
	mutexSync := redsync.New(redisPool)

	keyStorage := KeyStorage{
		logger:    logger,
		client:    client,
		pool:      redisPool,
		sync:      mutexSync,
		KeyPrefix: options.KeyPrefix,
	}
	return &keyStorage, nil
}

func (obj *KeyStorage) Close() error {
	return obj.client.Close()
}

func (obj *KeyStorage) Key(key string) string {
	return fmt.Sprintf("%s-%s", obj.KeyPrefix, key)
}

func (obj *KeyStorage) DerCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	derivedCtx, cancelFunc := context.WithTimeout(ctx, time.Second*15)
	return derivedCtx, cancelFunc
}

func (obj *KeyStorage) AcquireLock(ctx context.Context, key string, duration time.Duration) (ILock, error) {
	// Acquire lock
	mutex := obj.sync.NewMutex(obj.Key(key), redsync.WithExpiry(duration))
	if err := mutex.TryLockContext(ctx); err != nil {
		return nil, err
	}
	return mutex, nil
}

func (obj *KeyStorage) ReleaseLock(ctx context.Context, lock ILock) (bool, error) {
	ok, err := lock.UnlockContext(ctx)
	return ok, err
}

func (obj *KeyStorage) ClaimPartition(ctx context.Context, partition elements.Partition, duration time.Duration) (ILock, error) {
	key := fmt.Sprintf("%s/table-state/part-lock/%s/%s", obj.KeyPrefix, partition.TableName, partition.Key)
	return obj.AcquireLock(ctx, key, duration)
}

func (obj *KeyStorage) ReleasePartitionLock(ctx context.Context, lock ILock) (bool, error) {
	return obj.ReleaseLock(ctx, lock)
}

func (obj *KeyStorage) AddItemsToTablePartition(ctx context.Context, partition elements.Partition, items [][]byte) (int64, error) {
	key := fmt.Sprintf("%s/table-state/part-tuples/%s/%s/%s", obj.KeyPrefix, partition.TableName, partition.SubscriptionSourceName, partition.Key)

	// have to convert [][]byte to []interface{} to pass to SAdd
	interfaceItems := make([]interface{}, len(items))
	for i, item := range items {
		interfaceItems[i] = item
	}

	ctx, cancelFunc := obj.DerCtx(ctx)
	defer cancelFunc()

	result := obj.client.SAdd(ctx, obj.Key(key), interfaceItems)

	return result.Val(), result.Err()
}

func (obj *KeyStorage) GetTablePartitionItems(ctx context.Context, partition elements.Partition, count int) ([]string, error) {
	key := fmt.Sprintf("%s/table-state/part-tuples/%s/%s/%s", obj.KeyPrefix, partition.TableName, partition.SubscriptionSourceName, partition.Key)
	ctx, cancelFunc := obj.DerCtx(ctx)
	defer cancelFunc()
	result := obj.client.SPopN(ctx, obj.Key(key), int64(count))
	return result.Val(), result.Err()
}

func (obj *KeyStorage) GetTablePartitionTimestamp(ctx context.Context, partition elements.Partition) (time.Time, error) {
	key := fmt.Sprintf("%s/table-state/part-ts/%s/%s/%s", obj.KeyPrefix, partition.TableName, partition.SubscriptionSourceName, partition.Key)
	ctx, cancelFunc := obj.DerCtx(ctx)
	defer cancelFunc()
	result := obj.client.Get(ctx, obj.Key(key))
	if result.Err() != nil {
		return time.Time{}, result.Err()
	}
	ts, err := strconv.ParseInt(result.Val(), 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ts).UTC(), nil
}

func (obj *KeyStorage) SetTablePartitionTimestamp(ctx context.Context, partition elements.Partition) (bool, error) {
	key := fmt.Sprintf("%s/table-state/part-ts/%s/%s/%s", obj.KeyPrefix, partition.TableName, partition.SubscriptionSourceName, partition.Key)
	result := obj.client.SetNX(ctx, obj.Key(key), time.Now().UTC().UnixMilli(), 0)
	return result.Val(), result.Err()
}

func (obj *KeyStorage) DeleteTablePartitionTimestamp(ctx context.Context, partition elements.Partition) (bool, error) {
	key := fmt.Sprintf("%s/table-state/part-ts/%s/%s/%s", obj.KeyPrefix, partition.TableName, partition.SubscriptionSourceName, partition.Key)
	result := obj.client.Del(ctx, obj.Key(key))
	return result.Val() == 1, result.Err()
}

func (obj *KeyStorage) GetTablePartitions(ctx context.Context, tableName string, cursor uint64, count int64) ([]elements.Partition, error) {
	keyPrefix := fmt.Sprintf("%s/table-state/part-tuples/%s/*/*", obj.KeyPrefix, tableName)
	ctx, cancelFunc := obj.DerCtx(ctx)
	defer cancelFunc()
	keys, err := obj.getKeysByPrefix(ctx, keyPrefix, cursor, count)
	if err != nil {
		return nil, err
	}

	partitions := make([]elements.Partition, len(keys))
	for idx, key := range keys {
		partitionKey := strings.Split(key, "/")
		partitions[idx] = elements.Partition{
			TableName:              tableName,
			SubscriptionSourceName: partitionKey[len(partitionKey)-2],
			Key:                    partitionKey[len(partitionKey)-1],
		}
	}

	return partitions, nil
}

func (obj *KeyStorage) getKeysByPrefix(ctx context.Context, prefixPattern string, cursor uint64, count int64) ([]string, error) {
	ctx, cancelFunc := obj.DerCtx(ctx)
	defer cancelFunc()
	result := obj.client.Scan(ctx, cursor, obj.Key(prefixPattern), count)
	keys, _ := result.Val()
	err := result.Err()
	if err != nil {
		return nil, errs.NewStackError(err)
	}
	return keys, nil
}
