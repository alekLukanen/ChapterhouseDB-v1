package tasker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alekLukanen/errs"
	goredislib "github.com/redis/go-redis/v9"
)

type Options struct {
	KeyDBAddress  string
	KeyDBPassword string
	KeyPrefix     string
}

type Tasker struct {
	logger *slog.Logger

	client        goredislib.Client
	taskRegistry  *taskRegistry
	queueRegistry *queueRegistry

	keyPrefix string
}

func NewTasker(
	ctx context.Context,
	logger *slog.Logger,
	options Options,
) (*Tasker, error) {

	client := goredislib.NewClient(&goredislib.Options{
		Addr:     options.KeyDBAddress,
		Password: options.KeyDBPassword, // no password set
		DB:       0,                     // use default DB
	})

	return &Tasker{
		logger:        logger,
		client:        *client,
		taskRegistry:  newTaskRegistry(),
		queueRegistry: newQueueRegistry(),
		keyPrefix:     options.KeyPrefix,
	}, nil

}

func (obj *Tasker) RegisterTask(name string, bldr func() ITask) *Tasker {
	obj.taskRegistry.addTask(name, bldr)
	return obj
}

func (obj *Tasker) RegisterQueue(queue Queue) *Tasker {
	return obj
}

func (obj *Tasker) QueueKey(q Queue) string {
	return fmt.Sprintf("%s-%s", obj.keyPrefix, q.Name)
}

/*
Add a task to the provided queue. If the queue is not a delayed queue then
return an error.
*/
func (obj *Tasker) DelayTask(ctx context.Context, task ITask, queue string, delay time.Duration, replace bool) error {

	q, err := obj.queueRegistry.findQueue(queue)
	if err != nil {
		return errs.NewStackError(err)
	}
	if q.Type != DelayedQueue {
		return errs.NewStackError(ErrQueueTypeInvalidForOperation)
	}

	qKey := obj.QueueKey(q)

	data, err := task.Marshal()
	if err != nil {
		return err
	}

	// add the task to the sorted set
	// add the task to the hash
	cTs := time.Now().UTC().UnixMilli()
	pipe := obj.client.Pipeline()
	if replace {
		pipe.ZAdd(ctx, qKey, goredislib.Z{Score: float64(cTs), Member: task.Id()})
	} else {
		pipe.ZAddNX(ctx, qKey, goredislib.Z{Score: float64(cTs), Member: task.Id()})
	}
	pipe.HSet(ctx, task.Id(), map[string]interface{}{
		"id":   task.Id(),
		"name": task.Name(),
		"data": data,
	})
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}

	return nil

}
