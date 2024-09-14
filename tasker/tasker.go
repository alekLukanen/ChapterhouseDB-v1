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

func (obj *Tasker) RegisterTask(task ITask) *Tasker {
	obj.taskRegistry.addTask(task)
	return obj
}

func (obj *Tasker) RegisterQueue(q Queue) *Tasker {
	obj.queueRegistry.addQueue(q)
	return obj
}

func (obj *Tasker) QueueKey(q Queue) string {
	return fmt.Sprintf("%s-%s", obj.keyPrefix, q.Name)
}

func (obj *Tasker) TaskKey(q Queue, td ITaskData) string {
	return fmt.Sprintf("%s-%s-%s", obj.keyPrefix, q.Name, td.Id())
}

/*
Add a task to the provided queue. If the queue is not a delayed queue then
return an error.
*/
func (obj *Tasker) DelayTask(
	ctx context.Context,
	td ITaskData,
	queue string,
	delay time.Duration,
	replace bool,
) (bool, error) {

	replaceNum := 0
	if replace {
		replaceNum = 1
	}

	q, err := obj.queueRegistry.findQueue(queue)
	if err != nil {
		return false, errs.NewStackError(err)
	}
	if q.Type != DelayedQueue {
		return false, errs.NewStackError(ErrQueueTypeInvalidForOperation)
	}

	data, err := td.Marshal()
	if err != nil {
		return false, err
	}

	/*
				-- Arguments:
				-- KEYS[1] - Sorted set key
				-- KEYS[2] - Hash key
				-- ARGV[1] - Sorted set member
				-- ARGV[2] - Score for the sorted set
				-- ARGV[3] - Value for the "id" field in the hash
				-- ARGV[4] - Value for the "name" field in the hash
				-- ARGV[5] - Value for the "data" field in the hash
		    -- ARGV[6] - Allow overwrites (0 - false, 1 - true)
	*/
	script := `
local exists = redis.call('ZSCORE', KEYS[1], ARGV[1])

if not exists or tonumber(ARGV[6]) == 1 then
    redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])

    redis.call('HSET', KEYS[2], 'id', ARGV[3], 'name', ARGV[4], 'data', ARGV[5])

    return "ADDED"
else
    return "ALREADY_EXISTS"
end
  `

	// add the task to the sorted set
	// add the task to the hash
	qKey := obj.QueueKey(q)
	tKey := obj.TaskKey(q, td)
	cTs := time.Now().UTC().UnixMilli()
	cmd := obj.client.Eval(
		ctx,
		script,
		[]string{qKey, tKey},
		tKey,
		cTs,
		td.Id(),
		td.TaskName(),
		data,
		replaceNum,
	)
	val, err := cmd.Val(), cmd.Err()
	if err != nil {
		return false, err
	}

	return val == "ADDED", nil

}
