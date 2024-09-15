package tasker

import (
	"context"
	"errors"
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
	TaskTimeout   time.Duration
}

type Tasker struct {
	logger *slog.Logger

	client        goredislib.Client
	taskRegistry  *taskRegistry
	queueRegistry *queueRegistry

	keyPrefix   string
	taskTimeout time.Duration
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
		taskTimeout:   options.TaskTimeout,
	}, nil

}

func (obj *Tasker) RegisterTask(task ITask) *Tasker {
	obj.taskRegistry.addTask(task)
	return obj
}

func (obj *Tasker) RegisterTaskPacket(packet ITaskPacket) *Tasker {
	obj.taskRegistry.addTaskPacket(packet)
	return obj
}

func (obj *Tasker) RegisterQueue(q Queue) *Tasker {
	obj.queueRegistry.addQueue(q)
	return obj
}

func (obj *Tasker) QueueKey(q Queue) string {
	return fmt.Sprintf("%s-%s", obj.keyPrefix, q.Name)
}

func (obj *Tasker) TaskKey(q Queue, td ITaskPacket) string {
	return fmt.Sprintf("%s-%s-%s", obj.keyPrefix, q.Name, td.Id())
}

func (obj *Tasker) DelayedTaskLoop(ctx context.Context) error {

	for {

		qIdx := -1
		looped := false
		receivedTask := false
		for {

			if looped {
				break
			}
			if ctx.Err() != nil {
				return errs.NewStackError(ctx.Err())
			}

			qIdx++

			q, distToEnd, err := obj.queueRegistry.nextQueue(qIdx)
			if err != nil {
				return errs.Wrap(err)
			}

			if distToEnd == 0 {
				looped = true
			}
			if q.Type != DelayedQueue {
				continue
			}

			tp, err := obj.claimNextDelayedTask(ctx, q)
			if errors.Is(err, ErrTaskNotAvailable) {
				continue
			} else if err != nil {
				return errs.Wrap(err)
			}
			receivedTask = true

			obj.logger.Debug(fmt.Sprintf("processing task packet: %s", tp.Name()))

			t, err := obj.taskRegistry.findTask(tp.TaskName())
			if err != nil {
				return errs.Wrap(err)
			}

			pCtx, pCtxCancel := context.WithTimeout(ctx, obj.taskTimeout)

			res, tErr := t.Process(pCtx, tp)
			pCtxCancel()
			if tErr != nil {
				// WILL HANDLE RETRIES LATER
				obj.logger.Error("task failed to process", slog.String("error", tErr.Error()))
				continue
			}

			err = obj.handleDelayedTaskResult(ctx, q, tp, res)
			if err != nil {
				return errs.Wrap(err)
			}

		}

		// slows down processing when there aren't any tasks
		// in any of the queues
		if !receivedTask {
			time.Sleep(50 * time.Millisecond)
		}

	}

}

func (obj *Tasker) handleDelayedTaskResult(ctx context.Context, q Queue, tp ITaskPacket, res Result) error {

	if res.Requeue {

		_, err := obj.DelayTask(ctx, tp, q.Name, 0*time.Second, false)
		if err != nil {
			return errs.Wrap(err)
		}

		return nil

	} else {

		_, err := obj.removeDelayedTask(ctx, q, tp)
		if err != nil {
			return errs.Wrap(err)
		}

		return nil

	}

}

/*
Remove the delayed task if it hasn't been requeued.
*/
func (obj *Tasker) removeDelayedTask(ctx context.Context, q Queue, tp ITaskPacket) (bool, error) {

	script := `
  local exists = redis.call('ZSCORE', KEYS[1], ARGV[1])

  if not exists then
    redis.call('DEL', ARGV[1])
    return 'REMOVED'
  end

  return 'KEPT'
  `

	qKey := obj.QueueKey(q)
	tKey := obj.TaskKey(q, tp)
	cmd := obj.client.Eval(ctx, script, []string{qKey}, tKey)
	val, err := cmd.Val(), cmd.Err()
	if err != nil {
		return false, errs.NewStackError(err)
	}

	return val == "REMOVED", nil

}

func (obj *Tasker) claimNextDelayedTask(ctx context.Context, q Queue) (ITaskPacket, error) {

	// - Arguments:
	// -- KEYS[1] - Sorted set key
	// -- ARGV[1] - Currnet UNIX timestamp in milliseconds
	script := `
  local oldest = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, 1)

  if #oldest == 0 then
    return "NO_ITEMS"
  end

  redis.call('ZREM', KEYS[1], oldest[1])

  local name = redis.call('HGET', oldest[1], 'name')
  local data = redis.call('HGET', oldest[1], 'data')

  return { name, data }
  `
	qKey := obj.QueueKey(q)
	cTs := time.Now().UTC().UnixMilli()
	cmd := obj.client.Eval(ctx, script, []string{qKey}, cTs)
	val, err := cmd.Val(), cmd.Err()
	if err != nil {
		return nil, errs.NewStackError(err)
	}

	// Check if no items were found
	if val == "NO_ITEMS" {
		return nil, errs.NewStackError(ErrTaskNotAvailable)
	}

	result, ok := val.([]interface{})
	if !ok {
		return nil, errs.NewStackError(fmt.Errorf("%w| could not cast to []interface{}", ErrKeyDBResponseInvalid))
	}
	if len(result) != 2 {
		return nil, errs.NewStackError(fmt.Errorf("%w| received %d items", ErrKeyDBResponseInvalid, len(result)))
	}

	packetName, ok := result[0].(string)
	if !ok {
		return nil, errs.NewStackError(fmt.Errorf("%w| packet name was not a string", ErrKeyDBResponseInvalid))
	}

	taskData, ok := result[1].(string)
	if !ok {
		return nil, errs.NewStackError(fmt.Errorf("%w| task data was not a []byte", ErrKeyDBResponseInvalid))
	}

	tp, err := obj.taskRegistry.buildTaskPacket(packetName, []byte(taskData))
	if err != nil {
		return nil, errs.Wrap(err, fmt.Errorf("."))
	}

	return tp, nil

}

func (obj *Tasker) delayTask(
	ctx context.Context,
	tp ITaskPacket,
	q Queue,
	delay time.Duration,
	replace bool,
) (bool, error) {

	replaceNum := 0
	if replace {
		replaceNum = 1
	}

	data, err := tp.Marshal()
	if err != nil {
		return false, errs.Wrap(err)
	}

	// -- Arguments:
	// -- KEYS[1] - Sorted set key
	// -- KEYS[2] - Hash key
	// -- ARGV[1] - Sorted set member
	// -- ARGV[2] - Score for the sorted set
	// -- ARGV[3] - Value for the "id" field in the hash
	// -- ARGV[4] - Value for the "name" field in the hash
	// -- ARGV[5] - Value for the "data" field in the hash
	// -- ARGV[6] - Allow overwrites (0 - false, 1 - true)
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
	tKey := obj.TaskKey(q, tp)
	cTs := time.Now().UTC().Add(delay).UnixMilli()
	cmd := obj.client.Eval(
		ctx,
		script,
		[]string{qKey, tKey},
		tKey,
		cTs,
		tp.Id(),
		tp.Name(),
		data,
		replaceNum,
	)
	val, err := cmd.Val(), cmd.Err()
	if err != nil {
		return false, errs.NewStackError(err)
	}

	return val == "ADDED", nil

}

/*
Add a task to the provided queue. If the queue is not a delayed queue then
return an error.
*/
func (obj *Tasker) DelayTask(
	ctx context.Context,
	td ITaskPacket,
	queue string,
	delay time.Duration,
	replace bool,
) (bool, error) {

	q, err := obj.queueRegistry.findQueue(queue)
	if err != nil {
		return false, errs.Wrap(err)
	}
	if q.Type != DelayedQueue {
		return false, errs.NewStackError(ErrQueueTypeInvalidForOperation)
	}

	return obj.delayTask(ctx, td, q, delay, replace)

}
