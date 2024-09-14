package tasker

import "github.com/alekLukanen/errs"

const (
	DelayedQueue = "delayed"
)

type Queue struct {
	Name string
	Type string
}

type queueRegistry struct {
	queues []Queue
}

func newQueueRegistry() *queueRegistry {
	return &queueRegistry{
		queues: make([]Queue, 0),
	}
}

func (obj *queueRegistry) addQueue(q Queue) {
	obj.queues = append(obj.queues, q)
}

func (obj *queueRegistry) findQueue(name string) (Queue, error) {
	for _, q := range obj.queues {
		if q.Name == name {
			return q, nil
		}
	}
	return Queue{}, errs.NewStackError(ErrQueueNotFoundInRegistry)
}

func (obj *queueRegistry) nextQueue(idx int) (Queue, int, error) {
	if len(obj.queues) == 0 {
		return Queue{}, 0, errs.NewStackError(ErrQueueRegistryIsEmpty)
	}
	qLen := len(obj.queues)
	qMod := idx % len(obj.queues)
	return obj.queues[qMod], qLen - qMod - 1, nil
}
