package tasker

import (
	"fmt"

	"github.com/alekLukanen/errs"
)

type taskRegistry struct {
	tasks []ITask
}

func newTaskRegistry() *taskRegistry {
	return &taskRegistry{
		tasks: make([]ITask, 0),
	}
}

func (obj *taskRegistry) addTask(task ITask) {
	obj.tasks = append(obj.tasks, task)
}

func (obj *taskRegistry) findTask(name string) (ITask, error) {

	for _, t := range obj.tasks {
		if t.Name() == name {
			return t, nil
		}
	}

	return nil, errs.NewStackError(fmt.Errorf("%w| task name %s", ErrTaskNotFoundInRegistry, name))

}

func (obj *taskRegistry) buildTaskData(name string, data []byte) (ITaskData, error) {

	t, err := obj.findTask(name)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	td := t.NewData()
	err = td.Unmarshal(data)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return td, nil

}
