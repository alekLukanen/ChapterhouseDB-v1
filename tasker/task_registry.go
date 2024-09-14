package tasker

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

	return nil, ErrTaskNotFoundInRegistry

}
