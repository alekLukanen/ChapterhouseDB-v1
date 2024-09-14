package tasker

type nameToBldr struct {
	name string
	bldr func() ITask
}

type taskRegistry struct {
	taskBldrs []nameToBldr
}

func newTaskRegistry() *taskRegistry {
	return &taskRegistry{
		taskBldrs: make([]nameToBldr, 0),
	}
}

func (obj *taskRegistry) addTask(name string, bldr func() ITask) {
	obj.taskBldrs = append(obj.taskBldrs, nameToBldr{name: name, bldr: bldr})
}

func (obj *taskRegistry) findTaskBldr(name string) (func() ITask, error) {

	for _, ntb := range obj.taskBldrs {
		if ntb.name == name {
			return ntb.bldr, nil
		}
	}

	return nil, ErrTaskNotFoundInRegistry

}
