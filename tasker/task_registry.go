package tasker

import (
	"fmt"

	"github.com/alekLukanen/errs"
)

type taskRegistry struct {
	tasks       []ITask
	taskPackets []ITaskPacket
}

func newTaskRegistry() *taskRegistry {
	return &taskRegistry{
		tasks:       make([]ITask, 0),
		taskPackets: make([]ITaskPacket, 0),
	}
}

func (obj *taskRegistry) addTask(task ITask) {
	for idx, t := range obj.tasks {
		if t.Name() == task.Name() {
			obj.tasks[idx] = task
			return
		}
	}

	obj.tasks = append(obj.tasks, task)
}

func (obj *taskRegistry) addTaskPacket(packet ITaskPacket) {
	for idx, tp := range obj.taskPackets {
		if tp.Name() == packet.Name() {
			obj.taskPackets[idx] = packet
			return
		}
	}

	obj.taskPackets = append(obj.taskPackets, packet)
}

func (obj *taskRegistry) findTask(name string) (ITask, error) {

	for _, t := range obj.tasks {
		if t.Name() == name {
			return t, nil
		}
	}

	return nil, errs.NewStackError(fmt.Errorf("%w| task name %s", ErrNotFoundInRegistry, name))

}

func (obj *taskRegistry) findTaskPacket(name string) (ITaskPacket, error) {

	for _, t := range obj.taskPackets {
		if t.Name() == name {
			return t, nil
		}
	}

	return nil, errs.NewStackError(fmt.Errorf("%w| task Packet name %s", ErrNotFoundInRegistry, name))

}

func (obj *taskRegistry) buildTaskPacket(name string, data []byte) (ITaskPacket, error) {

	t, err := obj.findTaskPacket(name)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	td := t.New()
	err = td.Unmarshal(data)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return td, nil

}
