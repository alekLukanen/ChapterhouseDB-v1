package tasker

import "context"

type ITask interface {
	Name() string
	Process(context.Context, ITaskPacket) (Result, error)
}

type ITaskPacket interface {
	Id() string
	Name() string
	New() ITaskPacket
	TaskName() string

	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type Result struct {
	Requeue bool
}
