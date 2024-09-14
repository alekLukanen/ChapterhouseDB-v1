package tasker

type ITask interface {
	Name() string
	NewData() ITaskData
	Process(ITaskData) error
}

type ITaskData interface {
	Id() string
	TaskName() string

	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}
