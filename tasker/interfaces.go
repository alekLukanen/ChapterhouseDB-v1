package tasker

type ITask interface {
	Id() string
	Name() string
	Bldr() func() ITask
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}
