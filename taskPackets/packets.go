package taskpackets

import (
	"encoding/json"
	"fmt"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/alekLukanen/ChapterhouseDB/tasker"
)

const (
	TablePartitionTaskName = "table-partition-task"
)

type TablePartitionTaskPacket struct {
	Partition elements.Partition
}

func (obj *TablePartitionTaskPacket) Id() string {
	return fmt.Sprintf(
		"%s-%s-%s",
		obj.Partition.TableName,
		obj.Partition.SubscriptionSourceName,
		obj.Partition.Key)
}
func (obj *TablePartitionTaskPacket) Name() string { return "table-partition-task-packet" }
func (obj *TablePartitionTaskPacket) TaskName() string {
	return TablePartitionTaskName
}
func (obj *TablePartitionTaskPacket) New() tasker.ITaskPacket { return &TablePartitionTaskPacket{} }
func (obj *TablePartitionTaskPacket) Marshal() ([]byte, error) {
	return json.Marshal(obj)
}
func (obj *TablePartitionTaskPacket) Unmarshal(d []byte) error {
	return json.Unmarshal(d, obj)
}
