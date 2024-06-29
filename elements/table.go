package elements

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
)

type TableOptions struct {
	BatchProcessingDelay time.Duration
	BatchProcessingSize  int
}

type IValidatable interface {
	IsValid() bool
}
type ISubscription interface {
	SourceName() string
	Columns() []Column
	Transformer() Transformer
	IValidatable
}
type Table struct {
	name               string
	columns            []Column
	columnPartitions   []ColumnPartition
	subscriptionGroups []SubscriptionGroup
	options            TableOptions
}

func NewTable(name string) *Table {
	return &Table{
		name:               name,
		columns:            []Column{},
		columnPartitions:   []ColumnPartition{},
		subscriptionGroups: []SubscriptionGroup{},
		options:            TableOptions{},
	}
}

func (obj *Table) TableName() string {
	return obj.name
}

func (obj *Table) Columns() []Column {
	return obj.columns
}

func (obj *Table) ColumnPartitions() []ColumnPartition {
	return obj.columnPartitions
}

func (obj *Table) SubscriptionGroups() []SubscriptionGroup {
	return obj.subscriptionGroups
}

func (obj *Table) AddColumns(columns ...Column) *Table {
	obj.columns = append(obj.columns, columns...)
	return obj
}

func (obj *Table) SetOptions(options TableOptions) *Table {
	obj.options = options
	return obj
}

func (obj *Table) Options() TableOptions {
	return obj.options
}

func (obj *Table) AddColumnPartitions(partitions ...ColumnPartition) *Table {
	obj.columnPartitions = append(obj.columnPartitions, partitions...)
	return obj
}

func (obj *Table) AddSubscriptionGroups(groups ...SubscriptionGroup) *Table {
	obj.subscriptionGroups = append(obj.subscriptionGroups, groups...)
	return obj
}

func (obj *Table) GetSubscriptionBySourceName(sourceName string) (ISubscription, error) {
	for _, subGroup := range obj.subscriptionGroups {
		for _, sub := range subGroup.subscriptions {
			if sub.SourceName() == sourceName {
				return sub, nil
			}
		}
	}
	return nil, ErrSubscriptionNotFound
}

func (obj *Table) IsValid() bool {
	if obj.name == "" {
		return false
	}

	if len(obj.columns) == 0 {
		return false
	}

	for _, col := range obj.columns {
		if !col.IsValid() {
			return false
		}
	}

	for _, subGroup := range obj.subscriptionGroups {
		if !subGroup.IsValid() {
			return false
		}
		for _, sub := range subGroup.subscriptions {
			subCols := sub.Columns()
			// must have at least the same number of tuple columns as column partitions
			if len(subCols) < len(obj.columnPartitions) {
				return false
			}
			// iterate over the common tuple columns and partition columns
			// all tuple columns after the partitioned columns can be any type
			for colIdx, subCol := range subCols[:len(obj.columnPartitions)] {
				col, err := obj.GetColumnByName(obj.columnPartitions[colIdx].columnName)
				if err != nil {
					return false
				}
				if subCol.Dtype != col.Dtype {
					return false
				}
			}
		}
	}

	return true
}

func (obj *Table) GetColumnByName(name string) (Column, error) {
	for _, col := range obj.columns {
		if col.Name == name {
			return col, nil
		}
	}
	return Column{}, ErrColumnNotFound
}

////////////////////////////////////////

type Column struct {
	Name  string
	Dtype arrow.DataType
}

func NewColumn(name string, dtype arrow.DataType) Column {
	return Column{
		Name:  name,
		Dtype: dtype,
	}
}

func (obj *Column) IsValid() bool {
	if obj.Name == "" {
		return false
	}

	if obj.Dtype == nil {
		return false
	}
	return true
}

////////////////////////////////////////

type SubscriptionGroup struct {
	name string
	// each subscription must have the same types in the same order
	subscriptions []ISubscription
}

func NewSubscriptionGroup(name string) SubscriptionGroup {
	return SubscriptionGroup{
		name: name,
	}
}

func (obj SubscriptionGroup) AddSubscriptions(subs ...ISubscription) SubscriptionGroup {
	obj.subscriptions = append(obj.subscriptions, subs...)
	return obj
}

func (obj *SubscriptionGroup) IsValid() bool {
	if obj.name == "" {
		return false
	}
	if len(obj.subscriptions) == 0 {
		return false
	}
	for _, sub := range obj.subscriptions {
		if !sub.IsValid() {
			return false
		}
	}
	return true
}

// //////////////////////////////////////

type ExternalSubscription struct {
	externalSource string
	columns        []Column
	transformer    Transformer
}

func NewExternalSubscription(externalSource string, transformer Transformer, columns ...Column) *ExternalSubscription {
	return &ExternalSubscription{
		externalSource: externalSource,
		transformer:    transformer,
		columns:        columns,
	}
}

func (obj *ExternalSubscription) IsValid() bool {
	if obj.externalSource == "" {
		return false
	}
	return true
}

func (obj *ExternalSubscription) SourceName() string {
	return fmt.Sprintf("external.%s", obj.externalSource)
}

func (obj *ExternalSubscription) Columns() []Column {
	return obj.columns
}

func (obj *ExternalSubscription) Transformer() Transformer {
	return obj.transformer
}

////////////////////////////////////////

type IPartitionOptions interface {
	PartitionType() string
	PartitionMetaData() map[string]string
	PartitionFunc() PartitionFunc
}
type ColumnPartition struct {
	columnName       string
	partitionOptions IPartitionOptions
}

func NewColumnPartition(columnName string, partitionOptions IPartitionOptions) ColumnPartition {
	return ColumnPartition{
		columnName:       columnName,
		partitionOptions: partitionOptions,
	}
}

func (obj ColumnPartition) Name() string {
	return obj.columnName
}
func (obj ColumnPartition) Options() IPartitionOptions {
	return obj.partitionOptions
}

////////////////////////////////////////

type Partition struct {
	TableName              string
	SubscriptionSourceName string
	Key                    string
}
