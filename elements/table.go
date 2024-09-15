package elements

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
)

type TableOptions struct {
	BatchProcessingDelay time.Duration
	BatchProcessingSize  int

	// partitioning options
	MaxObjectSize int
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
		options: TableOptions{
			BatchProcessingDelay: 10 * time.Second,
			BatchProcessingSize:  1000,
			MaxObjectSize:        10_000,
		},
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

func (obj *Table) IsValid() error {
	if obj.name == "" {
		return fmt.Errorf("%w| name invalid", ErrTableInvalid)
	}

	if len(obj.columns) == 0 {
		return fmt.Errorf("%w| table does not have columns", ErrTableInvalid)
	}

	for _, col := range obj.columns {
		if !col.IsValid() {
			return fmt.Errorf("%w| table has invalid column", ErrTableInvalid)
		}
	}

	// each partition column is uniq
	uniqPartitionColumns := make(map[string]Column)
	for _, colPart := range obj.columnPartitions {
		col, err := obj.GetColumnByName(colPart.columnName)
		if err != nil {
			return fmt.Errorf("%w| partition column is not a column in the table", ErrTableInvalid)
		}
		uniqPartitionColumns[colPart.columnName] = col
	}
	if len(uniqPartitionColumns) < len(obj.columnPartitions) {
		return fmt.Errorf("%w| duplicate partition columns", ErrTableInvalid)
	}

	for _, subGroup := range obj.subscriptionGroups {
		if !subGroup.IsValid() {
			return fmt.Errorf("%w| subscription group invalid", ErrTableInvalid)
		}
		for _, sub := range subGroup.subscriptions {
			subCols := sub.Columns()
			// must have at least the same number of tuple columns as column partitions
			if len(subCols) < len(obj.columnPartitions) {
				return fmt.Errorf("%w| subscription does not have all partition columns", ErrTableInvalid)
			}
			// all subscriptions must have the partition columns
			partColNum := 0
			for _, subCol := range subCols {
				if colPart, ok := uniqPartitionColumns[subCol.Name]; ok {
					if subCol.Dtype != colPart.Dtype {
						return fmt.Errorf("%w| partition column must have the same type between subscriptions and table", ErrTableInvalid)
					}
					partColNum++
				}
			}
			if partColNum < len(obj.columnPartitions) {
				return fmt.Errorf("%w| subscription does not have all partition columns", ErrTableInvalid)
			}
		}
	}

	return nil
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

func NewExternalSubscription(externalSource string, transformer Transformer, columns []Column) *ExternalSubscription {
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
	if len(obj.columns) == 0 {
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
	TableName              string `json:"table_name"`
	SubscriptionSourceName string `json:"subscription_source_name"`
	Key                    string `json:"key"`
}
