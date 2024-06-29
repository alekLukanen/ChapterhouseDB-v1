package operations

import (
	"context"
	"log/slog"

	"github.com/alekLukanen/chapterhouseDB/elements"
)

type ITableRegistry interface {
	AddTables(tables ...*elements.Table) error
	GetTable(tableName string) (*elements.Table, error)
	TableExists(tableName string) bool
	Tables() []*elements.Table
}

type TableRegistry struct {
	logger *slog.Logger

	tables map[string]*elements.Table
}

func NewTableRegistry(ctx context.Context, logger *slog.Logger) *TableRegistry {
	return &TableRegistry{
		logger: logger,
		tables: make(map[string]*elements.Table),
	}
}

func (obj *TableRegistry) AddTables(tables ...*elements.Table) error {
	for _, table := range tables {
		if _, exists := obj.tables[table.TableName()]; exists {
			return ErrTableAlreadyAddedToRegistry
		}
		obj.tables[table.TableName()] = table
	}
	return nil
}

func (obj *TableRegistry) GetTable(tableName string) (*elements.Table, error) {
	table, exists := obj.tables[tableName]
	if !exists {
		return nil, ErrTableNotFound
	}
	return table, nil
}

func (obj *TableRegistry) TableExists(tableName string) bool {
	_, exists := obj.tables[tableName]
	return exists
}

func (obj *TableRegistry) Tables() []*elements.Table {
	tables := make([]*elements.Table, 0, len(obj.tables))

	var idx int
	for _, table := range obj.tables {
		tables[idx] = table
		idx++
	}

	return tables
}
