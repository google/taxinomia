/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package models

import (
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

type DataModel struct {
	// key is table name "." column name
	tables map[string]*tables.DataTable

	// entity type to list of table.column
	columnsByEntityType map[string][]TableColumnRef
	// language -> list of table.column that specify languages
	// canton -> list of table.column that specify cantons
}

// NewDataModel creates a new DataModel instance
func NewDataModel() *DataModel {
	return &DataModel{
		tables:              make(map[string]*tables.DataTable),
		columnsByEntityType: make(map[string][]TableColumnRef),
	}
}

// AddTable adds a table to the data model and automatically registers entity types
func (dm *DataModel) AddTable(name string, table *tables.DataTable) {
	dm.tables[name] = table

	// Automatically register entity types for all columns in the table
	columnNames := table.GetColumnNames()
	for _, colName := range columnNames {
		col := table.GetColumn(colName)
		if col != nil {
			entityType := col.ColumnDef().EntityType()
			if entityType != "" {
				dm.columnsByEntityType[entityType] = append(dm.columnsByEntityType[entityType], TableColumnRef{
					TableName:  name,
					ColumnName: colName,
				})
			}
		}
	}
}

// GetTable returns a table by name
func (dm *DataModel) GetTable(name string) *tables.DataTable {
	return dm.tables[name]
}

// GetAllTables returns all tables in the data model
func (dm *DataModel) GetAllTables() map[string]*tables.DataTable {
	return dm.tables
}

// GetColumnsByEntityType returns all columns for a specific entity type
func (dm *DataModel) GetColumnsByEntityType(entityType string) []columns.IDataColumn {
	var columns []columns.IDataColumn

	// Get the table/column references for this entity type
	refs := dm.columnsByEntityType[entityType]
	for _, ref := range refs {
		if table := dm.tables[ref.TableName]; table != nil {
			if col := table.GetColumn(ref.ColumnName); col != nil {
				columns = append(columns, col)
			}
		}
	}

	return columns
}

// EntityTypeUsage represents where an entity type is used
type EntityTypeUsage struct {
	EntityType string
	Usage      []TableColumnRef
}

// TableColumnRef represents a reference to a table and column
type TableColumnRef struct {
	TableName  string
	ColumnName string
}

// GetAllEntityTypes returns all entity types and their usage across tables
func (dm *DataModel) GetAllEntityTypes() []EntityTypeUsage {
	var result []EntityTypeUsage

	// Simply convert the columnsByEntityType map to the result format
	for entityType, usage := range dm.columnsByEntityType {
		result = append(result, EntityTypeUsage{
			EntityType: entityType,
			Usage:      usage,
		})
	}

	return result
}
