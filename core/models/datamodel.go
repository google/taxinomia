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
	"fmt"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// Join represents a relationship between columns in different tables
// type Join struct {
// 	// Source table and column
// 	FromTable  string
// 	FromColumn string

// 	// Target table and column
// 	ToTable  string
// 	ToColumn string

// 	// The entity type that connects these columns
// 	EntityType string
// }

// // NewJoin creates a new join definition
// func NewJoin(fromTable, fromColumn, toTable, toColumn, entityType string) *Join {
// 	return &Join{
// 		FromTable:  fromTable,
// 		FromColumn: fromColumn,
// 		ToTable:    toTable,
// 		ToColumn:   toColumn,
// 		EntityType: entityType,
// 	}
// }

// String returns a string representation of the join
func (j *Join) String() string {
	return fmt.Sprintf("%s (via %s)", j.Key, j.EntityType)
}

type DataModel struct {
	// key is table name "." column name
	tables map[string]*tables.DataTable

	// entity type to list of table.column
	columnsByEntityType map[string][]TableColumnRef
	// language -> list of table.column that specify languages
	// canton -> list of table.column that specify cantons

	// joins between columns in different tables
	// key is the join key (e.g., "orders.region->regions.region")
	joins map[string]*Join
}

// NewDataModel creates a new DataModel instance
func NewDataModel() *DataModel {
	return &DataModel{
		tables:              make(map[string]*tables.DataTable),
		columnsByEntityType: make(map[string][]TableColumnRef),
		joins:               make(map[string]*Join),
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

	// Auto-discover and update joins after adding the table
	dm.discoverJoins()
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

// RegisterJoin adds a join relationship between columns in different tables
func (dm *DataModel) RegisterJoin(join *Join) error {
	// When using automatic discovery, validation is redundant since the discovery
	// algorithm already enforces all the same rules that the validator checks.

	// Add the join to our registry
	dm.joins[join.Key] = join
	return nil
}

// GetJoins returns all registered joins
func (dm *DataModel) GetJoins() []*Join {
	joins := make([]*Join, 0, len(dm.joins))
	for _, join := range dm.joins {
		joins = append(joins, join)
	}
	return joins
}

// GetJoin returns a specific join by its key
// Returns nil if the join doesn't exist (not a typed nil)
func (dm *DataModel) GetJoin(key string) interface{} {
	join, exists := dm.joins[key]
	if !exists {
		return nil
	}
	return join
}

// GetJoinsForTable returns all joins that involve the specified table
// func (dm *DataModel) GetJoinsForTable(tableName string) []*Join {
// 	var tableJoins []*Join
// 	for _, join := range dm.joins {
// 		if join.FromTable == tableName || join.ToTable == tableName {
// 			tableJoins = append(tableJoins, join)
// 		}
// 	}
// 	return tableJoins
// }

// GetJoinsForColumn returns all joins that involve the specified column
// func (dm *DataModel) GetJoinsForColumn(tableName, columnName string) []*Join {
// 	var columnJoins []*Join
// 	for _, join := range dm.joins {
// 		if (join.FromTable == tableName && join.FromColumn == columnName) ||
// 			(join.ToTable == tableName && join.ToColumn == columnName) {
// 			columnJoins = append(columnJoins, join)
// 		}
// 	}
// 	return columnJoins
// }

// discoverJoins automatically discovers and registers joins based on entity types and IsKey property
func (dm *DataModel) discoverJoins() {
	// Clear existing joins to re-discover them
	dm.joins = make(map[string]*Join)

	// Get all entity types and their usage
	entityTypes := dm.GetAllEntityTypes()

	// For each entity type, find potential joins
	for _, entityUsage := range entityTypes {
		if entityUsage.EntityType == "" {
			continue // Skip empty entity types
		}

		// Find all columns with this entity type that have IsKey = true
		var keyColumns []TableColumnRef
		for _, ref := range entityUsage.Usage {
			table := dm.GetTable(ref.TableName)
			if table != nil {
				col := table.GetColumn(ref.ColumnName)
				if col != nil && col.IsKey() {
					keyColumns = append(keyColumns, ref)
				}
			}
		}

		// If no key columns, skip this entity type
		if len(keyColumns) == 0 {
			continue
		}

		// For each column with this entity type, create joins to all key columns
		// This includes both non-key columns (foreign keys) and key columns (for chained lookups)
		for _, sourceRef := range entityUsage.Usage {
			sourceTable := dm.GetTable(sourceRef.TableName)
			if sourceTable == nil {
				continue
			}

			sourceCol := sourceTable.GetColumn(sourceRef.ColumnName)
			if sourceCol == nil {
				continue
			}

			// Create joins from this column to all key columns
			for _, targetRef := range keyColumns {
				// Don't create self-joins (same table and column)
				if sourceRef.TableName == targetRef.TableName && sourceRef.ColumnName == targetRef.ColumnName {
					continue
				}

				join := NewJoin(
					sourceRef.TableName,
					sourceRef.ColumnName,
					targetRef.TableName,
					targetRef.ColumnName,
					entityUsage.EntityType,
					dm,
				)

				// Only add the join if a joiner could be created (same column types)
				if join.Joiner != nil {
					dm.joins[join.Key] = join
				}
			}
		}
	}
}

func (dm *DataModel) createJoiner(fromColumn columns.IDataColumn, toColumn columns.IDataColumn) columns.IJoiner {
	// Both columns must be the same type to create a joiner
	switch from := fromColumn.(type) {
	case *columns.StringColumn:
		if to, ok := toColumn.(*columns.StringColumn); ok {
			return &columns.Joiner[string]{
				FromColumn: from,
				ToColumn:   to,
			}
		}
	case *columns.Uint32Column:
		if to, ok := toColumn.(*columns.Uint32Column); ok {
			return &columns.Joiner[uint32]{
				FromColumn: from,
				ToColumn:   to,
			}
		}
	case *columns.Uint64Column:
		if to, ok := toColumn.(*columns.Uint64Column); ok {
			return &columns.Joiner[uint64]{
				FromColumn: from,
				ToColumn:   to,
			}
		}
	case *columns.Int64Column:
		if to, ok := toColumn.(*columns.Int64Column); ok {
			return &columns.Joiner[int64]{
				FromColumn: from,
				ToColumn:   to,
			}
		}
	}
	// Type mismatch or unsupported type - cannot create joiner
	return nil
}

// Join represents a relationship between columns in different tables
type Join struct {
	Key string
	// Source table and column
	FromTable  *tables.DataTable
	FromColumn columns.IDataColumn

	// Target table and column
	ToTable  *tables.DataTable
	ToColumn columns.IDataColumn

	// The entity type that connects these columns
	EntityType string

	Joiner columns.IJoiner
}

// GetJoiner returns the joiner for this join
func (j *Join) GetJoiner() columns.IJoiner {
	return j.Joiner
}

// NewJoin creates a new join definition
func NewJoin(fromTable, fromColumn, toTable, toColumn, entityType string, dm *DataModel) *Join {
	return &Join{
		Key:        fmt.Sprintf("%s.%s->%s.%s", fromTable, fromColumn, toTable, toColumn),
		FromTable:  dm.GetTable(fromTable),
		FromColumn: dm.GetTable(fromTable).GetColumn(fromColumn),
		ToTable:    dm.GetTable(toTable),
		ToColumn:   dm.GetTable(toTable).GetColumn(toColumn),
		Joiner:     dm.createJoiner(dm.GetTable(fromTable).GetColumn(fromColumn), dm.GetTable(toTable).GetColumn(toColumn)),
		EntityType: entityType,
	}
}
