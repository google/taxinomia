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
	"sort"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// System table name constants
const (
	ColumnsTableName = "_columns"
)

// BuildColumnsTable creates a system table containing metadata about all columns
// in the DataModel. Each row represents one column from any table.
//
// Schema:
//   - table_name: string - The table this column belongs to
//   - column_name: string - The column's internal name
//   - display_name: string - The column's display name
//   - data_type: string - The data type ("string" or "uint32")
//   - entity_type: string - The entity type for joins (empty if none)
//   - is_key: string - "true" if column contains unique values, "false" otherwise
//   - row_count: uint32 - Number of rows in the column
//   - position: uint32 - Column index within the table
func BuildColumnsTable(dm *DataModel) *tables.DataTable {
	// Create columns for the _columns table
	tableNameCol := columns.NewStringColumn(columns.NewColumnDef("table_name", "Table", ""))
	columnNameCol := columns.NewStringColumn(columns.NewColumnDef("column_name", "Column", ""))
	displayNameCol := columns.NewStringColumn(columns.NewColumnDef("display_name", "Display Name", ""))
	dataTypeCol := columns.NewStringColumn(columns.NewColumnDef("data_type", "Data Type", ""))
	entityTypeCol := columns.NewStringColumn(columns.NewColumnDef("entity_type", "Entity Type", ""))
	isKeyCol := columns.NewStringColumn(columns.NewColumnDef("is_key", "Is Key", ""))
	rowCountCol := columns.NewUint32Column(columns.NewColumnDef("row_count", "Row Count", ""))
	positionCol := columns.NewUint32Column(columns.NewColumnDef("position", "Position", ""))

	// Get all tables and sort by name for consistent ordering
	allTables := dm.GetAllTables()
	tableNames := make([]string, 0, len(allTables))
	for name := range allTables {
		// Skip system tables
		if isSystemTable(name) {
			continue
		}
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	// Iterate through all tables and columns
	for _, tableName := range tableNames {
		table := allTables[tableName]
		colNames := table.GetColumnNames()

		// Sort column names for consistent ordering
		sort.Strings(colNames)

		for position, colName := range colNames {
			col := table.GetColumn(colName)
			if col == nil {
				continue
			}

			colDef := col.ColumnDef()

			// Determine data type
			dataType := getColumnType(col)

			// Determine is_key as string
			isKey := "false"
			if col.IsKey() {
				isKey = "true"
			}

			// Append row data
			tableNameCol.Append(tableName)
			columnNameCol.Append(colName)
			displayNameCol.Append(colDef.DisplayName())
			dataTypeCol.Append(dataType)
			entityTypeCol.Append(colDef.EntityType())
			isKeyCol.Append(isKey)
			rowCountCol.Append(uint32(col.Length()))
			positionCol.Append(uint32(position))
		}
	}

	// Finalize columns to set isKey flags
	tableNameCol.FinalizeColumn()
	columnNameCol.FinalizeColumn()
	displayNameCol.FinalizeColumn()
	dataTypeCol.FinalizeColumn()
	entityTypeCol.FinalizeColumn()
	isKeyCol.FinalizeColumn()
	rowCountCol.FinalizeColumn()
	positionCol.FinalizeColumn()

	// Create and populate the table
	columnsTable := tables.NewDataTable()
	columnsTable.AddColumn(tableNameCol)
	columnsTable.AddColumn(columnNameCol)
	columnsTable.AddColumn(displayNameCol)
	columnsTable.AddColumn(dataTypeCol)
	columnsTable.AddColumn(entityTypeCol)
	columnsTable.AddColumn(isKeyCol)
	columnsTable.AddColumn(rowCountCol)
	columnsTable.AddColumn(positionCol)

	return columnsTable
}

// getColumnType returns the type name for a column
func getColumnType(col columns.IDataColumn) string {
	switch col.(type) {
	case *columns.StringColumn:
		return "string"
	case *columns.Uint32Column:
		return "uint32"
	case *columns.Int64Column:
		return "int64"
	case *columns.Uint64Column:
		return "uint64"
	case *columns.Float64Column:
		return "float64"
	case *columns.BoolColumn:
		return "bool"
	case *columns.DatetimeColumn:
		return "datetime"
	case *columns.DurationColumn:
		return "duration"
	default:
		return "unknown"
	}
}

// isSystemTable returns true if the table name is a system table
func isSystemTable(name string) bool {
	return name == ColumnsTableName
}

// AddSystemTables creates and adds all system tables to the DataModel.
// This should be called after all user tables have been added.
func AddSystemTables(dm *DataModel) {
	columnsTable := BuildColumnsTable(dm)
	dm.AddTable(ColumnsTableName, columnsTable)
}
