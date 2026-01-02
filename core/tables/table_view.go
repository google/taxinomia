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

package tables

import (
	"fmt"
	"strings"

	"github.com/google/taxinomia/core/columns"
)

// JoinResolver is an interface for resolving join and table information
// The GetJoin method returns an object that has a GetJoiner() method
type JoinResolver interface {
	GetJoin(key string) interface{} // Returns object with GetJoiner() columns.IJoiner method
	GetTable(name string) *DataTable
}

// TableView represents a view of a DataTable with additional joined columns.
// This allows the underlying DataTable to remain immutable while views can
// have their own set of joined columns based on the query context.
type TableView struct {
	baseTable *DataTable
	tableName string
	joins     map[string]columns.IJoinedDataColumn
}

// NewTableView creates a new TableView wrapping a DataTable
func NewTableView(baseTable *DataTable, tableName string) *TableView {
	return &TableView{
		baseTable: baseTable,
		tableName: tableName,
		joins:     make(map[string]columns.IJoinedDataColumn),
	}
}

// GetBaseTable returns the underlying immutable DataTable
func (tv *TableView) GetBaseTable() *DataTable {
	return tv.baseTable
}

// AddJoinedColumn adds a joined column to this view
func (tv *TableView) AddJoinedColumn(joinedCol columns.IJoinedDataColumn) {
	tv.joins[joinedCol.ColumnDef().Name()] = joinedCol
}

// RemoveJoinedColumn removes a joined column from this view
func (tv *TableView) RemoveJoinedColumn(name string) {
	delete(tv.joins, name)
}

// GetColumn retrieves a column by name, checking both base table and joined columns
func (tv *TableView) GetColumn(name string) columns.IDataColumn {
	// First check base table columns
	if col := tv.baseTable.GetColumn(name); col != nil {
		return col
	}
	// Then check view's joined columns
	if col, ok := tv.joins[name]; ok {
		return col
	}
	return nil
}

// GetColumnNames returns column names from the base table only
func (tv *TableView) GetColumnNames() []string {
	return tv.baseTable.GetColumnNames()
}

// GetAllColumnNames returns all column names including joined columns in this view
func (tv *TableView) GetAllColumnNames() []string {
	names := make([]string, 0, len(tv.baseTable.columns)+len(tv.joins))

	// Add regular columns from base table
	for name := range tv.baseTable.columns {
		names = append(names, name)
	}

	// Add joined columns from this view
	for name := range tv.joins {
		names = append(names, name)
	}

	return names
}

// GetJoinedColumnNames returns only joined column names in this view
func (tv *TableView) GetJoinedColumnNames() []string {
	names := make([]string, 0, len(tv.joins))
	for name := range tv.joins {
		names = append(names, name)
	}
	return names
}

// UpdateJoinedColumns updates the joined columns in this view to match the requested columns
// It adds new joined columns and removes ones that are no longer needed
// Joined columns are identified by the format: fromColumn.toTable.toColumn.selectedColumn
func (tv *TableView) UpdateJoinedColumns(columnNames []string, resolver JoinResolver) {
	// Debug: Print processing info
	fmt.Printf("\n=== UpdateJoinedColumns Debug Info ===\n")
	fmt.Printf("Table: %s\n", tv.tableName)
	fmt.Printf("Columns to process: %v\n", columnNames)

	// Track which joined columns we need
	neededJoinedColumns := make(map[string]bool)

	// Parse columns to identify joined ones (with dots)
	for _, colName := range columnNames {
		if strings.Contains(colName, ".") {
			// This is a joined column - format: fromColumn.toTable.toColumn.selectedColumn
			parts := strings.Split(colName, ".")
			if len(parts) == 4 {
				neededJoinedColumns[colName] = true
			}
		}
	}

	// Remove joined columns that are no longer needed
	currentJoinedColumns := tv.GetJoinedColumnNames()
	for _, colName := range currentJoinedColumns {
		if !neededJoinedColumns[colName] {
			fmt.Printf("Removing joined column %s from table view\n", colName)
			tv.RemoveJoinedColumn(colName)
		}
	}

	// Add needed joined columns that aren't already in the view
	for colName := range neededJoinedColumns {
		// Skip if already exists
		if tv.joins[colName] != nil {
			continue
		}
		// Parse the column name
		parts := strings.Split(colName, ".")
		if len(parts) != 4 {
			continue
		}

		fromColumn := parts[0]
		toTable := parts[1]
		toColumn := parts[2]
		selectedColumn := parts[3]

		// Find the join that connects these tables
		// Build the join key to look up directly
		joinKey := fmt.Sprintf("%s.%s->%s.%s", tv.tableName, fromColumn, toTable, toColumn)
		foundJoin := resolver.GetJoin(joinKey)

		if foundJoin != nil {
			// Create the joined column
			targetTable := resolver.GetTable(toTable)
			if targetTable != nil {
				targetDataCol := targetTable.GetColumn(selectedColumn)
				if targetDataCol != nil {
					colDef := columns.NewColumnDef(
						colName,
						fmt.Sprintf("%s %s", toTable, targetDataCol.ColumnDef().DisplayName()),
						"", // Joined columns don't have entity types
					)

					// Get the joiner from the join info using type assertion
					// We expect the join object to have a GetJoiner() method
					type JoinWithJoiner interface {
						GetJoiner() columns.IJoiner
					}

					if joinWithJoiner, ok := foundJoin.(JoinWithJoiner); ok {
						joiner := joinWithJoiner.GetJoiner()
						joinedColumn := targetDataCol.CreateJoinedColumn(colDef, joiner)

						fmt.Printf("Adding joined column %s to table view\n", colName)
						tv.AddJoinedColumn(joinedColumn)
					}
				}
			}
		}
	}

	// Debug: Print final state
	fmt.Printf("Joined Columns in TableView: %v\n", tv.GetJoinedColumnNames())
	fmt.Printf("All Columns in TableView: %v\n", tv.GetAllColumnNames())
	fmt.Printf("===============================================\n\n")
}
