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

package views

import (
	"sort"
	"github.com/google/taxinomia/core/tables"
)

// TableViewModel contains the data from the table formatted for template consumption
type TableViewModel struct {
	Title         string
	Headers       []string              // Column display names
	Columns       []string              // Column names (for data access)
	Rows          []map[string]string   // Each row is a map of column name to value
	AllColumns    []ColumnInfo          // All available columns with metadata
	CurrentQuery  string                // Current query string
}

// ColumnInfo contains information about a column for UI display
type ColumnInfo struct {
	Name        string // Column internal name
	DisplayName string // Column display name
	IsVisible   bool   // Whether column is currently visible
}

// BuildViewModel creates a ViewModel from a Table using the specified View
func BuildViewModel(table *tables.DataTable, view TableView, title string) TableViewModel {
	vm := TableViewModel{
		Title:      title,
		Headers:    []string{},
		Columns:    []string{},
		Rows:       []map[string]string{},
		AllColumns: []ColumnInfo{},
	}

	// Create a map of visible columns for quick lookup
	visibleCols := make(map[string]bool)
	for _, colName := range view.Columns {
		visibleCols[colName] = true
	}

	// Build all columns info
	allColumnNames := table.GetColumnNames()
	for _, colName := range allColumnNames {
		col := table.GetColumn(colName)
		if col != nil {
			vm.AllColumns = append(vm.AllColumns, ColumnInfo{
				Name:        colName,
				DisplayName: col.ColumnDef().DisplayName(),
				IsVisible:   visibleCols[colName],
			})
		}
	}

	// Sort all columns alphabetically by DisplayName
	sort.Slice(vm.AllColumns, func(i, j int) bool {
		return vm.AllColumns[i].DisplayName < vm.AllColumns[j].DisplayName
	})

	// Build headers and columns from view
	for _, colName := range view.Columns {
		col := table.GetColumn(colName)
		if col != nil {
			vm.Headers = append(vm.Headers, col.ColumnDef().DisplayName())
			vm.Columns = append(vm.Columns, colName)
		}
	}

	// Get the number of rows (assumes all columns have same length)
	numRows := 0
	if len(view.Columns) > 0 {
		firstCol := table.GetColumn(view.Columns[0])
		if firstCol != nil {
			numRows = firstCol.Length()
		}
	}

	// Build rows
	for i := 0; i < numRows; i++ {
		row := make(map[string]string)
		for _, colName := range view.Columns {
			col := table.GetColumn(colName)
			if col != nil {
				value, _ := col.GetString(i)
				row[colName] = value
			}
		}
		vm.Rows = append(vm.Rows, row)
	}

	return vm
}