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
	"github.com/google/taxinomia/core/tables"
)

// TableViewModel contains the data from the table formatted for template consumption
type TableViewModel struct {
	Title    string
	Headers  []string              // Column display names
	Columns  []string              // Column names (for data access)
	Rows     []map[string]string   // Each row is a map of column name to value
}

// BuildViewModel creates a ViewModel from a Table using the specified View
func BuildViewModel(table *tables.DataTable, view TableView, title string) TableViewModel {
	vm := TableViewModel{
		Title:   title,
		Headers: []string{},
		Columns: []string{},
		Rows:    []map[string]string{},
	}

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