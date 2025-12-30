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
	"github.com/google/taxinomia/core/columns"
)

type DataTable struct {
	columns map[string]columns.IDataColumn
	joins   map[string]columns.IJoinedDataColumn
}

func NewDataTable() *DataTable {
	return &DataTable{
		columns: make(map[string]columns.IDataColumn),
		joins:   make(map[string]columns.IJoinedDataColumn),
	}
}

func (dt *DataTable) AddColumn(col columns.IDataColumn) {
	// Initialize the column with empty data
	def := col.ColumnDef()
	name := def.Name()
	dt.columns[name] = col
}

func (dt *DataTable) AddJoinedColumn(joinedCol columns.IJoinedDataColumn) {
	dt.joins[joinedCol.ColumnDef().Name()] = joinedCol
}

// RemoveJoinedColumn removes a joined column from the table
func (dt *DataTable) RemoveJoinedColumn(name string) {
	delete(dt.joins, name)
}

func (dt *DataTable) GetColumn(name string) columns.IDataColumn {
	if col, ok := dt.columns[name]; ok {
		return col
	}
	if col, ok := dt.joins[name]; ok {
		return col
	}
	return nil
}

// GetColumnNames returns all column names in the table
func (dt *DataTable) GetColumnNames() []string {
	names := make([]string, 0, len(dt.columns))
	for name := range dt.columns {
		names = append(names, name)
	}
	return names
}

// GetAllColumnNames returns all column names including joined columns
func (dt *DataTable) GetAllColumnNames() []string {
	names := make([]string, 0, len(dt.columns)+len(dt.joins))

	// Add regular columns
	for name := range dt.columns {
		names = append(names, name)
	}

	// Add joined columns
	for name := range dt.joins {
		names = append(names, name)
	}

	return names
}

// GetJoinedColumnNames returns only joined column names
func (dt *DataTable) GetJoinedColumnNames() []string {
	names := make([]string, 0, len(dt.joins))
	for name := range dt.joins {
		names = append(names, name)
	}
	return names
}
