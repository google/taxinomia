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

package columns

import (
	"fmt"
)

// Uint32Column is optimized for uint32 numeric data.
// It stores uint32 values directly without key mapping overhead.
type Uint32Column struct {
	//IDataColumnT[uint32]
	columnDef  *ColumnDef
	data       []uint32
	isKey      bool
	valueIndex map[uint32]int // value -> rowIndex, only populated if isKey is true
}

// NewUint32Column creates a new uint32 column
func NewUint32Column(columnDef *ColumnDef) *Uint32Column {
	return &Uint32Column{
		columnDef: columnDef,
		data:      make([]uint32, 0),
	}
}

func (c *Uint32Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	// the joiner is based on the columns on which the join is based
	return NewJoinedUint32Column(columnDef, joiner, c)
}

func (c *Uint32Column) Append(value uint32) {
	c.data = append(c.data, value)
}

func (c *Uint32Column) Length() int {
	return len(c.data)
}

func (c *Uint32Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

// func (c *Uint32Column) NewJoiner(onColumn IDataColumnT[uint32]) IJoiner {
// 	return &Joiner[uint32]{
// 		FromColumn: c,
// 		ToColumn:   onColumn,
// 	}
// }

// GetString returns the string representation of the value at index i
func (c *Uint32Column) GetString(i uint32) string {
	return fmt.Sprintf("%d", c.data[i])
}

func (c *Uint32Column) GetValue(i uint32) uint32 {
	return c.data[i]
}

// GetIndex returns the index of the given value, or -1 if not found
func (c *Uint32Column) GetIndex(v uint32) uint32 {
	if idx, exists := c.valueIndex[v]; exists {
		return uint32(idx)
	}
	return uint32(0xFFFFFFFF) // Indicate not found
}

// Filter returns indices where the predicate returns true
func (c *Uint32Column) Filter(predicate func(uint32) bool) []int {
	indices := make([]int, 0)
	for i, v := range c.data {
		if predicate(v) {
			indices = append(indices, i)
		}
	}
	return indices
}

// IsKey returns whether all values in the column are unique
func (c *Uint32Column) IsKey() bool {
	return c.isKey
}

// FinalizeColumn should be called after all data has been added to detect uniqueness
// and build indexes if the column contains unique values
func (c *Uint32Column) FinalizeColumn() {
	// Build a temporary index to check for uniqueness
	tempIndex := make(map[uint32]int)
	isUnique := true

	for i, value := range c.data {
		if _, exists := tempIndex[value]; exists {
			// Duplicate found
			isUnique = false
			break
		}
		tempIndex[value] = i
	}

	c.isKey = isUnique

	// Only keep the index if values are unique and it's an entity type column
	if isUnique && c.columnDef.EntityType() != "" {
		c.valueIndex = tempIndex
	} else {
		c.valueIndex = nil
	}
}

// type Uint32JoinedColumn struct {
// 	IJoinedColumn[uint32]
// 	ColumnDef    *ColumnDef
// 	join         IJoinedDataColumn
// 	sourceColumn IDataColumn
// 	joinColumn   IDataColumn
// }

// func NewUint32JoinedColumn(columnDef *ColumnDef, join *joins.Join, dataModel *models.DataModel) *Uint32JoinedColumn {
// 	parts := strings.Split(columnDef.name, ".")
// 	sourceTable := dataModel.GetTable(parts[0])
// 	sourceColumn := sourceTable.GetColumn(parts[1])
// 	joinColumn := sourceTable.GetColumn(join.ToColumn)
// 	return &Uint32JoinedColumn{
// 		ColumnDef:    columnDef,
// 		join:         join,
// 		sourceColumn: sourceColumn,
// 		joinColumn:   joinColumn,
// 	}
// }

// // let's store more data in here...
// // in table.colum., table is the joined table, column is the column
// // the joining is made on the join.fromColumn -> join.toColumn
// // index is in fromTable, this index is used to find the fromCOlumn value, which is then used to lookup the index in the toTable.toColumn, which is then used to load the toTable.column value

// func (c *Uint32JoinedColumn) GetValue(i uint32) (uint32, error) {
// 	//
// }
