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

// StringColumn is optimized for high-cardinality string data where most values are distinct.
// It stores strings directly without key mapping overhead.
type StringColumn struct {
	// IDataColumnT[string]
	columnDef  *ColumnDef
	data       []string
	isKey      bool
	valueIndex map[string]int // value -> rowIndex, only populated if isKey is true
}

// NewStringColumn creates a new string column optimized for distinct values
func NewStringColumn(columnDef *ColumnDef) *StringColumn {
	return &StringColumn{
		columnDef: columnDef,
		data:      make([]string, 0),
	}
}

func (c *StringColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	// the joiner is based on the columns on which the join is based
	return NewJoinedStringColumn(columnDef, joiner, c)
}

func (c *StringColumn) Append(value string) {
	c.data = append(c.data, value)
}

func (c *StringColumn) Length() int {
	return len(c.data)
}

func (c *StringColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *StringColumn) GetValue(i uint32) (string, error) {
	if i >= uint32(len(c.data)) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	return c.data[i], nil
}

func (c *StringColumn) GetIndex(v string) (uint32, error) {
	if idx, exists := c.valueIndex[v]; exists {
		return uint32(idx), nil
	}
	return 0, fmt.Errorf("value %q not found in column %q", v, c.columnDef.Name())
}

// GetString returns the string value at index i
func (c *StringColumn) GetString(i uint32) (string, error) {
	if i >= uint32(len(c.data)) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	return c.data[i], nil
}

// Filter returns indices where the predicate returns true
func (c *StringColumn) Filter(predicate func(string) bool) []int {
	indices := make([]int, 0)
	for i, v := range c.data {
		if predicate(v) {
			indices = append(indices, i)
		}
	}
	return indices
}

// IsKey returns whether all values in the column are unique
func (c *StringColumn) IsKey() bool {
	return c.isKey
}

// Finalizeolumn should be called after all data has been added to detect uniqueness
// and build indexes if the column contains unique values
func (c *StringColumn) FinalizeColumn() {
	// Build a temporary index to check for uniqueness
	tempIndex := make(map[string]int)
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

// type StringJoinedColumn struct {
// 	IJoinedColumn[string]
// 	ColumnDef *ColumnDef
// 	join      IJoinedDataColumn
// }

// func NewStringJoinedColumn(columnDef *ColumnDef, join IJoinedDataColumn) *StringJoinedColumn {
// 	return &StringJoinedColumn{
// 		ColumnDef: columnDef,
// 		join:      join,
// 	}
// }

func (c *StringColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	// for now assume just default grouping by value
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[string]uint32{}
	for _, i := range indices {
		value := c.data[i]
		if groupKey, ok := valueToGroupKey[value]; ok {
			// Add to existing group
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			// Create new group with a unique group key
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	// StringColumn always resolves all indices, no unmapped values
	return groupedIndices, nil
}
