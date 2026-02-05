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

// Int64Column is optimized for int64 numeric data.
// It stores int64 values directly without key mapping overhead.
type Int64Column struct {
	columnDef  *ColumnDef
	data       []int64
	isKey      bool
	valueIndex map[int64]int // value -> rowIndex, only populated if isKey is true
}

// NewInt64Column creates a new int64 column
func NewInt64Column(columnDef *ColumnDef) *Int64Column {
	return &Int64Column{
		columnDef: columnDef,
		data:      make([]int64, 0),
	}
}

func (c *Int64Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedInt64Column(columnDef, joiner, c)
}

func (c *Int64Column) Append(value int64) {
	c.data = append(c.data, value)
}

func (c *Int64Column) Length() int {
	return len(c.data)
}

func (c *Int64Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

// GetString returns the string representation of the value at index i
func (c *Int64Column) GetString(i uint32) (string, error) {
	if i >= uint32(len(c.data)) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	return fmt.Sprintf("%d", c.data[i]), nil
}

func (c *Int64Column) GetValue(i uint32) (int64, error) {
	if i >= uint32(len(c.data)) {
		return 0, fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	return c.data[i], nil
}

// GetIndex returns the index of the given value
func (c *Int64Column) GetIndex(v int64) (uint32, error) {
	if idx, exists := c.valueIndex[v]; exists {
		return uint32(idx), nil
	}
	return 0, fmt.Errorf("value %d not found in column %q", v, c.columnDef.Name())
}

// Filter returns indices where the predicate returns true
func (c *Int64Column) Filter(predicate func(int64) bool) []int {
	indices := make([]int, 0)
	for i, v := range c.data {
		if predicate(v) {
			indices = append(indices, i)
		}
	}
	return indices
}

// IsKey returns whether all values in the column are unique
func (c *Int64Column) IsKey() bool {
	return c.isKey
}

// FinalizeColumn should be called after all data has been added to detect uniqueness
// and build indexes if the column contains unique values
func (c *Int64Column) FinalizeColumn() {
	// Build a temporary index to check for uniqueness
	tempIndex := make(map[int64]int)
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

func (c *Int64Column) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[int64]uint32{}

	for _, i := range indices {
		if int(i) >= len(c.data) {
			continue
		}
		value := c.data[i]

		if groupKey, ok := valueToGroupKey[value]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}

	// Int64Column always resolves all indices, no unmapped values
	return groupedIndices, nil
}
