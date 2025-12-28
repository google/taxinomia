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

import "fmt"

// Uint32Column is optimized for uint32 numeric data.
// It stores uint32 values directly without key mapping overhead.
type Uint32Column struct {
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

func (c *Uint32Column) Append(value uint32) {
	c.data = append(c.data, value)
}

func (c *Uint32Column) Length() int {
	return len(c.data)
}

func (c *Uint32Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

// GetString returns the string representation of the value at index i
func (c *Uint32Column) GetString(i int) (string, error) {
	if i < 0 || i >= len(c.data) {
		return "", nil
	}
	return fmt.Sprintf("%d", c.data[i]), nil
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

// Contains returns true if the column contains the value
func (c *Uint32Column) Contains(value uint32) bool {
	for _, v := range c.data {
		if v == value {
			return true
		}
	}
	return false
}

// Unique returns all unique values in the column
func (c *Uint32Column) Unique() []uint32 {
	seen := make(map[uint32]bool)
	unique := make([]uint32, 0)

	for _, v := range c.data {
		if !seen[v] {
			seen[v] = true
			unique = append(unique, v)
		}
	}

	return unique
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