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

// StringColumn is optimized for high-cardinality string data where most values are distinct.
// It stores strings directly without key mapping overhead.
type StringColumn struct {
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

func (c *StringColumn) Append(value string) {
	c.data = append(c.data, value)
}

func (c *StringColumn) Length() int {
	return len(c.data)
}

func (c *StringColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

// GetString returns the string value at index i
func (c *StringColumn) GetString(i int) (string, error) {
	if i < 0 || i >= len(c.data) {
		return "", nil
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

// Contains returns true if the column contains the value
func (c *StringColumn) Contains(value string) bool {
	for _, v := range c.data {
		if v == value {
			return true
		}
	}
	return false
}

// Unique returns all unique values in the column
func (c *StringColumn) Unique() []string {
	seen := make(map[string]bool)
	unique := make([]string, 0)

	for _, v := range c.data {
		if !seen[v] {
			seen[v] = true
			unique = append(unique, v)
		}
	}

	return unique
}


// IsKey returns whether all values in the column are unique
func (c *StringColumn) IsKey() bool {
	return c.isKey
}


// FinalizeColumn should be called after all data has been added to detect uniqueness
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