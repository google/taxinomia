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
	"strings"
)

// BoolColumn stores boolean values efficiently.
type BoolColumn struct {
	columnDef  *ColumnDef
	data       []bool
	isKey      bool
	valueIndex map[bool]int // value -> rowIndex, only populated if isKey is true
}

// NewBoolColumn creates a new boolean column.
func NewBoolColumn(columnDef *ColumnDef) *BoolColumn {
	return &BoolColumn{
		columnDef: columnDef,
		data:      make([]bool, 0),
	}
}

// ColumnDef returns the column definition.
func (c *BoolColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

// Length returns the number of rows in the column.
func (c *BoolColumn) Length() int {
	return len(c.data)
}

// Append adds a boolean value to the column.
func (c *BoolColumn) Append(value bool) {
	c.data = append(c.data, value)
}

// AppendString parses and adds a boolean from a string.
// Accepts: "true", "false", "1", "0", "yes", "no", "t", "f", "y", "n" (case-insensitive).
func (c *BoolColumn) AppendString(s string) error {
	b, err := ParseBool(s)
	if err != nil {
		return err
	}
	c.data = append(c.data, b)
	return nil
}

// ParseBool parses a string to a boolean value.
// Accepts: "true", "false", "1", "0", "yes", "no", "t", "f", "y", "n" (case-insensitive).
func ParseBool(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1", "yes", "t", "y":
		return true, nil
	case "false", "0", "no", "f", "n", "":
		return false, nil
	default:
		return false, fmt.Errorf("cannot parse %q as boolean", s)
	}
}

// GetValue returns the boolean value at the given index.
func (c *BoolColumn) GetValue(i uint32) (bool, error) {
	if int(i) >= len(c.data) {
		return false, fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	return c.data[i], nil
}

// GetString returns the string representation of the boolean at the given index.
func (c *BoolColumn) GetString(i uint32) (string, error) {
	if int(i) >= len(c.data) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	if c.data[i] {
		return "True", nil
	}
	return "False", nil
}

// GetIndex returns the row index for the given value (key columns only).
func (c *BoolColumn) GetIndex(v bool) (uint32, error) {
	if idx, exists := c.valueIndex[v]; exists {
		return uint32(idx), nil
	}
	return 0, fmt.Errorf("value %v not found in column %q", v, c.columnDef.Name())
}

// IsKey returns whether this is a key column.
func (c *BoolColumn) IsKey() bool {
	return c.isKey
}

// SetAsKey marks this column as a key column and builds an index.
func (c *BoolColumn) SetAsKey() {
	c.isKey = true
	c.valueIndex = make(map[bool]int)
	for i, v := range c.data {
		c.valueIndex[v] = i
	}
}

// FinalizeColumn performs any final processing after all data is loaded.
func (c *BoolColumn) FinalizeColumn() {
	// Bool columns can only be unique if they have at most 2 values
	// For simplicity, we don't auto-detect key status for bool columns
}

// CreateJoinedColumn creates a joined column for this bool column.
func (c *BoolColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedBoolColumn(columnDef, joiner, c)
}

// GroupIndices groups the given indices by boolean value.
func (c *BoolColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	// Use fixed group keys: 0 for false, 1 for true
	for _, i := range indices {
		if int(i) >= len(c.data) {
			continue
		}
		var groupKey uint32
		if c.data[i] {
			groupKey = 1
		} else {
			groupKey = 0
		}
		groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
	}
	return groupedIndices, nil
}

// Filter returns indices where the predicate returns true.
func (c *BoolColumn) Filter(predicate func(bool) bool) []int {
	indices := make([]int, 0)
	for i, v := range c.data {
		if predicate(v) {
			indices = append(indices, i)
		}
	}
	return indices
}

// CountTrue returns the number of true values in the column.
func (c *BoolColumn) CountTrue() int {
	count := 0
	for _, v := range c.data {
		if v {
			count++
		}
	}
	return count
}

// CountFalse returns the number of false values in the column.
func (c *BoolColumn) CountFalse() int {
	return len(c.data) - c.CountTrue()
}
