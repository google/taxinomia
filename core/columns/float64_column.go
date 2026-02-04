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
	"math"
	"strconv"
)

// Float64Column stores float64 (double) values.
type Float64Column struct {
	columnDef  *ColumnDef
	data       []float64
	isKey      bool
	valueIndex map[float64]int // value -> rowIndex, only for key columns (rare for floats)
}

// NewFloat64Column creates a new float64 column.
func NewFloat64Column(columnDef *ColumnDef) *Float64Column {
	return &Float64Column{
		columnDef: columnDef,
		data:      make([]float64, 0),
	}
}

// ColumnDef returns the column definition.
func (c *Float64Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

// Length returns the number of rows in the column.
func (c *Float64Column) Length() int {
	return len(c.data)
}

// GetString returns the string representation of the value at the given index.
// Returns "NaN" for NaN values, "+Inf"/"-Inf" for infinities.
func (c *Float64Column) GetString(i uint32) (string, error) {
	if int(i) >= len(c.data) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	return FormatFloat64(c.data[i]), nil
}

// FormatFloat64 formats a float64 value for display.
// Returns "NaN" for NaN, "+Inf"/"-Inf" for infinities.
func FormatFloat64(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	// Use 'g' format for compact representation without trailing zeros
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// GetValue returns the float64 value at the given index.
func (c *Float64Column) GetValue(i uint32) (float64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	return c.data[i], nil
}

// GetIndex returns the row index for the given value (key columns only).
func (c *Float64Column) GetIndex(v float64) (uint32, error) {
	if c.valueIndex == nil {
		return 0, fmt.Errorf("column %q is not a key column", c.columnDef.Name())
	}
	if idx, exists := c.valueIndex[v]; exists {
		return uint32(idx), nil
	}
	return 0, fmt.Errorf("value %v not found in column %q", v, c.columnDef.Name())
}

// Append adds a float64 value to the column.
func (c *Float64Column) Append(value float64) {
	c.data = append(c.data, value)
}

// AppendString parses and adds a float64 from a string.
// Recognizes "NaN", "Inf", "+Inf", "-Inf" as special values.
func (c *Float64Column) AppendString(s string) error {
	v, err := ParseFloat64(s)
	if err != nil {
		return err
	}
	c.data = append(c.data, v)
	return nil
}

// ParseFloat64 parses a string to float64.
// Recognizes "NaN", "Inf", "+Inf", "-Inf" as special values.
func ParseFloat64(s string) (float64, error) {
	// Handle special values explicitly
	switch s {
	case "NaN", "nan", "NAN":
		return math.NaN(), nil
	case "Inf", "+Inf", "inf", "+inf":
		return math.Inf(1), nil
	case "-Inf", "-inf":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(s, 64)
}

// IsKey returns whether this column is a key column.
func (c *Float64Column) IsKey() bool {
	return c.isKey
}

// FinalizeColumn should be called after all data has been added.
// Detects uniqueness and builds index if values are unique.
func (c *Float64Column) FinalizeColumn() {
	// Build a temporary index to check for uniqueness
	tempIndex := make(map[float64]int)
	isUnique := true

	for i, value := range c.data {
		// NaN values are never equal to each other, so they break uniqueness detection
		if math.IsNaN(value) {
			isUnique = false
			break
		}
		if _, exists := tempIndex[value]; exists {
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

// CreateJoinedColumn creates a joined column for this float64 column.
func (c *Float64Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedFloat64Column(columnDef, joiner, c)
}

// GroupIndices groups the given indices by float64 value.
// Note: NaN values are grouped together (even though NaN != NaN mathematically).
func (c *Float64Column) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[float64]uint32{}
	var nanGroupKey uint32 = math.MaxUint32
	hasNanGroup := false

	for _, i := range indices {
		if int(i) >= len(c.data) {
			continue
		}
		value := c.data[i]

		// Handle NaN specially since NaN != NaN
		if math.IsNaN(value) {
			if !hasNanGroup {
				nanGroupKey = uint32(len(valueToGroupKey))
				hasNanGroup = true
			}
			groupedIndices[nanGroupKey] = append(groupedIndices[nanGroupKey], i)
			continue
		}

		if groupKey, ok := valueToGroupKey[value]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			if hasNanGroup {
				groupKey++ // Account for NaN group
			}
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}

	return groupedIndices, nil
}

// Filter returns indices where the predicate returns true.
func (c *Float64Column) Filter(predicate func(float64) bool) []int {
	indices := make([]int, 0)
	for i, v := range c.data {
		if predicate(v) {
			indices = append(indices, i)
		}
	}
	return indices
}

// Statistics methods

// Sum returns the sum of all values (NaN if any value is NaN).
func (c *Float64Column) Sum() float64 {
	var total float64
	for _, v := range c.data {
		total += v
	}
	return total
}

// Avg returns the average of all values.
func (c *Float64Column) Avg() float64 {
	if len(c.data) == 0 {
		return math.NaN()
	}
	return c.Sum() / float64(len(c.data))
}

// Min returns the minimum value (ignoring NaN).
func (c *Float64Column) Min() float64 {
	if len(c.data) == 0 {
		return math.NaN()
	}
	min := math.Inf(1)
	for _, v := range c.data {
		if !math.IsNaN(v) && v < min {
			min = v
		}
	}
	if math.IsInf(min, 1) {
		return math.NaN() // All values were NaN
	}
	return min
}

// Max returns the maximum value (ignoring NaN).
func (c *Float64Column) Max() float64 {
	if len(c.data) == 0 {
		return math.NaN()
	}
	max := math.Inf(-1)
	for _, v := range c.data {
		if !math.IsNaN(v) && v > max {
			max = v
		}
	}
	if math.IsInf(max, -1) {
		return math.NaN() // All values were NaN
	}
	return max
}
