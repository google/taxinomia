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

// ComputeStringFn is a function that computes a string value for a given row index.
// The function should capture any source columns it needs in a closure.
type ComputeStringFn func(i uint32) (string, error)

// ComputeUint32Fn is a function that computes a uint32 value for a given row index.
// The function should capture any source columns it needs in a closure.
type ComputeUint32Fn func(i uint32) (uint32, error)

// ComputedStringColumn represents a column whose values are computed from other columns.
type ComputedStringColumn struct {
	columnDef *ColumnDef
	computeFn ComputeStringFn
	length    int
}

// NewComputedStringColumn creates a new computed string column.
// The length parameter specifies the number of rows (should match source columns).
// The computeFn should be a closure that captures source columns and computes the value for each row.
func NewComputedStringColumn(columnDef *ColumnDef, length int, computeFn ComputeStringFn) *ComputedStringColumn {
	return &ComputedStringColumn{
		columnDef: columnDef,
		computeFn: computeFn,
		length:    length,
	}
}

func (c *ComputedStringColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ComputedStringColumn) Length() int {
	return c.length
}

func (c *ComputedStringColumn) GetValue(i uint32) (string, error) {
	if i >= uint32(c.length) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, c.length)
	}
	return c.computeFn(i)
}

func (c *ComputedStringColumn) GetString(i uint32) (string, error) {
	return c.GetValue(i)
}

func (c *ComputedStringColumn) GetIndex(value string) (uint32, error) {
	return 0, fmt.Errorf("column %q is a computed column and doesn't support reverse lookups", c.columnDef.Name())
}

func (c *ComputedStringColumn) IsKey() bool {
	return false
}

func (c *ComputedStringColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *ComputedStringColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[string]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		value, err := c.GetValue(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		if groupKey, ok := valueToGroupKey[value]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}

// ComputedUint32Column represents a column whose uint32 values are computed from other columns.
type ComputedUint32Column struct {
	columnDef *ColumnDef
	computeFn ComputeUint32Fn
	length    int
}

// NewComputedUint32Column creates a new computed uint32 column.
// The length parameter specifies the number of rows (should match source columns).
// The computeFn should be a closure that captures source columns and computes the value for each row.
func NewComputedUint32Column(columnDef *ColumnDef, length int, computeFn ComputeUint32Fn) *ComputedUint32Column {
	return &ComputedUint32Column{
		columnDef: columnDef,
		computeFn: computeFn,
		length:    length,
	}
}

func (c *ComputedUint32Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ComputedUint32Column) Length() int {
	return c.length
}

func (c *ComputedUint32Column) GetValue(i uint32) (uint32, error) {
	if i >= uint32(c.length) {
		return 0, fmt.Errorf("index %d out of bounds (length: %d)", i, c.length)
	}
	return c.computeFn(i)
}

func (c *ComputedUint32Column) GetString(i uint32) (string, error) {
	value, err := c.GetValue(i)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", value), nil
}

func (c *ComputedUint32Column) GetIndex(value uint32) (uint32, error) {
	return 0, fmt.Errorf("column %q is a computed column and doesn't support reverse lookups", c.columnDef.Name())
}

func (c *ComputedUint32Column) IsKey() bool {
	return false
}

func (c *ComputedUint32Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *ComputedUint32Column) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[uint32]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		value, err := c.GetValue(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		if groupKey, ok := valueToGroupKey[value]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}
