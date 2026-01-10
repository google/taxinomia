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

// JoinedStringColumn represents a column that gets its data by joining to a string column in another table
type JoinedStringColumn struct {
	IJoinedDataColumn
	columnDef    *ColumnDef
	sourceColumn IDataColumnT[string]
	joiner       IJoiner
}

// NewJoinedStringColumn creates a new joined column for string data
func NewJoinedStringColumn(columnDef *ColumnDef, joiner IJoiner, sourceColumn IDataColumnT[string]) *JoinedStringColumn {
	return &JoinedStringColumn{
		columnDef:    columnDef,
		joiner:       joiner,
		sourceColumn: sourceColumn,
	}
}

func (c *JoinedStringColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *JoinedStringColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	// the joiner is based on the columns on which the join is based
	return nil
}

func (c *JoinedStringColumn) Length() int {
	// TODO not sure if this is correct
	return c.sourceColumn.Length()
}

func (c *JoinedStringColumn) GetValue(i uint32) (string, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", fmt.Errorf("join lookup failed for column %q at index %d: %w", c.columnDef.Name(), i, err)
	}
	return c.sourceColumn.GetValue(targetIndex)
}

func (c *JoinedStringColumn) GetString(i uint32) (string, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", fmt.Errorf("join lookup failed for column %q at index %d: %w", c.columnDef.Name(), i, err)
	}
	return c.sourceColumn.GetString(targetIndex)
}

func (c *JoinedStringColumn) GetIndex(value string) (uint32, error) {
	// Joined columns don't support reverse lookups
	return 0, fmt.Errorf("column %q is a joined column and doesn't support reverse lookups", c.columnDef.Name())
}

func (c *JoinedStringColumn) IsKey() bool {
	// Joined columns are typically not keys
	return c.sourceColumn.IsKey()
}

func (c *JoinedStringColumn) GroupIndices(indices []uint32, columnView *ColumnView) map[uint32][]uint32 {
	// TODO: Not implemented - need to handle unresolved indices
	panic("GroupIndices not implemented for JoinedStringColumn")
}

type JoinedUint32Column struct {
	columnDef    *ColumnDef
	sourceColumn IDataColumnT[uint32]
	joiner       IJoiner
}

func NewJoinedUint32Column(columnDef *ColumnDef, joiner IJoiner, sourceColumn IDataColumnT[uint32]) *JoinedUint32Column {
	return &JoinedUint32Column{
		columnDef:    columnDef,
		joiner:       joiner,
		sourceColumn: sourceColumn,
	}
}

func (c *JoinedUint32Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *JoinedUint32Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *JoinedUint32Column) Length() int {
	// The length is the same as the source column
	return c.sourceColumn.Length()
}

func (c *JoinedUint32Column) GetValue(i uint32) (uint32, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return 0, fmt.Errorf("join lookup failed for column %q at index %d: %w", c.columnDef.Name(), i, err)
	}
	return c.sourceColumn.GetValue(targetIndex)
}

func (c *JoinedUint32Column) GetString(i uint32) (string, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", fmt.Errorf("join lookup failed for column %q at index %d: %w", c.columnDef.Name(), i, err)
	}
	return c.sourceColumn.GetString(targetIndex)
}

func (c *JoinedUint32Column) GetIndex(value uint32) (uint32, error) {
	// Joined columns don't support reverse lookups
	return 0, fmt.Errorf("column %q is a joined column and doesn't support reverse lookups", c.columnDef.Name())
}

func (c *JoinedUint32Column) IsKey() bool {
	// Joined columns are typically not keys
	return false
}

func (c *JoinedUint32Column) GroupIndices(indices []uint32, columnView *ColumnView) map[uint32][]uint32 {
	// TODO: Not implemented - need to handle unresolved indices
	panic("GroupIndices not implemented for JoinedUint32Column")
}
