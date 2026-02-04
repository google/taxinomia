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
	"time"
)

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
		return "", ErrUnmatched
	}
	return c.sourceColumn.GetValue(targetIndex)
}

func (c *JoinedStringColumn) GetString(i uint32) (string, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", ErrUnmatched
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

func (c *JoinedStringColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[string]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		// Resolve the index through the join to get the target index
		targetIndex, err := c.joiner.Lookup(i)
		if err != nil {
			// Collect unresolved indices
			unmapped = append(unmapped, i)
			continue
		}

		// Get the value from the source column at the resolved index
		value, err := c.sourceColumn.GetValue(targetIndex)
		if err != nil {
			// Treat errors as unmapped
			unmapped = append(unmapped, i)
			continue
		}

		// Group by the resolved value
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
		return 0, ErrUnmatched
	}
	return c.sourceColumn.GetValue(targetIndex)
}

func (c *JoinedUint32Column) GetString(i uint32) (string, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", ErrUnmatched
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

func (c *JoinedUint32Column) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[uint32]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		// Resolve the index through the join to get the target index
		targetIndex, err := c.joiner.Lookup(i)
		if err != nil {
			// Collect unresolved indices
			unmapped = append(unmapped, i)
			continue
		}

		// Get the value from the source column at the resolved index
		value, err := c.sourceColumn.GetValue(targetIndex)
		if err != nil {
			// Treat errors as unmapped
			unmapped = append(unmapped, i)
			continue
		}

		// Group by the resolved value
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

// JoinedDatetimeColumn represents a column that gets its data by joining to a datetime column in another table
type JoinedDatetimeColumn struct {
	columnDef    *ColumnDef
	sourceColumn *DatetimeColumn
	joiner       IJoiner
}

// NewJoinedDatetimeColumn creates a new joined column for datetime data
func NewJoinedDatetimeColumn(columnDef *ColumnDef, joiner IJoiner, sourceColumn *DatetimeColumn) *JoinedDatetimeColumn {
	return &JoinedDatetimeColumn{
		columnDef:    columnDef,
		joiner:       joiner,
		sourceColumn: sourceColumn,
	}
}

func (c *JoinedDatetimeColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *JoinedDatetimeColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *JoinedDatetimeColumn) Length() int {
	return c.sourceColumn.Length()
}

func (c *JoinedDatetimeColumn) GetString(i uint32) (string, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", ErrUnmatched
	}
	return c.sourceColumn.GetString(targetIndex)
}

func (c *JoinedDatetimeColumn) GetValue(i uint32) (time.Time, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return time.Time{}, ErrUnmatched
	}
	return c.sourceColumn.GetValue(targetIndex)
}

func (c *JoinedDatetimeColumn) IsKey() bool {
	return false
}

func (c *JoinedDatetimeColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[int64]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		targetIndex, err := c.joiner.Lookup(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		value, err := c.sourceColumn.GetValue(targetIndex)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		nanos := value.UnixNano()
		if groupKey, ok := valueToGroupKey[nanos]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[nanos] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}

// JoinedDurationColumn represents a column that gets its data by joining to a duration column in another table
type JoinedDurationColumn struct {
	columnDef    *ColumnDef
	sourceColumn *DurationColumn
	joiner       IJoiner
}

// NewJoinedDurationColumn creates a new joined column for duration data
func NewJoinedDurationColumn(columnDef *ColumnDef, joiner IJoiner, sourceColumn *DurationColumn) *JoinedDurationColumn {
	return &JoinedDurationColumn{
		columnDef:    columnDef,
		joiner:       joiner,
		sourceColumn: sourceColumn,
	}
}

func (c *JoinedDurationColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *JoinedDurationColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *JoinedDurationColumn) Length() int {
	return c.sourceColumn.Length()
}

func (c *JoinedDurationColumn) GetString(i uint32) (string, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", ErrUnmatched
	}
	return c.sourceColumn.GetString(targetIndex)
}

func (c *JoinedDurationColumn) Nanoseconds(i uint32) (int64, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return 0, ErrUnmatched
	}
	return c.sourceColumn.Nanoseconds(targetIndex)
}

func (c *JoinedDurationColumn) IsKey() bool {
	return false
}

func (c *JoinedDurationColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[int64]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		targetIndex, err := c.joiner.Lookup(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		value, err := c.sourceColumn.GetValue(targetIndex)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		nanos := int64(value)
		if groupKey, ok := valueToGroupKey[nanos]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[nanos] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}

// JoinedBoolColumn represents a column that gets its data by joining to a bool column in another table
type JoinedBoolColumn struct {
	columnDef    *ColumnDef
	sourceColumn *BoolColumn
	joiner       IJoiner
}

// NewJoinedBoolColumn creates a new joined column for bool data
func NewJoinedBoolColumn(columnDef *ColumnDef, joiner IJoiner, sourceColumn *BoolColumn) *JoinedBoolColumn {
	return &JoinedBoolColumn{
		columnDef:    columnDef,
		joiner:       joiner,
		sourceColumn: sourceColumn,
	}
}

func (c *JoinedBoolColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *JoinedBoolColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *JoinedBoolColumn) Length() int {
	return c.sourceColumn.Length()
}

func (c *JoinedBoolColumn) GetString(i uint32) (string, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", ErrUnmatched
	}
	return c.sourceColumn.GetString(targetIndex)
}

func (c *JoinedBoolColumn) GetValue(i uint32) (bool, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return false, ErrUnmatched
	}
	return c.sourceColumn.GetValue(targetIndex)
}

func (c *JoinedBoolColumn) IsKey() bool {
	return false
}

func (c *JoinedBoolColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		targetIndex, err := c.joiner.Lookup(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		value, err := c.sourceColumn.GetValue(targetIndex)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		// Use fixed group keys: 0 for false, 1 for true
		var groupKey uint32
		if value {
			groupKey = 1
		} else {
			groupKey = 0
		}
		groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
	}
	return groupedIndices, unmapped
}

// JoinedFloat64Column represents a column that gets its data by joining to a float64 column in another table
type JoinedFloat64Column struct {
	columnDef    *ColumnDef
	sourceColumn *Float64Column
	joiner       IJoiner
}

// NewJoinedFloat64Column creates a new joined column for float64 data
func NewJoinedFloat64Column(columnDef *ColumnDef, joiner IJoiner, sourceColumn *Float64Column) *JoinedFloat64Column {
	return &JoinedFloat64Column{
		columnDef:    columnDef,
		joiner:       joiner,
		sourceColumn: sourceColumn,
	}
}

func (c *JoinedFloat64Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *JoinedFloat64Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *JoinedFloat64Column) Length() int {
	return c.sourceColumn.Length()
}

func (c *JoinedFloat64Column) GetString(i uint32) (string, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", ErrUnmatched
	}
	return c.sourceColumn.GetString(targetIndex)
}

func (c *JoinedFloat64Column) GetValue(i uint32) (float64, error) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return 0, ErrUnmatched
	}
	return c.sourceColumn.GetValue(targetIndex)
}

func (c *JoinedFloat64Column) IsKey() bool {
	return false
}

func (c *JoinedFloat64Column) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[float64]uint32{}
	var unmapped []uint32
	var nanGroupKey uint32 = math.MaxUint32
	hasNanGroup := false

	for _, i := range indices {
		targetIndex, err := c.joiner.Lookup(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		value, err := c.sourceColumn.GetValue(targetIndex)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

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
				groupKey++
			}
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}
