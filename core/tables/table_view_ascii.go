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

package tables

import (
	"fmt"
	"strings"

	"github.com/google/taxinomia/core/grouping"
)

// ToAscii returns a string representation of the grouped table with ASCII borders
func (t *TableView) ToAscii() string {
	var sb strings.Builder
	var groupAtVerticalOffset = make(map[string]map[int]*grouping.Group)

	// Get all grouped column names in order
	groupedCols := t.getGroupedColumnNames()

	// Calculate column widths
	colWidths := t.calculateColumnWidths(groupedCols)
	// TODO calculate group text heights (for now default to 1)

	// This will set the vertical offsets for each group
	groupAtVerticalOffset = make(map[string]map[int]*grouping.Group)
	for name, groupedColumn := range t.groupedColumns {
		groupVerticalOffsets := make(map[*grouping.Group]int)
		offset := 0
		for _, block := range groupedColumn.Blocks {
			for _, group := range block.Groups {
				groupVerticalOffsets[group] = offset
				offset += group.AsciiHeight()
			}
		}
		// Need to know  in advance the position at which to draw the values and the borders
		for group, offset := range groupVerticalOffsets {
			if _, exists := groupAtVerticalOffset[name]; !exists {
				groupAtVerticalOffset[name] = make(map[int]*grouping.Group)
			}
			groupAtVerticalOffset[name][offset] = group
		}
		fmt.Println("Group vertical offsets for", name, ":", groupAtVerticalOffset[name])
	}

	block := t.firstBlock

	line := -2
	for k, group := range block.Groups {
		for j := 0; j < group.AsciiHeight(); j++ {
			last := j == group.AsciiHeight()-1 && k == len(block.Groups)-1
			line += 1
			// Now go through every column
			for col := 0; col < len(groupedCols); col++ {
				sb.WriteString("|")
				if _, ok := groupAtVerticalOffset[groupedCols[col]][line+1]; ok {
					sb.WriteString(strings.Repeat("-", colWidths[col]))
				} else if last {
					sb.WriteString(strings.Repeat("-", colWidths[col]))
				} else if g, ok := groupAtVerticalOffset[groupedCols[col]][line]; ok {
					// Get the display value for this group
					valueStr, _ := g.Block.GroupedColumn.DataColumn.GetString(g.Indices[0])
					sb.WriteString(fmt.Sprintf("%-*s", colWidths[col], valueStr))
				} else {
					sb.WriteString(strings.Repeat(" ", colWidths[col]))
				}
			}
			sb.WriteString("|")
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

// getGroupedColumnNames returns the list of grouped column names in level order
func (t *TableView) getGroupedColumnNames() []string {
	if t.firstBlock == nil {
		return nil
	}

	result := make([]string, 0)
	maxLevel := -1

	// Find max level
	for _, gc := range t.groupedColumns {
		if gc.Level > maxLevel {
			maxLevel = gc.Level
		}
	}

	// Create result array with proper size
	result = make([]string, maxLevel+1)

	// Fill in column names by level
	for name, gc := range t.groupedColumns {
		result[gc.Level] = name
	}

	return result
}

// calculateColumnWidths calculates the width needed for each column
func (t *TableView) calculateColumnWidths(groupedCols []string) []int {
	widths := make([]int, len(groupedCols))

	// Set minimum width to 1
	for i := range widths {
		widths[i] = 1
	}

	for _, gc := range t.groupedColumns {
		for _, block := range gc.Blocks {
			for _, group := range block.Groups {
				val, err := block.GroupedColumn.DataColumn.GetString(group.Indices[0])
				if err == nil && len(val) > widths[gc.Level] {
					widths[gc.Level] = len(val)
				}
			}
		}
	}
	return widths
}
