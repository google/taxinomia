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

	// Calculate column widths
	colWidths := t.calculateColumnWidths()
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
	}

	block := t.firstBlock

	line := -2
	for k, group := range block.Groups {
		for j := 0; j < group.AsciiHeight(); j++ {
			last := j == group.AsciiHeight()-1 && k == len(block.Groups)-1
			line += 1
			// Now go through every column
			for _, name := range t.groupingOrder {
				sb.WriteString("|")
				if _, ok := groupAtVerticalOffset[name][line+1]; ok {
					sb.WriteString(strings.Repeat("-", colWidths[name]))
				} else if last {
					sb.WriteString(strings.Repeat("-", colWidths[name]))
				} else if g, ok := groupAtVerticalOffset[name][line]; ok {
					// Get the display value for this group
					valueStr, _ := g.Block.GroupedColumn.DataColumn.GetString(g.Indices[0])
					sb.WriteString(fmt.Sprintf("%-*s", colWidths[name], valueStr))
				} else {
					sb.WriteString(strings.Repeat(" ", colWidths[name]))
				}
			}
			sb.WriteString("|")
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

// calculateColumnWidths calculates the width needed for each column
func (t *TableView) calculateColumnWidths() map[string]int {
	widths := make(map[string]int)

	// Set minimum width to 1
	for name := range t.groupedColumns {
		widths[name] = 1
	}

	for _, gc := range t.groupedColumns {
		for _, block := range gc.Blocks {
			for _, group := range block.Groups {
				val, err := block.GroupedColumn.DataColumn.GetString(group.Indices[0])
				if err == nil && len(val) > widths[gc.DataColumn.ColumnDef().Name()] {
					widths[gc.DataColumn.ColumnDef().Name()] = len(val)
				}
			}
		}
	}
	return widths
}
