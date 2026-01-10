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
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
)

// TestGroupTable tests the GroupTable functionality
func TestGroupTable(t *testing.T) {
	// Create a simple test table with string data
	table := NewDataTable()

	// Create a status column with repeating values
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	statusCol.Append("Active")
	statusCol.Append("Active")
	statusCol.Append("Inactive")
	statusCol.Append("Active")
	statusCol.Append("Inactive")
	statusCol.Append("Pending")
	statusCol.FinalizeColumn()

	// Create a region column
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))
	regionCol.Append("North")
	regionCol.Append("South")
	regionCol.Append("North")
	regionCol.Append("East")
	regionCol.Append("South")
	regionCol.Append("West")
	regionCol.FinalizeColumn()

	// Add columns to table
	table.AddColumn(statusCol)
	table.AddColumn(regionCol)

	// Create a TableView
	tableView := NewTableView(table, "test_table")

	// Initialize columnViews map with a basic ColumnView for the status column
	tableView.columnViews = make(map[string]*columns.ColumnView)
	tableView.columnViews["status"] = &columns.ColumnView{}
	tableView.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView.blocksByColumn = make(map[string][]*grouping.Block)

	// Test GroupTable with the status column
	columns := []string{"status"}
	aggregatedColumns := []string{}
	compare := make(map[string]Compare)
	asc := make(map[string]bool)

	// Debug: check table length
	t.Logf("Table length: %d", table.Length())
	t.Logf("Status column length: %d", statusCol.Length())

	// Call GroupTable with no mask (all rows)
	tableView.GroupTable(nil, columns, aggregatedColumns, compare, asc)

	// Verify that grouping was performed
	if tableView.firstBlock == nil {
		t.Fatal("Expected firstBlock to be set, but it was nil")
	}

	// Check that groups were created
	if len(tableView.firstBlock.Groups) == 0 {
		t.Fatal("Expected groups to be created, but none were found")
	}

	// Debug: print group information
	for i, group := range tableView.firstBlock.Groups {
		t.Logf("Group %d: GroupKey=%d, NumIndices=%d, Indices=%v", i, group.GroupKey, len(group.Indices), group.Indices)
	}

	// We should have 3 groups: Active, Inactive, Pending
	expectedGroups := 3
	actualGroups := len(tableView.firstBlock.Groups)
	if actualGroups != expectedGroups {
		t.Errorf("Expected %d groups, got %d", expectedGroups, actualGroups)
	}

	// Verify that each group has the correct number of indices
	groupCounts := make(map[uint32]int)
	for _, group := range tableView.firstBlock.Groups {
		groupCounts[group.GroupKey] = len(group.Indices)
	}

	// We should have different group sizes:
	// - Active: 3 occurrences (indices 0, 1, 3)
	// - Inactive: 2 occurrences (indices 2, 4)
	// - Pending: 1 occurrence (index 5)
	totalIndices := 0
	for _, count := range groupCounts {
		totalIndices += count
	}

	expectedTotalIndices := 6
	if totalIndices != expectedTotalIndices {
		t.Errorf("Expected total of %d indices across all groups, got %d", expectedTotalIndices, totalIndices)
	}

	// Verify that the groupedColumns map was updated
	if _, exists := tableView.groupedColumns["status"]; !exists {
		t.Error("Expected 'status' to be in groupedColumns map")
	}

	// Verify that blocksByColumn was updated
	if blocks, exists := tableView.blocksByColumn["status"]; !exists || len(blocks) == 0 {
		t.Error("Expected 'status' to have blocks in blocksByColumn map")
	}
}

// TestGroupTableWithMask tests grouping with a filtered subset of rows
func TestGroupTableWithMask(t *testing.T) {
	// Create a test table
	table := NewDataTable()

	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	statusCol.Append("Active")
	statusCol.Append("Active")
	statusCol.Append("Inactive")
	statusCol.Append("Active")
	statusCol.Append("Inactive")
	statusCol.FinalizeColumn()

	table.AddColumn(statusCol)

	// Create TableView
	tableView := NewTableView(table, "test_table")
	tableView.columnViews = make(map[string]*columns.ColumnView)
	tableView.columnViews["status"] = &columns.ColumnView{}
	tableView.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView.blocksByColumn = make(map[string][]*grouping.Block)

	// Create a mask that filters to only indices 0, 1, 3 (all "Active" rows)
	mask := []bool{true, true, false, true, false}

	columns := []string{"status"}
	tableView.GroupTable(mask, columns, []string{}, make(map[string]Compare), make(map[string]bool))

	// With the mask, we should only have 1 group (Active)
	if len(tableView.firstBlock.Groups) != 1 {
		t.Errorf("Expected 1 group with mask, got %d", len(tableView.firstBlock.Groups))
	}

	// The single group should have 3 indices
	if len(tableView.firstBlock.Groups[0].Indices) != 3 {
		t.Errorf("Expected 3 indices in the group, got %d", len(tableView.firstBlock.Groups[0].Indices))
	}
}

// TestGroupTableWithUint32Column tests grouping with numeric data
func TestGroupTableWithUint32Column(t *testing.T) {
	// Create a test table with uint32 column
	table := NewDataTable()

	categoryCol := columns.NewUint32Column(columns.NewColumnDef("category_id", "Category ID", ""))
	categoryCol.Append(1)
	categoryCol.Append(2)
	categoryCol.Append(1)
	categoryCol.Append(3)
	categoryCol.Append(2)
	categoryCol.Append(1)
	categoryCol.FinalizeColumn()

	table.AddColumn(categoryCol)

	// Create TableView
	tableView := NewTableView(table, "test_table")
	tableView.columnViews = make(map[string]*columns.ColumnView)
	tableView.columnViews["category_id"] = &columns.ColumnView{}
	tableView.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView.blocksByColumn = make(map[string][]*grouping.Block)

	columns := []string{"category_id"}
	tableView.GroupTable(nil, columns, []string{}, make(map[string]Compare), make(map[string]bool))

	// Should have 3 groups (category IDs: 1, 2, 3)
	expectedGroups := 3
	actualGroups := len(tableView.firstBlock.Groups)
	if actualGroups != expectedGroups {
		t.Errorf("Expected %d groups, got %d", expectedGroups, actualGroups)
	}

	// Verify total indices
	totalIndices := 0
	for _, group := range tableView.firstBlock.Groups {
		totalIndices += len(group.Indices)
	}

	if totalIndices != 6 {
		t.Errorf("Expected 6 total indices, got %d", totalIndices)
	}
}

// TestGroupTableTwoColumns tests hierarchical grouping with two columns
func TestGroupTableTwoColumns(t *testing.T) {
	// Create a test table with two grouping columns
	table := NewDataTable()

	// Create a status column
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	statusCol.Append("Active")   // 0
	statusCol.Append("Active")   // 1
	statusCol.Append("Inactive") // 2
	statusCol.Append("Active")   // 3
	statusCol.Append("Inactive") // 4
	statusCol.Append("Active")   // 5
	statusCol.FinalizeColumn()

	// Create a region column
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))
	regionCol.Append("North") // 0
	regionCol.Append("South") // 1
	regionCol.Append("North") // 2
	regionCol.Append("North") // 3
	regionCol.Append("South") // 4
	regionCol.Append("East")  // 5
	regionCol.FinalizeColumn()

	// Add columns to table
	table.AddColumn(statusCol)
	table.AddColumn(regionCol)

	// Create TableView
	tableView := NewTableView(table, "test_table")
	tableView.columnViews = make(map[string]*columns.ColumnView)
	tableView.columnViews["status"] = &columns.ColumnView{}
	tableView.columnViews["region"] = &columns.ColumnView{}
	tableView.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView.blocksByColumn = make(map[string][]*grouping.Block)

	// Group by status, then by region (hierarchical grouping)
	columns := []string{"status", "region"}
	tableView.GroupTable(nil, columns, []string{}, make(map[string]Compare), make(map[string]bool))

	// Verify first level grouping (status)
	if tableView.firstBlock == nil {
		t.Fatal("Expected firstBlock to be set")
	}

	// First level should have 2 groups: Active and Inactive
	if len(tableView.firstBlock.Groups) != 2 {
		t.Errorf("Expected 2 first-level groups, got %d", len(tableView.firstBlock.Groups))
	}

	t.Log("First level groups (status):")
	for i, group := range tableView.firstBlock.Groups {
		t.Logf("  Group %d: GroupKey=%d, NumIndices=%d, Indices=%v", i, group.GroupKey, len(group.Indices), group.Indices)
	}

	// Verify that the region column has a GroupedColumn
	regionGroupedCol, exists := tableView.groupedColumns["region"]
	if !exists {
		t.Fatal("Expected region to be in groupedColumns")
	}

	if regionGroupedCol.Level != 1 {
		t.Errorf("Expected region GroupedColumn to have level 1, got %d", regionGroupedCol.Level)
	}

	// Each first-level group should spawn blocks for the second level
	// Active group (indices 0,1,3,5) should have regions: North, South, East
	// Inactive group (indices 2,4) should have regions: North, South
	t.Log("\nSecond level blocks (region):")
	totalSecondLevelGroups := 0
	for i, block := range regionGroupedCol.Blocks {
		t.Logf("  Block %d: ParentGroup=%v, NumGroups=%d", i, block.ParentGroup.GroupKey, len(block.Groups))
		for j, group := range block.Groups {
			t.Logf("    Group %d: GroupKey=%d, Indices=%v", j, group.GroupKey, group.Indices)
			totalSecondLevelGroups++
		}
	}

	// We should have 2 blocks (one per first-level group)
	if len(regionGroupedCol.Blocks) != 2 {
		t.Errorf("Expected 2 blocks for region, got %d", len(regionGroupedCol.Blocks))
	}

	// Verify all indices are accounted for
	totalIndices := 0
	for _, block := range regionGroupedCol.Blocks {
		for _, group := range block.Groups {
			totalIndices += len(group.Indices)
		}
	}

	if totalIndices != 6 {
		t.Errorf("Expected 6 total indices in second level, got %d", totalIndices)
	}
}

// TestGroupTableTwoColumnsWithUint32 tests hierarchical grouping with numeric columns
func TestGroupTableTwoColumnsWithUint32(t *testing.T) {
	// Create a test table
	table := NewDataTable()

	// Create category column
	categoryCol := columns.NewUint32Column(columns.NewColumnDef("category", "Category", ""))
	categoryCol.Append(1) // 0
	categoryCol.Append(1) // 1
	categoryCol.Append(2) // 2
	categoryCol.Append(1) // 3
	categoryCol.Append(2) // 4
	categoryCol.Append(3) // 5
	categoryCol.FinalizeColumn()

	// Create priority column
	priorityCol := columns.NewUint32Column(columns.NewColumnDef("priority", "Priority", ""))
	priorityCol.Append(10) // 0
	priorityCol.Append(20) // 1
	priorityCol.Append(10) // 2
	priorityCol.Append(10) // 3
	priorityCol.Append(20) // 4
	priorityCol.Append(10) // 5
	priorityCol.FinalizeColumn()

	table.AddColumn(categoryCol)
	table.AddColumn(priorityCol)

	// Create TableView
	tableView := NewTableView(table, "test_table")
	tableView.columnViews = make(map[string]*columns.ColumnView)
	tableView.columnViews["category"] = &columns.ColumnView{}
	tableView.columnViews["priority"] = &columns.ColumnView{}
	tableView.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView.blocksByColumn = make(map[string][]*grouping.Block)

	// Group by category, then by priority
	columns := []string{"category", "priority"}
	tableView.GroupTable(nil, columns, []string{}, make(map[string]Compare), make(map[string]bool))

	// First level: should have 3 category groups (1, 2, 3)
	if len(tableView.firstBlock.Groups) != 3 {
		t.Errorf("Expected 3 first-level groups, got %d", len(tableView.firstBlock.Groups))
	}

	// Verify priority grouping exists
	priorityGroupedCol, exists := tableView.groupedColumns["priority"]
	if !exists {
		t.Fatal("Expected priority to be in groupedColumns")
	}

	// Should have 3 blocks (one per category)
	if len(priorityGroupedCol.Blocks) != 3 {
		t.Errorf("Expected 3 blocks for priority, got %d", len(priorityGroupedCol.Blocks))
	}

	t.Log("Category groups:")
	for i, group := range tableView.firstBlock.Groups {
		t.Logf("  Category %d: GroupKey=%d, Indices=%v", i, group.GroupKey, group.Indices)
	}

	t.Log("\nPriority blocks within each category:")
	for i, block := range priorityGroupedCol.Blocks {
		t.Logf("  Block %d (parent category GroupKey=%d):", i, block.ParentGroup.GroupKey)
		for j, group := range block.Groups {
			t.Logf("    Priority group %d: GroupKey=%d, Indices=%v", j, group.GroupKey, group.Indices)
		}
	}

	// Verify all 6 indices are accounted for
	totalIndices := 0
	for _, block := range priorityGroupedCol.Blocks {
		for _, group := range block.Groups {
			totalIndices += len(group.Indices)
		}
	}

	if totalIndices != 6 {
		t.Errorf("Expected 6 total indices, got %d", totalIndices)
	}
}

// TestGroupTableThreeColumns tests hierarchical grouping with three levels
func TestGroupTableThreeColumns(t *testing.T) {
	// Create a test table
	table := NewDataTable()

	// Create first grouping column
	col1 := columns.NewStringColumn(columns.NewColumnDef("col1", "Column 1", ""))
	col1.Append("A")
	col1.Append("A")
	col1.Append("B")
	col1.Append("A")
	col1.FinalizeColumn()

	// Create second grouping column
	col2 := columns.NewStringColumn(columns.NewColumnDef("col2", "Column 2", ""))
	col2.Append("X")
	col2.Append("Y")
	col2.Append("X")
	col2.Append("X")
	col2.FinalizeColumn()

	// Create third grouping column
	col3 := columns.NewUint32Column(columns.NewColumnDef("col3", "Column 3", ""))
	col3.Append(1)
	col3.Append(2)
	col3.Append(1)
	col3.Append(1)
	col3.FinalizeColumn()

	table.AddColumn(col1)
	table.AddColumn(col2)
	table.AddColumn(col3)

	// Create TableView
	tableView := NewTableView(table, "test_table")
	tableView.columnViews = make(map[string]*columns.ColumnView)
	tableView.columnViews["col1"] = &columns.ColumnView{}
	tableView.columnViews["col2"] = &columns.ColumnView{}
	tableView.columnViews["col3"] = &columns.ColumnView{}
	tableView.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView.blocksByColumn = make(map[string][]*grouping.Block)

	// Group by all three columns
	columns := []string{"col1", "col2", "col3"}
	tableView.GroupTable(nil, columns, []string{}, make(map[string]Compare), make(map[string]bool))

	// Level 1: should have 2 groups (A, B)
	if len(tableView.firstBlock.Groups) != 2 {
		t.Errorf("Expected 2 first-level groups, got %d", len(tableView.firstBlock.Groups))
	}

	// Level 2: verify col2 grouping
	col2Grouped, exists := tableView.groupedColumns["col2"]
	if !exists {
		t.Fatal("Expected col2 to be in groupedColumns")
	}
	if col2Grouped.Level != 1 {
		t.Errorf("Expected col2 level to be 1, got %d", col2Grouped.Level)
	}

	// Level 3: verify col3 grouping
	col3Grouped, exists := tableView.groupedColumns["col3"]
	if !exists {
		t.Fatal("Expected col3 to be in groupedColumns")
	}
	if col3Grouped.Level != 2 {
		t.Errorf("Expected col3 level to be 2, got %d", col3Grouped.Level)
	}

	// Verify all indices are accounted for at the deepest level
	totalIndices := 0
	for _, block := range col3Grouped.Blocks {
		for _, group := range block.Groups {
			totalIndices += len(group.Indices)
		}
	}

	if totalIndices != 4 {
		t.Errorf("Expected 4 total indices at deepest level, got %d", totalIndices)
	}

	t.Log("Three-level grouping structure:")
	t.Logf("Level 1 (col1): %d groups", len(tableView.firstBlock.Groups))
	t.Logf("Level 2 (col2): %d blocks", len(col2Grouped.Blocks))
	t.Logf("Level 3 (col3): %d blocks", len(col3Grouped.Blocks))
}
