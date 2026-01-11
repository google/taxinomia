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

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
)

// JoinResolver is an interface for resolving join and table information
// The GetJoin method returns an object that has a GetJoiner() method
type JoinResolver interface {
	GetJoin(key string) interface{} // Returns object with GetJoiner() columns.IJoiner method
	GetTable(name string) *DataTable
}

// Compare is a placeholder type for comparison functions (WIP)
type Compare int

// TableView represents a view of a DataTable with additional joined columns.
// This allows the underlying DataTable to remain immutable while views can
// have their own set of joined columns based on the query context.
type TableView struct {
	baseTable      *DataTable
	tableName      string
	VisibleColumns []string
	joins          map[string]columns.IJoinedDataColumn

	groupedColumns map[string]*grouping.GroupedColumn
	groupingOrder  []string
	blocksByColumn map[string][]*grouping.Block
	columnViews    map[string]*columns.ColumnView
	firstBlock     *grouping.Block

	// Filtering
	filterMask []bool // Cached filter mask (nil = no filter, all rows shown)
}

// ApplyFilters builds and caches a filter mask based on the provided filters
// Each filter is a column name mapped to a filter value
// Filter matching:
//   - If filter value is enclosed in double quotes (e.g., "exact"), performs case-sensitive exact match
//   - Otherwise, performs case-insensitive substring match
// All filters must match (AND logic) for a row to pass
//
// Optimization: Processes each column once, applying filter logic column-by-column
// rather than row-by-row. This minimizes redundant condition checks and improves
// cache locality when accessing column data sequentially.
func (t *TableView) ApplyFilters(filters map[string]string) {
	// If no filters, clear the mask
	if len(filters) == 0 {
		t.filterMask = nil
		return
	}

	// Initialize filter mask - start with all rows passing
	t.filterMask = make([]bool, t.baseTable.Length())
	for i := range t.filterMask {
		t.filterMask[i] = true
	}

	// Apply each filter one column at a time
	for colName, filterValue := range filters {
		col := t.GetColumn(colName)
		if col == nil {
			// Column not found - no rows pass
			for i := range t.filterMask {
				t.filterMask[i] = false
			}
			return
		}

		// Determine filter type once per column
		isExactMatch := len(filterValue) >= 2 && filterValue[0] == '"' && filterValue[len(filterValue)-1] == '"'

		if isExactMatch {
			// Exact match (case-sensitive) - strip quotes
			exactValue := filterValue[1 : len(filterValue)-1]
			for i := 0; i < t.baseTable.Length(); i++ {
				if !t.filterMask[i] {
					continue
				}
				rowValue, err := col.GetString(uint32(i))
				if err != nil || rowValue != exactValue {
					t.filterMask[i] = false
				}
			}
		} else {
			// Substring match (case-insensitive)
			substringValue := strings.ToLower(filterValue)
			for i := 0; i < t.baseTable.Length(); i++ {
				if !t.filterMask[i] {
					continue
				}
				rowValue, err := col.GetString(uint32(i))
				if err != nil || !strings.Contains(strings.ToLower(rowValue), substringValue) {
					t.filterMask[i] = false
				}
			}
		}
	}
}

// ClearFilters removes the active filter mask
func (t *TableView) ClearFilters() {
	t.filterMask = nil
}

// GetFilteredRowCount returns the number of rows that pass the current filter
// Returns total row count if no filter is active
func (t *TableView) GetFilteredRowCount() int {
	if t.filterMask == nil {
		return t.baseTable.Length()
	}
	count := 0
	for _, passes := range t.filterMask {
		if passes {
			count++
		}
	}
	return count
}

// GetFilteredIndices returns the indices of rows that pass the current filter
// Returns all indices if no filter is active
func (t *TableView) GetFilteredIndices() []uint32 {
	if t.filterMask == nil {
		// No filter - return all indices
		indices := make([]uint32, t.baseTable.Length())
		for i := 0; i < t.baseTable.Length(); i++ {
			indices[i] = uint32(i)
		}
		return indices
	}

	// Filter active - return indices that pass
	indices := make([]uint32, 0, t.GetFilteredRowCount())
	for i, passes := range t.filterMask {
		if passes {
			indices = append(indices, uint32(i))
		}
	}
	return indices
}

// GetFilteredRows returns rows as maps of column name to string value
// Returns only rows that pass the current filter, up to the specified limit
// If limit <= 0, returns all filtered rows
func (t *TableView) GetFilteredRows(columnNames []string, limit int) []map[string]string {
	filteredIndices := t.GetFilteredIndices()

	// Determine how many rows to return
	rowCount := len(filteredIndices)
	if limit > 0 && limit < rowCount {
		rowCount = limit
	}

	rows := make([]map[string]string, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		rowIndex := filteredIndices[i]
		row := make(map[string]string)
		for _, colName := range columnNames {
			col := t.GetColumn(colName)
			if col != nil {
				value, err := col.GetString(rowIndex)
				if err != nil {
					row[colName] = "[error]"
				} else {
					row[colName] = value
				}
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func (t *TableView) NumRows() int {
	return t.baseTable.Length()
}

func (t *TableView) GetGroupCount(col string) int {
	return t.groupedColumns[col].GetGroupCount()
}

func (t *TableView) ClearGroupings() {
	t.groupedColumns = make(map[string]*grouping.GroupedColumn)
	t.firstBlock = nil
}

func (t *TableView) GroupTable(groupingOrder []string, aggregatedColumns []string, compare map[string]Compare, asc map[string]bool) {
	// clear current groups
	t.groupedColumns = make(map[string]*grouping.GroupedColumn)
	t.firstBlock = nil

	// get indices from cached filter mask
	t.groupingOrder = groupingOrder
	indices := []uint32{}
	if t.filterMask == nil {
		// No filter - include all rows
		indices = make([]uint32, t.baseTable.Length())
		for i := 0; i < t.baseTable.Length(); i++ {
			indices[i] = uint32(i)
		}
	} else {
		// Use filter mask to select rows
		for i, passes := range t.filterMask {
			if passes {
				indices = append(indices, uint32(i))
			}
		}
	}
	// groupedColumns: map[string]*GroupedColumn{},
	// 	//groupsByColumn: map[string][]*Group2{},
	// 	blocksByColumn: map[string][]*Block{},

	// Process first column
	//groupedTable.columns = columns
	parentBlocks := t.groupFirstColumnInTable(indices)
	t.firstBlock = parentBlocks[0]

	// Process subsequent columns
	t.groupSubsequentColumnsInTable(indices, t.groupingOrder[1:], parentBlocks)

}

func (t *TableView) groupFirstColumnInTable(indices []uint32) []*grouping.Block {
	// TODO this is limited to base table columns for now
	firstColumn := t.groupingOrder[0]
	columnView := t.columnViews[firstColumn]
	dataColumn := t.baseTable.columns[firstColumn]

	g := &grouping.GroupedColumn{
		DataColumn: dataColumn,
		ColumnView: columnView,
		Level:      0,
		Tag:        "first",
	}

	t.groupedColumns[firstColumn] = g

	b := &grouping.Block{
		Groups:        nil,
		ParentGroup:   nil,
		GroupedColumn: g,
	}
	g.Blocks = append(g.Blocks, b)
	t.blocksByColumn[firstColumn] = append(t.blocksByColumn[firstColumn], b)

	indicesByGroupKey := dataColumn.GroupIndices(indices, columnView)
	for groupKey, groupIndices := range indicesByGroupKey {
		g2 := &grouping.Group{
			GroupKey:    groupKey,
			Indices:     groupIndices,
			ParentGroup: nil,
			Block:       b,
		}
		b.Groups = append(b.Groups, g2)
	}

	return []*grouping.Block{b}
}

func (t *TableView) groupSubsequentColumnsInTable(indices []uint32, columns []string, parentBlocks []*grouping.Block) {
	// for following columns, each parent group spawns a child block
	for level, col := range columns {
		dataColumn := t.baseTable.GetColumn(col)
		columnView := t.columnViews[col]

		g := &grouping.GroupedColumn{
			DataColumn: dataColumn,
			ColumnView: columnView,
			Level:      level + 1,
			Tag:        "next",
		}

		t.groupedColumns[col] = g

		// every parent group spawns a block
		for _, parentBlock := range parentBlocks {
			for _, parentGroup := range parentBlock.Groups {
				b := &grouping.Block{
					ParentGroup:   parentGroup,
					GroupedColumn: g,
				}
				g.Blocks = append(g.Blocks, b)
				t.blocksByColumn[col] = append(t.blocksByColumn[col], b)

				// Link the parent group to this child block
				parentGroup.ChildBlock = b

				// now group within the parent group
				indicesByGroupKey := dataColumn.GroupIndices(parentGroup.Indices, columnView)
				for groupKey, groupIndices := range indicesByGroupKey {
					g2 := &grouping.Group{
						GroupKey:    groupKey,
						Indices:     groupIndices,
						ParentGroup: parentGroup,
						Block:       b,
					}
					b.Groups = append(b.Groups, g2)
				}
			}
		}
		parentBlocks = g.Blocks
	}
}

// NewTableView creates a new TableView wrapping a DataTable
func NewTableView(baseTable *DataTable, tableName string) *TableView {
	return &TableView{
		baseTable:      baseTable,
		tableName:      tableName,
		joins:          make(map[string]columns.IJoinedDataColumn),
		columnViews:    make(map[string]*columns.ColumnView),
		groupedColumns: make(map[string]*grouping.GroupedColumn),
		blocksByColumn: make(map[string][]*grouping.Block),
	}
}

// GetBaseTable returns the underlying immutable DataTable
func (tv *TableView) GetBaseTable() *DataTable {
	return tv.baseTable
}

// AddJoinedColumn adds a joined column to this view
func (tv *TableView) AddJoinedColumn(joinedCol columns.IJoinedDataColumn) {
	tv.joins[joinedCol.ColumnDef().Name()] = joinedCol
}

// RemoveJoinedColumn removes a joined column from this view
func (tv *TableView) RemoveJoinedColumn(name string) {
	delete(tv.joins, name)
}

// GetColumn retrieves a column by name, checking both base table and joined columns
func (tv *TableView) GetColumn(name string) columns.IDataColumn {
	// First check base table columns
	if col := tv.baseTable.GetColumn(name); col != nil {
		return col
	}
	// Then check view's joined columns
	if col, ok := tv.joins[name]; ok {
		return col
	}
	return nil
}

// GetColumnNames returns column names from the base table only
func (tv *TableView) GetColumnNames() []string {
	return tv.baseTable.GetColumnNames()
}

// GetAllColumnNames returns all column names including joined columns in this view
func (tv *TableView) GetAllColumnNames() []string {
	names := make([]string, 0, len(tv.baseTable.columns)+len(tv.joins))

	// Add regular columns from base table
	for name := range tv.baseTable.columns {
		names = append(names, name)
	}

	// Add joined columns from this view
	for name := range tv.joins {
		names = append(names, name)
	}

	return names
}

// GetJoinedColumnNames returns only joined column names in this view
func (tv *TableView) GetJoinedColumnNames() []string {
	names := make([]string, 0, len(tv.joins))
	for name := range tv.joins {
		names = append(names, name)
	}
	return names
}

// UpdateJoinedColumns updates the joined columns in this view to match the requested columns
// It adds new joined columns and removes ones that are no longer needed
// Joined columns are identified by the format: fromColumn.toTable.toColumn.selectedColumn
func (tv *TableView) UpdateJoinedColumns(columnNames []string, resolver JoinResolver) {
	// Debug: Print processing info
	fmt.Printf("\n=== UpdateJoinedColumns Debug Info ===\n")
	fmt.Printf("Table: %s\n", tv.tableName)
	fmt.Printf("Columns to process: %v\n", columnNames)

	// Track which joined columns we need
	neededJoinedColumns := make(map[string]bool)

	// Parse columns to identify joined ones (with dots)
	for _, colName := range columnNames {
		if strings.Contains(colName, ".") {
			// This is a joined column - format: fromColumn.toTable.toColumn.selectedColumn
			parts := strings.Split(colName, ".")
			if len(parts) == 4 {
				neededJoinedColumns[colName] = true
			}
		}
	}

	// Remove joined columns that are no longer needed
	currentJoinedColumns := tv.GetJoinedColumnNames()
	for _, colName := range currentJoinedColumns {
		if !neededJoinedColumns[colName] {
			fmt.Printf("Removing joined column %s from table view\n", colName)
			tv.RemoveJoinedColumn(colName)
		}
	}

	// Add needed joined columns that aren't already in the view
	for colName := range neededJoinedColumns {
		// Skip if already exists
		if tv.joins[colName] != nil {
			continue
		}
		// Parse the column name
		parts := strings.Split(colName, ".")
		if len(parts) != 4 {
			continue
		}

		fromColumn := parts[0]
		toTable := parts[1]
		toColumn := parts[2]
		selectedColumn := parts[3]

		// Find the join that connects these tables
		// Build the join key to look up directly
		joinKey := fmt.Sprintf("%s.%s->%s.%s", tv.tableName, fromColumn, toTable, toColumn)
		foundJoin := resolver.GetJoin(joinKey)

		if foundJoin != nil {
			// Create the joined column
			targetTable := resolver.GetTable(toTable)
			if targetTable != nil {
				targetDataCol := targetTable.GetColumn(selectedColumn)
				if targetDataCol != nil {
					colDef := columns.NewColumnDef(
						colName,
						fmt.Sprintf("%s %s", toTable, targetDataCol.ColumnDef().DisplayName()),
						"", // Joined columns don't have entity types
					)

					// Get the joiner from the join info using type assertion
					// We expect the join object to have a GetJoiner() method
					type JoinWithJoiner interface {
						GetJoiner() columns.IJoiner
					}

					if joinWithJoiner, ok := foundJoin.(JoinWithJoiner); ok {
						joiner := joinWithJoiner.GetJoiner()
						joinedColumn := targetDataCol.CreateJoinedColumn(colDef, joiner)

						fmt.Printf("Adding joined column %s to table view\n", colName)
						tv.AddJoinedColumn(joinedColumn)
					}
				}
			}
		}
	}

	// Debug: Print final state
	fmt.Printf("Joined Columns in TableView: %v\n", tv.GetJoinedColumnNames())
	fmt.Printf("All Columns in TableView: %v\n", tv.GetAllColumnNames())
	fmt.Printf("===============================================\n\n")
}

// IsGrouped returns true if the table has active grouping
func (tv *TableView) IsGrouped() bool {
	return len(tv.groupedColumns) > 0
	//	return tv.firstBlock != nil
}

func (tv *TableView) IsColGrouped(colName string) bool {
	_, ok := tv.groupedColumns[colName]
	return ok
}

// GetFirstBlock returns the first block of the grouping hierarchy
// Returns nil if no grouping is active
func (tv *TableView) GetFirstBlock() *grouping.Block {
	return tv.firstBlock
}

// GetGroupingOrder returns the ordered list of grouped column names
func (tv *TableView) GetGroupingOrder() []string {
	return tv.groupingOrder
}

// GetLeafColumns returns the names of the non-grouped columns (aggregated/leaf columns)
// These are all columns that are not in the grouping order
func (tv *TableView) GetLeafColumns() []string {
	fmt.Println()
	fmt.Println("tv.groupedColumns", tv.groupedColumns)
	fmt.Println("tv.VisibleColumns", tv.VisibleColumns)
	src := tv.VisibleColumns[len(tv.groupedColumns):]
	leafColumns := make([]string, len(src))
	copy(leafColumns, src)
	fmt.Println("leafColumns", leafColumns)
	return leafColumns
}
