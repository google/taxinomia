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
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
	"github.com/google/taxinomia/core/query"
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
	baseTable       *DataTable
	tableName       string
	VisibleColumns  []string
	joins           map[string]columns.IJoinedDataColumn
	computedColumns map[string]columns.IDataColumn

	groupedColumns map[string]*grouping.GroupedColumn
	groupingOrder  []string
	blocksByColumn map[string][]*grouping.Block
	columnViews    map[string]*columns.ColumnView
	firstBlock     *grouping.Block

	// Filtering
	filterMask  []bool            // Cached filter mask (nil = no filter, all rows shown)
	lastFilters map[string]string // Filters that produced current mask (for change detection)

	// Grouping cache tracking
	lastGroupingOrder   []string          // Grouping order when grouping was computed
	lastGroupingFilters map[string]string // Filter state when grouping was computed
	lastGroupingSortAsc map[string]bool   // Sort direction when grouping was computed
}

// ApplyFilters builds and caches a filter mask based on the provided filters
// Each filter is a column name mapped to a filter value
// Filter matching:
//   - If filter value is enclosed in double quotes (e.g., "exact"), performs case-sensitive exact match
//   - Otherwise, performs case-insensitive substring match
//
// All filters must match (AND logic) for a row to pass
//
// Optimization: Processes each column once, applying filter logic column-by-column
// rather than row-by-row. This minimizes redundant condition checks and improves
// cache locality when accessing column data sequentially.
//
// Caching: Skips recomputation if filters are identical to the previous call.
func (t *TableView) ApplyFilters(filters map[string]string) {
	// Check if filters are unchanged - skip recomputation
	if t.filtersEqual(filters) {
		return
	}

	// If no filters, clear the mask
	if len(filters) == 0 {
		t.filterMask = nil
		t.lastFilters = nil
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

		// Check for multi-value filter (pipe-separated exact matches)
		if strings.Contains(filterValue, "|") {
			// Multi-value OR filter - match any of the pipe-separated values (exact match)
			values := strings.Split(filterValue, "|")
			valueSet := make(map[string]bool, len(values))
			for _, v := range values {
				valueSet[v] = true
			}
			for i := 0; i < t.baseTable.Length(); i++ {
				if !t.filterMask[i] {
					continue
				}
				rowValue, err := col.GetString(uint32(i))
				if err != nil || !valueSet[rowValue] {
					t.filterMask[i] = false
				}
			}
		} else {
			// Single value filter - determine filter type
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

	// Save the filters that produced this mask
	t.lastFilters = make(map[string]string, len(filters))
	for k, v := range filters {
		t.lastFilters[k] = v
	}
}

// filtersEqual checks if the provided filters match the last applied filters
func (t *TableView) filtersEqual(filters map[string]string) bool {
	if len(filters) != len(t.lastFilters) {
		return false
	}
	for k, v := range filters {
		if t.lastFilters[k] != v {
			return false
		}
	}
	return true
}

// ClearFilters removes the active filter mask
func (t *TableView) ClearFilters() {
	t.filterMask = nil
	t.lastFilters = nil
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
					if errors.Is(err, columns.ErrUnmatched) {
						row[colName] = columns.UnmatchedLabel
					} else {
						row[colName] = columns.ErrorLabel
					}
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
	t.lastGroupingOrder = nil
	t.lastGroupingFilters = nil
	t.lastGroupingSortAsc = nil
}

func (t *TableView) GroupTable(groupingOrder []string, aggregatedColumns []string, compare map[string]Compare, asc map[string]bool) {
	// Check if grouping inputs are unchanged - skip recomputation
	if t.groupingEqual(groupingOrder, asc) {
		return
	}

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

	// Process first column
	// groupedTable.columns = columns
	parentBlocks := t.groupFirstColumnInTable(indices)
	t.firstBlock = parentBlocks[0]

	// Sort first column groups
	firstColumn := groupingOrder[0]
	ascending, hasSort := asc[firstColumn]
	descending := hasSort && !ascending // default to ascending if not specified
	t.sortGroupsInBlock(t.firstBlock, descending)

	// Process subsequent columns
	t.groupSubsequentColumnsInTable(indices, t.groupingOrder[1:], parentBlocks, asc)

	// Save the state that produced this grouping
	t.lastGroupingOrder = make([]string, len(groupingOrder))
	copy(t.lastGroupingOrder, groupingOrder)
	t.lastGroupingFilters = make(map[string]string, len(t.lastFilters))
	for k, v := range t.lastFilters {
		t.lastGroupingFilters[k] = v
	}
	t.lastGroupingSortAsc = make(map[string]bool, len(asc))
	for k, v := range asc {
		t.lastGroupingSortAsc[k] = v
	}
}

// sortGroupsInBlock sorts the groups within a block based on their values
func (t *TableView) sortGroupsInBlock(block *grouping.Block, descending bool) {
	if block == nil || len(block.Groups) <= 1 {
		return
	}

	col := block.GroupedColumn.DataColumn
	sort.Slice(block.Groups, func(i, j int) bool {
		// Compare groups by their first index (all indices in a group have the same value)
		idxI := block.Groups[i].Indices[0]
		idxJ := block.Groups[j].Indices[0]
		cmp := columns.CompareAtIndex(col, idxI, idxJ)
		if descending {
			return cmp > 0
		}
		return cmp < 0
	})
}

// groupingEqual checks if the grouping inputs match the last computed grouping
func (t *TableView) groupingEqual(groupingOrder []string, asc map[string]bool) bool {
	// Check grouping order
	if len(groupingOrder) != len(t.lastGroupingOrder) {
		return false
	}
	for i, col := range groupingOrder {
		if t.lastGroupingOrder[i] != col {
			return false
		}
	}
	// Check if filter state matches what was used for grouping
	if len(t.lastFilters) != len(t.lastGroupingFilters) {
		return false
	}
	for k, v := range t.lastFilters {
		if t.lastGroupingFilters[k] != v {
			return false
		}
	}
	// Check if sort direction matches what was used for grouping
	if len(asc) != len(t.lastGroupingSortAsc) {
		return false
	}
	for k, v := range asc {
		if t.lastGroupingSortAsc[k] != v {
			return false
		}
	}
	return true
}

func (t *TableView) groupFirstColumnInTable(indices []uint32) []*grouping.Block {
	firstColumn := t.groupingOrder[0]
	columnView := t.columnViews[firstColumn]
	dataColumn := t.GetColumn(firstColumn)

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

	indicesByGroupKey, _ := dataColumn.GroupIndices(indices, columnView)
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

func (t *TableView) groupSubsequentColumnsInTable(indices []uint32, columns []string, parentBlocks []*grouping.Block, asc map[string]bool) {
	// for following columns, each parent group spawns a child block
	for level, col := range columns {
		dataColumn := t.GetColumn(col)
		columnView := t.columnViews[col]

		g := &grouping.GroupedColumn{
			DataColumn: dataColumn,
			ColumnView: columnView,
			Level:      level + 1,
			Tag:        "next",
		}

		t.groupedColumns[col] = g

		// Determine sort direction for this column
		ascending, hasSort := asc[col]
		descending := hasSort && !ascending // default to ascending if not specified

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
				indicesByGroupKey, _ := dataColumn.GroupIndices(parentGroup.Indices, columnView)
				for groupKey, groupIndices := range indicesByGroupKey {
					g2 := &grouping.Group{
						GroupKey:    groupKey,
						Indices:     groupIndices,
						ParentGroup: parentGroup,
						Block:       b,
					}
					b.Groups = append(b.Groups, g2)
				}

				// Sort groups within this block
				t.sortGroupsInBlock(b, descending)
			}
		}
		parentBlocks = g.Blocks
	}
}

// NewTableView creates a new TableView wrapping a DataTable
func NewTableView(baseTable *DataTable, tableName string) *TableView {
	return &TableView{
		baseTable:       baseTable,
		tableName:       tableName,
		joins:           make(map[string]columns.IJoinedDataColumn),
		computedColumns: make(map[string]columns.IDataColumn),
		columnViews:     make(map[string]*columns.ColumnView),
		groupedColumns:  make(map[string]*grouping.GroupedColumn),
		blocksByColumn:  make(map[string][]*grouping.Block),
	}
}

// AddComputedColumn adds a computed column to the view
func (tv *TableView) AddComputedColumn(name string, col columns.IDataColumn) {
	tv.computedColumns[name] = col
	// Also add a column view so the column can be used for grouping
	tv.columnViews[name] = &columns.ColumnView{}
}

// RemoveComputedColumn removes a computed column from the view
func (tv *TableView) RemoveComputedColumn(name string) {
	delete(tv.computedColumns, name)
	delete(tv.columnViews, name)
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

// GetColumn retrieves a column by name, checking base table, joined columns, and computed columns
func (tv *TableView) GetColumn(name string) columns.IDataColumn {
	// First check base table columns
	if col := tv.baseTable.GetColumn(name); col != nil {
		return col
	}
	// Then check view's joined columns
	if col, ok := tv.joins[name]; ok {
		return col
	}
	// Finally check computed columns
	if col, ok := tv.computedColumns[name]; ok {
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
// Joined columns are identified by the format:
// - Single hop: fromColumn.toTable.toColumn.selectedColumn (4 parts)
// - Multi hop: fromColumn.toTable.toColumn.fromColumn2.toTable2.toColumn2.selectedColumn (7 parts for 2 hops, etc.)
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
			// This is a joined column
			// Valid formats: 4 parts (1 hop), 7 parts (2 hops), 10 parts (3 hops), etc.
			// Pattern: 4 + 3*(n-1) = 3n + 1 parts for n hops
			parts := strings.Split(colName, ".")
			numParts := len(parts)
			// Check if it's a valid join path: (numParts - 1) must be divisible by 3
			if numParts >= 4 && (numParts-1)%3 == 0 {
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

		joinedColumn := tv.createChainedJoinedColumn(colName, resolver)
		if joinedColumn != nil {
			fmt.Printf("Adding joined column %s to table view\n", colName)
			tv.AddJoinedColumn(joinedColumn)
		}
	}

	// Debug: Print final state
	fmt.Printf("Joined Columns in TableView: %v\n", tv.GetJoinedColumnNames())
	fmt.Printf("All Columns in TableView: %v\n", tv.GetAllColumnNames())
	fmt.Printf("===============================================\n\n")
}

// createChainedJoinedColumn creates a joined column that may chain through multiple tables
// Format: fromColumn.toTable.toColumn.fromColumn2.toTable2.toColumn2...selectedColumn
func (tv *TableView) createChainedJoinedColumn(colName string, resolver JoinResolver) columns.IJoinedDataColumn {
	parts := strings.Split(colName, ".")
	numParts := len(parts)

	// Calculate number of hops: (numParts - 1) / 3
	numHops := (numParts - 1) / 3
	if numHops < 1 {
		return nil
	}

	type JoinWithJoiner interface {
		GetJoiner() columns.IJoiner
	}

	// Collect all joiners for the chain
	joiners := make([]columns.IJoiner, 0, numHops)
	currentTableName := tv.tableName
	var lastTargetTable string

	// Process each hop to collect joiners
	for hop := 0; hop < numHops; hop++ {
		// Calculate indices for this hop
		// Hop 0: parts[0]=fromCol, parts[1]=toTable, parts[2]=toCol
		// Hop 1: parts[3]=fromCol, parts[4]=toTable, parts[5]=toCol
		baseIdx := hop * 3
		fromColumn := parts[baseIdx]
		toTable := parts[baseIdx+1]
		toColumn := parts[baseIdx+2]

		// Build join key for this hop
		joinKey := fmt.Sprintf("%s.%s->%s.%s", currentTableName, fromColumn, toTable, toColumn)
		foundJoin := resolver.GetJoin(joinKey)

		if foundJoin == nil {
			fmt.Printf("Could not find join for key: %s\n", joinKey)
			return nil
		}

		joinWithJoiner, ok := foundJoin.(JoinWithJoiner)
		if !ok {
			fmt.Printf("Join does not have GetJoiner method: %s\n", joinKey)
			return nil
		}

		joiner := joinWithJoiner.GetJoiner()
		if joiner == nil {
			fmt.Printf("Join has nil joiner: %s\n", joinKey)
			return nil
		}
		joiners = append(joiners, joiner)
		lastTargetTable = toTable
		currentTableName = toTable
	}

	// Get the final target column (last part of the path)
	selectedColName := parts[numParts-1]
	targetTable := resolver.GetTable(lastTargetTable)
	if targetTable == nil {
		fmt.Printf("Could not find target table: %s\n", lastTargetTable)
		return nil
	}

	targetDataCol := targetTable.GetColumn(selectedColName)
	if targetDataCol == nil {
		fmt.Printf("Could not find target column: %s.%s\n", lastTargetTable, selectedColName)
		return nil
	}

	// Create the final joined column with either a single joiner or a chained joiner
	colDef := columns.NewColumnDef(
		colName,
		fmt.Sprintf("%s → %s", lastTargetTable, targetDataCol.ColumnDef().DisplayName()),
		"",
	)

	var joiner columns.IJoiner
	if len(joiners) == 1 {
		joiner = joiners[0]
	} else {
		joiner = columns.NewChainedJoiner(joiners...)
	}

	return targetDataCol.CreateJoinedColumn(colDef, joiner)
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

// GetLeafColumns returns the names of the non-grouped columns (leaf columns)
// These are columns that are not grouped - both filtered and others
// With filters, the visible columns order is: filtered → grouped → others
// Leaf columns include both filtered (displayed before grouped) and others (displayed after grouped)
// This maintains the display order: filtered leaves, grouped columns, other leaves
func (tv *TableView) GetLeafColumns() []string {
	var leafColumns []string
	for _, colName := range tv.VisibleColumns {
		if !tv.IsColGrouped(colName) {
			leafColumns = append(leafColumns, colName)
		}
	}
	return leafColumns
}

// IsColFiltered checks if a column has an active filter
func (tv *TableView) IsColFiltered(colName string) bool {
	if tv.lastFilters == nil {
		return false
	}
	_, hasFilter := tv.lastFilters[colName]
	return hasFilter
}

// GetFilteredLeafColumns returns the names of leaf columns that have active filters
// These are non-grouped columns with active filters
func (tv *TableView) GetFilteredLeafColumns() []string {
	var filteredLeafColumns []string
	for _, colName := range tv.VisibleColumns {
		if !tv.IsColGrouped(colName) && tv.IsColFiltered(colName) {
			filteredLeafColumns = append(filteredLeafColumns, colName)
		}
	}
	return filteredLeafColumns
}

// GetOtherLeafColumns returns the names of leaf columns that are not grouped
// (regardless of filter state - this returns all non-grouped columns except filtered ones)
func (tv *TableView) GetOtherLeafColumns() []string {
	var otherLeafColumns []string
	for _, colName := range tv.VisibleColumns {
		if !tv.IsColGrouped(colName) && !tv.IsColFiltered(colName) {
			otherLeafColumns = append(otherLeafColumns, colName)
		}
	}
	return otherLeafColumns
}

// ComputeAggregates computes aggregates for all groups in the hierarchy.
// It uses bottom-up aggregation: leaf groups compute from data, parent groups combine children.
// leafColumns specifies which columns to aggregate; columnTypes maps column names to types.
func (tv *TableView) ComputeAggregates(leafColumns []string, columnTypes map[string]query.ColumnType) {
	if tv.firstBlock == nil || len(leafColumns) == 0 {
		return
	}

	// Walk the hierarchy bottom-up, starting from leaves
	tv.computeAggregatesForBlock(tv.firstBlock, leafColumns, columnTypes)
}

// computeAggregatesForBlock recursively computes aggregates for a block and its children.
// Returns after processing all groups in the block.
func (tv *TableView) computeAggregatesForBlock(block *grouping.Block, leafColumns []string, columnTypes map[string]query.ColumnType) {
	if block == nil {
		return
	}

	for _, group := range block.Groups {
		// First, process child block (if any) - bottom-up
		if group.ChildBlock != nil {
			tv.computeAggregatesForBlock(group.ChildBlock, leafColumns, columnTypes)
		}

		// Now compute aggregates for this group
		group.Aggregates = make(map[string]aggregates.AggregateState)

		if group.ChildBlock == nil {
			// Leaf group: compute from indices
			tv.computeLeafAggregates(group, leafColumns, columnTypes)
		} else {
			// Parent group: combine from children
			tv.combineChildAggregates(group, leafColumns, columnTypes)
		}
	}
}

// computeLeafAggregates computes aggregates for a leaf group by iterating over its indices.
func (tv *TableView) computeLeafAggregates(group *grouping.Group, leafColumns []string, columnTypes map[string]query.ColumnType) {
	for _, colName := range leafColumns {
		colType := columnTypes[colName]
		col := tv.GetColumn(colName)
		if col == nil {
			continue
		}

		state := aggregates.CreateAggState(colType)

		// Add each value from the group's indices
		for _, idx := range group.Indices {
			switch colType {
			case query.ColumnTypeNumeric:
				if numState, ok := state.(*aggregates.NumericAggState); ok {
					tv.addNumericValue(numState, col, idx)
				}
			case query.ColumnTypeBool:
				if boolState, ok := state.(*aggregates.BoolAggState); ok {
					tv.addBoolValue(boolState, col, idx)
				}
			case query.ColumnTypeDatetime:
				if dtState, ok := state.(*aggregates.DatetimeAggState); ok {
					tv.addDatetimeValue(dtState, col, idx)
				}
			case query.ColumnTypeString:
				if strState, ok := state.(*aggregates.StringAggState); ok {
					tv.addStringValue(strState, col, idx)
				}
			}
		}

		group.Aggregates[colName] = state
	}
}

// combineChildAggregates combines aggregates from child groups into a parent group.
func (tv *TableView) combineChildAggregates(group *grouping.Group, leafColumns []string, columnTypes map[string]query.ColumnType) {
	for _, colName := range leafColumns {
		colType := columnTypes[colName]
		parentState := aggregates.CreateAggState(colType)

		// Combine from all child groups
		for _, childGroup := range group.ChildBlock.Groups {
			if childState, ok := childGroup.Aggregates[colName]; ok {
				parentState.Combine(childState)
			}
		}

		group.Aggregates[colName] = parentState
	}
}

// addNumericValue adds a numeric value from a column to the aggregate state.
func (tv *TableView) addNumericValue(state *aggregates.NumericAggState, col columns.IDataColumn, idx uint32) {
	// Try to get numeric value - check for typed columns
	switch typedCol := col.(type) {
	case *columns.Uint32Column:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.AddUint32(val)
		}
	case interface{ GetValue(uint32) (float64, error) }:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	case interface{ GetValue(uint32) (int64, error) }:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(float64(val))
		}
	default:
		// Fallback: try to parse string as number
		if strVal, err := col.GetString(idx); err == nil {
			var f float64
			if _, err := fmt.Sscanf(strVal, "%f", &f); err == nil {
				state.Add(f)
			}
		}
	}
}

// addBoolValue adds a boolean value from a column to the aggregate state.
func (tv *TableView) addBoolValue(state *aggregates.BoolAggState, col columns.IDataColumn, idx uint32) {
	switch typedCol := col.(type) {
	case *columns.BoolColumn:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	case interface{ GetValue(uint32) (bool, error) }:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	default:
		// Fallback: parse string
		if strVal, err := col.GetString(idx); err == nil {
			if val, err := columns.ParseBool(strVal); err == nil {
				state.Add(val)
			}
		}
	}
}

// addDatetimeValue adds a datetime value from a column to the aggregate state.
func (tv *TableView) addDatetimeValue(state *aggregates.DatetimeAggState, col columns.IDataColumn, idx uint32) {
	switch typedCol := col.(type) {
	case *columns.DatetimeColumn:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	case interface{ GetValue(uint32) (time.Time, error) }:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	default:
		// Fallback: parse string
		if strVal, err := col.GetString(idx); err == nil {
			if val, err := columns.ParseDatetime(strVal, time.UTC); err == nil && !val.IsZero() {
				state.Add(val)
			}
		}
	}
}

// addStringValue adds a string value from a column to the aggregate state.
func (tv *TableView) addStringValue(state *aggregates.StringAggState, col columns.IDataColumn, idx uint32) {
	if strVal, err := col.GetString(idx); err == nil {
		state.Add(strVal)
	}
}

// GetColumnType determines the column type for aggregate purposes.
func (tv *TableView) GetColumnType(colName string) query.ColumnType {
	col := tv.GetColumn(colName)
	if col == nil {
		return query.ColumnTypeString
	}

	// Check concrete types
	switch col.(type) {
	case *columns.Uint32Column:
		return query.ColumnTypeNumeric
	case *columns.BoolColumn:
		return query.ColumnTypeBool
	case *columns.DatetimeColumn:
		return query.ColumnTypeDatetime
	case *columns.StringColumn:
		return query.ColumnTypeString
	case *columns.DurationColumn:
		return query.ColumnTypeDatetime // Duration treated like datetime for aggregation
	// Computed column types
	case *columns.ComputedUint32Column:
		return query.ColumnTypeNumeric
	case *columns.ComputedFloat64Column:
		return query.ColumnTypeNumeric
	case *columns.ComputedInt64Column:
		return query.ColumnTypeNumeric
	case *columns.ComputedStringColumn:
		return query.ColumnTypeString
	}

	// Check for typed column interfaces (for computed/joined columns)
	switch col.(type) {
	case interface{ GetValue(uint32) (uint32, error) }:
		return query.ColumnTypeNumeric
	case interface{ GetValue(uint32) (int64, error) }:
		return query.ColumnTypeNumeric
	case interface{ GetValue(uint32) (float64, error) }:
		return query.ColumnTypeNumeric
	case interface{ GetValue(uint32) (bool, error) }:
		return query.ColumnTypeBool
	case interface{ GetValue(uint32) (time.Time, error) }:
		return query.ColumnTypeDatetime
	}

	return query.ColumnTypeString
}

// SortGroupsByAggregate re-sorts groups in a block by their aggregate value.
// This should be called after ComputeAggregates.
// groupAggSorts maps grouped column names to their aggregate sort specification.
func (tv *TableView) SortGroupsByAggregate(groupAggSorts map[string]*query.GroupAggSort) {
	if tv.firstBlock == nil || len(groupAggSorts) == 0 {
		return
	}

	// Walk through grouping hierarchy and sort blocks that have aggregate sort specified
	tv.sortBlockByAggregate(tv.firstBlock, groupAggSorts)
}

// sortBlockByAggregate recursively sorts groups in a block and its children by aggregate values.
func (tv *TableView) sortBlockByAggregate(block *grouping.Block, groupAggSorts map[string]*query.GroupAggSort) {
	if block == nil {
		return
	}

	// Get the grouped column name for this block
	groupedColName := ""
	for name, gc := range tv.groupedColumns {
		if gc == block.GroupedColumn {
			groupedColName = name
			break
		}
	}

	// Check if this grouped column has an aggregate sort
	if aggSort, ok := groupAggSorts[groupedColName]; ok && aggSort != nil {
		// Sort groups based on sort type
		sort.Slice(block.Groups, func(i, j int) bool {
			var cmp int

			switch aggSort.AggType {
			case query.AggRowCount:
				// Sort by total row count in the group
				countI := tv.getGroupRowCount(block.Groups[i])
				countJ := tv.getGroupRowCount(block.Groups[j])
				if countI < countJ {
					cmp = -1
				} else if countI > countJ {
					cmp = 1
				}
			case query.AggSubgroupCount:
				// Sort by number of subgroups
				countI := tv.getGroupSubgroupCount(block.Groups[i])
				countJ := tv.getGroupSubgroupCount(block.Groups[j])
				if countI < countJ {
					cmp = -1
				} else if countI > countJ {
					cmp = 1
				}
			default:
				// Sort by leaf column aggregate value
				stateI := block.Groups[i].Aggregates[aggSort.LeafColumn]
				stateJ := block.Groups[j].Aggregates[aggSort.LeafColumn]
				cmp = tv.compareAggregateValues(stateI, stateJ, aggSort.AggType)
			}

			if aggSort.Descending {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	// Recursively sort child blocks
	for _, group := range block.Groups {
		if group.ChildBlock != nil {
			tv.sortBlockByAggregate(group.ChildBlock, groupAggSorts)
		}
	}
}

// getGroupRowCount returns the total number of rows in a group (recursively counting leaf indices).
func (tv *TableView) getGroupRowCount(group *grouping.Group) int {
	if group.ChildBlock == nil {
		// Leaf group - return direct index count
		return len(group.Indices)
	}
	// Parent group - sum up all child group row counts
	total := 0
	for _, childGroup := range group.ChildBlock.Groups {
		total += tv.getGroupRowCount(childGroup)
	}
	return total
}

// getGroupSubgroupCount returns the number of direct subgroups in a group.
func (tv *TableView) getGroupSubgroupCount(group *grouping.Group) int {
	if group.ChildBlock == nil {
		return 0
	}
	return len(group.ChildBlock.Groups)
}

// compareAggregateValues compares two aggregate states for a specific aggregate type.
// Returns -1 if a < b, 0 if equal, 1 if a > b.
func (tv *TableView) compareAggregateValues(a, b aggregates.AggregateState, aggType query.AggregateType) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1 // nil sorts last
	}
	if b == nil {
		return -1
	}

	// Extract comparable values based on aggregate type
	valA := tv.getAggregateNumericValue(a, aggType)
	valB := tv.getAggregateNumericValue(b, aggType)

	if valA < valB {
		return -1
	}
	if valA > valB {
		return 1
	}
	return 0
}

// getAggregateNumericValue extracts a numeric value from an aggregate state for comparison.
func (tv *TableView) getAggregateNumericValue(state aggregates.AggregateState, aggType query.AggregateType) float64 {
	switch s := state.(type) {
	case *aggregates.NumericAggState:
		switch aggType {
		case query.AggCount:
			return float64(s.Count)
		case query.AggSum:
			return s.Sum
		case query.AggAvg:
			return s.Avg()
		case query.AggStdDev:
			return s.StdDev()
		case query.AggMin:
			return s.Min
		case query.AggMax:
			return s.Max
		}
	case *aggregates.BoolAggState:
		switch aggType {
		case query.AggCount:
			return float64(s.Count)
		case query.AggTrue:
			return float64(s.TrueCount)
		case query.AggFalse:
			return float64(s.FalseCount)
		case query.AggRatio:
			return s.Ratio()
		}
	case *aggregates.StringAggState:
		switch aggType {
		case query.AggCount:
			return float64(s.Count)
		case query.AggUnique:
			return float64(s.UniqueCount())
		}
	case *aggregates.DatetimeAggState:
		switch aggType {
		case query.AggCount:
			return float64(s.Count)
		case query.AggMin:
			return float64(s.Min) // epoch nanoseconds
		case query.AggMax:
			return float64(s.Max) // epoch nanoseconds
		case query.AggAvg:
			// Average time as epoch nanoseconds
			if s.Count == 0 {
				return 0
			}
			return s.Sum / float64(s.Count)
		case query.AggStdDev:
			return float64(s.StdDev()) // duration in nanoseconds
		case query.AggSpan:
			return float64(s.Max - s.Min) // duration in nanoseconds
		}
	}
	return 0
}
