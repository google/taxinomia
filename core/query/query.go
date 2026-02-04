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

package query

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/google/safehtml"
)

// ComputedColumnDef represents a computed column definition from URL
type ComputedColumnDef struct {
	Name       string // Column name
	Expression string // Expression as entered by user (e.g., "add(price,qty)")
}

// SortColumn represents a column in the sort order with its direction
type SortColumn struct {
	Name       string // Column name
	Descending bool   // true = descending (-), false = ascending (+)
}

// GroupAggSort specifies how a grouped column should be sorted by an aggregate value
type GroupAggSort struct {
	GroupedColumn string        // The grouped column to sort
	LeafColumn    string        // The leaf column whose aggregate to sort by
	AggType       AggregateType // The aggregate type to sort by
	Descending    bool          // Sort direction
}

// AggregateType represents a type of aggregate function
type AggregateType string

// Aggregate types for numeric columns
const (
	AggSum    AggregateType = "sum"    // Sum of values
	AggAvg    AggregateType = "avg"    // Average/mean
	AggStdDev AggregateType = "stddev" // Standard deviation
	AggMin    AggregateType = "min"    // Minimum value
	AggMax    AggregateType = "max"    // Maximum value
	AggCount  AggregateType = "count"  // Count of non-null values
)

// Aggregate types for string columns
const (
	AggUnique AggregateType = "unique" // Count of unique values
)

// Aggregate types for boolean columns
const (
	AggTrue  AggregateType = "true"  // Count of true values
	AggFalse AggregateType = "false" // Count of false values
	AggRatio AggregateType = "ratio" // Ratio of true to total
)

// Aggregate types for datetime columns (also uses min, max, avg, stddev)
const (
	AggSpan AggregateType = "span" // Time span (max - min)
)

// Special aggregate types for group-level sorting (not leaf column aggregates)
const (
	AggRowCount      AggregateType = "rows"      // Total row count in group
	AggSubgroupCount AggregateType = "subgroups" // Number of subgroups
)

// ColumnType represents the data type of a column for aggregate purposes
type ColumnType string

const (
	ColumnTypeNumeric  ColumnType = "numeric"
	ColumnTypeString   ColumnType = "string"
	ColumnTypeBool     ColumnType = "bool"
	ColumnTypeDatetime ColumnType = "datetime"
)

// GetAvailableAggregates returns the list of available aggregate types for a column type
func GetAvailableAggregates(colType ColumnType) []AggregateType {
	switch colType {
	case ColumnTypeNumeric:
		return []AggregateType{AggCount, AggSum, AggAvg, AggStdDev, AggMin, AggMax}
	case ColumnTypeString:
		return []AggregateType{AggCount, AggUnique, AggMin, AggMax}
	case ColumnTypeBool:
		return []AggregateType{AggCount, AggTrue, AggFalse, AggRatio}
	case ColumnTypeDatetime:
		return []AggregateType{AggCount, AggMin, AggMax, AggAvg, AggStdDev, AggSpan}
	default:
		return []AggregateType{AggCount}
	}
}

// AggregateSymbol returns the display symbol for an aggregate type
func AggregateSymbol(agg AggregateType) string {
	switch agg {
	case AggSum:
		return "Σ"
	case AggAvg:
		return "μ"
	case AggStdDev:
		return "σ"
	case AggMin:
		return "↓"
	case AggMax:
		return "↑"
	case AggCount:
		return "#"
	case AggUnique:
		return "◇"
	case AggTrue:
		return "✓"
	case AggFalse:
		return "✗"
	case AggRatio:
		return "%"
	case AggSpan:
		return "Δ"
	case AggRowCount:
		return "≡"
	case AggSubgroupCount:
		return "⊞"
	default:
		return string(agg)
	}
}

// AggregateTitle returns the tooltip title for an aggregate type
func AggregateTitle(agg AggregateType) string {
	switch agg {
	case AggSum:
		return "Sum"
	case AggAvg:
		return "Average"
	case AggStdDev:
		return "Standard Deviation"
	case AggMin:
		return "Minimum"
	case AggMax:
		return "Maximum"
	case AggCount:
		return "Count"
	case AggUnique:
		return "Unique Values"
	case AggTrue:
		return "True Count"
	case AggFalse:
		return "False Count"
	case AggRatio:
		return "True Ratio"
	case AggSpan:
		return "Time Span"
	case AggRowCount:
		return "Row Count"
	case AggSubgroupCount:
		return "Subgroup Count"
	default:
		return string(agg)
	}
}

// Query represents the parsed state of a table view URL
type Query struct {
	// Base path (e.g., "/table")
	Path string

	// Core parameters
	Table              string                       // The table being viewed
	Columns            []string                     // Ordered list of visible columns (reordered: filtered, grouped, then others)
	ColumnWidths       map[string]int               // Column widths in pixels (columnName -> width)
	Expanded           []string                     // List of expanded paths in the sidebar
	GroupedColumns     []string                     // Ordered list of columns to group by
	Filters            map[string]string            // Column filters (columnName -> filterValue)
	Limit              int                          // Number of rows to display (0 = show all)
	ComputedColumns    []ComputedColumnDef          // Computed column definitions
	SortOrder          []SortColumn                 // Ordered list of sort columns (all visible columns with +/- direction)
	AggregateSettings  map[string][]AggregateType   // Enabled aggregates per column (columnName -> list of enabled aggregates)
	GroupAggregateSorts map[string]*GroupAggSort    // Aggregate sort for grouped columns (groupedColumn -> sort spec)

	// UI state
	ShowInfoPane bool   // Whether the info pane is visible (default: true)
	InfoPaneTab  string // Active tab in info pane ("url" or "perf")
}

// NewQuery creates a Query from a URL
func NewQuery(u *url.URL) *Query {
	// The URL is already parsed and safe to use since it comes from http.Request
	// No additional sanitization needed here as we're just extracting query parameters

	state := &Query{
		Path:                u.Path,
		Filters:             make(map[string]string),
		ColumnWidths:        make(map[string]int),
		AggregateSettings:   make(map[string][]AggregateType),
		GroupAggregateSorts: make(map[string]*GroupAggSort),
		Limit:               25,     // Default limit
		ShowInfoPane:        true,   // Default to showing info pane
		InfoPaneTab:         "url",  // Default to URL tab
	}

	// Parse query parameters
	q := u.Query()

	// Extract table parameter
	state.Table = q.Get("table")

	// Extract columns parameter (format: col1:width,col2,col3:width)
	columnsStr := q.Get("columns")
	if columnsStr != "" {
		columnParts := strings.Split(columnsStr, ",")
		state.Columns = make([]string, 0, len(columnParts))
		for _, part := range columnParts {
			// Check if column has a width suffix (e.g., "status:120")
			if colonIdx := strings.LastIndex(part, ":"); colonIdx != -1 {
				colName := part[:colonIdx]
				widthStr := part[colonIdx+1:]
				if width, err := strconv.Atoi(widthStr); err == nil && width > 0 {
					state.Columns = append(state.Columns, colName)
					state.ColumnWidths[colName] = width
				} else {
					// Invalid width, treat the whole thing as column name
					state.Columns = append(state.Columns, part)
				}
			} else {
				state.Columns = append(state.Columns, part)
			}
		}
	} else {
		state.Columns = []string{}
	}

	// Extract expanded parameter
	expandedStr := q.Get("expanded")
	if expandedStr != "" {
		state.Expanded = strings.Split(expandedStr, ",")
	} else {
		state.Expanded = []string{}
	}

	// Extract grouped columns parameter
	groupedStr := q.Get("grouped")
	if groupedStr != "" {
		state.GroupedColumns = strings.Split(groupedStr, ",")
	} else {
		state.GroupedColumns = []string{}
	}

	// Extract limit parameter
	limitStr := q.Get("limit")
	if limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil && limit >= 0 {
			state.Limit = limit
		}
	}

	// Extract filter parameters (format: filter:columnName=value)
	for key, values := range q {
		if strings.HasPrefix(key, "filter:") && len(values) > 0 {
			columnName := strings.TrimPrefix(key, "filter:")
			state.Filters[columnName] = values[0]
		}
	}

	// Extract aggregate parameters (format: agg:columnName=sum,avg,min)
	for key, values := range q {
		if strings.HasPrefix(key, "agg:") && len(values) > 0 {
			columnName := strings.TrimPrefix(key, "agg:")
			state.AggregateSettings[columnName] = parseAggregates(values[0])
		}
	}

	// Extract computed columns parameter (format: name=operation(col1,col2);name2=operation2(col3,col4))
	computedStr := q.Get("computed")
	if computedStr != "" {
		state.ComputedColumns = parseComputedColumns(computedStr)
	}

	// Extract sort parameter (format: +col1,-col2,+col3)
	sortStr := q.Get("sort")
	if sortStr != "" {
		state.SortOrder = parseSortOrder(sortStr)
	}

	// Extract group aggregate sort parameters (format: groupsort:groupedCol=+leafCol:aggType or -leafCol:aggType)
	for key, values := range q {
		if strings.HasPrefix(key, "groupsort:") && len(values) > 0 {
			groupedCol := strings.TrimPrefix(key, "groupsort:")
			if aggSort := parseGroupAggSort(groupedCol, values[0]); aggSort != nil {
				state.GroupAggregateSorts[groupedCol] = aggSort
			}
		}
	}

	// Extract info pane state parameters
	infoParam := q.Get("info")
	if infoParam == "0" {
		state.ShowInfoPane = false
	}
	infotabParam := q.Get("infotab")
	if infotabParam != "" {
		state.InfoPaneTab = infotabParam
	}

	// Reorder columns: filtered columns first, then grouped columns, then others
	state.reorderColumns()

	return state
}

// parseComputedColumns parses the computed parameter string into ComputedColumnDef slice
// Format: name=expression (e.g., name=operation(col1,col2) or name=anything)
func parseComputedColumns(computedStr string) []ComputedColumnDef {
	var result []ComputedColumnDef
	definitions := strings.Split(computedStr, ";")
	for _, def := range definitions {
		if def == "" {
			continue
		}
		// Parse: name=expression
		eqIdx := strings.Index(def, "=")
		if eqIdx == -1 {
			continue
		}
		name := def[:eqIdx]
		expr := def[eqIdx+1:]

		result = append(result, ComputedColumnDef{
			Name:       name,
			Expression: expr,
		})
	}
	return result
}

// parseSortOrder parses the sort parameter string into SortColumn slice
// Format: +col1,-col2,+col3 (+ = ascending, - = descending)
func parseSortOrder(sortStr string) []SortColumn {
	var result []SortColumn
	parts := strings.Split(sortStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// First character must be + or -
		if len(part) < 2 {
			continue
		}
		switch part[0] {
		case '+':
			result = append(result, SortColumn{Name: part[1:], Descending: false})
		case '-':
			result = append(result, SortColumn{Name: part[1:], Descending: true})
		default:
			// Invalid format, skip
			continue
		}
	}
	return result
}

// parseAggregates parses the aggregate parameter string into AggregateType slice
// Format: sum,avg,min,max
func parseAggregates(aggStr string) []AggregateType {
	var result []AggregateType
	parts := strings.Split(aggStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Validate it's a known aggregate type
		aggType := AggregateType(part)
		switch aggType {
		case AggSum, AggAvg, AggStdDev, AggMin, AggMax, AggCount,
			AggUnique, AggTrue, AggFalse, AggRatio, AggSpan:
			result = append(result, aggType)
		}
	}
	return result
}

// parseGroupAggSort parses a group aggregate sort specification
// Format: +leafCol:aggType or -leafCol:aggType (e.g., +amount:sum or -amount:count)
func parseGroupAggSort(groupedCol, value string) *GroupAggSort {
	if len(value) < 3 {
		return nil
	}

	// First character must be + or -
	descending := false
	switch value[0] {
	case '+':
		descending = false
	case '-':
		descending = true
	default:
		return nil
	}

	// Rest is leafCol:aggType (leafCol can be empty for group-level sorts like row count)
	rest := value[1:]
	colonIdx := strings.LastIndex(rest, ":")
	if colonIdx == -1 || colonIdx == len(rest)-1 {
		return nil
	}

	leafCol := rest[:colonIdx]
	aggTypeStr := rest[colonIdx+1:]

	// Validate aggregate type
	aggType := AggregateType(aggTypeStr)
	switch aggType {
	case AggSum, AggAvg, AggStdDev, AggMin, AggMax, AggCount,
		AggUnique, AggTrue, AggFalse, AggRatio, AggSpan,
		AggRowCount, AggSubgroupCount:
		// Valid
	default:
		return nil
	}

	return &GroupAggSort{
		GroupedColumn: groupedCol,
		LeafColumn:    leafCol,
		AggType:       aggType,
		Descending:    descending,
	}
}

// Clone creates a deep copy of the Query
func (s *Query) Clone() *Query {
	clone := &Query{
		Path:                s.Path,
		Table:               s.Table,
		Columns:             make([]string, len(s.Columns)),
		ColumnWidths:        make(map[string]int),
		Expanded:            make([]string, len(s.Expanded)),
		GroupedColumns:      make([]string, len(s.GroupedColumns)),
		Filters:             make(map[string]string),
		Limit:               s.Limit,
		ComputedColumns:     make([]ComputedColumnDef, len(s.ComputedColumns)),
		SortOrder:           make([]SortColumn, len(s.SortOrder)),
		AggregateSettings:   make(map[string][]AggregateType),
		GroupAggregateSorts: make(map[string]*GroupAggSort),
		ShowInfoPane:        s.ShowInfoPane,
		InfoPaneTab:         s.InfoPaneTab,
	}

	// Deep copy columns
	copy(clone.Columns, s.Columns)

	// Deep copy column widths
	for colName, width := range s.ColumnWidths {
		clone.ColumnWidths[colName] = width
	}

	// Deep copy expanded
	copy(clone.Expanded, s.Expanded)

	// Deep copy grouped columns
	copy(clone.GroupedColumns, s.GroupedColumns)

	// Deep copy filters
	for colName, filterValue := range s.Filters {
		clone.Filters[colName] = filterValue
	}

	// Deep copy computed columns
	copy(clone.ComputedColumns, s.ComputedColumns)

	// Deep copy sort order
	copy(clone.SortOrder, s.SortOrder)

	// Deep copy aggregate settings
	for colName, aggs := range s.AggregateSettings {
		aggsCopy := make([]AggregateType, len(aggs))
		copy(aggsCopy, aggs)
		clone.AggregateSettings[colName] = aggsCopy
	}

	// Deep copy group aggregate sorts
	for groupedCol, aggSort := range s.GroupAggregateSorts {
		clone.GroupAggregateSorts[groupedCol] = &GroupAggSort{
			GroupedColumn: aggSort.GroupedColumn,
			LeafColumn:    aggSort.LeafColumn,
			AggType:       aggSort.AggType,
			Descending:    aggSort.Descending,
		}
	}

	return clone
}

// reorderColumns reorders the Columns slice to maintain:
// 1. Filtered columns (leftmost) - only columns that are filtered but NOT grouped
// 2. Grouped columns (middle) - in GroupedColumns order (the grouping hierarchy)
// 3. Other columns (rightmost)
// Note: A column can be both filtered and grouped simultaneously. In this case,
// it stays in the grouped section to preserve the grouping display position.
func (s *Query) reorderColumns() {
	if len(s.Columns) == 0 {
		return
	}

	// Create sets for quick lookup
	filteredCols := make(map[string]bool)
	for colName := range s.Filters {
		filteredCols[colName] = true
	}

	groupedCols := make(map[string]bool)
	for _, colName := range s.GroupedColumns {
		groupedCols[colName] = true
	}

	// Track which columns are visible
	visibleCols := make(map[string]bool)
	for _, colName := range s.Columns {
		visibleCols[colName] = true
	}

	// Collect filtered-only and other columns from s.Columns
	var filtered, others []string
	for _, colName := range s.Columns {
		if groupedCols[colName] {
			// Skip - will add from GroupedColumns in their order
			continue
		} else if filteredCols[colName] {
			filtered = append(filtered, colName)
		} else {
			others = append(others, colName)
		}
	}

	// Collect visible grouped columns in GroupedColumns order (grouping hierarchy)
	var grouped []string
	for _, colName := range s.GroupedColumns {
		if visibleCols[colName] {
			grouped = append(grouped, colName)
		}
	}

	// Rebuild Columns in the correct order
	s.Columns = make([]string, 0, len(filtered)+len(grouped)+len(others))
	s.Columns = append(s.Columns, filtered...)
	s.Columns = append(s.Columns, grouped...)
	s.Columns = append(s.Columns, others...)
}

// WithColumn returns a URL with the column added (if not already present)
func (s *Query) WithColumn(column string) safehtml.URL {
	// Check if already present
	for _, col := range s.Columns {
		if col == column {
			return s.ToSafeURL() // Already present, return current URL
		}
	}

	// Clone and add
	newState := s.Clone()
	newState.Columns = append(newState.Columns, column)
	return newState.ToSafeURL()
}

// WithoutColumn returns a URL with the column removed
func (s *Query) WithoutColumn(column string) safehtml.URL {
	// Clone and filter
	newState := s.Clone()
	newColumns := make([]string, 0, len(s.Columns))
	for _, col := range s.Columns {
		if col != column {
			newColumns = append(newColumns, col)
		}
	}
	newState.Columns = newColumns
	return newState.ToSafeURL()
}

// WithColumnToggled returns a URL with the column toggled (added if not present, removed if present)
func (s *Query) WithColumnToggled(column string) safehtml.URL {
	// Clone and toggle
	newState := s.Clone()
	found := false
	newColumns := make([]string, 0, len(s.Columns))
	for _, col := range s.Columns {
		if col == column {
			found = true
		} else {
			newColumns = append(newColumns, col)
		}
	}

	if found {
		newState.Columns = newColumns
	} else {
		newState.Columns = append(s.Columns, column)
	}

	return newState.ToSafeURL()
}

// WithExpanded returns a URL with the expanded path added (if not already present)
func (s *Query) WithExpanded(path string) safehtml.URL {
	// Check if already present
	for _, exp := range s.Expanded {
		if exp == path {
			return s.ToSafeURL() // Already present, return current URL
		}
	}

	// Clone and add
	newState := s.Clone()
	newState.Expanded = append(newState.Expanded, path)
	return newState.ToSafeURL()
}

// WithoutExpanded returns a URL with the expanded path removed
func (s *Query) WithoutExpanded(path string) safehtml.URL {
	// Clone and filter
	newState := s.Clone()
	newExpanded := make([]string, 0, len(s.Expanded))
	for _, exp := range s.Expanded {
		if exp != path {
			newExpanded = append(newExpanded, exp)
		}
	}
	newState.Expanded = newExpanded
	return newState.ToSafeURL()
}

// WithExpandedToggled returns a URL with the expanded path toggled
func (s *Query) WithExpandedToggled(path string) safehtml.URL {
	// Clone and toggle
	newState := s.Clone()
	found := false
	newExpanded := make([]string, 0, len(s.Expanded))
	for _, exp := range s.Expanded {
		if exp == path {
			found = true
		} else {
			newExpanded = append(newExpanded, exp)
		}
	}

	if found {
		newState.Expanded = newExpanded
	} else {
		newState.Expanded = append(s.Expanded, path)
	}

	return newState.ToSafeURL()
}

// ToURL converts the Query back to a URL string
func (s *Query) ToURL() string {
	u := &url.URL{
		Path: s.Path,
	}

	q := u.Query()

	// Add table parameter
	if s.Table != "" {
		q.Set("table", s.Table)
	}

	// Add columns parameter (with widths if present)
	if len(s.Columns) > 0 {
		columnStrs := make([]string, 0, len(s.Columns))
		for _, col := range s.Columns {
			if width, hasWidth := s.ColumnWidths[col]; hasWidth {
				columnStrs = append(columnStrs, col+":"+strconv.Itoa(width))
			} else {
				columnStrs = append(columnStrs, col)
			}
		}
		q.Set("columns", strings.Join(columnStrs, ","))
	}

	// Add expanded parameter
	if len(s.Expanded) > 0 {
		q.Set("expanded", strings.Join(s.Expanded, ","))
	}

	// Add grouped columns parameter
	if len(s.GroupedColumns) > 0 {
		q.Set("grouped", strings.Join(s.GroupedColumns, ","))
	}

	// Add filter parameters (format: filter:columnName=value)
	for colName, filterValue := range s.Filters {
		if filterValue != "" {
			q.Set("filter:"+colName, filterValue)
		}
	}

	// Add limit parameter (always included in URL)
	q.Set("limit", strconv.Itoa(s.Limit))

	// Add computed columns parameter
	if len(s.ComputedColumns) > 0 {
		var computedStrs []string
		for _, comp := range s.ComputedColumns {
			computedStrs = append(computedStrs, comp.Name+"="+comp.Expression)
		}
		q.Set("computed", strings.Join(computedStrs, ";"))
	}

	// Add sort parameter (format: +col1,-col2,+col3)
	if len(s.SortOrder) > 0 {
		var sortStrs []string
		for _, sc := range s.SortOrder {
			if sc.Descending {
				sortStrs = append(sortStrs, "-"+sc.Name)
			} else {
				sortStrs = append(sortStrs, "+"+sc.Name)
			}
		}
		q.Set("sort", strings.Join(sortStrs, ","))
	}

	// Add aggregate parameters (format: agg:columnName=sum,avg,min)
	for colName, aggs := range s.AggregateSettings {
		if len(aggs) > 0 {
			var aggStrs []string
			for _, agg := range aggs {
				aggStrs = append(aggStrs, string(agg))
			}
			q.Set("agg:"+colName, strings.Join(aggStrs, ","))
		}
	}

	// Add group aggregate sort parameters (format: groupsort:groupedCol=+leafCol:aggType)
	for groupedCol, aggSort := range s.GroupAggregateSorts {
		sign := "+"
		if aggSort.Descending {
			sign = "-"
		}
		q.Set("groupsort:"+groupedCol, sign+aggSort.LeafColumn+":"+string(aggSort.AggType))
	}

	// Add info pane state parameters
	if !s.ShowInfoPane {
		q.Set("info", "0")
	}
	if s.InfoPaneTab != "" && s.InfoPaneTab != "url" {
		q.Set("infotab", s.InfoPaneTab)
	}

	u.RawQuery = q.Encode()
	return u.String()
}

// ToSafeURL converts the Query to a safehtml.URL
func (s *Query) ToSafeURL() safehtml.URL {
	urlStr := s.ToURL()
	// URLSanitized sanitizes the input string and returns a URL
	return safehtml.URLSanitized(urlStr)
}

// IsColumnVisible checks if a column is in the visible columns list
func (s *Query) IsColumnVisible(column string) bool {
	for _, col := range s.Columns {
		if col == column {
			return true
		}
	}
	return false
}

// IsPathExpanded checks if a path is in the expanded list
func (s *Query) IsPathExpanded(path string) bool {
	for _, exp := range s.Expanded {
		if exp == path {
			return true
		}
	}
	return false
}

// WithLimit returns a URL with a different row limit
func (s *Query) WithLimit(limit int) safehtml.URL {
	newState := s.Clone()
	newState.Limit = limit
	return newState.ToSafeURL()
}

// WithGroupedColumnToggled returns a URL with the grouped column toggled
// If the column is already grouped, it's removed from grouping
// If the column is not grouped, it's added to the end of the grouping order
// This method also reorders the Columns list to ensure grouped columns appear first
func (s *Query) WithGroupedColumnToggled(column string) safehtml.URL {
	newState := s.Clone()
	found := false
	newGrouped := make([]string, 0, len(s.GroupedColumns))

	for _, col := range s.GroupedColumns {
		if col == column {
			found = true
		} else {
			newGrouped = append(newGrouped, col)
		}
	}

	if found {
		// Column was grouped, remove it
		newState.GroupedColumns = newGrouped
	} else {
		// Column was not grouped, add it to the end
		newState.GroupedColumns = append(s.GroupedColumns, column)
	}

	// Reorder columns: filtered first, then grouped, then others
	newState.reorderColumns()

	return newState.ToSafeURL()
}

// IsColumnGrouped checks if a column is in the grouped columns list
func (s *Query) IsColumnGrouped(column string) bool {
	for _, col := range s.GroupedColumns {
		if col == column {
			return true
		}
	}
	return false
}

// WithFilterAndUngrouped returns a URL that adds a filter for the column and removes it from grouping
func (s *Query) WithFilterAndUngrouped(column, value string) safehtml.URL {
	newState := s.Clone()

	// Add the filter
	newState.Filters[column] = value

	// Remove column from grouping
	newGrouped := make([]string, 0, len(s.GroupedColumns))
	for _, col := range s.GroupedColumns {
		if col != column {
			newGrouped = append(newGrouped, col)
		}
	}
	newState.GroupedColumns = newGrouped

	// Reorder columns: filtered first, then grouped, then others
	newState.reorderColumns()

	return newState.ToSafeURL()
}

// WithSortToggled returns a URL with the sort direction toggled for a column.
// Clicking cycles: ascending (move to front) -> descending -> ascending
// The column is moved to the front of the sort order (highest priority).
func (s *Query) WithSortToggled(column string) safehtml.URL {
	newState := s.Clone()

	// Find if column is already in sort order
	var existingIdx = -1
	var wasDescending bool
	for i, sc := range newState.SortOrder {
		if sc.Name == column {
			existingIdx = i
			wasDescending = sc.Descending
			break
		}
	}

	if existingIdx >= 0 {
		// Column exists - toggle direction and move to front
		newSortOrder := make([]SortColumn, 0, len(newState.SortOrder))
		newSortOrder = append(newSortOrder, SortColumn{Name: column, Descending: !wasDescending})
		for i, sc := range newState.SortOrder {
			if i != existingIdx {
				newSortOrder = append(newSortOrder, sc)
			}
		}
		newState.SortOrder = newSortOrder
	} else {
		// Column not in sort order - add to front as ascending
		newSortOrder := make([]SortColumn, 0, len(newState.SortOrder)+1)
		newSortOrder = append(newSortOrder, SortColumn{Name: column, Descending: false})
		newSortOrder = append(newSortOrder, newState.SortOrder...)
		newState.SortOrder = newSortOrder
	}

	return newState.ToSafeURL()
}

// GetSortIndex returns the 1-based sort priority index for a column, or 0 if not sorted.
func (s *Query) GetSortIndex(column string) int {
	for i, sc := range s.SortOrder {
		if sc.Name == column {
			return i + 1
		}
	}
	return 0
}

// IsSortedDescending returns true if the column is sorted in descending order.
func (s *Query) IsSortedDescending(column string) bool {
	for _, sc := range s.SortOrder {
		if sc.Name == column {
			return sc.Descending
		}
	}
	return false
}

// WithAggregateToggled returns a URL with the specified aggregate toggled for a column.
// If the aggregate is enabled, it's disabled; if disabled, it's enabled.
// Handles the case where count is enabled by default when no explicit settings exist.
func (s *Query) WithAggregateToggled(column string, aggType AggregateType) safehtml.URL {
	newState := s.Clone()

	// Check if aggregate is currently enabled (considering default count)
	isCurrentlyEnabled := s.IsAggregateEnabled(column, aggType)

	currentAggs, hasExplicitSettings := newState.AggregateSettings[column]

	if isCurrentlyEnabled {
		// Currently enabled, need to disable
		if hasExplicitSettings {
			// Remove from explicit list
			newAggs := make([]AggregateType, 0, len(currentAggs))
			for _, agg := range currentAggs {
				if agg != aggType {
					newAggs = append(newAggs, agg)
				}
			}
			newState.AggregateSettings[column] = newAggs
		} else {
			// Was enabled by default (count), explicitly set to empty to disable
			newState.AggregateSettings[column] = []AggregateType{}
		}

		// Remove any GroupAggSort that uses this aggregate on this column
		for groupedCol, aggSort := range newState.GroupAggregateSorts {
			if aggSort != nil && aggSort.LeafColumn == column && aggSort.AggType == aggType {
				delete(newState.GroupAggregateSorts, groupedCol)
			}
		}
	} else {
		// Currently disabled, need to enable
		newState.AggregateSettings[column] = append(currentAggs, aggType)
	}

	return newState.ToSafeURL()
}

// IsAggregateEnabled returns true if the specified aggregate is enabled for a column.
// Count is enabled by default when no aggregates are explicitly set.
// If explicitly set to empty, nothing is enabled (user disabled all aggregates).
func (s *Query) IsAggregateEnabled(column string, aggType AggregateType) bool {
	aggs, exists := s.AggregateSettings[column]
	if !exists {
		// No explicit settings: count is enabled by default
		return aggType == AggCount
	}
	// Explicit settings exist (even if empty slice means nothing is enabled)
	for _, agg := range aggs {
		if agg == aggType {
			return true
		}
	}
	return false
}

// GetEnabledAggregates returns the list of enabled aggregates for a column.
// The results are ordered according to GetAvailableAggregates to match the toggle order in the UI.
// If no aggregates are explicitly set (no entry in map), returns [AggCount] as the default.
// If explicitly set to empty, returns nil (user disabled all aggregates including default count).
func (s *Query) GetEnabledAggregates(column string, colType ColumnType) []AggregateType {
	enabled, exists := s.AggregateSettings[column]
	if !exists {
		// No explicit settings: default to count
		return []AggregateType{AggCount}
	}
	if len(enabled) == 0 {
		// Explicitly set to empty (user disabled all aggregates)
		return nil
	}

	// Build a set of enabled aggregates for quick lookup
	enabledSet := make(map[AggregateType]bool)
	for _, agg := range enabled {
		enabledSet[agg] = true
	}

	// Return aggregates in the canonical order defined by GetAvailableAggregates
	available := GetAvailableAggregates(colType)
	result := make([]AggregateType, 0, len(enabled))
	for _, agg := range available {
		if enabledSet[agg] {
			result = append(result, agg)
		}
	}
	return result
}

// WithNextGroupAggSort cycles through aggregate sort options for a grouped column.
// Options cycle through: no aggregate sort -> row count -> subgroup count -> each enabled aggregate of each leaf column -> back to no aggregate sort
// leafColumns is an ordered list of leaf column names; enabledAggs maps leaf column name to enabled aggregates.
// Note: Count aggregates are skipped since they are not displayed in the UI.
func (s *Query) WithNextGroupAggSort(groupedColumn string, leafColumns []string, enabledAggs map[string][]AggregateType) safehtml.URL {
	newState := s.Clone()

	// Build the list of all possible aggregate sort options
	// Each option is (leafColumn, aggType)
	// Start with group-level sorts (row count, subgroup count), then leaf column aggregates
	type sortOption struct {
		leafCol string
		aggType AggregateType
	}
	var options []sortOption

	// Add group-level sort options first (these use empty leafCol)
	options = append(options, sortOption{"", AggRowCount})
	options = append(options, sortOption{"", AggSubgroupCount})

	// Add leaf column aggregate options
	// Skip count aggregates since they are not displayed in the UI
	for _, leafCol := range leafColumns {
		for _, agg := range enabledAggs[leafCol] {
			if agg == AggCount {
				continue // Skip count - not displayed in UI
			}
			options = append(options, sortOption{leafCol, agg})
		}
	}

	// Find current position in cycle
	currentSort := s.GroupAggregateSorts[groupedColumn]
	currentIdx := -1 // -1 means "no aggregate sort" (sort by column value)
	if currentSort != nil {
		for i, opt := range options {
			if opt.leafCol == currentSort.LeafColumn && opt.aggType == currentSort.AggType {
				currentIdx = i
				break
			}
		}
	}

	// Move to next option
	nextIdx := currentIdx + 1
	if nextIdx >= len(options) {
		// Cycle back to no aggregate sort
		delete(newState.GroupAggregateSorts, groupedColumn)
	} else {
		// Set next aggregate sort (ascending by default)
		opt := options[nextIdx]
		newState.GroupAggregateSorts[groupedColumn] = &GroupAggSort{
			GroupedColumn: groupedColumn,
			LeafColumn:    opt.leafCol,
			AggType:       opt.aggType,
			Descending:    false,
		}
	}

	return newState.ToSafeURL()
}

// GetGroupAggSort returns the aggregate sort for a grouped column, or nil if none.
func (s *Query) GetGroupAggSort(groupedColumn string) *GroupAggSort {
	return s.GroupAggregateSorts[groupedColumn]
}

// WithGroupAggSortDirectionToggled toggles the direction (asc/desc) of an existing aggregate sort.
// If no aggregate sort exists for the column, this has no effect.
func (s *Query) WithGroupAggSortDirectionToggled(groupedColumn string) safehtml.URL {
	newState := s.Clone()

	if aggSort, exists := newState.GroupAggregateSorts[groupedColumn]; exists {
		aggSort.Descending = !aggSort.Descending
	}

	return newState.ToSafeURL()
}
