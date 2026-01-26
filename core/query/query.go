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

// Query represents the parsed state of a table view URL
type Query struct {
	// Base path (e.g., "/table")
	Path string

	// Core parameters
	Table           string              // The table being viewed
	Columns         []string            // Ordered list of visible columns (reordered: filtered, grouped, then others)
	ColumnWidths    map[string]int      // Column widths in pixels (columnName -> width)
	Expanded        []string            // List of expanded paths in the sidebar
	GroupedColumns  []string            // Ordered list of columns to group by
	Filters         map[string]string   // Column filters (columnName -> filterValue)
	Limit           int                 // Number of rows to display (0 = show all)
	ComputedColumns []ComputedColumnDef // Computed column definitions
}

// NewQuery creates a Query from a URL
func NewQuery(u *url.URL) *Query {
	// The URL is already parsed and safe to use since it comes from http.Request
	// No additional sanitization needed here as we're just extracting query parameters

	state := &Query{
		Path:         u.Path,
		Filters:      make(map[string]string),
		ColumnWidths: make(map[string]int),
		Limit:        25, // Default limit
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

	// Extract computed columns parameter (format: name=operation(col1,col2);name2=operation2(col3,col4))
	computedStr := q.Get("computed")
	if computedStr != "" {
		state.ComputedColumns = parseComputedColumns(computedStr)
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

// Clone creates a deep copy of the Query
func (s *Query) Clone() *Query {
	clone := &Query{
		Path:            s.Path,
		Table:           s.Table,
		Columns:         make([]string, len(s.Columns)),
		ColumnWidths:    make(map[string]int),
		Expanded:        make([]string, len(s.Expanded)),
		GroupedColumns:  make([]string, len(s.GroupedColumns)),
		Filters:         make(map[string]string),
		Limit:           s.Limit,
		ComputedColumns: make([]ComputedColumnDef, len(s.ComputedColumns)),
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
