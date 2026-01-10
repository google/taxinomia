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

// Query represents the parsed state of a table view URL
type Query struct {
	// Base path (e.g., "/table")
	Path string

	// Core parameters
	Table          string   // The table being viewed
	Columns        []string // Ordered list of visible columns
	Expanded       []string // List of expanded paths in the sidebar
	GroupedColumns []string // Ordered list of columns to group by
	Limit          int      // Number of rows to display (0 = show all)

	// Preserve any other parameters we don't know about
	OtherParams map[string][]string
}

// NewQuery creates a Query from a URL
func NewQuery(u *url.URL) *Query {
	// The URL is already parsed and safe to use since it comes from http.Request
	// No additional sanitization needed here as we're just extracting query parameters

	state := &Query{
		Path:        u.Path,
		OtherParams: make(map[string][]string),
		Limit:       25, // Default limit
	}

	// Parse query parameters
	q := u.Query()

	// Extract table parameter
	state.Table = q.Get("table")

	// Extract columns parameter
	columnsStr := q.Get("columns")
	if columnsStr != "" {
		state.Columns = strings.Split(columnsStr, ",")
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

	// Store all other parameters
	for key, values := range q {
		if key != "table" && key != "columns" && key != "expanded" && key != "grouped" && key != "limit" {
			state.OtherParams[key] = values
		}
	}

	return state
}

// Clone creates a deep copy of the Query
func (s *Query) Clone() *Query {
	clone := &Query{
		Path:           s.Path,
		Table:          s.Table,
		Columns:        make([]string, len(s.Columns)),
		Expanded:       make([]string, len(s.Expanded)),
		GroupedColumns: make([]string, len(s.GroupedColumns)),
		Limit:          s.Limit,
		OtherParams:    make(map[string][]string),
	}

	// Deep copy columns
	copy(clone.Columns, s.Columns)

	// Deep copy expanded
	copy(clone.Expanded, s.Expanded)

	// Deep copy grouped columns
	copy(clone.GroupedColumns, s.GroupedColumns)

	// Deep copy other params
	for key, values := range s.OtherParams {
		clone.OtherParams[key] = make([]string, len(values))
		copy(clone.OtherParams[key], values)
	}

	return clone
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

	// Add columns parameter
	if len(s.Columns) > 0 {
		q.Set("columns", strings.Join(s.Columns, ","))
	}

	// Add expanded parameter
	if len(s.Expanded) > 0 {
		q.Set("expanded", strings.Join(s.Expanded, ","))
	}

	// Add grouped columns parameter
	if len(s.GroupedColumns) > 0 {
		q.Set("grouped", strings.Join(s.GroupedColumns, ","))
	}

	// Add limit parameter (always included in URL)
	q.Set("limit", strconv.Itoa(s.Limit))

	// Add all other parameters
	for key, values := range s.OtherParams {
		for _, value := range values {
			q.Add(key, value)
		}
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

	// Reorder columns: grouped columns first, then ungrouped columns
	newState.reorderColumnsForGrouping()

	return newState.ToSafeURL()
}

// reorderColumnsForGrouping reorders the Columns slice so that grouped columns
// appear first in their grouping order, followed by ungrouped columns in their
// original relative order
func (s *Query) reorderColumnsForGrouping() {
	// Create a map for fast lookup of grouped columns and their order
	groupedOrder := make(map[string]int)
	for i, col := range s.GroupedColumns {
		groupedOrder[col] = i
	}

	// Split columns into grouped and ungrouped, preserving order
	var grouped []string
	var ungrouped []string

	for _, col := range s.Columns {
		if _, isGrouped := groupedOrder[col]; isGrouped {
			grouped = append(grouped, col)
		} else {
			ungrouped = append(ungrouped, col)
		}
	}

	// Sort the grouped columns according to their grouping order
	for i := 0; i < len(grouped)-1; i++ {
		for j := i + 1; j < len(grouped); j++ {
			if groupedOrder[grouped[i]] > groupedOrder[grouped[j]] {
				grouped[i], grouped[j] = grouped[j], grouped[i]
			}
		}
	}

	// Reconstruct the Columns slice: grouped first, then ungrouped
	s.Columns = make([]string, 0, len(grouped)+len(ungrouped))
	s.Columns = append(s.Columns, grouped...)
	s.Columns = append(s.Columns, ungrouped...)
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
