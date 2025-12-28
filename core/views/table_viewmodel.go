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

package views

import (
	"fmt"
	"sort"
	"strings"
	"net/url"
	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/tables"
)

// TableViewModel contains the data from the table formatted for template consumption
type TableViewModel struct {
	Title         string
	Headers       []string              // Column display names
	Columns       []string              // Column names (for data access)
	Rows          []map[string]string   // Each row is a map of column name to value
	AllColumns    []ColumnInfo          // All available columns with metadata
	CurrentQuery  string                // Current query string
	CurrentURL    string                // Current URL for building toggle links
}

// ColumnInfo contains information about a column for UI display
type ColumnInfo struct {
	Name         string // Column internal name
	DisplayName  string // Column display name
	IsVisible    bool   // Whether column is currently visible
	HasEntityType bool   // Whether column defines an entity type
	IsKey        bool   // Whether column has all unique values
	JoinTargets  []JoinTarget // Tables/columns this column can join to
	IsExpanded   bool   // Whether this column's join list is expanded
	Path         string // Path for URL encoding (e.g., "column1")
	ToggleURL    string // URL to toggle expansion
	ToggleColumnURL string // URL to toggle column visibility (preserves all query params)
}

// JoinTarget represents a column that can be joined to
type JoinTarget struct {
	TableName        string
	ColumnName       string
	DisplayName      string // "TableName.ColumnName" for display
	AvailableColumns []ColumnSummary // Columns available in the joined table
	IsBlocked        bool   // True if this target is blocked due to cycle prevention
	IsExpanded       bool   // Whether this join target is expanded
	Path             string // Path for URL encoding (e.g., "column1/table2.column2")
	ToggleURL        string // URL to toggle expansion
}

// ColumnSummary represents a column in a joined table
type ColumnSummary struct {
	Name          string
	DisplayName   string
	HasEntityType bool
	IsKey         bool
	TableName     string       // The table this column belongs to
	Path          string       // Path for URL encoding
	JoinTargets   []JoinTarget // Tables/columns this column can join to
	IsExpanded    bool         // Whether this column's join list is expanded
	ToggleURL     string       // URL to toggle expansion
	AddColumnURL  string       // URL to add this column and its join to the view
	IsSelected    bool         // Whether this column is already in the current view
}

// detectCycle checks if a table appears in the path to prevent infinite loops
func detectCycle(path string, targetTable string) bool {
	// Split the path and check if the target table already appears
	parts := strings.Split(path, "/")
	for _, part := range parts {
		// Each part after the first is in format "table.column"
		if strings.Contains(part, ".") {
			tableName := strings.Split(part, ".")[0]
			if tableName == targetTable {
				return true
			}
		}
	}
	return false
}

// buildJoinTargetsForColumn builds join targets for a column (one level only)
func buildJoinTargetsForColumn(dataModel *models.DataModel, tableName, columnName string, basePath string, expandedPaths map[string]bool, currentURL string) []JoinTarget {

	var joinTargets []JoinTarget
	columnJoins := dataModel.GetJoinsForColumn(tableName, columnName)

	for _, join := range columnJoins {
		var target JoinTarget
		var targetTableName, targetColumnName string

		if join.FromTable == tableName && join.FromColumn == columnName {
			// This is an outgoing join
			targetTableName = join.ToTable
			targetColumnName = join.ToColumn
		} else if join.ToTable == tableName && join.ToColumn == columnName {
			// This is an incoming join (reverse)
			targetTableName = join.FromTable
			targetColumnName = join.FromColumn
		} else {
			continue
		}

		// Build the path for this target
		targetPath := fmt.Sprintf("%s/%s.%s", basePath, targetTableName, targetColumnName)

		// Check for cycles
		isBlocked := detectCycle(basePath, targetTableName)

		target = JoinTarget{
			TableName:   targetTableName,
			ColumnName:  targetColumnName,
			DisplayName: fmt.Sprintf("%s.%s", targetTableName, targetColumnName),
			IsBlocked:   isBlocked,
			IsExpanded:  expandedPaths[targetPath] && !isBlocked,
			Path:        targetPath,
			ToggleURL:   BuildToggleExpansionURL(currentURL, targetPath),
		}

		// Always check if the target table has columns (to show expansion toggle)
		targetTable := dataModel.GetTable(targetTableName)
		if targetTable != nil && !isBlocked {
			// Get available columns from the target table only if expanded
			if target.IsExpanded {
				var availableColumns []ColumnSummary
				targetColumnNames := targetTable.GetColumnNames()

			for _, targetColName := range targetColumnNames {
				targetCol := targetTable.GetColumn(targetColName)
				if targetCol != nil {
					entityType := targetCol.ColumnDef().EntityType()
					colPath := fmt.Sprintf("%s/%s", targetPath, targetColName)
					isExpanded := expandedPaths[colPath]

					// Build AddColumnURL for all columns (this adds both the join and the column)
					joinPath := fmt.Sprintf("%s.%s,%s.%s", tableName, columnName, targetTableName, targetColumnName)
					columnFullName := fmt.Sprintf("%s.%s", targetTableName, targetColName)
					addColumnURL := BuildAddColumnAndJoinURL(currentURL, joinPath, columnFullName)

					// Check if this column is already selected
					isSelected := false
					u, err := url.Parse(currentURL)
					if err == nil {
						q := u.Query()
						columnsStr := q.Get("columns")
						if columnsStr != "" {
							for _, col := range strings.Split(columnsStr, ",") {
								if col == columnFullName {
									isSelected = true
									break
								}
							}
						}
					}

					colSummary := ColumnSummary{
						Name:          targetColName,
						DisplayName:   targetCol.ColumnDef().DisplayName(),
						HasEntityType: entityType != "",
						IsKey:         targetCol.IsKey() && entityType != "",
						TableName:     targetTableName,
						Path:          colPath,
						IsExpanded:    isExpanded,
						ToggleURL:     BuildToggleExpansionURL(currentURL, colPath),
						AddColumnURL:  addColumnURL,
						IsSelected:    isSelected,
					}

					// Check if this column can join to other tables
					if entityType != "" {
						// Check for joins but avoid building them if they would create cycles
						columnJoins := dataModel.GetJoinsForColumn(targetTableName, targetColName)
						hasValidJoins := false

						// Check if any joins would be valid (not creating cycles)
						for _, join := range columnJoins {
							var checkTable string
							if join.FromTable == targetTableName && join.FromColumn == targetColName {
								checkTable = join.ToTable
							} else if join.ToTable == targetTableName && join.ToColumn == targetColName {
								checkTable = join.FromTable
							}

							if checkTable != "" && !detectCycle(colPath, checkTable) {
								hasValidJoins = true
								break
							}
						}

						if hasValidJoins {
							if isExpanded {
								// Recursively build join targets for this column
								colSummary.JoinTargets = buildJoinTargetsForColumn(dataModel, targetTableName, targetColName, colPath, expandedPaths, currentURL)
							} else {
								// Just mark that joins are available without building them
								colSummary.JoinTargets = []JoinTarget{} // Empty slice indicates joins exist but not expanded
							}
						}
					}

					availableColumns = append(availableColumns, colSummary)
				}
			}

				// Sort available columns alphabetically
				sort.Slice(availableColumns, func(i, j int) bool {
					return availableColumns[i].DisplayName < availableColumns[j].DisplayName
				})
				target.AvailableColumns = availableColumns
			} else {
				// Not expanded, but mark that columns are available
				target.AvailableColumns = []ColumnSummary{} // Empty slice indicates columns exist but not expanded
			}
		}

		joinTargets = append(joinTargets, target)
	}

	return joinTargets
}

// BuildViewModel creates a ViewModel from a Table using the specified View
func BuildViewModel(dataModel *models.DataModel, tableName string, table *tables.DataTable, view TableView, title string, currentURL string) TableViewModel {
	vm := TableViewModel{
		Title:      title,
		Headers:    []string{},
		Columns:    []string{},
		Rows:       []map[string]string{},
		AllColumns: []ColumnInfo{},
		CurrentURL: currentURL,
	}

	// Create a map of visible columns for quick lookup
	visibleCols := make(map[string]bool)
	for _, colName := range view.Columns {
		visibleCols[colName] = true
	}

	// Build all columns info
	allColumnNames := table.GetColumnNames()
	for _, colName := range allColumnNames {
		col := table.GetColumn(colName)
		if col != nil {
			// Check if column has an entity type
			hasEntityType := col.ColumnDef().EntityType() != ""

			// Use the column's IsKey property
			isKey := col.IsKey()

			// Find join targets for this column
			var joinTargets []JoinTarget
			if hasEntityType {
				// Build the base path for this column
				basePath := colName
				joinTargets = buildJoinTargetsForColumn(dataModel, tableName, colName, basePath, view.Expanded, currentURL)
			}

			// Check if this column is expanded
			isExpanded := false
			if view.Expanded != nil {
				isExpanded = view.Expanded[colName]
			}

			vm.AllColumns = append(vm.AllColumns, ColumnInfo{
				Name:         colName,
				DisplayName:  col.ColumnDef().DisplayName(),
				IsVisible:    visibleCols[colName],
				HasEntityType: hasEntityType,
				IsKey:        isKey && hasEntityType, // Only mark as key if it's also an entity type
				JoinTargets:  joinTargets,
				IsExpanded:   isExpanded,
				Path:         colName,
				ToggleURL:    BuildToggleExpansionURL(currentURL, colName),
				ToggleColumnURL: BuildToggleColumnURL(currentURL, view.Columns, colName),
			})
		}
	}

	// Sort all columns alphabetically by DisplayName
	sort.Slice(vm.AllColumns, func(i, j int) bool {
		return vm.AllColumns[i].DisplayName < vm.AllColumns[j].DisplayName
	})

	// Build headers and columns from view
	for _, colName := range view.Columns {
		col := table.GetColumn(colName)
		if col != nil {
			vm.Headers = append(vm.Headers, col.ColumnDef().DisplayName())
			vm.Columns = append(vm.Columns, colName)
		}
	}

	// Get the number of rows (assumes all columns have same length)
	numRows := 0
	if len(view.Columns) > 0 {
		firstCol := table.GetColumn(view.Columns[0])
		if firstCol != nil {
			numRows = firstCol.Length()
		}
	}

	// Build rows
	for i := 0; i < numRows; i++ {
		row := make(map[string]string)
		for _, colName := range view.Columns {
			col := table.GetColumn(colName)
			if col != nil {
				value, _ := col.GetString(i)
				row[colName] = value
			}
		}
		vm.Rows = append(vm.Rows, row)
	}

	return vm
}

// BuildToggleExpansionURL creates a URL that toggles the expansion state of a path
func BuildToggleExpansionURL(currentURL string, togglePath string) string {
	// Parse the current URL
	u, err := url.Parse(currentURL)
	if err != nil {
		return currentURL
	}

	// Get current expanded paths
	q := u.Query()
	expandedStr := q.Get("expanded")
	expandedPaths := make(map[string]bool)

	if expandedStr != "" {
		for _, path := range strings.Split(expandedStr, ",") {
			if path != "" {
				expandedPaths[path] = true
			}
		}
	}

	// Toggle the path
	if expandedPaths[togglePath] {
		delete(expandedPaths, togglePath)
	} else {
		expandedPaths[togglePath] = true
	}

	// Build new expanded parameter
	var paths []string
	for path := range expandedPaths {
		paths = append(paths, path)
	}
	sort.Strings(paths) // Consistent ordering

	if len(paths) > 0 {
		q.Set("expanded", strings.Join(paths, ","))
	} else {
		q.Del("expanded")
	}

	u.RawQuery = q.Encode()
	return u.String()
}

// ParseExpandedPaths extracts the expanded paths from URL parameters
func ParseExpandedPaths(expandedParam string) map[string]bool {
	expandedPaths := make(map[string]bool)
	if expandedParam != "" {
		for _, path := range strings.Split(expandedParam, ",") {
			if path != "" {
				expandedPaths[path] = true
			}
		}
	}
	return expandedPaths
}

// ParseJoinedPaths extracts the joined paths from URL parameters
func ParseJoinedPaths(joinedParam string) []string {
	var joinedPaths []string
	if joinedParam != "" {
		for _, path := range strings.Split(joinedParam, ",") {
			if path != "" {
				joinedPaths = append(joinedPaths, path)
			}
		}
	}
	return joinedPaths
}

// BuildAddColumnAndJoinURL creates a URL that toggles a column and manages its join
func BuildAddColumnAndJoinURL(currentURL string, joinPath string, columnName string) string {
	// Parse the current URL
	u, err := url.Parse(currentURL)
	if err != nil {
		return currentURL
	}

	q := u.Query()

	// Get current columns
	columnsStr := q.Get("columns")
	columns := make(map[string]bool)
	var columnOrder []string

	if columnsStr != "" {
		for _, col := range strings.Split(columnsStr, ",") {
			if col != "" {
				columns[col] = true
				columnOrder = append(columnOrder, col)
			}
		}
	}

	// Toggle the column
	isRemoving := false
	if columns[columnName] {
		// Remove the column
		delete(columns, columnName)
		isRemoving = true
		// Remove from order
		var newOrder []string
		for _, col := range columnOrder {
			if col != columnName {
				newOrder = append(newOrder, col)
			}
		}
		columnOrder = newOrder
	} else {
		// Add the column
		columns[columnName] = true
		columnOrder = append(columnOrder, columnName)
	}

	// Update columns parameter
	if len(columnOrder) > 0 {
		q.Set("columns", strings.Join(columnOrder, ","))
	} else {
		q.Del("columns")
	}

	// Handle joins
	joinsStr := q.Get("joins")
	joinedPaths := make(map[string]bool)

	if joinsStr != "" {
		for _, path := range strings.Split(joinsStr, ",") {
			if path != "" {
				joinedPaths[path] = true
			}
		}
	}

	if isRemoving {
		// Check if we should remove the join
		// Extract the table name from the join path
		joinParts := strings.Split(joinPath, ",")
		if len(joinParts) == 2 {
			toTableColumn := strings.Split(joinParts[1], ".")
			if len(toTableColumn) == 2 {
				joinedTableName := toTableColumn[0]

				// Check if any other columns from this table remain
				hasOtherColumns := false
				for col := range columns {
					if strings.HasPrefix(col, joinedTableName + ".") {
						hasOtherColumns = true
						break
					}
				}

				// If no other columns from this table, remove the join
				if !hasOtherColumns {
					delete(joinedPaths, joinPath)
				}
			}
		}
	} else {
		// Add the join path
		joinedPaths[joinPath] = true
	}

	// Build new joins parameter
	var joinPaths []string
	for path := range joinedPaths {
		joinPaths = append(joinPaths, path)
	}
	sort.Strings(joinPaths)

	if len(joinPaths) > 0 {
		q.Set("joins", strings.Join(joinPaths, ","))
	} else {
		q.Del("joins")
	}

	// IMPORTANT: Preserve the expanded parameter to maintain UI state
	// The expanded parameter controls which join targets are expanded in the sidebar
	// and should not be affected by adding/removing columns

	u.RawQuery = q.Encode()
	return u.String()
}

// BuildToggleJoinedURL creates a URL that toggles a join path
func BuildToggleJoinedURL(currentURL string, joinPath string) string {
	// Parse the current URL
	u, err := url.Parse(currentURL)
	if err != nil {
		return currentURL
	}

	// Get current joined paths
	q := u.Query()
	joinsStr := q.Get("joins")
	joinedPaths := make(map[string]bool)

	if joinsStr != "" {
		for _, path := range strings.Split(joinsStr, ",") {
			if path != "" {
				joinedPaths[path] = true
			}
		}
	}

	// Toggle the path
	if joinedPaths[joinPath] {
		delete(joinedPaths, joinPath)
	} else {
		joinedPaths[joinPath] = true
	}

	// Build new joined parameter
	var paths []string
	for path := range joinedPaths {
		paths = append(paths, path)
	}
	sort.Strings(paths) // Consistent ordering

	if len(paths) > 0 {
		q.Set("joins", strings.Join(paths, ","))
	} else {
		q.Del("joins")
	}

	u.RawQuery = q.Encode()
	return u.String()
}

// BuildToggleColumnURL creates a URL that toggles the visibility of a column while preserving all other query parameters
func BuildToggleColumnURL(currentURL string, currentColumns []string, toggleColumn string) string {
	// Parse the current URL
	u, err := url.Parse(currentURL)
	if err != nil {
		// If parsing fails, fall back to simple behavior
		return "?columns=" + toggleColumn
	}

	// Get all query parameters
	q := u.Query()

	// Create a new column list with the toggled column
	newCols := []string{}
	found := false

	// Check if column exists in current list
	for _, col := range currentColumns {
		if col == toggleColumn {
			found = true
		} else {
			newCols = append(newCols, col)
		}
	}

	// If not found, add it
	if !found {
		newCols = append(newCols, toggleColumn)
	}

	// Update the columns parameter
	if len(newCols) > 0 {
		q.Set("columns", strings.Join(newCols, ","))
	} else {
		q.Del("columns")
	}

	// IMPORTANT: All other parameters (expanded, joins, etc.) are preserved
	// because we're using q := u.Query() which gets all existing parameters

	u.RawQuery = q.Encode()
	return u.String()
}