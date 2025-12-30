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
	"net/url"
	"sort"
	"strings"

	"github.com/google/safehtml"
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/query"
	"github.com/google/taxinomia/core/tables"
)

// TableViewModel contains the data from the table formatted for template consumption
type TableViewModel struct {
	Title        string
	Headers      []string            // Column display names
	Columns      []string            // Column names (for data access)
	Rows         []map[string]string // Each row is a map of column name to value
	AllColumns   []ColumnInfo        // All available columns with metadata
	CurrentQuery string              // Current query string
	CurrentURL   safehtml.URL        // Current URL for building toggle links

	// Pagination info
	TotalRows    int          // Total number of rows in the table
	DisplayedRows int         // Number of rows actually displayed
	HasMoreRows  bool         // True if there are more rows than displayed
	CurrentLimit int          // Current row limit
}

// ColumnInfo contains information about a column for UI display
type ColumnInfo struct {
	Name            string       // Column internal name
	DisplayName     string       // Column display name
	IsVisible       bool         // Whether column is currently visible
	HasEntityType   bool         // Whether column defines an entity type
	IsKey           bool         // Whether column has all unique values
	JoinTargets     []JoinTarget // Tables/columns this column can join to
	IsExpanded      bool         // Whether this column's join list is expanded
	Path            string       // Path for URL encoding (e.g., "column1")
	ToggleURL       safehtml.URL // URL to toggle expansion
	ToggleColumnURL safehtml.URL // URL to toggle column visibility (preserves all query params)
}

// JoinTarget represents a column that can be joined to
type JoinTarget struct {
	TableName        string
	ColumnName       string
	DisplayName      string          // "TableName.ColumnName" for display
	AvailableColumns []ColumnSummary // Columns available in the joined table
	IsBlocked        bool            // True if this target is blocked due to cycle prevention
	IsExpanded       bool            // Whether this join target is expanded
	Path             string          // Path for URL encoding (e.g., "column1/table2.column2")
	ToggleURL        safehtml.URL    // URL to toggle expansion
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
	ToggleURL     safehtml.URL // URL to toggle expansion
	AddColumnURL  safehtml.URL // URL to add this column and its join to the view
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
func buildJoinTargetsForColumn(dataModel *models.DataModel, tableName, columnName string, basePath string, expandedPaths map[string]bool, q *query.Query) []JoinTarget {

	var joinTargets []JoinTarget
	allJoins := dataModel.GetJoins()

	for _, join := range allJoins {
		var target JoinTarget
		var targetTableName, targetColumnName string
		var fromColumnName string

		// Parse join key to get table names (format: "fromTable.fromColumn->toTable.toColumn")
		keyParts := strings.Split(join.Key, "->")
		if len(keyParts) != 2 {
			continue
		}
		fromParts := strings.Split(keyParts[0], ".")
		toParts := strings.Split(keyParts[1], ".")
		if len(fromParts) != 2 || len(toParts) != 2 {
			continue
		}

		fromTableKey := fromParts[0]
		fromColumnKey := fromParts[1]
		toTableKey := toParts[0]
		toColumnKey := toParts[1]

		// Check if this join involves our column
		if fromTableKey == tableName && fromColumnKey == columnName {
			// This is an outgoing join
			targetTableName = toTableKey
			targetColumnName = toColumnKey
			fromColumnName = columnName
		} else if toTableKey == tableName && toColumnKey == columnName {
			// This is an incoming join (reverse)
			targetTableName = fromTableKey
			targetColumnName = fromColumnKey
			fromColumnName = columnName
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
			ToggleURL:   BuildToggleExpansionURL(q, targetPath),
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

						// Build column name using new format: fromColumn.toTable.toColumn.selectedColumn
						columnFullName := fmt.Sprintf("%s.%s.%s.%s", fromColumnName, targetTableName, targetColumnName, targetColName)
						addColumnURL := BuildAddColumnURL(q, columnFullName)

						// Check if this column is already selected
						isSelected := false
						for _, col := range q.Columns {
							if col == columnFullName {
								isSelected = true
								break
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
							ToggleURL:     BuildToggleExpansionURL(q, colPath),
							AddColumnURL:  addColumnURL,
							IsSelected:    isSelected,
						}

						// Check if this column can join to other tables
						if entityType != "" {
							// Check for joins but avoid building them if they would create cycles
							hasValidJoins := false

							// Check if any joins would be valid (not creating cycles)
							for _, checkJoin := range allJoins {
								// Parse join key to check tables
								checkKeyParts := strings.Split(checkJoin.Key, "->")
								if len(checkKeyParts) != 2 {
									continue
								}
								checkFromParts := strings.Split(checkKeyParts[0], ".")
								checkToParts := strings.Split(checkKeyParts[1], ".")
								if len(checkFromParts) != 2 || len(checkToParts) != 2 {
									continue
								}

								var checkTable string
								if checkFromParts[0] == targetTableName && checkFromParts[1] == targetColName {
									checkTable = checkToParts[0]
								} else if checkToParts[0] == targetTableName && checkToParts[1] == targetColName {
									checkTable = checkFromParts[0]
								}

								if checkTable != "" && !detectCycle(colPath, checkTable) {
									hasValidJoins = true
									break
								}
							}

							if hasValidJoins {
								if isExpanded {
									// Recursively build join targets for this column
									colSummary.JoinTargets = buildJoinTargetsForColumn(dataModel, targetTableName, targetColName, colPath, expandedPaths, q)
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
func BuildViewModel(dataModel *models.DataModel, tableName string, table *tables.DataTable, view TableView, title string, q *query.Query) TableViewModel {
	// Generate currentURL from Query
	currentURL := q.ToSafeURL()

	vm := TableViewModel{
		Title:      title,
		Headers:    []string{},
		Columns:    []string{},
		Rows:       []map[string]string{},
		AllColumns: []ColumnInfo{},
		CurrentURL: currentURL,
	}

	// Debug: Print view model building info
	fmt.Printf("\n=== BuildViewModel Debug Info ===\n")
	fmt.Printf("Table: %s\n", tableName)
	fmt.Printf("View Columns to display: %v\n", view.Columns)

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
				joinTargets = buildJoinTargetsForColumn(dataModel, tableName, colName, basePath, view.Expanded, q)
			}

			// Check if this column is expanded
			isExpanded := false
			if view.Expanded != nil {
				isExpanded = view.Expanded[colName]
			}

			vm.AllColumns = append(vm.AllColumns, ColumnInfo{
				Name:            colName,
				DisplayName:     col.ColumnDef().DisplayName(),
				IsVisible:       visibleCols[colName],
				HasEntityType:   hasEntityType,
				IsKey:           isKey && hasEntityType, // Only mark as key if it's also an entity type
				JoinTargets:     joinTargets,
				IsExpanded:      isExpanded,
				Path:            colName,
				ToggleURL:       BuildToggleExpansionURL(q, colName),
				ToggleColumnURL: BuildToggleColumnURL(q, colName),
			})
		}
	}

	// Add joined columns to AllColumns if they are in the view
	for _, colName := range view.Columns {
		if strings.Contains(colName, ".") {
			// This is a joined column - format: fromColumn.toTable.toColumn.selectedColumn
			parts := strings.Split(colName, ".")
			if len(parts) == 4 {
				// Check if this column is already in AllColumns (it shouldn't be)
				found := false
				for _, col := range vm.AllColumns {
					if col.Name == colName {
						found = true
						break
					}
				}

				if !found {
					// Build a better display name
					displayName := fmt.Sprintf("%s → %s", parts[1], parts[3])

					// Add the joined column info
					vm.AllColumns = append(vm.AllColumns, ColumnInfo{
						Name:            colName,
						DisplayName:     displayName,
						IsVisible:       visibleCols[colName],
						HasEntityType:   false, // Joined columns don't have entity types in this context
						IsKey:           false,
						JoinTargets:     nil,
						IsExpanded:      false,
						Path:            colName,
						ToggleURL:       BuildToggleExpansionURL(q, colName),
						ToggleColumnURL: BuildToggleColumnURL(q, colName),
					})
				}
			}
		}
	}

	// Sort all columns alphabetically by DisplayName
	sort.Slice(vm.AllColumns, func(i, j int) bool {
		return vm.AllColumns[i].DisplayName < vm.AllColumns[j].DisplayName
	})

	// Build headers and columns from view
	for _, colName := range view.Columns {
		// Check if this is a joined column (format: fromColumn.toTable.toColumn.selectedColumn)
		if strings.Contains(colName, ".") {
			// This is a joined column
			parts := strings.Split(colName, ".")
			if len(parts) == 4 {
				// Build a display name: "TableName → ColumnName"
				displayName := fmt.Sprintf("%s → %s", parts[1], parts[3])
				vm.Headers = append(vm.Headers, displayName)
				vm.Columns = append(vm.Columns, colName)
			}
		} else {
			// Regular column from the main table
			col := table.GetColumn(colName)
			if col != nil {
				vm.Headers = append(vm.Headers, col.ColumnDef().DisplayName())
				vm.Columns = append(vm.Columns, colName)
			}
		}
	}

	// Get the number of rows (assumes all columns have same length)
	totalRows := 0
	if len(view.Columns) > 0 {
		// Find the first non-joined column to get row count
		for _, colName := range view.Columns {
			firstCol := table.GetColumn(colName)
			if firstCol != nil {
				totalRows = firstCol.Length()
				break
			}
		}
	}

	// Store total rows
	vm.TotalRows = totalRows

	// Apply limit
	rowsToDisplay := totalRows
	if q.Limit > 0 && q.Limit < totalRows {
		rowsToDisplay = q.Limit
		vm.HasMoreRows = true
	}
	vm.DisplayedRows = rowsToDisplay
	vm.CurrentLimit = q.Limit

	// Build rows (only up to the limit)
	for i := 0; i < rowsToDisplay; i++ {
		row := make(map[string]string)
		for _, colName := range view.Columns {
			col := table.GetColumn(colName)
			if col != nil {
				value := col.GetString(uint32(i))
				row[colName] = value
			}
		}
		vm.Rows = append(vm.Rows, row)
	}

	// Debug: Print final view model info
	fmt.Printf("Final VM Headers: %v\n", vm.Headers)
	fmt.Printf("Final VM Columns: %v\n", vm.Columns)
	fmt.Printf("Number of rows: %d displayed out of %d total\n", vm.DisplayedRows, vm.TotalRows)

	// Print all columns info
	fmt.Printf("All Columns in VM:\n")
	for _, col := range vm.AllColumns {
		fmt.Printf("  - %s (visible: %v, entity: %v, key: %v, joins: %d)\n",
			col.Name, col.IsVisible, col.HasEntityType, col.IsKey, len(col.JoinTargets))
	}
	fmt.Printf("=================================\n\n")

	return vm
}

// BuildToggleExpansionURL creates a URL that toggles the expansion state of a path
func BuildToggleExpansionURL(q *query.Query, togglePath string) safehtml.URL {
	return q.WithExpandedToggled(togglePath)
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

// ProcessJoinsAndUpdateColumns processes the columns from the URL and handles joined columns
// It ensures that:
// 1. Joined columns (format: fromColumn.toTable.toColumn.selectedColumn) are properly added
// 2. Columns from tables no longer requested are removed
// 3. The actual table has joined columns added/removed as needed
func ProcessJoinsAndUpdateColumns(tableName string, table *tables.DataTable, view *TableView, dataModel *models.DataModel) {
	// Debug: Print processing info
	fmt.Printf("\n=== ProcessJoinsAndUpdateColumns Debug Info ===\n")
	fmt.Printf("Table: %s\n", tableName)
	fmt.Printf("View Columns before processing: %v\n", view.Columns)

	// Track which joined columns we need
	neededJoinedColumns := make(map[string]bool)

	// Parse columns to identify joined ones (with dots)
	for _, colName := range view.Columns {
		if strings.Contains(colName, ".") {
			// This is a joined column - format: fromColumn.toTable.toColumn.selectedColumn
			parts := strings.Split(colName, ".")
			if len(parts) == 4 {
				neededJoinedColumns[colName] = true
			}
		}
	}

	// Get current joined columns in the table
	currentJoinedColumns := make(map[string]bool)
	for _, colName := range table.GetJoinedColumnNames() {
		currentJoinedColumns[colName] = true
	}

	// Add needed joined columns that aren't already in the table
	for colName := range neededJoinedColumns {
		if !currentJoinedColumns[colName] {
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
			joinKey := fmt.Sprintf("%s.%s->%s.%s", tableName, fromColumn, toTable, toColumn)
			foundJoin := dataModel.GetJoin(joinKey)

			if foundJoin != nil {
				// Create the joined column
				targetTable := dataModel.GetTable(toTable)
				if targetTable != nil {
					targetDataCol := targetTable.GetColumn(selectedColumn)
					if targetDataCol != nil {
						colDef := columns.NewColumnDef(
							colName,
							fmt.Sprintf("%s %s", toTable, targetDataCol.ColumnDef().DisplayName()),
							"", // Joined columns don't have entity types
						)
						joinedColumn := targetDataCol.CreateJoinedColumn(colDef, foundJoin.Joiner)
						// Create column definition for the joined column

						// TODO: Create the actual joined column implementation
						// This would require implementing the joined column logic
						fmt.Printf("Would add joined column %s to table (not implemented)\n", colName)
						table.AddJoinedColumn(joinedColumn)
					}
				}
			}
		}
	}

	// Remove joined columns that are no longer needed
	for colName := range currentJoinedColumns {
		if !neededJoinedColumns[colName] {
			table.RemoveJoinedColumn(colName)
			fmt.Printf("Removed joined column %s from table\n", colName)
		}
	}

	// Debug: Print final state
	fmt.Printf("View Columns after processing: %v\n", view.Columns)
	fmt.Printf("Regular Table Columns: %v\n", table.GetColumnNames())
	fmt.Printf("Joined Columns in Table: %v\n", table.GetJoinedColumnNames())
	fmt.Printf("All Columns in Table: %v\n", table.GetAllColumnNames())
	fmt.Printf("===============================================\n\n")
}

// BuildAddColumnURL creates a URL that toggles a column
func BuildAddColumnURL(q *query.Query, columnName string) safehtml.URL {
	return q.WithColumnToggled(columnName)
}

// BuildAddColumnAndJoinURL creates a URL that toggles a column and manages its join
// DEPRECATED: This function is no longer used as joins are now encoded in column names
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
		joinParts := strings.Split(joinPath, "-")
		if len(joinParts) == 2 {
			toTableColumn := strings.Split(joinParts[1], ".")
			if len(toTableColumn) == 2 {
				joinedTableName := toTableColumn[0]

				// Check if any other columns from this table remain
				hasOtherColumns := false
				for col := range columns {
					if strings.HasPrefix(col, joinedTableName+".") {
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
// DEPRECATED: This function is no longer used as joins are now encoded in column names
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
func BuildToggleColumnURL(q *query.Query, toggleColumn string) safehtml.URL {
	return q.WithColumnToggled(toggleColumn)
}
