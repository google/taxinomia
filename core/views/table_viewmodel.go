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
	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/grouping"
	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/query"
	"github.com/google/taxinomia/core/tables"
)

// TableViewModel contains the data from the table formatted for template consumption
type TableViewModel struct {
	Title           string
	Headers         []string               // Column display names
	Columns         []string               // Column names (for data access)
	ColumnWidths    map[string]int         // Column widths in pixels (from URL)
	Rows            []map[string]string    // Each row is a map of column name to value (flat table, ungrouped)
	RowURLs         []map[string]string    // URLs for each cell in flat rows (parallel to Rows)
	GroupedRows     []GroupedRow           // Hierarchical rows for grouped display
	IsGrouped       bool                   // Whether the table is currently grouped
	AllColumns      []ColumnInfo           // All available columns with metadata
	ComputedColumns []ComputedColumnInfo   // Computed columns defined by the user
	CurrentQuery    string                 // Current query string
	CurrentURL      safehtml.URL           // Current URL for building toggle links
	ColumnStats     []string               // Statistics for each visible column (e.g., "5 groups" or "100 rows")
	ColumnFilters    map[string]string // Filter values for each column (from URL parameters like filter:columnA=abc)
	ColumnFormulas   map[string]string // Formula for computed columns (columnName -> formula like "concat(a, b)")
	IsComputedColumn map[string]bool   // Tracks which columns are computed (for UI, even if formula is empty)

	// Pagination info
	TotalRows     int  // Total number of rows in the table
	DisplayedRows int  // Number of rows actually displayed
	HasMoreRows   bool // True if there are more rows than displayed
	CurrentLimit  int  // Current row limit

	// Validation errors
	ComputedColumnErrors map[string]ValidationError // Errors for computed columns (columnName -> error)
	FilterErrors         map[string]ValidationError // Errors for filters (columnName -> error)

	// Performance metrics
	RenderTimeMs   string        // Time to render the page in milliseconds (formatted)
	TimingBreakdown []TimingEntry // Detailed timing breakdown of operations

	// Info pane state (controlled via URL)
	ShowInfoPane bool   // Whether to show the info pane
	InfoPaneTab  string // Which tab is active: "url" or "perf"

	// Animation state (transient, from _anim URL parameter)
	AnimatedColumn string // Column that was just grouped (triggers animation)

	// Column types display (controlled via URL)
	ShowColumnTypes bool              // Whether to show the column types row
	ColumnTypes     map[string]string // Column internal types (columnName -> type string)

	// Entity type info for URL resolution
	ColumnEntityTypes map[string]string // Column entity types (columnName -> entityType)
}

// URLResolver is a function that resolves a URL for a given entity type and value.
// Returns an empty string if no URL is available.
type URLResolver func(entityType, value string) string

// TimingEntry represents a single timing measurement
type TimingEntry struct {
	Operation string // Name of the operation (e.g., "Parse Query", "Apply Filters")
	DurationMs string // Duration in milliseconds (formatted)
}

// ValidationError contains details about a validation error for display to the user
type ValidationError struct {
	Message    string // User-friendly error message
	Expression string // The problematic expression or filter value
}

// ComputedColumnInfo contains information about a computed column for UI display
type ComputedColumnInfo struct {
	Name        string       // Column name
	DisplayName string       // Display name (e.g., "concat(col1, col2)")
	IsVisible   bool         // Whether column is currently visible
	ToggleURL   safehtml.URL // URL to toggle column visibility
}

// GroupedRow represents a single row in the grouped table display
type GroupedRow struct {
	Cells []GroupedCell // Only cells that should be rendered (no skipped cells)
}

// GroupedCell represents a cell to be rendered in the grouped table
// Rowspan indicates how many rows this cell spans (1 = no span, >1 = spans multiple rows)
type GroupedCell struct {
	Value                 string       // Display value for the cell
	ValueURL              string       // URL for the cell value (if entity type has default URL)
	NumRows               int          // Number of rows in this group (for display in brackets)
	NumSubgroups          int          // Number of subgroups (0 if leaf group)
	Rowspan               int
	Title                 string       // Tooltip text for hover-over information
	FilterURL             safehtml.URL // URL to filter on this value and remove grouping
	IsGroupedColumn       bool         // True if this is a grouped column cell (shows filter link)
	ColumnName            string       // Column name for identifying cells in multi-select mode
	RawValue              string       // Raw value for multi-select filtering (without display formatting)
	IsValueSorted         bool         // True if sorting by grouped value (not aggregate) - value should be bold
	IsRowCountSorted      bool         // True if sorting by row count - row count should be bold
	IsSubgroupCountSorted bool         // True if sorting by subgroup count - subgroup count should be bold
	// ColumnAggregates holds the formatted aggregates for all leaf columns in this group.
	// Each entry represents one leaf column with its name and enabled aggregates.
	ColumnAggregates []aggregates.ColumnAggregateDisplay
	// IsIncomplete indicates the cell belongs to a group that was truncated due to display limits.
	// Such cells should be visually distinguished (e.g., light grey background).
	IsIncomplete bool
}

// AggregateToggle represents a single aggregate toggle button for the UI
type AggregateToggle struct {
	Type      query.AggregateType // The aggregate type
	Symbol    string              // Display symbol (e.g., "Σ", "μ")
	Title     string              // Tooltip text
	IsEnabled bool                // Whether this aggregate is currently enabled
	ToggleURL safehtml.URL        // URL to toggle this aggregate
}

// ColumnInfo contains information about a column for UI display
type ColumnInfo struct {
	Name              string             // Column internal name
	DisplayName       string             // Column display name
	IsVisible         bool               // Whether column is currently visible
	IsGrouped         bool               // Whether column is currently grouped
	IsFiltered        bool               // Whether column has an active filter
	HasEntityType     bool               // Whether column defines an entity type
	IsKey             bool               // Whether column has all unique values
	JoinTargets       []JoinTarget       // Tables/columns this column can join to
	IsExpanded        bool               // Whether this column's join list is expanded
	Path              string             // Path for URL encoding (e.g., "column1")
	ToggleURL         safehtml.URL       // URL to toggle expansion
	ToggleColumnURL   safehtml.URL       // URL to toggle column visibility (preserves all query params)
	ToggleGroupingURL safehtml.URL       // URL to toggle grouping for this column
	SortIndex         int                // 1-based sort priority (0 = not in sort order)
	IsSortDescending  bool               // True if sorted descending
	ToggleSortURL     safehtml.URL       // URL to toggle sort for this column
	ColumnType        query.ColumnType   // Column data type for determining available aggregates
	AggregateToggles  []AggregateToggle  // Available aggregate toggles for this column
	// Aggregate sort for grouped columns
	AggSortToggleURL     safehtml.URL       // URL to cycle through aggregate sort options
	AggSortDirectionURL  safehtml.URL       // URL to toggle aggregate sort direction (asc/desc)
	HasAggSort           bool               // Whether this grouped column has an aggregate sort
	AggSortLeafCol       string             // Leaf column being sorted by (if HasAggSort)
	AggSortAggType       string             // Aggregate type being sorted by (if HasAggSort)
	AggSortSymbol        string             // Symbol for the aggregate being sorted by
	IsAggSortDescending  bool               // Whether aggregate sort is descending
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
	Name           string
	DisplayName    string
	HasEntityType  bool
	IsKey          bool
	TableName      string       // The table this column belongs to
	Path           string       // Path for URL encoding
	JoinTargets    []JoinTarget // Tables/columns this column can join to
	HasJoinTargets bool         // Whether this column has join targets (even if not expanded)
	IsExpanded     bool         // Whether this column's join list is expanded
	ToggleURL      safehtml.URL // URL to toggle expansion
	AddColumnURL   safehtml.URL // URL to add this column and its join to the view
	IsSelected     bool         // Whether this column is already in the current view
}

// getColumnType determines the query.ColumnType for a column by checking its actual type.
// It delegates to TableView.GetColumnType which does proper type assertion on the column.
func getColumnType(colName string, tableView *tables.TableView) query.ColumnType {
	return tableView.GetColumnType(colName)
}

// buildAggregateToggles creates the aggregate toggle buttons for a column
func buildAggregateToggles(colName string, colType query.ColumnType, q *query.Query) []AggregateToggle {
	availableAggs := query.GetAvailableAggregates(colType)
	toggles := make([]AggregateToggle, 0, len(availableAggs))

	for _, aggType := range availableAggs {
		toggles = append(toggles, AggregateToggle{
			Type:      aggType,
			Symbol:    query.AggregateSymbol(aggType),
			Title:     query.AggregateTitle(aggType),
			IsEnabled: q.IsAggregateEnabled(colName, aggType),
			ToggleURL: q.WithAggregateToggled(colName, aggType),
		})
	}

	return toggles
}

// detectCycle checks if a table appears in the path to prevent infinite loops
// baseTable is the original table we're starting the join from (e.g., "orders")
func detectCycle(path string, targetTable string, baseTable string) bool {
	// First check if we're trying to join back to the base table
	if targetTable == baseTable {
		return true
	}
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

// buildJoinTargetsForColumn builds join targets for a column
// columnNamePrefix is the accumulated column name for chained joins (e.g., "region.regions.region" for a second hop)
// baseTable is the original table we started from (for cycle detection)
func buildJoinTargetsForColumn(dataModel *models.DataModel, tableName, columnName string, basePath string, columnNamePrefix string, baseTable string, expandedPaths map[string]bool, q *query.Query) []JoinTarget {

	var joinTargets []JoinTarget
	allJoins := dataModel.GetJoins()

	for _, join := range allJoins {
		var target JoinTarget
		var targetTableName, targetColumnName string

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
		} else if toTableKey == tableName && toColumnKey == columnName {
			// This is an incoming join (reverse)
			targetTableName = fromTableKey
			targetColumnName = fromColumnKey
		} else {
			continue
		}

		// Build the path for this target
		targetPath := fmt.Sprintf("%s/%s.%s", basePath, targetTableName, targetColumnName)

		// Check for cycles
		isBlocked := detectCycle(basePath, targetTableName, baseTable)

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

						// Build column name by appending to the prefix
						// Format: prefix.toTable.toColumn.selectedColumn
						// For single hop: region.regions.region.timezone
						// For double hop: region.regions.region.capital.capitals.capital.mayor
						columnFullName := fmt.Sprintf("%s.%s.%s.%s", columnNamePrefix, targetTableName, targetColumnName, targetColName)
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

								if checkTable != "" && !detectCycle(colPath, checkTable, baseTable) {
									hasValidJoins = true
									break
								}
							}

							if hasValidJoins {
								colSummary.HasJoinTargets = true
								if isExpanded {
									// Recursively build join targets for this column
									// The new prefix is the full path to this column: prefix.table.joinCol.thisCol
									// e.g., for "capital" column in regions with prefix "region":
									//   newPrefix = "region.regions.region.capital"
									// Then the next call will build: newPrefix.nextTable.nextJoinCol.selectedCol
									//   = "region.regions.region.capital.capitals.capital.mayor"
									newPrefix := fmt.Sprintf("%s.%s.%s.%s", columnNamePrefix, targetTableName, targetColumnName, targetColName)
									colSummary.JoinTargets = buildJoinTargetsForColumn(dataModel, targetTableName, targetColName, colPath, newPrefix, baseTable, expandedPaths, q)
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

// BuildViewModel creates a ViewModel from a TableView using the specified View
// computedColErrors and filterErrors are maps of column/filter names to error messages
// urlResolver is an optional function to resolve entity type URLs (can be nil)
func BuildViewModel(dataModel *models.DataModel, tableName string, tableView *tables.TableView, view View, title string, q *query.Query, computedColErrors, filterErrors map[string]string, urlResolver URLResolver) TableViewModel {
	// Generate currentURL from Query
	currentURL := q.ToSafeURL()

	vm := TableViewModel{
		Title:                title,
		Headers:              []string{},
		Columns:              []string{},
		ColumnWidths:         make(map[string]int),
		Rows:                 []map[string]string{},
		RowURLs:              []map[string]string{},
		AllColumns:           []ColumnInfo{},
		CurrentURL:           currentURL,
		ColumnFilters:        make(map[string]string),
		ColumnFormulas:       make(map[string]string),
		IsComputedColumn:     make(map[string]bool),
		ComputedColumnErrors: make(map[string]ValidationError),
		FilterErrors:         make(map[string]ValidationError),
		ColumnTypes:          make(map[string]string),
		ColumnEntityTypes:    make(map[string]string),
	}

	// Convert error strings to ValidationError structs
	for colName, errMsg := range computedColErrors {
		// Find the expression for this column
		expr := ""
		for _, comp := range q.ComputedColumns {
			if comp.Name == colName {
				expr = comp.Expression
				break
			}
		}
		vm.ComputedColumnErrors[colName] = ValidationError{
			Message:    errMsg,
			Expression: expr,
		}
	}
	for colName, errMsg := range filterErrors {
		vm.FilterErrors[colName] = ValidationError{
			Message:    errMsg,
			Expression: q.Filters[colName],
		}
	}

	// Copy filter parameters from Query
	vm.ColumnFilters = q.Filters

	// Copy column widths from Query
	for colName, width := range q.ColumnWidths {
		vm.ColumnWidths[colName] = width
	}

	// Create a map of visible columns for quick lookup
	visibleCols := make(map[string]bool)
	for _, colName := range view.Columns {
		visibleCols[colName] = true
	}

	// Build a map of grouped columns for quick lookup
	groupedColsMap := make(map[string]bool)
	for _, colName := range q.GroupedColumns {
		groupedColsMap[colName] = true
	}

	// Build leaf columns list (visible, non-grouped columns) and enabled aggregates for aggregate sort toggle
	var leafColumns []string
	enabledAggs := make(map[string][]query.AggregateType)
	for _, colName := range view.Columns {
		if !groupedColsMap[colName] {
			leafColumns = append(leafColumns, colName)
			colType := getColumnType(colName, tableView)
			enabledAggs[colName] = q.GetEnabledAggregates(colName, colType)
		}
	}

	// Build all columns info (base table columns only)
	allColumnNames := tableView.GetColumnNames()
	for _, colName := range allColumnNames {
		col := tableView.GetColumn(colName)
		if col != nil {
			// Check if column has an entity type
			entityType := col.ColumnDef().EntityType()
			hasEntityType := entityType != ""
			if hasEntityType {
				vm.ColumnEntityTypes[colName] = entityType
			}

			// Use the column's IsKey property
			isKey := col.IsKey()

			// Find join targets for this column
			var joinTargets []JoinTarget
			if hasEntityType {
				// Build the base path for this column
				basePath := colName
				// For first-level joins, the prefix is just the column name
				// Pass tableName as baseTable for cycle detection
				joinTargets = buildJoinTargetsForColumn(dataModel, tableName, colName, basePath, colName, tableName, view.Expanded, q)
			}

			// Check if this column is expanded
			isExpanded := false
			if view.Expanded != nil {
				isExpanded = view.Expanded[colName]
			}

			// Check if this column has an active filter
			_, isFiltered := q.Filters[colName]

			// Determine column type and build aggregate toggles
			colType := getColumnType(colName, tableView)
			aggToggles := buildAggregateToggles(colName, colType, q)

			// Build aggregate sort fields for grouped columns
			var aggSortToggleURL, aggSortDirectionURL safehtml.URL
			var hasAggSort, isAggSortDescending bool
			var aggSortLeafCol, aggSortAggType, aggSortSymbol string
			if q.IsColumnGrouped(colName) && len(leafColumns) > 0 {
				aggSortToggleURL = q.WithNextGroupAggSort(colName, leafColumns, enabledAggs)
				if aggSort := q.GetGroupAggSort(colName); aggSort != nil {
					hasAggSort = true
					aggSortLeafCol = aggSort.LeafColumn
					aggSortAggType = string(aggSort.AggType)
					aggSortSymbol = query.AggregateSymbol(aggSort.AggType)
					isAggSortDescending = aggSort.Descending
					aggSortDirectionURL = q.WithGroupAggSortDirectionToggled(colName)
				}
			}

			vm.AllColumns = append(vm.AllColumns, ColumnInfo{
				Name:                colName,
				DisplayName:         col.ColumnDef().DisplayName(),
				IsVisible:           visibleCols[colName],
				IsGrouped:           q.IsColumnGrouped(colName),
				IsFiltered:          isFiltered,
				HasEntityType:       hasEntityType,
				IsKey:               isKey && hasEntityType, // Only mark as key if it's also an entity type
				JoinTargets:         joinTargets,
				IsExpanded:          isExpanded,
				Path:                colName,
				ToggleURL:           BuildToggleExpansionURL(q, colName),
				ToggleColumnURL:     BuildToggleColumnURL(q, colName),
				ToggleGroupingURL:   BuildToggleGroupingURL(q, colName),
				SortIndex:           q.GetSortIndex(colName),
				IsSortDescending:    q.IsSortedDescending(colName),
				ToggleSortURL:       q.WithSortToggled(colName),
				ColumnType:          colType,
				AggregateToggles:    aggToggles,
				AggSortToggleURL:    aggSortToggleURL,
				AggSortDirectionURL: aggSortDirectionURL,
				HasAggSort:          hasAggSort,
				AggSortLeafCol:      aggSortLeafCol,
				AggSortAggType:      aggSortAggType,
				AggSortSymbol:       aggSortSymbol,
				IsAggSortDescending: isAggSortDescending,
			})
		}
	}

	// Add joined columns to AllColumns if they are in the view
	for _, colName := range view.Columns {
		if strings.Contains(colName, ".") {
			// This is a joined column
			// Valid formats: 4 parts (1 hop), 7 parts (2 hops), 10 parts (3 hops), etc.
			// Pattern: (numParts - 1) % 3 == 0
			parts := strings.Split(colName, ".")
			numParts := len(parts)
			if numParts >= 4 && (numParts-1)%3 == 0 {
				// Check if this column is already in AllColumns (it shouldn't be)
				found := false
				for _, col := range vm.AllColumns {
					if col.Name == colName {
						found = true
						break
					}
				}

				if !found {
					// Build a better display name from the last hop
					lastTable := parts[numParts-3]
					lastColumn := parts[numParts-1]
					displayName := fmt.Sprintf("%s → %s", lastTable, lastColumn)

					// Determine column type and build aggregate toggles for joined column
					// Use full colName path, not lastColumn, since tableView stores joined columns by full path
					colType := getColumnType(colName, tableView)
					aggToggles := buildAggregateToggles(colName, colType, q)

					// Build aggregate sort fields for grouped joined columns
					var aggSortToggleURL, aggSortDirectionURL safehtml.URL
					var hasAggSort, isAggSortDescending bool
					var aggSortLeafCol, aggSortAggType, aggSortSymbol string
					if q.IsColumnGrouped(colName) && len(leafColumns) > 0 {
						aggSortToggleURL = q.WithNextGroupAggSort(colName, leafColumns, enabledAggs)
						if aggSort := q.GetGroupAggSort(colName); aggSort != nil {
							hasAggSort = true
							aggSortLeafCol = aggSort.LeafColumn
							aggSortAggType = string(aggSort.AggType)
							aggSortSymbol = query.AggregateSymbol(aggSort.AggType)
							isAggSortDescending = aggSort.Descending
							aggSortDirectionURL = q.WithGroupAggSortDirectionToggled(colName)
						}
					}

					// Add the joined column info
					vm.AllColumns = append(vm.AllColumns, ColumnInfo{
						Name:                colName,
						DisplayName:         displayName,
						IsVisible:           visibleCols[colName],
						IsGrouped:           q.IsColumnGrouped(colName),
						HasEntityType:       false, // Joined columns don't have entity types in this context
						IsKey:               false,
						JoinTargets:         nil,
						IsExpanded:          false,
						Path:                colName,
						ToggleURL:           BuildToggleExpansionURL(q, colName),
						ToggleColumnURL:     BuildToggleColumnURL(q, colName),
						ToggleGroupingURL:   BuildToggleGroupingURL(q, colName),
						SortIndex:           q.GetSortIndex(colName),
						IsSortDescending:    q.IsSortedDescending(colName),
						ToggleSortURL:       q.WithSortToggled(colName),
						ColumnType:          colType,
						AggregateToggles:    aggToggles,
						AggSortToggleURL:    aggSortToggleURL,
						AggSortDirectionURL: aggSortDirectionURL,
						HasAggSort:          hasAggSort,
						AggSortLeafCol:      aggSortLeafCol,
						AggSortAggType:      aggSortAggType,
						AggSortSymbol:       aggSortSymbol,
						IsAggSortDescending: isAggSortDescending,
					})
				}
			}
		}
	}

	// Add computed columns to AllColumns
	for _, comp := range q.ComputedColumns {
		// Check if this column has an active filter
		_, isFiltered := q.Filters[comp.Name]

		// Get actual column type from the computed column
		colType := getColumnType(comp.Name, tableView)
		aggToggles := buildAggregateToggles(comp.Name, colType, q)

		// Build aggregate sort fields for grouped computed columns
		var aggSortToggleURL, aggSortDirectionURL safehtml.URL
		var hasAggSort, isAggSortDescending bool
		var aggSortLeafCol, aggSortAggType, aggSortSymbol string
		if q.IsColumnGrouped(comp.Name) && len(leafColumns) > 0 {
			aggSortToggleURL = q.WithNextGroupAggSort(comp.Name, leafColumns, enabledAggs)
			if aggSort := q.GetGroupAggSort(comp.Name); aggSort != nil {
				hasAggSort = true
				aggSortLeafCol = aggSort.LeafColumn
				aggSortAggType = string(aggSort.AggType)
				aggSortSymbol = query.AggregateSymbol(aggSort.AggType)
				isAggSortDescending = aggSort.Descending
				aggSortDirectionURL = q.WithGroupAggSortDirectionToggled(comp.Name)
			}
		}

		vm.AllColumns = append(vm.AllColumns, ColumnInfo{
			Name:                comp.Name,
			DisplayName:         comp.Name,
			IsVisible:           visibleCols[comp.Name],
			IsGrouped:           q.IsColumnGrouped(comp.Name),
			IsFiltered:          isFiltered,
			IsKey:               false,
			HasEntityType:       false,
			JoinTargets:         nil,
			IsExpanded:          false,
			Path:                comp.Name,
			ToggleURL:           BuildToggleExpansionURL(q, comp.Name),
			ToggleColumnURL:     BuildToggleColumnURL(q, comp.Name),
			ToggleGroupingURL:   BuildToggleGroupingURL(q, comp.Name),
			SortIndex:           q.GetSortIndex(comp.Name),
			IsSortDescending:    q.IsSortedDescending(comp.Name),
			ToggleSortURL:       q.WithSortToggled(comp.Name),
			ColumnType:          colType,
			AggregateToggles:    aggToggles,
			AggSortToggleURL:    aggSortToggleURL,
			AggSortDirectionURL: aggSortDirectionURL,
			HasAggSort:          hasAggSort,
			AggSortLeafCol:      aggSortLeafCol,
			AggSortAggType:      aggSortAggType,
			AggSortSymbol:       aggSortSymbol,
			IsAggSortDescending: isAggSortDescending,
		})
	}

	// Sort all columns alphabetically by DisplayName
	sort.Slice(vm.AllColumns, func(i, j int) bool {
		return vm.AllColumns[i].DisplayName < vm.AllColumns[j].DisplayName
	})

	// Build a map of computed column names for quick lookup
	computedColNames := make(map[string]bool)
	for _, comp := range q.ComputedColumns {
		computedColNames[comp.Name] = true
	}

	// Build headers and columns from view
	for _, colName := range view.Columns {
		// Check if this is a joined column (format: fromColumn.toTable.toColumn.selectedColumn)
		if strings.Contains(colName, ".") {
			// This is a joined column
			// Valid formats: 4 parts (1 hop), 7 parts (2 hops), 10 parts (3 hops), etc.
			// Pattern: (numParts - 1) % 3 == 0
			parts := strings.Split(colName, ".")
			numParts := len(parts)
			if numParts >= 4 && (numParts-1)%3 == 0 {
				// Build a display name from the last hop: "TableName → ColumnName"
				// For multi-hop, use the final table and column
				lastTable := parts[numParts-3]    // Second to last triplet's table
				lastColumn := parts[numParts-1]   // Selected column
				displayName := fmt.Sprintf("%s → %s", lastTable, lastColumn)
				vm.Headers = append(vm.Headers, displayName)
				vm.Columns = append(vm.Columns, colName)
			}
		} else {
			// Regular column from the main table or computed column
			col := tableView.GetColumn(colName)
			if col != nil {
				vm.Headers = append(vm.Headers, col.ColumnDef().DisplayName())
				vm.Columns = append(vm.Columns, colName)
			} else if computedColNames[colName] {
				// This is a computed column that couldn't be created (e.g., invalid expression)
				// Still add it to headers with its name so the user can see it
				vm.Headers = append(vm.Headers, colName)
				vm.Columns = append(vm.Columns, colName)
			}
		}

		// Populate column type for display (Go struct name)
		vm.ColumnTypes[colName] = tableView.GetColumnTypeName(colName)
	}

	// Get filtered row count and rows from TableView
	totalRows := tableView.GetFilteredRowCount()
	vm.TotalRows = totalRows

	// Get filtered rows with limit and sorting applied
	if len(q.SortOrder) > 0 {
		// Use sorted version with heap-based top-K selection
		vm.Rows = tableView.GetFilteredRowsSorted(view.Columns, q.SortOrder, q.Limit)
	} else {
		// No sorting - use basic filtered rows
		vm.Rows = tableView.GetFilteredRows(view.Columns, q.Limit)
	}
	vm.DisplayedRows = len(vm.Rows)
	vm.CurrentLimit = q.Limit
	vm.HasMoreRows = q.Limit > 0 && totalRows > q.Limit

	// Build RowURLs for flat rows if URL resolver is provided
	if urlResolver != nil && len(vm.ColumnEntityTypes) > 0 {
		vm.RowURLs = make([]map[string]string, len(vm.Rows))
		for i, row := range vm.Rows {
			rowURLs := make(map[string]string)
			for colName, entityType := range vm.ColumnEntityTypes {
				if value, ok := row[colName]; ok && value != "" {
					if url := urlResolver(entityType, value); url != "" {
						rowURLs[colName] = url
					}
				}
			}
			if len(rowURLs) > 0 {
				vm.RowURLs[i] = rowURLs
			}
		}
	}

	// Check if table is grouped and build grouped rows if needed
	if tableView.IsGrouped() {
		vm.IsGrouped = true
		// Sort groups by aggregate values if specified
		if len(q.GroupAggregateSorts) > 0 {
			tableView.SortGroupsByAggregate(q.GroupAggregateSorts)
		}
		// Build grouped rows with limit - stops early and marks incomplete groups
		groupResult := buildGroupedRows(tableView, view.Columns, q, q.Limit, vm.ColumnEntityTypes, urlResolver)
		vm.GroupedRows = groupResult.Rows

		// Update pagination info for grouped views
		vm.TotalRows = groupResult.TotalRows
		vm.DisplayedRows = groupResult.ShownRows
		vm.HasMoreRows = groupResult.ShownRows < groupResult.TotalRows
	}

	vm.ColumnStats = buildColumnStats(tableView)

	// Build computed columns info for sidebar display and populate formulas
	for _, computed := range q.ComputedColumns {
		isVisible := visibleCols[computed.Name]
		vm.ComputedColumns = append(vm.ComputedColumns, ComputedColumnInfo{
			Name:        computed.Name,
			DisplayName: computed.Expression,
			IsVisible:   isVisible,
			ToggleURL:   q.WithColumnToggled(computed.Name),
		})
		// Add formula to ColumnFormulas map for display in header row
		vm.ColumnFormulas[computed.Name] = computed.Expression
		// Mark this column as computed (for UI, even if formula is empty)
		vm.IsComputedColumn[computed.Name] = true
	}

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

// GetOrCreateTableView retrieves or creates a TableView for the specified table
// This should be called with a cached map to reuse TableViews across requests
func GetOrCreateTableView(tableName string, table *tables.DataTable, tableViewCache map[string]*tables.TableView) *tables.TableView {
	// Check if we already have a TableView for this table
	if tv, exists := tableViewCache[tableName]; exists {
		return tv
	}

	// Create a new TableView and cache it
	tableView := tables.NewTableView(table, tableName)
	tableViewCache[tableName] = tableView
	return tableView
}

// ProcessJoinsAndUpdateColumns processes the columns from the URL and handles joined columns
// It ensures that:
// 1. Joined columns (format: fromColumn.toTable.toColumn.selectedColumn) are properly added
// 2. Joined columns no longer needed are removed
// 3. Updates the provided TableView in place
func ProcessJoinsAndUpdateColumns(tableView *tables.TableView, view *View, dataModel *models.DataModel) {
	// Update joined columns using the TableView's method
	tableView.UpdateJoinedColumns(view.Columns, dataModel)
	tableView.VisibleColumns = view.Columns
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

// BuildToggleGroupingURL creates a URL that toggles grouping for a column
func BuildToggleGroupingURL(q *query.Query, columnName string) safehtml.URL {
	return q.WithGroupedColumnToggled(columnName)
}

// GroupBuildResult contains the result of building grouped rows
type GroupBuildResult struct {
	Rows       []GroupedRow
	Truncated  bool // True if display row limit was reached
	TotalRows  int  // Total display rows that would exist without limit
	ShownRows  int  // Actual display rows shown
}

// buildGroupedRows converts the hierarchical grouping structure into rows with rowspan
// It walks the group hierarchy recursively, using group.Height() for rowspan
// If limit > 0, stops after limit display rows and marks incomplete groups
func buildGroupedRows(tableView *tables.TableView, visibleColumns []string, q *query.Query, limit int, columnEntityTypes map[string]string, urlResolver URLResolver) GroupBuildResult {
	firstBlock := tableView.GetFirstBlock()
	if firstBlock == nil {
		return GroupBuildResult{}
	}

	// Calculate total rows without limit for display purposes
	totalRows := 0
	for _, g := range firstBlock.Groups {
		totalRows += g.Height()
	}

	var rows []GroupedRow = []GroupedRow{{Cells: []GroupedCell{}}}
	rowCount := 0
	truncated := walkGroupHierarchy(tableView, firstBlock, &rows, 0, q, limit, &rowCount, columnEntityTypes, urlResolver)

	// Remove trailing empty row if present
	if len(rows) > 0 && len(rows[len(rows)-1].Cells) == 0 {
		rows = rows[:len(rows)-1]
	}

	// If truncated, fix rowspans for cells that now extend beyond the array
	if truncated && len(rows) > 0 {
		fixRowspans(rows)
	}

	return GroupBuildResult{
		Rows:      rows,
		Truncated: truncated,
		TotalRows: totalRows,
		ShownRows: len(rows),
	}
}

// fixRowspans adjusts rowspans for cells that extend beyond the actual row count
// Also marks cells as incomplete if their rowspan was reduced (meaning they were truncated)
func fixRowspans(rows []GroupedRow) {
	limit := len(rows)
	for rowIdx := range rows {
		for cellIdx := range rows[rowIdx].Cells {
			cell := &rows[rowIdx].Cells[cellIdx]
			if cell.Rowspan > 1 {
				// Cell starts at rowIdx and spans Rowspan rows
				// After truncation, it can only span to the end of the array
				maxRowspan := limit - rowIdx
				if cell.Rowspan > maxRowspan {
					cell.Rowspan = maxRowspan
					// Mark as incomplete since this cell was truncated
					cell.IsIncomplete = true
				}
			}
		}
	}
}

// walkGroupHierarchy recursively walks the group hierarchy and builds rows
// level indicates the depth in the grouping hierarchy (0 = first grouped column)
// limit is the max display rows (0 = unlimited), rowCount tracks current count
// columnEntityTypes maps column names to their entity types for URL resolution
// urlResolver resolves entity type URLs (can be nil)
// Returns true if truncated due to limit
func walkGroupHierarchy(tableView *tables.TableView, block *grouping.Block, rows *[]GroupedRow, level int, q *query.Query, limit int, rowCount *int, columnEntityTypes map[string]string, urlResolver URLResolver) bool {
	if block == nil {
		return false
	}
	// Get the column name for this grouping level
	colName := block.GroupedColumn.DataColumn.ColumnDef().Name()

	for _, group := range block.Groups {
		// Check if we've hit the limit before processing this group
		if limit > 0 && *rowCount >= limit {
			return true
		}
		// Get the raw value for filtering
		rawValue := group.GetValue()
		numRows := len(group.Indices)
		numSubgroups := group.NumSubgroups()

		// Tooltip explains the bracket format
		var tooltip string
		if numSubgroups > 0 {
			tooltip = "[subgroups/rows]"
		} else {
			tooltip = "[rows]"
		}

		// Build column aggregates for this group
		// Only show aggregates in grouped column cells that are NOT the last grouped column,
		// because leaf cells already display their own aggregates.
		// Also check if this grouped column has an aggregate sort to mark the sorted aggregate.
		var columnAggs []aggregates.ColumnAggregateDisplay
		aggSort := q.GetGroupAggSort(colName)
		if group.Aggregates != nil && group.ChildBlock != nil {
			for _, leafColName := range tableView.GetLeafColumns() {
				state := group.Aggregates[leafColName]
				colType := tableView.GetColumnType(leafColName)
				enabledAggs := q.GetEnabledAggregates(leafColName, colType)
				if len(enabledAggs) > 0 && state != nil {
					// Mark the sorted aggregate if this is the sorted leaf column
					var sortedCol string
					var sortedAgg query.AggregateType
					if aggSort != nil && leafColName == aggSort.LeafColumn {
						sortedCol = leafColName
						sortedAgg = aggSort.AggType
					}
					columnAggs = append(columnAggs, aggregates.ColumnAggregateDisplay{
						ColumnName: leafColName,
						Aggregates: aggregates.FormatAggregatesWithSort(state, enabledAggs, sortedCol, sortedAgg),
					})
				}
			}
		}

		// Add the grouped column cell for this group
		// IsValueSorted is true if column is sorted and not using aggregate sort
		isValueSorted := q.GetSortIndex(colName) > 0 && aggSort == nil
		// Check if sorting by row count or subgroup count
		isRowCountSorted := aggSort != nil && aggSort.AggType == query.AggRowCount
		isSubgroupCountSorted := aggSort != nil && aggSort.AggType == query.AggSubgroupCount

		// Resolve URL for the cell value if entity type is defined
		var valueURL string
		if urlResolver != nil && rawValue != "" {
			if entityType, ok := columnEntityTypes[colName]; ok {
				valueURL = urlResolver(entityType, rawValue)
			}
		}

		groupedCell := GroupedCell{
			Value:                 rawValue,
			ValueURL:              valueURL,
			NumRows:               numRows,
			NumSubgroups:          numSubgroups,
			Rowspan:               group.Height(),
			Title:                 tooltip,
			FilterURL:             q.WithFilterAndUngrouped(colName, rawValue),
			IsGroupedColumn:       true,
			ColumnName:            colName,
			RawValue:              rawValue,
			IsValueSorted:         isValueSorted,
			IsRowCountSorted:      isRowCountSorted,
			IsSubgroupCountSorted: isSubgroupCountSorted,
			ColumnAggregates:      columnAggs,
			IsIncomplete:          false, // Set by fixRowspans based on rowspan reduction
		}
		(*rows)[len(*rows)-1].Cells = append((*rows)[len(*rows)-1].Cells, groupedCell)

		if group.ChildBlock == nil {
			// Leaf group - add cells for "other" (non-filtered) leaf columns with their aggregates
			for _, leafColName := range tableView.GetOtherLeafColumns() {
				// Build aggregates for this specific leaf column
				var leafAggs []aggregates.ColumnAggregateDisplay
				if group.Aggregates != nil {
					state := group.Aggregates[leafColName]
					colType := tableView.GetColumnType(leafColName)
					enabledAggs := q.GetEnabledAggregates(leafColName, colType)
					if len(enabledAggs) > 0 && state != nil {
						// Mark the sorted aggregate if this is the sorted leaf column
						var sortedCol string
						var sortedAgg query.AggregateType
						if aggSort != nil && leafColName == aggSort.LeafColumn {
							sortedCol = leafColName
							sortedAgg = aggSort.AggType
						}
						leafAggs = append(leafAggs, aggregates.ColumnAggregateDisplay{
							ColumnName: leafColName,
							Aggregates: aggregates.FormatAggregatesWithSort(state, enabledAggs, sortedCol, sortedAgg),
						})
					}
				}
				(*rows)[len(*rows)-1].Cells = append((*rows)[len(*rows)-1].Cells, GroupedCell{
					Value:            "",
					Rowspan:          group.Height(),
					Title:            "",
					ColumnName:       leafColName,
					ColumnAggregates: leafAggs,
					IsIncomplete:     false, // Set by fixRowspans based on rowspan reduction
				})
			}

			// Add cells for filtered leaf columns with their aggregates
			filteredLeafCols := tableView.GetFilteredLeafColumns()
			cells := make([]GroupedCell, len(filteredLeafCols))
			for i, leafColName := range filteredLeafCols {
				// Build aggregates for this specific leaf column
				var leafAggs []aggregates.ColumnAggregateDisplay
				if group.Aggregates != nil {
					state := group.Aggregates[leafColName]
					colType := tableView.GetColumnType(leafColName)
					enabledAggs := q.GetEnabledAggregates(leafColName, colType)
					if len(enabledAggs) > 0 && state != nil {
						// Mark the sorted aggregate if this is the sorted leaf column
						var sortedCol string
						var sortedAgg query.AggregateType
						if aggSort != nil && leafColName == aggSort.LeafColumn {
							sortedCol = leafColName
							sortedAgg = aggSort.AggType
						}
						leafAggs = append(leafAggs, aggregates.ColumnAggregateDisplay{
							ColumnName: leafColName,
							Aggregates: aggregates.FormatAggregatesWithSort(state, enabledAggs, sortedCol, sortedAgg),
						})
					}
				}
				cells[i] = GroupedCell{
					Value:            "",
					Rowspan:          group.Height(),
					Title:            "",
					ColumnName:       leafColName,
					ColumnAggregates: leafAggs,
					IsIncomplete:     false, // Set by fixRowspans based on rowspan reduction
				}
			}
			(*rows)[len(*rows)-1].Cells = append(cells, (*rows)[len(*rows)-1].Cells...)

			// Increment row count for this leaf group
			*rowCount++

			// Start a new row for the next group
			*rows = append(*rows, GroupedRow{Cells: []GroupedCell{}})
		} else {
			// Non-leaf group - recurse into child blocks
			truncated := walkGroupHierarchy(tableView, group.ChildBlock, rows, level+1, q, limit, rowCount, columnEntityTypes, urlResolver)
			if truncated {
				return true
			}
		}
	}
	return false
}

func buildColumnStats(tableView *tables.TableView) []string {
	stats := make([]string, len(tableView.VisibleColumns))
	filteredRows := tableView.GetFilteredRowCount()
	totalRows := tableView.NumRows()

	for i, colName := range tableView.VisibleColumns {
		if tableView.IsColGrouped(colName) {
			numGroups := tableView.GetGroupCount(colName)
			// For grouped columns: "groups / filtered / total"
			if filteredRows == totalRows {
				stats[i] = fmt.Sprintf("%d / %d", numGroups, totalRows)
			} else {
				stats[i] = fmt.Sprintf("%d / %d / %d", numGroups, filteredRows, totalRows)
			}
		} else {
			// For ungrouped columns: "filtered / total"
			if filteredRows == totalRows {
				stats[i] = fmt.Sprintf("%d", totalRows)
			} else {
				stats[i] = fmt.Sprintf("%d / %d", filteredRows, totalRows)
			}
		}
	}
	return stats
}
