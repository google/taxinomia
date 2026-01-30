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

package server

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/expr"
	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/query"
	"github.com/google/taxinomia/core/rendering"
	"github.com/google/taxinomia/core/tables"
	"github.com/google/taxinomia/core/users"
	"github.com/google/taxinomia/core/views"
)

// ProductConfig defines the configuration interface for a product.
// Products provide their own tables, landing page settings, and default columns.
type ProductConfig interface {
	GetName() string
	GetTitle() string
	GetSubtitle() string
	GetTables() []views.TableInfo
	GetDefaultColumns(tableName string) []string
}

// Server represents the application server with all its dependencies
type Server struct {
	dataModel      *models.DataModel
	renderer       *rendering.TableRenderer
	tableViewCache map[string]*tables.TableView
	userStore      users.UserStore

	// Caches for computed columns
	exprCache        map[string]*expr.Expression       // expression string -> compiled expression
	computedColState map[string]map[string]string      // tableName -> columnName -> expression
}

// NewServer creates a new server with the given data model
func NewServer(dataModel *models.DataModel) (*Server, error) {
	renderer, err := rendering.NewTableRenderer()
	if err != nil {
		return nil, fmt.Errorf("failed to create renderer: %w", err)
	}

	return &Server{
		dataModel:        dataModel,
		renderer:         renderer,
		tableViewCache:   make(map[string]*tables.TableView),
		exprCache:        make(map[string]*expr.Expression),
		computedColState: make(map[string]map[string]string),
	}, nil
}

// SetUserStore sets the user store for authentication
func (s *Server) SetUserStore(store users.UserStore) {
	s.userStore = store
}

// makeCacheKey creates a cache key combining user and table name
// This ensures each user has their own TableView with their own computed columns
func (s *Server) makeCacheKey(userName, tableName string) string {
	if userName == "" {
		return tableName
	}
	return userName + ":" + tableName
}

// TableHandlerResult represents the result of handling a table request
type TableHandlerResult struct {
	Error      error
	StatusCode int
	Message    string
}

// HandleTableRequest processes a table request and writes the response
// Returns an error result if the request is invalid, nil on success
func (s *Server) HandleTableRequest(w io.Writer, requestURL *url.URL, product ProductConfig, setHeader func(key, value string)) *TableHandlerResult {
	// Parse URL into Query
	q := query.NewQuery(requestURL)

	// Get user from URL parameter - cache is user-specific
	userName := requestURL.Query().Get("user")
	cacheKey := s.makeCacheKey(userName, q.Table)

	// Validate table parameter
	if q.Table == "" {
		return &TableHandlerResult{StatusCode: 400, Message: "Table parameter is required"}
	}

	// Get the table from data model
	table := s.dataModel.GetTable(q.Table)
	if table == nil {
		return &TableHandlerResult{StatusCode: 404, Message: fmt.Sprintf("Table '%s' not found", q.Table)}
	}

	// Get default columns from product, or use first few columns if not defined
	defaultColumns := product.GetDefaultColumns(q.Table)
	if len(defaultColumns) == 0 {
		// Use first 4 columns as default
		allCols := table.GetColumnNames()
		if len(allCols) > 4 {
			defaultColumns = allCols[:4]
		} else {
			defaultColumns = allCols
		}
	}

	// Use default columns if none specified
	if len(q.Columns) == 0 {
		q.Columns = defaultColumns
	}

	// Convert expanded paths to map for compatibility
	expandedPaths := make(map[string]bool)
	for _, path := range q.Expanded {
		expandedPaths[path] = true
	}

	// Define the view - which columns to display and in what order
	view := views.View{
		Columns:        q.Columns,
		Expanded:       expandedPaths,
		GroupedColumns: q.GroupedColumns,
	}

	// Get or create a cached TableView for this user+table combination
	tableView := views.GetOrCreateTableView(cacheKey, table, s.tableViewCache)

	// Update joined columns to match the current request
	views.ProcessJoinsAndUpdateColumns(tableView, &view, s.dataModel)

	// Create computed columns from the query (with caching)
	s.updateComputedColumns(tableView, q, cacheKey)

	// Apply filters to the table view
	tableView.ApplyFilters(q.Filters)

	// Apply grouping if grouped columns are specified
	if len(q.GroupedColumns) > 0 {
		// Call GroupTable - it will use the cached filter mask
		tableView.GroupTable(q.GroupedColumns, []string{}, make(map[string]tables.Compare), make(map[string]bool))

		// Output ASCII representation to console
		fmt.Println("\n=== Grouped Table (ASCII) ===")
		fmt.Printf("Table: %s\n", q.Table)
		fmt.Printf("Grouped by: %v\n", q.GroupedColumns)
		fmt.Println(tableView.ToAscii())
		fmt.Println("=============================")
	} else {
		tableView.ClearGroupings()
	}

	// Build the view model from the table view
	title := fmt.Sprintf("%s Table - Taxinomia Demo", strings.Title(q.Table))
	viewModel := views.BuildViewModel(s.dataModel, q.Table, tableView, view, title, q)

	// Set content type and render
	setHeader("Content-Type", "text/html; charset=utf-8")
	if err := s.renderer.Render(w, viewModel); err != nil {
		log.Printf("Template rendering error: %v", err)
		return &TableHandlerResult{Error: err}
	}

	return nil
}

// HandleLandingRequest processes the landing page request
func (s *Server) HandleLandingRequest(w io.Writer, requestURL *url.URL, product ProductConfig, setHeader func(key, value string)) error {
	setHeader("Content-Type", "text/html; charset=utf-8")

	// Get user from URL parameter (for testing)
	userName := requestURL.Query().Get("user")

	// Create a copy of the landing view model to filter tables
	vm := views.LandingViewModel{
		Title:    product.GetTitle(),
		Subtitle: product.GetSubtitle(),
	}

	// If we have a user store and a user parameter, filter tables by domain
	if s.userStore != nil && userName != "" {
		user := s.userStore.GetUser(userName)
		if user != nil {
			vm.UserName = userName

			// Filter tables to only those matching user's domains
			for _, table := range product.GetTables() {
				if users.HasAnyDomain(user, table.Domains) {
					vm.Tables = append(vm.Tables, table)
				}
			}
		} else {
			// Unknown user - show no tables
			vm.UserName = userName + " (unknown)"
		}
	} else {
		// No user filtering - show all tables
		vm.Tables = product.GetTables()
	}

	if err := s.renderer.RenderLanding(w, vm); err != nil {
		log.Printf("Landing page rendering error: %v", err)
		return err
	}
	return nil
}

// updateComputedColumns manages computed columns with caching.
// Only recompiles expressions and recreates columns when something has changed.
// The cacheKey is user+table specific to ensure users have isolated computed columns.
func (s *Server) updateComputedColumns(tableView *tables.TableView, q *query.Query, cacheKey string) {
	// Get current state for this user+table combination
	currentState, exists := s.computedColState[cacheKey]
	if !exists {
		currentState = make(map[string]string)
		s.computedColState[cacheKey] = currentState
	}

	// Build map of requested columns
	requested := make(map[string]string)
	for _, comp := range q.ComputedColumns {
		requested[comp.Name] = comp.Expression
	}

	// Remove columns that are no longer requested
	for name := range currentState {
		if _, ok := requested[name]; !ok {
			tableView.RemoveComputedColumn(name)
			delete(currentState, name)
		}
	}

	// Add or update columns
	for _, comp := range q.ComputedColumns {
		existingExpr, exists := currentState[comp.Name]

		// Skip if column exists with same expression
		if exists && existingExpr == comp.Expression {
			continue
		}

		// Create the column
		s.createComputedColumn(tableView, comp.Name, comp.Expression)
		currentState[comp.Name] = comp.Expression
	}
}

// createComputedColumn creates a single computed column, using cached compiled expressions.
func (s *Server) createComputedColumn(tableView *tables.TableView, name, expression string) {
	if expression == "" {
		tableView.AddComputedColumn(name, nil)
		return
	}

	// Check expression cache first
	compiled, ok := s.exprCache[expression]
	if !ok {
		var err error
		compiled, err = expr.Compile(expression)
		if err != nil {
			fmt.Printf("Warning: Failed to compile expression for column %s: %v\n", name, err)
			tableView.AddComputedColumn(name, nil)
			return
		}
		s.exprCache[expression] = compiled
	}

	// Get a reference column to determine length
	var length int
	colNames := tableView.GetColumnNames()
	if len(colNames) > 0 {
		if col := tableView.GetColumn(colNames[0]); col != nil {
			length = col.Length()
		}
	}
	if length == 0 {
		tableView.AddComputedColumn(name, nil)
		return
	}

	// Create a column getter function that retrieves values from the table view
	getColumn := func(colName string, rowIndex uint32) (expr.Value, error) {
		col := tableView.GetColumn(colName)
		if col == nil {
			return expr.NilValue(), fmt.Errorf("column '%s' not found", colName)
		}

		// Handle datetime columns - return datetime type for type-aware operations
		switch dtCol := col.(type) {
		case *columns.DatetimeColumn:
			t, err := dtCol.GetValue(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDatetime(t.UnixNano()), nil
		case *columns.JoinedDatetimeColumn:
			t, err := dtCol.GetValue(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDatetime(t.UnixNano()), nil
		case *columns.ComputedDatetimeColumn:
			nanos, err := dtCol.GetValue(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDatetime(nanos), nil
		case *columns.DurationColumn:
			nanos, err := dtCol.Nanoseconds(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDuration(nanos), nil
		case *columns.JoinedDurationColumn:
			nanos, err := dtCol.Nanoseconds(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDuration(nanos), nil
		case *columns.ComputedDurationColumn:
			nanos, err := dtCol.Nanoseconds(rowIndex)
			if err != nil {
				return expr.NilValue(), err
			}
			return expr.NewDuration(nanos), nil
		}

		strVal, err := col.GetString(rowIndex)
		if err != nil {
			return expr.NilValue(), err
		}
		// Try to parse as number first
		if numVal, err := strconv.ParseFloat(strVal, 64); err == nil {
			return expr.NewNumber(numVal), nil
		}
		return expr.NewString(strVal), nil
	}

	// Bind the expression to the column getter
	bound := compiled.Bind(getColumn)

	// Create the computed column definition
	colDef := columns.NewColumnDef(name, name, "")

	// Evaluate once on row 0 to detect the return type
	sampleVal, err := bound.Eval(0)
	if err != nil {
		// Can't determine type, fall back to string
		computedCol := columns.NewComputedStringColumn(colDef, length, func(i uint32) (string, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return "", err
			}
			return val.AsString(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
		return
	}

	// Create the appropriate column type based on the expression result
	if sampleVal.IsDuration() {
		// Duration value - create a duration column
		computedCol := columns.NewComputedDurationColumn(colDef, length, func(i uint32) (time.Duration, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return 0, err
			}
			return val.AsDuration(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	} else if sampleVal.IsDatetime() {
		// Datetime value - create a datetime column
		computedCol := columns.NewComputedDatetimeColumn(colDef, length, func(i uint32) (int64, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return 0, err
			}
			return int64(val.AsNumber()), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	} else if sampleVal.IsNumber() {
		num := sampleVal.AsNumber()
		// Check if it's an integer (no fractional part)
		if num == float64(int64(num)) {
			computedCol := columns.NewComputedInt64Column(colDef, length, func(i uint32) (int64, error) {
				val, err := bound.Eval(i)
				if err != nil {
					return 0, err
				}
				return int64(val.AsNumber()), nil
			})
			tableView.AddComputedColumn(name, computedCol)
		} else {
			computedCol := columns.NewComputedFloat64Column(colDef, length, func(i uint32) (float64, error) {
				val, err := bound.Eval(i)
				if err != nil {
					return 0, err
				}
				return val.AsNumber(), nil
			})
			tableView.AddComputedColumn(name, computedCol)
		}
	} else if sampleVal.IsBool() {
		// Boolean value - create a bool column
		computedCol := columns.NewComputedBoolColumn(colDef, length, func(i uint32) (bool, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return false, err
			}
			return val.AsBool(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	} else {
		// String - use string column
		computedCol := columns.NewComputedStringColumn(colDef, length, func(i uint32) (string, error) {
			val, err := bound.Eval(i)
			if err != nil {
				return "", err
			}
			return val.AsString(), nil
		})
		tableView.AddComputedColumn(name, computedCol)
	}
}
