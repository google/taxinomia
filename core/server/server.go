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
	"strings"

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
}

// NewServer creates a new server with the given data model
func NewServer(dataModel *models.DataModel) (*Server, error) {
	renderer, err := rendering.NewTableRenderer()
	if err != nil {
		return nil, fmt.Errorf("failed to create renderer: %w", err)
	}

	return &Server{
		dataModel:      dataModel,
		renderer:       renderer,
		tableViewCache: make(map[string]*tables.TableView),
	}, nil
}

// SetUserStore sets the user store for authentication
func (s *Server) SetUserStore(store users.UserStore) {
	s.userStore = store
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

	// Get or create a cached TableView for this table
	tableView := views.GetOrCreateTableView(q.Table, table, s.tableViewCache)

	// Update joined columns to match the current request
	views.ProcessJoinsAndUpdateColumns(tableView, &view, s.dataModel)

	// Create computed columns from the query
	createComputedColumns(tableView, q)

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

// createComputedColumns registers computed columns from the query with the table view.
// Expression parsing and evaluation will be implemented later.
func createComputedColumns(tableView *tables.TableView, q *query.Query) {
	for _, comp := range q.ComputedColumns {
		tableView.AddComputedColumn(comp.Name, nil)
	}
}
