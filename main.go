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

package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/query"
	"github.com/google/taxinomia/core/rendering"
	"github.com/google/taxinomia/core/views"
	"github.com/google/taxinomia/demo"
)

func main() {
	fmt.Println("Starting Interpunctus V2...")

	// Create a DataModel to manage tables and entity types
	dataModel := models.NewDataModel()

	// Create demo tables with sample data
	ordersTable := demo.CreateDemoTable()
	regionsTable := demo.CreateRegionsTable()
	capitalsTable := demo.CreateCapitalsTable()
	itemsTable := demo.CreateItemsTable()

	// Register tables in the data model
	// Note: Entity types and joins are automatically discovered when tables are added
	dataModel.AddTable("orders", ordersTable)
	dataModel.AddTable("regions", regionsTable)
	dataModel.AddTable("capitals", capitalsTable)
	dataModel.AddTable("items", itemsTable)

	// Print entity type usage report
	printEntityTypeUsageReport(dataModel)

	// Print join discovery report
	printJoinDiscoveryReport(dataModel)

	// Create renderer
	renderer, err := rendering.NewTableRenderer()
	if err != nil {
		log.Fatalf("Failed to create renderer: %v", err)
	}

	// Generic table handler
	http.HandleFunc("/table", func(w http.ResponseWriter, r *http.Request) {
		// Parse URL into Query
		q := query.NewQuery(r.URL)

		// Validate table parameter
		if q.Table == "" {
			http.Error(w, "Table parameter is required", http.StatusBadRequest)
			return
		}

		// Get the table from data model
		table := dataModel.GetTable(q.Table)
		if table == nil {
			http.Error(w, fmt.Sprintf("Table '%s' not found", q.Table), http.StatusNotFound)
			return
		}

		// Define default columns for each table
		defaultColumnsByTable := map[string][]string{
			"orders":   {"status", "region", "category", "amount"},
			"regions":  {"region", "population", "capital", "timezone", "gdp"},
			"capitals": {"capital", "region", "population", "founded", "mayor", "universities"},
			"items":    {"item_id", "item_name", "category", "subcategory", "price", "stock"},
		}

		// Get default columns for this table, or use first few columns if not defined
		defaultColumns, ok := defaultColumnsByTable[q.Table]
		if !ok {
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
		view := views.TableView{
			Columns:  q.Columns,
			Expanded: expandedPaths,
		}

		// Process joins and update columns before building the view model
		views.ProcessJoinsAndUpdateColumns(q.Table, table, &view, dataModel)

		// Build the view model from the table
		title := fmt.Sprintf("%s Table - Taxinomia Demo", strings.Title(q.Table))
		viewModel := views.BuildViewModel(dataModel, q.Table, table, view, title, q)

		// Render using the renderer
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := renderer.Render(w, viewModel); err != nil {
			// Log the error instead of trying to write an error response
			// since the renderer may have already written to the response
			log.Printf("Template rendering error: %v", err)
			return
		}
	})

	// Landing page with links to tables
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		landingVM := views.LandingViewModel{
			Title:    "Taxinomia Demo Tables",
			Subtitle: "Explore the power of dynamic table rendering with column visibility and drag-and-drop ordering",
			Tables: []views.TableInfo{
				{
					Name:           "Orders Table",
					Description:    "Track orders with status, region, category, and amount data. Perfect for analyzing sales patterns and order fulfillment.",
					URL:            "/table?table=orders&limit=25",
					RecordCount:    30,
					ColumnCount:    4,
					DefaultColumns: "4 columns",
					Categories:     "Sales, Logistics",
				},
				{
					Name:           "Regions Table",
					Description:    "Geographic and economic information about different regions including population, area, capital cities, and GDP.",
					URL:            "/table?table=regions&limit=25",
					RecordCount:    4,
					ColumnCount:    8,
					DefaultColumns: "5 columns",
					Categories:     "Geographic, Economic",
				},
				{
					Name:           "Capitals Table",
					Description:    "Detailed information about capital cities including population, founding year, elevation, and civic infrastructure.",
					URL:            "/table?table=capitals&limit=25",
					RecordCount:    4,
					ColumnCount:    11,
					DefaultColumns: "6 columns",
					Categories:     "Cities, Demographics",
				},
				{
					Name:           "Items Table",
					Description:    "Product catalog with category hierarchy, pricing, inventory levels, and supplier information.",
					URL:            "/table?table=items&limit=25",
					RecordCount:    15,
					ColumnCount:    11,
					DefaultColumns: "6 columns",
					Categories:     "Inventory, Products",
				},
			},
		}

		// Render using the landing template
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := renderer.RenderLanding(w, landingVM); err != nil {
			log.Printf("Landing page rendering error: %v", err)
			return
		}
	})

	fmt.Println("\nServer starting on http://127.0.0.1:8097")
	log.Fatal(http.ListenAndServe("127.0.0.1:8097", nil))
}

// printEntityTypeUsageReport prints a comprehensive report of all entity types
// and shows which tables and columns use each entity type
func printEntityTypeUsageReport(dm *models.DataModel) {
	fmt.Println("\n=== Entity Type Usage Report ===")
	fmt.Println("This report shows all entity types and where they are used across tables.")
	fmt.Println("(Empty entity types are not included)\n")

	// Get all entity types and their usage
	entityUsages := dm.GetAllEntityTypes()

	// Filter out empty entity types
	var filteredUsages []models.EntityTypeUsage
	for _, usage := range entityUsages {
		if usage.EntityType != "" {
			filteredUsages = append(filteredUsages, usage)
		}
	}

	// Sort by entity type name for consistent output
	for i := 0; i < len(filteredUsages)-1; i++ {
		for j := i + 1; j < len(filteredUsages); j++ {
			if filteredUsages[i].EntityType > filteredUsages[j].EntityType {
				filteredUsages[i], filteredUsages[j] = filteredUsages[j], filteredUsages[i]
			}
		}
	}

	// Print each entity type and its usage
	for _, usage := range filteredUsages {
		fmt.Printf("Entity Type: '%s'\n", usage.EntityType)
		fmt.Printf("  Used in %d location(s):\n", len(usage.Usage))

		// Print each usage with table.column format
		for _, ref := range usage.Usage {
			fmt.Printf("    - %s.%s\n", ref.TableName, ref.ColumnName)
		}
		fmt.Println()
	}

	// Print summary statistics
	fmt.Println("=== Summary ===")
	fmt.Printf("Total unique entity types: %d\n", len(filteredUsages))

	// Count total usages
	totalUsages := 0
	for _, usage := range filteredUsages {
		totalUsages += len(usage.Usage)
	}
	fmt.Printf("Total entity type usages: %d\n", totalUsages)

	// Find entity types used across multiple tables
	crossTableEntities := []string{}
	for _, usage := range filteredUsages {
		tables := make(map[string]bool)
		for _, ref := range usage.Usage {
			tables[ref.TableName] = true
		}
		if len(tables) > 1 {
			crossTableEntities = append(crossTableEntities, usage.EntityType)
		}
	}

	if len(crossTableEntities) > 0 {
		fmt.Printf("\nEntity types used across multiple tables: %v\n", crossTableEntities)
		fmt.Println("These entity types can be used to establish relationships between tables.")
	}

	// Count columns without entity types
	totalColumns := 0
	columnsWithoutEntityType := 0
	for _, table := range dm.GetAllTables() {
		columnNames := table.GetColumnNames()
		for _, colName := range columnNames {
			totalColumns++
			col := table.GetColumn(colName)
			if col != nil && col.ColumnDef().EntityType() == "" {
				columnsWithoutEntityType++
			}
		}
	}

	if columnsWithoutEntityType > 0 {
		fmt.Printf("\nNote: %d out of %d columns have no entity type assigned.\n", columnsWithoutEntityType, totalColumns)
	}

	fmt.Println("\n================================")
}

// printJoinDiscoveryReport prints information about automatically discovered joins
func printJoinDiscoveryReport(dm *models.DataModel) {
	fmt.Println("\n=== Auto-discovered Joins Report ===")
	fmt.Println("Joins are automatically discovered based on entity types and unique value columns (IsKey).")

	// Get all joins that were automatically discovered
	allJoins := dm.GetJoins()
	fmt.Printf("\nTotal auto-discovered joins: %d\n", len(allJoins))

	if len(allJoins) > 0 {
		// Group joins by entity type for better readability
		joinsByEntityType := make(map[string][]*models.Join)
		for _, join := range allJoins {
			joinsByEntityType[join.EntityType] = append(joinsByEntityType[join.EntityType], join)
		}

		// Print joins grouped by entity type
		for entityType, joins := range joinsByEntityType {
			fmt.Printf("\nEntity Type '%s' joins:\n", entityType)
			for _, j := range joins {
				fmt.Printf("  %s\n", j)
			}
		}

		// Print joins by table
		// fmt.Println("\nJoins by table:")
		// for tableName := range dm.GetAllTables() {
		// 	tableJoins := dm.GetJoinsForTable(tableName)
		// 	if len(tableJoins) > 0 {
		// 		fmt.Printf("\nTable '%s':\n", tableName)
		// 		for _, j := range tableJoins {
		// 			if j.FromTable == tableName {
		// 				fmt.Printf("  -> %s.%s (outgoing)\n", j.ToTable, j.ToColumn)
		// 			} else {
		// 				fmt.Printf("  <- %s.%s (incoming)\n", j.FromTable, j.FromColumn)
		// 			}
		// 		}
		// 	}
		// }
	}

	fmt.Println("\n================================")
}
