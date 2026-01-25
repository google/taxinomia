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

package demo

import (
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/server"
	"github.com/google/taxinomia/core/views"
)

// SetupDemoServer creates and configures a server with demo data
func SetupDemoServer() (*server.Server, *ProductRegistry, error) {
	fmt.Println("Starting Taxinomia...")

	// Create a DataModel to manage tables and entity types
	dataModel := models.NewDataModel()

	// Create demo tables with sample data
	ordersTable := CreateDemoTable()
	regionsTable := CreateRegionsTable()
	capitalsTable := CreateCapitalsTable()
	itemsTable := CreateItemsTable()

	// Register tables in the data model
	dataModel.AddTable("orders", ordersTable)
	dataModel.AddTable("regions", regionsTable)
	dataModel.AddTable("capitals", capitalsTable)
	dataModel.AddTable("items", itemsTable)

	// Create performance test tables for scalability testing
	fmt.Println("\n=== Creating Performance Test Tables ===")
	transactionsTable := CreatePerfTransactionsTable()
	usersTable := CreatePerfUsersTable()
	productsTable := CreatePerfProductsTable()
	categoriesTable := CreatePerfCategoriesTable()

	dataModel.AddTable("transactions_perf", transactionsTable)
	dataModel.AddTable("users_perf", usersTable)
	dataModel.AddTable("products_perf", productsTable)
	dataModel.AddTable("categories_perf", categoriesTable)
	fmt.Println("=== Performance Tables Created ===")
	fmt.Println()

	// Load textproto tables using protoloader
	fmt.Println("=== Loading Textproto Tables ===")
	_, currentFile, _, _ := runtime.Caller(0)
	protoLoader := NewProtoTableLoader()
	descriptorPath := filepath.Join(filepath.Dir(currentFile), "proto", "customer_orders.pb")
	if err := protoLoader.LoadDescriptorSet(descriptorPath); err != nil {
		fmt.Printf("Warning: Failed to load proto descriptors: %v\n", err)
	} else {
		textprotoPath := filepath.Join(filepath.Dir(currentFile), "data", "customer_orders.textproto")
		if customerOrdersTable, err := protoLoader.LoadTextprotoAsTable(textprotoPath, "taxinomia.demo.CustomerOrders"); err != nil {
			fmt.Printf("Warning: Failed to load customer_orders.textproto: %v\n", err)
		} else {
			dataModel.AddTable("customer_orders", customerOrdersTable)
			fmt.Printf("Loaded customer_orders table with %d rows\n", customerOrdersTable.Length())
		}
	}
	fmt.Println("=== Textproto Tables Loaded ===")
	fmt.Println()

	// Print reports
	printEntityTypeUsageReport(dataModel)
	printJoinDiscoveryReport(dataModel)

	// Add system tables (must be after all user tables are added)
	fmt.Println("\n=== Creating System Tables ===")
	models.AddSystemTables(dataModel)
	fmt.Println("=== System Tables Created ===")

	// Create the server
	srv, err := server.NewServer(dataModel)
	if err != nil {
		return nil, nil, err
	}

	// Load user profiles
	usersDir := filepath.Join(filepath.Dir(currentFile), "users")
	userStore := NewUserStore()
	if err := userStore.LoadFromDirectory(usersDir); err != nil {
		fmt.Printf("Warning: Failed to load user profiles: %v\n", err)
	} else {
		fmt.Printf("Loaded %d user profiles\n", len(userStore.GetAllUsers()))
		srv.SetUserStore(userStore)
	}

	// Load products from textproto files
	productsDir := filepath.Join(filepath.Dir(currentFile), "products")
	products := NewProductRegistry()

	// Set table metadata for products to filter
	// URLs are relative so they work with any product path (e.g., /default/table, /analytics/table)
	products.SetTables([]views.TableInfo{
		{
			Name:           "Orders Table",
			Description:    "Track orders with status, region, category, and amount data. Perfect for analyzing sales patterns and order fulfillment.",
			URL:            "table?table=orders&limit=25",
			RecordCount:    30,
			ColumnCount:    4,
			DefaultColumns: "4 columns",
			Categories:     "Sales, Logistics",
			Domains:        []string{"demo"},
		},
		{
			Name:           "Regions Table",
			Description:    "Geographic and economic information about different regions including population, area, capital cities, and GDP.",
			URL:            "table?table=regions&limit=25",
			RecordCount:    4,
			ColumnCount:    8,
			DefaultColumns: "5 columns",
			Categories:     "Geographic, Economic",
			Domains:        []string{"demo"},
		},
		{
			Name:           "Capitals Table",
			Description:    "Detailed information about capital cities including population, founding year, elevation, and civic infrastructure.",
			URL:            "table?table=capitals&limit=25",
			RecordCount:    4,
			ColumnCount:    11,
			DefaultColumns: "6 columns",
			Categories:     "Cities, Demographics",
			Domains:        []string{"demo"},
		},
		{
			Name:           "Items Table",
			Description:    "Product catalog with category hierarchy, pricing, inventory levels, and supplier information.",
			URL:            "table?table=items&limit=25",
			RecordCount:    15,
			ColumnCount:    11,
			DefaultColumns: "6 columns",
			Categories:     "Inventory, Products",
			Domains:        []string{"demo", "inventory"},
		},
		{
			Name:           "Customer Orders (Textproto)",
			Description:    "Denormalized customer order data loaded from textproto. Shows customer, orders, line items, and discounts.",
			URL:            "table?table=customer_orders&limit=25",
			RecordCount:    6,
			ColumnCount:    13,
			DefaultColumns: "all columns",
			Categories:     "Sales, Demo",
			Domains:        []string{"demo"},
		},
		{
			Name:           "Transactions Performance Table",
			Description:    "Large-scale transaction data (1M rows) for testing backend performance and scalability.",
			URL:            "table?table=transactions_perf&limit=100",
			RecordCount:    1000000,
			ColumnCount:    6,
			DefaultColumns: "6 columns",
			Categories:     "Performance, Testing",
			Domains:        []string{"sales", "inventory"},
		},
		{
			Name:           "Users Performance Table",
			Description:    "User data (800K rows) for high-cardinality join testing.",
			URL:            "table?table=users_perf&limit=100",
			RecordCount:    800000,
			ColumnCount:    4,
			DefaultColumns: "4 columns",
			Categories:     "Performance, Testing",
			Domains:        []string{"sales"},
		},
		{
			Name:           "Products Performance Table",
			Description:    "Product catalog (50K rows) for medium-cardinality join testing.",
			URL:            "table?table=products_perf&limit=100",
			RecordCount:    50000,
			ColumnCount:    4,
			DefaultColumns: "4 columns",
			Categories:     "Performance, Testing",
			Domains:        []string{"sales", "inventory"},
		},
		{
			Name:           "Categories Performance Table",
			Description:    "Category data (200 rows) for low-cardinality join testing.",
			URL:            "table?table=categories_perf&limit=100",
			RecordCount:    200,
			ColumnCount:    3,
			DefaultColumns: "3 columns",
			Categories:     "Performance, Testing",
			Domains:        []string{"inventory"},
		},
		{
			Name:           "System: Columns Metadata",
			Description:    "System table containing metadata about all columns across all tables.",
			URL:            "table?table=_columns&limit=100",
			RecordCount:    0,
			ColumnCount:    8,
			DefaultColumns: "5 columns",
			Categories:     "System, Metadata",
			Domains:        []string{"demo", "sales", "inventory"},
		},
	})

	if err := products.LoadFromDirectory(productsDir); err != nil {
		return nil, nil, fmt.Errorf("failed to load products: %w", err)
	}
	fmt.Printf("Loaded %d products\n", len(products.GetAll()))

	return srv, products, nil
}

// printEntityTypeUsageReport prints a comprehensive report of all entity types
func printEntityTypeUsageReport(dm *models.DataModel) {
	fmt.Println("\n=== Entity Type Usage Report ===")
	fmt.Println("This report shows all entity types and where they are used across tables.")
	fmt.Println("(Empty entity types are not included)")
	fmt.Println()

	entityUsages := dm.GetAllEntityTypes()

	var filteredUsages []models.EntityTypeUsage
	for _, usage := range entityUsages {
		if usage.EntityType != "" {
			filteredUsages = append(filteredUsages, usage)
		}
	}

	// Sort by entity type name
	for i := 0; i < len(filteredUsages)-1; i++ {
		for j := i + 1; j < len(filteredUsages); j++ {
			if filteredUsages[i].EntityType > filteredUsages[j].EntityType {
				filteredUsages[i], filteredUsages[j] = filteredUsages[j], filteredUsages[i]
			}
		}
	}

	for _, usage := range filteredUsages {
		fmt.Printf("Entity Type: '%s'\n", usage.EntityType)
		fmt.Printf("  Used in %d location(s):\n", len(usage.Usage))
		for _, ref := range usage.Usage {
			fmt.Printf("    - %s.%s\n", ref.TableName, ref.ColumnName)
		}
		fmt.Println()
	}

	fmt.Println("=== Summary ===")
	fmt.Printf("Total unique entity types: %d\n", len(filteredUsages))

	totalUsages := 0
	for _, usage := range filteredUsages {
		totalUsages += len(usage.Usage)
	}
	fmt.Printf("Total entity type usages: %d\n", totalUsages)

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
	allJoins := dm.GetJoins()

	joinsByEntityType := make(map[string][]*models.Join)
	for _, join := range allJoins {
		joinsByEntityType[join.EntityType] = append(joinsByEntityType[join.EntityType], join)
	}

	fmt.Printf("Auto-discovered %d joins across %d entity types\n", len(allJoins), len(joinsByEntityType))
}
