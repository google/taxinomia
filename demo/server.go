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
	"strings"

	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/query"
	"github.com/google/taxinomia/core/server"
	"github.com/google/taxinomia/core/views"
	"github.com/google/taxinomia/datasources"
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

	// Create events table with datetime columns
	fmt.Println("\n=== Creating Events Table (Datetime Demo) ===")
	eventsTable := CreateEventsTable()
	dataModel.AddTable("events", eventsTable)
	fmt.Println("=== Events Table Created ===")

	// Create metrics table with int64/uint64 columns
	fmt.Println("\n=== Creating Metrics Table (Int64/Uint64 Demo) ===")
	metricsTable := CreateMetricsTable()
	dataModel.AddTable("metrics", metricsTable)
	fmt.Println("=== Metrics Table Created ===")

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

	// Create Google infrastructure tables
	fmt.Println("=== Creating Google Infrastructure Tables ===")
	googleRegionsTable := CreateGoogleRegionsTable()
	googleZonesTable := CreateGoogleZonesTable()
	googleClustersTable := CreateGoogleClustersTable()
	googleRacksTable := CreateGoogleRacksTable()
	googleMachinesTable := CreateGoogleMachinesTable()
	googleCellsTable := CreateGoogleCellsTable()
	googleJobsTable := CreateGoogleJobsTable()
	googleTasksTable := CreateGoogleTasksTable(googleJobsTable)
	googleAllocsTable := CreateGoogleAllocsTable(googleTasksTable)

	dataModel.AddTable("google_regions", googleRegionsTable)
	dataModel.AddTable("google_zones", googleZonesTable)
	dataModel.AddTable("google_clusters", googleClustersTable)
	dataModel.AddTable("google_racks", googleRacksTable)
	dataModel.AddTable("google_machines", googleMachinesTable)
	dataModel.AddTable("google_cells", googleCellsTable)
	dataModel.AddTable("google_jobs", googleJobsTable)
	dataModel.AddTable("google_tasks", googleTasksTable)
	dataModel.AddTable("google_allocs", googleAllocsTable)

	fmt.Printf("Created Google tables: regions=%d, zones=%d, clusters=%d, racks=%d, machines=%d\n",
		googleRegionsTable.Length(), googleZonesTable.Length(), googleClustersTable.Length(),
		googleRacksTable.Length(), googleMachinesTable.Length())
	fmt.Printf("Created Google tables: cells=%d, jobs=%d, tasks=%d, allocs=%d\n",
		googleCellsTable.Length(), googleJobsTable.Length(), googleTasksTable.Length(),
		googleAllocsTable.Length())
	fmt.Println("=== Google Infrastructure Tables Created ===")
	fmt.Println()

	// Create manager early so we can register programmatic tables for hierarchy lookups
	dsManager := datasources.NewManager()
	dsManager.RegisterLoader(datasources.NewProtoLoader())

	// Register Google tables with dsManager for hierarchy lookups
	// This allows BuildHierarchyLookups to scan them for parent-child relationships
	dsManager.RegisterTable("google_regions", googleRegionsTable)
	dsManager.RegisterTable("google_zones", googleZonesTable)
	dsManager.RegisterTable("google_clusters", googleClustersTable)
	dsManager.RegisterTable("google_racks", googleRacksTable)
	dsManager.RegisterTable("google_machines", googleMachinesTable)
	dsManager.RegisterTable("google_cells", googleCellsTable)
	dsManager.RegisterTable("google_jobs", googleJobsTable)
	dsManager.RegisterTable("google_tasks", googleTasksTable)
	dsManager.RegisterTable("google_allocs", googleAllocsTable)

	// Load protobuf tables using datasources.Manager
	// This demonstrates the 3-phase loading architecture:
	// 1. Schema Discovery: loader.DiscoverSchema() discovers column names/types from data source
	// 2. Schema Enrichment: manager applies annotations (display names, entity types)
	// 3. Data Loading: loader.Load() creates table with enriched schema
	fmt.Println("=== Loading Protobuf Tables via DataSources Manager ===")
	_, currentFile, _, _ := runtime.Caller(0)

	// Load configuration from data_sources.textproto
	// - Annotations are loaded eagerly (display names, entity types)
	// - Source metadata is registered (data loaded lazily)
	configPath := filepath.Join(filepath.Dir(currentFile), "data", "data_sources.textproto")
	if err := dsManager.LoadConfig(configPath); err != nil {
		fmt.Printf("Warning: Failed to load data sources config: %v\n", err)
	} else {
		fmt.Printf("Loaded annotations: %v\n", dsManager.GetAnnotationIDs())
		fmt.Printf("Registered sources: %v\n", dsManager.GetSourceNames())

		// Load data on demand - this triggers the 3-phase process
		if customerOrdersTable, err := dsManager.LoadData("customer_orders"); err != nil {
			fmt.Printf("Warning: Failed to load customer_orders: %v\n", err)
		} else {
			dataModel.AddTable("customer_orders", customerOrdersTable)
			fmt.Printf("Loaded customer_orders (textproto) with %d rows\n", customerOrdersTable.Length())
		}

		// Load binary proto version - uses same annotations, different source
		if customerOrdersBinaryTable, err := dsManager.LoadData("customer_orders_binary"); err != nil {
			fmt.Printf("Warning: Failed to load customer_orders_binary: %v\n", err)
		} else {
			dataModel.AddTable("customer_orders_binary", customerOrdersBinaryTable)
			fmt.Printf("Loaded customer_orders_binary (binary proto) with %d rows\n", customerOrdersBinaryTable.Length())
		}
	}
	fmt.Println("=== Protobuf Tables Loaded ===")
	fmt.Println()

	// Add hierarchy ancestor columns to Google tables
	// This enables filtering/grouping by ancestor entity types (e.g., filter machines by region)
	// Columns are named with § prefix: §google.region, §google.zone, etc.
	// Columns are only added if the table doesn't already have a column with that entity type.
	fmt.Println("\n=== Adding Hierarchy Ancestor Columns ===")
	dsManager.AddHierarchyAncestorColumns(googleMachinesTable, "google.machine")
	dsManager.AddHierarchyAncestorColumns(googleRacksTable, "google.rack")
	dsManager.AddHierarchyAncestorColumns(googleClustersTable, "google.cluster")
	dsManager.AddHierarchyAncestorColumns(googleZonesTable, "google.zone")
	dsManager.AddHierarchyAncestorColumns(googleTasksTable, "google.task")
	dsManager.AddHierarchyAncestorColumns(googleAllocsTable, "google.alloc")
	dsManager.AddHierarchyAncestorColumns(googleJobsTable, "google.job")
	dsManager.AddHierarchyAncestorColumns(googleCellsTable, "google.cell")
	fmt.Println("=== Hierarchy Ancestor Columns Added ===")

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

	// Set up URL resolver for entity type links
	srv.SetURLResolver(dsManager.ResolveDefaultURL)

	// Set up all URLs resolver for detail panel
	srv.SetAllURLsResolver(func(entityType, value string) []views.EntityURL {
		resolved := dsManager.GetAllURLs(entityType, value)
		if len(resolved) == 0 {
			return nil
		}
		result := make([]views.EntityURL, len(resolved))
		for i, r := range resolved {
			result[i] = views.EntityURL{Name: r.Name, URL: r.URL}
		}
		return result
	})

	// Set up primary key resolver for table metadata
	// First try the datasources config, then programmatic tables, then IsKey columns
	googleTablePrimaryKeys := map[string]string{
		"google_regions":  "google.region",
		"google_zones":    "google.zone",
		"google_clusters": "google.cluster",
		"google_racks":    "google.rack",
		"google_machines": "google.machine",
		"google_cells":    "google.cell",
		"google_jobs":     "google.job",
		"google_tasks":    "google.task",
		"google_allocs":   "google.alloc",
	}
	srv.SetPrimaryKeyResolver(func(tableName string) string {
		// Try datasources config first
		if pk := dsManager.GetPrimaryKeyEntityType(tableName); pk != "" {
			return pk
		}
		// Try programmatic Google tables
		if pk, ok := googleTablePrimaryKeys[tableName]; ok {
			return pk
		}
		// Fall back to detecting from IsKey column in the table
		if table := dataModel.GetTable(tableName); table != nil {
			for _, colName := range table.GetColumnNames() {
				if col := table.GetColumn(colName); col != nil && col.IsKey() {
					if et := col.ColumnDef().EntityType(); et != "" {
						return et
					}
				}
			}
		}
		return ""
	})

	// Set up entity type description resolver
	srv.SetEntityTypeDescriptionResolver(dsManager.GetEntityTypeDescription)

	// Set up hierarchy context builder for the detail panel
	srv.SetHierarchyContextBuilder(func(
		currentQuery *query.Query,
		primaryKeyEntityType string,
		primaryKeyValue string,
		rowData map[string]string,
		columnEntityTypes map[string]string,
	) []views.HierarchyContext {
		hierarchies := dsManager.GetHierarchiesForEntityType(primaryKeyEntityType)
		if len(hierarchies) == 0 {
			return nil
		}

		// Build reverse map: entity type -> column name
		entityTypeToColumn := make(map[string]string)
		for colName, et := range columnEntityTypes {
			entityTypeToColumn[et] = colName
		}

		var contexts []views.HierarchyContext
		for _, h := range hierarchies {
			levels := h.GetLevels()

			// Find the position of the primary key entity type in this hierarchy
			currentIdx := -1
			for i, level := range levels {
				if level == primaryKeyEntityType {
					currentIdx = i
					break
				}
			}
			if currentIdx == -1 {
				continue // Entity type not found in hierarchy (shouldn't happen)
			}

			ctx := views.HierarchyContext{
				HierarchyName: h.GetName(),
				Description:   h.GetDescription(),
			}

			// Build ancestors (levels above current)
			for i := 0; i < currentIdx; i++ {
				et := levels[i]
				level := views.HierarchyLevel{
					EntityType:  et,
					DisplayName: formatEntityTypeName(et),
					Description: dsManager.GetEntityTypeDescription(et),
				}

				// Find the value from row data (ancestor columns are now available as §-prefixed columns)
				if colName, ok := entityTypeToColumn[et]; ok {
					if value, ok := rowData[colName]; ok && value != "" {
						level.Value = value
						// Generate internal navigation URL to select this ancestor in its table
						level.ValueURL = generateAncestorURL(currentQuery, et, value)
					}
				}

				ctx.Ancestors = append(ctx.Ancestors, level)
			}

			// Build current level
			ctx.Current = views.HierarchyLevel{
				EntityType:  primaryKeyEntityType,
				DisplayName: formatEntityTypeName(primaryKeyEntityType),
				Value:       primaryKeyValue,
				Description: dsManager.GetEntityTypeDescription(primaryKeyEntityType),
			}

			// Build descendants (levels below current)
			for i := currentIdx + 1; i < len(levels); i++ {
				et := levels[i]
				level := views.HierarchyLevel{
					EntityType:  et,
					DisplayName: formatEntityTypeNamePlural(et),
					Description: dsManager.GetEntityTypeDescription(et),
				}

				// Generate list URL that filters by the current item
				// Preserves non-table-specific query state (limit, info pane, etc.)
				level.ListURL = generateDescendantListURL(currentQuery, et, primaryKeyEntityType, primaryKeyValue)

				ctx.Descendants = append(ctx.Descendants, level)
			}

			contexts = append(contexts, ctx)
		}

		return contexts
	})

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
			Description:    "Denormalized customer order data loaded from textproto format. Shows customer, orders, line items, and discounts.",
			URL:            "table?table=customer_orders&limit=25",
			RecordCount:    6,
			ColumnCount:    13,
			DefaultColumns: "all columns",
			Categories:     "Sales, Demo",
			Domains:        []string{"demo"},
		},
		{
			Name:           "Customer Orders (Binary Proto)",
			Description:    "Same data as textproto version but loaded from binary protobuf format. Binary is ~70% smaller and faster to parse.",
			URL:            "table?table=customer_orders_binary&limit=25",
			RecordCount:    6,
			ColumnCount:    13,
			DefaultColumns: "all columns",
			Categories:     "Sales, Demo",
			Domains:        []string{"demo"},
		},
		{
			Name:           "Events Table (Datetime Demo)",
			Description:    "Demonstrates datetime column functionality with event scheduling data. Use epoch functions like months(), quarters(), years() for grouping.",
			URL:            "table?table=events&limit=50",
			RecordCount:    50,
			ColumnCount:    6,
			DefaultColumns: "6 columns",
			Categories:     "Demo, Datetime",
			Domains:        []string{"demo"},
		},
		{
			Name:           "Metrics Table (Int64/Uint64 Demo)",
			Description:    "Demonstrates int64 and uint64 column types with network metrics data. Features large record IDs, byte counts, and signed deltas.",
			URL:            "table?table=metrics&limit=30",
			RecordCount:    30,
			ColumnCount:    7,
			DefaultColumns: "7 columns",
			Categories:     "Demo, Metrics",
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
		// Google Infrastructure Tables
		{
			Name:           "Google: Regions",
			Description:    "Geographic regions containing data centers. Top of the physical hierarchy.",
			URL:            "table?table=google_regions&limit=25",
			RecordCount:    8,
			ColumnCount:    6,
			DefaultColumns: "6 columns",
			Categories:     "Google, Infrastructure",
			Domains:        []string{"google"},
		},
		{
			Name:           "Google: Zones",
			Description:    "Data center zones within regions. Contains clusters of machines.",
			URL:            "table?table=google_zones&limit=25",
			RecordCount:    24,
			ColumnCount:    8,
			DefaultColumns: "6 columns",
			Categories:     "Google, Infrastructure",
			Domains:        []string{"google"},
		},
		{
			Name:           "Google: Clusters",
			Description:    "Machine clusters within zones. Grouped by purpose (serving, batch, storage, ml).",
			URL:            "table?table=google_clusters&limit=50",
			RecordCount:    120,
			ColumnCount:    8,
			DefaultColumns: "6 columns",
			Categories:     "Google, Infrastructure",
			Domains:        []string{"google"},
		},
		{
			Name:           "Google: Racks",
			Description:    "Physical server racks within clusters. Contains 4-6 machines each.",
			URL:            "table?table=google_racks&limit=100",
			RecordCount:    2400,
			ColumnCount:    8,
			DefaultColumns: "6 columns",
			Categories:     "Google, Infrastructure",
			Domains:        []string{"google"},
		},
		{
			Name:           "Google: Machines",
			Description:    "Physical servers with CPU, memory, disk, and GPU resources. Shows health status and utilization.",
			URL:            "table?table=google_machines&limit=100",
			RecordCount:    12000,
			ColumnCount:    13,
			DefaultColumns: "8 columns",
			Categories:     "Google, Infrastructure",
			Domains:        []string{"google"},
		},
		{
			Name:           "Google: Cells",
			Description:    "Borg cells that manage workloads. One cell per cluster.",
			URL:            "table?table=google_cells&limit=50",
			RecordCount:    120,
			ColumnCount:    9,
			DefaultColumns: "6 columns",
			Categories:     "Google, Workloads",
			Domains:        []string{"google"},
		},
		{
			Name:           "Google: Jobs",
			Description:    "Workload definitions running in cells. Has priority (production, batch, best-effort).",
			URL:            "table?table=google_jobs&limit=100",
			RecordCount:    5000,
			ColumnCount:    12,
			DefaultColumns: "8 columns",
			Categories:     "Google, Workloads",
			Domains:        []string{"google"},
		},
		{
			Name:           "Google: Tasks",
			Description:    "Running task instances. Links jobs to machines. Shows CPU/memory usage.",
			URL:            "table?table=google_tasks&limit=100",
			RecordCount:    50000,
			ColumnCount:    12,
			DefaultColumns: "8 columns",
			Categories:     "Google, Workloads",
			Domains:        []string{"google"},
		},
		{
			Name:           "Google: Allocs",
			Description:    "Resource allocations (CPU, memory, disk) for tasks. Shows requested vs used.",
			URL:            "table?table=google_allocs&limit=100",
			RecordCount:    150000,
			ColumnCount:    9,
			DefaultColumns: "7 columns",
			Categories:     "Google, Workloads",
			Domains:        []string{"google"},
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

// formatEntityTypeName extracts a display name from an entity type.
// For example, "google.cluster" becomes "Cluster", "demo.order_id" becomes "Order Id".
func formatEntityTypeName(entityType string) string {
	// Remove prefix (e.g., "google." or "demo.")
	name := entityType
	if idx := strings.LastIndex(entityType, "."); idx != -1 {
		name = entityType[idx+1:]
	}

	// Convert underscores to spaces and title case
	name = strings.ReplaceAll(name, "_", " ")
	return strings.Title(name)
}

// formatEntityTypeNamePlural extracts a pluralized display name from an entity type.
// For example, "google.cluster" becomes "Clusters", "google.machine" becomes "Machines".
func formatEntityTypeNamePlural(entityType string) string {
	name := formatEntityTypeName(entityType)
	// Simple pluralization
	if strings.HasSuffix(name, "s") {
		return name + "es"
	}
	return name + "s"
}

// generateAncestorURL generates a URL to navigate to an ancestor's table with that row selected.
// For example, if viewing a machine and clicking on its cluster ancestor,
// this would generate a URL like "table?table=google_clusters&row=us-east-a-c0"
func generateAncestorURL(currentQuery *query.Query, entityType, value string) string {
	q := currentQuery.Clone()
	q.Path = "table"
	q.Table = entityTypeToTableName(entityType)
	q.ClearTableSpecificState()
	q.SelectedRowID = value

	return q.ToURL()
}

// generateDescendantListURL generates a URL to list items of a descendant entity type
// filtered by the current item's value. Preserves non-table-specific query state.
//
// Uses the column name derived from the entity type for filtering. Most tables already
// have columns for their ancestors (e.g., machines has cluster, zone columns).
// For example, if viewing a cluster (google.cluster) and the descendant is "google.machine",
// this would generate a URL like "table?table=google_machines&filter:cluster=us-east-a-c0"
func generateDescendantListURL(currentQuery *query.Query, descendantEntityType, parentEntityType, parentValue string) string {
	// Clone the current query to preserve non-table-specific state (limit, info pane, etc.)
	q := currentQuery.Clone()
	q.Path = "table"
	q.Table = entityTypeToTableName(descendantEntityType)
	q.ClearTableSpecificState()

	// Use the column name derived from entity type (e.g., "google.cluster" -> "cluster")
	// Tables typically have columns named after their ancestors directly
	columnName := entityTypeToColumnName(parentEntityType)
	q.Filters[columnName] = `"` + parentValue + `"`

	return q.ToURL()
}

// entityTypeToTableName converts an entity type to a table name.
// Convention: "google.cluster" -> "google_clusters", "google.machine" -> "google_machines"
func entityTypeToTableName(entityType string) string {
	// Remove prefix and add 's' for plural
	name := entityType
	if idx := strings.LastIndex(entityType, "."); idx != -1 {
		prefix := entityType[:idx]
		suffix := entityType[idx+1:]
		// Handle special pluralization
		if strings.HasSuffix(suffix, "s") {
			name = prefix + "_" + suffix + "es"
		} else {
			name = prefix + "_" + suffix + "s"
		}
	}
	return strings.ReplaceAll(name, ".", "_")
}

// entityTypeToColumnName extracts a column name from an entity type.
// Convention: "google.cluster" -> "cluster", "demo.order_id" -> "order_id"
func entityTypeToColumnName(entityType string) string {
	if idx := strings.LastIndex(entityType, "."); idx != -1 {
		return entityType[idx+1:]
	}
	return entityType
}
