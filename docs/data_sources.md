# Data Sources Architecture

This document describes the architecture for loading data into Taxinomia from various sources (protobuf, CSV, databases, etc.) with support for reusable column annotations.

## Design Principles

1. **Separation of Schema and Annotations**: The actual schema (column names, types) is discovered from the data source itself (proto descriptor, database schema, CSV header). Column annotations (display names, entity types) are loaded separately and applied on top.

2. **Reusable Column Annotations**: Column metadata (display names, entity types) is defined once per annotation set and shared across all sources using those annotations.

3. **Extensibility**: Custom loaders can be implemented for databases, APIs, or any other data source.

4. **Entity-based Joins**: Entity types enable joins between tables from different sources.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     DataSourcesConfig                            │
├─────────────────────────────────────────────────────────────────┤
│  annotations: map[annotations_id] → ColumnAnnotations            │
│    - Loaded eagerly at startup                                  │
│    - Contains column metadata (display_name, entity_type)       │
├─────────────────────────────────────────────────────────────────┤
│  sources: []DataSource                                          │
│    - Metadata only (name, type, annotations_id, connection info)│
│    - Data loaded lazily when requested                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DataSourceManager                           │
├─────────────────────────────────────────────────────────────────┤
│  LoadAnnotations() → Load all column annotations (eager)        │
│  GetSourceNames()  → List available data sources                │
│  GetAnnotations(id)→ Get annotations for a source               │
│  LoadData(name)    → Load data for a source (lazy)              │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration Schema

### Column Annotations

Column annotations define display metadata for columns. The actual column schema (names, types) comes from the data source itself:

```protobuf
message ColumnAnnotation {
  string name = 1;           // Column name (matches field name in data)
  string display_name = 2;   // Human-readable name for UI
  string entity_type = 3;    // Entity type for joins (e.g., "customer_id")
}

message ColumnAnnotations {
  string annotations_id = 1;      // Unique identifier for this annotation set
  repeated ColumnAnnotation columns = 2;
}
```

### Data Sources

Each data source references annotations and specifies how to load data. The design uses a **type + config** pattern for full extensibility - users can add new source types without modifying the proto definition:

```protobuf
message DataSource {
  string name = 1;           // Table name in data model
  string annotations_id = 2; // References a ColumnAnnotations
  repeated string domains = 3; // Logical grouping (e.g., "sales", "demo")

  // Extensible source type - users register loaders for custom types
  string source_type = 4;              // "proto", "csv", "postgres", "bigquery", etc.
  map<string, string> config = 5;      // Type-specific configuration
}
```

### Complete Configuration

```protobuf
message DataSourcesConfig {
  // Column annotations - loaded eagerly
  repeated ColumnAnnotations annotations = 1;

  // Data sources - metadata only, data loaded lazily
  repeated DataSource sources = 2;
}
```

## Example Configuration

```textproto
# Column annotations (loaded at startup)
annotations {
  annotations_id: "taxinomia.demo.CustomerOrders"
  columns { name: "customer_id"   display_name: "Customer ID"   entity_type: "customer_id" }
  columns { name: "customer_name" display_name: "Customer Name" }
  columns { name: "order_id"      display_name: "Order ID"      entity_type: "order_id" }
  columns { name: "order_date"    display_name: "Order Date" }
  columns { name: "status"        display_name: "Status"        entity_type: "order_status" }
  columns { name: "product_id"    display_name: "Product ID"    entity_type: "product_id" }
  columns { name: "quantity"      display_name: "Quantity" }
  columns { name: "unit_price"    display_name: "Unit Price" }
}

annotations {
  annotations_id: "sales.transactions"
  columns { name: "transaction_id" display_name: "Transaction" entity_type: "transaction_id" }
  columns { name: "customer_id"    display_name: "Customer"    entity_type: "customer_id" }
  columns { name: "amount"         display_name: "Amount" }
  columns { name: "timestamp"      display_name: "Time" }
}

# Data sources (data loaded on demand)
# Each source specifies a source_type and type-specific config

sources {
  name: "customer_orders"
  annotations_id: "taxinomia.demo.CustomerOrders"
  domains: "demo"
  source_type: "proto"
  config { key: "descriptor_set" value: "demo/customer_orders.pb" }
  config { key: "proto_file"     value: "demo/data/customer_orders.textproto" }
  config { key: "message_type"   value: "taxinomia.demo.CustomerOrders" }
  config { key: "format"         value: "textproto" }
}

sources {
  name: "customer_orders_binary"
  annotations_id: "taxinomia.demo.CustomerOrders"  # Same annotations as above
  domains: "demo"
  source_type: "proto"
  config { key: "descriptor_set" value: "demo/customer_orders.pb" }
  config { key: "proto_file"     value: "demo/data/customer_orders.binpb" }
  config { key: "message_type"   value: "taxinomia.demo.CustomerOrders" }
  config { key: "format"         value: "binary" }
}

sources {
  name: "sales_transactions"
  annotations_id: "sales.transactions"
  domains: "sales"
  source_type: "csv"
  config { key: "file_path"  value: "data/sales/transactions.csv" }
  config { key: "has_header" value: "true" }
}

sources {
  name: "live_transactions"
  annotations_id: "sales.transactions"  # Reuses the same annotations
  domains: "sales"
  source_type: "postgres"  # User-registered custom loader
  config { key: "connection_string" value: "postgres://localhost/sales" }
  config { key: "query"             value: "SELECT * FROM transactions" }
}

sources {
  name: "analytics_events"
  annotations_id: "analytics.events"
  domains: "analytics"
  source_type: "bigquery"  # Another user-registered loader
  config { key: "project"  value: "my-gcp-project" }
  config { key: "dataset"  value: "analytics" }
  config { key: "table"    value: "events" }
}
```

## Loading Behavior

### Eager: Annotation Loading

When the application starts, all annotations are loaded immediately:

```go
type DataSourceManager struct {
    annotations map[string]*ColumnAnnotations  // Loaded at init
    sources     map[string]*DataSource         // Metadata only
    tables      map[string]*tables.DataTable   // Lazily populated
    loaders     map[string]DataSourceLoader    // Custom loaders
}

func (m *DataSourceManager) LoadAnnotations(config *DataSourcesConfig) error {
    // Load all annotations into memory
    for _, ann := range config.Annotations {
        m.annotations[ann.AnnotationsId] = ann
    }

    // Store source metadata (not data)
    for _, source := range config.Sources {
        m.sources[source.Name] = source
    }

    return nil
}
```

This enables:
- Annotation validation at startup
- Immediate access to column metadata for UI
- Entity type discovery for potential joins

### Lazy: Data Loading

Data is only loaded when explicitly requested:

```go
func (m *DataSourceManager) LoadData(sourceName string) (*tables.DataTable, error) {
    // Return cached table if already loaded
    if table, ok := m.tables[sourceName]; ok {
        return table, nil
    }

    source := m.sources[sourceName]
    annotations := m.annotations[source.AnnotationsId]

    // Find the loader for this source type
    loader, ok := m.loaders[source.SourceType]
    if !ok {
        return nil, fmt.Errorf("no loader registered for source type %q", source.SourceType)
    }

    // Load the data
    table, err := loader.Load(source.Config, annotations)
    if err != nil {
        return nil, err
    }

    // Cache the loaded table
    m.tables[sourceName] = table
    return table, nil
}
```

## Custom Loaders

The loader system is fully extensible. Users implement the `DataSourceLoader` interface and register it with a type identifier:

```go
// DataSourceLoader is the interface for all data source loaders.
// Taxinomia provides built-in loaders for "proto" and "csv".
// Users can register additional loaders for databases, APIs, or custom formats.
type DataSourceLoader interface {
    // SourceType returns the type identifier used in config (e.g., "postgres", "bigquery")
    SourceType() string

    // DiscoverSchema returns the schema discovered from the data source.
    // This is called first to determine column names and types.
    DiscoverSchema(config map[string]string) (*TableSchema, error)

    // Load retrieves data and returns a DataTable.
    // The enriched columns contain the discovered schema plus annotations.
    Load(config map[string]string, columns []*EnrichedColumn) (*tables.DataTable, error)
}
```

The loading process has three distinct phases:

1. **Schema Discovery**: Loader discovers column names and types from the data source
2. **Schema Enrichment**: Manager applies annotations (display names, entity types)
3. **Data Loading**: Loader creates table using enriched schema

### Registering Loaders

```go
// Create the manager
manager := NewDataSourceManager()

// Built-in loaders are registered automatically
// manager.RegisterLoader(&ProtoLoader{})  // handles source_type: "proto"
// manager.RegisterLoader(&CsvLoader{})    // handles source_type: "csv"

// Register custom loaders
manager.RegisterLoader(&PostgresLoader{connPool: pool})
manager.RegisterLoader(&BigQueryLoader{client: bqClient})
manager.RegisterLoader(&MyCompanyInternalLoader{})
```

### Example: PostgreSQL Loader

```go
type PostgresLoader struct {
    connPool *sql.DB
}

func (l *PostgresLoader) SourceType() string {
    return "postgres"
}

func (l *PostgresLoader) DiscoverSchema(config map[string]string) (*TableSchema, error) {
    query := config["query"]

    // Query column metadata from database
    rows, err := l.connPool.Query(query + " LIMIT 0")
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    columnTypes, _ := rows.ColumnTypes()
    schema := &TableSchema{Columns: make([]*ColumnSchema, len(columnTypes))}
    for i, ct := range columnTypes {
        schema.Columns[i] = &ColumnSchema{
            Name: ct.Name(),
            Type: sqlTypeToColumnType(ct.DatabaseTypeName()),
        }
    }
    return schema, nil
}

func (l *PostgresLoader) Load(config map[string]string, columns []*EnrichedColumn) (*tables.DataTable, error) {
    query := config["query"]

    rows, err := l.connPool.Query(query)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    // Convert rows to DataTable using enriched column definitions
    return convertRowsToTable(rows, columns)
}
```

### Example: BigQuery Loader

```go
type BigQueryLoader struct {
    client *bigquery.Client
}

func (l *BigQueryLoader) SourceType() string {
    return "bigquery"
}

func (l *BigQueryLoader) DiscoverSchema(config map[string]string) (*TableSchema, error) {
    project := config["project"]
    dataset := config["dataset"]
    tableName := config["table"]

    // Get table metadata
    meta, err := l.client.Dataset(dataset).Table(tableName).Metadata(context.Background())
    if err != nil {
        return nil, err
    }

    schema := &TableSchema{Columns: make([]*ColumnSchema, len(meta.Schema))}
    for i, field := range meta.Schema {
        schema.Columns[i] = &ColumnSchema{
            Name: field.Name,
            Type: bqTypeToColumnType(field.Type),
        }
    }
    return schema, nil
}

func (l *BigQueryLoader) Load(config map[string]string, columns []*EnrichedColumn) (*tables.DataTable, error) {
    project := config["project"]
    dataset := config["dataset"]
    tableName := config["table"]

    query := l.client.Query(fmt.Sprintf(
        "SELECT * FROM `%s.%s.%s`",
        project, dataset, tableName,
    ))

    it, err := query.Read(context.Background())
    if err != nil {
        return nil, err
    }

    return convertBigQueryToTable(it, columns)
}
```

### Example: REST API Loader

```go
type RestAPILoader struct {
    httpClient *http.Client
}

func (l *RestAPILoader) SourceType() string {
    return "rest_api"
}

func (l *RestAPILoader) DiscoverSchema(config map[string]string) (*TableSchema, error) {
    // Fetch schema endpoint or sample data
    schemaURL := config["schema_url"]
    if schemaURL == "" {
        schemaURL = config["url"] + "/schema"
    }

    resp, err := l.httpClient.Get(schemaURL)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var schemaData struct {
        Columns []struct {
            Name string `json:"name"`
            Type string `json:"type"`
        } `json:"columns"`
    }
    json.NewDecoder(resp.Body).Decode(&schemaData)

    schema := &TableSchema{Columns: make([]*ColumnSchema, len(schemaData.Columns))}
    for i, col := range schemaData.Columns {
        schema.Columns[i] = &ColumnSchema{
            Name: col.Name,
            Type: jsonTypeToColumnType(col.Type),
        }
    }
    return schema, nil
}

func (l *RestAPILoader) Load(config map[string]string, columns []*EnrichedColumn) (*tables.DataTable, error) {
    url := config["url"]
    authHeader := config["auth_header"]

    req, _ := http.NewRequest("GET", url, nil)
    if authHeader != "" {
        req.Header.Set("Authorization", authHeader)
    }

    resp, err := l.httpClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var data []map[string]interface{}
    json.NewDecoder(resp.Body).Decode(&data)

    return convertJSONToTable(data, columns)
}
```

## Entity Types and Joins

Entity types enable joining tables from different sources. For example:

| Table | Column | Entity Type |
|-------|--------|-------------|
| customer_orders | customer_id | customer_id |
| sales_transactions | customer_id | customer_id |
| live_transactions | customer_id | customer_id |

All three tables can be joined on `customer_id` because they share the same entity type, even though they come from different sources (protobuf, CSV, and database).

## API Summary

```go
// Initialize manager
manager := NewDataSourceManager()

// Register custom loaders (optional)
manager.RegisterLoader("postgres", &PostgresLoader{})
manager.RegisterLoader("rest_api", &RestAPILoader{})

// Load configuration - annotations loaded eagerly, data sources registered
config := LoadConfig("data_sources.textproto")
manager.LoadAnnotations(config)

// Get available sources (no data loaded yet)
sources := manager.GetSourceNames() // ["customer_orders", "sales_transactions", ...]

// Get annotations for UI (instant, already loaded)
annotations := manager.GetAnnotations("taxinomia.demo.CustomerOrders")
for _, col := range annotations.Columns {
    fmt.Printf("%s: %s (entity: %s)\n", col.Name, col.DisplayName, col.EntityType)
}

// Load data on demand (lazy)
table, err := manager.LoadData("customer_orders")  // First call: loads from file
table2, err := manager.LoadData("customer_orders") // Second call: returns cached

// Find joinable columns across sources
joins := manager.FindJoinableColumns("customer_id") // All columns with entity_type "customer_id"
```

## Built-in Loaders

Taxinomia provides these built-in loaders:

| Source Type | Description | Required Config Keys |
|-------------|-------------|---------------------|
| `proto` | Protocol Buffer files (textproto/binary) | `descriptor_set`, `proto_file`, `message_type`, `format` |
| `csv` | CSV files | `file_path`, `has_header` (optional: `delimiter`) |

Users can register additional loaders for databases (PostgreSQL, MySQL, BigQuery), APIs, custom file formats, or any other data source.

## Implementation

The data sources system is implemented in:

| File | Description |
|------|-------------|
| `datasources/datasource.proto` | Proto definition for configuration |
| `datasources/loader.go` | `DataSourceLoader` interface |
| `datasources/manager.go` | `Manager` for loading annotations and data |
| `datasources/proto_loader.go` | Built-in proto loader |
| `datasources/csv_loader.go` | Built-in CSV loader (basic and typed) |
| `demo/data/data_sources.textproto` | Example configuration |

Note: The `datasources` package is outside of `core/` to keep source configuration separate from core table/column functionality.

### Quick Start

```go
import "github.com/google/taxinomia/datasources"

// Create manager and register loaders
manager := datasources.NewManager()
manager.RegisterLoader(datasources.NewProtoLoader())
manager.RegisterLoader(datasources.NewCsvLoader())

// Load config (annotations loaded eagerly)
manager.LoadConfig("data_sources.textproto")

// Load data on demand (lazy)
table, err := manager.LoadData("customer_orders")
```
