# Protobuf Support

Taxinomia supports loading data from Protocol Buffer textproto files and converting them into DataTables for querying and analysis.

## Overview

The protoloader package provides functionality to:
1. Load proto descriptor sets (compiled `.pb` files)
2. Parse textproto content into dynamic protobuf messages
3. Convert hierarchical protobuf data into denormalized DataTables

## Architecture

The protobuf loading is split into two layers:

### Core Layer (`core/protoloader`)

The core loader operates on raw bytes and requires a pre-populated proto registry:

```go
// Create a registry and loader
registry := new(protoregistry.Files)
loader := protoloader.NewLoader(registry)

// Parse textproto from bytes
msg, err := loader.ParseTextproto(data, "mypackage.MyMessage")

// Load textproto as a DataTable
table, err := loader.LoadTextprotoAsTable(data, "mypackage.MyMessage")
```

### Demo Layer (`demo/protoloader`)

The demo layer provides convenience methods for file-based loading:

```go
// Create a loader that manages its own registry
loader := demo.NewProtoTableLoader()

// Load descriptor set from file or bytes
loader.LoadDescriptorSet("schema.pb")
loader.LoadDescriptorSetFromBytes(descriptorBytes)

// Load textproto from file or bytes
table, err := loader.LoadTextprotoAsTable("data.textproto", "mypackage.MyMessage")
table, err := loader.LoadTextprotoAsTableFromBytes(data, "mypackage.MyMessage")
```

## Supported Protobuf Types

### Scalar Types

All protobuf scalar types are supported and mapped to appropriate Taxinomia column types:

| Protobuf Type | Column Type | Notes |
|---------------|-------------|-------|
| `bool` | `BoolColumn` | Native boolean support |
| `int32`, `sint32`, `sfixed32` | `Int64Column` | Stored as int64 for consistency |
| `int64`, `sint64`, `sfixed64` | `Int64Column` | Native int64 support |
| `uint32`, `fixed32` | `Uint32Column` | Native uint32 support |
| `uint64`, `fixed64` | `Uint64Column` | Native uint64 support |
| `float`, `double` | `Float64Column` | Both map to float64 |
| `string` | `StringColumn` | Direct string value |
| `bytes` | `StringColumn` | Converted to string |
| `enum` | `StringColumn` | Enum name (or number if unavailable) |

### Well-Known Types

| Type | Column Type | Handling |
|------|-------------|----------|
| `google.protobuf.Timestamp` | `DatetimeColumn` | Native datetime support with full precision |

### Message Hierarchies

The loader automatically denormalizes hierarchical protobuf structures into flat tables. It supports **linear hierarchies** where each level has at most one repeated message field leading to the next level.

Example proto:
```protobuf
message Customer {
  string customer_id = 1;
  string name = 2;
  repeated Order orders = 3;
}

message Order {
  string order_id = 1;
  string status = 2;
  repeated LineItem items = 3;
}

message LineItem {
  string product_id = 1;
  int32 quantity = 2;
}
```

This hierarchy `Customer -> Order -> LineItem` is denormalized into rows where each row contains fields from all three levels:

| customer_id | name | order_id | status | product_id | quantity |
|-------------|------|----------|--------|------------|----------|
| C001 | Alice | O001 | shipped | P001 | 2 |
| C001 | Alice | O001 | shipped | P002 | 1 |
| C001 | Alice | O002 | pending | P003 | 5 |

## Limitations

### Not Supported

- **Proto extensions**: Extensions are not resolved
- **Maps**: Map fields are not supported
- **Oneof fields**: Oneof semantics are not preserved (fields are treated independently)
- **Any types**: `google.protobuf.Any` is not unpacked
- **Non-linear hierarchies**: Multiple repeated message fields at the same level are not supported
- **Nested non-repeated messages**: Non-repeated message fields (other than Timestamp) are skipped

## Usage Example

### 1. Define your proto schema

This example demonstrates various protobuf types that map to typed columns:

```protobuf
// customer_orders.proto
syntax = "proto3";
package myapp;

import "google/protobuf/timestamp.proto";

message CustomerOrders {
  string customer_id = 1;           // -> StringColumn
  string customer_name = 2;         // -> StringColumn
  bool is_premium = 3;              // -> BoolColumn
  uint64 loyalty_points = 4;        // -> Uint64Column
  repeated Order orders = 5;
}

message Order {
  string order_id = 1;                        // -> StringColumn
  google.protobuf.Timestamp order_date = 2;   // -> DatetimeColumn
  string status = 3;                          // -> StringColumn
  int64 total_cents = 4;                      // -> Int64Column
  repeated LineItem items = 5;
}

message LineItem {
  string product_id = 1;      // -> StringColumn
  string product_name = 2;    // -> StringColumn
  int32 quantity = 3;         // -> Int64Column (int32 stored as int64)
  double unit_price = 4;      // -> Float64Column
  bool in_stock = 5;          // -> BoolColumn
  uint32 sku_number = 6;      // -> Uint32Column
}
```

### 2. Compile to descriptor set

```bash
protoc --descriptor_set_out=customer_orders.pb \
       --include_imports \
       customer_orders.proto
```

### 3. Create textproto data

```textproto
# customer_orders.textproto
customer_id: "CUST001"
customer_name: "Alice Johnson"
is_premium: true
loyalty_points: 15000
orders {
  order_id: "ORD-001"
  order_date { seconds: 1705276800 }
  status: "completed"
  total_cents: 105997
  items {
    product_id: "PROD-A1"
    product_name: "Laptop"
    quantity: 1
    unit_price: 999.99
    in_stock: true
    sku_number: 12345
  }
  items {
    product_id: "PROD-A2"
    product_name: "Mouse"
    quantity: 2
    unit_price: 29.99
    in_stock: true
    sku_number: 67890
  }
}
```

### 4. Load in Go

```go
loader := demo.NewProtoTableLoader()

// Load the schema
if err := loader.LoadDescriptorSet("customer_orders.pb"); err != nil {
    log.Fatal(err)
}

// Load the data
table, err := loader.LoadTextprotoAsTable(
    "customer_orders.textproto",
    "myapp.CustomerOrders",
)
if err != nil {
    log.Fatal(err)
}

// Use the table
fmt.Printf("Loaded %d rows\n", table.Length())
```

### 5. Resulting Table Structure

The loaded table will have the following typed columns:

| Column Name | Column Type | Proto Type |
|-------------|-------------|------------|
| `customer_id` | `StringColumn` | `string` |
| `customer_name` | `StringColumn` | `string` |
| `is_premium` | `BoolColumn` | `bool` |
| `loyalty_points` | `Uint64Column` | `uint64` |
| `order_id` | `StringColumn` | `string` |
| `order_date` | `DatetimeColumn` | `google.protobuf.Timestamp` |
| `status` | `StringColumn` | `string` |
| `total_cents` | `Int64Column` | `int64` |
| `product_id` | `StringColumn` | `string` |
| `product_name` | `StringColumn` | `string` |
| `quantity` | `Int64Column` | `int32` |
| `unit_price` | `Float64Column` | `double` |
| `in_stock` | `BoolColumn` | `bool` |
| `sku_number` | `Uint32Column` | `uint32` |

The data is denormalized, so each line item becomes a row with all parent fields copied:

| customer_id | is_premium | order_date | total_cents | product_name | quantity | unit_price | in_stock | sku_number |
|-------------|------------|------------|-------------|--------------|----------|------------|----------|------------|
| CUST001 | true | 2024-01-15T00:00:00Z | 105997 | Laptop | 1 | 999.99 | true | 12345 |
| CUST001 | true | 2024-01-15T00:00:00Z | 105997 | Mouse | 2 | 29.99 | true | 67890 |

## Descriptor Set Requirements

The descriptor set (`.pb` file) must include all imported dependencies. Use `--include_imports` when compiling:

```bash
protoc --descriptor_set_out=output.pb \
       --include_imports \
       --proto_path=. \
       your_schema.proto
```

For well-known types like `google.protobuf.Timestamp`, ensure the proto path includes the protobuf include directory:

```bash
protoc --descriptor_set_out=output.pb \
       --include_imports \
       --proto_path=. \
       --proto_path=/usr/include \
       your_schema.proto
```
