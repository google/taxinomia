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

package models

import (
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

func TestBuildColumnsTable(t *testing.T) {
	// Create a DataModel with test tables
	dm := NewDataModel()

	// Create first test table with 2 columns
	table1 := tables.NewDataTable()
	col1a := columns.NewStringColumn(columns.NewColumnDef("name", "Name", "person"))
	col1a.Append("Alice")
	col1a.Append("Bob")
	col1a.FinalizeColumn()

	col1b := columns.NewUint32Column(columns.NewColumnDef("age", "Age", ""))
	col1b.Append(30)
	col1b.Append(25)
	col1b.FinalizeColumn()

	table1.AddColumn(col1a)
	table1.AddColumn(col1b)
	dm.AddTable("users", table1)

	// Create second test table with 3 columns
	table2 := tables.NewDataTable()
	col2a := columns.NewStringColumn(columns.NewColumnDef("order_id", "Order ID", "order"))
	col2a.Append("ORD-001")
	col2a.Append("ORD-002")
	col2a.Append("ORD-003")
	col2a.FinalizeColumn()

	col2b := columns.NewStringColumn(columns.NewColumnDef("customer", "Customer", "person"))
	col2b.Append("Alice")
	col2b.Append("Bob")
	col2b.Append("Alice")
	col2b.FinalizeColumn()

	col2c := columns.NewUint32Column(columns.NewColumnDef("amount", "Amount", ""))
	col2c.Append(100)
	col2c.Append(200)
	col2c.Append(150)
	col2c.FinalizeColumn()

	table2.AddColumn(col2a)
	table2.AddColumn(col2b)
	table2.AddColumn(col2c)
	dm.AddTable("orders", table2)

	// Build the _columns table
	columnsTable := BuildColumnsTable(dm)

	// Verify the table was created
	if columnsTable == nil {
		t.Fatal("BuildColumnsTable returned nil")
	}

	// Should have 5 rows (2 from users + 3 from orders)
	expectedRows := 5
	if columnsTable.Length() != expectedRows {
		t.Errorf("Expected %d rows, got %d", expectedRows, columnsTable.Length())
	}

	// Verify columns exist
	expectedColumns := []string{"table_name", "column_name", "display_name", "data_type", "entity_type", "is_key", "row_count", "position"}
	for _, colName := range expectedColumns {
		col := columnsTable.GetColumn(colName)
		if col == nil {
			t.Errorf("Expected column %q not found", colName)
		}
	}

	// Verify some values
	tableNameCol := columnsTable.GetColumn("table_name")
	dataTypeCol := columnsTable.GetColumn("data_type")
	entityTypeCol := columnsTable.GetColumn("entity_type")

	// Check that we have entries from both tables
	foundUsers := false
	foundOrders := false
	for i := 0; i < columnsTable.Length(); i++ {
		tableName, _ := tableNameCol.GetString(uint32(i))
		if tableName == "users" {
			foundUsers = true
		}
		if tableName == "orders" {
			foundOrders = true
		}
	}

	if !foundUsers {
		t.Error("No entries found for 'users' table")
	}
	if !foundOrders {
		t.Error("No entries found for 'orders' table")
	}

	// Verify data types are correct
	for i := 0; i < columnsTable.Length(); i++ {
		dataType, _ := dataTypeCol.GetString(uint32(i))
		if dataType != "string" && dataType != "uint32" {
			t.Errorf("Unexpected data type: %q", dataType)
		}
	}

	// Verify entity types include expected values
	foundPersonEntity := false
	foundOrderEntity := false
	for i := 0; i < columnsTable.Length(); i++ {
		entityType, _ := entityTypeCol.GetString(uint32(i))
		if entityType == "person" {
			foundPersonEntity = true
		}
		if entityType == "order" {
			foundOrderEntity = true
		}
	}

	if !foundPersonEntity {
		t.Error("No column with entity_type='person' found")
	}
	if !foundOrderEntity {
		t.Error("No column with entity_type='order' found")
	}
}

func TestAddSystemTables(t *testing.T) {
	dm := NewDataModel()

	// Create a simple test table
	table := tables.NewDataTable()
	col := columns.NewStringColumn(columns.NewColumnDef("id", "ID", ""))
	col.Append("1")
	col.FinalizeColumn()
	table.AddColumn(col)
	dm.AddTable("test", table)

	// Add system tables
	AddSystemTables(dm)

	// Verify _columns table was added
	columnsTable := dm.GetTable(ColumnsTableName)
	if columnsTable == nil {
		t.Fatal("_columns table was not added to DataModel")
	}

	// Verify it has the expected structure
	if columnsTable.GetColumn("table_name") == nil {
		t.Error("_columns table missing 'table_name' column")
	}
}

func TestColumnsTableExcludesItself(t *testing.T) {
	dm := NewDataModel()

	// Create a simple test table
	table := tables.NewDataTable()
	col := columns.NewStringColumn(columns.NewColumnDef("id", "ID", ""))
	col.Append("1")
	col.FinalizeColumn()
	table.AddColumn(col)
	dm.AddTable("test", table)

	// Add _columns table first (simulating what happens if called twice)
	AddSystemTables(dm)

	// Build a new columns table - it should not include itself
	columnsTable := BuildColumnsTable(dm)

	tableNameCol := columnsTable.GetColumn("table_name")
	for i := 0; i < columnsTable.Length(); i++ {
		tableName, _ := tableNameCol.GetString(uint32(i))
		if tableName == "_columns" {
			t.Error("_columns table should not include itself in the metadata")
		}
	}
}
