/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package demo

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestProtoTableLoaderIntegration(t *testing.T) {
	// Get the directory of this test file
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get current file path")
	}
	demoDir := filepath.Dir(currentFile)

	loader := NewProtoTableLoader()

	// Load the descriptor set
	descriptorPath := filepath.Join(demoDir, "customer_orders.pb")
	if err := loader.LoadDescriptorSet(descriptorPath); err != nil {
		t.Fatalf("failed to load descriptor set: %v", err)
	}

	// Check registered messages
	messages := loader.GetRegisteredMessages()
	t.Logf("Registered messages: %v", messages)

	if len(messages) == 0 {
		t.Fatal("expected at least one registered message")
	}

	// Load the textproto file
	textprotoPath := filepath.Join(demoDir, "data", "customer_orders.textproto")
	table, err := loader.LoadTextprotoAsTable(textprotoPath, "taxinomia.demo.CustomerOrders")
	if err != nil {
		t.Fatalf("failed to load textproto: %v", err)
	}

	// Verify the table has data
	if table.Length() == 0 {
		t.Fatal("expected table to have rows")
	}

	t.Logf("Table has %d rows", table.Length())
	t.Logf("Columns: %v", table.GetColumnNames())

	// Verify we have the expected columns from the hierarchy
	expectedColumns := []string{
		"customer_id", "customer_name", "customer_email", // CustomerOrders level
		"order_id", "order_date", "status", // Order level
		"product_id", "product_name", "quantity", "unit_price", // LineItem level
		"discount_code", "discount_percent", "reason", // Discount level
	}

	colNames := table.GetColumnNames()
	for _, expected := range expectedColumns {
		found := false
		for _, col := range colNames {
			if col == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected column %q not found in table", expected)
		}
	}

	// Print sample data
	t.Log("\nSample data from table:")
	for i := 0; i < min(5, table.Length()); i++ {
		t.Logf("Row %d:", i)
		for _, colName := range colNames {
			col := table.GetColumn(colName)
			if col != nil {
				val, _ := col.GetString(uint32(i))
				t.Logf("  %s: %s", colName, val)
			}
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestBinaryProtoLoading tests loading binary protobuf files
func TestBinaryProtoLoading(t *testing.T) {
	// Get the directory of this test file
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get current file path")
	}
	demoDir := filepath.Dir(currentFile)

	loader := NewProtoTableLoader()

	// Load the descriptor set
	descriptorPath := filepath.Join(demoDir, "customer_orders.pb")
	if err := loader.LoadDescriptorSet(descriptorPath); err != nil {
		t.Fatalf("failed to load descriptor set: %v", err)
	}

	// First, load textproto to get the parsed message
	textprotoPath := filepath.Join(demoDir, "data", "customer_orders.textproto")
	textprotoData, err := os.ReadFile(textprotoPath)
	if err != nil {
		t.Fatalf("failed to read textproto file: %v", err)
	}

	// Parse the textproto to get a message we can serialize to binary
	msg, err := loader.loader.ParseTextproto(textprotoData, "taxinomia.demo.CustomerOrders")
	if err != nil {
		t.Fatalf("failed to parse textproto: %v", err)
	}

	// Serialize to binary protobuf
	binaryData, err := proto.Marshal(msg.Interface())
	if err != nil {
		t.Fatalf("failed to marshal to binary: %v", err)
	}

	t.Logf("Binary protobuf size: %d bytes (textproto was %d bytes)", len(binaryData), len(textprotoData))

	// Write the binary file to the data directory for demonstration
	binaryPath := filepath.Join(demoDir, "data", "customer_orders.binpb")
	if err := os.WriteFile(binaryPath, binaryData, 0644); err != nil {
		t.Fatalf("failed to write binary file: %v", err)
	}
	t.Logf("Wrote binary protobuf to: %s", binaryPath)

	// Now load the binary file and verify it produces the same table
	binaryTable, err := loader.LoadBinaryProtoAsTable(binaryPath, "taxinomia.demo.CustomerOrders")
	if err != nil {
		t.Fatalf("failed to load binary protobuf: %v", err)
	}

	// Also load the textproto for comparison
	textprotoTable, err := loader.LoadTextprotoAsTable(textprotoPath, "taxinomia.demo.CustomerOrders")
	if err != nil {
		t.Fatalf("failed to load textproto: %v", err)
	}

	// Verify both tables have the same number of rows
	if binaryTable.Length() != textprotoTable.Length() {
		t.Errorf("row count mismatch: binary=%d, textproto=%d", binaryTable.Length(), textprotoTable.Length())
	}

	t.Logf("Binary table has %d rows (same as textproto)", binaryTable.Length())
	t.Logf("Columns: %v", binaryTable.GetColumnNames())

	// Verify both tables have the same columns
	binaryCols := binaryTable.GetColumnNames()
	textprotoCols := textprotoTable.GetColumnNames()

	if len(binaryCols) != len(textprotoCols) {
		t.Errorf("column count mismatch: binary=%d, textproto=%d", len(binaryCols), len(textprotoCols))
	}

	// Verify data matches for a sample of rows
	t.Log("\nComparing data between binary and textproto:")
	for i := 0; i < min(3, binaryTable.Length()); i++ {
		t.Logf("Row %d:", i)
		for _, colName := range binaryCols {
			binaryCol := binaryTable.GetColumn(colName)
			textprotoCol := textprotoTable.GetColumn(colName)
			if binaryCol != nil && textprotoCol != nil {
				binaryVal, _ := binaryCol.GetString(uint32(i))
				textprotoVal, _ := textprotoCol.GetString(uint32(i))
				if binaryVal != textprotoVal {
					t.Errorf("  %s: MISMATCH binary=%q textproto=%q", colName, binaryVal, textprotoVal)
				} else {
					t.Logf("  %s: %s ✓", colName, binaryVal)
				}
			}
		}
	}
}
