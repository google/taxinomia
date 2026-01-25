/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package demo

import (
	"path/filepath"
	"runtime"
	"testing"
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
	descriptorPath := filepath.Join(demoDir, "proto", "customer_orders.pb")
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
