/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package datasources

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestManagerLoadConfig(t *testing.T) {
	// Get the path to the demo data directory
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get current file path")
	}
	demoDataDir := filepath.Join(filepath.Dir(currentFile), "..", "demo", "data")

	// Create manager and register proto loader
	manager := NewManager()
	protoLoader := NewProtoLoader()
	manager.RegisterLoader(protoLoader)

	// Load config
	configPath := filepath.Join(demoDataDir, "data_sources.textproto")
	if err := manager.LoadConfig(configPath); err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Verify annotations were loaded (eager)
	annotationIDs := manager.GetAnnotationIDs()
	if len(annotationIDs) != 1 {
		t.Errorf("expected 1 annotations set, got %d", len(annotationIDs))
	}

	annotations := manager.GetAnnotations("taxinomia.demo.CustomerOrders")
	if annotations == nil {
		t.Fatal("annotations not found")
	}
	t.Logf("Loaded annotations with %d columns", len(annotations.GetColumns()))

	// Verify sources were registered (but not loaded)
	sourceNames := manager.GetSourceNames()
	if len(sourceNames) != 2 {
		t.Errorf("expected 2 sources, got %d", len(sourceNames))
	}

	// Verify data is not loaded yet
	if manager.IsLoaded("customer_orders") {
		t.Error("customer_orders should not be loaded yet")
	}

	// Load data (lazy)
	table, err := manager.LoadData("customer_orders")
	if err != nil {
		t.Fatalf("failed to load data: %v", err)
	}

	t.Logf("Loaded table with %d rows", table.Length())

	// Verify it's now cached
	if !manager.IsLoaded("customer_orders") {
		t.Error("customer_orders should be loaded now")
	}

	// Verify entity types were applied (with domain prefix)
	customerIDCol := table.GetColumn("customer_id")
	if customerIDCol == nil {
		t.Fatal("customer_id column not found")
	}
	entityType := customerIDCol.ColumnDef().EntityType()
	if entityType != "demo.customer_id" {
		t.Errorf("expected entity type 'demo.customer_id', got %q", entityType)
	}

	// Verify display names were applied
	displayName := customerIDCol.ColumnDef().DisplayName()
	if displayName != "Customer ID" {
		t.Errorf("expected display name 'Customer ID', got %q", displayName)
	}

	// Load second table to verify caching works
	table2, err := manager.LoadData("customer_orders")
	if err != nil {
		t.Fatalf("failed to load cached data: %v", err)
	}
	if table != table2 {
		t.Error("expected same table instance from cache")
	}

	// Test invalidate cache
	manager.InvalidateCache("customer_orders")
	if manager.IsLoaded("customer_orders") {
		t.Error("customer_orders should not be loaded after invalidation")
	}
}

func TestManagerFindJoinableColumns(t *testing.T) {
	manager := NewManager()

	// Add annotations
	manager.AddAnnotations(&ColumnAnnotations{
		AnnotationsId: "annotations1",
		Columns: []*ColumnAnnotation{
			{Name: "id", EntityType: "user_id"},
			{Name: "name", EntityType: ""},
		},
	})
	manager.AddAnnotations(&ColumnAnnotations{
		AnnotationsId: "annotations2",
		Columns: []*ColumnAnnotation{
			{Name: "user_id", EntityType: "user_id"},
			{Name: "amount", EntityType: ""},
		},
	})

	// Add sources
	manager.AddSource(&DataSource{
		Name:          "users",
		AnnotationsId: "annotations1",
	})
	manager.AddSource(&DataSource{
		Name:          "transactions",
		AnnotationsId: "annotations2",
	})

	// Find joinable columns
	joinable := manager.FindJoinableColumns("user_id")
	if len(joinable) != 2 {
		t.Errorf("expected 2 joinable columns, got %d", len(joinable))
	}

	// Verify entity types
	entityTypes := manager.GetAllEntityTypes()
	if len(entityTypes) != 1 {
		t.Errorf("expected 1 entity type, got %d", len(entityTypes))
	}
}

func TestCsvLoader(t *testing.T) {
	// Create a temporary CSV file
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")

	csvContent := `name,age,active
Alice,30,true
Bob,25,false
Charlie,35,true`

	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	// Create manager and register CSV loader
	manager := NewManager()
	manager.RegisterLoader(NewCsvLoader())

	// Add annotations
	manager.AddAnnotations(&ColumnAnnotations{
		AnnotationsId: "test_annotations",
		Columns: []*ColumnAnnotation{
			{Name: "name", DisplayName: "Full Name", EntityType: "person_name"},
			{Name: "age", DisplayName: "Age"},
			{Name: "active", DisplayName: "Is Active"},
		},
	})

	// Add source
	manager.AddSource(&DataSource{
		Name:          "test_csv",
		AnnotationsId: "test_annotations",
		SourceType:    "csv",
		Config: map[string]string{
			"file_path":  csvPath,
			"has_header": "true",
		},
	})

	// Load data
	table, err := manager.LoadData("test_csv")
	if err != nil {
		t.Fatalf("failed to load CSV: %v", err)
	}

	if table.Length() != 3 {
		t.Errorf("expected 3 rows, got %d", table.Length())
	}

	// Verify column metadata
	nameCol := table.GetColumn("name")
	if nameCol == nil {
		t.Fatal("name column not found")
	}
	if nameCol.ColumnDef().DisplayName() != "Full Name" {
		t.Errorf("expected display name 'Full Name', got %q", nameCol.ColumnDef().DisplayName())
	}
	if nameCol.ColumnDef().EntityType() != "person_name" {
		t.Errorf("expected entity type 'person_name', got %q", nameCol.ColumnDef().EntityType())
	}
}

func TestCsvLoaderTyped(t *testing.T) {
	// Create a temporary CSV file
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test_typed.csv")

	csvContent := `name,age,score,active
Alice,30,95.5,true
Bob,25,88.0,false
Charlie,35,92.3,true`

	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}

	// Create manager and register typed CSV loader
	manager := NewManager()
	manager.RegisterLoader(NewCsvLoaderTyped())

	// Add source (no schema - test type inference)
	manager.AddSource(&DataSource{
		Name:       "test_csv_typed",
		SourceType: "csv_typed",
		Config: map[string]string{
			"file_path":  csvPath,
			"has_header": "true",
		},
	})

	// Load data
	table, err := manager.LoadData("test_csv_typed")
	if err != nil {
		t.Fatalf("failed to load CSV: %v", err)
	}

	if table.Length() != 3 {
		t.Errorf("expected 3 rows, got %d", table.Length())
	}

	// Verify type inference - age should be int
	ageCol := table.GetColumn("age")
	if ageCol == nil {
		t.Fatal("age column not found")
	}

	// Get first value as string to verify it loaded correctly
	val, err := ageCol.GetString(0)
	if err != nil {
		t.Fatalf("failed to get age value: %v", err)
	}
	if val != "30" {
		t.Errorf("expected age '30', got %q", val)
	}
}
