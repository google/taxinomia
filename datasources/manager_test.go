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

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
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
	manager.SetFileReader(os.ReadFile)

	// Load config (file I/O done outside the library)
	configPath := filepath.Join(demoDataDir, "data_sources.textproto")
	configData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read config: %v", err)
	}
	if err := manager.LoadConfigFromBytes(configData, demoDataDir); err != nil {
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
	manager.SetFileReader(os.ReadFile)

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

	// Phase 3c: loaders build chunked tables.
	if _, ok := nameCol.(columns.IChunkedColumn); !ok {
		t.Errorf("expected chunked column storage, got %T", nameCol)
	}
}

func TestEntityTypes(t *testing.T) {
	// Get the path to the demo data directory
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get current file path")
	}
	demoDataDir := filepath.Join(filepath.Dir(currentFile), "..", "demo", "data")

	// Create manager and register proto loader
	manager := NewManager()
	manager.RegisterLoader(NewProtoLoader())
	manager.SetFileReader(os.ReadFile)

	// Load config (file I/O done outside the library)
	configPath := filepath.Join(demoDataDir, "data_sources.textproto")
	configData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read config: %v", err)
	}
	if err := manager.LoadConfigFromBytes(configData, demoDataDir); err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Verify entity types were loaded
	entityTypeNames := manager.GetEntityTypeNames()
	if len(entityTypeNames) < 3 {
		t.Errorf("expected at least 3 entity types, got %d", len(entityTypeNames))
	}

	// Verify specific entity type
	customerET := manager.GetEntityType("demo.customer_id")
	if customerET == nil {
		t.Fatal("demo.customer_id entity type not found")
	}

	if customerET.GetDescription() == "" {
		t.Error("expected non-empty description")
	}

	urls := customerET.GetUrls()
	if len(urls) < 1 {
		t.Errorf("expected at least 1 URL template, got %d", len(urls))
	}

	// Test URL resolution
	resolvedURL := manager.ResolveURL("demo.customer_id", "CUST001", "")
	if resolvedURL == "" {
		t.Error("expected non-empty resolved URL")
	}
	if resolvedURL != "table?table=customer_orders&filter=customer_id:CUST001" {
		t.Errorf("unexpected resolved URL: %s", resolvedURL)
	}

	// Test URL resolution by name
	resolvedURL2 := manager.ResolveURL("demo.customer_id", "CUST001", "Orders by Status")
	if resolvedURL2 == "" {
		t.Error("expected non-empty resolved URL for 'Orders by Status'")
	}

	// Test non-existent entity type
	noURL := manager.ResolveURL("nonexistent.type", "value", "")
	if noURL != "" {
		t.Error("expected empty URL for non-existent entity type")
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
	manager.SetFileReader(os.ReadFile)

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

	// No primary key entity type and no sort key declared: the table stays
	// in load order and records no sort key.
	if table.SortKey() != nil {
		t.Errorf("expected no recorded sort key, got %v", table.SortKey())
	}
}

// newSortTestManager registers a csv_typed source over an unsorted file,
// with the name column carrying the primary key entity type.
func newSortTestManager(t *testing.T, source *DataSource) *Manager {
	t.Helper()
	csvPath := filepath.Join(t.TempDir(), "unsorted.csv")
	csvContent := `name,age,score
Delta,30,95.5
Alpha,25,88.0
Charlie,30,92.3
Bravo,20,90.1`
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to write test CSV: %v", err)
	}
	manager := NewManager()
	manager.RegisterLoader(NewCsvLoaderTyped())
	manager.SetFileReader(os.ReadFile)
	manager.AddAnnotations(&ColumnAnnotations{
		AnnotationsId: "sort_annotations",
		Columns: []*ColumnAnnotation{
			{Name: "name", EntityType: "test.person"},
		},
	})
	source.AnnotationsId = "sort_annotations"
	source.SourceType = "csv_typed"
	source.Config = map[string]string{"file_path": csvPath, "has_header": "true"}
	manager.AddSource(source)
	return manager
}

func columnValues(t *testing.T, table *tables.DataTable, name string) []string {
	t.Helper()
	col := table.GetColumn(name)
	if col == nil {
		t.Fatalf("column %q not found", name)
	}
	vals := make([]string, table.Length())
	for i := range vals {
		v, err := col.GetString(uint32(i))
		if err != nil {
			t.Fatalf("GetString(%s, %d): %v", name, i, err)
		}
		vals[i] = v
	}
	return vals
}

// With no sort_key declared, the table is sorted by the primary key column.
func TestLoadDataDefaultSortByPrimaryKey(t *testing.T) {
	manager := newSortTestManager(t, &DataSource{
		Name:                 "people",
		PrimaryKeyEntityType: "test.person",
	})
	table, err := manager.LoadData("people")
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}
	got := columnValues(t, table, "name")
	want := []string{"Alpha", "Bravo", "Charlie", "Delta"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: name %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	if key := table.SortKey(); len(key) != 1 || key[0] != "name" {
		t.Errorf("SortKey() = %v, want [name]", key)
	}
	// Rows stay aligned across columns.
	if ages := columnValues(t, table, "age"); ages[0] != "25" || ages[3] != "30" {
		t.Errorf("age column not aligned after sort: %v", ages)
	}
}

// A declared sort_key is honored, with the primary key column appended as
// the tie-breaker.
func TestLoadDataDeclaredSortKey(t *testing.T) {
	manager := newSortTestManager(t, &DataSource{
		Name:                 "people",
		PrimaryKeyEntityType: "test.person",
		SortKey:              []string{"age"},
	})
	table, err := manager.LoadData("people")
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}
	// age ascending; the age=30 tie broken by name (Charlie before Delta).
	gotNames := columnValues(t, table, "name")
	want := []string{"Bravo", "Alpha", "Charlie", "Delta"}
	for i := range want {
		if gotNames[i] != want[i] {
			t.Fatalf("row %d: name %q, want %q (all: %v)", i, gotNames[i], want[i], gotNames)
		}
	}
	if key := table.SortKey(); len(key) != 2 || key[0] != "age" || key[1] != "name" {
		t.Errorf("SortKey() = %v, want [age name]", key)
	}
}

// A declared sort_key naming a column the table does not have is a
// configuration error, not a silent skip.
func TestLoadDataBadSortKeyFails(t *testing.T) {
	manager := newSortTestManager(t, &DataSource{
		Name:    "people",
		SortKey: []string{"no_such_column"},
	})
	if _, err := manager.LoadData("people"); err == nil {
		t.Fatal("expected error for sort key naming an unknown column")
	}
}
