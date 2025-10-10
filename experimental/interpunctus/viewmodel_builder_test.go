package main

import (
	"strings"
	"testing"
)

func TestViewModelBuilder_Headers(t *testing.T) {
	// Set up test data
	table := &Table{}
	view := &View{
		order:        []string{"status", "region", "amount"},
		sorting:      map[string]bool{"status": true, "region": false, "amount": true},
		grouping:     []string{"status"},
		groupSortPos: map[string]int{"status": 1},
		groupOn:      map[string][]string{"status": []string{"active", "inactive"}},
		columnViews:  map[string]*ColumnView{},
	}

	builder := NewViewModelBuilder(table, view, nil, nil)
	vm := builder.Build()

	// Test that we have the expected number of header rows
	if len(vm.Headers) != 5 {
		t.Errorf("Expected 5 header rows, got %d", len(vm.Headers))
	}

	// Test header types
	expectedTypes := []HeaderType{
		HeaderTypeMove,
		HeaderTypeSort,
		HeaderTypeGroup,
		HeaderTypeFilter,
		HeaderTypeColumn,
	}

	for i, expectedType := range expectedTypes {
		if vm.Headers[i].Type != expectedType {
			t.Errorf("Header row %d: expected type %s, got %s", i, expectedType, vm.Headers[i].Type)
		}
	}

	// Test move commands row
	moveRow := vm.Headers[0]
	if len(moveRow.Cells) != 3 {
		t.Errorf("Expected 3 cells in move row, got %d", len(moveRow.Cells))
	}
	if len(moveRow.Cells[0].Commands) != 3 { // <<, <, >
		t.Errorf("Expected 3 move commands, got %d", len(moveRow.Cells[0].Commands))
	}

	// Test group commands row - check G vs U
	groupRow := vm.Headers[2]
	statusCell := groupRow.Cells[0] // status column
	if statusCell.Commands[0].Label != "U" {
		t.Errorf("Expected 'U' for grouped column, got '%s'", statusCell.Commands[0].Label)
	}
	regionCell := groupRow.Cells[1] // region column
	if regionCell.Commands[0].Label != "G" {
		t.Errorf("Expected 'G' for ungrouped column, got '%s'", regionCell.Commands[0].Label)
	}

	// Test filter row
	filterRow := vm.Headers[3]
	statusFilterCell := filterRow.Cells[0]
	if statusFilterCell.InputValue != "active||inactive" {
		t.Errorf("Expected filter value 'active||inactive', got '%s'", statusFilterCell.InputValue)
	}

	// Test column names row
	colRow := vm.Headers[4]
	if !strings.Contains(colRow.Cells[0].Content, "status") {
		t.Errorf("Expected column name to contain 'status', got '%s'", colRow.Cells[0].Content)
	}
	if !strings.Contains(colRow.Cells[0].Content, "(1)") {
		t.Errorf("Expected sort position (1) in column name, got '%s'", colRow.Cells[0].Content)
	}
	if !strings.Contains(colRow.Cells[0].Content, "↑") {
		t.Errorf("Expected ↑ for ascending sort, got '%s'", colRow.Cells[0].Content)
	}
}

func TestViewModelBuilder_UngroupedData(t *testing.T) {
	// Create test table with sample data
	colDefs := []*ColumnDef{
		{
			name:        "status",
			keyToValue:  map[uint32]string{0: "active", 1: "inactive"},
			valueToKey:  map[string]uint32{"active": 0, "inactive": 1},
			comparer:    CompareStrings,
		},
		{
			name:        "amount",
			keyToValue:  map[uint32]string{0: "100", 1: "200", 2: "300"},
			valueToKey:  map[string]uint32{"100": 0, "200": 1, "300": 2},
			comparer:    CompareStrings,
		},
	}

	// Create columns with the proper generic type
	statusColumn := NewColumn[uint32](colDefs[0])
	statusColumn.data = []uint32{0, 1, 0, 1} // active, inactive, active, inactive

	amountColumn := NewColumn[uint32](colDefs[1])
	amountColumn.data = []uint32{0, 1, 2, 0} // 100, 200, 300, 100

	columns := map[string]IColumn{
		"status": statusColumn,
		"amount": amountColumn,
	}

	table := &Table{columns: columns}
	view := &View{
		order:       []string{"status", "amount"},
		sorting:     map[string]bool{"status": true, "amount": true},
		grouping:    []string{}, // No grouping
		columnViews: map[string]*ColumnView{},
	}

	indices := []int{0, 1, 2, 3}
	builder := NewViewModelBuilder(table, view, nil, indices)
	vm := builder.Build()

	// Test ungrouped data rows
	if len(vm.Rows) != 4 {
		t.Errorf("Expected 4 data rows, got %d", len(vm.Rows))
	}

	// Check first row values
	if vm.Rows[0].Cells[0].Value != "active" {
		t.Errorf("Expected 'active' in first row status, got '%s'", vm.Rows[0].Cells[0].Value)
	}
	if vm.Rows[0].Cells[1].Value != "100" {
		t.Errorf("Expected '100' in first row amount, got '%s'", vm.Rows[0].Cells[1].Value)
	}
}

func TestViewModelBuilder_GroupedData(t *testing.T) {
	// This test requires more setup for grouped data
	// We'll test that cellBuilder is called properly and rows are converted

	table := &Table{}
	view := &View{
		order:              []string{"status", "amount"},
		sorting:            map[string]bool{"status": true, "amount": true},
		grouping:           []string{"status"},
		columnViews:        map[string]*ColumnView{},
	}

	// Create a simple group structure
	group := &Group{
		columnDef:  &ColumnDef{name: "status"},
		aggregates: map[string]int{"amount": 600},
	}

	builder := NewViewModelBuilder(table, view, group, nil)

	// For now just verify it doesn't panic
	vm := builder.Build()
	if vm == nil {
		t.Error("Expected non-nil view model")
	}
}

func TestViewModelBuilder_URLs(t *testing.T) {
	table := &Table{}
	view := &View{
		order:       []string{"col1"},
		sorting:     map[string]bool{"col1": true},
		grouping:    []string{},
		columnViews: map[string]*ColumnView{},
	}

	builder := NewViewModelBuilder(table, view, nil, nil)
	vm := builder.Build()

	// Test that URLs are properly formed
	moveRow := vm.Headers[0]
	moveLeftMost := moveRow.Cells[0].Commands[0]
	if !strings.HasPrefix(moveLeftMost.URL, "sorted?") {
		t.Errorf("Expected URL to start with 'sorted?', got '%s'", moveLeftMost.URL)
	}

	// Test group command URL
	groupRow := vm.Headers[2]
	groupCmd := groupRow.Cells[0].Commands[0]
	if groupCmd.Label == "G" && !strings.HasPrefix(groupCmd.URL, "grouped?") {
		t.Errorf("Expected group URL to start with 'grouped?', got '%s'", groupCmd.URL)
	}
}