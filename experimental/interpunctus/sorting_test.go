package main

import (
	"testing"
)

func TestGroupSorter_SortGroups(t *testing.T) {
	// Create test column views with groupKeyToOrder mappings
	columnViews := map[string]*ColumnView{
		"status": {
			groupKeyToOrder: map[uint32]uint32{
				1: 0, // First position
				2: 2, // Third position
				3: 1, // Second position
			},
		},
	}

	sorter := NewGroupSorter(columnViews)

	// Create test groups
	groups := []*sortGroup{
		{
			columnDef:  &ColumnDef{name: "status"},
			columnView: columnViews["status"],
			key:        2, // Should be third
		},
		{
			columnDef:  &ColumnDef{name: "status"},
			columnView: columnViews["status"],
			key:        1, // Should be first
		},
		{
			columnDef:  &ColumnDef{name: "status"},
			columnView: columnViews["status"],
			key:        3, // Should be second
		},
	}

	// Sort the groups
	sorter.SortGroups(groups)

	// Verify order
	expectedOrder := []uint32{1, 3, 2}
	for i, expected := range expectedOrder {
		if groups[i].key != expected {
			t.Errorf("Position %d: expected key %d, got %d", i, expected, groups[i].key)
		}
	}
}

func TestGroupSorter_SortIndices(t *testing.T) {
	// Create test column views
	columnViews := map[string]*ColumnView{
		"amount": {
			keyToGroupKey: map[uint32]uint32{
				10: 1, // value 10 -> group 1
				20: 2, // value 20 -> group 2
				30: 3, // value 30 -> group 3
			},
			groupKeyToOrder: map[uint32]uint32{
				1: 2, // group 1 -> position 2
				2: 0, // group 2 -> position 0
				3: 1, // group 3 -> position 1
			},
		},
	}

	sorter := NewGroupSorter(columnViews)

	// Create test indices
	indicesList := []*indices{
		{
			columnDef:  &ColumnDef{name: "amount"},
			columnView: columnViews["amount"],
			value:      10, // group 1, position 2
			indices:    []uint32{0, 1},
		},
		{
			columnDef:  &ColumnDef{name: "amount"},
			columnView: columnViews["amount"],
			value:      30, // group 3, position 1
			indices:    []uint32{4, 5},
		},
		{
			columnDef:  &ColumnDef{name: "amount"},
			columnView: columnViews["amount"],
			value:      20, // group 2, position 0
			indices:    []uint32{2, 3},
		},
	}

	// Sort the indices
	sorter.SortIndices(indicesList)

	// Verify order (should be sorted by position: 20, 30, 10)
	expectedOrder := []uint32{20, 30, 10}
	for i, expected := range expectedOrder {
		if indicesList[i].value != expected {
			t.Errorf("Position %d: expected value %d, got %d", i, expected, indicesList[i].value)
		}
	}
}

func TestGroupSorter_PrepareGroupsForSorting(t *testing.T) {
	columnViews := map[string]*ColumnView{
		"region": {
			groupKeyToOrder: map[uint32]uint32{},
		},
	}

	sorter := NewGroupSorter(columnViews)

	// Create a group with multiple subgroups
	group := &Group{
		columnDef: &ColumnDef{name: "region"},
		groups: map[uint32]*Group{
			1: &Group{value: 1},
			2: &Group{value: 2},
			3: &Group{value: 3},
		},
		asc: true,
	}

	// Prepare groups for sorting
	sortGroups := sorter.PrepareGroupsForSorting(group)

	// Verify we got all groups
	if len(sortGroups) != 3 {
		t.Errorf("Expected 3 sort groups, got %d", len(sortGroups))
	}

	// Verify each group has correct properties
	keys := make(map[uint32]bool)
	for _, sg := range sortGroups {
		keys[sg.key] = true
		if sg.columnDef.name != "region" {
			t.Errorf("Expected column name 'region', got '%s'", sg.columnDef.name)
		}
		if sg.asc != true {
			t.Error("Expected asc to be true")
		}
	}

	// Verify all keys are present
	for k := uint32(1); k <= 3; k++ {
		if !keys[k] {
			t.Errorf("Missing key %d in prepared groups", k)
		}
	}
}

func TestGroupSorter_CompareConsistency(t *testing.T) {
	// Test that compare functions are consistent (transitive)
	columnViews := map[string]*ColumnView{
		"test": {
			groupKeyToOrder: map[uint32]uint32{
				1: 0,
				2: 1,
				3: 2,
			},
		},
	}

	sorter := NewGroupSorter(columnViews)

	groups := []*sortGroup{
		{columnView: columnViews["test"], key: 1},
		{columnView: columnViews["test"], key: 2},
		{columnView: columnViews["test"], key: 3},
	}

	// Test transitivity: if a < b and b < c, then a < c
	if sorter.compareGroups(groups[0], groups[1]) >= 0 {
		t.Error("Expected group 1 < group 2")
	}
	if sorter.compareGroups(groups[1], groups[2]) >= 0 {
		t.Error("Expected group 2 < group 3")
	}
	if sorter.compareGroups(groups[0], groups[2]) >= 0 {
		t.Error("Expected group 1 < group 3 (transitivity)")
	}

	// Test reflexivity: a == a
	if sorter.compareGroups(groups[0], groups[0]) != 0 {
		t.Error("Expected group to equal itself")
	}
}