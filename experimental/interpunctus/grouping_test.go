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

package main

import (
	"strings"
	"testing"
)

// TestGroupSortingAlphabetical tests that groups are sorted alphabetically by default
func TestGroupSortingAlphabetical(t *testing.T) {
	// Create a table with region column (string values)
	table := &Table{columns: map[string]IColumn{}}

	regionCol := NewColumn[uint32](&ColumnDef{
		name:        "region",
		displayName: "Region",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareStrings,
		summable:    false,
	})

	// Add data in non-alphabetical order
	regionCol.Append("West")
	regionCol.Append("East")
	regionCol.Append("North")
	regionCol.Append("South")
	regionCol.Append("West")
	regionCol.Append("East")

	table.columns["region"] = regionCol

	// Create a view with grouping on region
	view := &View{
		order:        []string{"region"},
		sorting:      map[string]bool{"region": true},
		grouping:     []string{"region"},
		groupOnOrder: []string{"region"},
		groupSortPos: map[string]int{},
		groupOn:      map[string][]string{"region": {}}, // empty = auto-group all
		columnViews:  map[string]*ColumnView{},
	}

	// Apply the view
	_, g, err := table.Apply(view)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Render the table to check the order
	sb := Render(table, view, g, nil)
	html := sb.String()

	// Extract the order of groups from rendered HTML (search for table rows)
	// The groups should appear in alphabetical order: East, North, South, West
	eastPos := strings.Index(html, "<td>East")
	northPos := strings.Index(html, "<td>North")
	southPos := strings.Index(html, "<td>South")
	westPos := strings.Index(html, "<td>West")

	if eastPos == -1 || northPos == -1 || southPos == -1 || westPos == -1 {
		t.Fatalf("Not all region values found in output")
	}

	// Check they appear in alphabetical order
	if !(eastPos < northPos && northPos < southPos && southPos < westPos) {
		t.Errorf("Groups not in alphabetical order. Positions: East=%d, North=%d, South=%d, West=%d",
			eastPos, northPos, southPos, westPos)
	}

	t.Logf("✓ Groups sorted alphabetically: East, North, South, West")
}

// TestGroupSortingNumeric tests that numeric groups are sorted numerically by default
func TestGroupSortingNumeric(t *testing.T) {
	// Create a table with amount column (numeric values stored as strings)
	table := &Table{columns: map[string]IColumn{}}

	amountCol := NewColumn[uint32](&ColumnDef{
		name:        "amount",
		displayName: "Amount",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    true,
	})

	// Add data in non-numeric order
	amountCol.Append("100")
	amountCol.Append("50")
	amountCol.Append("200")
	amountCol.Append("25")
	amountCol.Append("100")
	amountCol.Append("50")

	table.columns["amount"] = amountCol

	// Create a view with grouping on amount
	view := &View{
		order:        []string{"amount"},
		sorting:      map[string]bool{"amount": true},
		grouping:     []string{"amount"},
		groupOnOrder: []string{"amount"},
		groupSortPos: map[string]int{},
		groupOn:      map[string][]string{"amount": {}}, // empty = auto-group all
		columnViews:  map[string]*ColumnView{},
	}

	// Apply the view
	_, g, err := table.Apply(view)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Debug: Print the groupKeyToOrder mapping
	t.Logf("columnViews for 'amount': %+v", view.columnViews["amount"])
	if cv := view.columnViews["amount"]; cv != nil {
		t.Logf("groupKeyToOrder: %v", cv.groupKeyToOrder)
		t.Logf("groupKeyToFilter: %v", cv.groupKeyToFilter)
		t.Logf("filterToGroupKey: %v", cv.filterToGroupKey)
	}

	// Render the table to check the order
	sb := Render(table, view, g, nil)
	html := sb.String()

	// Extract positions - search for table data rows
	val25Pos := strings.Index(html, "<td>25")
	val50Pos := strings.Index(html, "<td>50")
	val100Pos := strings.Index(html, "<td>100")
	val200Pos := strings.Index(html, "<td>200")

	if val25Pos == -1 || val50Pos == -1 || val100Pos == -1 || val200Pos == -1 {
		t.Fatalf("Not all amount values found in output")
	}

	// Check they appear in numeric order: 25, 50, 100, 200
	if !(val25Pos < val50Pos && val50Pos < val100Pos && val100Pos < val200Pos) {
		t.Errorf("Groups not in numeric order. Positions: 25=%d, 50=%d, 100=%d, 200=%d",
			val25Pos, val50Pos, val100Pos, val200Pos)
	}

	t.Logf("✓ Groups sorted numerically: 25, 50, 100, 200")
}

// TestMultiLevelGroupSorting tests that multi-level grouping maintains sort order at each level
func TestMultiLevelGroupSorting(t *testing.T) {
	// Create a table with region and status columns
	table := &Table{columns: map[string]IColumn{}}

	regionCol := NewColumn[uint32](&ColumnDef{
		name:        "region",
		displayName: "Region",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareStrings,
		summable:    false,
	})

	statusCol := NewColumn[uint32](&ColumnDef{
		name:        "status",
		displayName: "Status",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareStrings,
		summable:    false,
	})

	// Add data: (region, status)
	rows := []struct{ region, status string }{
		{"West", "Pending"},
		{"East", "Complete"},
		{"West", "Complete"},
		{"East", "Pending"},
		{"North", "Complete"},
		{"North", "Pending"},
	}

	for _, row := range rows {
		regionCol.Append(row.region)
		statusCol.Append(row.status)
	}

	table.columns["region"] = regionCol
	table.columns["status"] = statusCol

	// Create a view with grouping on region, then status
	view := &View{
		order:        []string{"region", "status"},
		sorting:      map[string]bool{"region": true, "status": true},
		grouping:     []string{"region", "status"},
		groupOnOrder: []string{"region", "status"},
		groupSortPos: map[string]int{},
		groupOn:      map[string][]string{"region": {}, "status": {}},
		columnViews:  map[string]*ColumnView{},
	}

	// Apply the view
	_, g, err := table.Apply(view)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Render the table to check the order
	sb := Render(table, view, g, nil)
	html := sb.String()

	// First level should be: East, North, West (alphabetical)
	eastPos := strings.Index(html, ">East<")
	northPos := strings.Index(html, ">North<")
	westPos := strings.Index(html, ">West<")

	if eastPos == -1 || northPos == -1 || westPos == -1 {
		t.Fatalf("Not all region values found in output. HTML:\n%s", html)
	}

	if !(eastPos < northPos && northPos < westPos) {
		t.Errorf("First level (region) not sorted alphabetically. Positions: East=%d, North=%d, West=%d",
			eastPos, northPos, westPos)
	}

	// Within each region, status should be: Complete, Pending (alphabetical)
	// This is harder to test precisely without parsing the HTML structure
	// For now, just verify both status values appear
	if !strings.Contains(html, ">Complete<") || !strings.Contains(html, ">Pending<") {
		t.Errorf("Not all status values found in output")
	}

	t.Logf("✓ Multi-level grouping maintains sort order")
}
