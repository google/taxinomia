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
	"fmt"
)

// testAggregation validates that aggregations are calculated correctly
func testAggregation() {
	fmt.Println("\n=== Testing Aggregation ===")

	// Create test table with known data
	// Region | Status | Amount | Quantity
	// -------|--------|--------|----------
	// North  | Open   | 100    | 5
	// North  | Open   | 200    | 10
	// North  | Closed | 150    | 8
	// South  | Open   | 300    | 15
	// South  | Closed | 250    | 12
	// South  | Closed | 350    | 18

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
	amountCol := NewColumn[uint32](&ColumnDef{
		name:        "amount",
		displayName: "Amount",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    true,
	})
	quantityCol := NewColumn[uint32](&ColumnDef{
		name:        "quantity",
		displayName: "Quantity",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    true,
	})

	// Add rows
	regionCol.Append("North")   // row 0
	statusCol.Append("Open")
	amountCol.Append("100")
	quantityCol.Append("5")

	regionCol.Append("North")   // row 1
	statusCol.Append("Open")
	amountCol.Append("200")
	quantityCol.Append("10")

	regionCol.Append("North")   // row 2
	statusCol.Append("Closed")
	amountCol.Append("150")
	quantityCol.Append("8")

	regionCol.Append("South")   // row 3
	statusCol.Append("Open")
	amountCol.Append("300")
	quantityCol.Append("15")

	regionCol.Append("South")   // row 4
	statusCol.Append("Closed")
	amountCol.Append("250")
	quantityCol.Append("12")

	regionCol.Append("South")   // row 5
	statusCol.Append("Closed")
	amountCol.Append("350")
	quantityCol.Append("18")

	table := &Table{
		columns: map[string]IColumn{
			"region":   regionCol,
			"status":   statusCol,
			"amount":   amountCol,
			"quantity": quantityCol,
		},
	}

	// Test 1: Single level grouping by region
	fmt.Println("\nTest 1: Group by region only")
	testSingleGrouping(table)

	// Test 2: Two level grouping by region, then status
	fmt.Println("\nTest 2: Group by region, then status")
	testTwoLevelGrouping(table)

	// Test 3: With filtering
	fmt.Println("\nTest 3: Group by region with status filter (Closed only)")
	testGroupingWithFilter(table)

	fmt.Println("\n✓ All aggregation tests passed!")
}

func testSingleGrouping(table *Table) {
	// Group by region only
	// Expected:
	//   North: amount=450 (100+200+150), quantity=23 (5+10+8)
	//   South: amount=900 (300+250+350), quantity=45 (15+12+18)
	//   Total: amount=1350, quantity=68

	v := &View{
		order:        []string{"region", "amount", "quantity"},
		sorting:      map[string]bool{"region": true, "amount": true, "quantity": true},
		grouping:     []string{"region"},
		groupOnOrder: []string{"region"},
		groupOn:      map[string][]string{"region": {}},
		groupSortPos: map[string]int{},
		columnViews:  map[string]*ColumnView{},
	}

	_, group, err := table.Apply(v)
	if err != nil {
		panic(fmt.Sprintf("Apply failed: %v", err))
	}

	// Validate root sums
	if group.sums["amount"] != 1350 {
		panic(fmt.Sprintf("Expected root amount sum=1350, got %d", group.sums["amount"]))
	}
	if group.sums["quantity"] != 68 {
		panic(fmt.Sprintf("Expected root quantity sum=68, got %d", group.sums["quantity"]))
	}

	// Validate region groups
	// Find North group (need to check keyToValue mapping)
	var northGroup, southGroup *Group
	for _, g := range group.groups {
		displayValue := g.columnDef.keyToValue[g.value]
		if displayValue == "North" {
			northGroup = g
		} else if displayValue == "South" {
			southGroup = g
		}
	}

	if northGroup == nil {
		panic("North group not found")
	}
	if southGroup == nil {
		panic("South group not found")
	}

	if northGroup.sums["amount"] != 450 {
		panic(fmt.Sprintf("Expected North amount sum=450, got %d", northGroup.sums["amount"]))
	}
	if northGroup.sums["quantity"] != 23 {
		panic(fmt.Sprintf("Expected North quantity sum=23, got %d", northGroup.sums["quantity"]))
	}

	if southGroup.sums["amount"] != 900 {
		panic(fmt.Sprintf("Expected South amount sum=900, got %d", southGroup.sums["amount"]))
	}
	if southGroup.sums["quantity"] != 45 {
		panic(fmt.Sprintf("Expected South quantity sum=45, got %d", southGroup.sums["quantity"]))
	}

	fmt.Println("  ✓ Single level grouping correct")
}

func testTwoLevelGrouping(table *Table) {
	// Group by region, then status
	// Expected:
	//   North:
	//     Open: amount=300 (100+200), quantity=15 (5+10)
	//     Closed: amount=150, quantity=8
	//   South:
	//     Open: amount=300, quantity=15
	//     Closed: amount=600 (250+350), quantity=30 (12+18)

	v := &View{
		order:        []string{"region", "status", "amount", "quantity"},
		sorting:      map[string]bool{"region": true, "status": true, "amount": true, "quantity": true},
		grouping:     []string{"region", "status"},
		groupOnOrder: []string{"region", "status"},
		groupOn:      map[string][]string{"region": {}, "status": {}},
		groupSortPos: map[string]int{},
		columnViews:  map[string]*ColumnView{},
	}

	_, group, err := table.Apply(v)
	if err != nil {
		panic(fmt.Sprintf("Apply failed: %v", err))
	}

	// Validate root sums
	if group.sums["amount"] != 1350 {
		panic(fmt.Sprintf("Expected root amount sum=1350, got %d", group.sums["amount"]))
	}
	if group.sums["quantity"] != 68 {
		panic(fmt.Sprintf("Expected root quantity sum=68, got %d", group.sums["quantity"]))
	}

	// Find region groups
	var northGroup, southGroup *Group
	for _, g := range group.groups {
		displayValue := g.columnDef.keyToValue[g.value]
		if displayValue == "North" {
			northGroup = g
		} else if displayValue == "South" {
			southGroup = g
		}
	}

	if northGroup == nil || southGroup == nil {
		panic("Region groups not found")
	}

	// Validate North subtotals
	if northGroup.sums["amount"] != 450 {
		panic(fmt.Sprintf("Expected North amount sum=450, got %d", northGroup.sums["amount"]))
	}
	if northGroup.sums["quantity"] != 23 {
		panic(fmt.Sprintf("Expected North quantity sum=23, got %d", northGroup.sums["quantity"]))
	}

	// Validate South subtotals
	if southGroup.sums["amount"] != 900 {
		panic(fmt.Sprintf("Expected South amount sum=900, got %d", southGroup.sums["amount"]))
	}
	if southGroup.sums["quantity"] != 45 {
		panic(fmt.Sprintf("Expected South quantity sum=45, got %d", southGroup.sums["quantity"]))
	}

	// Find status subgroups for North
	var northOpen, northClosed *Group
	for _, g := range northGroup.groups {
		displayValue := g.columnDef.keyToValue[g.value]
		if displayValue == "Open" {
			northOpen = g
		} else if displayValue == "Closed" {
			northClosed = g
		}
	}

	if northOpen == nil || northClosed == nil {
		panic("North status subgroups not found")
	}

	if northOpen.sums["amount"] != 300 {
		panic(fmt.Sprintf("Expected North/Open amount sum=300, got %d", northOpen.sums["amount"]))
	}
	if northOpen.sums["quantity"] != 15 {
		panic(fmt.Sprintf("Expected North/Open quantity sum=15, got %d", northOpen.sums["quantity"]))
	}

	if northClosed.sums["amount"] != 150 {
		panic(fmt.Sprintf("Expected North/Closed amount sum=150, got %d", northClosed.sums["amount"]))
	}
	if northClosed.sums["quantity"] != 8 {
		panic(fmt.Sprintf("Expected North/Closed quantity sum=8, got %d", northClosed.sums["quantity"]))
	}

	// Find status subgroups for South
	var southOpen, southClosed *Group
	for _, g := range southGroup.groups {
		displayValue := g.columnDef.keyToValue[g.value]
		if displayValue == "Open" {
			southOpen = g
		} else if displayValue == "Closed" {
			southClosed = g
		}
	}

	if southOpen == nil || southClosed == nil {
		panic("South status subgroups not found")
	}

	if southOpen.sums["amount"] != 300 {
		panic(fmt.Sprintf("Expected South/Open amount sum=300, got %d", southOpen.sums["amount"]))
	}
	if southOpen.sums["quantity"] != 15 {
		panic(fmt.Sprintf("Expected South/Open quantity sum=15, got %d", southOpen.sums["quantity"]))
	}

	if southClosed.sums["amount"] != 600 {
		panic(fmt.Sprintf("Expected South/Closed amount sum=600, got %d", southClosed.sums["amount"]))
	}
	if southClosed.sums["quantity"] != 30 {
		panic(fmt.Sprintf("Expected South/Closed quantity sum=30, got %d", southClosed.sums["quantity"]))
	}

	fmt.Println("  ✓ Two level grouping correct")
}

func testGroupingWithFilter(table *Table) {
	// Group by region, filter status=Closed only
	// Expected:
	//   North/Closed: amount=150, quantity=8
	//   South/Closed: amount=600 (250+350), quantity=30 (12+18)
	//   Total: amount=750, quantity=38

	v := &View{
		order:        []string{"status", "region", "amount", "quantity"},
		sorting:      map[string]bool{"region": true, "status": true, "amount": true, "quantity": true},
		grouping:     []string{"region"},
		groupOnOrder: []string{"status", "region"},
		groupOn:      map[string][]string{"status": {"Closed"}, "region": {}},
		groupSortPos: map[string]int{},
		columnViews:  map[string]*ColumnView{},
	}

	_, group, err := table.Apply(v)
	if err != nil {
		panic(fmt.Sprintf("Apply failed: %v", err))
	}

	// With filter for Closed only, we should see:
	// Total: amount=750 (150+250+350), quantity=38 (8+12+18)
	if group.sums["amount"] != 750 {
		panic(fmt.Sprintf("Expected filtered root amount sum=750, got %d", group.sums["amount"]))
	}
	if group.sums["quantity"] != 38 {
		panic(fmt.Sprintf("Expected filtered root quantity sum=38, got %d", group.sums["quantity"]))
	}

	// Find region groups
	var northGroup, southGroup *Group
	for _, g := range group.groups {
		displayValue := g.columnDef.keyToValue[g.value]
		if displayValue == "North" {
			northGroup = g
		} else if displayValue == "South" {
			southGroup = g
		}
	}

	if northGroup == nil || southGroup == nil {
		panic("Region groups not found in filtered results")
	}

	// North should only have Closed rows
	if northGroup.sums["amount"] != 150 {
		panic(fmt.Sprintf("Expected North (filtered) amount sum=150, got %d", northGroup.sums["amount"]))
	}
	if northGroup.sums["quantity"] != 8 {
		panic(fmt.Sprintf("Expected North (filtered) quantity sum=8, got %d", northGroup.sums["quantity"]))
	}

	// South should only have Closed rows
	if southGroup.sums["amount"] != 600 {
		panic(fmt.Sprintf("Expected South (filtered) amount sum=600, got %d", southGroup.sums["amount"]))
	}
	if southGroup.sums["quantity"] != 30 {
		panic(fmt.Sprintf("Expected South (filtered) quantity sum=30, got %d", southGroup.sums["quantity"]))
	}

	fmt.Println("  ✓ Grouping with filter correct")
}
