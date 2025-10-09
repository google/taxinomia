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
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// todo column names cannot contain any of the following characters ':' ',' '&' '=' ';'

type View struct {
	order []string // order in which to display the columns
	// let's follow column index sortPriority []string        // sort order of each column
	sorting      map[string]bool // sort direction of each column
	// for grouped columns this is a signed integer
	// for non-grouped ones it is a + or -
	grouping     []string              // DEPRECATED: use GroupingColumns() method instead. Kept for backward compatibility with clone/toggle operations.
	groupOnOrder []string              // ordered list of columns in groupOn (preserves hierarchy order from URL)
	groupSortPos map[string]int        // +-1 by value, +-2 is by number of subgroups, etc
	groupOn      map[string][]string   // filters for each column. Empty list = auto-group all distinct values, single value = filtering only

	columnViews map[string]*ColumnView
}

func (v *View) ToQuery() string {

	// visible and ordering of columns
	columns := "columns=" //+ strings.Join(v.order, ",")
	comma := ""
	for _, c := range v.order {
		direction := "-"
		if v.sorting[c] {
			direction = "+"
		}
		columns = columns + comma + c + direction
		comma = ","
	}

	sortBy := "sortby="
	comma = ""
	cols := []string{}
	for k, _ := range v.groupSortPos {
		cols = append(cols, k)
	}
	slices.Sort(cols)
	for _, c := range cols {
		sortBy = sortBy + comma + c + ":" + fmt.Sprintf("%d", v.groupSortPos[c])
		comma = ","
	}

	// Consolidated groupon: order defines hierarchy, filters define groups
	groupOn := "groupon="
	comma = ""
	for _, c := range v.groupOnOrder {
		filters := v.groupOn[c]
		if len(filters) == 0 {
			// Empty filters = auto-group all distinct values (default grouping)
			// Use cleaner format without colon
			groupOn = groupOn + comma + c
		} else {
			// Custom filters for this column
			f := ""
			semicolon := ""
			for _, filter := range filters {
				escaped := url.QueryEscape(filter)
				f = f + semicolon + escaped
				semicolon = ";"
			}
			groupOn = groupOn + comma + c + ":" + f
		}
		comma = ","
	}

	query := columns + "&" + sortBy + "&" + groupOn
	return query
}

func testToQuery() {
	// Test 1: No grouping
	v := View{
		order:   []string{"a", "b", "c"},
		sorting: map[string]bool{"a": false, "b": true, "c": false},
		groupOnOrder: []string{},
		groupOn: map[string][]string{},
	}
	query := v.ToQuery()
	if query != `columns=a-,b+,c-&sortby=&groupon=` {
		fmt.Println(query)
		panic("testToQuery error")
	}

	// Test 2: Auto-grouping (empty filters)
	v = View{
		order:        []string{"a", "b", "c"},
		sorting:      map[string]bool{"a": false, "b": false, "c": false},
		grouping:     []string{"b", "c", "a"},
		groupOnOrder: []string{"b", "c", "a"},
		groupOn:      map[string][]string{"b": {}, "c": {}, "a": {}},
		groupSortPos: map[string]int{"a": -2, "b": 2, "c": 1},
	}
	query = v.ToQuery()
	if query != "columns=a-,b-,c-&sortby=a:-2,b:2,c:1&groupon=b,c,a" {
		fmt.Println(query)
		panic("testToQuery error")
	}

	// Test 3: Custom filters
	v = View{
		order:        []string{"a", "b", "c"},
		sorting:      map[string]bool{"a": true, "b": true, "c": true},
		grouping:     []string{"a", "c"},
		groupOnOrder: []string{"a", "c"},
		groupOn:      map[string][]string{"a": {`""`}, "c": {`xyz\&,=:"`, "test"}},
	}
	query = v.ToQuery()
	if query != `columns=a+,b+,c+&sortby=&groupon=a:%22%22,c:xyz%5C%26%2C%3D%3A%22;test` {
		fmt.Println(query)
		panic("testToQuery error")
	}
}

func ParseQuery(query string) (*View, error) {
	v := &View{
		order:        []string{},
		sorting:      map[string]bool{},
		grouping:     []string{},
		groupOnOrder: []string{},
		groupOn:      map[string][]string{},
		groupSortPos: map[string]int{},
		columnViews:  map[string]*ColumnView{},
	}
	// Don't decode the entire query string - only decode individual values where needed
	for _, section := range strings.Split(query, "&") {
		parts := strings.SplitN(section, "=", 2)
		if len(parts) < 2 || len(parts[1]) == 0 {
			continue
		}
		switch parts[0] {
		case "columns":
			// Split on commas without decoding (commas aren't encoded by browsers)
			for _, e := range strings.Split(parts[1], ",") {
				if strings.HasSuffix(e, "+") {
					colName := e[:len(e)-1]
					v.sorting[colName] = true
					v.order = append(v.order, colName)
				} else if strings.HasSuffix(e, "-") {
					colName := e[:len(e)-1]
					v.sorting[colName] = false
					v.order = append(v.order, colName)
				} else {
					v.sorting[e] = true
					v.order = append(v.order, e)
				}
			}
			fmt.Println("order", v.order)
			fmt.Println("sorting", v.sorting)

		case "sortby":
			// Split on commas without decoding
			for _, e := range strings.Split(parts[1], ",") {
				// colanme+ or colname- for non-grouped columns
				// colname:-1 or colname:+2 for grouped columns
				// if strings.Contains(e, ":") {
				// grouped
				pp := strings.Split(e, ":")
				if len(pp) < 2 {
					continue
				}
				col := pp[0]
				sort, _ := strconv.Atoi(pp[1])
				v.groupSortPos[col] = sort
				// } else {
				// 	// non-grouped
				// 	c := e[:len(e)-1]
				// 	v.sortPriority = append(v.sortPriority, c)
				// 	if strings.HasSuffix(e, "+") {
				// 		v.sorting[c] = true
				// 	} else if strings.HasSuffix(e, "-") {
				// 		v.sorting[c] = false
				// 	} else {
				// 		// error
				// 	}
				// }
			}
		case "groupon":
			// Format: groupon=col1:filter1;filter2,col2:filter3,col3
			// Order in groupon defines hierarchy order
			// No colon (col3) = auto-group all distinct values (default grouping)
			// Empty after colon (col3:) = same as no colon (backward compatibility)
			// Single filter = filtering only (not grouping)
			// Don't decode first - structural separators are not encoded
			for _, e := range strings.Split(parts[1], ",") {
				p := strings.SplitN(e, ":", 2)
				if len(p) < 1 || p[0] == "" {
					continue
				}
				// Decode column name
				colName, _ := url.QueryUnescape(p[0])
				v.groupOnOrder = append(v.groupOnOrder, colName)

				// Check if there's a colon
				if len(p) == 2 && p[1] != "" {
					// Has filters after colon
					filterStr := p[1]

					// Support both || and ; as group separators
					filterStr = strings.ReplaceAll(filterStr, "||", ";")
					filters := strings.Split(filterStr, ";")
					v.groupOn[colName] = []string{}
					for _, f := range filters {
						if f != "" {
							// Decode each filter value
							unescaped, _ := url.QueryUnescape(f)
							v.groupOn[colName] = append(v.groupOn[colName], unescaped)
						}
					}
					// Add to grouping if NOT single-value filtering
					if len(v.groupOn[colName]) != 1 {
						v.grouping = append(v.grouping, colName)
					}
				} else {
					// No colon (len(p)==1) or empty after colon (len(p)==2 && p[1]=="")
					// Both mean: default grouping (auto-group all distinct values)
					v.groupOn[colName] = []string{}
					v.grouping = append(v.grouping, colName)
				}
			}
		default:
			return nil, fmt.Errorf("unknown section name:%s", parts[0])
		}
	}
	fmt.Println(v)
	return v, nil
}

// GroupingColumns returns columns that should create hierarchical groups
// A column is for grouping if it has multiple filter values or empty filters (auto-group)
// A column with a single filter value is for filtering only
func (v *View) GroupingColumns() []string {
	grouping := []string{}
	for _, col := range v.groupOnOrder {
		filters := v.groupOn[col]
		// Empty filters (auto-group) or multiple filters (explicit groups) = grouping
		// Single filter = filtering only
		if len(filters) == 0 || len(filters) > 1 {
			grouping = append(grouping, col)
		}
	}
	return grouping
}

func (v *View) AggregatedColumns() []string {
	grouping := v.grouping
	a := []string{}
	for _, c := range v.order {
		if !slices.Contains(grouping, c) {
			a = append(a, c)
		}
	}
	return a
}

func testParseQuery() {
	// Test 1: No grouping
	q := "columns=a,b,c&sortby=&groupon="
	a := "columns=a+,b+,c+&sortby=&groupon="
	v, _ := ParseQuery(q)
	if v.ToQuery() != a {
		fmt.Println("Test 1 failed")
		fmt.Println("Got:", v.ToQuery())
		fmt.Println("Expected:", a)
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 1: No grouping")

	// Test 2: Auto-grouping with sortby
	q = `columns=a,b,c&sortby=a:0,b:-1,c:2&groupon=b:,c:,a:`
	a = `columns=a+,b+,c+&sortby=a:0,b:-1,c:2&groupon=b,c,a`
	v, _ = ParseQuery(q)
	if v.ToQuery() != a {
		fmt.Println("Test 2 failed")
		fmt.Println("Got:", v.ToQuery())
		fmt.Println("Expected:", a)
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 2: Auto-grouping with sortby")

	// Test 3: Grouping with multiple special character values (using ;)
	// Browser encodes the whole parameter value, so commas/colons in data are encoded
	q = `columns=a,b,c&sortby=&groupon=a:%22%22;%22xyz%22,c:val1;val2`
	a = `columns=a+,b+,c+&sortby=&groupon=a:%22%22;%22xyz%22,c:val1;val2`
	v, _ = ParseQuery(q)
	if v.ToQuery() != a {
		fmt.Println("Test 3 failed")
		fmt.Println("Got:", v.ToQuery())
		fmt.Println("Expected:", a)
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 3: Grouping with multiple values")

	// Test 4: Columns with + and - for sorting (server-generated format)
	q = `columns=state+,author_type+,label_count+&sortby=&groupon=`
	a = `columns=state+,author_type+,label_count+&sortby=&groupon=`
	v, _ = ParseQuery(q)
	if v.ToQuery() != a {
		fmt.Println("Test 4 failed")
		fmt.Println("Got:", v.ToQuery())
		fmt.Println("Expected:", a)
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 4: Columns with sorting direction")

	// Test 5: Filter with || syntax (converted to ;)
	q = `columns=state+&sortby=&groupon=state:%22CLOSED%22;%22OPEN%22`
	a = `columns=state+&sortby=&groupon=state:%22CLOSED%22;%22OPEN%22`
	v, _ = ParseQuery(q)
	if v.ToQuery() != a {
		fmt.Println("Test 5 failed")
		fmt.Println("Got:", v.ToQuery())
		fmt.Println("Expected:", a)
		fmt.Println("Grouping:", v.grouping)
		fmt.Println("GroupOn:", v.groupOn)
		panic("testParseQuery error")
	}
	if len(v.groupOn["state"]) != 2 || v.groupOn["state"][0] != `"CLOSED"` || v.groupOn["state"][1] != `"OPEN"` {
		fmt.Println("Test 5 filter parsing failed")
		fmt.Println("Got:", v.groupOn["state"])
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 5: Filter with semicolon separator")

	// Test 6: Round-trip - Parse server URL with old format (col:) should normalize to new format (col)
	serverURL := `columns=state+,author_type+,label_count+,year+,has_assignee+,label+&sortby=label_count:0&groupon=state:,author_type:,label_count:,year:,has_assignee:`
	expected := `columns=state+,author_type+,label_count+,year+,has_assignee+,label+&sortby=label_count:0&groupon=state,author_type,label_count,year,has_assignee`
	v, _ = ParseQuery(serverURL)
	if v.ToQuery() != expected {
		fmt.Println("Test 6 failed - Round trip")
		fmt.Println("Got:", v.ToQuery())
		fmt.Println("Expected:", expected)
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 6: Round-trip server URL")

	// Test 7: Column names should NOT have spaces
	q = `columns=state+,author_type+,label+&sortby=&groupon=state:%22CLOSED%22`
	v, _ = ParseQuery(q)
	for _, col := range v.order {
		if strings.Contains(col, " ") {
			fmt.Println("Test 7 failed - Column has spaces:", col)
			panic("testParseQuery error")
		}
	}
	if v.order[0] != "state" || v.order[1] != "author_type" || v.order[2] != "label" {
		fmt.Println("Test 7 failed - Wrong column names")
		fmt.Println("Got:", v.order)
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 7: Column names clean (no spaces)")

	// Test 8: Single value (no ||) should be filtering only, not grouping
	q = `columns=state+,author_type+&sortby=&groupon=state:CLOSED`
	v, _ = ParseQuery(q)
	if slices.Contains(v.grouping, "state") {
		fmt.Println("Test 8 failed - Single value should not add to grouping")
		fmt.Println("Grouping:", v.grouping)
		fmt.Println("GroupOn:", v.groupOn)
		panic("testParseQuery error")
	}
	if len(v.groupOn["state"]) != 1 || v.groupOn["state"][0] != "CLOSED" {
		fmt.Println("Test 8 failed - Filter not stored correctly")
		fmt.Println("GroupOn:", v.groupOn)
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 8: Single value = filtering (not in grouping)")

	// Test 9: Multiple values with || should be grouping
	q = `columns=state+,author_type+&sortby=&groupon=state:CLOSED||OPEN`
	v, _ = ParseQuery(q)
	if !slices.Contains(v.grouping, "state") {
		fmt.Println("Test 9 failed - Multiple values should add to grouping")
		fmt.Println("Grouping:", v.grouping)
		fmt.Println("GroupOn:", v.groupOn)
		panic("testParseQuery error")
	}
	if len(v.groupOn["state"]) != 2 || v.groupOn["state"][0] != "CLOSED" || v.groupOn["state"][1] != "OPEN" {
		fmt.Println("Test 9 failed - Filters not stored correctly")
		fmt.Println("GroupOn:", v.groupOn)
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 9: Multiple values with || = grouping (in grouping)")

	// Test 10: Empty value should be grouping (auto-group all)
	q = `columns=state+&sortby=&groupon=state:`
	v, _ = ParseQuery(q)
	if !slices.Contains(v.grouping, "state") {
		fmt.Println("Test 10 failed - Empty should add to grouping")
		fmt.Println("Grouping:", v.grouping)
		panic("testParseQuery error")
	}
	if len(v.groupOn["state"]) != 0 {
		fmt.Println("Test 10 failed - Empty should have no filters")
		fmt.Println("GroupOn:", v.groupOn)
		panic("testParseQuery error")
	}
	fmt.Println("✓ Test 10: Empty value = auto-grouping (in grouping)")

	fmt.Println("\nAll tests passed!")
}

// generate html
// transform view

// group sorting
//  - value
//  - subgroups
//    - count
//    - sum
//    - whatever else
// depth and aggregation need to be specified

// operations on view

func (v *View) clone() *View {
	w := &View{}
	w.order = []string{}
	for _, o := range v.order {
		w.order = append(w.order, o)
	}
	w.grouping = []string{}
	for _, o := range v.grouping {
		w.grouping = append(w.grouping, o)
	}
	// w.sortPriority = []string{}
	// for _, o := range v.sortPriority {
	// 	w.sortPriority = append(w.sortPriority, o)
	// }
	w.sorting = map[string]bool{}
	for k, v := range v.sorting {
		w.sorting[k] = v
	}
	w.groupSortPos = map[string]int{}
	for k, v := range v.groupSortPos {
		w.groupSortPos[k] = v
	}
	w.groupOn = map[string][]string{}
	for k, v := range v.groupOn {
		w.groupOn[k] = slices.Clone(v)
	}
	w.groupOnOrder = []string{}
	for _, o := range v.groupOnOrder {
		w.groupOnOrder = append(w.groupOnOrder, o)
	}
	return w
}

//////////////
// grouping //
//////////////

func (v *View) GroupFirst(col string) (*View, error) {
	w := v.clone()
	w.grouping = []string{}
	found := false
	for _, c := range v.grouping {
		if c == col {
			found = true
		} else {
			w.grouping = append(w.grouping, c)
		}
	}
	if found {
		w.grouping = append([]string{col}, w.grouping...)
	}
	return w, nil
}

func testGroupFirst() {
	v := View{grouping: []string{}, groupOnOrder: []string{}}
	w, _ := v.GroupFirst("x")
	if strings.Join(w.grouping, ",") != "" {
		fmt.Println(strings.Join(w.grouping, ""))
		panic("testGroupFirst error")
	}

	v = View{grouping: []string{"a", "b", "c"}, groupOnOrder: []string{"a", "b", "c"}}
	w, _ = v.GroupFirst("c")
	if strings.Join(w.grouping, ",") != "c,a,b" {
		fmt.Println(strings.Join(w.grouping, ","))
		panic("testGroupFirst error")
	}
	w, _ = v.GroupFirst("b")
	if strings.Join(w.grouping, ",") != "b,a,c" {
		fmt.Println(strings.Join(w.grouping, ","))
		panic("testGroupFirst error")
	}
	w, _ = v.GroupFirst("a")
	if strings.Join(w.grouping, ",") != "a,b,c" {
		fmt.Println(strings.Join(w.grouping, ","))
		panic("testGroupFirst error")
	}
	w, _ = v.GroupFirst("x")
	if strings.Join(w.grouping, ",") != "a,b,c" {
		fmt.Println(strings.Join(w.grouping, ","))
		panic("testGroupFirst error")
	}
}

// support switch style statement for grouping, in its simplest form it will create two groups, one that matches and another that doesn't match
// this is incompatible with grouping
// what should its behaviour be with repect to the normal filtering?
func (v *View) SetGroupOn(col string, filters string) (*View, error) {
	w := v.clone()
	if filters == "" {
		delete(w.groupOn, col)
		return w, nil
	}
	w.groupOn[col] = strings.Split(filters, ",")

	w.grouping = []string{}
	removed := false
	for _, c := range v.grouping {
		if c == col {
			removed = true
		} else {
			w.grouping = append(w.grouping, c)
		}
	}
	if !removed {
		w.grouping = append([]string{col}, w.grouping...)
	}

	return w.reorderColums(), nil
}

func (v *View) ToggleGrouping(col string) (*View, error) {
	w := v.clone()
	w.grouping = []string{}
	removed := false
	for _, c := range v.grouping {
		if c == col {
			removed = true
		} else {
			w.grouping = append(w.grouping, c)
		}
	}
	if !removed {
		w.grouping = append(w.grouping, col)
	}

	// Clean up groupOn: remove columns that are not grouped and have no filters
	for c, filters := range w.groupOn {
		isGrouped := slices.Contains(w.grouping, c)
		hasFilters := len(filters) > 0
		if !isGrouped && !hasFilters {
			// Column is not grouped and has no filters - remove it entirely
			delete(w.groupOn, c)
		}
	}

	// Update groupOnOrder to match the new grouping state
	// groupOnOrder should contain: grouped columns (in grouping order) + filtering-only columns
	w.groupOnOrder = []string{}
	groupedCols := map[string]bool{}
	for _, c := range w.grouping {
		groupedCols[c] = true
		w.groupOnOrder = append(w.groupOnOrder, c)
	}
	// Add filtering-only columns (columns in groupOn but not in grouping)
	// Preserve original order from v.groupOnOrder
	for _, c := range v.groupOnOrder {
		if _, exists := w.groupOn[c]; exists && !groupedCols[c] {
			// This column is for filtering only, preserve it
			w.groupOnOrder = append(w.groupOnOrder, c)
		}
	}

	return w.reorderColums(), nil
}

// reorder following a grouping/ungrouping change
// New order: filtered columns (left), then grouped columns (middle), then aggregated columns (right)
// Most recently filtered/grouped appears rightmost within its section (reverse hierarchy)
func (v *View) reorderColums() *View {
	w := v.clone()
	w.order = []string{}

	// Build sets for quick lookup
	grouped := map[string]bool{}
	for _, c := range v.grouping {
		grouped[c] = true
	}

	filtered := map[string]bool{}
	for _, c := range v.groupOnOrder {
		if !grouped[c] {
			// Column in groupOn but not in grouping = filtering only
			filtered[c] = true
		}
	}

	// First section: Filtered-only columns (most recent = leftmost/first)
	// Forward order from groupOnOrder (most recent is last in groupOnOrder, becomes first in display)
	filteredCols := []string{}
	for _, c := range v.groupOnOrder {
		if filtered[c] {
			filteredCols = append(filteredCols, c)
		}
	}
	// Most recent filter is last in groupOnOrder, we want it first in display
	// So iterate backwards
	for i := len(filteredCols) - 1; i >= 0; i-- {
		w.order = append(w.order, filteredCols[i])
	}

	// Second section: Grouped columns (most recent = rightmost)
	// grouping array is already in order with most recent at the end
	for _, c := range v.grouping {
		w.order = append(w.order, c)
	}

	// Third section: Aggregated/regular columns (preserve original order)
	for _, c := range v.order {
		if !grouped[c] && !filtered[c] {
			w.order = append(w.order, c)
		}
	}

	return w
}

func testToggleGrouping() {
	v := View{grouping: []string{}, groupOnOrder: []string{}}
	w, _ := v.ToggleGrouping("a")
	if strings.Join(w.grouping, ",") != "a" {
		fmt.Println(strings.Join(w.grouping, ","))
		panic("testToggleGrouping error")
	}

	v = View{grouping: []string{"a", "b", "c"}, groupOnOrder: []string{"a", "b", "c"}}
	w, _ = v.ToggleGrouping("a")
	if strings.Join(w.grouping, ",") != "b,c" {
		fmt.Println(strings.Join(w.grouping, ","))
		panic("testToggleGrouping error")
	}
	w, _ = v.ToggleGrouping("b")
	if strings.Join(w.grouping, ",") != "a,c" {
		fmt.Println(strings.Join(w.grouping, ","))
		panic("testToggleGrouping error")
	}
	w, _ = v.ToggleGrouping("c")
	if strings.Join(w.grouping, ",") != "a,b" {
		fmt.Println(strings.Join(w.grouping, ","))
		panic("testToggleGrouping error")
	}
	w, _ = v.ToggleGrouping("d")
	if strings.Join(w.grouping, ",") != "a,b,c,d" {
		fmt.Println(strings.Join(w.grouping, ","))
		panic("testToggleGrouping error")
	}

	// Test: Preserve filters when ungrouping
	// Start with state:CLOSED (filtering), year (grouping)
	v = View{
		grouping:     []string{"year"},
		groupOnOrder: []string{"state", "year"},
		groupOn: map[string][]string{
			"state": {"CLOSED"},
			"year":  {},
		},
	}
	w, _ = v.ToggleGrouping("year")
	// After ungrouping year, state filter should be preserved
	if strings.Join(w.grouping, ",") != "" {
		fmt.Println("Expected empty grouping, got:", strings.Join(w.grouping, ","))
		panic("testToggleGrouping error - grouping should be empty")
	}
	if strings.Join(w.groupOnOrder, ",") != "state" {
		fmt.Println("Expected groupOnOrder='state', got:", strings.Join(w.groupOnOrder, ","))
		panic("testToggleGrouping error - state filter should be preserved in groupOnOrder")
	}
	if len(w.groupOn) != 1 {
		fmt.Println("Expected groupOn to have 1 entry (state only), got:", len(w.groupOn))
		panic("testToggleGrouping error - ungrouped column with no filters should be removed from groupOn")
	}
	if len(w.groupOn["state"]) != 1 || w.groupOn["state"][0] != "CLOSED" {
		fmt.Println("Expected state filter CLOSED, got:", w.groupOn["state"])
		panic("testToggleGrouping error - state filter value should be preserved")
	}

	// Test: Preserve filters when toggling a different column
	// Start with state:CLOSED (filtering), year (grouping), author (not grouped)
	v = View{
		grouping:     []string{"year"},
		groupOnOrder: []string{"state", "year"},
		groupOn: map[string][]string{
			"state": {"CLOSED"},
			"year":  {},
		},
		order: []string{"state", "year", "author"},
	}
	w, _ = v.ToggleGrouping("author")
	// After grouping author, state filter should still be preserved
	if strings.Join(w.grouping, ",") != "year,author" {
		fmt.Println("Expected grouping='year,author', got:", strings.Join(w.grouping, ","))
		panic("testToggleGrouping error - author should be added to grouping")
	}
	if !slices.Contains(w.groupOnOrder, "state") {
		fmt.Println("Expected state in groupOnOrder, got:", strings.Join(w.groupOnOrder, ","))
		panic("testToggleGrouping error - state filter should be preserved")
	}
	if len(w.groupOn["state"]) != 1 || w.groupOn["state"][0] != "CLOSED" {
		fmt.Println("Expected state filter CLOSED, got:", w.groupOn["state"])
		panic("testToggleGrouping error - state filter value should be preserved")
	}

	fmt.Println("✓ ToggleGrouping preserves filters correctly")
}

/////////////////
// sorting  //
/////////////////

func (v *View) RightShiftSortLevel(col string) (*View, error) {
	w := v.clone()
	//fmt.Println(col, w.groupSortPos[col])
	// todo test for valid col
	// test grouped
	pos := -1
	for i, c := range v.grouping {
		if c == col {
			pos = i
			break
		}
	}
	// grouped by a,b,c ->
	//    a can have sorting depth can be 0, 1, 2, 3
	//    b can have sorting depth can be 0, 1, 2
	//    c can have sorting depth can be 0, 1
	// min is 0
	// max is length(group) - pos
	if w.groupSortPos[col] == len(v.grouping)-pos {
		w.groupSortPos[col] = 0
	} else {
		w.groupSortPos[col] += 1
	}
	//fmt.Println(">", col, w.groupSortPos[col])
	return w, nil
}

func testRightShiftSortLevel() {
	v := View{grouping: []string{"a", "b", "c"}, groupOnOrder: []string{"a", "b", "c"}, groupSortPos: map[string]int{"a": 0, "b": 0, "c": 0}}
	w, _ := v.RightShiftSortLevel("a")
	if w.groupSortPos["a"] != 1 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testRightShiftSortLevel error")
	}
	x, _ := v.RightShiftSortLevel("a")
	w, _ = x.RightShiftSortLevel("a")
	if w.groupSortPos["a"] != 2 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testRightShiftSortLevel error")
	}
	x, _ = v.RightShiftSortLevel("a")
	y, _ := x.RightShiftSortLevel("a")
	w, _ = y.RightShiftSortLevel("a")
	if w.groupSortPos["a"] != 3 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testRightShiftSortLevel error")
	}
	x, _ = v.RightShiftSortLevel("a")
	y, _ = x.RightShiftSortLevel("a")
	z, _ := y.RightShiftSortLevel("a")
	w, _ = z.RightShiftSortLevel("a")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testRightShiftSortLevel error")
	}

	w, _ = v.RightShiftSortLevel("b")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 1 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testRightShiftSortLevel error")
	}
	x, _ = v.RightShiftSortLevel("b")
	w, _ = x.RightShiftSortLevel("b")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 2 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testRightShiftSortLevel error")
	}
	x, _ = v.RightShiftSortLevel("b")
	y, _ = x.RightShiftSortLevel("b")
	w, _ = y.RightShiftSortLevel("b")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testRightShiftSortLevel error")
	}

	w, _ = v.RightShiftSortLevel("c")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 1 {
		fmt.Println(w.groupSortPos)
		panic("testRightShiftSortLevel error")
	}
	x, _ = v.RightShiftSortLevel("c")
	w, _ = x.RightShiftSortLevel("c")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testRightShiftSortLevel error")
	}
}

func (v *View) LeftShiftSortLevel(col string) (*View, error) {
	w := v.clone()
	//fmt.Println(col, w.groupSortPos[col])
	// todo test for valid col
	// test grouped
	pos := -1
	for i, c := range v.grouping {
		if c == col {
			pos = i
			break
		}
	}
	// grouped by a,b,c ->
	//    a can have sorting depth can be 0, 1, 2, 3
	//    b can have sorting depth can be 0, 1, 2
	//    c can have sorting depth can be 0, 1
	// min is 0
	// max is length(group) - pos
	if w.groupSortPos[col] == 0 {
		w.groupSortPos[col] = len(v.grouping) - pos
	} else {
		w.groupSortPos[col] -= 1
	}
	//fmt.Println(">", col, w.groupSortPos[col])
	return w, nil
}

func testLeftShiftSortLevel() {
	v := View{grouping: []string{"a", "b", "c"}, groupOnOrder: []string{"a", "b", "c"}, groupSortPos: map[string]int{"a": 0, "b": 0, "c": 0}}
	w, _ := v.LeftShiftSortLevel("a")
	if w.groupSortPos["a"] != 3 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testLeftShiftSortLevel error")
	}
	x, _ := v.LeftShiftSortLevel("a")
	w, _ = x.LeftShiftSortLevel("a")
	if w.groupSortPos["a"] != 2 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testLeftShiftSortLevel error")
	}
	x, _ = v.LeftShiftSortLevel("a")
	y, _ := x.LeftShiftSortLevel("a")
	w, _ = y.LeftShiftSortLevel("a")
	if w.groupSortPos["a"] != 1 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testLeftShiftSortLevel error")
	}
	x, _ = v.LeftShiftSortLevel("a")
	y, _ = x.LeftShiftSortLevel("a")
	z, _ := y.LeftShiftSortLevel("a")
	w, _ = z.LeftShiftSortLevel("a")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testLeftShiftSortLevel error")
	}

	w, _ = v.LeftShiftSortLevel("b")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 2 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testLeftShiftSortLevel error")
	}
	x, _ = v.LeftShiftSortLevel("b")
	w, _ = x.LeftShiftSortLevel("b")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 1 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testLeftShiftSortLevel error")
	}
	x, _ = v.LeftShiftSortLevel("b")
	y, _ = x.LeftShiftSortLevel("b")
	w, _ = y.LeftShiftSortLevel("b")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testLeftShiftSortLevel error")
	}

	w, _ = v.LeftShiftSortLevel("c")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 1 {
		fmt.Println(w.groupSortPos)
		panic("testLeftShiftSortLevel error")
	}
	x, _ = v.LeftShiftSortLevel("c")
	w, _ = x.LeftShiftSortLevel("c")
	if w.groupSortPos["a"] != 0 || w.groupSortPos["b"] != 0 || w.groupSortPos["c"] != 0 {
		fmt.Println(w.groupSortPos)
		panic("testLeftShiftSortLevel error")
	}
}

func (v *View) ToggleSortDirection(col string) (*View, error) {
	w := v.clone()
	fmt.Println("ToggleSortDirection", col)
	fmt.Println(col, w.sorting[col])
	// todo test for valid col
	grouped := false
	for _, c := range v.grouping {
		if c == col {
			grouped = true
			break
		}
	}
	fmt.Println("grouped", grouped)
	if grouped {
		w.groupSortPos[col] = -w.groupSortPos[col]
	} else {
		//if _, x := w.sorting[col]; x {
		w.sorting[col] = !w.sorting[col]
		//}
	}
	fmt.Println(">>", col, w.sorting[col])
	return w, nil
}

func testToggleSortDirection() {
	v := View{grouping: []string{"a", "b", "c"}, groupOnOrder: []string{"a", "b", "c"}, groupSortPos: map[string]int{"a": 1, "b": 1, "c": 1}}
	w, _ := v.ToggleSortDirection("x")
	if w.groupSortPos["a"] != 1 || w.groupSortPos["b"] != 1 || w.groupSortPos["c"] != 1 {
		fmt.Println(w.groupSortPos)
		panic("testToggleSortDirection error")
	}
	w, _ = v.ToggleSortDirection("a")
	if w.groupSortPos["a"] != -1 || w.groupSortPos["b"] != 1 || w.groupSortPos["c"] != 1 {
		fmt.Println(w.groupSortPos)
		panic("testToggleSortDirection error")
	}
	w, _ = v.ToggleSortDirection("b")
	if w.groupSortPos["a"] != 1 || w.groupSortPos["b"] != -1 || w.groupSortPos["c"] != 1 {
		fmt.Println(w.groupSortPos)
		panic("testToggleSortDirection error")
	}
	w, _ = v.ToggleSortDirection("c")
	if w.groupSortPos["a"] != 1 || w.groupSortPos["b"] != 1 || w.groupSortPos["c"] != -1 {
		fmt.Println(w.groupSortPos)
		panic("testToggleSortDirection error")
	}

	v = View{sorting: map[string]bool{"a": true, "b": true, "c": true}, groupSortPos: map[string]int{"a": 1, "b": 1, "c": 1}}
	w, _ = v.ToggleSortDirection("x")
	if !w.sorting["a"] || !w.sorting["b"] || !w.sorting["c"] {
		fmt.Println(w.sorting)
		panic("testToggleSortDirection error")
	}
	w, _ = v.ToggleSortDirection("a")
	if w.sorting["a"] || !w.sorting["b"] || !w.sorting["c"] {
		fmt.Println(w.sorting)
		panic("testToggleSortDirection error")
	}
	w, _ = v.ToggleSortDirection("b")
	if !w.sorting["a"] || w.sorting["b"] || !w.sorting["c"] {
		fmt.Println(w.sorting)
		panic("testToggleSortDirection error")
	}
	w, _ = v.ToggleSortDirection("c")
	if !w.sorting["a"] || !w.sorting["b"] || w.sorting["c"] {
		fmt.Println(w.sorting)
		panic("testToggleSortDirection error")
	}

}

// func (v *View) OrderFirst(col string) (*View, error) {
// 	w := v.clone()
// 	w.sortPriority = []string{col}
// 	found := false
// 	for _, c := range v.sortPriority {
// 		if c == col {
// 			found = true
// 		} else {
// 			w.sortPriority = append(w.sortPriority, c)
// 		}
// 	}
// 	if !found {
// 		w = v.clone()
// 	}
// 	return w, nil
// }

// func testOrderFirst() {
// 	v := View{sortPriority: []string{"a", "b", "c"}}
// 	w, _ := v.OrderFirst("x")
// 	if strings.Join(w.sortPriority, ",") != "a,b,c" {
// 		fmt.Println(strings.Join(w.sortPriority, ","))
// 		panic("testOrderFirst error")
// 	}
// 	w, _ = v.OrderFirst("a")
// 	if strings.Join(w.sortPriority, ",") != "a,b,c" {
// 		fmt.Println(strings.Join(w.sortPriority, ","))
// 		panic("testOrderFirst error")
// 	}
// 	w, _ = v.OrderFirst("b")
// 	if strings.Join(w.sortPriority, ",") != "b,a,c" {
// 		fmt.Println(strings.Join(w.sortPriority, ","))
// 		panic("testOrderFirst error")
// 	}
// 	w, _ = v.OrderFirst("c")
// 	if strings.Join(w.sortPriority, ",") != "c,a,b" {
// 		fmt.Println(strings.Join(w.sortPriority, ","))
// 		panic("testOrderFirst error")
// 	}
// }

/////////////////
// change columns order //
/////////////////

func (v *View) Hide(col string) (*View, error) {
	w := v.clone()
	w.order = []string{}
	for _, c := range v.order {
		if c != col {
			w.order = append(w.order, c)
		}
	}
	return w, nil
}

func testHide() {
	v := View{order: []string{"a", "b", "c"}}
	w, _ := v.Hide("x")
	if strings.Join(w.order, ",") != "a,b,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeftMost error")
	}
	w, _ = v.Hide("a")
	if strings.Join(w.order, ",") != "b,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeftMost error")
	}
	w, _ = v.Hide("b")
	if strings.Join(w.order, ",") != "a,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeftMost error")
	}
	w, _ = v.Hide("c")
	if strings.Join(w.order, ",") != "a,b" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeftMost error")
	}
}

func (v *View) MoveLeftMost(col string) (*View, error) {
	w := v.clone()
	w.order = []string{col}
	found := false
	for _, c := range v.order {
		if c == col {
			found = true
		} else {
			w.order = append(w.order, c)
		}
	}
	if !found {
		w = v.clone()
	}
	return w, nil
}

// [a,b,c,d] => [a,c,b,d]
//
//	0 1 2 3
//
// index = 2
// new index = 1
func (v *View) MoveLeft(col string) (*View, error) {
	w := v.clone()
	order := []string{}
	index := -1
	for i, c := range v.order {
		if c == col {
			index = i
		} else {
			order = append(order, c)
		}
	}
	if index == -1 {
		return w, nil
	}
	if index == 0 {
		index = len(v.order) - 1
	} else {
		index -= 1
	}
	w.order = []string{}
	for i, c := range order {
		if i == index {
			w.order = append(w.order, col)
		}
		w.order = append(w.order, c)
	}
	if index == len(v.order)-1 {
		w.order = append(w.order, col)
	}
	return w, nil
}

func testMoveLeftMost() {
	v := View{order: []string{"a", "b", "c"}}
	w, _ := v.MoveLeftMost("x")
	if strings.Join(w.order, ",") != "a,b,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeftMost error")
	}
	w, _ = v.MoveLeftMost("a")
	if strings.Join(w.order, ",") != "a,b,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeftMost error")
	}
	w, _ = v.MoveLeftMost("b")
	if strings.Join(w.order, ",") != "b,a,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeftMost error")
	}
	w, _ = v.MoveLeftMost("c")
	if strings.Join(w.order, ",") != "c,a,b" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeftMost error")
	}
}

func testMoveLeft() {
	v := View{order: []string{"a", "b", "c"}}
	w, _ := v.MoveLeft("x")
	if strings.Join(w.order, ",") != "a,b,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeft error")
	}
	w, _ = v.MoveLeft("a")
	if strings.Join(w.order, ",") != "b,c,a" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeft error")
	}
	w, _ = v.MoveLeft("b")
	if strings.Join(w.order, ",") != "b,a,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeft error")
	}
	w, _ = v.MoveLeft("c")
	if strings.Join(w.order, ",") != "a,c,b" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveLeft error")
	}
}

func (v *View) MoveRight(col string) (*View, error) {
	w := v.clone()
	order := []string{}
	index := -1
	for i, c := range v.order {
		if c == col {
			index = i
		} else {
			order = append(order, c)
		}
	}
	if index == -1 {
		return w, nil
	}
	if index == len(v.order)-1 {
		index = 0
	} else {
		index += 1
	}

	w.order = []string{}
	for i, c := range order {
		if i == index {
			w.order = append(w.order, col)
		}
		w.order = append(w.order, c)
	}
	if index == len(v.order)-1 {
		w.order = append(w.order, col)
	}
	return w, nil
}

func testMoveRight() {
	v := View{order: []string{"a", "b", "c"}}
	w, _ := v.MoveRight("x")
	if strings.Join(w.order, ",") != "a,b,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveRight error")
	}
	w, _ = v.MoveRight("a")
	if strings.Join(w.order, ",") != "b,a,c" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveRight error")
	}
	w, _ = v.MoveRight("b")
	if strings.Join(w.order, ",") != "a,c,b" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveRight error")
	}
	w, _ = v.MoveRight("c")
	if strings.Join(w.order, ",") != "c,a,b" {
		fmt.Println(strings.Join(w.order, ","))
		panic("testMoveRight error")
	}
}
