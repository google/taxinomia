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

package tables

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/taxinomia/core/columns"
)

// termsTable: 6 rows of region (text), amount (number), day (date).
//
//	East          5    2023-12-31
//	Northeast    10    2024-01-15
//	West         50    2024-03-01
//	North       100    2024-06-30
//	South       150    2024-07-01
//	East        200    2025-02-10
func termsTable() *DataTable {
	t := NewDataTable()
	region := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))
	amount := columns.NewFloat64Column(columns.NewColumnDef("amount", "Amount", ""))
	day := columns.NewDatetimeColumn(columns.NewColumnDef("day", "Day", ""))
	rows := []struct {
		r string
		a float64
		d string
	}{
		{"East", 5, "2023-12-31"}, {"Northeast", 10, "2024-01-15"}, {"West", 50, "2024-03-01"},
		{"North", 100, "2024-06-30"}, {"South", 150, "2024-07-01"}, {"East", 200, "2025-02-10"},
	}
	for _, r := range rows {
		region.Append(r.r)
		amount.Append(r.a)
		tm, _ := time.Parse("2006-01-02", r.d)
		day.Append(tm)
	}
	region.FinalizeColumn()
	amount.FinalizeColumn()
	day.FinalizeColumn()
	t.AddColumn(region)
	t.AddColumn(amount)
	t.AddColumn(day)
	return t
}

func filteredRegions(t *testing.T, tv *TableView, filters map[string]string) string {
	t.Helper()
	tv.ApplyFilters(filters)
	col := tv.GetColumn("region")
	var out []string
	for i := 0; i < col.Length(); i++ {
		if tv.filterSel == nil || tv.filterSel.Contains(uint32(i)) {
			v, _ := col.GetString(uint32(i))
			out = append(out, v)
		}
	}
	return strings.Join(out, ",")
}

func TestFilterRanges(t *testing.T) {
	tv := NewTableView(termsTable(), "t")
	cases := []struct {
		col, filter, want string
	}{
		{"amount", "10..100", "Northeast,West"},
		{"amount", "<=10", "East,Northeast"},
		{"amount", ">100", "South,East"},
		{"amount", "..10|>=150", "East,South,East"},
		{"amount", "100..", "North,South,East"},
		{"day", "2024", "Northeast,West,North,South"}, // text match on the shown date
		{"day", "2024-01..2024-07", "Northeast,West,North"},
		{"day", "<2024", "East"},
		{"day", "<=2024-06", "East,Northeast,West,North"},
		{"day", ">2024-06", "South,East"},
		{"day", ">=2025", "East"},
		{"region", `"East"|"West"`, "East,West,East"},             // quoted alternatives
		{"region", "east|WEST", "East,Northeast,West,East"},       // bare alternatives: contains, any case
		{"region", "north|\"East\"", "East,Northeast,North,East"}, // mixed
		{"region", "<b", ""},                                      // text column: plain text
	}
	for _, c := range cases {
		if got := filteredRegions(t, tv, map[string]string{c.col: c.filter}); got != c.want {
			t.Errorf("%s %q: %s, want %s", c.col, c.filter, got, c.want)
		}
	}
}

func groupLabels(t *testing.T, tv *TableView, grouped []string) string {
	t.Helper()
	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), map[string]bool{}, 0, GroupExpansion{ExpandAll: true})
	var out []string
	for _, g := range tv.GetFirstBlock().Groups {
		out = append(out, g.GetValue()+"="+strconv.Itoa(g.Length()))
	}
	return strings.Join(out, " ")
}

func TestGroupDefinitions(t *testing.T) {
	tv := NewTableView(termsTable(), "t")
	tv.VisibleColumns = []string{"region", "amount", "day"}

	// Typed order, first match wins (Northeast goes to east), no [other]:
	// South matches nothing and leaves the view.
	if errs := tv.SetGroupDefinitions(map[string]string{"region": "west || east || north"}); len(errs) != 0 {
		t.Fatal(errs)
	}
	if got, want := filteredRegions(t, tv, nil), "East,Northeast,West,North,East"; got != want {
		t.Errorf("rows kept = %s, want %s", got, want)
	}
	if got, want := groupLabels(t, tv, []string{"region"}), "west=1 east=3 north=1"; got != want {
		t.Errorf("groups = %s, want %s", got, want)
	}
	if term, other, ok := tv.DefinedGroup("region", "east"); !ok || other || term != "east" {
		t.Errorf("DefinedGroup(east) = %q %v %v", term, other, ok)
	}

	// A trailing || adds [other], last; a changed definition is not served
	// from the caches.
	tv.SetGroupDefinitions(map[string]string{"region": `"East"|"West" || north ||`})
	if got, want := filteredRegions(t, tv, nil), "East,Northeast,West,North,South,East"; got != want {
		t.Errorf("rows kept = %s, want %s", got, want)
	}
	if got, want := groupLabels(t, tv, []string{"region"}), `"East"|"West"=3 north=2 [other]=1`; got != want {
		t.Errorf("groups = %s, want %s", got, want)
	}
	if _, other, ok := tv.DefinedGroup("region", OtherGroupLabel); !ok || !other {
		t.Errorf("[other] not reported as other")
	}

	// Number ranges as groups.
	tv.SetGroupDefinitions(map[string]string{"amount": "..10 || 10..100 || 100.."})
	if got, want := groupLabels(t, tv, []string{"amount"}), "..10=1 10..100=2 100..=3"; got != want {
		t.Errorf("amount groups = %s, want %s", got, want)
	}

	// Removing the definition groups by value again.
	tv.SetGroupDefinitions(nil)
	if got := groupLabels(t, tv, []string{"region"}); !strings.HasPrefix(got, "East=2 ") {
		t.Errorf("value groups = %s", got)
	}
}

func TestGroupDefinitionWithCondition(t *testing.T) {
	tv := NewTableView(termsTable(), "t")
	tv.VisibleColumns = []string{"region", "amount"}
	tv.SetGroupDefinitions(map[string]string{"region": "east || west ||"})
	cond, err := tv.CompileGroupCondition([]string{"region"}, 0, "count() >= 2")
	if err != nil {
		t.Fatal(err)
	}
	tv.SetGroupConditions([]*GroupCondition{cond})
	// east has 3 rows (East, Northeast, East), west 1, [other] 2 (North, South).
	if got, want := filteredRegions(t, tv, nil), "East,Northeast,North,South,East"; got != want {
		t.Errorf("rows kept = %s, want %s", got, want)
	}
}

func TestParseGroupDefinition(t *testing.T) {
	if terms, other, err := ParseGroupDefinition(`a|"b||c" || d ||`); err != nil || !other || strings.Join(terms, ";") != `a|"b||c";d` {
		t.Errorf("got %q %v %v", terms, other, err)
	}
	for _, bad := range []string{"||", "a |||| b", "a || a"} {
		if _, _, err := ParseGroupDefinition(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if IsGroupDefinition("a|b") || IsGroupDefinition(`"a||b"`) || !IsGroupDefinition("a||") {
		t.Error("IsGroupDefinition")
	}
}
