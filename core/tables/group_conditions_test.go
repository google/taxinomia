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
	"strings"
	"testing"
	"time"

	"github.com/google/taxinomia/core/columns"
)

// region amount active day
// N      10     true   2024-01-01
// N      20     true   2024-01-05
// N      30     false  2024-01-20
// S       5     false  2024-02-01
// S       5     false  2024-02-02
// E     100     true   2024-03-01
func groupConditionView(t *testing.T) *TableView {
	t.Helper()
	dt := NewDataTable()
	region := columns.NewChunkedStringColumn(columns.NewColumnDef("region", "Region", ""))
	amount := columns.NewChunkedInt64Column(columns.NewColumnDef("amount", "Amount", ""))
	active := columns.NewChunkedBoolColumn(columns.NewColumnDef("active", "Active", ""))
	day := columns.NewChunkedDatetimeColumn(columns.NewColumnDef("day", "Day", ""))
	rows := []struct {
		r string
		a int64
		b bool
		d string
	}{
		{"N", 10, true, "2024-01-01"}, {"N", 20, true, "2024-01-05"}, {"N", 30, false, "2024-01-20"},
		{"S", 5, false, "2024-02-01"}, {"S", 5, false, "2024-02-02"}, {"E", 100, true, "2024-03-01"},
	}
	for _, r := range rows {
		region.Append(r.r)
		amount.Append(r.a)
		active.Append(r.b)
		d, _ := time.Parse("2006-01-02", r.d)
		day.Append(d)
	}
	for _, c := range []interface{ FinalizeColumn() }{region, amount, active, day} {
		c.FinalizeColumn()
	}
	dt.AddColumn(region)
	dt.AddColumn(amount)
	dt.AddColumn(active)
	dt.AddColumn(day)
	return NewTableView(dt, "t")
}

func keptRows(t *testing.T, tv *TableView, grouping []string, level int, src string) int {
	t.Helper()
	c, err := tv.CompileGroupCondition(grouping, level, src)
	if err != nil {
		t.Fatalf("%q: %v", src, err)
	}
	tv.SetGroupConditions([]*GroupCondition{c})
	tv.ApplyFilters(nil)
	return tv.GetFilteredRowCount()
}

func TestGroupConditions(t *testing.T) {
	for _, tc := range []struct {
		src  string
		want int
	}{
		{"sum(amount) > 50", 4},                          // N (60) and E (100)
		{"count() >= 2", 5},                              // N and S
		{"avg(amount) < 10", 2},                          // S
		{"max(amount) - min(amount) > 15", 3},            // N
		{"any(active)", 4},                               // N and E
		{"all(active)", 1},                               // E
		{"ratio(active) > 0.5", 4},                       // N (2/3) and E
		{`max(day) > date("2024-02-15")`, 1},             // E
		{`span(day) > duration("10d")`, 3},               // N (19 days)
		{"count() >= 2 and not any(active)", 2},          // S
		{"unique(region) == 1 and sum(amount) == 10", 2}, // S
	} {
		if got := keptRows(t, groupConditionView(t), []string{"region"}, 0, tc.src); got != tc.want {
			t.Errorf("%q kept %d rows, want %d", tc.src, got, tc.want)
		}
	}
}

// A condition on the second level drops subgroups; a parent whose
// subgroups are all dropped goes too.
func TestGroupConditionsSecondLevel(t *testing.T) {
	tv := groupConditionView(t)
	// (N,true)=2 kept, (N,false)=1 dropped, (S,false)=2 kept, (E,true)=1 dropped.
	if got := keptRows(t, tv, []string{"region", "active"}, 1, "count() >= 2"); got != 4 {
		t.Errorf("kept %d rows, want 4", got)
	}
	// subgroups() counts a level-0 group's subgroups.
	if got := keptRows(t, groupConditionView(t), []string{"region", "active"}, 0, "subgroups() > 1"); got != 3 {
		t.Errorf("subgroups() > 1 kept %d rows, want 3 (N)", got)
	}
}

// Conditions combine with row filters (filters first) and are part of the
// filter cache: changing or removing a condition recomputes.
func TestGroupConditionsWithFiltersAndCache(t *testing.T) {
	tv := groupConditionView(t)
	c, err := tv.CompileGroupCondition([]string{"region"}, 0, "sum(amount) > 20")
	if err != nil {
		t.Fatal(err)
	}
	tv.SetGroupConditions([]*GroupCondition{c})
	tv.ApplyFilters(map[string]string{"region": "N|S"}) // N (sum 60) kept, S (sum 10) dropped
	if got := tv.GetFilteredRowCount(); got != 3 {
		t.Errorf("filter then condition: %d rows, want 3", got)
	}
	tv.ApplyFilters(map[string]string{"region": "N|S"})
	if tv.LastFiltersRecomputed() {
		t.Errorf("same filters and condition: recomputed")
	}
	tv.SetGroupConditions(nil)
	tv.ApplyFilters(map[string]string{"region": "N|S"})
	if !tv.LastFiltersRecomputed() || tv.GetFilteredRowCount() != 5 {
		t.Errorf("condition removed: recomputed=%v rows=%d, want true and 5", tv.LastFiltersRecomputed(), tv.GetFilteredRowCount())
	}
}

func TestGroupConditionErrors(t *testing.T) {
	tv := groupConditionView(t)
	for _, tc := range []struct{ src, want string }{
		{"sum(region) > 1", "sum needs a number column; region is text"},
		{"any(amount)", "any needs a yes/no column"},
		{"count(amount) > 1", "count() takes no column"},
		{"sum() > 1", "sum needs a column"},
		{"sum(nope) > 1", `no column "nope"`},
		{"amount > 1", "use an aggregate"},
		{"count() > 1 and amount > 1", "inside an aggregate"},
		{"count()", "must be yes/no"},
		{"count() >", "syntax"},
	} {
		_, err := tv.CompileGroupCondition([]string{"region"}, 0, tc.src)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: error %v, want %q", tc.src, err, tc.want)
		}
	}
	if _, err := tv.CompileGroupCondition([]string{"region"}, 1, "count() > 1"); err == nil {
		t.Errorf("level outside the grouping: want an error")
	}
}
