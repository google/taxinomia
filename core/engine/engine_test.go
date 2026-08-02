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

// Translation test for the engine contract: a Request can be built from a URL
// via the existing core/query parser, and the fields map correctly. This is
// the only consumer of the engine types for now; the real URL ⇄ Request
// translator stays on the presentation side (in or next to core/query) so
// that the engine never imports URL or safehtml machinery.
package engine_test

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/google/taxinomia/core/engine"
	"github.com/google/taxinomia/core/query"
)

// requestFromQuery maps the URL-derived query state onto the engine contract.
// Presentation-only state (column widths, info pane, animation, selected row,
// sidebar expansion) deliberately does not map: it never reaches the engine.
func requestFromQuery(q *query.Query) engine.Request {
	req := engine.Request{
		Table:    q.Table,
		Columns:  append([]string(nil), q.Columns...),
		GroupBy:  append([]string(nil), q.GroupedColumns...),
		Viewport: engine.Viewport{Offset: 0, Limit: q.Limit},
	}
	if len(q.Filters) > 0 {
		req.Filters = make(map[string]string, len(q.Filters))
		for col, f := range q.Filters {
			req.Filters[col] = f
		}
	}
	for _, cc := range q.ComputedColumns {
		req.Computed = append(req.Computed, engine.ComputedColumn{
			Name:       cc.Name,
			Expression: cc.Expression,
		})
	}
	if len(q.AggregateSettings) > 0 {
		req.Aggregates = make(map[string][]engine.AggregateKind, len(q.AggregateSettings))
		for col, aggs := range q.AggregateSettings {
			kinds := make([]engine.AggregateKind, len(aggs))
			for i, a := range aggs {
				kinds[i] = engine.AggregateKind(a)
			}
			req.Aggregates[col] = kinds
		}
	}
	for _, sc := range q.SortOrder {
		req.Sort = append(req.Sort, engine.SortKey{Column: sc.Name, Descending: sc.Descending})
	}
	// GroupAggregateSorts is keyed by grouped column; emit in grouping-level
	// order so the result is deterministic.
	for _, groupedCol := range q.GroupedColumns {
		if gs := q.GroupAggregateSorts[groupedCol]; gs != nil {
			req.GroupSorts = append(req.GroupSorts, engine.GroupSort{
				GroupColumn: gs.GroupedColumn,
				AggColumn:   gs.LeafColumn,
				Agg:         engine.AggregateKind(gs.AggType),
				Descending:  gs.Descending,
			})
		}
	}
	// q.Expanded is sidebar join-list expansion — presentation state. Group
	// expansion has no URL encoding yet (phase 1c extends core/query); an
	// empty Expanded reproduces today's behaviour.
	return req
}

func mustParse(t *testing.T, rawURL string) *url.URL {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", rawURL, err)
	}
	return u
}

func TestRequestFromURL(t *testing.T) {
	u := mustParse(t, "/table?table=machines"+
		"&columns=name,rack,cpu:120,status"+
		"&grouped=rack,status"+
		"&filter:status=running&filter:cpu=%3E2"+
		"&limit=50"+
		"&computed=load=div(cpu,mem)"+
		"&sort=-cpu,%2Bname"+
		"&agg:cpu=sum,avg&agg:status=true,ratio"+
		"&groupsort:rack=-cpu:sum&groupsort:status=%2B:rows")

	req := requestFromQuery(query.NewQuery(u))

	if req.Table != "machines" {
		t.Errorf("Table = %q, want %q", req.Table, "machines")
	}

	// core/query reorders: filtered-only first, then grouped (in grouping
	// order), then the rest. The engine request preserves that order.
	wantColumns := []string{"cpu", "rack", "status", "name"}
	if !reflect.DeepEqual(req.Columns, wantColumns) {
		t.Errorf("Columns = %v, want %v", req.Columns, wantColumns)
	}

	wantGroupBy := []string{"rack", "status"}
	if !reflect.DeepEqual(req.GroupBy, wantGroupBy) {
		t.Errorf("GroupBy = %v, want %v", req.GroupBy, wantGroupBy)
	}

	wantFilters := map[string]string{"status": "running", "cpu": ">2"}
	if !reflect.DeepEqual(req.Filters, wantFilters) {
		t.Errorf("Filters = %v, want %v", req.Filters, wantFilters)
	}

	wantComputed := []engine.ComputedColumn{{Name: "load", Expression: "div(cpu,mem)"}}
	if !reflect.DeepEqual(req.Computed, wantComputed) {
		t.Errorf("Computed = %v, want %v", req.Computed, wantComputed)
	}

	wantSort := []engine.SortKey{
		{Column: "cpu", Descending: true},
		{Column: "name", Descending: false},
	}
	if !reflect.DeepEqual(req.Sort, wantSort) {
		t.Errorf("Sort = %v, want %v", req.Sort, wantSort)
	}

	wantAggregates := map[string][]engine.AggregateKind{
		"cpu":    {engine.AggSum, engine.AggAvg},
		"status": {engine.AggTrue, engine.AggRatio},
	}
	if !reflect.DeepEqual(req.Aggregates, wantAggregates) {
		t.Errorf("Aggregates = %v, want %v", req.Aggregates, wantAggregates)
	}

	wantGroupSorts := []engine.GroupSort{
		{GroupColumn: "rack", AggColumn: "cpu", Agg: engine.AggSum, Descending: true},
		{GroupColumn: "status", AggColumn: "", Agg: engine.AggRowCount, Descending: false},
	}
	if !reflect.DeepEqual(req.GroupSorts, wantGroupSorts) {
		t.Errorf("GroupSorts = %v, want %v", req.GroupSorts, wantGroupSorts)
	}

	wantViewport := engine.Viewport{Offset: 0, Limit: 50}
	if req.Viewport != wantViewport {
		t.Errorf("Viewport = %v, want %v", req.Viewport, wantViewport)
	}

	if len(req.Expanded) != 0 {
		t.Errorf("Expanded = %v, want empty (no URL encoding for group expansion yet)", req.Expanded)
	}
}

func TestRequestFromURL_Defaults(t *testing.T) {
	req := requestFromQuery(query.NewQuery(mustParse(t, "/table?table=machines")))

	if req.Table != "machines" {
		t.Errorf("Table = %q, want %q", req.Table, "machines")
	}
	if len(req.Columns) != 0 {
		t.Errorf("Columns = %v, want empty", req.Columns)
	}
	if len(req.GroupBy) != 0 {
		t.Errorf("GroupBy = %v, want empty", req.GroupBy)
	}
	if req.Filters != nil {
		t.Errorf("Filters = %v, want nil", req.Filters)
	}
	if req.Computed != nil {
		t.Errorf("Computed = %v, want nil", req.Computed)
	}
	if req.Sort != nil {
		t.Errorf("Sort = %v, want nil", req.Sort)
	}
	if req.Aggregates != nil {
		t.Errorf("Aggregates = %v, want nil", req.Aggregates)
	}
	if req.GroupSorts != nil {
		t.Errorf("GroupSorts = %v, want nil", req.GroupSorts)
	}
	// The parser's default limit carries through as the viewport.
	wantViewport := engine.Viewport{Offset: 0, Limit: 25}
	if req.Viewport != wantViewport {
		t.Errorf("Viewport = %v, want %v", req.Viewport, wantViewport)
	}
	if len(req.Expanded) != 0 {
		t.Errorf("Expanded = %v, want empty", req.Expanded)
	}
}

// TestLimitZeroMeansUnlimited pins the "show all" semantics: limit=0 in the
// URL maps to Viewport.Limit 0, which the contract defines as no limit.
func TestLimitZeroMeansUnlimited(t *testing.T) {
	req := requestFromQuery(query.NewQuery(mustParse(t, "/table?table=machines&limit=0")))
	if req.Viewport.Limit != 0 {
		t.Errorf("Viewport.Limit = %d, want 0", req.Viewport.Limit)
	}
}
