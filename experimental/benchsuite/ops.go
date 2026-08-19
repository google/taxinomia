/*
SPDX-License-Identifier: Apache-2.0

Copyright 2026 The Taxinomia Authors

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
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/expr"
	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/queryspec"
	"github.com/google/taxinomia/core/tables"
	"github.com/google/taxinomia/web/urlquery"
)

// suite holds the built dataset and run parameters shared by all ops.
type suite struct {
	n     int
	seed  uint64
	iters int
	dm    *models.DataModel
	fact  *tables.DataTable
	build buildPhases
}

// opResult is one (operation × scale) cell.
type opResult struct {
	Op      string  `json:"op"`
	MedianS float64 `json:"median_s"`
	ColdS   float64 `json:"cold_s,omitempty"` // first run, where it differs (memo/summary builds)
	Note    string  `json:"note,omitempty"`
}

func median(ds []time.Duration) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(a, b int) bool { return s[a] < s[b] })
	return s[len(s)/2]
}

// measure runs f iters times and returns per-run durations. Fast ops are
// batched (testing.B style) until one sample spans ≥2ms, so per-op times stay
// meaningful under the platform timer granularity.
func measure(iters int, f func()) []time.Duration {
	b := 1
	var first time.Duration
	for {
		t0 := time.Now()
		for j := 0; j < b; j++ {
			f()
		}
		first = time.Since(t0)
		if first >= 2*time.Millisecond || b >= 1<<20 {
			break
		}
		b *= 4
	}
	ds := make([]time.Duration, iters)
	ds[0] = first / time.Duration(b)
	for k := 1; k < iters; k++ {
		t0 := time.Now()
		for j := 0; j < b; j++ {
			f()
		}
		ds[k] = time.Since(t0) / time.Duration(b)
	}
	return ds
}

func (s *suite) view(visible ...string) *tables.TableView {
	tv := tables.NewTableView(s.fact, "fact")
	tv.VisibleColumns = visible
	if hasJoined(visible) {
		tv.UpdateJoinedColumns(visible, s.dm)
	}
	return tv
}

func hasJoined(cols []string) bool {
	for _, c := range cols {
		if n := len(splitDots(c)); n >= 4 && (n-1)%3 == 0 {
			return true
		}
	}
	return false
}

func splitDots(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

var noCompare = make(map[string]tables.Compare)
var noAsc = make(map[string]bool)
var expandAll = tables.GroupExpansion{ExpandAll: true}

func group(tv *tables.TableView, cols []string, limit int, exp tables.GroupExpansion) {
	tv.ClearGroupings()
	if err := tv.GroupTableWindowedContext(context.Background(), cols, nil, noCompare, noAsc, limit, exp); err != nil {
		panic(err)
	}
}

// addComputed compiles an expression and registers it as a computed column —
// a minimal reimplementation of the handler's createComputedColumn (which
// lives in the proto-dependent web/handlers package this module cannot
// import). Numeric, bool and string results only.
func addComputed(tv *tables.TableView, name, expression string) error {
	compiled, err := expr.Compile(expression)
	if err != nil {
		return err
	}
	getColumn := func(colName string, row uint32) (expr.Value, error) {
		col := tv.GetColumn(colName)
		if col == nil {
			return expr.NilValue(), fmt.Errorf("column %q not found", colName)
		}
		sv, err := col.GetString(row)
		if err != nil {
			return expr.NilValue(), err
		}
		if iv, err := strconv.ParseInt(sv, 10, 64); err == nil {
			return expr.NewInt(iv), nil
		}
		if fv, err := strconv.ParseFloat(sv, 64); err == nil {
			return expr.NewFloat(fv), nil
		}
		if sv == "true" || sv == "false" {
			return expr.NewBool(sv == "true"), nil
		}
		return expr.NewString(sv), nil
	}
	bound := compiled.Bind(getColumn)
	length := tv.GetColumn(tv.GetColumnNames()[0]).Length()
	colDef := columns.NewColumnDef(name, name, "")
	sample, err := bound.Eval(0)
	if err != nil {
		return err
	}
	switch {
	case sample.IsInt():
		tv.AddComputedColumn(name, columns.NewComputedInt64Column(colDef, length, func(i uint32) (int64, error) {
			v, err := bound.Eval(i)
			if err != nil {
				return 0, err
			}
			return v.AsInt(), nil
		}))
	case sample.IsFloat():
		tv.AddComputedColumn(name, columns.NewComputedFloat64Column(colDef, length, func(i uint32) (float64, error) {
			v, err := bound.Eval(i)
			if err != nil {
				return 0, err
			}
			return v.AsFloat(), nil
		}))
	case sample.IsBool():
		tv.AddComputedColumn(name, columns.NewComputedBoolColumn(colDef, length, func(i uint32) (bool, error) {
			v, err := bound.Eval(i)
			if err != nil {
				return false, err
			}
			return v.AsBool(), nil
		}))
	default:
		tv.AddComputedColumn(name, columns.NewComputedStringColumn(colDef, length, func(i uint32) (string, error) {
			v, err := bound.Eval(i)
			if err != nil {
				return "", err
			}
			return v.AsString(), nil
		}))
	}
	return nil
}

// request executes one URL against a view, mirroring the handler sequence
// (web/handlers/server.go handleTable): joined columns, computed columns,
// filters, grouping (with the aggregate-sort-aware effective limit),
// aggregate group sort, then the leaf listing when ungrouped. HTTP transport,
// auth and HTML templating are excluded (this module cannot import the
// proto-dependent handler package); those are per-request constants.
func request(dm *models.DataModel, tv *tables.TableView, rawURL string) {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	q := urlquery.NewQuery(u)
	if len(q.Columns) > 0 {
		tv.VisibleColumns = q.Columns
		if hasJoined(q.Columns) {
			tv.UpdateJoinedColumns(q.Columns, dm)
		}
	}
	for _, comp := range q.ComputedColumns {
		if tv.GetColumn(comp.Name) == nil {
			if err := addComputed(tv, comp.Name, comp.Expression); err != nil {
				panic(fmt.Sprintf("computed %s: %v", comp.Name, err))
			}
		}
	}
	if err := tv.ApplyFiltersContext(context.Background(), q.Filters); err != nil {
		panic(err)
	}
	if len(q.GroupedColumns) > 0 {
		asc := make(map[string]bool)
		for _, col := range q.GroupedColumns {
			asc[col] = true
			for _, sc := range q.SortOrder {
				if sc.Name == col {
					asc[col] = !sc.Descending
				}
			}
		}
		exp := tables.GroupExpansion{ExpandAll: !q.HasExpandedGroups, Paths: q.ExpandedGroups}
		if err := tv.GroupTableWindowedContext(context.Background(), q.GroupedColumns, nil, noCompare, asc, q.EffectiveGroupDisplayLimit(), exp); err != nil {
			panic(err)
		}
		if len(q.GroupAggregateSorts) > 0 {
			aggSorts := make(map[string]*queryspec.GroupAggSort, len(q.GroupAggregateSorts))
			for k, v := range q.GroupAggregateSorts {
				aggSorts[k] = v
			}
			tv.SortGroupsByAggregate(aggSorts)
		}
	} else {
		tv.ClearGroupings()
		_ = tv.GetFilteredRowsSorted(q.Columns, q.SortOrder, q.Limit)
	}
}

// ---- individual ops ----

type op struct {
	name string
	run  func(s *suite) opResult
}

var allOps = []op{
	{"Q1", q1}, {"Q2", q2}, {"Q3", q3}, {"Q4", q4}, {"Q5", q5}, {"Q6", q6},
	{"Q7", q7}, {"Q8", q8}, {"Q9", q9}, {"Q10", q10}, {"Q11", q11}, {"Q12", q12},
	{"Q13", q13}, {"Q14", q14}, {"Q15", q15}, {"Q16", q16}, {"Q17", q17},
	{"C1", c1}, {"C2", c2}, {"C3", c3}, {"C4", c4}, {"C5", c5},
}

func q1(s *suite) opResult {
	col, ok := s.fact.GetColumn("id").(interface {
		GetIndex(string) (uint32, error)
	})
	if !ok {
		return opResult{Op: "Q1", Note: "SKIP: id column has no GetIndex"}
	}
	const lookups = 10_000
	ds := measure(s.iters, func() {
		for k := 0; k < lookups; k++ {
			id := fmt.Sprintf("id%09d", int(h(s.seed, 90, uint64(k))%uint64(s.n)))
			if _, err := col.GetIndex(id); err != nil {
				panic(err)
			}
		}
	})
	return opResult{Op: "Q1", MedianS: (median(ds) / lookups).Seconds(), Note: "per lookup"}
}

func q2(s *suite) opResult {
	k := 0
	ds := measure(s.iters, func() {
		tv := s.view("country", "amount")
		tv.ApplyFilters(map[string]string{"country": fmt.Sprintf("%q", fmt.Sprintf("c%03d", k%200))})
		_ = tv.GetFilteredRowCount()
		k++
	})
	return opResult{Op: "Q2", MedianS: median(ds).Seconds(), Note: "equality on dict column"}
}

func q3(s *suite) opResult {
	col := s.fact.GetColumn("seq")
	rc, ok := col.(interface {
		FilterSelectionRange(lo, hi *int64) *columns.Selection
	})
	if !ok {
		return opResult{Op: "Q3", Note: "SKIP: seq column has no FilterSelectionRange"}
	}
	window := int64(s.n / 100)
	k := int64(0)
	ds := measure(s.iters, func() {
		lo := (k * 37) % int64(s.n-1)
		hi := lo + window
		_ = rc.FilterSelectionRange(&lo, &hi)
		k++
	})
	return opResult{Op: "Q3", MedianS: median(ds).Seconds(), Note: "1% range via column API (no URL syntax for ranges)"}
}

func q4(s *suite) opResult {
	k := 0
	ds := measure(s.iters, func() {
		tv := s.view("note", "amount")
		tv.ApplyFilters(map[string]string{"note": fmt.Sprintf("%02x", k%256)})
		_ = tv.GetFilteredRowCount()
		k++
	})
	return opResult{Op: "Q4", MedianS: median(ds).Seconds(), Note: "opaque substring scan"}
}

func groupOp(s *suite, name, groupCol, leaf, note string) opResult {
	tv := s.view(groupCol, leaf)
	ds := measure(s.iters+1, func() { group(tv, []string{groupCol}, 0, expandAll) })
	return opResult{Op: name, MedianS: median(ds[1:]).Seconds(), ColdS: ds[0].Seconds(), Note: note}
}

func q5(s *suite) opResult {
	return groupOp(s, "Q5", "category", "amount", "dense d=8; cold includes per-chunk pre-agg build")
}

func q6(s *suite) opResult {
	return groupOp(s, "Q6", "bucket", "amount", "hash path d=n/10")
}

func q7(s *suite) opResult {
	// Cold: fresh joined column (memo rebuilt). Warm: memo reused.
	cold := measure(s.iters, func() {
		tv := s.view("fk_dim.dim.id.name", "amount")
		group(tv, []string{"fk_dim.dim.id.name"}, 0, expandAll)
	})
	tv := s.view("fk_dim.dim.id.name", "amount")
	group(tv, []string{"fk_dim.dim.id.name"}, 0, expandAll)
	warm := measure(s.iters, func() { group(tv, []string{"fk_dim.dim.id.name"}, 0, expandAll) })
	return opResult{Op: "Q7", MedianS: median(warm).Seconds(), ColdS: median(cold).Seconds(), Note: "group by joined; cold rebuilds join memo"}
}

func q8(s *suite) opResult {
	tv := s.view("country", "amount")
	aggSorts := map[string]*queryspec.GroupAggSort{
		"country": {GroupedColumn: "country", LeafColumn: "amount", AggType: queryspec.AggSum, Descending: true},
	}
	ds := measure(s.iters, func() {
		group(tv, []string{"country"}, 0, expandAll)
		tv.SortGroupsByAggregate(aggSorts)
	})
	return opResult{Op: "Q8", MedianS: median(ds).Seconds(), Note: "untrimmed build + aggregate sort (top-25 read at render)"}
}

func q9(s *suite) opResult {
	tv := s.view("id", "amount", "country")
	sortOrder := []queryspec.SortColumn{{Name: "amount", Descending: true}}
	ds := measure(s.iters, func() {
		_ = tv.GetFilteredRowsSorted([]string{"id", "amount", "country"}, sortOrder, 25)
	})
	return opResult{Op: "Q9", MedianS: median(ds).Seconds(), Note: "leaf top-25 heap"}
}

const q10URL = "/table?table=fact&columns=country,category,amount&filter:country=%22c042%22&grouped=category&limit=25"

func q10(s *suite) opResult {
	tv := s.view("country", "category", "amount")
	one := measure(s.iters, func() { request(s.dm, tv, q10URL) ; tv.ClearGroupings(); tv.ApplyFilters(nil) })

	views := make([]*tables.TableView, 8)
	for i := range views {
		views[i] = s.view("country", "category", "amount")
	}
	conc := measure(s.iters, func() {
		var wg sync.WaitGroup
		for _, v := range views {
			wg.Add(1)
			go func(v *tables.TableView) {
				defer wg.Done()
				request(s.dm, v, q10URL)
				v.ClearGroupings()
				v.ApplyFilters(nil)
			}(v)
		}
		wg.Wait()
	})
	return opResult{Op: "Q10", MedianS: median(one).Seconds(), ColdS: median(conc).Seconds(),
		Note: "pipeline sans HTTP/HTML; cold_s column = 8 concurrent requests wall"}
}

func q11(s *suite) opResult {
	tv := s.view("country", "status", "amount")
	if err := addComputed(tv, "pair", `country + "·" + status`); err != nil {
		panic(err)
	}
	ds := measure(s.iters, func() { group(tv, []string{"pair"}, 0, expandAll) })
	return opResult{Op: "Q11", MedianS: median(ds).Seconds(), Note: "group by computed key (interpreter path)"}
}

func q12(s *suite) opResult {
	tv := s.view("country", "category", "amount")
	f := map[string]string{"country": `"c042"`}
	tv.ApplyFilters(f)
	group(tv, []string{"category"}, 0, expandAll)
	ds := measure(s.iters, func() {
		tv.ApplyFilters(f) // identical → cached
		if err := tv.GroupTableWindowedContext(context.Background(), []string{"category"}, nil, noCompare, noAsc, 0, expandAll); err != nil {
			panic(err)
		}
	})
	return opResult{Op: "Q12", MedianS: median(ds).Seconds(), Note: "warm re-issue (filter+grouping caches)"}
}

func q13(s *suite) opResult {
	cold := measure(s.iters, func() {
		tv := s.view("fk_dim.dim.id.tier", "amount")
		tv.ApplyFilters(map[string]string{"fk_dim.dim.id.tier": `"gold"`})
		_ = tv.GetFilteredRowCount()
	})
	tv := s.view("fk_dim.dim.id.tier", "amount")
	k := 0
	warm := measure(s.iters, func() {
		tv.ApplyFilters(map[string]string{"fk_dim.dim.id.tier": fmt.Sprintf("%q", tierNames[k%4])})
		_ = tv.GetFilteredRowCount()
		k++
	})
	return opResult{Op: "Q13", MedianS: median(warm).Seconds(), ColdS: median(cold).Seconds(), Note: "filter through join (per-row; memo on warm)"}
}

func q14(s *suite) opResult {
	cols := []string{"id", "fk_dim.dim.id.name", "fk_dim.dim.id.tier", "fk_dim.dim.id.weight", "amount"}
	tv := s.view(cols...)
	sortOrder := []queryspec.SortColumn{{Name: "amount", Descending: true}}
	ds := measure(s.iters, func() { _ = tv.GetFilteredRowsSorted(cols, sortOrder, 25) })
	return opResult{Op: "Q14", MedianS: median(ds).Seconds(), Note: "top-25 listing with 3 joined columns"}
}

func q15(s *suite) opResult {
	const chain = "fk_dim.dim.id.parent.dim2.id.name"
	cold := measure(s.iters, func() {
		tv := s.view(chain, "amount")
		group(tv, []string{chain}, 0, expandAll)
	})
	tv := s.view(chain, "amount")
	group(tv, []string{chain}, 0, expandAll)
	warm := measure(s.iters, func() { group(tv, []string{chain}, 0, expandAll) })
	return opResult{Op: "Q15", MedianS: median(warm).Seconds(), ColdS: median(cold).Seconds(), Note: "two-hop chained join"}
}

func q16(s *suite) opResult {
	tv := s.view("country", "category", "amount")
	ds := measure(s.iters, func() { group(tv, []string{"country", "category"}, 0, expandAll) })
	return opResult{Op: "Q16", MedianS: median(ds).Seconds(), Note: "2-level eager (≤1600 leaves)"}
}

func q17(s *suite) opResult {
	k := 0
	level0 := measure(s.iters, func() {
		tv := s.view("country", "category", "amount")
		group(tv, []string{"country", "category"}, 0, tables.GroupExpansion{})
		k++
	})
	expandOne := measure(s.iters, func() {
		tv := s.view("country", "category", "amount")
		group(tv, []string{"country", "category"}, 0, tables.GroupExpansion{})
		path := fmt.Sprintf("c%03d", k%200)
		if err := tv.GroupTableWindowedContext(context.Background(), []string{"country", "category"}, nil, noCompare, noAsc, 0,
			tables.GroupExpansion{Paths: [][]string{{path}}}); err != nil {
			panic(err)
		}
		k++
	})
	return opResult{Op: "Q17", MedianS: median(level0).Seconds(), ColdS: median(expandOne).Seconds(),
		Note: "lazy level-0 only; cold_s column = level-0 + one expansion"}
}

// c1Filters: five mixed filters. The plan's seq-range is not URL-expressible
// (no range filter syntax); an opaque substring on note stands in as the
// fifth, keeping the mix structured+multi+bool+joined+opaque.
func c1Filters(k int) map[string]string {
	return map[string]string{
		"country":            fmt.Sprintf("%q", fmt.Sprintf("c%03d", k%200)),
		"status":             "ok|pending",
		"active":             `"true"`,
		"fk_dim.dim.id.tier": `"gold"`,
		"note":               fmt.Sprintf("%02x", k%256),
	}
}

func c1(s *suite) opResult {
	tv := s.view("id", "country", "status", "active", "fk_dim.dim.id.tier", "note", "amount")
	sortOrder := []queryspec.SortColumn{{Name: "amount", Descending: true}}
	k := 0
	ds := measure(s.iters, func() {
		tv.ApplyFilters(c1Filters(k))
		_ = tv.GetFilteredRowsSorted([]string{"id", "amount"}, sortOrder, 25)
		k++
	})
	return opResult{Op: "C1", MedianS: median(ds).Seconds(), Note: "five-filter listing"}
}

func c2(s *suite) opResult {
	tv := s.view("fk_dim.dim.id.name", "category", "country", "status", "active", "note", "fk_dim.dim.id.tier", "amount", "price")
	aggSorts := map[string]*queryspec.GroupAggSort{
		"fk_dim.dim.id.name": {GroupedColumn: "fk_dim.dim.id.name", LeafColumn: "amount", AggType: queryspec.AggSum, Descending: true},
	}
	k := 0
	ds := measure(s.iters, func() {
		tv.ApplyFilters(c1Filters(k))
		group(tv, []string{"fk_dim.dim.id.name", "category"}, 0, expandAll)
		tv.SortGroupsByAggregate(aggSorts)
		k++
	})
	return opResult{Op: "C2", MedianS: median(ds).Seconds(), Note: "filters + join-grouped 2-level + agg sort"}
}

func c3(s *suite) opResult {
	tv := s.view("country", "status", "amount", "price", "qty")
	if err := addComputed(tv, "margin", `(amount - price * qty) / amount`); err != nil {
		panic(err)
	}
	if err := addComputed(tv, "hi_margin", `(amount - price * qty) / amount > 0.2`); err != nil {
		panic(err)
	}
	if err := addComputed(tv, "pair", `country + "·" + status`); err != nil {
		panic(err)
	}
	tv.VisibleColumns = []string{"pair", "margin", "country", "status"}
	ds := measure(s.iters, func() {
		tv.ApplyFilters(map[string]string{"hi_margin": `"true"`})
		group(tv, []string{"pair"}, 0, expandAll)
		tv.ApplyFilters(nil)
	})
	return opResult{Op: "C3", MedianS: median(ds).Seconds(), Note: "computed as filter+group-key+measure"}
}

func c4(s *suite) opResult {
	mk := func() *tables.TableView {
		tv := s.view("fk_dim.dim.id.name", "category", "status", "country", "active", "note", "fk_dim.dim.id.tier", "amount", "price", "qty")
		if err := addComputed(tv, "hi_margin", `(amount - price * qty) / amount > 0.2`); err != nil {
			panic(err)
		}
		return tv
	}
	aggSorts := map[string]*queryspec.GroupAggSort{
		"fk_dim.dim.id.name": {GroupedColumn: "fk_dim.dim.id.name", LeafColumn: "amount", AggType: queryspec.AggSum, Descending: true},
	}
	runOnce := func(tv *tables.TableView, k int) {
		f := c1Filters(k)
		f["hi_margin"] = `"true"`
		tv.ApplyFilters(f)
		tv.ClearGroupings()
		if err := tv.GroupTableWindowedContext(context.Background(), []string{"fk_dim.dim.id.name", "category", "status"}, nil, noCompare, noAsc, 0,
			tables.GroupExpansion{Paths: [][]string{{"Dim 00042"}}}); err != nil {
			panic(err)
		}
		tv.SortGroupsByAggregate(aggSorts)
	}
	tv := mk()
	k := 0
	one := measure(s.iters, func() { runOnce(tv, k); k++ })
	views := make([]*tables.TableView, 8)
	for i := range views {
		views[i] = mk()
	}
	conc := measure(s.iters, func() {
		var wg sync.WaitGroup
		for i, v := range views {
			wg.Add(1)
			go func(v *tables.TableView, i int) {
				defer wg.Done()
				runOnce(v, k+i)
			}(v, i)
		}
		wg.Wait()
		k++
	})
	return opResult{Op: "C4", MedianS: median(one).Seconds(), ColdS: median(conc).Seconds(),
		Note: "kitchen sink; cold_s column = 8 concurrent wall"}
}

func c5(s *suite) opResult {
	steps := []string{
		"/table?table=fact&columns=id,country,category,amount&limit=25",
		"/table?table=fact&columns=id,country,category,amount&limit=25&filter:country=%22c042%22",
		"/table?table=fact&columns=country,category,amount&limit=25&filter:country=%22c042%22&grouped=category",
		"/table?table=fact&columns=country,category,status,amount&limit=25&filter:country=%22c042%22&filter:category=%22cat_c%22&grouped=status",
		"/table?table=fact&columns=country,category,status,amount&limit=25&filter:country=%22c042%22&filter:category=%22cat_c%22&grouped=status&groupsort:status=-amount:sum",
		"/table?table=fact&columns=country,category,status,amount,margin&limit=25&filter:country=%22c042%22&filter:category=%22cat_c%22&grouped=status&groupsort:status=-amount:sum&computed=margin%3D(amount%20-%20price%20*%20qty)%20%2F%20amount",
	}
	// The headline number is the whole six-step sequence on a fresh view
	// (batched, so it stays meaningful under timer granularity); one
	// unbatched pass provides the per-step split.
	total := measure(s.iters, func() {
		tv := s.view("id", "country", "category", "amount")
		for _, u := range steps {
			request(s.dm, tv, u)
		}
	})
	tv := s.view("id", "country", "category", "amount")
	note := "steps:"
	for si, u := range steps {
		t0 := time.Now()
		request(s.dm, tv, u)
		note += fmt.Sprintf(" s%d=%.1fms", si+1, time.Since(t0).Seconds()*1000)
	}
	return opResult{Op: "C5", MedianS: median(total).Seconds(), Note: note}
}
