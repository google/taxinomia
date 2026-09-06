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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/taxinomia/core/columns"
)

// TestClientPathTiming measures the first grouping on a 5-value column of
// a simple6-shaped 10M-row table along the paths an embedding server can
// take, to explain why the same grouping costs ~20 ms through the table
// handler and over a second through some client pipelines. It only logs;
// run it with -v and without -short.
func TestClientPathTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("10M-row timing experiment; skipped in short mode")
	}
	const n = 10_000_000

	type variant struct {
		name    string
		table   string          // which table shape (built once, immutable, shared)
		needs   map[string]bool // nil = legacy (state for every visible column)
		eager   bool            // GroupTable (eager full tree) vs windowed with limit 25
		visible bool            // all 6 columns visible (VisibleColumns set, as the handler does)
	}
	builders := map[string]func() *DataTable{
		"encoded": func() *DataTable { tbl := simple6Like(n, true); tbl.SelectEncodings(); return tbl },
		"raw":     func() *DataTable { return simple6Like(n, true) },
		"plain":   func() *DataTable { return simple6Like(n, false) },
	}
	tables := map[string]*DataTable{}
	none := map[string]bool{}

	variants := []variant{
		{"encoded, needs{}, windowed, no visible columns", "encoded", none, false, false},
		{"encoded, needs nil, eager, no visible columns", "encoded", nil, true, false},
		{"HANDLER PATH: encoded, needs{}, windowed, 6 visible", "encoded", none, false, true},
		{"encoded, needs nil (legacy), eager, 6 visible", "encoded", nil, true, true},
		{"encoded, needs nil (legacy), windowed, 6 visible", "encoded", nil, false, true},
		{"NO SelectEncodings (raw chunked), needs{}, windowed, 6 vis", "raw", none, false, true},
		{"NO SelectEncodings (raw chunked), needs nil, eager, 6 vis", "raw", nil, true, true},
		{"plain StringColumn (unchunked), needs{}, windowed, 6 vis", "plain", none, false, true},
		{"plain StringColumn (unchunked), needs nil, eager, 6 vis", "plain", nil, true, true},
	}
	for _, v := range variants {
		tbl, ok := tables[v.table]
		if !ok {
			start := time.Now()
			tbl = builders[v.table]()
			tables[v.table] = tbl
			t.Logf("built %s table in %.1fs", v.table, time.Since(start).Seconds())
		}
		tv := NewTableView(tbl, "simple6")
		if v.visible {
			tv.VisibleColumns = []string{"id", "flavor", "stage", "entity", "code", "amount"}
		}
		tv.SetAggregateNeeds(v.needs)
		start := time.Now()
		if v.eager {
			tv.GroupTable([]string{"flavor"}, []string{}, map[string]Compare{}, map[string]bool{})
		} else {
			tv.GroupTableWindowed([]string{"flavor"}, []string{}, map[string]Compare{}, map[string]bool{}, 25, GroupExpansion{ExpandAll: true})
		}
		first := time.Since(start)
		// Same grouping again on the cached view (what a repeated request sees).
		tv.ClearGroupings()
		start = time.Now()
		if v.eager {
			tv.GroupTable([]string{"flavor"}, []string{}, map[string]Compare{}, map[string]bool{})
		} else {
			tv.GroupTableWindowed([]string{"flavor"}, []string{}, map[string]Compare{}, map[string]bool{}, 25, GroupExpansion{ExpandAll: true})
		}
		second := time.Since(start)
		t.Logf("%-62s flavor is %-40T first %8.1f ms  regroup %8.1f ms", v.name, tbl.GetColumn("flavor"), float64(first.Microseconds())/1000, float64(second.Microseconds())/1000)
	}
}

// simple6Like mirrors demo.CreateSimple6Table's shape (same distributions,
// deterministic hash generator) without importing demo. chunked=false
// builds the columns as unchunked StringColumn/Float64Column, the shape
// of a table assembled row by row by older client code.
func simple6Like(n int, chunked bool) *DataTable {
	hash := func(tag, i uint64) uint64 {
		x := (tag<<32 ^ i) * 0x9E3779B97F4A7C15
		x ^= x >> 29
		x *= 0xBF58476D1CE4E5B9
		x ^= x >> 32
		return x
	}
	u01 := func(v uint64) float64 { return float64(v>>11) / float64(1<<53) }
	def := func(name string) *columns.ColumnDef { return columns.NewColumnDef(name, name, "") }
	flavors := []string{"apple", "berry", "citrus", "date", "elder"}
	stages := []string{"raw", "queued", "active", "done", "failed"}

	var id, flavor, stage, entity, code interface {
		columns.IDataColumn
		Append(string)
	}
	var amount interface {
		columns.IDataColumn
		Append(float64)
	}
	if chunked {
		id, flavor, stage, entity, code = columns.NewChunkedStringColumn(def("id")), columns.NewChunkedStringColumn(def("flavor")), columns.NewChunkedStringColumn(def("stage")), columns.NewChunkedStringColumn(def("entity")), columns.NewChunkedStringColumn(def("code"))
		amount = columns.NewChunkedFloat64Column(def("amount"))
	} else {
		id, flavor, stage, entity, code = columns.NewStringColumn(def("id")), columns.NewStringColumn(def("flavor")), columns.NewStringColumn(def("stage")), columns.NewStringColumn(def("entity")), columns.NewStringColumn(def("code"))
		amount = columns.NewFloat64Column(def("amount"))
	}
	for i := 0; i < n; i++ {
		u := uint64(i)
		id.Append(fmt.Sprintf("k%09d", i))
		flavor.Append(flavors[hash(51, u)%5])
		stage.Append(stages[hash(52, u)%5])
		p := u01(hash(53, u))
		entity.Append(fmt.Sprintf("e%05d", int(10000*p*p*p)))
		code.Append(fmt.Sprintf("c%03d", hash(54, u)%100))
		amount.Append(10 * math.Exp(2*u01(hash(55, u))))
	}
	tbl := NewDataTable()
	for _, c := range []columns.IDataColumn{id, flavor, stage, entity, code, amount} {
		if f, ok := c.(interface{ FinalizeColumn() }); ok {
			f.FinalizeColumn()
		}
		tbl.AddColumn(c)
	}
	return tbl
}
