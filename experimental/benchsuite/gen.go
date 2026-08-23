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
	"fmt"
	"math"
	"time"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// Deterministic per-cell randomness: value = h(seed, column tag, row).
// Generation order therefore cannot change the data.
func h(seed, tag, i uint64) uint64 {
	x := seed ^ tag*0x9E3779B97F4A7C15 ^ i*0xBF58476D1CE4E5B9
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return x
}

func u01(v uint64) float64 { return float64(v>>11) / float64(1<<53) }

// zipfPick picks one of len(cum) values by cumulative permille thresholds.
func zipfPick(v uint64, cum []uint64) int {
	p := v % 1000
	for k, c := range cum {
		if p < c {
			return k
		}
	}
	return len(cum) - 1
}

var statusCum = []uint64{500, 750, 870, 930, 970, 1000} // zipf-ish over 6
var statusNames = []string{"ok", "pending", "shipped", "blocked", "failed", "void"}
var tierNames = []string{"bronze", "silver", "gold", "platinum"}
var kindNames = []string{"core", "edge", "lab", "shadow", "test"}

// buildPhases is the L1 result: wall time of each load-path phase.
type buildPhases struct {
	Append   time.Duration
	Sort     time.Duration
	Encode   time.Duration
	RowsPerS float64
}

const dimRowsMax = 10_000
const dim2Rows = 100

func dDim(n int) int {
	d := n / 10
	if d > dimRowsMax {
		d = dimRowsMax
	}
	if d < 1 {
		d = 1
	}
	return d
}

func dTag(n int) int {
	d := n / 100
	if d < 1 {
		d = 1
	}
	return d
}

// buildFact generates the full20 fact table at n rows and runs the load
// pipeline (sort by sortKey, then encoding selection). Column mix per
// the benchmark plan.
func buildFact(n int, seed uint64, sortKey []string) (*tables.DataTable, buildPhases) {
	var ph buildPhases
	t0 := time.Now()

	def := func(name, entity string) *columns.ColumnDef { return columns.NewColumnDef(name, name, entity) }

	id := columns.NewChunkedStringColumn(def("id", "bench.fact"))
	seq := columns.NewChunkedInt64Column(def("seq", ""))
	fkDim := columns.NewChunkedStringColumn(def("fk_dim", "bench.dim"))
	category := columns.NewChunkedStringColumn(def("category", ""))
	status := columns.NewChunkedStringColumn(def("status", ""))
	region := columns.NewChunkedStringColumn(def("region", ""))
	country := columns.NewChunkedStringColumn(def("country", ""))
	active := columns.NewChunkedBoolColumn(def("active", ""))
	flagRare := columns.NewChunkedBoolColumn(def("flag_rare", ""))
	amount := columns.NewChunkedFloat64Column(def("amount", ""))
	qty := columns.NewChunkedInt64Column(def("qty", ""))
	price := columns.NewChunkedFloat64Column(def("price", ""))
	score := columns.NewChunkedFloat64Column(def("score", ""))
	created := columns.NewChunkedDatetimeColumn(def("created", ""))
	updatedNS := columns.NewChunkedInt64Column(def("updated_ns", ""))
	label := columns.NewChunkedStringColumn(def("label", ""))
	device := columns.NewChunkedStringColumn(def("device", ""))
	bucket := columns.NewChunkedUint32Column(def("bucket", ""))
	note := columns.NewChunkedStringColumn(def("note", ""))
	tag := columns.NewChunkedStringColumn(def("tag", ""))

	dD := uint64(dDim(n))
	dT := uint64(dTag(n))
	dBucket := uint64(n / 10)
	if dBucket < 1 {
		dBucket = 1
	}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	for i := 0; i < n; i++ {
		u := uint64(i)
		id.Append(fmt.Sprintf("id%09d", i))
		seq.Append(int64(i))
		fkDim.Append(fmt.Sprintf("d%05d", h(seed, 3, u)%dD))
		category.Append(fmt.Sprintf("cat_%c", 'a'+byte(h(seed, 4, u)%8)))
		status.Append(statusNames[zipfPick(h(seed, 5, u), statusCum)])
		c := h(seed, 7, u) % 200
		country.Append(fmt.Sprintf("c%03d", c))
		region.Append(fmt.Sprintf("r%02d", c/10)) // country → region, functional
		active.Append(h(seed, 8, u)%2 == 0)
		flagRare.Append(h(seed, 9, u)%100 == 0)
		amount.Append(10 * math.Exp(2*u01(h(seed, 10, u))))
		qty.Append(int64(1 + h(seed, 11, u)%100))
		price.Append(float64(1+h(seed, 12, u)%1000) / 100)
		if h(seed, 13, u)%1000 == 0 {
			score.Append(math.NaN())
		} else {
			score.Append(u01(h(seed, 13, u)))
		}
		created.AppendUnix(base + int64(i) + int64(h(seed, 14, u)%120) - 60) // near-sorted
		updatedNS.Append(int64(h(seed, 15, u)%(2*365*24*3600))*1e9 + base*1e9)
		label.Append(fmt.Sprintf("lb%05d", h(seed, 16, u)%65536))
		device.Append(fmt.Sprintf("dev%04d", h(seed, 17, u)%1024))
		bucket.Append(uint32(h(seed, 18, u) % dBucket))
		note.Append(fmt.Sprintf("n%011x", h(seed, 19, u)&0xFFFFFFFFFFF))
		tag.Append(fmt.Sprintf("t%06d", h(seed, 20, u)%dT))
	}

	table := tables.NewDataTable()
	for _, c := range []columns.IDataColumn{
		id, seq, fkDim, category, status, region, country, active, flagRare,
		amount, qty, price, score, created, updatedNS, label, device, bucket, note, tag,
	} {
		table.AddColumn(c)
	}
	ph.Append = time.Since(t0)

	t1 := time.Now()
	if err := table.SortByKey(sortKey); err != nil {
		panic(err)
	}
	ph.Sort = time.Since(t1)

	t2 := time.Now()
	table.SelectEncodings()
	ph.Encode = time.Since(t2)

	ph.RowsPerS = float64(n) / time.Since(t0).Seconds()
	return table, ph
}

// buildDim builds the 10^4-row dimension table (keyed by id, entity
// bench.dim) with a parent FK into dim2 (entity bench.dim2).
func buildDim(seed uint64) *tables.DataTable {
	def := func(name, entity string) *columns.ColumnDef { return columns.NewColumnDef(name, name, entity) }
	id := columns.NewChunkedStringColumn(def("id", "bench.dim"))
	name := columns.NewChunkedStringColumn(def("name", ""))
	tier := columns.NewChunkedStringColumn(def("tier", ""))
	weight := columns.NewChunkedFloat64Column(def("weight", ""))
	parent := columns.NewChunkedStringColumn(def("parent", "bench.dim2"))
	for i := 0; i < dimRowsMax; i++ {
		u := uint64(i)
		id.Append(fmt.Sprintf("d%05d", i))
		name.Append(fmt.Sprintf("Dim %05d", i))
		tier.Append(tierNames[h(seed, 31, u)%4])
		weight.Append(u01(h(seed, 32, u)) * 100)
		parent.Append(fmt.Sprintf("p%03d", i%dim2Rows))
	}
	t := tables.NewDataTable()
	for _, c := range []columns.IDataColumn{id, name, tier, weight, parent} {
		t.AddColumn(c)
	}
	if err := t.SortByKey([]string{"id"}); err != nil {
		panic(err)
	}
	t.SelectEncodings()
	return t
}

func buildDim2(seed uint64) *tables.DataTable {
	def := func(name, entity string) *columns.ColumnDef { return columns.NewColumnDef(name, name, entity) }
	id := columns.NewChunkedStringColumn(def("id", "bench.dim2"))
	name := columns.NewChunkedStringColumn(def("name", ""))
	kind := columns.NewChunkedStringColumn(def("kind", ""))
	for i := 0; i < dim2Rows; i++ {
		id.Append(fmt.Sprintf("p%03d", i))
		name.Append(fmt.Sprintf("Parent %03d", i))
		kind.Append(kindNames[h(seed, 41, uint64(i))%5])
	}
	t := tables.NewDataTable()
	for _, c := range []columns.IDataColumn{id, name, kind} {
		t.AddColumn(c)
	}
	if err := t.SortByKey([]string{"id"}); err != nil {
		panic(err)
	}
	t.SelectEncodings()
	return t
}
