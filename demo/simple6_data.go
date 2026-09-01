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

package demo

import (
	"fmt"
	"math"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// Simple6Rows is the row count of the simple6 table — the focused
// perf-validation target (six columns, 10^7 rows). Mirrors the benchsuite
// simple6 schema with the same seed and distributions, so numbers measured
// there correspond to what this table serves interactively.
const Simple6Rows = 10_000_000

const simple6Seed = 42

// simple6Hash is deterministic per-cell randomness: value = h(seed, tag, row).
func simple6Hash(tag, i uint64) uint64 {
	x := simple6Seed ^ tag*0x9E3779B97F4A7C15 ^ i*0xBF58476D1CE4E5B9
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return x
}

func simple6U01(v uint64) float64 { return float64(v>>11) / float64(1<<53) }

// CreateSimple6Table builds the simple6 table through the real load
// pipeline: chunked appends, one table sort by the primary key, then
// encoding selection (front-coded PK, dict dimensions, power-law entity
// key at d=10'000).
func CreateSimple6Table() *tables.DataTable {
	fmt.Printf("Creating simple6 table with %d rows (perf-validation target)...\n", Simple6Rows)

	def := func(name string) *columns.ColumnDef { return columns.NewColumnDef(name, name, "") }
	id := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "id", "bench.simple"))
	flavor := columns.NewChunkedStringColumn(def("flavor"))
	stage := columns.NewChunkedStringColumn(def("stage"))
	entity := columns.NewChunkedStringColumn(def("entity"))
	code := columns.NewChunkedStringColumn(def("code"))
	amount := columns.NewChunkedFloat64Column(def("amount"))

	flavors := []string{"apple", "berry", "citrus", "date", "elder"}
	stages := []string{"raw", "queued", "active", "done", "failed"}
	const dEntity = 10_000

	for i := 0; i < Simple6Rows; i++ {
		u := uint64(i)
		id.Append(fmt.Sprintf("k%09d", i))
		flavor.Append(flavors[simple6Hash(51, u)%5])
		stage.Append(stages[simple6Hash(52, u)%5])
		p := simple6U01(simple6Hash(53, u))
		entity.Append(fmt.Sprintf("e%05d", int(float64(dEntity)*p*p*p)))
		code.Append(fmt.Sprintf("c%03d", simple6Hash(54, u)%100))
		amount.Append(10 * math.Exp(2*simple6U01(simple6Hash(55, u))))
	}

	t := tables.NewDataTable()
	for _, c := range []columns.IDataColumn{id, flavor, stage, entity, code, amount} {
		t.AddColumn(c)
	}
	if err := t.SortByKey([]string{"id"}); err != nil {
		panic(err)
	}
	t.SelectEncodings()
	fmt.Printf("  simple6 ready: %d rows, sorted by id, encodings selected\n", t.Length())
	return t
}
