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
	"testing"

	"github.com/google/taxinomia/core/columns"
)

// BenchmarkGroupAggregates1M is the 6b end-to-end number: a full grouping
// request (level-0 partition + aggregates) on 1M rows, d=1000 dictionary
// dimension, two numeric measures — with leaf aggregates accumulated per row
// versus assembled from cached per-chunk summaries (steady state; the first
// bulk request additionally pays one summary build per measure, benchmarked
// separately in columns' BenchmarkPerCodeAggs).
func BenchmarkGroupAggregates1M(b *testing.B) {
	const n, d = 1 << 20, 1000
	build := func() *TableView {
		table := NewDataTable()
		dim := columns.NewChunkedDictStringColumn[uint16](columns.NewColumnDef("dim", "Dim", ""))
		score := columns.NewChunkedFloat64Column(columns.NewColumnDef("score", "Score", ""))
		amount := columns.NewChunkedUint32Column(columns.NewColumnDef("amount", "Amount", ""))
		runLen := n / d
		for i := 0; i < n; i++ {
			dim.Append(fmt.Sprintf("v%06d", (i/runLen)%d))
			score.Append(float64(i % 1000))
			amount.Append(uint32(i % 500))
		}
		dim.FinalizeColumn()
		score.FinalizeColumn()
		amount.FinalizeColumn()
		table.AddColumn(dim)
		table.AddColumn(score)
		table.AddColumn(amount)
		tv := NewTableView(table, "bench")
		tv.VisibleColumns = []string{"dim", "score", "amount"}
		return tv
	}
	request := func(tv *TableView) {
		tv.ClearGroupings()
		tv.GroupTable([]string{"dim"}, nil, make(map[string]Compare), map[string]bool{"dim": true})
	}

	b.Run("per-row", func(b *testing.B) {
		tv := build()
		disableBulkAggregates = true
		defer func() { disableBulkAggregates = false }()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			request(tv)
		}
	})

	b.Run("bulk", func(b *testing.B) {
		tv := build()
		request(tv) // warm the per-chunk summaries
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			request(tv)
		}
	})
}
