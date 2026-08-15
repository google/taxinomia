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

package columns

import (
	"context"
	"fmt"
	"testing"
)

// benchPreAggData builds the 6b benchmark shape: 1M rows, a dictionary
// dimension with d=1000 values in runs (the sorted-storage shape), and a
// float64 measure.
func benchPreAggData(n, d int) (*ChunkedDictStringColumn[uint16], *ChunkedFloat64Column) {
	dim := NewChunkedDictStringColumn[uint16](NewColumnDef("dim", "Dim", ""))
	measure := NewChunkedFloat64Column(NewColumnDef("m", "M", ""))
	runLen := n / d
	for i := 0; i < n; i++ {
		dim.Append(fmt.Sprintf("v%06d", (i/runLen)%d))
		measure.Append(float64(i % 1000))
	}
	dim.FinalizeColumn()
	measure.FinalizeColumn()
	return dim, measure
}

// BenchmarkPerCodeAggs compares the level-0 aggregate pass shapes at 1M rows,
// d=1000: the per-row reference (what computeLeafAggregates costs), the first
// PerCodeAggs call (summary build + merge), and the steady state (merge of
// cached chunk summaries).
func BenchmarkPerCodeAggs(b *testing.B) {
	const n, d = 1 << 20, 1000

	b.Run("per-row reference", func(b *testing.B) {
		dim, measure := benchPreAggData(n, d)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			out := make([]NumericCodeAgg, dim.Cardinality())
			for c := range out {
				out[c] = initNumericCodeAgg()
			}
			AllRows(n).ForEachRow(func(row uint32) bool {
				v, _ := measure.GetValue(row)
				out[dim.GetCode(row)].add(v)
				return true
			})
		}
	})

	b.Run("first call (build+merge)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dim, measure := benchPreAggData(n, d)
			b.StartTimer()
			if _, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), measure); !handled || err != nil {
				b.Fatalf("handled=%v err=%v", handled, err)
			}
		}
	})

	b.Run("warm merge", func(b *testing.B) {
		dim, measure := benchPreAggData(n, d)
		if _, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), measure); !handled || err != nil {
			b.Fatalf("handled=%v err=%v", handled, err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), measure); !handled || err != nil {
				b.Fatalf("handled=%v err=%v", handled, err)
			}
		}
	})
}
