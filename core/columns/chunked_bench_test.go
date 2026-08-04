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
	"fmt"
	"testing"
)

// Chunked-versus-flat benchmarks: the price of (chunkID, offset) addressing
// on build, filter and group scans at 1M rows. Run with:
//
//	go test ./core/columns/ -run '^$' -bench 'BenchmarkChunked' -benchtime 5x

const chunkedBenchRows = 1_000_000

func buildInt64Flat(n int) *Int64Column {
	c := NewInt64Column(NewColumnDef("v", "V", ""))
	for i := 0; i < n; i++ {
		c.Append(int64(i % 1000))
	}
	c.FinalizeColumn()
	return c
}

func buildInt64Chunked(n int) *ChunkedInt64Column {
	c := NewChunkedInt64Column(NewColumnDef("v", "V", ""))
	for i := 0; i < n; i++ {
		c.Append(int64(i % 1000))
	}
	c.FinalizeColumn()
	return c
}

func buildDictFlat(n, distinct int) *DictStringColumn[uint16] {
	c := NewDictStringColumn[uint16](NewColumnDef("d", "D", ""))
	for i := 0; i < n; i++ {
		c.Append(fmt.Sprintf("v%03d", i%distinct))
	}
	c.FinalizeColumn()
	return c
}

func buildDictChunked(n, distinct int) *ChunkedDictStringColumn[uint16] {
	c := NewChunkedDictStringColumn[uint16](NewColumnDef("d", "D", ""))
	for i := 0; i < n; i++ {
		c.Append(fmt.Sprintf("v%03d", i%distinct))
	}
	c.FinalizeColumn()
	return c
}

func BenchmarkChunkedInt64_Build_Flat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buildInt64Flat(chunkedBenchRows)
	}
}

func BenchmarkChunkedInt64_Build_Chunked(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buildInt64Chunked(chunkedBenchRows)
	}
}

func BenchmarkChunkedInt64_FilterSelection_Flat(b *testing.B) {
	c := buildInt64Flat(chunkedBenchRows)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelection(func(v int64) bool { return v < 500 })
	}
}

func BenchmarkChunkedInt64_FilterSelection_Chunked(b *testing.B) {
	c := buildInt64Chunked(chunkedBenchRows)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelection(func(v int64) bool { return v < 500 })
	}
}

func BenchmarkChunkedDict_GroupCounts_Flat(b *testing.B) {
	c := buildDictFlat(chunkedBenchRows, 100)
	sel := AllRows(chunkedBenchRows)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.GroupCounts(sel)
	}
}

func BenchmarkChunkedDict_GroupCounts_Chunked(b *testing.B) {
	c := buildDictChunked(chunkedBenchRows, 100)
	sel := AllRows(chunkedBenchRows)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.GroupCounts(sel)
	}
}

// Zone-map pruning benchmarks (phase 3b): selective filters on data sorted
// by the column, the layout the design targets (docs/scaling-to-1b-rows.md
// §5). Naive = predicate scan of every chunk; pruned = zone maps skip the
// chunks that cannot match. Unsorted is the honest worst case: every chunk's
// bounds admit the target, so pruning saves nothing and only costs the
// per-chunk bound check.

func buildInt64ChunkedSorted(n int) *ChunkedInt64Column {
	c := NewChunkedInt64Column(NewColumnDef("v", "V", ""))
	for i := 0; i < n; i++ {
		c.Append(int64(i))
	}
	c.FinalizeColumn()
	return c
}

func buildDictChunkedSorted(n, distinct int) *ChunkedDictStringColumn[uint16] {
	c := NewChunkedDictStringColumn[uint16](NewColumnDef("d", "D", ""))
	for i := 0; i < n; i++ {
		c.Append(fmt.Sprintf("v%03d", i/(n/distinct)))
	}
	c.FinalizeColumn()
	return c
}

func BenchmarkZoneMapInt64_EqualSorted_Naive(b *testing.B) {
	c := buildInt64ChunkedSorted(chunkedBenchRows)
	target := int64(chunkedBenchRows / 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelection(func(v int64) bool { return v == target })
	}
}

func BenchmarkZoneMapInt64_EqualSorted_Pruned(b *testing.B) {
	c := buildInt64ChunkedSorted(chunkedBenchRows)
	target := int64(chunkedBenchRows / 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelectionEqual(target)
	}
}

func BenchmarkZoneMapInt64_RangeSorted_Naive(b *testing.B) {
	c := buildInt64ChunkedSorted(chunkedBenchRows)
	lo, hi := int64(chunkedBenchRows/2), int64(chunkedBenchRows/2+10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelection(func(v int64) bool { return v >= lo && v <= hi })
	}
}

func BenchmarkZoneMapInt64_RangeSorted_Pruned(b *testing.B) {
	c := buildInt64ChunkedSorted(chunkedBenchRows)
	lo, hi := int64(chunkedBenchRows/2), int64(chunkedBenchRows/2+10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelectionRange(&lo, &hi)
	}
}

func BenchmarkZoneMapInt64_EqualUnsorted_Naive(b *testing.B) {
	c := buildInt64Chunked(chunkedBenchRows) // i % 1000: every chunk holds every value
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelection(func(v int64) bool { return v == 500 })
	}
}

func BenchmarkZoneMapInt64_EqualUnsorted_Pruned(b *testing.B) {
	c := buildInt64Chunked(chunkedBenchRows)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelectionEqual(500)
	}
}

func BenchmarkZoneMapDict_EqualSorted_Naive(b *testing.B) {
	c := buildDictChunkedSorted(chunkedBenchRows, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelection(func(s string) bool { return s == "v050" })
	}
}

func BenchmarkZoneMapDict_EqualSorted_Pruned(b *testing.B) {
	c := buildDictChunkedSorted(chunkedBenchRows, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelectionEqual("v050")
	}
}

func BenchmarkChunkedDict_FilterSelection_Flat(b *testing.B) {
	c := buildDictFlat(chunkedBenchRows, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelection(func(s string) bool { return s < "v050" })
	}
}

func BenchmarkChunkedDict_FilterSelection_Chunked(b *testing.B) {
	c := buildDictChunked(chunkedBenchRows, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.FilterSelection(func(s string) bool { return s < "v050" })
	}
}
