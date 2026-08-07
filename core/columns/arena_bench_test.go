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
	"strings"
	"testing"
)

// Phase 4c benchmarks: arena versus []string storage for scans, and the cost
// of front-coded access on a sorted string key.

const arenaBenchRows = 1_000_000

func benchPlainStrings(b *testing.B) *ChunkedStringColumn {
	b.Helper()
	col := NewChunkedStringColumn(NewColumnDef("id", "ID", "test.id"))
	for i := 0; i < arenaBenchRows; i++ {
		col.Append(fmt.Sprintf("cust_%07d", i))
	}
	col.FinalizeColumn()
	return col
}

func BenchmarkArena_SubstringScan(b *testing.B) {
	pred := func(s string) bool { return strings.HasSuffix(s, "9") }
	src := benchPlainStrings(b)
	arena := ArenaEncodeChunkedStringColumn(src)
	b.Run("plain", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			src.FilterSelection(pred)
		}
	})
	b.Run("arena", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			arena.FilterSelection(pred)
		}
	})
}

func BenchmarkFrontCoded_GetString(b *testing.B) {
	src := benchPlainStrings(b)
	fc, ok := FrontCodeChunkedStringColumn(src)
	if !ok {
		b.Fatal("front coding declined")
	}
	arena := ArenaEncodeChunkedStringColumn(src)
	b.Run("arena", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			arena.GetString(uint32((i * 2_654_435_761) % arenaBenchRows))
		}
	})
	b.Run("frontcoded", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			fc.GetString(uint32((i * 2_654_435_761) % arenaBenchRows))
		}
	})
}

func BenchmarkFrontCoded_GetIndex(b *testing.B) {
	src := benchPlainStrings(b)
	fc, ok := FrontCodeChunkedStringColumn(src)
	if !ok {
		b.Fatal("front coding declined")
	}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("cust_%07d", (i*977)%arenaBenchRows)
	}
	b.Run("plain-sparse", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			src.GetIndex(keys[i%len(keys)])
		}
	})
	b.Run("frontcoded", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			fc.GetIndex(keys[i%len(keys)])
		}
	})
}
