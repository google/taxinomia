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

// memeval measures what memory a C++ rewrite could actually reclaim
// (scaling plan, Open question 1). Three modes:
//
//	-mode retained  per-representation retained bytes/row of finalized
//	                columns versus the analytic flat-layout (C) minimum
//	-mode gc        GC cost of a large live heap: build-time GC stats and
//	                forced-GC wall times for a pointer-free chunked dataset
//	                versus the []string representation; -gogc / -memlimit
//	                set the corresponding runtime knobs first
//	-mode query     transient allocations of grouping and filtering at scale
//
// It is a standalone excluded module (like experimental/kerneleval): never
// built by `go build ./...` from the repo root or by Bazel.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/taxinomia/core/columns"
)

var (
	mode     = flag.String("mode", "retained", "retained | gc | query")
	rowsFlag = flag.Int("rows", 100_000_000, "base row count (map-bearing cases use rows/10)")
	caseFlag = flag.String("case", "", "run only the named case (default: all in the mode)")
	gogc     = flag.Int("gogc", 0, "if > 0, debug.SetGCPercent(gogc) before measuring")
	memlimit = flag.Int64("memlimit", 0, "if > 0, debug.SetMemoryLimit(memlimit) bytes before measuring")
)

func main() {
	flag.Parse()
	if *gogc > 0 {
		debug.SetGCPercent(*gogc)
	}
	if *memlimit > 0 {
		debug.SetMemoryLimit(*memlimit)
	}
	fmt.Printf("memeval mode=%s rows=%d gogc=%d memlimit=%d GOMAXPROCS=%d\n",
		*mode, *rowsFlag, *gogc, *memlimit, runtime.GOMAXPROCS(0))
	switch *mode {
	case "retained":
		runRetained(*rowsFlag, *caseFlag)
	case "gc":
		runGC(*rowsFlag, *caseFlag)
	case "query":
		runQuery(*rowsFlag)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		os.Exit(2)
	}
}

// --- value generators (deterministic; no math/rand so runs are reproducible) ---

// splitmix64: a bijection on uint64, so distinct inputs give distinct values.
func splitmix(i uint64) uint64 {
	i += 0x9e3779b97f4a7c15
	z := i
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

const hexdigits = "0123456789abcdef"

func appendHex(dst []byte, v uint64, nibbles int) []byte {
	for k := nibbles - 1; k >= 0; k-- {
		dst = append(dst, hexdigits[(v>>(uint(k)*4))&0xf])
	}
	return dst
}

func appendDec(dst []byte, v uint64, width int) []byte {
	var buf [20]byte
	p := len(buf)
	for v > 0 || p == len(buf) {
		p--
		buf[p] = byte('0' + v%10)
		v /= 10
	}
	for pad := width - (len(buf) - p); pad > 0; pad-- {
		dst = append(dst, '0')
	}
	return append(dst, buf[p:]...)
}

// random24 returns a pseudo-random 24-byte string for row i. Row 1 repeats
// row 0 so uniqueness detection in FinalizeColumn bails out on the second row
// instead of building a transient n-entry map (non-key cases measure storage,
// not key machinery).
func random24(i int) string {
	if i == 1 {
		i = 0
	}
	b := make([]byte, 0, 24)
	b = appendHex(b, splitmix(uint64(2*i)), 16)
	b = appendHex(b, splitmix(uint64(2*i+1)), 8)
	return string(b)
}

// sortedPK returns "id" + 9 decimal digits: an 11-byte sorted unique primary
// key, the shape 4a's declared sort keys produce.
func sortedPK(i int) string {
	b := make([]byte, 0, 11)
	b = append(b, 'i', 'd')
	b = appendDec(b, uint64(i), 9)
	return string(b)
}

// randomKey16 returns a unique unsorted 16-byte key for row i.
func randomKey16(i int) string {
	return string(appendHex(make([]byte, 0, 16), splitmix(uint64(i)), 16))
}

func dictValues() []string {
	vals := make([]string, 1000)
	for i := range vals {
		vals[i] = string(appendDec([]byte("value_"), uint64(i), 3)) // 9 bytes
	}
	return vals
}

func def(name, entityType string) *columns.ColumnDef {
	return columns.NewColumnDef(name, name, entityType)
}

// --- measurement helpers ---

func settle() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

func fmtBytes(b float64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GiB", b/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MiB", b/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KiB", b/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", b)
	}
}

func progress(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "  ... "+format+"\n", args...)
}

// --- mode: retained ---

type retainedCase struct {
	name  string
	rows  func(base int) int
	flat  float64 // analytic C-layout minimum, bytes per row
	note  string
	build func(rows int) any
}

var retainedCases = []retainedCase{
	{
		name: "int64", rows: base, flat: 8,
		note: "fixed-width chunks",
		build: func(n int) any {
			c := columns.NewChunkedInt64Column(def("i64", ""))
			for i := 0; i < n; i++ {
				c.Append(int64(i)) // sorted: finalize records order, builds no map
			}
			c.FinalizeColumn()
			return c
		},
	},
	{
		name: "float64", rows: base, flat: 8,
		note: "fixed-width chunks",
		build: func(n int) any {
			c := columns.NewChunkedFloat64Column(def("f64", ""))
			for i := 0; i < n; i++ {
				c.Append(float64(i))
			}
			c.FinalizeColumn()
			return c
		},
	},
	{
		name: "uint32", rows: base, flat: 4,
		note: "fixed-width chunks",
		build: func(n int) any {
			c := columns.NewChunkedUint32Column(def("u32", ""))
			for i := 0; i < n; i++ {
				c.Append(uint32(i))
			}
			c.FinalizeColumn()
			return c
		},
	},
	{
		name: "bool", rows: base, flat: 1,
		note: "fixed-width chunks",
		build: func(n int) any {
			c := columns.NewChunkedBoolColumn(def("b", ""))
			for i := 0; i < n; i++ {
				c.Append(i&1 == 0)
			}
			c.FinalizeColumn()
			return c
		},
	},
	{
		name: "dict16-d1000", rows: base, flat: 2,
		note: "uint16 codes; +9 KB dict payload (excluded from flat)",
		build: func(n int) any {
			vals := dictValues()
			c := columns.NewChunkedDictStringColumn[uint16](def("dict16", ""))
			for i := 0; i < n; i++ {
				c.Append(vals[i%1000])
			}
			c.FinalizeColumn()
			return c
		},
	},
	{
		name: "arena-24B", rows: base, flat: 28,
		note: "24 B payload + 4 B offset; non-key",
		build: func(n int) any {
			c := columns.NewChunkedArenaStringColumn(def("arena", ""))
			for i := 0; i < n; i++ {
				c.Append(random24(i))
			}
			c.FinalizeColumn()
			return c
		},
	},
	{
		name: "gostrings-24B", rows: base, flat: 28,
		note: "[]string rep of the same data (what 4c replaced); flat = arena minimum",
		build: func(n int) any {
			c := columns.NewChunkedStringColumn(def("strs", ""))
			for i := 0; i < n; i++ {
				c.Append(random24(i))
			}
			c.FinalizeColumn()
			return c
		},
	},
	{
		name: "pk-front-coded-11B", rows: base, flat: 11,
		note: "flat = raw key payload; front coding may go below it",
		build: func(n int) any {
			src := columns.NewChunkedStringColumn(def("pk", "row"))
			for i := 0; i < n; i++ {
				src.Append(sortedPK(i))
			}
			src.FinalizeColumn() // sorted+unique: no reverse-lookup map
			fc, ok := columns.FrontCodeChunkedStringColumn(src)
			if !ok {
				panic("front coding declined")
			}
			return fc // src is dropped; settle() collects it
		},
	},
	{
		name: "pk-arena-sorted-11B", rows: base, flat: 15,
		note: "sorted key in arena storage: 11 B payload + 4 B offset, no map",
		build: func(n int) any {
			c := columns.NewChunkedArenaStringColumn(def("pk", "row"))
			for i := 0; i < n; i++ {
				c.Append(sortedPK(i))
			}
			c.FinalizeColumn()
			return c
		},
	},
	{
		name: "key-unsorted-map-16B", rows: tenth, flat: 20,
		note: "unsorted key retains map[string]uint32 (the Go-only overhead); rows/10",
		build: func(n int) any {
			c := columns.NewChunkedArenaStringColumn(def("key", "thing"))
			for i := 0; i < n; i++ {
				c.Append(randomKey16(i))
			}
			c.FinalizeColumn()
			return c
		},
	},
}

func base(b int) int  { return b }
func tenth(b int) int { return b / 10 }

func runRetained(baseRows int, only string) {
	fmt.Printf("%-24s %12s %14s %8s %8s %9s  %s\n",
		"case", "rows", "retained", "B/row", "flat", "overhead", "note")
	for _, rc := range retainedCases {
		if only != "" && rc.name != only {
			continue
		}
		n := rc.rows(baseRows)
		progress("building %s (%d rows)", rc.name, n)
		before := settle()
		start := time.Now()
		col := rc.build(n)
		buildTime := time.Since(start)
		after := settle()
		retained := float64(after) - float64(before)
		perRow := retained / float64(n)
		overhead := (perRow/rc.flat - 1) * 100
		fmt.Printf("%-24s %12d %14s %8.3f %8.3f %+8.1f%%  %s\n",
			rc.name, n, fmtBytes(retained), perRow, rc.flat, overhead, rc.note)
		progress("built in %v", buildTime.Round(time.Millisecond))
		runtime.KeepAlive(col)
	}

	// The dictionary interning map held until FinalizeColumn releases it.
	if only == "" || only == "dict16-prefinalize" {
		n := baseRows
		progress("building dict16 pre/post finalize (%d rows)", n)
		vals := dictValues()
		before := settle()
		c := columns.NewChunkedDictStringColumn[uint16](def("dict16", ""))
		for i := 0; i < n; i++ {
			c.Append(vals[i%1000])
		}
		pre := settle()
		c.FinalizeColumn()
		post := settle()
		fmt.Printf("%-24s %12d pre-finalize retains %s more than finalized (%s) — interning map released by FinalizeColumn\n",
			"dict16-prefinalize", n, fmtBytes(float64(pre)-float64(post)), fmtBytes(float64(post)-float64(before)))
		runtime.KeepAlive(c)
	}
}

// --- mode: gc ---

type peakSampler struct {
	stop                    chan struct{}
	done                    chan struct{}
	heapAlloc, heapSys, sys uint64
}

func startSampler() *peakSampler {
	s := &peakSampler{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		var ms runtime.MemStats
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				s.heapAlloc = max(s.heapAlloc, ms.HeapAlloc)
				s.heapSys = max(s.heapSys, ms.HeapSys)
				s.sys = max(s.sys, ms.Sys)
			}
		}
	}()
	return s
}

func (s *peakSampler) finish() {
	close(s.stop)
	<-s.done
}

func runGC(rows int, only string) {
	build := func() {
		switch only {
		case "", "pointerfree":
			only = "pointerfree"
			vals := dictValues()
			i64 := columns.NewChunkedInt64Column(def("i64", ""))
			f64 := columns.NewChunkedFloat64Column(def("f64", ""))
			u32 := columns.NewChunkedUint32Column(def("u32", ""))
			dict := columns.NewChunkedDictStringColumn[uint16](def("dict16", ""))
			arena := columns.NewChunkedArenaStringColumn(def("arena", ""))
			for i := 0; i < rows; i++ {
				i64.Append(int64(i))
				f64.Append(float64(i))
				u32.Append(uint32(i))
				dict.Append(vals[i%1000])
				arena.Append(random24(i))
			}
			for _, c := range []interface{ FinalizeColumn() }{i64, f64, u32, dict, arena} {
				c.FinalizeColumn()
			}
			gcDataset = []any{i64, f64, u32, dict, arena}
		case "gostrings":
			c := columns.NewChunkedStringColumn(def("strs", ""))
			for i := 0; i < rows; i++ {
				c.Append(random24(i))
			}
			c.FinalizeColumn()
			gcDataset = []any{c}
		default:
			fmt.Fprintf(os.Stderr, "unknown gc case %q\n", only)
			os.Exit(2)
		}
	}

	settle()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	sampler := startSampler()
	start := time.Now()
	build()
	buildTime := time.Since(start)
	sampler.finish()

	live := settle()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	fmt.Printf("gc case=%s rows=%d\n", only, rows)
	fmt.Printf("  build time            %v\n", buildTime.Round(10*time.Millisecond))
	fmt.Printf("  live heap after build %s\n", fmtBytes(float64(live)))
	fmt.Printf("  peak HeapAlloc        %s\n", fmtBytes(float64(sampler.heapAlloc)))
	fmt.Printf("  peak HeapSys          %s\n", fmtBytes(float64(sampler.heapSys)))
	fmt.Printf("  peak Sys              %s\n", fmtBytes(float64(sampler.sys)))
	fmt.Printf("  GC cycles during run  %d\n", after.NumGC-before.NumGC)
	fmt.Printf("  STW pause total       %v\n", time.Duration(after.PauseTotalNs-before.PauseTotalNs).Round(10*time.Microsecond))
	fmt.Printf("  GCCPUFraction         %.4f\n", after.GCCPUFraction)

	// Forced full collections over the live heap: the marginal mark cost of
	// keeping this dataset resident. Pointer-free chunks are not scanned, so
	// this should stay near-constant as the dataset grows; []string is the
	// pointer-per-row counterexample.
	fmt.Printf("  forced GC wall times  ")
	for k := 0; k < 5; k++ {
		t0 := time.Now()
		runtime.GC()
		fmt.Printf("%v ", time.Since(t0).Round(100*time.Microsecond))
	}
	fmt.Println()
	runtime.KeepAlive(gcDataset)
}

var gcDataset []any

// --- mode: query ---

func runQuery(rows int) {
	vals := dictValues()
	progress("building dict16 (%d rows)", rows)
	col := columns.NewChunkedDictStringColumn[uint16](def("dict16", ""))
	for i := 0; i < rows; i++ {
		col.Append(vals[i%1000])
	}
	col.FinalizeColumn()
	baseline := settle()
	fmt.Printf("query dataset: dict16-d1000, %d rows, live %s\n", rows, fmtBytes(float64(baseline)))

	measureOp := func(name string, op func() any) {
		op() // warm: one-time caches (Ranks, pre-agg, pools) built outside the measurement
		settle()
		var ms0, ms1 runtime.MemStats
		runtime.ReadMemStats(&ms0)
		start := time.Now()
		var out any
		for k := 0; k < 3; k++ {
			out = op()
		}
		wall := time.Since(start) / 3
		runtime.ReadMemStats(&ms1)
		alloc := (float64(ms1.TotalAlloc) - float64(ms0.TotalAlloc)) / 3
		runtime.KeepAlive(out)
		out = nil
		retained := float64(settle()) - float64(baseline)
		fmt.Printf("  %-34s %10v/op  allocates %12s/op  retained after drop %+d B\n",
			name, wall.Round(10*time.Microsecond), fmtBytes(alloc), int64(retained))
	}

	sel := columns.AllRows(rows)
	ops := columns.IGroupOps(col)
	measureOp("GroupCounts (full universe)", func() any {
		counts, firsts := ops.GroupCounts(sel)
		return [2][]uint32{counts, firsts}
	})
	measureOp("FilterSelection (substring)", func() any {
		return col.FilterSelection(func(s string) bool { return strings.HasPrefix(s, "value_5") })
	})
	measureOp("Filter+GroupCounts (chained)", func() any {
		s := col.FilterSelection(func(v string) bool { return strings.HasPrefix(v, "value_5") })
		counts, _ := ops.GroupCounts(s)
		return counts
	})
	// Keep the dataset live through the last measurement's settle(): without
	// this, liveness analysis lets the GC collect the column during the final
	// retained-after-drop reading, which then goes hugely negative.
	runtime.KeepAlive(col)
}
