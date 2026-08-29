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

// benchsuite is the scale-sweep benchmark suite:
// one deterministic 20-column reference table at 10^4 / 10^6 / 10^8 rows,
// a fixed operation matrix (L1–L2 load, Q1–Q17 single-mechanism queries,
// C1–C5 combination scenarios), same ops at every scale.
//
// Standalone excluded module (like experimental/memeval): never built by
// `go build ./...` from the repo root or by Bazel. Run:
//
//	cd experimental/benchsuite && go run . -scale 1e6
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/google/taxinomia/core/tables"
)

var (
	scaleFlag = flag.String("scale", "1e6", "row count: 1e4 | 1e6 | 1e8 (any int works)")
	sortKey   = flag.String("sortkey", "pk", "pk (id) | drill (country,category,id); full20 only")
	schemaFlag = flag.String("schema", "full20", "full20 | simple6 (6 cols, power-law entity key)")
	opsFlag   = flag.String("ops", "", "comma-separated op names (default: all)")
	jsonFlag  = flag.String("json", "", "write results JSON to this path")
	seedFlag  = flag.Uint64("seed", 42, "generator seed")
	itersFlag = flag.Int("iters", 0, "runs per op (default: 50 up to 1e6 rows, 5 above)")
)

type runReport struct {
	Scale     int         `json:"scale"`
	Seed      uint64      `json:"seed"`
	SortKey   string      `json:"sort_key"`
	GoVersion string      `json:"go_version"`
	NumCPU    int         `json:"num_cpu"`
	AppendS   float64     `json:"l1_append_s"`
	SortS     float64     `json:"l1_sort_s"`
	EncodeS   float64     `json:"l1_encode_s"`
	RowsPerS  float64     `json:"l1_rows_per_s"`
	RetainedB float64     `json:"l2_retained_b_per_row"`
	GCPauseS  float64     `json:"l2_forced_gc_s"`
	Results   []opResult  `json:"results"`
	Decomp    []decompRow `json:"decomposability,omitempty"`
}

type decompRow struct {
	C            string  `json:"c"`
	Constituents string  `json:"constituents"`
	Ratio        float64 `json:"ratio"`
}

// decompMap: which Q-ops a C-op should decompose into (per the benchmark
// plan §2: within ~2× of the constituent sum).
var decompMap = map[string][]string{
	"C1": {"Q2", "Q4", "Q13", "Q9"},
	"C2": {"Q2", "Q4", "Q13", "Q7", "Q16", "Q8"},
	"C3": {"Q11", "Q4"},
	"C4": {"Q2", "Q4", "Q13", "Q11", "Q15", "Q17", "Q8"},
}

func main() {
	flag.Parse()
	out := os.Stdout
	// The engine has debug prints on the join path; keep benchmark output
	// clean by pointing package stdout at the null device during runs.
	if devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0); err == nil {
		os.Stdout = devnull
	}

	scaleF, err := strconv.ParseFloat(*scaleFlag, 64)
	if err != nil || scaleF < 1 {
		fmt.Fprintf(os.Stderr, "bad -scale %q\n", *scaleFlag)
		os.Exit(2)
	}
	n := int(scaleF)
	iters := *itersFlag
	if iters == 0 {
		if n <= 1_000_000 {
			iters = 50
		} else {
			iters = 5
		}
	}
	key := []string{"id"}
	if *sortKey == "drill" {
		key = []string{"country", "category", "id"}
	}

	progress := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "[benchsuite] "+format+"\n", args...)
	}

	// L2 baseline before any build.
	runtime.GC()
	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)

	s := &suite{n: n, seed: *seedFlag, iters: iters}
	var fact *tables.DataTable
	var phases buildPhases
	var dim, dim2 *tables.DataTable
	if *schemaFlag == "simple6" {
		progress("building simple6 fact table: %d rows", n)
		fact, phases = buildSimple6(n, *seedFlag)
	} else {
		progress("building dim (10^4) + dim2 (10^2)")
		dim = buildDim(*seedFlag)
		dim2 = buildDim2(*seedFlag)
		progress("building fact table: %d rows, sortkey=%v", n, key)
		fact, phases = buildFact(n, *seedFlag, key)
	}
	s.fact = fact
	s.build = phases
	progress("L1: append=%.2fs sort=%.2fs encode=%.2fs (%.0f rows/s)",
		phases.Append.Seconds(), phases.Sort.Seconds(), phases.Encode.Seconds(), phases.RowsPerS)

	// L2: retained bytes/row and forced-GC pause with the dataset live.
	runtime.GC()
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	retained := float64(m1.HeapAlloc-m0.HeapAlloc) / float64(n)
	t0 := time.Now()
	runtime.GC()
	gcPause := time.Since(t0)
	progress("L2: retained %.1f B/row, forced GC %.1f ms", retained, gcPause.Seconds()*1000)

	progress("registering data model")
	if *schemaFlag == "simple6" {
		s.dm = newFactOnlyModel(fact)
	} else {
		s.dm = newModel(fact, dim, dim2)
	}

	selected := allOps
	if *schemaFlag == "simple6" {
		selected = simple6Ops
	}
	if *opsFlag != "" {
		want := map[string]bool{}
		for _, o := range strings.Split(*opsFlag, ",") {
			want[strings.TrimSpace(strings.ToUpper(o))] = true
		}
		base := selected
		selected = nil
		for _, o := range base {
			if want[o.name] {
				selected = append(selected, o)
			}
		}
	}

	report := runReport{
		Scale: n, Seed: *seedFlag, SortKey: *sortKey,
		GoVersion: runtime.Version(), NumCPU: runtime.NumCPU(),
		AppendS: phases.Append.Seconds(), SortS: phases.Sort.Seconds(),
		EncodeS: phases.Encode.Seconds(), RowsPerS: phases.RowsPerS,
		RetainedB: retained, GCPauseS: gcPause.Seconds(),
	}

	byName := map[string]opResult{}
	for _, o := range selected {
		progress("running %s (%d iters)", o.name, iters)
		r := o.run(s)
		report.Results = append(report.Results, r)
		byName[o.name] = r
	}

	// Decomposability: C-op vs sum of its constituent Q-ops.
	for _, c := range []string{"C1", "C2", "C3", "C4"} {
		cr, ok := byName[c]
		if !ok {
			continue
		}
		var sum float64
		complete := true
		for _, q := range decompMap[c] {
			qr, ok := byName[q]
			if !ok {
				complete = false
				break
			}
			sum += qr.MedianS
		}
		if complete && sum > 0 {
			report.Decomp = append(report.Decomp, decompRow{
				C: c, Constituents: strings.Join(decompMap[c], "+"), Ratio: cr.MedianS / sum,
			})
		}
	}

	// Markdown report in the BENCHMARKS.md house style.
	fmt.Fprintf(out, "\n## benchsuite %s rows (schema=%s, sortkey=%s, seed=%d, iters=%d, %s, %d cpu)\n\n",
		*scaleFlag, *schemaFlag, *sortKey, *seedFlag, iters, runtime.Version(), runtime.NumCPU())
	fmt.Fprintf(out, "| Op | Median | Cold/×8 | Note |\n|---|---:|---:|---|\n")
	fmt.Fprintf(out, "| L1 build | %.2fs | — | append %.2fs · sort %.2fs · encode %.2fs (%.0f rows/s) |\n",
		phases.Append.Seconds()+phases.Sort.Seconds()+phases.Encode.Seconds(),
		phases.Append.Seconds(), phases.Sort.Seconds(), phases.Encode.Seconds(), phases.RowsPerS)
	fmt.Fprintf(out, "| L2 memory | %.1f B/row | %.1f ms | retained per row (incl. dim tables; partial-chunk capacity dominates below ~1e6 rows) · forced GC pause |\n", retained, gcPause.Seconds()*1000)
	for _, r := range report.Results {
		cold := "—"
		if r.ColdS > 0 {
			cold = fmtDur(r.ColdS)
		}
		fmt.Fprintf(out, "| %s | %s | %s | %s |\n", r.Op, fmtDur(r.MedianS), cold, r.Note)
	}
	if len(report.Decomp) > 0 {
		fmt.Fprintf(out, "\n| C-op | Constituents | C / Σ constituents |\n|---|---|---:|\n")
		for _, d := range report.Decomp {
			flag := ""
			if d.Ratio > 2 {
				flag = "  ⚠ exceeds 2×"
			}
			fmt.Fprintf(out, "| %s | %s | %.2f%s |\n", d.C, d.Constituents, d.Ratio, flag)
		}
	}

	if *jsonFlag != "" {
		data, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(*jsonFlag, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", *jsonFlag, err)
			os.Exit(1)
		}
		progress("wrote %s", *jsonFlag)
	}
}

func fmtDur(sec float64) string {
	switch {
	case sec < 1e-6:
		return fmt.Sprintf("%.0fns", sec*1e9)
	case sec < 1e-3:
		return fmt.Sprintf("%.1fµs", sec*1e6)
	case sec < 1:
		return fmt.Sprintf("%.1fms", sec*1e3)
	default:
		return fmt.Sprintf("%.2fs", sec)
	}
}
