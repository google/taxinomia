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

package demo

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"testing"

	"github.com/google/taxinomia/web/handlers"
	"github.com/google/taxinomia/web/rendering"
	"github.com/google/taxinomia/web/urlquery"
)

// Golden tests for the full demo request path: SetupDemoServer +
// HandleTableRequest, HTML output compared byte-for-byte against files in
// testdata/. They pin the navigation surface — entity URL resolution on
// cells, the detail panel's hierarchy contexts and related tables — across
// the S4 resolver inversion.
//
// Update with: bazelisk test //demo:demo_test --test_env=UPDATE_GOLDENS=1
//
// Only deterministic slices of the demo data are rendered: the CSV-backed
// orders table, the textproto-backed customer_orders table, and the
// identifier columns of google_clusters (its measure columns and the
// google_jobs/tasks/allocs names are generated with an unseeded rand and
// differ per process). Server-side timing values are normalized out.

var (
	goldenSetupOnce sync.Once
	goldenServer    *handlers.Server
	goldenProducts  *ProductRegistry
	goldenSetupErr  error
)

func goldenSetup(t *testing.T) (*handlers.Server, *ProductRegistry) {
	t.Helper()
	goldenSetupOnce.Do(func() {
		goldenServer, goldenProducts, goldenSetupErr = SetupDemoServer(os.ReadFile, testDirReader)
	})
	if goldenSetupErr != nil {
		t.Fatalf("SetupDemoServer failed: %v", goldenSetupErr)
	}
	return goldenServer, goldenProducts
}

// timingRE matches the rendered server-side timing values ("12.34ms" inside
// perf-duration / timing-value spans); buildRE matches the build version
// chip and the perf tab's build facts, which change with every commit and
// toolchain. These are the only nondeterministic bytes on the pages under
// test.
var (
	timingRE = regexp.MustCompile(`(class="(?:perf-duration|timing-value)">)[0-9][0-9.]*ms`)
	buildRE  = regexp.MustCompile(`(class="(?:build-version|perf-build)")(?: title="[^"]*")?>[^<]*<`)
	// The perf tab's per-row cost and rate derive from the durations.
	volumeRE = regexp.MustCompile(`(class="perf-volume">)[^<]+<`)
)

func normalizeHTML(b []byte) []byte {
	b = timingRE.ReplaceAll(b, []byte(`${1}0.00ms`))
	b = volumeRE.ReplaceAll(b, []byte(`${1}VOLUME<`))
	return buildRE.ReplaceAll(b, []byte(`${1}>BUILD<`))
}

func TestGoldenDemoPages(t *testing.T) {
	srv, products := goldenSetup(t)
	product := products.Get("default")
	if product == nil {
		t.Fatal("default product not found")
	}

	// Under bazel runtime.Caller yields a runfiles-relative path, so golden
	// updates would land in the runfiles tree; GOLDEN_DIR overrides the
	// location for updating the checked-in files.
	testdataDir := os.Getenv("GOLDEN_DIR")
	if testdataDir == "" {
		_, currentFile, _, ok := runtime.Caller(0)
		if !ok {
			t.Fatal("failed to get current file path")
		}
		testdataDir = filepath.Join(filepath.Dir(currentFile), "testdata")
	}

	cases := []struct {
		name string
		url  string
	}{
		// Flat rows with entity-URL resolution on cells (region/status/... columns).
		{"orders_flat", "/default/table?table=orders&limit=10"},
		// Grouped rows: URL resolution inside the group walk.
		{"orders_grouped", "/default/table?table=orders&grouped=region&limit=25"},
		// Detail panel on a textproto-loaded row: SelectedRowData with all
		// entity URLs, hierarchy contexts via the no-position branch (the
		// google hierarchies do not contain demo.order_id), and the related
		// tables list (customer_orders_binary shares the demo.order_id column).
		// Columns are pinned because the default (first 4 of GetColumnNames)
		// is nondeterministic for proto-loaded tables.
		{"customer_orders_row", "/default/table?table=customer_orders&columns=order_id%2Ccustomer_id%2Cproduct_id%2Cstatus&row=ORD-2024-001&limit=25"},
		// Detail panel on a programmatic google table row: hierarchy ancestors
		// with value URLs (region, zone), descendants with list URLs (rack,
		// machine), and the second hierarchy's no-position branch.
		{"google_clusters_row", "/default/table?table=google_clusters&columns=cluster%2Czone%2Cregion&row=us-east-a-c0&limit=5"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(tc.url)
			if err != nil {
				t.Fatalf("bad test url: %v", err)
			}
			var buf bytes.Buffer
			if res := srv.HandleTableRequest(&buf, u, product, func(k, v string) {}); res != nil {
				t.Fatalf("HandleTableRequest failed: %+v", res)
			}
			got := normalizeHTML(buf.Bytes())

			goldenPath := filepath.Join(testdataDir, tc.name+".golden.html")
			if os.Getenv("UPDATE_GOLDENS") != "" {
				if err := os.MkdirAll(testdataDir, 0o755); err != nil {
					t.Fatalf("mkdir testdata: %v", err)
				}
				if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				t.Logf("updated %s (%d bytes)", goldenPath, len(got))
				return
			}
			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("read golden (run with UPDATE_GOLDENS=1 to create): %v", err)
			}
			if !bytes.Equal(got, want) {
				diffPath := filepath.Join(t.TempDir(), tc.name+".actual.html")
				os.WriteFile(diffPath, got, 0o644)
				t.Errorf("rendered HTML differs from %s (%d vs %d bytes); actual written to %s",
					goldenPath, len(got), len(want), diffPath)
			}
		})
	}
}

// TestExecuteMatchesHandler pins the extracted pipeline to the handler: a
// page built from Execute + BuildViewModel + the renderer — the path an
// embedding server takes — is byte-identical to HandleTableRequest's.
func TestExecuteMatchesHandler(t *testing.T) {
	srv, products := goldenSetup(t)
	product := products.Get("default")
	renderer, err := rendering.NewTableRenderer()
	if err != nil {
		t.Fatal(err)
	}
	// Same asset delivery as the demo server (SetupDemoServer), or the
	// page heads differ.
	renderer.UseStaticAssets("/static")
	for _, raw := range []string{
		"/default/table?table=orders&limit=10",
		"/default/table?table=orders&grouped=region&limit=25&infotab=perf",
		"/default/table?table=customer_orders&columns=order_id%2Ccustomer_id%2Cproduct_id%2Cstatus&row=ORD-2024-001&limit=25&types=1",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		var viaHandler bytes.Buffer
		if res := srv.HandleTableRequest(&viaHandler, u, product, func(k, v string) {}); res != nil {
			t.Fatalf("HandleTableRequest(%s): %+v", raw, res)
		}

		q := urlquery.NewQuery(u)
		exec, res := srv.Execute(context.Background(), q, handlers.ExecOptions{DefaultColumns: product.GetDefaultColumns(q.Table)})
		if res != nil {
			t.Fatalf("Execute(%s): %+v", raw, res)
		}
		// Get TableView, Process Joins, Computed Columns, Apply Filters, Grouping
		// (the grouping build's sub-steps are listed under the last one).
		phases := 0
		for _, e := range exec.Timing.GetEntries() {
			if !e.Sub {
				phases++
			}
		}
		if phases != 5 {
			t.Errorf("Execute(%s) recorded %d phases, want 5", raw, phases)
		}
		var viaExecute bytes.Buffer
		if err := renderer.Render(&viaExecute, srv.BuildViewModel(exec)); err != nil {
			t.Fatal(err)
		}
		// The handler additionally records "Parse Query" as its first phase;
		// drop that row before comparing. The grouping sub-steps differ too:
		// the first render builds the grouping, the second is served from the
		// view's cache and reports a single "cached" step.
		got := subStepRowRE.ReplaceAllString(string(normalizeHTML(viaExecute.Bytes())), "")
		want := subStepRowRE.ReplaceAllString(parseQueryRowRE.ReplaceAllString(string(normalizeHTML(viaHandler.Bytes())), ""), "")
		if got != want {
			i := 0
			for i < len(got) && i < len(want) && got[i] == want[i] {
				i++
			}
			lo := max(0, i-120)
			t.Errorf("Execute+BuildViewModel differs from HandleTableRequest for %s at byte %d:\n execute: %q\n handler: %q", raw, i, got[lo:min(len(got), i+120)], want[lo:min(len(want), i+120)])
		}
	}
}

var (
	parseQueryRowRE = regexp.MustCompile(`(?s)\s*<li class="perf-timing-item">\s*<span class="perf-operation">Parse Query</span>.*?</li>`)
	subStepRowRE    = regexp.MustCompile(`(?s)\s*<li class="perf-timing-item sub">.*?</li>`)
)
