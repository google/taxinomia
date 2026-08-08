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
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"testing"

	"github.com/google/taxinomia/web/handlers"
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
// perf-duration / timing-value spans), the only nondeterministic bytes on the
// pages under test.
var timingRE = regexp.MustCompile(`(class="(?:perf-duration|timing-value)">)[0-9][0-9.]*ms`)

func normalizeHTML(b []byte) []byte {
	return timingRE.ReplaceAll(b, []byte(`${1}0.00ms`))
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
