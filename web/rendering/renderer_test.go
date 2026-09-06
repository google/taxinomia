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

package rendering

import (
	"bytes"
	"strings"
	"testing"

	"github.com/google/taxinomia/web/viewmodel"
)

// TestRenderWithoutTimings covers a view model built outside the table
// handler (a client's own pipeline): the Performance tab must say that no
// timings were recorded instead of printing an empty total.
func TestRenderWithoutTimings(t *testing.T) {
	r, err := NewTableRenderer()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := r.Render(&buf, viewmodel.TableViewModel{Title: "t", InfoPaneTab: "perf", ShowInfoPane: true}); err != nil {
		t.Fatal(err)
	}
	html := buf.String()
	if !strings.Contains(html, "No phase timings were recorded") || !strings.Contains(html, "docs/perf-timing.md") {
		t.Error("empty breakdown must explain itself and point at docs/perf-timing.md")
	}
	if strings.Contains(html, "Total Server Time") {
		t.Error("no total row without timings")
	}

	// With timings, the rows and the total are listed.
	buf.Reset()
	vm := viewmodel.TableViewModel{Title: "t", RenderTimeMs: "1.50", TimingBreakdown: []viewmodel.TimingEntry{{Operation: "Apply Filters", DurationMs: "1.25"}}}
	if err := r.Render(&buf, vm); err != nil {
		t.Fatal(err)
	}
	html = buf.String()
	for _, want := range []string{"Apply Filters", "1.25ms", "Total Server Time", "1.50ms"} {
		if !strings.Contains(html, want) {
			t.Errorf("missing %q in rendered perf tab", want)
		}
	}
	if strings.Contains(html, "No phase timings were recorded") {
		t.Error("explanation must not show when timings exist")
	}
}
