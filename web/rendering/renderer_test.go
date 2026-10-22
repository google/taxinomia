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
	"strconv"
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

// TestEntityLinkBlocks renders the entity page's link column: every icon a
// link can name has a symbol in the page's sprite, an area's block carries
// its name and colour, links without an area get a plain block, and
// commands come with a copy button and its script.
func TestEntityLinkBlocks(t *testing.T) {
	page, err := templateFS.ReadFile("templates/entity.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, icon := range viewmodel.LinkIcons {
		if !strings.Contains(string(page), `<symbol id="i-`+icon+`"`) {
			t.Errorf("entity.html has no symbol for icon %q", icon)
		}
	}

	r, err := NewTableRenderer()
	if err != nil {
		t.Fatal(err)
	}
	links := []viewmodel.EntityURL{
		{Name: "Dashboard", URL: "https://example.com/d", Area: "Monitoring", Icon: "chart"},
		{Name: "Wiki", URL: "https://example.com/w", Icon: "book"},
	}
	var buf bytes.Buffer
	commands := []viewmodel.EntityCommand{{Name: "Log in", Command: "ssh 'a b'"}}
	if err := r.RenderEntity(&buf, viewmodel.EntityViewModel{Found: true, ExternalURLs: links, Commands: commands, LinkBlocks: viewmodel.BuildLinkBlocks(links, commands)}); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	for _, want := range []string{
		`<div class="entity-layout has-links">`,
		`<section class="link-block" style="--hue: ` + strconv.Itoa(viewmodel.AreaHue("Monitoring")) + `">`,
		`<h2>Monitoring</h2>`,
		`<use href="#i-chart"></use>`,
		`<section class="link-block link-block-plain">`,
		`<span>Wiki</span>`,
		`<h2>Commands</h2>`,
		`<code>ssh &#39;a b&#39;</code>`,
		`data-command="ssh &#39;a b&#39;"`,
		`document.addEventListener('click'`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("entity page lacks %s", want)
		}
	}
}
