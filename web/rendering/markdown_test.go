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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/taxinomia/web/viewmodel"
)

func TestRenderMarkdown(t *testing.T) {
	src := strings.Join([]string{
		"# Rows Where an Expression Fails",
		"",
		"A <b>tag</b> & `x < 1` and **bold**, see [the docs](#filtering).",
		"Bad [link](javascript:alert) stays text.",
		"",
		"- first item",
		"  continues here",
		"- second `a|b`",
		"",
		"| You type | Keeps |",
		"|----------|-------|",
		"| `North\\|South` | exactly North or South |",
		"",
		"```",
		"price * <qty>",
		"```",
	}, "\n")
	got := renderMarkdown(src).String()
	for _, want := range []string{
		`<h1 id="rows-where-an-expression-fails">Rows Where an Expression Fails</h1>`,
		`A &lt;b&gt;tag&lt;/b&gt; &amp; <code>x &lt; 1</code> and <strong>bold</strong>, see <a href="#filtering">the docs</a>.`,
		`Bad link stays text.`,
		`<li>first item continues here</li>`,
		`<li>second <code>a|b</code></li>`,
		`<th>You type</th><th>Keeps</th>`,
		`<td><code>North|South</code></td><td>exactly North or South</td>`,
		`<pre><code>price * &lt;qty&gt;</code></pre>`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	if strings.Contains(got, "<b>") || strings.Contains(got, "javascript:") {
		t.Errorf("unescaped markup or unsafe link in\n%s", got)
	}
}

// The embedded expression reference must be the doc other docs link to.
func TestHelpExpressionsMatchesDocs(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "expression_language.md"))
	if err != nil {
		t.Fatalf("read docs/expression_language.md: %v", err)
	}
	embedded, err := helpFS.ReadFile("help/expressions.md")
	if err != nil {
		t.Fatalf("read embedded help: %v", err)
	}
	norm := func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n")) }
	if !bytes.Equal(norm(doc), norm(embedded)) {
		t.Errorf("web/rendering/help/expressions.md differs from docs/expression_language.md: copy the doc over it")
	}
}

func TestRenderHelp(t *testing.T) {
	r, err := NewTableRenderer()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := r.RenderHelp(&buf, viewmodel.HelpViewModel{}); err != nil {
		t.Fatal(err)
	}
	page := buf.String()
	for _, want := range []string{`id="filtering"`, `id="grouping"`, `id="expressions"`, "Expression Language Reference", "<table>"} {
		if !strings.Contains(page, want) {
			t.Errorf("help page lacks %q", want)
		}
	}
}
