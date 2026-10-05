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
	"html"
	"regexp"
	"strings"

	"github.com/google/safehtml"
	"github.com/google/safehtml/uncheckedconversions"
)

// renderMarkdown renders the small Markdown subset the embedded help pages
// are written in: # headings (with anchor ids), paragraphs, "- " lists
// (items may continue on indented lines), tables (| a | b |, a separator
// row, "\|" for a literal bar), ``` code blocks, `code`, **bold** and
// [text](link) with a relative, #anchor or http(s) link. Everything else is
// text. All text is HTML-escaped, so the output satisfies the HTML type
// contract whatever the input; it is only ever fed taxinomia's own help
// files, but does not rely on that.
func renderMarkdown(src string) safehtml.HTML {
	var b strings.Builder
	lines := strings.Split(strings.ReplaceAll(src, "\r\n", "\n"), "\n")
	var para []string
	flushPara := func() {
		if len(para) > 0 {
			b.WriteString("<p>" + renderInline(strings.Join(para, " ")) + "</p>\n")
			para = nil
		}
	}
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "":
			flushPara()
		case strings.HasPrefix(trimmed, "```"):
			flushPara()
			var code []string
			for i++; i < len(lines) && !strings.HasPrefix(strings.TrimSpace(lines[i]), "```"); i++ {
				code = append(code, lines[i])
			}
			b.WriteString("<pre><code>" + html.EscapeString(strings.Join(code, "\n")) + "</code></pre>\n")
		case strings.HasPrefix(trimmed, "#"):
			flushPara()
			level := len(trimmed) - len(strings.TrimLeft(trimmed, "#"))
			if level > 4 {
				level = 4
			}
			text := strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
			tag := string(rune('0' + level))
			b.WriteString("<h" + tag + ` id="` + html.EscapeString(anchorID(text)) + `">` + renderInline(text) + "</h" + tag + ">\n")
		case strings.HasPrefix(trimmed, "- ") || strings.HasPrefix(trimmed, "* "):
			flushPara()
			b.WriteString("<ul>\n")
			for i < len(lines) {
				t := strings.TrimSpace(lines[i])
				if !strings.HasPrefix(t, "- ") && !strings.HasPrefix(t, "* ") {
					break
				}
				item := strings.TrimSpace(t[2:])
				// Continuation lines: indented, not a new item, not blank.
				for i+1 < len(lines) && strings.HasPrefix(lines[i+1], " ") && strings.TrimSpace(lines[i+1]) != "" &&
					!strings.HasPrefix(strings.TrimSpace(lines[i+1]), "- ") && !strings.HasPrefix(strings.TrimSpace(lines[i+1]), "* ") {
					i++
					item += " " + strings.TrimSpace(lines[i])
				}
				b.WriteString("<li>" + renderInline(item) + "</li>\n")
				i++
			}
			i--
			b.WriteString("</ul>\n")
		case strings.HasPrefix(trimmed, "|") && i+1 < len(lines) && isTableSeparator(lines[i+1]):
			flushPara()
			b.WriteString("<table>\n<thead><tr>")
			for _, cell := range tableCells(trimmed) {
				b.WriteString("<th>" + renderInline(cell) + "</th>")
			}
			b.WriteString("</tr></thead>\n<tbody>\n")
			for i += 2; i < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[i]), "|"); i++ {
				b.WriteString("<tr>")
				for _, cell := range tableCells(strings.TrimSpace(lines[i])) {
					b.WriteString("<td>" + renderInline(cell) + "</td>")
				}
				b.WriteString("</tr>\n")
			}
			i--
			b.WriteString("</tbody>\n</table>\n")
		default:
			para = append(para, trimmed)
		}
	}
	flushPara()
	return uncheckedconversions.HTMLFromStringKnownToSatisfyTypeContract(b.String())
}

var tableSeparatorRE = regexp.MustCompile(`^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?\s*$`)

func isTableSeparator(line string) bool {
	return tableSeparatorRE.MatchString(line)
}

// tableCells splits "| a | b |" into its cells; "\|" is a literal bar.
func tableCells(row string) []string {
	row = strings.TrimSpace(row)
	row = strings.TrimPrefix(row, "|")
	row = strings.TrimSuffix(row, "|")
	var cells []string
	var cur strings.Builder
	for i := 0; i < len(row); i++ {
		switch {
		case row[i] == '\\' && i+1 < len(row) && row[i+1] == '|':
			cur.WriteByte('|')
			i++
		case row[i] == '|':
			cells = append(cells, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteByte(row[i])
		}
	}
	return append(cells, strings.TrimSpace(cur.String()))
}

var (
	inlineCodeRE = regexp.MustCompile("`([^`]+)`")
	boldRE       = regexp.MustCompile(`\*\*([^*]+)\*\*`)
	linkRE       = regexp.MustCompile(`\[([^\]]+)\]\(([^)\s]+)\)`)
)

// renderInline escapes text and renders `code`, **bold** and safe links.
// Code spans are cut out first so their content is never formatted.
func renderInline(text string) string {
	var out strings.Builder
	for {
		loc := inlineCodeRE.FindStringSubmatchIndex(text)
		if loc == nil {
			out.WriteString(formatText(text))
			return out.String()
		}
		out.WriteString(formatText(text[:loc[0]]))
		out.WriteString("<code>" + html.EscapeString(text[loc[2]:loc[3]]) + "</code>")
		text = text[loc[1]:]
	}
}

func formatText(text string) string {
	s := html.EscapeString(text)
	s = boldRE.ReplaceAllString(s, "<strong>$1</strong>")
	return linkRE.ReplaceAllStringFunc(s, func(m string) string {
		parts := linkRE.FindStringSubmatch(m)
		href := html.UnescapeString(parts[2])
		if !safeHelpLink(href) {
			return parts[1]
		}
		return `<a href="` + html.EscapeString(href) + `">` + parts[1] + "</a>"
	})
}

// safeHelpLink accepts #anchors, relative links and http(s) links.
func safeHelpLink(href string) bool {
	lower := strings.ToLower(href)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") || strings.HasPrefix(href, "#") {
		return true
	}
	return !strings.Contains(href, ":")
}

// anchorID is a heading's id: lower case, letters and digits joined by "-"
// (as GitHub does, so links into the docs work the same way).
func anchorID(text string) string {
	var b strings.Builder
	dash := false
	for _, r := range strings.ToLower(text) {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			b.WriteRune(r)
			dash = false
		case r == ' ' || r == '-':
			if !dash && b.Len() > 0 {
				b.WriteByte('-')
				dash = true
			}
		}
	}
	return strings.TrimSuffix(b.String(), "-")
}
