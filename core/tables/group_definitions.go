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

package tables

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/taxinomia/core/columns"
)

// Group definitions: a grouped column's groups given as filter terms
// separated by ||, in the column's filter box, instead of one group per
// value:
//
//	east || west            two groups; rows matching neither leave the view
//	"North"|"South" || west ||
//	                        North or South together, containing west, and
//	                        [other] for everything else (the trailing ||)
//	..10 || 10..100 || 100..
//	                        number ranges (dates the same, see filter_terms.go)
//
// Each group is written exactly as a filter. A row belongs to the first group
// it matches; groups keep the order they were typed in, [other] last. The
// grouping runs on a hidden column holding each row's group (its label is
// the term as typed), which stands in for the real column only where the
// grouping reads it (groupingColumn).

// OtherGroupLabel labels the group of the rows no defined group takes, when
// the definition ends with ||. Bracketed like [error] and [unmatched], so it
// cannot be mistaken for a value.
const OtherGroupLabel = "[other]"

// GroupSeparator separates the groups of a group definition.
const GroupSeparator = "||"

// IsGroupDefinition reports whether a filter text defines groups (contains
// || outside quotes) instead of filtering rows.
func IsGroupDefinition(text string) bool {
	return len(splitOutsideQuotes(text, GroupSeparator)) > 1
}

// ParseGroupDefinition splits a group definition into its groups' filter
// terms, in order, and whether it ends with || ([other]).
func ParseGroupDefinition(text string) (terms []string, other bool, err error) {
	parts := splitOutsideQuotes(text, GroupSeparator)
	if len(parts) > 1 && strings.TrimSpace(parts[len(parts)-1]) == "" {
		other = true
		parts = parts[:len(parts)-1]
	}
	seen := map[string]bool{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, false, errors.New("a group is empty: write a filter between each || and the next")
		}
		if seen[p] {
			return nil, false, fmt.Errorf("the group %s appears twice", p)
		}
		seen[p] = true
		terms = append(terms, p)
	}
	return terms, other, nil
}

// groupDefinition is one compiled definition.
type groupDefinition struct {
	text   string
	source columns.IDataColumn // the real column the terms read
	column *groupDefColumn
}

// SetGroupDefinitions sets the group definitions of grouped columns
// (column -> definition text, see IsGroupDefinition), replacing earlier
// ones. A definition whose text and column are unchanged is kept as it was.
// It returns why a definition could not be used, per column; those columns
// group by value as usual.
func (t *TableView) SetGroupDefinitions(defs map[string]string) map[string]error {
	errs := map[string]error{}
	next := make(map[string]*groupDefinition, len(defs))
	for name, text := range defs {
		src := t.GetColumn(name)
		if src == nil {
			errs[name] = fmt.Errorf("column %q does not exist", name)
			continue
		}
		if old := t.groupDefs[name]; old != nil && old.text == text && old.source == src {
			next[name] = old
			continue
		}
		terms, other, err := ParseGroupDefinition(text)
		if err != nil {
			errs[name] = err
			continue
		}
		next[name] = &groupDefinition{text: text, source: src, column: newGroupDefColumn(src, terms, other)}
	}
	t.groupDefs = next
	return errs
}

// groupingColumn is the column grouping reads for name: the hidden group
// column when name has a group definition, the column itself otherwise.
func (t *TableView) groupingColumn(name string) columns.IDataColumn {
	if d := t.groupDefs[name]; d != nil {
		return d.column
	}
	return t.GetColumn(name)
}

// DefinedGroup describes a group of a column with a group definition: the
// filter term of the group labelled label, or other for [other]. ok is
// false when the column has no definition (its groups are values).
func (t *TableView) DefinedGroup(column, label string) (term string, other, ok bool) {
	d := t.groupDefs[column]
	if d == nil {
		return "", false, false
	}
	if d.column.other && label == OtherGroupLabel {
		return "", true, true
	}
	return label, false, true
}

// HasGroupDefinition reports whether column groups by a definition.
func (t *TableView) HasGroupDefinition(column string) bool {
	return t.groupDefs[column] != nil
}

// groupDefinitionsKey is the reserved lastFilters key holding the group
// definitions' signature: a changed definition misses the filter and the
// grouping caches.
const groupDefinitionsKey = "\x00group-definitions"

func (t *TableView) groupDefinitionsSignature() string {
	if len(t.groupDefs) == 0 {
		return ""
	}
	names := make([]string, 0, len(t.groupDefs))
	for n := range t.groupDefs {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		fmt.Fprintf(&b, "%s=%s\x00", n, t.groupDefs[n].text)
	}
	return b.String()
}

// applyGroupDefinitions drops from the filter selection the rows no group
// takes, for definitions without [other].
func (t *TableView) applyGroupDefinitions() {
	for _, d := range t.groupDefs {
		if d.column.other {
			continue
		}
		codes := d.column.codes
		t.filterSel.ForEach(func(i uint32) {
			if codes[i] < 0 {
				t.filterSel.Remove(i)
			}
		})
	}
}

// groupDefColumn is the hidden column of a group definition: each row's
// group, as its position in the definition ([other] last). Its codes are
// the group positions, so groups sort in the order they were typed.
type groupDefColumn struct {
	def    *columns.ColumnDef
	codes  []int32 // per row: group position, -1 when no group takes it
	labels []string
	other  bool
}

var errNoGroup = errors.New("no group takes this row")

func newGroupDefColumn(src columns.IDataColumn, terms []string, other bool) *groupDefColumn {
	srcDef := src.ColumnDef()
	c := &groupDefColumn{
		def:    columns.NewColumnDef(srcDef.Name(), srcDef.DisplayName(), ""),
		codes:  make([]int32, src.Length()),
		labels: append([]string(nil), terms...),
		other:  other,
	}
	if other {
		c.labels = append(c.labels, OtherGroupLabel)
	}
	preds := make([]rowPredicate, len(terms))
	for k, term := range terms {
		preds[k] = compileFilterText(src, term)
	}
	for i := range c.codes {
		c.codes[i] = -1
		for k, p := range preds {
			if p(uint32(i)) {
				c.codes[i] = int32(k)
				break
			}
		}
		if c.codes[i] < 0 && other {
			c.codes[i] = int32(len(terms))
		}
	}
	return c
}

func (c *groupDefColumn) ColumnDef() *columns.ColumnDef { return c.def }
func (c *groupDefColumn) Length() int                   { return len(c.codes) }
func (c *groupDefColumn) IsKey() bool                   { return false }

func (c *groupDefColumn) GetString(i uint32) (string, error) {
	if int(i) >= len(c.codes) || c.codes[i] < 0 {
		return "", errNoGroup
	}
	return c.labels[c.codes[i]], nil
}

func (c *groupDefColumn) CreateJoinedColumn(*columns.ColumnDef, columns.IJoiner) columns.IJoinedDataColumn {
	return nil
}

// GroupIndices implements the deprecated membership interface.
func (c *groupDefColumn) GroupIndices(indices []uint32, _ *columns.ColumnView) (map[uint32][]uint32, []uint32) {
	groups := map[uint32][]uint32{}
	var unmapped []uint32
	for _, i := range indices {
		if int(i) >= len(c.codes) || c.codes[i] < 0 {
			unmapped = append(unmapped, i)
			continue
		}
		code := uint32(c.codes[i])
		groups[code] = append(groups[code], i)
	}
	return groups, unmapped
}

// IGroupOps: codes are group positions, dense in [0, len(labels)).

func (c *groupDefColumn) GroupCounts(sel columns.RowSet) (counts, firsts []uint32) {
	counts = make([]uint32, len(c.labels))
	firsts = make([]uint32, len(c.labels))
	sel.ForEachRow(func(i uint32) bool {
		if int(i) < len(c.codes) && c.codes[i] >= 0 {
			code := c.codes[i]
			if counts[code] == 0 {
				firsts[code] = i
			}
			counts[code]++
		}
		return true
	})
	return counts, firsts
}

func (c *groupDefColumn) GroupAggregates(sel columns.RowSet, acc columns.GroupAccumulator) {
	sel.ForEachRow(func(i uint32) bool {
		if int(i) < len(c.codes) && c.codes[i] >= 0 {
			acc.Add(uint32(c.codes[i]), i)
		}
		return true
	})
}

func (c *groupDefColumn) GroupMembers(sel columns.RowSet, code uint32, offset, n int) []uint32 {
	var out []uint32
	if n == 0 {
		return nil
	}
	sel.ForEachRow(func(i uint32) bool {
		if int(i) < len(c.codes) && c.codes[i] == int32(code) {
			if offset > 0 {
				offset--
				return true
			}
			out = append(out, i)
			if n > 0 && len(out) >= n {
				return false
			}
		}
		return true
	})
	return out
}

// CompareRows orders rows by group position: the typed order, [other] last.
func (c *groupDefColumn) CompareRows(i, j uint32) int {
	return int(c.codes[i]) - int(c.codes[j])
}
