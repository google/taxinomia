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

package viewmodel

import (
	"sort"

	"github.com/google/safehtml"
	"github.com/google/taxinomia/core/tables"
	"github.com/google/taxinomia/web/urlquery"
)

// PaneClass is one class of the column pane: a heading over the columns
// whose ColumnDef.Class it is, in the source's column order. The columns
// without a class come last, alphabetical, under OtherPaneClass.
type PaneClass struct {
	Name      string
	Columns   []ColumnInfo
	Total     int // columns in the class
	Shown     int // of them in the view
	Expanded  bool
	ToggleURL safehtml.URL // expand or collapse (the URL's expanded list, "class:<name>")
	// ShowURL shows every column of the class, or hides them all when all
	// are shown (AllShown): the heading's tick box.
	ShowURL  safehtml.URL
	AllShown bool
}

// OtherPaneClass heads the columns without a class, in a pane with classes.
const OtherPaneClass = "Other"

// PaneClassKey is the entry of an expanded class in the URL's expanded list.
func PaneClassKey(name string) string { return "class:" + name }

// paneColumnOrder ranks the table's columns by their position in the
// source: stored columns in load order, then the table's computed columns
// in declaration order.
func paneColumnOrder(tv *tables.TableView) map[string]int {
	order := map[string]int{}
	if tv == nil {
		return order
	}
	base := tv.GetBaseTable()
	for i, name := range base.ColumnNamesInOrder() {
		order[name] = i
	}
	n := len(order)
	for i, d := range base.ComputedDefinitions() {
		order[d.Name] = n + i
	}
	return order
}

// paneClasses groups the pane's columns by class. Classes come in the order
// of their first column; a class lists its columns in source order. Columns
// without a class come last, under OtherPaneClass, in their alphabetical
// order.
// Nil when no column has a class, or when all have the same one (a single
// heading would only add a line): the pane then stays a flat list.
//
// A class is collapsed unless the URL's expanded list names it; collapsed,
// it still lists the columns that are in the view, and keeps the others
// hidden (PaneHidden) for the pane's search to reveal.
func paneClasses(cols []ColumnInfo, order map[string]int, q *urlquery.Query) []PaneClass {
	byClass := map[string][]ColumnInfo{}
	var names []string
	var unclassified []ColumnInfo
	for _, c := range cols {
		if c.Class == "" {
			unclassified = append(unclassified, c)
			continue
		}
		if _, ok := byClass[c.Class]; !ok {
			names = append(names, c.Class)
		}
		byClass[c.Class] = append(byClass[c.Class], c)
	}
	if len(names) == 0 || (len(names) == 1 && len(unclassified) == 0) {
		return nil
	}
	rank := func(c ColumnInfo) int {
		if r, ok := order[c.Name]; ok {
			return r
		}
		return len(order) // not in the source order: last
	}
	for _, name := range names {
		cs := byClass[name]
		sort.SliceStable(cs, func(i, j int) bool { return rank(cs[i]) < rank(cs[j]) })
	}
	first := func(name string) int { return rank(byClass[name][0]) }
	sort.SliceStable(names, func(i, j int) bool { return first(names[i]) < first(names[j]) })
	if len(unclassified) > 0 {
		// The columns without a class, last, under their own heading, in
		// their alphabetical order: otherwise they would read as part of the
		// class above them.
		names = append(names, OtherPaneClass)
		byClass[OtherPaneClass] = unclassified
	}

	expanded := map[string]bool{}
	for _, e := range q.Expanded {
		expanded[e] = true
	}
	classes := make([]PaneClass, 0, len(names)+1)
	for _, name := range names {
		cs := byClass[name]
		pc := PaneClass{Name: name, Total: len(cs), Expanded: expanded[PaneClassKey(name)]}
		var colNames []string
		for i := range cs {
			colNames = append(colNames, cs[i].Name)
			if cs[i].IsVisible {
				pc.Shown++
			} else if !pc.Expanded {
				cs[i].PaneHidden = true
			}
		}
		pc.Columns = cs
		pc.AllShown = pc.Shown == pc.Total
		pc.ShowURL = q.WithColumnsShown(colNames, !pc.AllShown)
		pc.ToggleURL = q.WithExpandedToggled(PaneClassKey(name))
		classes = append(classes, pc)
	}
	return classes
}

// paneSearchFrom: the column pane offers its search box above this many
// columns.
const paneSearchFrom = 12
