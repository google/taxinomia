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

package handlers

import (
	"github.com/google/safehtml"
	"github.com/google/taxinomia/core/tables"
	"github.com/google/taxinomia/web/navigation"
	"github.com/google/taxinomia/web/urlquery"
	"github.com/google/taxinomia/web/viewmodel"
)

// hierarchyPathViews builds the hierarchy strip above the table: each
// hierarchy the table can be grouped by, with a link per level that groups
// by the path down to it (the whole path for the name). A link whose
// grouping is the current one ungroups instead.
func hierarchyPathViews(nav *navigation.Navigator, tv *tables.TableView, q *urlquery.Query, visible []string) []viewmodel.HierarchyPathView {
	if tv == nil {
		return nil
	}
	base := tv.GetBaseTable()
	colTypes := make(map[string]string)
	labels := make(map[string]string)
	for _, name := range base.GetColumnNames() {
		if col := base.GetColumn(name); col != nil {
			def := col.ColumnDef()
			colTypes[name] = def.EntityType()
			labels[name] = def.DisplayName()
		}
	}
	paths := nav.TableHierarchyPaths(q.Table, colTypes)
	if len(paths) == 0 {
		return nil
	}
	views := make([]viewmodel.HierarchyPathView, 0, len(paths))
	for _, p := range paths {
		cols := make([]string, len(p.Levels))
		for i, l := range p.Levels {
			cols[i] = l.Column
		}
		link := func(n int) (safehtml.URL, bool) {
			active := equalStrings(q.GroupedColumns, cols[:n])
			if active {
				return q.WithGroupingReplaced(nil, visible), true
			}
			return q.WithGroupingReplaced(cols[:n], visible), false
		}
		v := viewmodel.HierarchyPathView{Name: p.Name, Description: p.Description}
		v.URL, v.Active = link(len(cols))
		for i, l := range p.Levels {
			label := labels[l.Column]
			if label == "" {
				label = l.Column
			}
			lv := viewmodel.HierarchyPathLevelView{
				Label:   label,
				Column:  l.Column,
				Grouped: i < len(q.GroupedColumns) && q.GroupedColumns[i] == l.Column && equalStrings(q.GroupedColumns[:i], cols[:i]),
			}
			lv.URL, lv.Active = link(i + 1)
			v.Levels = append(v.Levels, lv)
		}
		views = append(views, v)
	}
	return views
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
