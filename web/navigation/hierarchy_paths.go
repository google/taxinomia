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

package navigation

import "strings"

// HierarchyPath is a hierarchy as one table can group by it: the levels the
// table has a column for, root first, each with that column.
type HierarchyPath struct {
	Name        string
	Description string
	Levels      []HierarchyPathLevel
}

// HierarchyPathLevel is one level of a HierarchyPath.
type HierarchyPathLevel struct {
	EntityType string
	Column     string
}

// TableHierarchyPaths lists, in declaration order, the hierarchies the
// table can be grouped by. columnEntityTypes maps each of the table's
// columns (hierarchy ancestor columns included) to its entity type. A
// hierarchy's path holds the levels above the table's own primary key
// level — grouping by the key gives one row per group — that have a
// column, skipping levels without one; a path of fewer than two levels is
// left out, as a single level is an ordinary grouping.
func (n *Navigator) TableHierarchyPaths(tableName string, columnEntityTypes map[string]string) []HierarchyPath {
	byType := make(map[string]string)
	for col, et := range columnEntityTypes {
		if et == "" {
			continue
		}
		if cur, ok := byType[et]; !ok || preferColumn(col, cur) {
			byType[et] = col
		}
	}
	pk := n.PrimaryKeyEntityType(tableName)
	var paths []HierarchyPath
	for _, h := range n.catalog.Hierarchies {
		var levels []HierarchyPathLevel
		for _, et := range h.Levels {
			if et == pk {
				break
			}
			if col, ok := byType[et]; ok {
				levels = append(levels, HierarchyPathLevel{EntityType: et, Column: col})
			}
		}
		if len(levels) >= 2 {
			paths = append(paths, HierarchyPath{Name: h.Name, Description: h.Description, Levels: levels})
		}
	}
	return paths
}

// preferColumn reports whether a is the better column for an entity type
// than b: a plain name over a disambiguated hierarchy ancestor column (§),
// then the shorter name, then the alphabetically first.
func preferColumn(a, b string) bool {
	pa, pb := strings.HasPrefix(a, "§"), strings.HasPrefix(b, "§")
	if pa != pb {
		return pb
	}
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}
