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
	"github.com/google/taxinomia/core/columns"
)

// View defines which columns to display and their order
type View struct {
	Columns        []string        // Column names in display order (including joined columns like "fromColumn.toTable.toColumn.selectedColumn")
	Expanded       map[string]bool // Set of expanded paths (e.g., "column1", "column1/table2.column2")
	GroupedColumns []string        // Column names to group by, in grouping order
	// CellLinks, when set, gives the external links shown in a cell as
	// labelled chips after its value (entity type links with a table label).
	CellLinks   CellLinksResolver
	columnViews map[string]*columns.ColumnView
}

// CellLink is an external link shown in a table cell as a small chip.
type CellLink struct {
	Label string // the chip's text (the link's table label)
	Title string // the link's full name, shown on hover
	URL   string
}

// CellLinksResolver returns the links to show in a cell holding value, a
// value of entityType; nil when there are none.
type CellLinksResolver func(entityType, value string) []CellLink

// cellLinks resolves a cell's chips, leaving out the link the value itself
// already opens (skipURL).
func cellLinks(resolve CellLinksResolver, entityType, value, skipURL string) []CellLink {
	if resolve == nil || entityType == "" || value == "" {
		return nil
	}
	var links []CellLink
	for _, l := range resolve(entityType, value) {
		if l.URL != "" && l.URL != skipURL {
			links = append(links, l)
		}
	}
	return links
}
