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
	"github.com/google/taxinomia/core/buildinfo"
)

// EntityViewModel is the page for one entity: an entity type that is some
// table's primary key (its home table), and one value of it. The page is
// mostly links: the entity's own row (values that are entities themselves
// link to their pages), its place in each hierarchy, every table that
// refers to it (as a filtered list) and the external links declared for
// its type.
type EntityViewModel struct {
	Title       string // e.g. "Cluster asia-east-a-c1"
	EntityType  string // e.g. "google.cluster"
	TypeLabel   string // e.g. "Cluster" (the home table's key column display name)
	Description string // the entity type's description
	Value       string

	Found     bool   // false: no row of the home table has this value
	HomeTable string // the table whose primary key is the entity type
	HomeTitle string // its title, as on the table page
	HomeURL   string // the home table filtered to this entity
	RowCount  int    // rows of the home table with this value (normally 1)

	Fields       []EntityField      // the entity's row, every column of the home table
	Hierarchies  []HierarchyContext // its place in each hierarchy
	Related      []RelatedTable     // tables that refer to it, as filtered lists
	ExternalURLs []EntityURL        // links declared for the entity type

	Assets Assets         // stylesheet (shared with the table page)
	Build  buildinfo.Info // build stamp for the status line
}

// EntityField is one value of the entity's row.
type EntityField struct {
	Name        string // column name
	DisplayName string // column display name
	Value       string
	URL         string // link for the value: its entity page when the value is an entity, else its default link; "" for none
	IsEntity    bool   // the value is itself an entity (URL is its entity page)
	IsKey       bool   // this is the entity's own key column
}
