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

package engine

// Catalog describes what exists — tables, hierarchies, entity types, joins —
// as plain data, not callbacks. Presentation asks the engine what exists and
// builds navigation itself; the engine never calls back into presentation.
type Catalog struct {
	Tables      []TableMeta
	Hierarchies []Hierarchy
	EntityTypes []EntityTypeMeta
	Joins       []JoinMeta

	// Journeys are guided walks through the data, declared by the product's
	// authors (data source configuration); presentation plays them.
	Journeys []Journey
}

// TableMeta describes one queryable table.
type TableMeta struct {
	Name        string
	DisplayName string
	RowCount    int64

	// PrimaryKeyEntityType is the entity type of the table's primary key
	// column, "" if the table declares none.
	PrimaryKeyEntityType string

	// Columns lists all of the table's columns (not a request's visible
	// subset).
	Columns []ColumnMeta
}

// Hierarchy is an ordered chain of entity types, root first, e.g.
// zone → cluster → rack → machine.
type Hierarchy struct {
	Name        string
	Description string
	Levels      []string // entity type per level, root first
}

// EntityTypeMeta describes one entity type.
type EntityTypeMeta struct {
	Name        string
	Description string

	// URLs are the external link templates declared for values of this
	// entity type, in declaration order.
	URLs []URLTemplate
}

// URLTemplate is one external link template for an entity type. Template may
// contain the placeholders {value} and {entity_type}.
type URLTemplate struct {
	Name      string
	Template  string
	IsDefault bool // preferred template for single-link contexts
}

// JoinMeta describes one declared join between two tables, by name — the
// live tables and columns stay inside the engine.
type JoinMeta struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string

	// EntityType is the entity type that connects the two columns.
	EntityType string
}

// Journey is a guided walk through the data: each step opens a view and
// points at one control with a caption.
type Journey struct {
	Name        string   // identifier, used in links
	Title       string   // shown in the list and above each step
	Description string   // one sentence on what it shows
	Products    []string // products it is offered in; empty = all
	Steps       []JourneyStep
}

// JourneyStep is one view of a journey.
type JourneyStep struct {
	Caption string // what to look at or do
	Link    string // the view: a table page URL query, e.g. "table=orders&grouped=region"
	Target  string // the control pointed at, e.g. "group:region" (see datasources.JourneyStep)
}
