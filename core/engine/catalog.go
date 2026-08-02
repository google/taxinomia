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
	Name   string
	Levels []string // entity type per level, root first
}

// EntityTypeMeta describes one entity type.
type EntityTypeMeta struct {
	Name        string
	Description string
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
