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

package datasources

import (
	"reflect"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

const catalogTestConfig = `
sources {
  name: "declared"
  annotations_id: "unused"
  source_type: "proto"
  primary_key_entity_type: "test.declared_pk"
}
entity_types {
  name: "test.declared_pk"
  description: "Declared primary key"
  urls { name: "Details" template: "https://example.com/{entity_type}/{value}" }
  urls { name: "Audit"   template: "https://audit.example.com/{value}" is_default: true }
}
entity_types {
  name: "test.item"
  description: "An item"
}
hierarchies {
  name: "test.h"
  description: "Test hierarchy"
  levels: "test.declared_pk"
  levels: "test.item"
}
`

func buildCatalogTestTable(t *testing.T, name, entityType string, key bool) *tables.DataTable {
	t.Helper()
	table := tables.NewDataTable()
	col := columns.NewStringColumn(columns.NewColumnDef(name, "Display "+name, entityType))
	values := []string{"a", "b", "c"}
	if !key {
		values = []string{"a", "a", "b"}
	}
	for _, v := range values {
		col.Append(v)
	}
	col.FinalizeColumn()
	if col.IsKey() != key {
		t.Fatalf("test column keyness: got %v, want %v", col.IsKey(), key)
	}
	table.AddColumn(col)
	return table
}

func TestBuildCatalog(t *testing.T) {
	manager := NewManager()
	if err := manager.LoadConfigFromBytes([]byte(catalogTestConfig), "."); err != nil {
		t.Fatalf("LoadConfigFromBytes failed: %v", err)
	}

	tbls := map[string]*tables.DataTable{
		// Source declaration wins even though the column is also a key.
		"declared": buildCatalogTestTable(t, "id", "test.declared_pk", true),
		// No declaration: primary key falls back to the key column's entity type.
		"fallback": buildCatalogTestTable(t, "item", "test.item", true),
		// Neither declared nor detectable: no primary key.
		"plain": buildCatalogTestTable(t, "note", "", false),
	}

	cat := manager.BuildCatalog(tbls)

	names := make([]string, len(cat.Tables))
	for i, tm := range cat.Tables {
		names[i] = tm.Name
	}
	if want := []string{"declared", "fallback", "plain"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("tables sorted by name: got %v, want %v", names, want)
	}

	if got := cat.Tables[0].PrimaryKeyEntityType; got != "test.declared_pk" {
		t.Errorf("declared PK: got %q", got)
	}
	if got := cat.Tables[0].RowCount; got != 3 {
		t.Errorf("row count: got %d, want 3", got)
	}
	if got := cat.Tables[1].PrimaryKeyEntityType; got != "test.item" {
		t.Errorf("IsKey-fallback PK: got %q", got)
	}
	if got := cat.Tables[2].PrimaryKeyEntityType; got != "" {
		t.Errorf("no-PK table: got %q, want empty", got)
	}

	wantCols := []struct{ name, display, entityType string }{
		{"id", "Display id", "test.declared_pk"},
	}
	for i, w := range wantCols {
		c := cat.Tables[0].Columns[i]
		if c.Name != w.name || c.DisplayName != w.display || c.EntityType != w.entityType || !c.IsKey {
			t.Errorf("column %d: %+v, want %+v (key)", i, c, w)
		}
	}

	if len(cat.Hierarchies) != 1 {
		t.Fatalf("hierarchies: got %d, want 1", len(cat.Hierarchies))
	}
	h := cat.Hierarchies[0]
	if h.Name != "test.h" || h.Description != "Test hierarchy" ||
		!reflect.DeepEqual(h.Levels, []string{"test.declared_pk", "test.item"}) {
		t.Errorf("hierarchy: %+v", h)
	}

	if len(cat.EntityTypes) != 2 {
		t.Fatalf("entity types: got %d, want 2", len(cat.EntityTypes))
	}
	et := cat.EntityTypes[0]
	if et.Name != "test.declared_pk" || et.Description != "Declared primary key" {
		t.Errorf("entity type meta: %+v", et)
	}
	if len(et.URLs) != 2 || et.URLs[0].Name != "Details" || et.URLs[0].IsDefault ||
		et.URLs[1].Name != "Audit" || !et.URLs[1].IsDefault ||
		et.URLs[1].Template != "https://audit.example.com/{value}" {
		t.Errorf("URL templates: %+v", et.URLs)
	}
	if got := cat.EntityTypes[1].Name; got != "test.item" {
		t.Errorf("entity types sorted: second is %q", got)
	}

	if cat.Joins != nil {
		t.Errorf("joins should be empty, got %+v", cat.Joins)
	}
}
