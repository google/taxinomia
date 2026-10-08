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
	"sort"
	"strings"

	"github.com/google/taxinomia/core/engine"
	"github.com/google/taxinomia/core/tables"
)

// BuildCatalog assembles an engine.Catalog from this manager's declarative
// configuration (sources, entity types, hierarchies) and the given live
// tables. The catalog is plain data: presentation code (web/navigation)
// reads it to build links and hierarchy panels instead of calling back into
// the manager.
//
// tbls is the full table set to describe, normally DataModel.GetAllTables();
// entries are emitted sorted by table name. A table's PrimaryKeyEntityType is
// taken from the source declaration of the same name when present, otherwise
// from its first key column (GetColumnNames order) with a non-empty entity
// type — the same precedence the demo's resolver used. Callers may patch the
// returned catalog afterwards for tables the configuration does not cover.
//
// Catalog.Joins is left empty: join discovery lives in models.DataModel, and
// navigation does not consume joins.
func (m *Manager) BuildCatalog(tbls map[string]*tables.DataTable) *engine.Catalog {
	cat := &engine.Catalog{}

	names := make([]string, 0, len(tbls))
	for name := range tbls {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t := tbls[name]
		tm := engine.TableMeta{
			Name:                 name,
			RowCount:             int64(t.Length()),
			PrimaryKeyEntityType: m.GetPrimaryKeyEntityType(name),
		}
		colNames := t.GetColumnNames()
		sort.Strings(colNames) // deterministic: GetColumnNames is map order
		var keyCandidates []engine.ColumnMeta
		for _, colName := range colNames {
			col := t.GetColumn(colName)
			if col == nil {
				continue
			}
			cm := engine.ColumnMeta{
				Name:        colName,
				DisplayName: col.ColumnDef().DisplayName(),
				EntityType:  col.ColumnDef().EntityType(),
				IsKey:       col.IsKey(),
			}
			if cm.IsKey && cm.EntityType != "" {
				keyCandidates = append(keyCandidates, cm)
			}
			tm.Columns = append(tm.Columns, cm)
		}
		if tm.PrimaryKeyEntityType == "" {
			tm.PrimaryKeyEntityType = inferredPrimaryKey(name, keyCandidates)
		}
		cat.Tables = append(cat.Tables, tm)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, j := range m.journeys {
		jm := engine.Journey{
			Name:        j.GetName(),
			Title:       j.GetTitle(),
			Description: j.GetDescription(),
			Products:    append([]string(nil), j.GetProducts()...),
		}
		for _, st := range j.GetSteps() {
			jm.Steps = append(jm.Steps, engine.JourneyStep{Caption: st.GetCaption(), Link: st.GetLink(), Target: st.GetTarget()})
		}
		cat.Journeys = append(cat.Journeys, jm)
	}

	for _, name := range m.hierarchyOrder {
		h, ok := m.hierarchies[name]
		if !ok {
			continue
		}
		cat.Hierarchies = append(cat.Hierarchies, engine.Hierarchy{
			Name:        h.GetName(),
			Description: h.GetDescription(),
			Levels:      append([]string(nil), h.GetLevels()...),
		})
	}

	etNames := make([]string, 0, len(m.entityTypes))
	for name := range m.entityTypes {
		etNames = append(etNames, name)
	}
	sort.Strings(etNames)
	for _, name := range etNames {
		et := m.entityTypes[name]
		meta := engine.EntityTypeMeta{
			Name:        et.GetName(),
			Description: et.GetDescription(),
		}
		for _, u := range et.GetUrls() {
			meta.URLs = append(meta.URLs, engine.URLTemplate{
				Name:       u.GetName(),
				Template:   u.GetTemplate(),
				IsDefault:  u.GetIsDefault(),
				TableLabel: u.GetTableLabel(),
			})
		}
		cat.EntityTypes = append(cat.EntityTypes, meta)
	}

	return cat
}

// inferredPrimaryKey picks the primary key entity type of a table that
// declares none, from its key columns that carry an entity type (sorted by
// name): the one named like the table ("region" for "regions"), else the
// first. A table with several such columns (regions: region and capital)
// used to get whichever came first in map order, which varied between runs.
func inferredPrimaryKey(tableName string, candidates []engine.ColumnMeta) string {
	if len(candidates) == 0 {
		return ""
	}
	singular := strings.TrimSuffix(tableName, "s")
	for _, c := range candidates {
		if c.Name == tableName || c.Name == singular {
			return c.EntityType
		}
	}
	return candidates[0].EntityType
}
