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
	"context"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/google/taxinomia/core/buildinfo"
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
	"github.com/google/taxinomia/web/urlquery"
	"github.com/google/taxinomia/web/viewmodel"
)

// The single-entity page is served on the table route, so an app needs no
// new route: <table path>?entity=<entity type>&value=<value>. An entity is
// an entity type that is some table's primary key (its home table); cells
// holding an entity's value link to its page instead of an external link,
// and the page lists the external links.

// entityHomes maps each entity type that is a table's primary key to that
// table (the first by name if several tables share one).
func (s *Server) entityHomes() map[string]string {
	_, _, pkRes, _, _, _ := s.effectiveResolvers()
	homes := make(map[string]string)
	if pkRes == nil || s.dataModel == nil {
		return homes
	}
	names := make([]string, 0, len(s.dataModel.GetAllTables()))
	for name := range s.dataModel.GetAllTables() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		et := pkRes(name)
		if et == "" {
			continue
		}
		if _, taken := homes[et]; taken {
			continue
		}
		if keyColumnFor(s.dataModel.GetTable(name), et) != "" {
			homes[et] = name
		}
	}
	return homes
}

// keyColumnFor returns the column of table whose entity type is et.
func keyColumnFor(table *tables.DataTable, et string) string {
	if table == nil {
		return ""
	}
	for _, name := range table.GetColumnNames() {
		if col := table.GetColumn(name); col != nil && col.ColumnDef().EntityType() == et {
			return name
		}
	}
	return ""
}

// EntityPageURL is the address of an entity's page on the table route at
// path (e.g. "/google/table").
func EntityPageURL(path, entityType, value string) string {
	v := url.Values{}
	v.Set("entity", entityType)
	v.Set("value", value)
	return path + "?" + v.Encode()
}

// entityLinking wraps a cell-link resolver: values of an entity type link
// to the entity's page; other values keep the resolver's link.
func (s *Server) entityLinking(path string, homes map[string]string, fallback viewmodel.URLResolver) viewmodel.URLResolver {
	return func(entityType, value string) string {
		if _, ok := homes[entityType]; ok && value != "" && !isUnreadableLabel(value) {
			return EntityPageURL(path, entityType, value)
		}
		if fallback != nil {
			return fallback(entityType, value)
		}
		return ""
	}
}

func isUnreadableLabel(v string) bool {
	return v == columns.ErrorLabel || v == columns.UnmatchedLabel
}

// handleEntityRequest renders the single-entity page.
func (s *Server) handleEntityRequest(ctx context.Context, w io.Writer, requestURL *url.URL, product ProductConfig, setHeader func(key, value string)) *TableHandlerResult {
	params := requestURL.Query()
	entityType := params.Get("entity")
	value := params.Get("value")
	homes := s.entityHomes()
	home, ok := homes[entityType]
	if !ok {
		return &TableHandlerResult{StatusCode: http.StatusNotFound, Message: "no table has " + entityType + " as its key"}
	}
	vm := s.buildEntityViewModel(requestURL.Path, entityType, value, home, homes)
	setHeader("Content-Type", "text/html; charset=utf-8")
	setHeader(versionHeader, vm.Build.Version())
	if err := s.renderer.RenderEntity(w, vm); err != nil {
		log.Printf("Entity page rendering error: %v", err)
		return &TableHandlerResult{Error: err}
	}
	return nil
}

func (s *Server) buildEntityViewModel(path, entityType, value, home string, homes map[string]string) viewmodel.EntityViewModel {
	urlRes, allURLs, _, descRes, hierarchies, _ := s.effectiveResolvers()
	table := s.dataModel.GetTable(home)
	keyCol := keyColumnFor(table, entityType)

	typeLabel := keyCol
	if col := table.GetColumn(keyCol); col != nil {
		typeLabel = col.ColumnDef().DisplayName()
	}
	vm := viewmodel.EntityViewModel{
		Title:      typeLabel + " " + value,
		EntityType: entityType,
		TypeLabel:  typeLabel,
		Value:      value,
		HomeTable:  home,
		HomeTitle:  strings.Title(home), //nolint:staticcheck // matches the table page heading
		Build:      buildinfo.Get(),
	}
	if descRes != nil {
		vm.Description = descRes(entityType)
	}
	homeQuery := url.Values{}
	homeQuery.Set("table", home)
	homeQuery.Set("filter:"+keyCol, `"`+value+`"`)
	vm.HomeURL = path + "?" + homeQuery.Encode()

	rows := findRows(table, keyCol, value)
	vm.RowCount = len(rows)
	vm.Found = len(rows) > 0
	if !vm.Found {
		return vm
	}
	row := rows[0]

	// The entity's row: its key first, then the other columns by display name.
	rowData := make(map[string]string)
	columnEntityTypes := make(map[string]string)
	names := table.GetColumnNames()
	sort.SliceStable(names, func(i, j int) bool {
		if (names[i] == keyCol) != (names[j] == keyCol) {
			return names[i] == keyCol
		}
		return strings.ToLower(table.GetColumn(names[i]).ColumnDef().DisplayName()) < strings.ToLower(table.GetColumn(names[j]).ColumnDef().DisplayName())
	})
	for _, name := range names {
		col := table.GetColumn(name)
		v, err := col.GetString(row)
		if err != nil {
			v = columns.ErrorLabel
		}
		et := col.ColumnDef().EntityType()
		rowData[name] = v
		if et != "" {
			columnEntityTypes[name] = et
		}
		field := viewmodel.EntityField{Name: name, DisplayName: col.ColumnDef().DisplayName(), Value: v, IsKey: name == keyCol}
		if !field.IsKey && et != "" && v != "" && !isUnreadableLabel(v) {
			if _, isEntity := homes[et]; isEntity {
				field.URL = EntityPageURL(path, et, v)
				field.IsEntity = true
			} else if urlRes != nil {
				field.URL = urlRes(et, v)
			}
		}
		vm.Fields = append(vm.Fields, field)
	}

	// Hierarchies and related tables, built like the row detail panel's on
	// a query for the home table; values that are entities link to their
	// pages.
	q := urlquery.NewQuery(&url.URL{Path: path, RawQuery: url.Values{"table": {home}}.Encode()})
	if hierarchies != nil {
		for _, h := range hierarchies(q, entityType, value, rowData, columnEntityTypes) {
			if h.Current.Value == "" {
				continue // the entity has no place in this hierarchy
			}
			for j := range h.Ancestors {
				linkLevel(&h.Ancestors[j], path, homes)
			}
			for j := range h.Descendants {
				linkLevel(&h.Descendants[j], path, homes)
			}
			vm.Hierarchies = append(vm.Hierarchies, h)
		}
	}
	// Every table that refers to the entity (the detail panel's resolver
	// leaves out tables already in a hierarchy; this page lists them all).
	vm.Related = s.referringTables(path, home, entityType, value)
	if allURLs != nil {
		vm.ExternalURLs = allURLs(entityType, value)
	}
	return vm
}

// linkLevel points a hierarchy level with a value at its entity page.
func linkLevel(level *viewmodel.HierarchyLevel, path string, homes map[string]string) {
	if level.Value == "" {
		return
	}
	if _, ok := homes[level.EntityType]; ok {
		level.ValueURL = EntityPageURL(path, level.EntityType, level.Value)
	}
}

// findRows returns the rows of table whose keyCol is value: a key column's
// reverse lookup when it has one (checked), else a scan.
func findRows(table *tables.DataTable, keyCol, value string) []uint32 {
	col := table.GetColumn(keyCol)
	if col == nil {
		return nil
	}
	if rev, ok := col.(interface{ GetIndex(string) (uint32, error) }); ok && col.IsKey() {
		if idx, err := rev.GetIndex(value); err == nil {
			if v, err := col.GetString(idx); err == nil && v == value {
				return []uint32{idx}
			}
		}
	}
	var rows []uint32
	n := col.Length()
	for i := 0; i < n; i++ {
		if v, err := col.GetString(uint32(i)); err == nil && v == value {
			rows = append(rows, uint32(i))
		}
	}
	return rows
}

// referringTables lists every column of every other table whose entity type
// is the entity's, as a link to that table filtered to the entity.
func (s *Server) referringTables(path, home, entityType, value string) []viewmodel.RelatedTable {
	names := make([]string, 0, len(s.dataModel.GetAllTables()))
	for name := range s.dataModel.GetAllTables() {
		if name != home && !strings.HasPrefix(name, "_") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var refs []viewmodel.RelatedTable
	for _, name := range names {
		table := s.dataModel.GetTable(name)
		for _, colName := range table.GetColumnNames() {
			col := table.GetColumn(colName)
			if col == nil || col.ColumnDef().EntityType() != entityType {
				continue
			}
			v := url.Values{}
			v.Set("table", name)
			v.Set("filter:"+colName, `"`+value+`"`)
			refs = append(refs, viewmodel.RelatedTable{
				TableName:        name,
				DisplayName:      strings.Title(name), //nolint:staticcheck // matches the table page heading
				ColumnName:       col.ColumnDef().DisplayName(),
				ColumnEntityType: entityType,
				FilterURL:        path + "?" + v.Encode(),
			})
		}
	}
	return refs
}
