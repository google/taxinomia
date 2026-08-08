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

// Package navigation builds the presentation's navigation structures —
// entity links, hierarchy panels, related-table lists — from an
// engine.Catalog. This inverts the resolver callbacks: instead of the
// engine calling back into presentation (the Set*Resolver seams on
// handlers.Server), presentation asks the catalog what exists and builds
// navigation itself. The logic is the former demo/server.go resolver
// bodies, moved here and fed by catalog data.
package navigation

import (
	"strings"

	"github.com/google/taxinomia/core/engine"
	"github.com/google/taxinomia/web/urlquery"
	"github.com/google/taxinomia/web/viewmodel"
)

// Navigator answers navigation questions from a catalog. Its methods match
// the resolver signatures handlers.Server accepts, so a Navigator provides
// the catalog-driven defaults behind every Set*Resolver seam.
type Navigator struct {
	catalog     *engine.Catalog
	tables      map[string]*engine.TableMeta
	entityTypes map[string]*engine.EntityTypeMeta

	// hierarchyEntityTypes holds every entity type appearing as a level of
	// any hierarchy; tables keyed by these are reachable through hierarchy
	// navigation and are excluded from related-table lists.
	hierarchyEntityTypes map[string]bool
}

// NewNavigator builds a Navigator over the given catalog. The catalog is
// read, never modified; callers may patch it before construction (e.g. to
// declare primary keys for tables the configuration does not cover).
func NewNavigator(catalog *engine.Catalog) *Navigator {
	n := &Navigator{
		catalog:              catalog,
		tables:               make(map[string]*engine.TableMeta, len(catalog.Tables)),
		entityTypes:          make(map[string]*engine.EntityTypeMeta, len(catalog.EntityTypes)),
		hierarchyEntityTypes: make(map[string]bool),
	}
	for i := range catalog.Tables {
		n.tables[catalog.Tables[i].Name] = &catalog.Tables[i]
	}
	for i := range catalog.EntityTypes {
		n.entityTypes[catalog.EntityTypes[i].Name] = &catalog.EntityTypes[i]
	}
	for _, h := range catalog.Hierarchies {
		for _, level := range h.Levels {
			n.hierarchyEntityTypes[level] = true
		}
	}
	return n
}

// ResolveDefaultURL resolves the default external URL for an entity value:
// the template marked default, or the first one. Returns "" if the entity
// type declares no URLs.
func (n *Navigator) ResolveDefaultURL(entityType, value string) string {
	et := n.entityTypes[entityType]
	if et == nil || len(et.URLs) == 0 {
		return ""
	}
	template := et.URLs[0].Template
	for _, u := range et.URLs {
		if u.IsDefault {
			template = u.Template
			break
		}
	}
	return expandTemplate(template, value, entityType)
}

// AllURLs resolves every declared URL template for an entity value, in
// declaration order. Returns nil if the entity type declares none.
func (n *Navigator) AllURLs(entityType, value string) []viewmodel.EntityURL {
	et := n.entityTypes[entityType]
	if et == nil || len(et.URLs) == 0 {
		return nil
	}
	result := make([]viewmodel.EntityURL, len(et.URLs))
	for i, u := range et.URLs {
		result[i] = viewmodel.EntityURL{
			Name: u.Name,
			URL:  expandTemplate(u.Template, value, entityType),
		}
	}
	return result
}

// PrimaryKeyEntityType returns the primary key entity type recorded for the
// table, "" if the table is unknown or declares none.
func (n *Navigator) PrimaryKeyEntityType(tableName string) string {
	if tm := n.tables[tableName]; tm != nil {
		return tm.PrimaryKeyEntityType
	}
	return ""
}

// EntityTypeDescription returns the description recorded for the entity
// type, "" if unknown.
func (n *Navigator) EntityTypeDescription(entityType string) string {
	if et := n.entityTypes[entityType]; et != nil {
		return et.Description
	}
	return ""
}

// HierarchyContexts builds the detail panel's hierarchy sections for a
// selected item. All hierarchies are shown, not just those containing the
// primary key entity type; within each, the item's position is the primary
// key's level when the hierarchy contains it, otherwise the deepest level
// for which the row has a column value.
func (n *Navigator) HierarchyContexts(
	currentQuery *urlquery.Query,
	primaryKeyEntityType string,
	primaryKeyValue string,
	rowData map[string]string,
	columnEntityTypes map[string]string,
) []viewmodel.HierarchyContext {
	if len(n.catalog.Hierarchies) == 0 {
		return nil
	}

	// Build reverse map: entity type -> column name
	entityTypeToColumn := make(map[string]string)
	for colName, et := range columnEntityTypes {
		entityTypeToColumn[et] = colName
	}

	var contexts []viewmodel.HierarchyContext
	for _, h := range n.catalog.Hierarchies {
		levels := h.Levels

		// Find the deepest level in this hierarchy where the item has a value
		// This determines the "current" position in the hierarchy
		currentIdx := -1
		var currentEntityType string
		var currentValue string

		// First, check if the primary key entity type is in this hierarchy
		for i, level := range levels {
			if level == primaryKeyEntityType {
				currentIdx = i
				currentEntityType = primaryKeyEntityType
				currentValue = primaryKeyValue
				break
			}
		}

		// If primary key isn't in this hierarchy, find the deepest column that is
		if currentIdx == -1 {
			for i := len(levels) - 1; i >= 0; i-- {
				et := levels[i]
				if colName, ok := entityTypeToColumn[et]; ok {
					if value, ok := rowData[colName]; ok && value != "" {
						currentIdx = i
						currentEntityType = et
						currentValue = value
						break
					}
				}
			}
		}

		ctx := viewmodel.HierarchyContext{
			HierarchyName: h.Name,
			Description:   h.Description,
		}

		// Handle case where item has no direct position in this hierarchy
		// Show all levels as potential navigation (filtered by primary key)
		// Only show as links if the target table has a column for the primary key entity type
		if currentIdx == -1 {
			for _, et := range levels {
				level := viewmodel.HierarchyLevel{
					EntityType:  et,
					DisplayName: formatEntityTypeNamePlural(et),
					Description: n.EntityTypeDescription(et),
				}
				// Check if the target table has a column with the primary key entity type
				// Only generate a link if it does (otherwise the filter won't work)
				if targetTable := n.tables[entityTypeToTableName(et)]; targetTable != nil {
					for _, col := range targetTable.Columns {
						if col.EntityType == primaryKeyEntityType {
							// Target table has a column we can filter by
							level.ListURL = generateDescendantListURL(currentQuery, et, primaryKeyEntityType, primaryKeyValue)
							break
						}
					}
				}
				ctx.Descendants = append(ctx.Descendants, level)
			}
			contexts = append(contexts, ctx)
			continue
		}

		// Build ancestors (levels above current)
		for i := 0; i < currentIdx; i++ {
			et := levels[i]
			level := viewmodel.HierarchyLevel{
				EntityType:  et,
				DisplayName: formatEntityTypeName(et),
				Description: n.EntityTypeDescription(et),
			}

			// Find the value from row data
			if colName, ok := entityTypeToColumn[et]; ok {
				if value, ok := rowData[colName]; ok && value != "" {
					level.Value = value
					// Generate internal navigation URL to select this ancestor in its table
					level.ValueURL = generateAncestorURL(currentQuery, et, value)
				}
			}

			ctx.Ancestors = append(ctx.Ancestors, level)
		}

		// Build current level
		ctx.Current = viewmodel.HierarchyLevel{
			EntityType:  currentEntityType,
			DisplayName: formatEntityTypeName(currentEntityType),
			Value:       currentValue,
			Description: n.EntityTypeDescription(currentEntityType),
		}
		// If current level is not the item's primary key, make it a clickable link
		if currentEntityType != primaryKeyEntityType {
			ctx.Current.ValueURL = generateAncestorURL(currentQuery, currentEntityType, currentValue)
		}

		// Build descendants (levels below current)
		for i := currentIdx + 1; i < len(levels); i++ {
			et := levels[i]
			level := viewmodel.HierarchyLevel{
				EntityType:  et,
				Description: n.EntityTypeDescription(et),
			}

			// Check if row has a specific value for this descendant entity type
			// (e.g., a task row has a machine column)
			if colName, ok := entityTypeToColumn[et]; ok {
				if value, ok := rowData[colName]; ok && value != "" {
					// Row has a specific value - make it a direct link
					level.DisplayName = formatEntityTypeName(et) // Singular
					level.Value = value
					level.ValueURL = generateAncestorURL(currentQuery, et, value)
				} else {
					// No specific value - show as list link
					level.DisplayName = formatEntityTypeNamePlural(et)
					level.ListURL = generateDescendantListURL(currentQuery, et, currentEntityType, currentValue)
				}
			} else {
				// No column for this entity type - show as list link
				level.DisplayName = formatEntityTypeNamePlural(et)
				level.ListURL = generateDescendantListURL(currentQuery, et, currentEntityType, currentValue)
			}

			ctx.Descendants = append(ctx.Descendants, level)
		}

		contexts = append(contexts, ctx)
	}

	return contexts
}

// RelatedTables finds tables that have a column matching the selected item's
// primary key entity type, for the detail panel's "Related Tables" section.
// Tables whose own primary key is part of a hierarchy are excluded — those
// are already reachable through hierarchy navigation. Results follow the
// catalog's table order.
func (n *Navigator) RelatedTables(
	currentQuery *urlquery.Query,
	currentTableName string,
	primaryKeyEntityType string,
	primaryKeyValue string,
) []viewmodel.RelatedTable {
	var relatedTables []viewmodel.RelatedTable

	for i := range n.catalog.Tables {
		table := &n.catalog.Tables[i]
		// Skip the current table and system tables
		if table.Name == currentTableName || strings.HasPrefix(table.Name, "_") {
			continue
		}

		// Skip tables whose primary key is part of a hierarchy
		// (those are already shown in hierarchy navigation)
		if n.hierarchyEntityTypes[table.PrimaryKeyEntityType] {
			continue
		}

		// Check each column for a matching entity type
		for _, col := range table.Columns {
			if col.EntityType != primaryKeyEntityType {
				continue
			}
			// Found a matching column - generate the filter URL
			q := currentQuery.Clone()
			q.Path = "table"
			q.Table = table.Name
			q.ClearTableSpecificState()
			q.Filters[col.Name] = `"` + primaryKeyValue + `"`

			relatedTables = append(relatedTables, viewmodel.RelatedTable{
				TableName:        table.Name,
				DisplayName:      formatTableDisplayName(table.Name),
				ColumnName:       col.Name,
				ColumnEntityType: col.EntityType,
				FilterURL:        q.ToURL(),
			})

			// Only add one entry per table (even if multiple columns match)
			break
		}
	}

	return relatedTables
}

// expandTemplate replaces the {value} and {entity_type} placeholders in a
// URL template.
func expandTemplate(template, value, entityType string) string {
	result := strings.ReplaceAll(template, "{value}", value)
	return strings.ReplaceAll(result, "{entity_type}", entityType)
}

// formatEntityTypeName extracts a display name from an entity type.
// For example, "google.cluster" becomes "Cluster", "demo.order_id" becomes "Order Id".
func formatEntityTypeName(entityType string) string {
	// Remove prefix (e.g., "google." or "demo.")
	name := entityType
	if idx := strings.LastIndex(entityType, "."); idx != -1 {
		name = entityType[idx+1:]
	}

	// Convert underscores to spaces and title case
	name = strings.ReplaceAll(name, "_", " ")
	return strings.Title(name)
}

// formatEntityTypeNamePlural extracts a pluralized display name from an entity type.
// For example, "google.cluster" becomes "Clusters", "google.machine" becomes "Machines".
func formatEntityTypeNamePlural(entityType string) string {
	name := formatEntityTypeName(entityType)
	// Simple pluralization
	if strings.HasSuffix(name, "s") {
		return name + "es"
	}
	return name + "s"
}

// generateAncestorURL generates a URL to navigate to an ancestor's table with that row selected.
// For example, if viewing a machine and clicking on its cluster ancestor,
// this would generate a URL like "table?table=google_clusters&row=us-east-a-c0"
func generateAncestorURL(currentQuery *urlquery.Query, entityType, value string) string {
	q := currentQuery.Clone()
	q.Path = "table"
	q.Table = entityTypeToTableName(entityType)
	q.ClearTableSpecificState()
	q.SelectedRowID = value

	return q.ToURL()
}

// generateDescendantListURL generates a URL to list items of a descendant entity type
// filtered by the current item's value. Preserves non-table-specific query state.
//
// Uses the column name derived from the entity type for filtering. Most tables already
// have columns for their ancestors (e.g., machines has cluster, zone columns).
// For example, if viewing a cluster (google.cluster) and the descendant is "google.machine",
// this would generate a URL like "table?table=google_machines&filter:cluster=us-east-a-c0"
func generateDescendantListURL(currentQuery *urlquery.Query, descendantEntityType, parentEntityType, parentValue string) string {
	// Clone the current query to preserve non-table-specific state (limit, info pane, etc.)
	q := currentQuery.Clone()
	q.Path = "table"
	q.Table = entityTypeToTableName(descendantEntityType)
	q.ClearTableSpecificState()

	// Use the column name derived from entity type (e.g., "google.cluster" -> "cluster")
	// Tables typically have columns named after their ancestors directly
	columnName := entityTypeToColumnName(parentEntityType)
	q.Filters[columnName] = `"` + parentValue + `"`

	return q.ToURL()
}

// entityTypeToTableName converts an entity type to a table name.
// Convention: "google.cluster" -> "google_clusters", "google.machine" -> "google_machines"
func entityTypeToTableName(entityType string) string {
	// Remove prefix and add 's' for plural
	name := entityType
	if idx := strings.LastIndex(entityType, "."); idx != -1 {
		prefix := entityType[:idx]
		suffix := entityType[idx+1:]
		// Handle special pluralization
		if strings.HasSuffix(suffix, "s") {
			name = prefix + "_" + suffix + "es"
		} else {
			name = prefix + "_" + suffix + "s"
		}
	}
	return strings.ReplaceAll(name, ".", "_")
}

// entityTypeToColumnName extracts a column name from an entity type.
// Convention: "google.cluster" -> "cluster", "demo.order_id" -> "order_id"
func entityTypeToColumnName(entityType string) string {
	if idx := strings.LastIndex(entityType, "."); idx != -1 {
		return entityType[idx+1:]
	}
	return entityType
}

// formatTableDisplayName creates a user-friendly display name from a table name.
// Examples: "google_clusters" -> "Clusters", "customer_orders" -> "Customer Orders"
func formatTableDisplayName(tableName string) string {
	// Remove common prefixes
	name := tableName
	if idx := strings.Index(name, "_"); idx != -1 {
		// Check if the prefix looks like a namespace (e.g., "google_")
		prefix := name[:idx]
		if prefix == "google" || prefix == "demo" {
			name = name[idx+1:]
		}
	}

	// Replace underscores with spaces and title case
	name = strings.ReplaceAll(name, "_", " ")
	return strings.Title(name)
}
