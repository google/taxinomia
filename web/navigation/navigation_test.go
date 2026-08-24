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

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/google/taxinomia/core/engine"
	"github.com/google/taxinomia/web/urlquery"
	"github.com/google/taxinomia/web/viewmodel"
)

// testCatalog mirrors the demo setup in miniature: a physical hierarchy over
// google entity types, plus order tables sharing a primary key entity type.
func testCatalog() *engine.Catalog {
	return &engine.Catalog{
		Tables: []engine.TableMeta{
			{
				Name: "_columns",
				Columns: []engine.ColumnMeta{
					{Name: "order_id", EntityType: "demo.order_id"},
				},
			},
			{
				Name:                 "customer_orders",
				PrimaryKeyEntityType: "demo.order_id",
				Columns: []engine.ColumnMeta{
					{Name: "order_id", EntityType: "demo.order_id", IsKey: true},
					{Name: "customer_id", EntityType: "demo.customer_id"},
				},
			},
			{
				Name:                 "customer_orders_binary",
				PrimaryKeyEntityType: "demo.order_id",
				Columns: []engine.ColumnMeta{
					{Name: "order_id", EntityType: "demo.order_id", IsKey: true},
					{Name: "order_ref", EntityType: "demo.order_id"},
				},
			},
			{
				Name:                 "google_clusters",
				PrimaryKeyEntityType: "google.cluster",
				Columns: []engine.ColumnMeta{
					{Name: "cluster", EntityType: "google.cluster", IsKey: true},
					{Name: "zone", EntityType: "google.zone"},
					{Name: "region", EntityType: "google.region"},
				},
			},
			{
				Name:                 "google_machines",
				PrimaryKeyEntityType: "google.machine",
				Columns: []engine.ColumnMeta{
					{Name: "machine", EntityType: "google.machine", IsKey: true},
					{Name: "cluster", EntityType: "google.cluster"},
					{Name: "zone", EntityType: "google.zone"},
					{Name: "region", EntityType: "google.region"},
				},
			},
			{
				Name:                 "google_regions",
				PrimaryKeyEntityType: "google.region",
				Columns: []engine.ColumnMeta{
					{Name: "region", EntityType: "google.region", IsKey: true},
				},
			},
		},
		Hierarchies: []engine.Hierarchy{
			{
				Name:        "google.infrastructure",
				Description: "Physical infrastructure",
				Levels:      []string{"google.region", "google.zone", "google.cluster", "google.machine"},
			},
			{
				Name:   "google.workloads",
				Levels: []string{"google.cell", "google.job"},
			},
		},
		EntityTypes: []engine.EntityTypeMeta{
			{
				Name:        "demo.order_id",
				Description: "Unique identifier for an order",
				URLs: []engine.URLTemplate{
					{Name: "Order Details", Template: "https://orders.example.com/orders/{value}"},
					{Name: "Tracking", Template: "https://track.example.com/{entity_type}/{value}", IsDefault: true},
				},
			},
			{
				Name:        "google.region",
				Description: "A geographic region",
				URLs: []engine.URLTemplate{
					{Name: "Region Dashboard", Template: "https://dash.example.com/regions/{value}"},
				},
			},
			{Name: "google.cluster", Description: "A machine cluster"},
			{Name: "google.zone", Description: "A data center zone"},
		},
	}
}

func testQuery(t *testing.T, rawURL string) *urlquery.Query {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("bad url %q: %v", rawURL, err)
	}
	return urlquery.NewQuery(u)
}

func TestResolveDefaultURL(t *testing.T) {
	n := NewNavigator(testCatalog())

	// The template marked default wins even when it is not first, and both
	// placeholders are expanded.
	if got, want := n.ResolveDefaultURL("demo.order_id", "ORD-1"),
		"https://track.example.com/demo.order_id/ORD-1"; got != want {
		t.Errorf("default-flagged template: got %q, want %q", got, want)
	}
	// Without a default flag the first template is used.
	if got, want := n.ResolveDefaultURL("google.region", "us-east"),
		"https://dash.example.com/regions/us-east"; got != want {
		t.Errorf("first template fallback: got %q, want %q", got, want)
	}
	// No URLs declared, and unknown entity type.
	if got := n.ResolveDefaultURL("google.cluster", "c0"); got != "" {
		t.Errorf("no templates: got %q, want empty", got)
	}
	if got := n.ResolveDefaultURL("nope", "x"); got != "" {
		t.Errorf("unknown entity type: got %q, want empty", got)
	}
}

func TestAllURLs(t *testing.T) {
	n := NewNavigator(testCatalog())

	got := n.AllURLs("demo.order_id", "ORD-1")
	want := []viewmodel.EntityURL{
		{Name: "Order Details", URL: "https://orders.example.com/orders/ORD-1"},
		{Name: "Tracking", URL: "https://track.example.com/demo.order_id/ORD-1"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("AllURLs: got %+v, want %+v", got, want)
	}
	if got := n.AllURLs("google.cluster", "c0"); got != nil {
		t.Errorf("no templates: got %+v, want nil", got)
	}
	if got := n.AllURLs("nope", "x"); got != nil {
		t.Errorf("unknown entity type: got %+v, want nil", got)
	}
}

func TestPrimaryKeyEntityTypeAndDescription(t *testing.T) {
	n := NewNavigator(testCatalog())

	if got, want := n.PrimaryKeyEntityType("google_machines"), "google.machine"; got != want {
		t.Errorf("PrimaryKeyEntityType: got %q, want %q", got, want)
	}
	if got := n.PrimaryKeyEntityType("nope"); got != "" {
		t.Errorf("unknown table: got %q, want empty", got)
	}
	if got, want := n.EntityTypeDescription("google.zone"), "A data center zone"; got != want {
		t.Errorf("EntityTypeDescription: got %q, want %q", got, want)
	}
	if got := n.EntityTypeDescription("nope"); got != "" {
		t.Errorf("unknown entity type: got %q, want empty", got)
	}
}

// TestHierarchyContextsPrimaryKeyInHierarchy: the selected item's primary key
// entity type is a hierarchy level — ancestors above it get values (and value
// URLs) from row data, the current level is not linked.
func TestHierarchyContextsPrimaryKeyInHierarchy(t *testing.T) {
	n := NewNavigator(testCatalog())
	q := testQuery(t, "/default/table?table=google_machines&limit=25&row=m01")

	rowData := map[string]string{
		"machine": "m01",
		"cluster": "us-east-a-c0",
		"zone":    "us-east-a",
		"region":  "us-east",
	}
	columnEntityTypes := map[string]string{
		"machine": "google.machine",
		"cluster": "google.cluster",
		"zone":    "google.zone",
		"region":  "google.region",
	}

	contexts := n.HierarchyContexts(q, "google.machine", "m01", rowData, columnEntityTypes)
	if len(contexts) != 2 {
		t.Fatalf("expected 2 contexts (one per hierarchy), got %d", len(contexts))
	}

	infra := contexts[0]
	if infra.HierarchyName != "google.infrastructure" || infra.Description != "Physical infrastructure" {
		t.Errorf("infra header: %+v", infra)
	}
	wantAncestors := []viewmodel.HierarchyLevel{
		{
			EntityType:  "google.region",
			DisplayName: "Region",
			Value:       "us-east",
			ValueURL:    "table?limit=25&row=us-east&table=google_regions",
			Description: "A geographic region",
		},
		{
			EntityType:  "google.zone",
			DisplayName: "Zone",
			Value:       "us-east-a",
			ValueURL:    "table?limit=25&row=us-east-a&table=google_zones",
			Description: "A data center zone",
		},
		{
			EntityType:  "google.cluster",
			DisplayName: "Cluster",
			Value:       "us-east-a-c0",
			ValueURL:    "table?limit=25&row=us-east-a-c0&table=google_clusters",
			Description: "A machine cluster",
		},
	}
	if !reflect.DeepEqual(infra.Ancestors, wantAncestors) {
		t.Errorf("infra ancestors:\n got %+v\nwant %+v", infra.Ancestors, wantAncestors)
	}
	wantCurrent := viewmodel.HierarchyLevel{
		EntityType:  "google.machine",
		DisplayName: "Machine",
		Value:       "m01",
	}
	if !reflect.DeepEqual(infra.Current, wantCurrent) {
		t.Errorf("infra current: got %+v, want %+v", infra.Current, wantCurrent)
	}
	if len(infra.Descendants) != 0 {
		t.Errorf("machine is the leaf level; descendants: %+v", infra.Descendants)
	}

	// The workloads hierarchy does not contain the item: every level is a
	// potential descendant, without list URLs because no google_cells /
	// google_jobs table with a google.machine column exists in the catalog.
	workloads := contexts[1]
	wantWorkloads := []viewmodel.HierarchyLevel{
		{EntityType: "google.cell", DisplayName: "Cells"},
		{EntityType: "google.job", DisplayName: "Jobs"},
	}
	if !reflect.DeepEqual(workloads.Descendants, wantWorkloads) {
		t.Errorf("workloads descendants:\n got %+v\nwant %+v", workloads.Descendants, wantWorkloads)
	}
}

// TestHierarchyContextsDescendants: a mid-hierarchy item links its known
// descendant levels as filtered list URLs, via the naming conventions
// (entity type -> table name, entity type -> column name).
func TestHierarchyContextsDescendants(t *testing.T) {
	n := NewNavigator(testCatalog())
	q := testQuery(t, "/default/table?table=google_clusters&limit=5&row=us-east-a-c0")

	rowData := map[string]string{
		"cluster": "us-east-a-c0",
		"zone":    "us-east-a",
		"region":  "us-east",
	}
	columnEntityTypes := map[string]string{
		"cluster": "google.cluster",
		"zone":    "google.zone",
		"region":  "google.region",
	}

	contexts := n.HierarchyContexts(q, "google.cluster", "us-east-a-c0", rowData, columnEntityTypes)
	infra := contexts[0]
	wantDescendants := []viewmodel.HierarchyLevel{
		{
			EntityType:  "google.machine",
			DisplayName: "Machines",
			ListURL:     "table?filter%3Acluster=%22us-east-a-c0%22&limit=5&table=google_machines",
		},
	}
	if !reflect.DeepEqual(infra.Descendants, wantDescendants) {
		t.Errorf("descendants:\n got %+v\nwant %+v", infra.Descendants, wantDescendants)
	}
}

// TestHierarchyContextsDeepestColumnMatch: the primary key is not a level of
// the hierarchy, so the position is the deepest level with a row value, and
// the current level links to its own table.
func TestHierarchyContextsDeepestColumnMatch(t *testing.T) {
	n := NewNavigator(testCatalog())
	q := testQuery(t, "/default/table?table=tasks&limit=25&row=t1")

	rowData := map[string]string{"cluster": "us-east-a-c0", "zone": "us-east-a"}
	columnEntityTypes := map[string]string{"cluster": "google.cluster", "zone": "google.zone"}

	contexts := n.HierarchyContexts(q, "task.id", "t1", rowData, columnEntityTypes)
	infra := contexts[0]
	if infra.Current.EntityType != "google.cluster" || infra.Current.Value != "us-east-a-c0" {
		t.Fatalf("current should be the deepest matched level: %+v", infra.Current)
	}
	if infra.Current.ValueURL == "" {
		t.Error("current level is not the primary key, so it must link to its table")
	}
	if len(infra.Ancestors) != 2 {
		t.Fatalf("expected region+zone ancestors, got %+v", infra.Ancestors)
	}
	if infra.Ancestors[0].Value != "" {
		t.Errorf("region has no column in this table, so no value: %+v", infra.Ancestors[0])
	}
	if infra.Ancestors[1].Value != "us-east-a" {
		t.Errorf("zone ancestor value: %+v", infra.Ancestors[1])
	}
}

func TestRelatedTables(t *testing.T) {
	n := NewNavigator(testCatalog())
	q := testQuery(t, "/default/table?table=customer_orders&limit=25&row=ORD-1")

	got := n.RelatedTables(q, "customer_orders", "demo.order_id", "ORD-1")
	// customer_orders is the current table, _columns is a system table, and
	// every google table's primary key is a hierarchy level — only
	// customer_orders_binary remains, with one entry despite two matching
	// columns (the first wins).
	want := []viewmodel.RelatedTable{
		{
			TableName:        "customer_orders_binary",
			DisplayName:      "Customer Orders Binary",
			ColumnName:       "order_id",
			ColumnEntityType: "demo.order_id",
			FilterURL:        "table?filter%3Aorder_id=%22ORD-1%22&limit=25&table=customer_orders_binary",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("RelatedTables:\n got %+v\nwant %+v", got, want)
	}
}

func TestNamingHelpers(t *testing.T) {
	cases := []struct{ entityType, table, column, name, plural string }{
		{"google.cluster", "google_clusters", "cluster", "Cluster", "Clusters"},
		{"google.alloc", "google_allocs", "alloc", "Alloc", "Allocs"},
		{"demo.order_status", "demo_order_statuses", "order_status", "Order Status", "Order Statuses"},
		{"region", "region", "region", "Region", "Regions"},
	}
	for _, c := range cases {
		if got := entityTypeToTableName(c.entityType); got != c.table {
			t.Errorf("entityTypeToTableName(%q) = %q, want %q", c.entityType, got, c.table)
		}
		if got := entityTypeToColumnName(c.entityType); got != c.column {
			t.Errorf("entityTypeToColumnName(%q) = %q, want %q", c.entityType, got, c.column)
		}
		if got := formatEntityTypeName(c.entityType); got != c.name {
			t.Errorf("formatEntityTypeName(%q) = %q, want %q", c.entityType, got, c.name)
		}
		if got := formatEntityTypeNamePlural(c.entityType); got != c.plural {
			t.Errorf("formatEntityTypeNamePlural(%q) = %q, want %q", c.entityType, got, c.plural)
		}
	}
	if got, want := formatTableDisplayName("google_clusters"), "Clusters"; got != want {
		t.Errorf("formatTableDisplayName: got %q, want %q", got, want)
	}
	if got, want := formatTableDisplayName("customer_orders"), "Customer Orders"; got != want {
		t.Errorf("formatTableDisplayName: got %q, want %q", got, want)
	}
}

// TestExpandTemplateEscapesValues: substituted values must not be able to
// rewrite the URL's path or query structure on the target host — metacharacters
// percent-encode, in both path-position and query-position templates.
func TestExpandTemplateEscapesValues(t *testing.T) {
	cases := []struct {
		name     string
		template string
		value    string
		want     string
	}{
		{"plain id, path position", "https://crm.example.com/customers/{value}", "C-0001",
			"https://crm.example.com/customers/C-0001"},
		{"query injection blocked", "https://support.example.com/tickets?customer={value}", "x&admin=true",
			"https://support.example.com/tickets?customer=x%26admin%3Dtrue"},
		{"path traversal blocked", "https://crm.example.com/customers/{value}", "../../logout",
			"https://crm.example.com/customers/..%2F..%2Flogout"},
		{"fragment and space", "https://en.wikipedia.org/wiki/{value}", "New York#History",
			"https://en.wikipedia.org/wiki/New%20York%23History"},
		{"entity type placeholder", "https://ops.example.com/{entity_type}/{value}", "a/b",
			"https://ops.example.com/demo.status/a%2Fb"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := expandTemplate(tc.template, tc.value, "demo.status"); got != tc.want {
				t.Errorf("expandTemplate(%q, %q) = %q, want %q", tc.template, tc.value, got, tc.want)
			}
		})
	}
}
