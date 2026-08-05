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
	"fmt"
	"slices"

	"github.com/google/taxinomia/core/tables"
)

// applySortKey sorts a freshly loaded table into its declared physical order
// (the DataSource.sort_key field), before the table is published.
//
// The effective sort key is the declared column list with the primary key
// column appended as the final tie-breaker (unless already listed), making
// the order total. With no declaration the table is sorted by the primary
// key column alone. With no resolvable primary key column either, the table
// stays in load order.
//
// A declared sort key that cannot be honored (unknown column, a column type
// that cannot be reordered) is a configuration error. The declaration-free
// default is an optimization only: if the table cannot be reordered — custom
// loaders may build column types without reorder support — it silently stays
// in load order, which is correct, just slower.
func applySortKey(source *DataSource, table *tables.DataTable) error {
	pkCol := primaryKeyColumn(source.GetPrimaryKeyEntityType(), table)
	declared := source.GetSortKey()
	if len(declared) == 0 {
		if pkCol == "" {
			return nil
		}
		table.SortByKey([]string{pkCol}) //nolint:errcheck // default sort is best-effort
		return nil
	}
	key := slices.Clone(declared)
	if pkCol != "" && !slices.Contains(key, pkCol) {
		key = append(key, pkCol)
	}
	if err := table.SortByKey(key); err != nil {
		return fmt.Errorf("declared sort key %v: %w", declared, err)
	}
	return nil
}

// primaryKeyColumn resolves a source's primary key entity type to the table
// column carrying it. Returns "" when the entity type is empty, no column
// carries it, or more than one does (ambiguous — a primary key is one
// column).
func primaryKeyColumn(entityType string, table *tables.DataTable) string {
	if entityType == "" {
		return ""
	}
	found := ""
	for _, name := range table.GetColumnNames() {
		if table.GetColumn(name).ColumnDef().EntityType() == entityType {
			if found != "" {
				return ""
			}
			found = name
		}
	}
	return found
}
