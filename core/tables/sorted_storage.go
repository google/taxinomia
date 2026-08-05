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

package tables

import (
	"fmt"
	"sort"

	"github.com/google/taxinomia/core/columns"
)

// SortByKey sorts the table's physical storage by the given columns, most
// significant first, and records the result (docs/scaling-to-1b-rows.md §5:
// sort order is a query-performance decision — long runs in the leading
// columns, effective zone-map pruning, contiguous chunk ranges per group).
//
// One permutation is computed from the sort key using each column's value
// ordering, then every column is replaced by its reordered copy so all
// columns stay row-aligned. The copies are finalized, so zone maps describe
// the sorted data. When the sort key does not order the rows totally (no
// unique tie-breaker), equal rows keep their load order.
//
// It fails without modifying the table if a sort key column is missing or if
// any column does not implement columns.Reorderable (external column
// implementations); an unsorted table stays correct, just slower.
func (dt *DataTable) SortByKey(sortKey []string) error {
	if len(sortKey) == 0 {
		return fmt.Errorf("SortByKey: empty sort key")
	}
	keyCols := make([]columns.IDataColumn, len(sortKey))
	for i, name := range sortKey {
		col, ok := dt.columns[name]
		if !ok {
			return fmt.Errorf("SortByKey: sort key column %q not in table", name)
		}
		keyCols[i] = col
	}
	for name, col := range dt.columns {
		if _, ok := col.(columns.Reorderable); !ok {
			return fmt.Errorf("SortByKey: column %q (%T) does not support reordering", name, col)
		}
	}

	n := dt.Length()
	perm := make([]uint32, n)
	for i := range perm {
		perm[i] = uint32(i)
	}
	sort.SliceStable(perm, func(a, b int) bool {
		for _, col := range keyCols {
			if c := columns.CompareAtIndex(col, perm[a], perm[b]); c != 0 {
				return c < 0
			}
		}
		return false
	})

	for name, col := range dt.columns {
		dt.columns[name] = col.(columns.Reorderable).Reorder(perm)
	}
	dt.sortKey = append([]string(nil), sortKey...)
	return nil
}

// SortKey returns the column names the table's storage is physically sorted
// by (most significant first), or nil when the storage order is unspecified
// (load order). Sortedness is a recorded property of the stored table: it is
// set only by SortByKey, never inferred.
func (dt *DataTable) SortKey() []string {
	return dt.sortKey
}
