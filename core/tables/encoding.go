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
	"slices"

	"github.com/google/taxinomia/core/columns"
)

// SelectEncodings replaces each column's storage with the encoding its role
// calls for (docs/scaling-to-1b-rows.md §5: encoding is chosen per role, not
// per threshold). It is meant to run once at load, after SortByKey has put
// the storage into its declared physical order; row order and the recorded
// sort key are unchanged, only representations are.
//
// Roles recognized today:
//
//   - Sort-key dimension (a column of the recorded sort key): dictionary
//     encoded regardless of row count — a declared dimension is
//     low-cardinality by role, so the size heuristic does not apply. The
//     cardinality cap still does.
//   - Other string columns: dictionary encoded when the size thresholds say
//     it pays (CompactChunkedStringColumn).
//   - Key columns (the primary key): left as-is — with every value distinct,
//     dictionary encoding is strictly negative. Their reverse-lookup map is
//     already replaced by the sparse-index binary search at FinalizeColumn
//     when the storage is sorted by them; front-coded arena storage is a
//     later phase.
//   - Numeric measures: plain chunked storage (bitpacked and delta encodings
//     are not built; they need a packed chunk representation first).
//
// Only in-repo chunked string columns are re-encoded; every other column type
// passes through untouched, so external column implementations are safe.
func (dt *DataTable) SelectEncodings() {
	for name, col := range dt.columns {
		sc, ok := col.(*columns.ChunkedStringColumn)
		if !ok {
			continue
		}
		var encoded columns.IDataColumn
		var changed bool
		if slices.Contains(dt.sortKey, name) {
			encoded, changed = columns.CompactDeclaredDimension(sc)
		} else {
			encoded, changed = columns.CompactChunkedStringColumn(sc)
		}
		if changed {
			dt.columns[name] = encoded
		}
	}
}
