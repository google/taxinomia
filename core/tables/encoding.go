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
//   - String primary key on sorted storage (unique, in value order):
//     front-coded arena — prefix-shared bytes plus the sparse index, the §5
//     "String PK" row. Dictionary encoding is strictly negative there
//     (d = n).
//   - Sort-key dimension (a column of the recorded sort key): dictionary
//     encoded regardless of row count — a declared dimension is
//     low-cardinality by role, so the size heuristic does not apply. The
//     cardinality cap still does.
//   - Other string columns: dictionary encoded when the size thresholds say
//     it pays (CompactChunkedStringColumn); otherwise a plain byte arena —
//     the high-cardinality fallback is never []string (§5), so no loaded
//     string column keeps per-row string headers.
//   - Numeric measures: plain chunked storage (bitpacked and delta encodings
//     are not built; they need a packed chunk representation first).
//
// Only in-repo chunked string columns are re-encoded; every other column type
// passes through untouched, so external column implementations are safe.
func (dt *DataTable) SelectEncodings() {
	dt.encodeOnce.Do(func() {})
	dt.selectEncodings()
}

// EnsureEncodings runs SelectEncodings the first time it is called and is a
// no-op afterwards, so the query pipeline can guarantee encoded storage
// without depending on every loader remembering the call. A table that
// never had its encodings selected groups ~20x slower on dictionary-shaped
// columns. Safe for concurrent first queries; a loader that already called
// SelectEncodings pays nothing here.
func (dt *DataTable) EnsureEncodings() {
	dt.encodeOnce.Do(dt.selectEncodings)
}

func (dt *DataTable) selectEncodings() {
	for name, col := range dt.columns {
		sc, ok := col.(*columns.ChunkedStringColumn)
		if !ok {
			continue
		}
		if fc, ok := columns.FrontCodeChunkedStringColumn(sc); ok {
			dt.columns[name] = fc
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
			continue
		}
		dt.columns[name] = columns.ArenaEncodeChunkedStringColumn(sc)
	}
}
