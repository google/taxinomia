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
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/queryspec"
)

// disableBulkAggregates forces the per-row leaf aggregate path. Tests and
// benchmarks only, to compare the two paths on identical data; never set in
// production code.
var disableBulkAggregates = false

// bulkAggregatedColumns counts leaf columns whose level-0 aggregates were
// assembled from pre-aggregated partials. Tests read it to assert the fast
// path actually engaged rather than silently falling back.
var bulkAggregatedColumns atomic.Uint64

// level0CodeAggs holds, per leaf column that took the fast path, the merged
// per-dictionary-code aggregate partials for the level-0 grouping. Group keys
// of the level-0 block are dictionary codes exactly when the column's
// PerCodeAggs handles the selection, so indexing by Group.GroupKey is sound.
type level0CodeAggs map[string]*columns.CodeAggs

// bulkLevel0Aggregates computes per-code aggregate partials for every leaf
// column the level-0 grouping column can pre-aggregate
// (docs/scaling-to-1b-rows.md §4): fully-covered chunks are merged from
// cached per-chunk summaries instead of being rescanned per row. Columns not
// handled — unsupported measure types, non-dictionary grouping columns,
// small-subset selections — are absent from the result and take the per-row
// path unchanged. Returns nil when nothing would consume the partials.
func (tv *TableView) bulkLevel0Aggregates(ctx context.Context, leafColumns []string, columnTypes map[string]queryspec.ColumnType) (level0CodeAggs, error) {
	if disableBulkAggregates || tv.firstBlock == nil {
		return nil, nil
	}
	gcol := tv.firstBlock.GroupedColumn
	if gcol == nil {
		return nil, nil
	}
	src, ok := gcol.DataColumn.(columns.PerCodeAggSource)
	if !ok {
		return nil, nil
	}
	// Repeat calls keep already-computed level-0 aggregates; merge only when
	// some level-0 leaf group actually needs them.
	needed := false
	for _, g := range tv.firstBlock.Groups {
		if g.ChildBlock == nil && (g.Indices != nil || g.Aggregates == nil) {
			needed = true
			break
		}
	}
	if !needed {
		return nil, nil
	}
	sel := tv.rowSet()
	var bulk level0CodeAggs
	for _, colName := range leafColumns {
		col := tv.GetColumn(colName)
		if col == nil {
			continue
		}
		step := tv.stepStart()
		aggs, handled, err := src.PerCodeAggs(ctx, sel, col)
		if err != nil {
			return nil, err
		}
		// The partial kind must agree with the type the per-row path would
		// aggregate the column as, or formatting semantics would change.
		if !handled || !codeAggsMatchType(aggs, columnTypes[colName]) {
			continue
		}
		if bulk == nil {
			bulk = level0CodeAggs{}
		}
		bulk[colName] = aggs
		bulkAggregatedColumns.Add(1)
		tv.recordStep(fmt.Sprintf("aggregates level 0 for %s: merge per-chunk partials", colName), step)
	}
	return bulk, nil
}

// codeAggsMatchType reports whether the partial kind PerCodeAggs produced is
// the one computeLeafAggregates would accumulate for a column of this type.
func codeAggsMatchType(a *columns.CodeAggs, colType queryspec.ColumnType) bool {
	switch colType {
	case queryspec.ColumnTypeNumeric:
		return a.Numeric != nil
	case queryspec.ColumnTypeBool:
		return a.Bool != nil
	case queryspec.ColumnTypeDatetime:
		return a.Datetime != nil
	default:
		return false
	}
}

// codeAggState assembles the aggregate state for one group from the merged
// per-code partials. The field-for-field copies land exactly where per-row
// accumulation into a fresh state would: the partials' initial values are the
// states' initial values, and their add/combine semantics mirror the states'.
// Returns nil (caller falls back to per-row) when the group key is outside
// the dictionary — impossible for groups built on the dense path, checked
// defensively.
func codeAggState(a *columns.CodeAggs, code uint32) aggregates.AggregateState {
	switch {
	case a.Numeric != nil:
		if int(code) >= len(a.Numeric) {
			return nil
		}
		p := a.Numeric[code]
		return &aggregates.NumericAggState{Count: p.Count, Sum: p.Sum, SumSq: p.SumSq, Min: p.Min, Max: p.Max}
	case a.Bool != nil:
		if int(code) >= len(a.Bool) {
			return nil
		}
		p := a.Bool[code]
		return &aggregates.BoolAggState{Count: p.Count, TrueCount: p.True, FalseCount: p.Count - p.True}
	case a.Datetime != nil:
		if int(code) >= len(a.Datetime) {
			return nil
		}
		p := a.Datetime[code]
		return &aggregates.DatetimeAggState{Count: p.Count, Sum: p.Sum, SumSq: p.SumSq, Min: p.Min, Max: p.Max}
	default:
		return nil
	}
}
