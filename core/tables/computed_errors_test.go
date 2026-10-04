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
	"errors"
	"testing"

	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/queryspec"
)

// A view with "inv" = 100 / d, which fails where d is 0 (rows 1 and 3).
func failingComputedView(t *testing.T) *TableView {
	t.Helper()
	dt := NewDataTable()
	d := columns.NewChunkedInt64Column(columns.NewColumnDef("d", "D", ""))
	for _, v := range []int64{5, 0, 10, 0, 20} {
		d.Append(v)
	}
	d.FinalizeColumn()
	dt.AddColumn(d)
	tv := NewTableView(dt, "t")
	inv := columns.NewComputedFloat64Column(columns.NewColumnDef("inv", "inv", ""), 5, func(i uint32) (float64, error) {
		v, err := d.GetValue(i)
		if err != nil {
			return 0, err
		}
		if v == 0 {
			return 0, errors.New("division by zero")
		}
		return 100 / float64(v), nil
	})
	tv.AddComputedColumn("inv", inv)
	return tv
}

// Failing rows match a filter only by their label, never by a substring of it.
func TestFilterMatchesFailedRowsByLabel(t *testing.T) {
	for _, tc := range []struct {
		filter string
		want   int
	}{
		{"[error]", 2},     // the label, as a substring filter
		{"\"[error]\"", 2}, // exact
		{"[error]|20", 3},  // listed among values (20 = 100/5)
		{"e", 0},           // a substring of the label: no match
		{"2", 1},           // 20 only
	} {
		tv := failingComputedView(t)
		tv.ApplyFilters(map[string]string{"inv": tc.filter})
		if got := tv.GetFilteredRowCount(); got != tc.want {
			t.Errorf("filter %q: %d rows, want %d", tc.filter, got, tc.want)
		}
	}
}

// Numeric aggregates leave failing rows out and count them; Combine carries
// the count; the formatted aggregates report it.
func TestNumericAggregatesCountFailedRows(t *testing.T) {
	tv := failingComputedView(t)
	col := tv.GetColumn("inv")
	state := aggregates.NewNumericAggState()
	for i := uint32(0); i < 5; i++ {
		tv.addNumericValue(state, col, i)
	}
	if state.Count != 3 || state.Failed != 2 {
		t.Fatalf("count=%d failed=%d, want 3 and 2", state.Count, state.Failed)
	}
	other := aggregates.NewNumericAggState()
	other.Failed = 1
	state.Combine(other)
	if state.Failed != 3 {
		t.Errorf("after Combine failed=%d, want 3", state.Failed)
	}
	formatted := aggregates.FormatAggregates(state, nil)
	if formatted != nil {
		t.Errorf("no aggregates enabled: got %v", formatted)
	}
	formatted = aggregates.FormatAggregates(state, []queryspec.AggregateType{queryspec.AggSum})
	if len(formatted) != 2 || formatted[1].Value != "3" {
		t.Errorf("formatted = %+v, want the sum and a failed count of 3", formatted)
	}

	// The column remembers why.
	if fe, ok := col.(interface{ FirstError() string }); !ok || fe.FirstError() != "division by zero" {
		t.Errorf("FirstError not recorded")
	}
}
