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
	"strings"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

func computedTestTable() *tables.DataTable {
	dt := tables.NewDataTable()
	q := columns.NewChunkedInt64Column(columns.NewColumnDef("quantity", "Quantity", ""))
	q.Append(2)
	q.FinalizeColumn()
	dt.AddColumn(q)
	return dt
}

func TestApplyComputedColumns(t *testing.T) {
	source := &DataSource{
		Name: "t",
		ComputedColumns: []*ComputedColumn{
			{Name: "double", DisplayName: "Double", Expression: "quantity * 2", EntityType: "demo.x"},
			{Name: "quad", Expression: "double * 2"},
		},
	}
	dt := computedTestTable()
	if err := applyComputedColumns(source, dt); err != nil {
		t.Fatalf("applyComputedColumns: %v", err)
	}
	got := dt.ComputedDefinitions()
	want := []tables.ComputedDefinition{
		{Name: "double", DisplayName: "Double", Expression: "quantity * 2", EntityType: "demo.x"},
		{Name: "quad", Expression: "double * 2"},
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("definitions = %+v, want %+v", got, want)
	}

	// No computed columns: nothing declared, no error.
	dt = computedTestTable()
	if err := applyComputedColumns(&DataSource{Name: "t"}, dt); err != nil || len(dt.ComputedDefinitions()) != 0 {
		t.Errorf("empty: err=%v defs=%v", err, dt.ComputedDefinitions())
	}

	// A broken definition fails the load, naming the column.
	for _, tc := range []struct {
		col  *ComputedColumn
		want string
	}{
		{&ComputedColumn{Name: "bad", Expression: "quantity *"}, `"bad"`},
		{&ComputedColumn{Name: "quantity", Expression: "1"}, "stored column"},
	} {
		err := applyComputedColumns(&DataSource{Name: "t", ComputedColumns: []*ComputedColumn{tc.col}}, computedTestTable())
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: error = %v, want %q", tc.col, err, tc.want)
		}
	}
}
