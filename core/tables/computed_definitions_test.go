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
	"strings"
	"testing"

	"github.com/google/taxinomia/core/columns"
)

func TestComputedDefinitions(t *testing.T) {
	dt := NewDataTable()
	amount := columns.NewChunkedInt64Column(columns.NewColumnDef("amount", "Amount", ""))
	amount.Append(5)
	amount.FinalizeColumn()
	dt.AddColumn(amount)

	defs := []ComputedDefinition{
		{Name: "double", Expression: "amount * 2"},
		{Name: "quad", DisplayName: "Quadruple", Expression: "double * 2"},
	}
	if err := dt.SetComputedDefinitions(defs); err != nil {
		t.Fatalf("SetComputedDefinitions: %v", err)
	}
	defs[0].Name = "changed" // the table keeps its own copy
	got := dt.ComputedDefinitions()
	if len(got) != 2 || got[0].Name != "double" || got[1].DisplayName != "Quadruple" {
		t.Fatalf("ComputedDefinitions = %+v", got)
	}

	for _, tc := range []struct {
		defs []ComputedDefinition
		want string
	}{
		{[]ComputedDefinition{{Name: "", Expression: "1"}}, "without a name"},
		{[]ComputedDefinition{{Name: "x"}}, "no expression"},
		{[]ComputedDefinition{{Name: "x", Expression: "1"}, {Name: "x", Expression: "2"}}, "declared twice"},
		{[]ComputedDefinition{{Name: "amount", Expression: "1"}}, "stored column"},
		{[]ComputedDefinition{{Name: "a.b", Expression: "1"}}, "contains one of"},
	} {
		err := dt.SetComputedDefinitions(tc.defs)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("SetComputedDefinitions(%+v) error = %v, want %q", tc.defs, err, tc.want)
		}
	}
}

type identityJoiner struct{}

func (identityJoiner) Lookup(i uint32) (uint32, error) { return i, nil }

// A joined column attached to a table reports the length of the table it
// comes from; the table's length must come from its stored columns, on
// every call (map order used to decide).
func TestDataTableLengthIgnoresJoinedColumns(t *testing.T) {
	dt := NewDataTable()
	stored := columns.NewChunkedInt64Column(columns.NewColumnDef("amount", "Amount", ""))
	for i := 0; i < 5; i++ {
		stored.Append(int64(i))
	}
	stored.FinalizeColumn()
	other := columns.NewChunkedStringColumn(columns.NewColumnDef("zone", "Zone", ""))
	for _, v := range []string{"a", "b", "c"} {
		other.Append(v)
	}
	other.FinalizeColumn()
	dt.AddColumn(stored)
	for i := 0; i < 8; i++ { // several joined columns make the old bug near-certain
		dt.AddColumn(other.CreateJoinedColumn(columns.NewColumnDef(fmt.Sprintf("j%d", i), "J", ""), identityJoiner{}))
	}
	for i := 0; i < 50; i++ {
		if n := dt.Length(); n != 5 {
			t.Fatalf("Length() = %d on call %d, want 5", n, i)
		}
	}
}
