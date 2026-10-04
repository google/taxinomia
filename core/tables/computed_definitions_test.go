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
