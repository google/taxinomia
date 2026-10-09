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

package aggregates

import (
	"testing"

	"github.com/google/taxinomia/core/queryspec"
)

func TestBoolAnyAll(t *testing.T) {
	mixed, allTrue, allFalse, empty := NewBoolAggState(), NewBoolAggState(), NewBoolAggState(), NewBoolAggState()
	for _, v := range []bool{true, false, true} {
		mixed.Add(v)
	}
	allTrue.Add(true)
	allTrue.Add(true)
	allFalse.Add(false)
	cases := []struct {
		s         *BoolAggState
		any, all_ string
	}{
		{mixed, "true", "false"},
		{allTrue, "true", "true"},
		{allFalse, "false", "false"},
		{empty, "-", "-"},
	}
	for i, c := range cases {
		if got := c.s.Format(queryspec.AggAny); got != c.any {
			t.Errorf("case %d: any = %s, want %s", i, got, c.any)
		}
		if got := c.s.Format(queryspec.AggAll); got != c.all_ {
			t.Errorf("case %d: all = %s, want %s", i, got, c.all_)
		}
	}
	// In a chip, the word symbol is followed by a space: "any true".
	chips := FormatAggregatesWithSort(mixed, []queryspec.AggregateType{queryspec.AggTrue, queryspec.AggAny}, "", "")
	if chips[0].Symbol+chips[0].Value != "✓2" || chips[1].Symbol+chips[1].Value != "any true" {
		t.Errorf("chips = %q %q", chips[0].Symbol+chips[0].Value, chips[1].Symbol+chips[1].Value)
	}
}
