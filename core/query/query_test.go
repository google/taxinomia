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

package query

import (
	"net/url"
	"testing"
)

// TestColumnReorderingOnGrouping tests that columns are reordered when grouping is toggled
func TestColumnReorderingOnGrouping(t *testing.T) {
	// Test 1: Group a middle column - it should move to first position
	t.Run("Group middle column", func(t *testing.T) {
		baseURL, _ := url.Parse("/table?table=test&columns=status,region,category,amount")
		q := NewQuery(baseURL)

		// Group the second column (region)
		newURL := q.WithGroupedColumnToggled("region")
		parsedURL, _ := url.Parse(newURL.String())
		newState := NewQuery(parsedURL)

		if len(newState.GroupedColumns) != 1 || newState.GroupedColumns[0] != "region" {
			t.Errorf("Expected grouped columns [region], got %v", newState.GroupedColumns)
		}
		expected := []string{"region", "status", "category", "amount"}
		if !equalStringSlices(newState.Columns, expected) {
			t.Errorf("Expected columns %v, got %v", expected, newState.Columns)
		}
	})

	// Test 2: Group multiple columns in sequence
	t.Run("Group multiple columns", func(t *testing.T) {
		baseURL, _ := url.Parse("/table?table=test&columns=status,region,category,amount")
		q := NewQuery(baseURL)

		// Group status (first column)
		url1 := q.WithGroupedColumnToggled("status")
		parsed1, _ := url.Parse(url1.String())
		q1 := NewQuery(parsed1)

		// Group category (third column)
		url2 := q1.WithGroupedColumnToggled("category")
		parsed2, _ := url.Parse(url2.String())
		q2 := NewQuery(parsed2)

		expectedGrouped := []string{"status", "category"}
		if !equalStringSlices(q2.GroupedColumns, expectedGrouped) {
			t.Errorf("Expected grouped columns %v, got %v", expectedGrouped, q2.GroupedColumns)
		}
		expectedColumns := []string{"status", "category", "region", "amount"}
		if !equalStringSlices(q2.Columns, expectedColumns) {
			t.Errorf("Expected columns %v, got %v", expectedColumns, q2.Columns)
		}
	})

	// Test 3: Ungroup a middle grouped column
	t.Run("Ungroup middle grouped column", func(t *testing.T) {
		baseURL, _ := url.Parse("/table?table=test&columns=status,region,category,amount&grouped=status,region,category")
		q := NewQuery(baseURL)

		// Ungroup the middle grouped column (region)
		newURL := q.WithGroupedColumnToggled("region")
		parsedURL, _ := url.Parse(newURL.String())
		newState := NewQuery(parsedURL)

		expectedGrouped := []string{"status", "category"}
		if !equalStringSlices(newState.GroupedColumns, expectedGrouped) {
			t.Errorf("Expected grouped columns %v, got %v", expectedGrouped, newState.GroupedColumns)
		}
		// region should move to right after the last grouped column (category)
		expectedColumns := []string{"status", "category", "region", "amount"}
		if !equalStringSlices(newState.Columns, expectedColumns) {
			t.Errorf("Expected columns %v, got %v", expectedColumns, newState.Columns)
		}
	})
}

// equalStringSlices compares two string slices for equality
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}