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

package main

import "testing"

func TestMatch_ExactMatch(t *testing.T) {
	tests := []struct {
		filter   string
		value    string
		expected bool
	}{
		{`"a"`, "a", true},
		{`"a"`, "b", false},
		{`"a"`, "ab", false},
		{`"bug"`, "bug", true},
		{`"bug"`, "feature", false},
	}

	for _, tt := range tests {
		result := Match(tt.filter, tt.value)
		if result != tt.expected {
			t.Errorf("Match(%q, %q) = %v; expected %v", tt.filter, tt.value, result, tt.expected)
		}
	}
}

func TestMatch_Contains(t *testing.T) {
	tests := []struct {
		filter   string
		value    string
		expected bool
	}{
		{`'a'`, "a", true},
		{`'a'`, "bab", true},
		{`'a'`, "b", false},
		{`'bug'`, "bugfix", true},
		{`'bug'`, "debug", true},
		{`'bug'`, "feature", false},
	}

	for _, tt := range tests {
		result := Match(tt.filter, tt.value)
		if result != tt.expected {
			t.Errorf("Match(%q, %q) = %v; expected %v", tt.filter, tt.value, result, tt.expected)
		}
	}
}

func TestMatch_OR(t *testing.T) {
	tests := []struct {
		filter   string
		value    string
		expected bool
	}{
		{`"a"|"b"`, "a", true},
		{`"a"|"b"`, "b", true},
		{`"a"|"b"`, "c", false},
		{`"bug"|"feature"`, "bug", true},
		{`"bug"|"feature"`, "feature", true},
		{`"bug"|"feature"`, "docs", false},
	}

	for _, tt := range tests {
		result := Match(tt.filter, tt.value)
		if result != tt.expected {
			t.Errorf("Match(%q, %q) = %v; expected %v", tt.filter, tt.value, result, tt.expected)
		}
	}
}

func TestMatch_AND(t *testing.T) {
	tests := []struct {
		filter   string
		value    string
		expected bool
	}{
		{`'a'&'b'`, "ab", true},
		{`'a'&'b'`, "ba", true},
		{`'a'&'b'`, "abc", true},
		{`'a'&'b'`, "a", false},
		{`'a'&'b'`, "b", false},
		{`'bug'&'critical'`, "critical-bug", true},
		{`'bug'&'critical'`, "bug", false},
	}

	for _, tt := range tests {
		result := Match(tt.filter, tt.value)
		if result != tt.expected {
			t.Errorf("Match(%q, %q) = %v; expected %v", tt.filter, tt.value, result, tt.expected)
		}
	}
}

func TestMatch_NOT(t *testing.T) {
	tests := []struct {
		filter   string
		value    string
		expected bool
	}{
		{`!'a'`, "a", false},
		{`!'a'`, "bab", false},
		{`!'a'`, "b", true},
		{`!"bug"`, "bug", false},
		{`!"bug"`, "feature", true},
		{`!"bug"`, "bugfix", true},
	}

	for _, tt := range tests {
		result := Match(tt.filter, tt.value)
		if result != tt.expected {
			t.Errorf("Match(%q, %q) = %v; expected %v", tt.filter, tt.value, result, tt.expected)
		}
	}
}

func TestMatch_Complex(t *testing.T) {
	tests := []struct {
		filter   string
		value    string
		expected bool
	}{
		// AND with NOT
		{`'a'&!'b'`, "a", true},
		{`'a'&!'b'`, "ab", false},
		{`'a'&!'b'`, "ba", false},
		{`'a'&!'b'`, "c", false},

		// Double NOT
		{`!'a'&!'b'`, "c", true},
		{`!'a'&!'b'`, "a", false},
		{`!'a'&!'b'`, "b", false},
		{`!'a'&!'b'`, "ab", false},

		// Precedence: AND before OR
		{`'a'&'b'|'c'`, "ab", true},
		{`'a'&'b'|'c'`, "c", true},
		{`'a'&'b'|'c'`, "a", false},
		{`'a'&'b'|'c'`, "b", false},

		// Real-world examples
		{`"OPEN"&'bug'`, "OPEN", false},
		{`'2024'&!'critical'`, "issue-2024", true},
		{`'2024'&!'critical'`, "critical-2024", false},
	}

	for _, tt := range tests {
		result := Match(tt.filter, tt.value)
		if result != tt.expected {
			t.Errorf("Match(%q, %q) = %v; expected %v", tt.filter, tt.value, result, tt.expected)
		}
	}
}

func TestMatch_EdgeCases(t *testing.T) {
	tests := []struct {
		filter   string
		value    string
		expected bool
	}{
		// Empty quotes
		{`""`, "", true},
		{`""`, "a", false},
		{`''`, "", true},
		{`''`, "a", true}, // contains empty string

		// Impossible exact match AND
		{`"a"&"b"`, "a", false},
		{`"a"&"b"`, "b", false},
		{`"a"&"b"`, "ab", false},

		// Spaces
		{`"a b"`, "a b", true},
		{`'a b'`, "a b", true},
		{`'a b'`, "tab", false}, // "a b" with space doesn't match "tab"
	}

	for _, tt := range tests {
		result := Match(tt.filter, tt.value)
		if result != tt.expected {
			t.Errorf("Match(%q, %q) = %v; expected %v", tt.filter, tt.value, result, tt.expected)
		}
	}
}

func TestMatch_GroupingScenarios(t *testing.T) {
	// Test cases for groupOn functionality
	tests := []struct {
		filter   string
		value    string
		expected bool
		desc     string
	}{
		// Year grouping
		{`"2023"|"2024"`, "2023", true, "2023-2024 group includes 2023"},
		{`"2023"|"2024"`, "2024", true, "2023-2024 group includes 2024"},
		{`"2023"|"2024"`, "2025", false, "2023-2024 group excludes 2025"},

		// Label categories
		{`'bug'`, "bug-fix", true, "bug category includes bug-fix"},
		{`'bug'`, "debugging", true, "bug category includes debugging"},
		{`'bug'`, "feature", false, "bug category excludes feature"},

		// Author type
		{`!"top-10"`, "community", true, "non-top-10 includes community"},
		{`!"top-10"`, "top-10", false, "non-top-10 excludes top-10"},

		// State + label combination
		{`"OPEN"&'bug'`, "OPEN-bug", false, "requires exact OPEN AND contains bug"},
		{`'OPEN'&'bug'`, "OPEN-bug-critical", true, "contains OPEN AND bug"},
	}

	for _, tt := range tests {
		result := Match(tt.filter, tt.value)
		if result != tt.expected {
			t.Errorf("%s: Match(%q, %q) = %v; expected %v", tt.desc, tt.filter, tt.value, result, tt.expected)
		}
	}
}
