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

package viewmodel

import (
	"strconv"

	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/columns"
)

// FormatIntString inserts apostrophe thousands separators (Swiss style,
// 74543001 → 74'543'001) when s is a plain integer of five or more digits.
// Anything else — shorter integers, floats, text, percentages — passes
// through unchanged. Display-only: raw values keep feeding filters, URLs
// and data attributes.
func FormatIntString(s string) string {
	digits := s
	neg := false
	if len(s) > 0 && s[0] == '-' {
		neg = true
		digits = s[1:]
	}
	if len(digits) < 5 {
		return s
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return s
		}
	}
	var b []byte
	if neg {
		b = append(b, '-')
	}
	lead := len(digits) % 3
	if lead > 0 {
		b = append(b, digits[:lead]...)
	}
	for i := lead; i < len(digits); i += 3 {
		if len(b) > 0 && !(neg && len(b) == 1) {
			b = append(b, '\'')
		}
		b = append(b, digits[i:i+3]...)
	}
	return string(b)
}

// FormatCount formats an integer count for display with separators.
func FormatCount(n int) string {
	return FormatIntString(strconv.Itoa(n))
}

// isIntegerColumn reports whether col's values are integers, so its cell
// values may take display separators. Strings that merely look numeric
// (zip codes, phone numbers) must not.
func isIntegerColumn(col columns.IDataColumn) bool {
	switch col.(type) {
	case *columns.ChunkedInt64Column, *columns.ChunkedUint64Column, *columns.ChunkedUint32Column,
		*columns.Int64Column, *columns.Uint64Column, *columns.Uint32Column,
		*columns.ComputedInt64Column, *columns.ComputedUint32Column,
		*columns.JoinedInt64Column, *columns.JoinedUint64Column, *columns.JoinedUint32Column:
		return true
	}
	return false
}

// withThousands applies FormatIntString to formatted aggregate values —
// integer-valued chips (counts, integer sums) gain separators, everything
// else is untouched.
func withThousands(aggs []aggregates.FormattedAggregate) []aggregates.FormattedAggregate {
	for i := range aggs {
		aggs[i].Value = FormatIntString(aggs[i].Value)
	}
	return aggs
}
