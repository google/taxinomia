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

package columns

import "strings"

// Collation is how a string column's values are ordered — for sorting rows,
// ordering groups and sorting storage. It is part of the column definition
// (ColumnDef.Collation), declared in the table/column configuration; every
// string comparison in the engine goes through CompareStrings.
type Collation int

const (
	// CollationDefault orders bytewise (strings.Compare): "k10" < "k9",
	// "Zebra" < "apple".
	CollationDefault Collation = iota
	// CollationNatural orders like numbers wherever both values have a run
	// of digits: "k9" < "k10", "item-2" < "item-10", "1.5" < "1.25" is NOT
	// numeric (each digit run is an integer). Non-digit stretches compare
	// bytewise. Digit runs equal in value but different in text ("7" vs
	// "007") order shorter first, so the order stays total.
	CollationNatural
)

// String names the collation as written in configuration.
func (c Collation) String() string {
	switch c {
	case CollationNatural:
		return "natural"
	default:
		return "default"
	}
}

// CompareStrings compares a and b under the collation: negative when a
// orders first, zero when equal, positive when b orders first.
func CompareStrings(c Collation, a, b string) int {
	if c == CollationNatural {
		return compareNatural(a, b)
	}
	return strings.Compare(a, b)
}

func isDigit(b byte) bool { return '0' <= b && b <= '9' }

// compareNatural is CollationNatural's comparison.
func compareNatural(a, b string) int {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if isDigit(a[i]) && isDigit(b[j]) {
			// Compare the two digit runs as integers: skip leading zeros,
			// then longer run is larger, then bytewise.
			si, sj := i, j
			for i < len(a) && a[i] == '0' {
				i++
			}
			for j < len(b) && b[j] == '0' {
				j++
			}
			ei, ej := i, j
			for ei < len(a) && isDigit(a[ei]) {
				ei++
			}
			for ej < len(b) && isDigit(b[ej]) {
				ej++
			}
			if d := (ei - i) - (ej - j); d != 0 {
				return d
			}
			if c := strings.Compare(a[i:ei], b[j:ej]); c != 0 {
				return c
			}
			// Same value: fewer leading zeros first (total order).
			if d := (ei - si) - (ej - sj); d != 0 {
				return d
			}
			i, j = ei, ej
			continue
		}
		if a[i] != b[j] {
			if a[i] < b[j] {
				return -1
			}
			return 1
		}
		i++
		j++
	}
	// Prefix rule: the shorter string orders first.
	return (len(a) - i) - (len(b) - j)
}
