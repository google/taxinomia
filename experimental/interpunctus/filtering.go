/*
SPDX-License-Identifier: Apache-2.0

# Copyright 2024 The Taxinomia Authors

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

import (
	"fmt"
	"strings"
)

// syntax
// double quote means exact match, single quotes means contains, bare string defaults to exact match
// ! | & are supported, precedence is from top or and not
// no support for parenthesis at the moment
//
// Examples:
//   "CLOSED" - exact match
//   'CLOSED' - contains match
//   CLOSED   - exact match (bare string)
//   "CLOSED"|"OPEN" - exact match CLOSED or OPEN

func Match(filter string, value string) bool {
	orMatch := false
	for _, or := range strings.Split(filter, "|") {
		andMatch := true
		for _, and := range strings.Split(or, "&") {
			and = strings.Trim(and, " ")
			not := false
			if strings.HasPrefix(and, "!") {
				not = true
				and = and[1:]
			}
			match := false
			if strings.HasPrefix(and, `"`) {
				match = value == and[1:len(and)-1]
			} else if strings.HasPrefix(and, "'") {
				match = strings.Contains(value, and[1:len(and)-1])
			} else if and != "" {
				// Bare string defaults to exact match
				match = value == and
			}
			if not {
				match = !match
			}
			andMatch = andMatch && match
		}
		orMatch = orMatch || andMatch
	}
	return orMatch
}

func test(filter string, value string, match bool) {
	res := Match(filter, value)
	if match != res {
		fmt.Println(filter, value, Match(filter, value))
		panic("Match error")

	}
}

func testMatch() {
	filter := "'a'" // contains a
	test(filter, "a", true)
	test(filter, "bab", true)
	test(filter, "b", false)

	filter = `"a"` // exact match a
	test(filter, "a", true)
	test(filter, "bab", false)
	test(filter, "b", false)

	filter = `"a"|"b"`
	test(filter, "a", true)
	test(filter, "b", true)
	test(filter, "c", false)
	test(filter, "bab", false)

	filter = `"a"&"b"` // impossible ;-)
	test(filter, "a", false)
	test(filter, "b", false)
	test(filter, "c", false)
	test(filter, "ab", false)

	filter = `'a'&'b'` //
	test(filter, "ab", true)
	test(filter, "ba", true)
	test(filter, "a", false)
	test(filter, "b", false)
	test(filter, "c", false)

	// not
	filter = "!'a'" // contains a
	test(filter, "a", false)
	test(filter, "bab", false)
	test(filter, "b", true)

	filter = `!"a"` // exact match a
	test(filter, "a", false)
	test(filter, "bab", true)
	test(filter, "b", true)

	filter = `'a'&!'b'` //
	test(filter, "ab", false)
	test(filter, "ba", false)
	test(filter, "a", true)
	test(filter, "b", false)
	test(filter, "c", false)

	filter = `!'a'&!'b'` //
	test(filter, "ab", false)
	test(filter, "ba", false)
	test(filter, "a", false)
	test(filter, "b", false)
	test(filter, "c", true)

	// precedence
	filter = `'a'&'b'|'c'` //
	test(filter, "ab", true)
	test(filter, "ba", true)
	test(filter, "a", false)
	test(filter, "b", false)
	test(filter, "c", true)
	test(filter, "abc", true)

	// bare strings (no quotes) - default to exact match
	filter = `CLOSED`
	test(filter, "CLOSED", true)
	test(filter, "OPEN", false)
	test(filter, "CLOSED_DUPLICATE", false)

	filter = `CLOSED|OPEN`
	test(filter, "CLOSED", true)
	test(filter, "OPEN", true)
	test(filter, "PENDING", false)

	fmt.Println("✓ All filter tests passed")
}
