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

import (
	"fmt"
	"strings"
)

// Syntax
// double quote means exact match, single quotes means contains
// || deliminates groups
// ! | & are supported, precedence is from top or and not
// no support for parenthesis

// Ideally this should be translated into a compiled regex expression...
// Tough this will only be applied to the unique values of any column
func Match(filter string, value string) int {
	groupIndex := 1
	for _, group := range strings.Split(filter, "||") {
	    orMatch := false
		for _, or := range strings.Split(group, "|") {
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
				}
				if not {
					match = !match
				}
				andMatch = andMatch && match
			}
			orMatch = orMatch || andMatch
		}
		if orMatch {
			return groupIndex
		}
		groupIndex++
	}
	return 0
}

func test(filter string, value string, group int) {
	res := Match(filter, value)
	if group != res {
		fmt.Println(filter, value, Match(filter, value))
		panic("Match error")
	}
}

func testBool(filter string, value string, match bool) {
	if match {
		test(filter, value, 1)
	} else {
		test(filter, value, 0)
	}
}
func testGrouping() {
	filter := "'a'||'b'" // contains a
	test(filter, "x", 0)
	test(filter, "a", 1)
	test(filter, "ax", 1)
	test(filter, "xa", 1)
	test(filter, "b", 2)
	test(filter, "bx", 2)
	test(filter, "xb", 2)

	filter = "'a'|'b'||'c'" // contains a
	test(filter, "x", 0)
	test(filter, "a", 1)
	test(filter, "ax", 1)
	test(filter, "xa", 1)
	test(filter, "b", 1)
	test(filter, "bx", 1)
	test(filter, "xb", 1)
	test(filter, "c", 2)
	test(filter, "cx", 2)
	test(filter, "xc", 2)

}

func testMatch() {
	filter := "'a'" // contains a
	testBool(filter, "a", true)
	testBool(filter, "bab", true)
	testBool(filter, "b", false)

	filter = `"a"` // exact match a
	testBool(filter, "a", true)
	testBool(filter, "bab", false)
	testBool(filter, "b", false)

	filter = `"a"|"b"`
	testBool(filter, "a", true)
	testBool(filter, "b", true)
	testBool(filter, "c", false)
	testBool(filter, "bab", false)

	filter = `"a"&"b"` // impossible ;-)
	testBool(filter, "a", false)
	testBool(filter, "b", false)
	testBool(filter, "c", false)
	testBool(filter, "ab", false)

	filter = `'a'&'b'` //
	testBool(filter, "ab", true)
	testBool(filter, "ba", true)
	testBool(filter, "a", false)
	testBool(filter, "b", false)
	testBool(filter, "c", false)

	// not
	filter = "!'a'" // contains a
	testBool(filter, "a", false)
	testBool(filter, "bab", false)
	testBool(filter, "b", true)

	filter = `!"a"` // exact match a
	testBool(filter, "a", false)
	testBool(filter, "bab", true)
	testBool(filter, "b", true)

	filter = `'a'&!'b'` //
	testBool(filter, "ab", false)
	testBool(filter, "ba", false)
	testBool(filter, "a", true)
	testBool(filter, "b", false)
	testBool(filter, "c", false)

	filter = `!'a'&!'b'` //
	testBool(filter, "ab", false)
	testBool(filter, "ba", false)
	testBool(filter, "a", false)
	testBool(filter, "b", false)
	testBool(filter, "c", true)

	// precedence
	filter = `'a'&'b'|'c'` //
	testBool(filter, "ab", true)
	testBool(filter, "ba", true)
	testBool(filter, "a", false)
	testBool(filter, "b", false)
	testBool(filter, "c", true)
	testBool(filter, "abc", true)

}

