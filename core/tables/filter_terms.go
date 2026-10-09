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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/taxinomia/core/columns"
)

// Filter terms, row by row: the filter language of one column's filter box,
// as a predicate over row indices. ApplyFiltersContext keeps its faster
// structured paths for the plain forms and uses these for ranges, quoted
// alternatives and group definitions (group_definitions.go).
//
//	east           contains "east", any case
//	"East"         exactly East
//	a|b            any of the alternatives; each is exact ("quoted" or bare)
//	               or a range
//	[error]        the row's value could not be computed ([unmatched]: no
//	               match in a joined table)
//	<10  <=10  >10  >=10  10..100  ..10  100..
//	               on number and date columns: a comparison or a half-open
//	               range (10..100 is 10 <= x < 100). On a date column a
//	               partial date stands for its whole period: 2024 is the
//	               year, 2024-03 the month, 2024-03-01 the day, so
//	               2024-01..2024-07 is January to June and <=2024 is up to
//	               the end of 2024. On other columns these are plain text.

// rowPredicate reports whether row i matches.
type rowPredicate func(i uint32) bool

// compileFilterText returns the predicate of a filter text (no group
// separator) on col.
func compileFilterText(col columns.IDataColumn, text string) rowPredicate {
	alts := splitOutsideQuotes(text, "|")
	if len(alts) > 1 {
		preds := make([]rowPredicate, len(alts))
		for k, a := range alts {
			preds[k] = compileAlternative(col, a)
		}
		return func(i uint32) bool {
			for _, p := range preds {
				if p(i) {
					return true
				}
			}
			return false
		}
	}
	if p := compileRange(col, text); p != nil {
		return p
	}
	if exact, ok := unquote(text); ok {
		return exactPredicate(col, exact)
	}
	return containsPredicate(col, text)
}

// compileAlternative: one alternative of a|b — a range, else an exact value
// (quotes optional).
func compileAlternative(col columns.IDataColumn, alt string) rowPredicate {
	if p := compileRange(col, alt); p != nil {
		return p
	}
	if exact, ok := unquote(alt); ok {
		return exactPredicate(col, exact)
	}
	return exactPredicate(col, alt)
}

// needsRowPredicate reports whether ApplyFiltersContext must use the row
// predicate for text instead of its structured paths: a range somewhere, or
// quoted alternatives (the structured a|b path compares the quotes too).
func needsRowPredicate(col columns.IDataColumn, text string) bool {
	alts := splitOutsideQuotes(text, "|")
	for _, a := range alts {
		if compileRange(col, a) != nil {
			return true
		}
		if _, ok := unquote(a); ok && len(alts) > 1 {
			return true
		}
	}
	return len(splitOutsideQuotes(text, "|")) != len(strings.Split(text, "|"))
}

func cellText(col columns.IDataColumn, i uint32) string {
	v, err := col.GetString(i)
	if err != nil {
		return unreadableLabel(err)
	}
	return v
}

func exactPredicate(col columns.IDataColumn, want string) rowPredicate {
	return func(i uint32) bool { return cellText(col, i) == want }
}

func containsPredicate(col columns.IDataColumn, text string) rowPredicate {
	lower := strings.ToLower(text)
	return func(i uint32) bool {
		v, err := col.GetString(i)
		if err != nil {
			return strings.EqualFold(text, unreadableLabel(err))
		}
		return strings.Contains(strings.ToLower(v), lower)
	}
}

func unquote(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1], true
	}
	return "", false
}

// splitOutsideQuotes splits s at sep where sep is not inside "...".
func splitOutsideQuotes(s, sep string) []string {
	var parts []string
	quoted := false
	start := 0
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '"':
			quoted = !quoted
		case !quoted && strings.HasPrefix(s[i:], sep):
			parts = append(parts, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	return append(parts, s[start:])
}

// --- ranges -----------------------------------------------------------

// columnKind tells how a column's values compare in a range.
type columnKind int

const (
	kindOther columnKind = iota
	kindNumber
	kindTime
)

func kindOf(col columns.IDataColumn) columnKind {
	switch col.(type) {
	case *columns.ComputedDatetimeColumn:
		return kindTime
	case *columns.DurationColumn, *columns.ComputedDurationColumn, *columns.JoinedDurationColumn:
		return kindOther
	}
	switch col.(type) {
	case interface {
		GetValue(uint32) (time.Time, error)
	}:
		return kindTime
	case interface{ GetValue(uint32) (int64, error) },
		interface{ GetValue(uint32) (uint64, error) },
		interface{ GetValue(uint32) (uint32, error) },
		interface{ GetValue(uint32) (float64, error) }:
		return kindNumber
	}
	return kindOther
}

// numberAt reads row i of a number column; ok is false for a row without a
// value.
func numberAt(col columns.IDataColumn, i uint32) (float64, bool) {
	switch c := col.(type) {
	case interface{ GetValue(uint32) (float64, error) }:
		v, err := c.GetValue(i)
		return v, err == nil && !math.IsNaN(v)
	case interface{ GetValue(uint32) (int64, error) }:
		v, err := c.GetValue(i)
		return float64(v), err == nil
	case interface{ GetValue(uint32) (uint64, error) }:
		v, err := c.GetValue(i)
		return float64(v), err == nil
	case interface{ GetValue(uint32) (uint32, error) }:
		v, err := c.GetValue(i)
		return float64(v), err == nil
	}
	return 0, false
}

// timeAt reads row i of a date column as Unix nanoseconds.
func timeAt(col columns.IDataColumn, i uint32) (int64, bool) {
	switch c := col.(type) {
	case *columns.ComputedDatetimeColumn:
		v, err := c.GetValue(i)
		return v, err == nil
	case interface {
		GetValue(uint32) (time.Time, error)
	}:
		v, err := c.GetValue(i)
		if err != nil || v.IsZero() {
			return 0, false
		}
		return v.UnixNano(), true
	}
	return 0, false
}

// bound is one end of a range: at value v, included or not.
type bound struct {
	v    float64 // numbers
	t    int64   // dates, Unix nanoseconds
	incl bool
	set  bool
}

// compileRange returns the predicate of a comparison or range term on a
// number or date column, or nil when text is not one (it is then text).
func compileRange(col columns.IDataColumn, text string) rowPredicate {
	kind := kindOf(col)
	if kind == kindOther {
		return nil
	}
	lo, hi, ok := parseRange(strings.TrimSpace(text), kind)
	if !ok {
		return nil
	}
	if kind == kindNumber {
		return func(i uint32) bool {
			v, ok := numberAt(col, i)
			if !ok {
				return false
			}
			return (!lo.set || v > lo.v || (lo.incl && v == lo.v)) &&
				(!hi.set || v < hi.v || (hi.incl && v == hi.v))
		}
	}
	return func(i uint32) bool {
		t, ok := timeAt(col, i)
		if !ok {
			return false
		}
		return (!lo.set || t > lo.t || (lo.incl && t == lo.t)) &&
			(!hi.set || t < hi.t || (hi.incl && t == hi.t))
	}
}

// parseRange reads <x <=x >x >=x a..b ..b a.. into bounds. For dates every
// endpoint is a period [start, end): <P is before its start, <=P before its
// end, >P from its end, >=P from its start, a..b from a's start to b's
// start.
func parseRange(s string, kind columnKind) (lo, hi bound, ok bool) {
	ends := func(x string) (start, end bound, ok bool) {
		x = strings.TrimSpace(x)
		if x == "" {
			return bound{}, bound{}, false
		}
		if kind == kindNumber {
			f, err := strconv.ParseFloat(x, 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				return bound{}, bound{}, false
			}
			return bound{v: f, set: true}, bound{v: f, set: true}, true
		}
		s, e, ok := datePeriod(x)
		return bound{t: s, set: true}, bound{t: e, set: true}, ok
	}
	switch {
	case strings.HasPrefix(s, "<="):
		_, e, ok := ends(s[2:])
		if kind == kindNumber {
			e.incl = true
		}
		return bound{}, e, ok
	case strings.HasPrefix(s, ">="):
		st, _, ok := ends(s[2:])
		st.incl = true
		return st, bound{}, ok
	case strings.HasPrefix(s, "<"):
		st, _, ok := ends(s[1:])
		return bound{}, st, ok
	case strings.HasPrefix(s, ">"):
		_, e, ok := ends(s[1:])
		if kind == kindTime {
			e.incl = true // from the period's end on
		}
		return e, bound{}, ok
	}
	a, b, found := strings.Cut(s, "..")
	if !found || strings.Contains(b, "..") {
		return bound{}, bound{}, false
	}
	if strings.TrimSpace(a) != "" {
		st, _, ok := ends(a)
		if !ok {
			return bound{}, bound{}, false
		}
		st.incl = true
		lo = st
	}
	if strings.TrimSpace(b) != "" {
		st, _, ok := ends(b)
		if !ok {
			return bound{}, bound{}, false
		}
		hi = st
	}
	return lo, hi, lo.set || hi.set
}

// datePeriod reads a date or a part of one as the period it names, as Unix
// nanoseconds [start, end): 2024, 2024-03, 2024-03-01, or a full date and
// time (a period of one nanosecond). Times are UTC, like the columns'.
func datePeriod(x string) (start, end int64, ok bool) {
	for _, p := range []struct {
		layout string
		next   func(time.Time) time.Time
	}{
		{"2006", func(t time.Time) time.Time { return t.AddDate(1, 0, 0) }},
		{"2006-01", func(t time.Time) time.Time { return t.AddDate(0, 1, 0) }},
		{"2006-01-02", func(t time.Time) time.Time { return t.AddDate(0, 0, 1) }},
	} {
		if t, err := time.ParseInLocation(p.layout, x, time.UTC); err == nil {
			return t.UnixNano(), p.next(t).UnixNano(), true
		}
	}
	if !strings.ContainsAny(x, "-/:") {
		return 0, 0, false // not a date (ParseDatetime takes plain numbers as timestamps)
	}
	t, err := columns.ParseDatetime(x, time.UTC)
	if err != nil || t.IsZero() {
		return 0, 0, false
	}
	return t.UnixNano(), t.UnixNano() + 1, true
}
