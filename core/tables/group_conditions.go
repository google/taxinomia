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
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/expr"
	"github.com/google/taxinomia/core/queryspec"
)

// Group conditions keep only the groups of one grouping level whose
// aggregates satisfy a condition, e.g. "sum(amount) > 10000" or
// "count() >= 50 and avg(cpu_cores) < 64". They are a true filter: the
// rows of a dropped group leave the view, so counts and the aggregates of
// the levels above describe the kept groups only, and a parent whose
// subgroups are all dropped disappears too.
//
// A condition is the expression language (comparisons, and/or/not,
// arithmetic) plus aggregate calls over the table's columns:
//
//	count()  subgroups()                    any column
//	sum avg stddev min max (col)            numbers
//	unique min max (col)                    text
//	true false ratio any all (col)          yes/no
//	min max avg stddev span (col)           dates and times
//
// Conditions apply after the row filters and before grouping for display,
// level by level from the top: each level's groups are formed from the
// rows the levels above kept.

// GroupCondition is a compiled condition on one grouping level.
type GroupCondition struct {
	Source  string   // as written
	Columns []string // the grouping order the condition was compiled for
	Level   int      // index in Columns of the grouped column it applies to

	aggs []groupAggRef
	expr *expr.Expression
}

type groupAggRef struct {
	fn, col, ident string
	colType        queryspec.ColumnType
}

var groupAggCallRE = regexp.MustCompile(`\b(count|subgroups|sum|avg|stddev|min|max|unique|true|false|ratio|span|any|all)\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)?\s*\)`)

// groupAggFns lists, per aggregate call, the column types it accepts
// (none: it takes no column).
var groupAggFns = map[string][]queryspec.ColumnType{
	"count":     nil,
	"subgroups": nil,
	"sum":       {queryspec.ColumnTypeNumeric},
	"avg":       {queryspec.ColumnTypeNumeric, queryspec.ColumnTypeDatetime},
	"stddev":    {queryspec.ColumnTypeNumeric, queryspec.ColumnTypeDatetime},
	"min":       {queryspec.ColumnTypeNumeric, queryspec.ColumnTypeString, queryspec.ColumnTypeDatetime},
	"max":       {queryspec.ColumnTypeNumeric, queryspec.ColumnTypeString, queryspec.ColumnTypeDatetime},
	"unique":    {queryspec.ColumnTypeString},
	"true":      {queryspec.ColumnTypeBool},
	"false":     {queryspec.ColumnTypeBool},
	"ratio":     {queryspec.ColumnTypeBool},
	"any":       {queryspec.ColumnTypeBool},
	"all":       {queryspec.ColumnTypeBool},
	"span":      {queryspec.ColumnTypeDatetime},
}

var columnTypeWords = map[queryspec.ColumnType]string{
	queryspec.ColumnTypeNumeric:  "number",
	queryspec.ColumnTypeString:   "text",
	queryspec.ColumnTypeBool:     "yes/no",
	queryspec.ColumnTypeDatetime: "date",
}

// CompileGroupCondition compiles src as a condition on the groups of
// groupingOrder[level]. It checks the aggregate calls (known, on an
// existing column of a fitting type) and the expression, and evaluates it
// once on sample values so a misspelled name or a type mismatch is
// reported now rather than silently dropping every group.
func (t *TableView) CompileGroupCondition(groupingOrder []string, level int, src string) (*GroupCondition, error) {
	if level < 0 || level >= len(groupingOrder) {
		return nil, fmt.Errorf("the column is not grouped")
	}
	src = strings.TrimSpace(src)
	if src == "" {
		return nil, fmt.Errorf("the condition is empty")
	}
	c := &GroupCondition{Source: src, Columns: append([]string(nil), groupingOrder...), Level: level}
	var compileErr error
	rewritten := groupAggCallRE.ReplaceAllStringFunc(src, func(call string) string {
		m := groupAggCallRE.FindStringSubmatch(call)
		fn, col := m[1], m[2]
		accepted := groupAggFns[fn]
		ref := groupAggRef{fn: fn, col: col, ident: fmt.Sprintf("__group_agg_%d", len(c.aggs))}
		switch {
		case accepted == nil && col != "":
			compileErr = fmt.Errorf("%s() takes no column", fn)
		case accepted != nil && col == "":
			compileErr = fmt.Errorf("%s needs a column, e.g. %s(amount)", fn, fn)
		case accepted != nil:
			if t.GetColumn(col) == nil {
				compileErr = fmt.Errorf("no column %q", col)
				break
			}
			ref.colType = t.GetColumnType(col)
			ok := false
			for _, ct := range accepted {
				ok = ok || ct == ref.colType
			}
			if !ok {
				var words []string
				for _, ct := range accepted {
					words = append(words, columnTypeWords[ct])
				}
				compileErr = fmt.Errorf("%s needs a %s column; %s is %s", fn, strings.Join(words, " or "), col, columnTypeWords[ref.colType])
			}
		}
		c.aggs = append(c.aggs, ref)
		return ref.ident
	})
	if compileErr != nil {
		return nil, compileErr
	}
	if len(c.aggs) == 0 {
		return nil, fmt.Errorf("use an aggregate, e.g. count() > 10 or sum(amount) > 1000")
	}
	if bare := bareName(rewritten); bare != "" {
		return nil, fmt.Errorf("%s is not an aggregate (a column must be inside an aggregate, e.g. sum(%s))", bare, bare)
	}
	compiled, err := expr.Compile(rewritten)
	if err != nil {
		return nil, fmt.Errorf("syntax: %v", err)
	}
	c.expr = compiled
	// Dry run on sample values: catches bare column names (which are not
	// aggregates) and conditions that do not produce yes/no.
	sample := make(map[string]expr.Value, len(c.aggs))
	for _, a := range c.aggs {
		sample[a.ident] = sampleAggValue(a)
	}
	if _, err := c.eval(sample); err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "unknown") {
			return nil, fmt.Errorf("%v (a column must be inside an aggregate, e.g. sum(amount))", err)
		}
		if strings.Contains(err.Error(), "yes/no") {
			return nil, err
		}
	}
	return c, nil
}

func sampleAggValue(a groupAggRef) expr.Value {
	switch a.fn {
	case "count", "subgroups", "unique", "true", "false":
		return expr.NewInt(1)
	case "ratio", "sum":
		return expr.NewFloat(1)
	case "any", "all":
		return expr.NewBool(true)
	case "span":
		return expr.NewDuration(int64(time.Hour))
	}
	switch a.colType {
	case queryspec.ColumnTypeString:
		return expr.NewString("a")
	case queryspec.ColumnTypeDatetime:
		if a.fn == "stddev" {
			return expr.NewDuration(int64(time.Hour))
		}
		return expr.NewDatetime(time.Now().UnixNano())
	}
	return expr.NewFloat(1)
}

// eval evaluates the condition on one group's aggregate values.
func (c *GroupCondition) eval(values map[string]expr.Value) (bool, error) {
	bound := c.expr.Bind(func(name string, _ uint32) (expr.Value, error) {
		if v, ok := values[name]; ok {
			return v, nil
		}
		return expr.NilValue(), fmt.Errorf("unknown name %q", name)
	})
	v, err := bound.Eval(0)
	if err != nil {
		return false, err
	}
	if !v.IsBool() {
		return false, fmt.Errorf("the condition must be yes/no, e.g. a comparison (got %s)", v.TypeName())
	}
	return v.AsBool(), nil
}

// groupConditionsKey is the reserved lastFilters key under which the
// signature of the applied group conditions is kept, so the filter cache
// and the grouping cache see a changed condition as a changed filter. Its
// NUL byte cannot occur in a column name.
const groupConditionsKey = "\x00group-conditions"

// SetGroupConditions sets the group conditions the next ApplyFilters call
// applies after the row filters (nil: none). Conditions are applied level
// by level from the top.
func (t *TableView) SetGroupConditions(conds []*GroupCondition) {
	t.groupConds = append([]*GroupCondition(nil), conds...)
	sort.SliceStable(t.groupConds, func(i, j int) bool { return t.groupConds[i].Level < t.groupConds[j].Level })
}

func (t *TableView) groupConditionsSignature() string {
	if len(t.groupConds) == 0 {
		return ""
	}
	var b strings.Builder
	for _, c := range t.groupConds {
		fmt.Fprintf(&b, "%s\x01%d\x01%s\x02", strings.Join(c.Columns, "\x03"), c.Level, c.Source)
	}
	return b.String()
}

// applyGroupConditions removes from the filter selection the rows of the
// groups that fail their level's condition.
func (t *TableView) applyGroupConditions(ctx context.Context) error {
	for _, c := range t.groupConds {
		if err := t.applyGroupCondition(ctx, c, t.filterSel, 0); err != nil {
			return err
		}
	}
	return nil
}

type groupMemberLists map[uint32][]uint32

func (l groupMemberLists) Add(code, row uint32) { l[code] = append(l[code], row) }

// applyGroupCondition partitions sel by the grouped column at level and
// recurses down to the condition's level, where each group is evaluated.
func (t *TableView) applyGroupCondition(ctx context.Context, c *GroupCondition, sel columns.RowSet, level int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	col := t.GetColumn(c.Columns[level])
	if col == nil {
		return nil
	}
	lists := groupMemberLists{}
	columns.GroupOpsFor(col, t.columnViews[c.Columns[level]]).GroupAggregates(sel, lists)
	for _, members := range lists {
		if level < c.Level {
			if err := t.applyGroupCondition(ctx, c, columns.RowIndices(members), level+1); err != nil {
				return err
			}
			continue
		}
		keep, err := c.eval(t.groupAggValues(c, members))
		if err != nil || !keep { // a group the condition cannot evaluate is not kept
			for _, r := range members {
				t.filterSel.Remove(r)
			}
		}
	}
	return nil
}

// groupAggValues computes the aggregate values a condition uses for one
// group's member rows.
func (t *TableView) groupAggValues(c *GroupCondition, members []uint32) map[string]expr.Value {
	values := make(map[string]expr.Value, len(c.aggs))
	states := map[string]aggregates.AggregateState{}
	for _, a := range c.aggs {
		switch a.fn {
		case "count":
			values[a.ident] = expr.NewInt(int64(len(members)))
			continue
		case "subgroups":
			n := 0
			if c.Level+1 < len(c.Columns) {
				if next := t.GetColumn(c.Columns[c.Level+1]); next != nil {
					counts, _ := columns.GroupOpsFor(next, t.columnViews[c.Columns[c.Level+1]]).GroupCounts(columns.RowIndices(members))
					for _, k := range counts {
						if k > 0 {
							n++
						}
					}
				}
			}
			values[a.ident] = expr.NewInt(int64(n))
			continue
		}
		state, ok := states[a.col]
		if !ok {
			state = t.aggStateOver(a.col, a.colType, members)
			states[a.col] = state
		}
		values[a.ident] = aggValue(a, state)
	}
	return values
}

// aggStateOver builds one column's aggregate state over rows.
func (t *TableView) aggStateOver(colName string, colType queryspec.ColumnType, rows []uint32) aggregates.AggregateState {
	col := t.GetColumn(colName)
	state := aggregates.CreateAggState(colType)
	for _, idx := range rows {
		switch s := state.(type) {
		case *aggregates.NumericAggState:
			t.addNumericValue(s, col, idx)
		case *aggregates.BoolAggState:
			t.addBoolValue(s, col, idx)
		case *aggregates.DatetimeAggState:
			t.addDatetimeValue(s, col, idx)
		case *aggregates.StringAggState:
			t.addStringValue(s, col, idx)
		}
	}
	return state
}

// aggValue reads one aggregate of a state as an expression value; an
// aggregate over no values is nil (a comparison with it does not hold).
func aggValue(a groupAggRef, state aggregates.AggregateState) expr.Value {
	switch s := state.(type) {
	case *aggregates.NumericAggState:
		if s.Count == 0 {
			if a.fn == "sum" {
				return expr.NewFloat(0)
			}
			return expr.NilValue()
		}
		switch a.fn {
		case "sum":
			return expr.NewFloat(s.Sum)
		case "avg":
			return expr.NewFloat(s.Avg())
		case "stddev":
			return expr.NewFloat(s.StdDev())
		case "min":
			return expr.NewFloat(s.Min)
		case "max":
			return expr.NewFloat(s.Max)
		}
	case *aggregates.StringAggState:
		switch a.fn {
		case "unique":
			return expr.NewInt(int64(s.UniqueCount()))
		case "min":
			if s.HasValues {
				return expr.NewString(s.Min)
			}
		case "max":
			if s.HasValues {
				return expr.NewString(s.Max)
			}
		}
	case *aggregates.BoolAggState:
		switch a.fn {
		case "true":
			return expr.NewInt(s.TrueCount)
		case "false":
			return expr.NewInt(s.FalseCount)
		case "ratio":
			if s.Count > 0 {
				return expr.NewFloat(s.Ratio())
			}
		case "any":
			return expr.NewBool(s.TrueCount > 0)
		case "all":
			return expr.NewBool(s.Count > 0 && s.FalseCount == 0)
		}
	case *aggregates.DatetimeAggState:
		if s.Count == 0 {
			return expr.NilValue()
		}
		mean := s.Sum / float64(s.Count)
		switch a.fn {
		case "min":
			return expr.NewDatetime(s.Min)
		case "max":
			return expr.NewDatetime(s.Max)
		case "avg":
			return expr.NewDatetime(int64(mean))
		case "stddev":
			v := s.SumSq/float64(s.Count) - mean*mean
			if v < 0 {
				v = 0
			}
			return expr.NewDuration(int64(math.Sqrt(v)))
		case "span":
			return expr.NewDuration(s.Max - s.Min)
		}
	}
	return expr.NilValue()
}

var (
	quotedRE = regexp.MustCompile(`"[^"]*"|'[^']*'`)
	nameRE   = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
)

// conditionKeywords are names a condition may contain outside aggregates.
var conditionKeywords = map[string]bool{"and": true, "or": true, "not": true, "True": true, "False": true, "None": true, "in": true}

// bareName returns the first name in a rewritten condition that is neither
// an aggregate placeholder, a function or method call, nor a keyword: a
// column used outside an aggregate. Checked statically because evaluation
// short-circuits (count() > 1 and amount > 1 never reads amount when the
// left side is false).
func bareName(rewritten string) string {
	s := quotedRE.ReplaceAllString(rewritten, `""`)
	for _, loc := range nameRE.FindAllStringIndex(s, -1) {
		name := s[loc[0]:loc[1]]
		if strings.HasPrefix(name, "__group_agg_") || conditionKeywords[name] {
			continue
		}
		if loc[0] > 0 && (s[loc[0]-1] == '.' || (s[loc[0]-1] >= '0' && s[loc[0]-1] <= '9')) {
			continue // a method name, or a number such as 1e10
		}
		if strings.HasPrefix(strings.TrimLeft(s[loc[1]:], " \t"), "(") {
			continue // a function call
		}
		return name
	}
	return ""
}
