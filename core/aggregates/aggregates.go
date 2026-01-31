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

// Package aggregates provides aggregate state types for hierarchical aggregation.
// These types store intermediate state that can be combined up a grouping hierarchy,
// allowing aggregates to be computed at leaf level and merged up to parent groups.
package aggregates

import (
	"fmt"
	"math"
	"time"

	"github.com/google/taxinomia/core/query"
)

// AggregateState is the interface for all aggregate state types.
// It provides methods for combining states and formatting results.
type AggregateState interface {
	// Combine merges another state into this one (for hierarchical aggregation).
	Combine(other AggregateState)
	// Format returns a formatted string for the given aggregate type.
	Format(aggType query.AggregateType) string
	// ColumnType returns the column type this state is for.
	ColumnType() query.ColumnType
}

// NumericAggState stores intermediate state for numeric column aggregates.
// It can derive sum, avg, stddev, min, max, and count.
type NumericAggState struct {
	Count int64   // Number of values
	Sum   float64 // Sum of values
	SumSq float64 // Sum of squared values (for stddev)
	Min   float64 // Minimum value
	Max   float64 // Maximum value
}

// NewNumericAggState creates a new empty numeric aggregate state.
func NewNumericAggState() *NumericAggState {
	return &NumericAggState{
		Min: math.MaxFloat64,
		Max: -math.MaxFloat64,
	}
}

// Add adds a single value to the aggregate state.
func (s *NumericAggState) Add(value float64) {
	s.Count++
	s.Sum += value
	s.SumSq += value * value
	if value < s.Min {
		s.Min = value
	}
	if value > s.Max {
		s.Max = value
	}
}

// AddUint32 adds a uint32 value to the aggregate state.
func (s *NumericAggState) AddUint32(value uint32) {
	s.Add(float64(value))
}

// Combine merges another numeric state into this one.
func (s *NumericAggState) Combine(other AggregateState) {
	o, ok := other.(*NumericAggState)
	if !ok || o.Count == 0 {
		return
	}
	s.Count += o.Count
	s.Sum += o.Sum
	s.SumSq += o.SumSq
	if o.Min < s.Min {
		s.Min = o.Min
	}
	if o.Max > s.Max {
		s.Max = o.Max
	}
}

// Avg returns the average (mean) of the values.
func (s *NumericAggState) Avg() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / float64(s.Count)
}

// StdDev returns the population standard deviation.
func (s *NumericAggState) StdDev() float64 {
	if s.Count == 0 {
		return 0
	}
	mean := s.Avg()
	// Variance = E[X²] - (E[X])²
	variance := (s.SumSq / float64(s.Count)) - (mean * mean)
	if variance < 0 {
		// Handle floating point precision issues
		variance = 0
	}
	return math.Sqrt(variance)
}

// Format returns a formatted string for the given aggregate type.
func (s *NumericAggState) Format(aggType query.AggregateType) string {
	if s.Count == 0 {
		return "-"
	}
	switch aggType {
	case query.AggCount:
		return fmt.Sprintf("%d", s.Count)
	case query.AggSum:
		return formatNumber(s.Sum)
	case query.AggAvg:
		return formatNumber(s.Avg())
	case query.AggStdDev:
		return formatNumber(s.StdDev())
	case query.AggMin:
		return formatNumber(s.Min)
	case query.AggMax:
		return formatNumber(s.Max)
	default:
		return "-"
	}
}

// ColumnType returns the column type this state is for.
func (s *NumericAggState) ColumnType() query.ColumnType {
	return query.ColumnTypeNumeric
}

// BoolAggState stores intermediate state for boolean column aggregates.
// It can derive count, true count, false count, and ratio.
type BoolAggState struct {
	Count      int64 // Total count
	TrueCount  int64 // Count of true values
	FalseCount int64 // Count of false values
}

// NewBoolAggState creates a new empty boolean aggregate state.
func NewBoolAggState() *BoolAggState {
	return &BoolAggState{}
}

// Add adds a single boolean value to the aggregate state.
func (s *BoolAggState) Add(value bool) {
	s.Count++
	if value {
		s.TrueCount++
	} else {
		s.FalseCount++
	}
}

// Combine merges another boolean state into this one.
func (s *BoolAggState) Combine(other AggregateState) {
	o, ok := other.(*BoolAggState)
	if !ok || o.Count == 0 {
		return
	}
	s.Count += o.Count
	s.TrueCount += o.TrueCount
	s.FalseCount += o.FalseCount
}

// Ratio returns the ratio of true values to total (0.0 to 1.0).
func (s *BoolAggState) Ratio() float64 {
	if s.Count == 0 {
		return 0
	}
	return float64(s.TrueCount) / float64(s.Count)
}

// Format returns a formatted string for the given aggregate type.
func (s *BoolAggState) Format(aggType query.AggregateType) string {
	if s.Count == 0 {
		return "-"
	}
	switch aggType {
	case query.AggCount:
		return fmt.Sprintf("%d", s.Count)
	case query.AggTrue:
		return fmt.Sprintf("%d", s.TrueCount)
	case query.AggFalse:
		return fmt.Sprintf("%d", s.FalseCount)
	case query.AggRatio:
		return fmt.Sprintf("%.1f%%", s.Ratio()*100)
	default:
		return "-"
	}
}

// ColumnType returns the column type this state is for.
func (s *BoolAggState) ColumnType() query.ColumnType {
	return query.ColumnTypeBool
}

// StringAggState stores intermediate state for string column aggregates.
// It can derive count, unique count, min (alphabetically smallest), and max (alphabetically largest).
type StringAggState struct {
	Count     int64               // Total count
	UniqueSet map[string]struct{} // Set of unique values
	Min       string              // Alphabetically smallest value
	Max       string              // Alphabetically largest value
	HasValues bool                // Whether Min/Max have been set
}

// NewStringAggState creates a new empty string aggregate state.
func NewStringAggState() *StringAggState {
	return &StringAggState{
		UniqueSet: make(map[string]struct{}),
	}
}

// Add adds a single string value to the aggregate state.
func (s *StringAggState) Add(value string) {
	if !s.HasValues {
		s.Min = value
		s.Max = value
		s.HasValues = true
	} else {
		if value < s.Min {
			s.Min = value
		}
		if value > s.Max {
			s.Max = value
		}
	}
	s.Count++
	s.UniqueSet[value] = struct{}{}
}

// Combine merges another string state into this one.
func (s *StringAggState) Combine(other AggregateState) {
	o, ok := other.(*StringAggState)
	if !ok || o.Count == 0 {
		return
	}
	if !s.HasValues && o.HasValues {
		s.Min = o.Min
		s.Max = o.Max
		s.HasValues = true
	} else if o.HasValues {
		if o.Min < s.Min {
			s.Min = o.Min
		}
		if o.Max > s.Max {
			s.Max = o.Max
		}
	}
	s.Count += o.Count
	for k := range o.UniqueSet {
		s.UniqueSet[k] = struct{}{}
	}
}

// UniqueCount returns the number of unique values.
func (s *StringAggState) UniqueCount() int {
	return len(s.UniqueSet)
}

// Format returns a formatted string for the given aggregate type.
func (s *StringAggState) Format(aggType query.AggregateType) string {
	if s.Count == 0 {
		return "-"
	}
	switch aggType {
	case query.AggCount:
		return fmt.Sprintf("%d", s.Count)
	case query.AggUnique:
		return fmt.Sprintf("%d", s.UniqueCount())
	case query.AggMin:
		return s.Min
	case query.AggMax:
		return s.Max
	default:
		return "-"
	}
}

// ColumnType returns the column type this state is for.
func (s *StringAggState) ColumnType() query.ColumnType {
	return query.ColumnTypeString
}

// DatetimeAggState stores intermediate state for datetime column aggregates.
// Values are stored as nanoseconds since Unix epoch for consistent math.
// It can derive count, min, max, avg, stddev, and span.
type DatetimeAggState struct {
	Count int64   // Number of values
	Sum   float64 // Sum of epoch nanoseconds (as float64 for precision)
	SumSq float64 // Sum of squared epoch nanoseconds (for stddev)
	Min   int64   // Minimum epoch nanoseconds
	Max   int64   // Maximum epoch nanoseconds
}

// NewDatetimeAggState creates a new empty datetime aggregate state.
func NewDatetimeAggState() *DatetimeAggState {
	return &DatetimeAggState{
		Min: math.MaxInt64,
		Max: math.MinInt64,
	}
}

// Add adds a single time value to the aggregate state.
func (s *DatetimeAggState) Add(value time.Time) {
	nanos := value.UnixNano()
	s.Count++
	s.Sum += float64(nanos)
	s.SumSq += float64(nanos) * float64(nanos)
	if nanos < s.Min {
		s.Min = nanos
	}
	if nanos > s.Max {
		s.Max = nanos
	}
}

// Combine merges another datetime state into this one.
func (s *DatetimeAggState) Combine(other AggregateState) {
	o, ok := other.(*DatetimeAggState)
	if !ok || o.Count == 0 {
		return
	}
	s.Count += o.Count
	s.Sum += o.Sum
	s.SumSq += o.SumSq
	if o.Min < s.Min {
		s.Min = o.Min
	}
	if o.Max > s.Max {
		s.Max = o.Max
	}
}

// Avg returns the average time.
func (s *DatetimeAggState) Avg() time.Time {
	if s.Count == 0 {
		return time.Time{}
	}
	avgNanos := int64(s.Sum / float64(s.Count))
	return time.Unix(0, avgNanos).UTC()
}

// StdDev returns the standard deviation as a duration.
func (s *DatetimeAggState) StdDev() time.Duration {
	if s.Count == 0 {
		return 0
	}
	mean := s.Sum / float64(s.Count)
	variance := (s.SumSq / float64(s.Count)) - (mean * mean)
	if variance < 0 {
		variance = 0
	}
	return time.Duration(math.Sqrt(variance))
}

// Span returns the time span (max - min).
func (s *DatetimeAggState) Span() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return time.Duration(s.Max - s.Min)
}

// MinTime returns the minimum time value.
func (s *DatetimeAggState) MinTime() time.Time {
	if s.Count == 0 {
		return time.Time{}
	}
	return time.Unix(0, s.Min).UTC()
}

// MaxTime returns the maximum time value.
func (s *DatetimeAggState) MaxTime() time.Time {
	if s.Count == 0 {
		return time.Time{}
	}
	return time.Unix(0, s.Max).UTC()
}

// Format returns a formatted string for the given aggregate type.
func (s *DatetimeAggState) Format(aggType query.AggregateType) string {
	if s.Count == 0 {
		return "-"
	}
	switch aggType {
	case query.AggCount:
		return fmt.Sprintf("%d", s.Count)
	case query.AggMin:
		return formatDatetime(s.MinTime())
	case query.AggMax:
		return formatDatetime(s.MaxTime())
	case query.AggAvg:
		return formatDatetime(s.Avg())
	case query.AggStdDev:
		return formatDuration(s.StdDev())
	case query.AggSpan:
		return formatDuration(s.Span())
	default:
		return "-"
	}
}

// ColumnType returns the column type this state is for.
func (s *DatetimeAggState) ColumnType() query.ColumnType {
	return query.ColumnTypeDatetime
}

// --- Formatting helpers ---

// formatNumber formats a float64 for display, using appropriate precision.
func formatNumber(v float64) string {
	if v == float64(int64(v)) {
		// Integer value
		return fmt.Sprintf("%d", int64(v))
	}
	// Show up to 2 decimal places, trimming trailing zeros
	formatted := fmt.Sprintf("%.2f", v)
	// Trim trailing zeros after decimal point
	if idx := len(formatted) - 1; formatted[idx] == '0' {
		formatted = formatted[:idx]
		if idx--; formatted[idx] == '0' {
			formatted = formatted[:idx]
		}
	}
	// Remove trailing decimal point if no decimals
	if formatted[len(formatted)-1] == '.' {
		formatted = formatted[:len(formatted)-1]
	}
	return formatted
}

// formatDatetime formats a time.Time for display.
func formatDatetime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("2006-01-02 15:04")
}

// formatDuration formats a duration for display.
func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0"
	}
	// Use appropriate units based on magnitude
	hours := d.Hours()
	if hours >= 24*365 {
		years := hours / (24 * 365)
		return fmt.Sprintf("%.1fy", years)
	}
	if hours >= 24*30 {
		months := hours / (24 * 30)
		return fmt.Sprintf("%.1fmo", months)
	}
	if hours >= 24 {
		days := hours / 24
		return fmt.Sprintf("%.1fd", days)
	}
	if hours >= 1 {
		return fmt.Sprintf("%.1fh", hours)
	}
	if d.Minutes() >= 1 {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// CreateAggState creates a new aggregate state for the given column type.
func CreateAggState(colType query.ColumnType) AggregateState {
	switch colType {
	case query.ColumnTypeNumeric:
		return NewNumericAggState()
	case query.ColumnTypeBool:
		return NewBoolAggState()
	case query.ColumnTypeString:
		return NewStringAggState()
	case query.ColumnTypeDatetime:
		return NewDatetimeAggState()
	default:
		return NewStringAggState() // Default to string
	}
}

// ColumnAggregator holds a column reference and its aggregate state for accumulation.
type ColumnAggregator struct {
	ColumnName string
	ColumnType query.ColumnType
	State      AggregateState
}

// NewColumnAggregator creates a new aggregator for a column.
func NewColumnAggregator(colName string, colType query.ColumnType) *ColumnAggregator {
	return &ColumnAggregator{
		ColumnName: colName,
		ColumnType: colType,
		State:      CreateAggState(colType),
	}
}

// FormattedAggregate represents a single formatted aggregate value.
type FormattedAggregate struct {
	Symbol   string // e.g., Σ, μ, ↓, ↑
	Value    string // Formatted value
	Title    string // Tooltip title
	IsSorted bool   // Whether this aggregate is the one being sorted by
}

// ColumnAggregateDisplay represents the aggregates for a single leaf column.
type ColumnAggregateDisplay struct {
	ColumnName string               // Column name
	Aggregates []FormattedAggregate // Formatted aggregates
}

// FormatAggregates returns formatted aggregates for display, based on enabled aggregate types.
func FormatAggregates(state AggregateState, enabledAggs []query.AggregateType) []FormattedAggregate {
	return FormatAggregatesWithSort(state, enabledAggs, "", query.AggCount)
}

// FormatAggregatesWithSort returns formatted aggregates with an optional sorted indicator.
// If sortedColName matches the column being formatted and sortedAggType matches one of the aggregates,
// that aggregate will have IsSorted=true.
func FormatAggregatesWithSort(state AggregateState, enabledAggs []query.AggregateType, sortedColName string, sortedAggType query.AggregateType) []FormattedAggregate {
	if state == nil || len(enabledAggs) == 0 {
		return nil
	}
	result := make([]FormattedAggregate, 0, len(enabledAggs))
	for _, aggType := range enabledAggs {
		isSorted := sortedColName != "" && aggType == sortedAggType
		result = append(result, FormattedAggregate{
			Symbol:   query.AggregateSymbol(aggType),
			Value:    state.Format(aggType),
			Title:    query.AggregateTitle(aggType),
			IsSorted: isSorted,
		})
	}
	return result
}
