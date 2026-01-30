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

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// DurationFormat specifies how durations are displayed.
type DurationFormat int

const (
	// DurationFormatCompact displays durations in Go-style compact format (e.g., "2h30m")
	DurationFormatCompact DurationFormat = iota
	// DurationFormatVerbose displays durations in human-readable format (e.g., "2 hours 30 minutes")
	DurationFormatVerbose
)

// DurationColumn stores duration values using Go's time.Duration type.
type DurationColumn struct {
	columnDef     *ColumnDef
	data          []time.Duration
	isKey         bool
	valueIndex    map[int64]int // nanoseconds -> rowIndex for key columns
	displayFormat DurationFormat
}

// NewDurationColumn creates a new duration column with compact display format.
func NewDurationColumn(columnDef *ColumnDef) *DurationColumn {
	return &DurationColumn{
		columnDef:     columnDef,
		data:          make([]time.Duration, 0),
		displayFormat: DurationFormatCompact,
	}
}

// NewDurationColumnWithFormat creates a new duration column with a custom display format.
func NewDurationColumnWithFormat(columnDef *ColumnDef, format DurationFormat) *DurationColumn {
	return &DurationColumn{
		columnDef:     columnDef,
		data:          make([]time.Duration, 0),
		displayFormat: format,
	}
}

// ColumnDef returns the column definition.
func (c *DurationColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

// Length returns the number of rows in the column.
func (c *DurationColumn) Length() int {
	return len(c.data)
}

// GetString returns the string representation of the duration at the given index.
func (c *DurationColumn) GetString(i uint32) (string, error) {
	if int(i) >= len(c.data) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	return c.formatDuration(c.data[i]), nil
}

// formatDuration formats a duration according to the column's display format.
func (c *DurationColumn) formatDuration(d time.Duration) string {
	switch c.displayFormat {
	case DurationFormatVerbose:
		return formatDurationVerbose(d)
	default:
		return formatDurationCompact(d)
	}
}

// formatDurationCompact returns a compact representation like "2h30m" or "3d4h".
func formatDurationCompact(d time.Duration) string {
	if d == 0 {
		return "0s"
	}

	negative := d < 0
	if negative {
		d = -d
	}

	var result strings.Builder
	if negative {
		result.WriteString("-")
	}

	// Handle days specially (not in standard Go duration)
	days := d / (24 * time.Hour)
	d = d % (24 * time.Hour)

	if days > 0 {
		result.WriteString(strconv.FormatInt(int64(days), 10))
		result.WriteString("d")
	}

	// Use Go's standard formatting for the rest, but strip "0s" suffix if we have larger units
	if d > 0 || days == 0 {
		remaining := d.String()
		// If we have days and the remaining is just "0s", skip it
		if days > 0 && remaining == "0s" {
			return result.String()
		}
		result.WriteString(remaining)
	}

	return result.String()
}

// formatDurationVerbose returns a human-readable representation like "2 hours 30 minutes".
func formatDurationVerbose(d time.Duration) string {
	if d == 0 {
		return "0 seconds"
	}

	negative := d < 0
	if negative {
		d = -d
	}

	var parts []string

	days := d / (24 * time.Hour)
	d = d % (24 * time.Hour)
	if days > 0 {
		if days == 1 {
			parts = append(parts, "1 day")
		} else {
			parts = append(parts, fmt.Sprintf("%d days", days))
		}
	}

	hours := d / time.Hour
	d = d % time.Hour
	if hours > 0 {
		if hours == 1 {
			parts = append(parts, "1 hour")
		} else {
			parts = append(parts, fmt.Sprintf("%d hours", hours))
		}
	}

	minutes := d / time.Minute
	d = d % time.Minute
	if minutes > 0 {
		if minutes == 1 {
			parts = append(parts, "1 minute")
		} else {
			parts = append(parts, fmt.Sprintf("%d minutes", minutes))
		}
	}

	seconds := d / time.Second
	d = d % time.Second
	if seconds > 0 || len(parts) == 0 {
		if seconds == 1 && d == 0 {
			parts = append(parts, "1 second")
		} else if d == 0 {
			parts = append(parts, fmt.Sprintf("%d seconds", seconds))
		} else {
			// Include fractional seconds
			totalSeconds := float64(seconds) + float64(d)/float64(time.Second)
			parts = append(parts, fmt.Sprintf("%.3f seconds", totalSeconds))
		}
	}

	result := strings.Join(parts, " ")
	if negative {
		return "-" + result
	}
	return result
}

// GetValue returns the duration value at the given index.
func (c *DurationColumn) GetValue(i uint32) (time.Duration, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return c.data[i], nil
}

// Append adds a duration value to the column.
func (c *DurationColumn) Append(d time.Duration) {
	c.data = append(c.data, d)
}

// AppendNanoseconds adds a duration from nanoseconds.
func (c *DurationColumn) AppendNanoseconds(nanos int64) {
	c.data = append(c.data, time.Duration(nanos))
}

// AppendSeconds adds a duration from seconds (can be fractional).
func (c *DurationColumn) AppendSeconds(seconds float64) {
	nanos := int64(seconds * float64(time.Second))
	c.data = append(c.data, time.Duration(nanos))
}

// AppendString parses and adds a duration from a string.
// Supports Go duration format (e.g., "2h30m") and extended format with days (e.g., "3d2h").
func (c *DurationColumn) AppendString(s string) error {
	d, err := ParseDuration(s)
	if err != nil {
		return err
	}
	c.data = append(c.data, d)
	return nil
}

// ParseDuration parses a duration string, supporting Go format plus days.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	// Check for days component (e.g., "3d2h30m")
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}

	var total time.Duration

	// Look for 'd' for days
	if idx := strings.Index(s, "d"); idx != -1 {
		daysStr := s[:idx]
		days, err := strconv.ParseInt(daysStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid days in duration: %s", daysStr)
		}
		total = time.Duration(days) * 24 * time.Hour
		s = s[idx+1:]
	}

	// Parse remaining with Go's time.ParseDuration
	if s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("invalid duration: %w", err)
		}
		total += d
	}

	if negative {
		total = -total
	}

	return total, nil
}

// SetAsKey marks this column as a key column and builds an index.
func (c *DurationColumn) SetAsKey() {
	c.isKey = true
	c.valueIndex = make(map[int64]int)
	for i, d := range c.data {
		c.valueIndex[int64(d)] = i
	}
}

// IsKey returns true if this is a key column.
func (c *DurationColumn) IsKey() bool {
	return c.isKey
}

// FinalizeColumn performs any final processing after all data is loaded.
func (c *DurationColumn) FinalizeColumn() {
	// Nothing to finalize for duration columns
}

// LookupKey returns the row index for the given duration value (key columns only).
func (c *DurationColumn) LookupKey(d time.Duration) (int, bool) {
	if !c.isKey || c.valueIndex == nil {
		return -1, false
	}
	idx, ok := c.valueIndex[int64(d)]
	return idx, ok
}

// GroupByValue returns a map of duration values to row indices (internal use).
func (c *DurationColumn) GroupByValue() map[time.Duration][]uint32 {
	groups := make(map[time.Duration][]uint32)
	for i, d := range c.data {
		groups[d] = append(groups[d], uint32(i))
	}
	return groups
}

// CreateJoinedColumn creates a joined column for this duration column.
func (c *DurationColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedDurationColumn(columnDef, joiner, c)
}

// GroupIndices groups the given indices by duration value.
func (c *DurationColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[int64]uint32{}

	for _, i := range indices {
		if int(i) >= len(c.data) {
			continue
		}
		nanos := int64(c.data[i])
		if groupKey, ok := valueToGroupKey[nanos]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[nanos] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}

	return groupedIndices, nil
}

// Duration extraction methods

// Nanoseconds returns the duration in nanoseconds at the given index.
func (c *DurationColumn) Nanoseconds(i uint32) (int64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return int64(c.data[i]), nil
}

// Seconds returns the duration in seconds (fractional) at the given index.
func (c *DurationColumn) Seconds(i uint32) (float64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return c.data[i].Seconds(), nil
}

// Minutes returns the duration in minutes (fractional) at the given index.
func (c *DurationColumn) Minutes(i uint32) (float64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return c.data[i].Minutes(), nil
}

// Hours returns the duration in hours (fractional) at the given index.
func (c *DurationColumn) Hours(i uint32) (float64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return c.data[i].Hours(), nil
}

// Days returns the duration in days (fractional) at the given index.
func (c *DurationColumn) Days(i uint32) (float64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return c.data[i].Hours() / 24, nil
}

// TotalSeconds returns the total seconds (truncated) at the given index.
func (c *DurationColumn) TotalSeconds(i uint32) (int64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return int64(c.data[i] / time.Second), nil
}

// TotalMinutes returns the total minutes (truncated) at the given index.
func (c *DurationColumn) TotalMinutes(i uint32) (int64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return int64(c.data[i] / time.Minute), nil
}

// TotalHours returns the total hours (truncated) at the given index.
func (c *DurationColumn) TotalHours(i uint32) (int64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return int64(c.data[i] / time.Hour), nil
}

// TotalDays returns the total days (truncated) at the given index.
func (c *DurationColumn) TotalDays(i uint32) (int64, error) {
	if int(i) >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range", i)
	}
	return int64(c.data[i] / (24 * time.Hour)), nil
}

// Statistics methods

// Sum returns the sum of all durations in the column.
func (c *DurationColumn) Sum() time.Duration {
	var total time.Duration
	for _, d := range c.data {
		total += d
	}
	return total
}

// Avg returns the average duration in the column.
func (c *DurationColumn) Avg() time.Duration {
	if len(c.data) == 0 {
		return 0
	}
	return c.Sum() / time.Duration(len(c.data))
}

// Min returns the minimum duration in the column.
func (c *DurationColumn) Min() time.Duration {
	if len(c.data) == 0 {
		return 0
	}
	min := c.data[0]
	for _, d := range c.data[1:] {
		if d < min {
			min = d
		}
	}
	return min
}

// Max returns the maximum duration in the column.
func (c *DurationColumn) Max() time.Duration {
	if len(c.data) == 0 {
		return 0
	}
	max := c.data[0]
	for _, d := range c.data[1:] {
		if d > max {
			max = d
		}
	}
	return max
}

// Helper functions for creating durations

// DurationFromUnit creates a duration from a value and unit string.
// Supported units: "nanoseconds", "microseconds", "milliseconds", "seconds", "minutes", "hours", "days", "weeks"
func DurationFromUnit(value float64, unit string) (time.Duration, error) {
	unit = strings.ToLower(strings.TrimSpace(unit))

	var multiplier time.Duration
	switch unit {
	case "nanosecond", "nanoseconds", "ns":
		multiplier = time.Nanosecond
	case "microsecond", "microseconds", "us", "µs":
		multiplier = time.Microsecond
	case "millisecond", "milliseconds", "ms":
		multiplier = time.Millisecond
	case "second", "seconds", "s":
		multiplier = time.Second
	case "minute", "minutes", "m":
		multiplier = time.Minute
	case "hour", "hours", "h":
		multiplier = time.Hour
	case "day", "days", "d":
		multiplier = 24 * time.Hour
	case "week", "weeks", "w":
		multiplier = 7 * 24 * time.Hour
	default:
		return 0, fmt.Errorf("unknown duration unit: %s", unit)
	}

	// Handle fractional values
	nanos := value * float64(multiplier)
	if math.IsInf(nanos, 0) || math.IsNaN(nanos) {
		return 0, fmt.Errorf("duration overflow or invalid value")
	}

	return time.Duration(nanos), nil
}
