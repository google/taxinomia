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
	"testing"
	"time"
)

func TestDurationColumnBasicOperations(t *testing.T) {
	col := NewDurationColumn(NewColumnDef("duration", "Duration", ""))

	// Test Append
	col.Append(2 * time.Hour)
	col.Append(30 * time.Minute)
	col.Append(45 * time.Second)

	if col.Length() != 3 {
		t.Errorf("Expected length 3, got %d", col.Length())
	}

	// Test GetValue
	val, err := col.GetValue(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 2*time.Hour {
		t.Errorf("Expected 2h, got %v", val)
	}

	// Test GetString (compact format)
	str, err := col.GetString(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if str != "2h0m0s" {
		t.Errorf("Expected '2h0m0s', got '%s'", str)
	}
}

func TestDurationColumnAppendMethods(t *testing.T) {
	col := NewDurationColumn(NewColumnDef("duration", "Duration", ""))

	// Test AppendNanoseconds
	col.AppendNanoseconds(int64(3 * time.Hour))
	val, _ := col.GetValue(0)
	if val != 3*time.Hour {
		t.Errorf("AppendNanoseconds: expected 3h, got %v", val)
	}

	// Test AppendSeconds
	col.AppendSeconds(90.5) // 1 minute 30.5 seconds
	val, _ = col.GetValue(1)
	expected := time.Duration(90.5 * float64(time.Second))
	if val != expected {
		t.Errorf("AppendSeconds: expected %v, got %v", expected, val)
	}

	// Test AppendString
	err := col.AppendString("1h30m")
	if err != nil {
		t.Errorf("AppendString: unexpected error: %v", err)
	}
	val, _ = col.GetValue(2)
	if val != 90*time.Minute {
		t.Errorf("AppendString: expected 1h30m, got %v", val)
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
		hasError bool
	}{
		// Standard Go format
		{"1h", time.Hour, false},
		{"30m", 30 * time.Minute, false},
		{"45s", 45 * time.Second, false},
		{"1h30m", 90 * time.Minute, false},
		{"2h30m45s", 2*time.Hour + 30*time.Minute + 45*time.Second, false},

		// Extended format with days
		{"1d", 24 * time.Hour, false},
		{"3d", 72 * time.Hour, false},
		{"1d12h", 36 * time.Hour, false},
		{"2d3h30m", 2*24*time.Hour + 3*time.Hour + 30*time.Minute, false},

		// Negative durations
		{"-1h", -time.Hour, false},
		{"-2d", -48 * time.Hour, false},
		{"-1d12h", -36 * time.Hour, false},

		// Edge cases
		{"0s", 0, false},
		{"", 0, false},
		{"  1h  ", time.Hour, false},

		// Invalid input
		{"invalid", 0, true},
		{"1x", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result, err := ParseDuration(tc.input)
			if tc.hasError {
				if err == nil {
					t.Errorf("Expected error for input '%s', got nil", tc.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for input '%s': %v", tc.input, err)
				}
				if result != tc.expected {
					t.Errorf("For input '%s': expected %v, got %v", tc.input, tc.expected, result)
				}
			}
		})
	}
}

func TestFormatDurationCompact(t *testing.T) {
	tests := []struct {
		input    time.Duration
		expected string
	}{
		{0, "0s"},
		{time.Second, "1s"},
		{time.Minute, "1m0s"},
		{time.Hour, "1h0m0s"},
		{90 * time.Minute, "1h30m0s"},
		{24 * time.Hour, "1d"},
		{25 * time.Hour, "1d1h0m0s"},
		{48*time.Hour + 2*time.Hour + 30*time.Minute, "2d2h30m0s"},
		{-time.Hour, "-1h0m0s"},
		{-24 * time.Hour, "-1d"},
		{500 * time.Millisecond, "500ms"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			result := formatDurationCompact(tc.input)
			if result != tc.expected {
				t.Errorf("For duration %v: expected '%s', got '%s'", tc.input, tc.expected, result)
			}
		})
	}
}

func TestFormatDurationVerbose(t *testing.T) {
	tests := []struct {
		input    time.Duration
		expected string
	}{
		{0, "0 seconds"},
		{time.Second, "1 second"},
		{2 * time.Second, "2 seconds"},
		{time.Minute, "1 minute"},
		{2 * time.Minute, "2 minutes"},
		{time.Hour, "1 hour"},
		{2 * time.Hour, "2 hours"},
		{24 * time.Hour, "1 day"},
		{48 * time.Hour, "2 days"},
		{90 * time.Minute, "1 hour 30 minutes"},
		{2*time.Hour + 30*time.Minute + 45*time.Second, "2 hours 30 minutes 45 seconds"},
		{-time.Hour, "-1 hour"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			result := formatDurationVerbose(tc.input)
			if result != tc.expected {
				t.Errorf("For duration %v: expected '%s', got '%s'", tc.input, tc.expected, result)
			}
		})
	}
}

func TestDurationColumnExtractionMethods(t *testing.T) {
	col := NewDurationColumn(NewColumnDef("duration", "Duration", ""))

	// Add a duration of 2 days, 3 hours, 30 minutes, 45 seconds
	d := 2*24*time.Hour + 3*time.Hour + 30*time.Minute + 45*time.Second
	col.Append(d)

	// Test Nanoseconds
	nanos, _ := col.Nanoseconds(0)
	if nanos != int64(d) {
		t.Errorf("Nanoseconds: expected %d, got %d", int64(d), nanos)
	}

	// Test Seconds
	secs, _ := col.Seconds(0)
	expectedSecs := d.Seconds()
	if secs != expectedSecs {
		t.Errorf("Seconds: expected %f, got %f", expectedSecs, secs)
	}

	// Test Minutes
	mins, _ := col.Minutes(0)
	expectedMins := d.Minutes()
	if mins != expectedMins {
		t.Errorf("Minutes: expected %f, got %f", expectedMins, mins)
	}

	// Test Hours
	hrs, _ := col.Hours(0)
	expectedHrs := d.Hours()
	if hrs != expectedHrs {
		t.Errorf("Hours: expected %f, got %f", expectedHrs, hrs)
	}

	// Test Days
	days, _ := col.Days(0)
	expectedDays := d.Hours() / 24
	if days != expectedDays {
		t.Errorf("Days: expected %f, got %f", expectedDays, days)
	}

	// Test TotalSeconds (truncated)
	totalSecs, _ := col.TotalSeconds(0)
	expectedTotalSecs := int64(d / time.Second)
	if totalSecs != expectedTotalSecs {
		t.Errorf("TotalSeconds: expected %d, got %d", expectedTotalSecs, totalSecs)
	}

	// Test TotalMinutes (truncated)
	totalMins, _ := col.TotalMinutes(0)
	expectedTotalMins := int64(d / time.Minute)
	if totalMins != expectedTotalMins {
		t.Errorf("TotalMinutes: expected %d, got %d", expectedTotalMins, totalMins)
	}

	// Test TotalHours (truncated)
	totalHrs, _ := col.TotalHours(0)
	expectedTotalHrs := int64(d / time.Hour)
	if totalHrs != expectedTotalHrs {
		t.Errorf("TotalHours: expected %d, got %d", expectedTotalHrs, totalHrs)
	}

	// Test TotalDays (truncated)
	totalDays, _ := col.TotalDays(0)
	expectedTotalDays := int64(d / (24 * time.Hour))
	if totalDays != expectedTotalDays {
		t.Errorf("TotalDays: expected %d, got %d", expectedTotalDays, totalDays)
	}
}

func TestDurationColumnStatistics(t *testing.T) {
	col := NewDurationColumn(NewColumnDef("duration", "Duration", ""))

	col.Append(1 * time.Hour)
	col.Append(2 * time.Hour)
	col.Append(3 * time.Hour)
	col.Append(4 * time.Hour)

	// Test Sum
	sum := col.Sum()
	if sum != 10*time.Hour {
		t.Errorf("Sum: expected 10h, got %v", sum)
	}

	// Test Avg
	avg := col.Avg()
	if avg != 2*time.Hour+30*time.Minute {
		t.Errorf("Avg: expected 2h30m, got %v", avg)
	}

	// Test Min
	min := col.Min()
	if min != time.Hour {
		t.Errorf("Min: expected 1h, got %v", min)
	}

	// Test Max
	max := col.Max()
	if max != 4*time.Hour {
		t.Errorf("Max: expected 4h, got %v", max)
	}
}

func TestDurationColumnKey(t *testing.T) {
	col := NewDurationColumn(NewColumnDef("duration", "Duration", ""))

	col.Append(1 * time.Hour)
	col.Append(2 * time.Hour)
	col.Append(3 * time.Hour)

	col.SetAsKey()

	if !col.IsKey() {
		t.Error("Expected column to be marked as key")
	}

	// Test LookupKey
	idx, found := col.LookupKey(2 * time.Hour)
	if !found {
		t.Error("Expected to find 2h in key index")
	}
	if idx != 1 {
		t.Errorf("Expected index 1 for 2h, got %d", idx)
	}

	// Test not found
	_, found = col.LookupKey(5 * time.Hour)
	if found {
		t.Error("Expected not to find 5h in key index")
	}
}

func TestDurationColumnGroupByValue(t *testing.T) {
	col := NewDurationColumn(NewColumnDef("duration", "Duration", ""))

	col.Append(1 * time.Hour)
	col.Append(2 * time.Hour)
	col.Append(1 * time.Hour) // Duplicate
	col.Append(3 * time.Hour)
	col.Append(2 * time.Hour) // Duplicate

	groups := col.GroupByValue()

	if len(groups) != 3 {
		t.Errorf("Expected 3 groups, got %d", len(groups))
	}

	if len(groups[1*time.Hour]) != 2 {
		t.Errorf("Expected 2 items in 1h group, got %d", len(groups[1*time.Hour]))
	}

	if len(groups[2*time.Hour]) != 2 {
		t.Errorf("Expected 2 items in 2h group, got %d", len(groups[2*time.Hour]))
	}

	if len(groups[3*time.Hour]) != 1 {
		t.Errorf("Expected 1 item in 3h group, got %d", len(groups[3*time.Hour]))
	}
}

func TestDurationFromUnit(t *testing.T) {
	tests := []struct {
		value    float64
		unit     string
		expected time.Duration
		hasError bool
	}{
		{1, "nanosecond", time.Nanosecond, false},
		{1, "nanoseconds", time.Nanosecond, false},
		{1, "ns", time.Nanosecond, false},
		{1, "microsecond", time.Microsecond, false},
		{1, "us", time.Microsecond, false},
		{1, "millisecond", time.Millisecond, false},
		{1, "ms", time.Millisecond, false},
		{1, "second", time.Second, false},
		{1, "seconds", time.Second, false},
		{1, "s", time.Second, false},
		{1, "minute", time.Minute, false},
		{1, "minutes", time.Minute, false},
		{1, "m", time.Minute, false},
		{1, "hour", time.Hour, false},
		{1, "hours", time.Hour, false},
		{1, "h", time.Hour, false},
		{1, "day", 24 * time.Hour, false},
		{1, "days", 24 * time.Hour, false},
		{1, "d", 24 * time.Hour, false},
		{1, "week", 7 * 24 * time.Hour, false},
		{1, "weeks", 7 * 24 * time.Hour, false},
		{1, "w", 7 * 24 * time.Hour, false},

		// Fractional values
		{1.5, "hours", time.Hour + 30*time.Minute, false},
		{2.5, "days", 60 * time.Hour, false},

		// Case insensitivity
		{1, "HOUR", time.Hour, false},
		{1, "Hour", time.Hour, false},

		// Invalid unit
		{1, "invalid", 0, true},
		{1, "xyz", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.unit, func(t *testing.T) {
			result, err := DurationFromUnit(tc.value, tc.unit)
			if tc.hasError {
				if err == nil {
					t.Errorf("Expected error for unit '%s', got nil", tc.unit)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for unit '%s': %v", tc.unit, err)
				}
				if result != tc.expected {
					t.Errorf("For %f %s: expected %v, got %v", tc.value, tc.unit, tc.expected, result)
				}
			}
		})
	}
}

func TestDurationColumnWithFormat(t *testing.T) {
	// Test compact format
	compactCol := NewDurationColumnWithFormat(NewColumnDef("compact", "Compact", ""), DurationFormatCompact)
	compactCol.Append(2*time.Hour + 30*time.Minute)
	str, err := compactCol.GetString(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if str != "2h30m0s" {
		t.Errorf("Compact format: expected '2h30m0s', got '%s'", str)
	}

	// Test verbose format
	verboseCol := NewDurationColumnWithFormat(NewColumnDef("verbose", "Verbose", ""), DurationFormatVerbose)
	verboseCol.Append(2*time.Hour + 30*time.Minute)
	str, err = verboseCol.GetString(0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if str != "2 hours 30 minutes" {
		t.Errorf("Verbose format: expected '2 hours 30 minutes', got '%s'", str)
	}
}

func TestDurationColumnOutOfBounds(t *testing.T) {
	col := NewDurationColumn(NewColumnDef("duration", "Duration", ""))
	col.Append(time.Hour)

	// Test GetValue out of bounds
	_, err := col.GetValue(5)
	if err == nil {
		t.Error("Expected error for out of bounds GetValue")
	}

	// Test GetString out of bounds (returns error)
	_, err = col.GetString(5)
	if err == nil {
		t.Error("Expected error for out of bounds GetString")
	}

	// Test extraction methods out of bounds
	_, err = col.Nanoseconds(5)
	if err == nil {
		t.Error("Expected error for out of bounds Nanoseconds")
	}

	_, err = col.Seconds(5)
	if err == nil {
		t.Error("Expected error for out of bounds Seconds")
	}
}

func TestDurationColumnEmptyStatistics(t *testing.T) {
	col := NewDurationColumn(NewColumnDef("duration", "Duration", ""))

	// Statistics on empty column should return zero
	if col.Sum() != 0 {
		t.Error("Expected Sum() = 0 for empty column")
	}
	if col.Avg() != 0 {
		t.Error("Expected Avg() = 0 for empty column")
	}
	if col.Min() != 0 {
		t.Error("Expected Min() = 0 for empty column")
	}
	if col.Max() != 0 {
		t.Error("Expected Max() = 0 for empty column")
	}
}
