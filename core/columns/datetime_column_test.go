/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package columns

import (
	"testing"
	"time"
)

func TestDatetimeColumn_BasicOperations(t *testing.T) {
	colDef := NewColumnDef("created_at", "Created At", "")
	col := NewDatetimeColumn(colDef)

	// Append various values
	t1 := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	t2 := time.Date(2024, 6, 20, 14, 45, 30, 0, time.UTC)
	t3 := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) // Epoch

	col.Append(t1)
	col.Append(t2)
	col.Append(t3)

	if col.Length() != 3 {
		t.Errorf("expected length 3, got %d", col.Length())
	}

	// Test GetValue
	got, err := col.GetValue(0)
	if err != nil {
		t.Errorf("GetValue(0) error: %v", err)
	}
	if !got.Equal(t1) {
		t.Errorf("GetValue(0) = %v, want %v", got, t1)
	}

	// Test GetString
	str, err := col.GetString(0)
	if err != nil {
		t.Errorf("GetString(0) error: %v", err)
	}
	expected := "2024-01-15 10:30:00"
	if str != expected {
		t.Errorf("GetString(0) = %q, want %q", str, expected)
	}
}

func TestDatetimeColumn_AppendString(t *testing.T) {
	colDef := NewColumnDef("date", "Date", "")
	col := NewDatetimeColumn(colDef)

	testCases := []struct {
		input    string
		wantYear int
		wantMon  time.Month
		wantDay  int
	}{
		{"2024-01-15", 2024, 1, 15},
		{"2024-01-15T10:30:00Z", 2024, 1, 15},
		{"2024-01-15 14:30:00", 2024, 1, 15},
		{"2024/06/20", 2024, 6, 20},
	}

	for _, tc := range testCases {
		err := col.AppendString(tc.input)
		if err != nil {
			t.Errorf("AppendString(%q) error: %v", tc.input, err)
			continue
		}
	}

	if col.Length() != len(testCases) {
		t.Errorf("expected length %d, got %d", len(testCases), col.Length())
	}

	// Verify first value
	val, _ := col.GetValue(0)
	if val.Year() != 2024 || val.Month() != 1 || val.Day() != 15 {
		t.Errorf("first value = %v, want 2024-01-15", val)
	}
}

func TestDatetimeColumn_AppendUnix(t *testing.T) {
	colDef := NewColumnDef("timestamp", "Timestamp", "")
	col := NewDatetimeColumn(colDef)

	// Unix epoch
	col.AppendUnix(0)
	val, _ := col.GetValue(0)
	if !val.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("AppendUnix(0) = %v, want epoch", val)
	}

	// A known timestamp: 2024-01-01 00:00:00 UTC = 1704067200
	col.AppendUnix(1704067200)
	val, _ = col.GetValue(1)
	if val.Year() != 2024 || val.Month() != 1 || val.Day() != 1 {
		t.Errorf("AppendUnix(1704067200) = %v, want 2024-01-01", val)
	}
}

func TestDatetimeColumn_EpochFunctions(t *testing.T) {
	colDef := NewColumnDef("date", "Date", "")
	col := NewDatetimeColumn(colDef)

	// Test with 2024-07-15 12:30:00 UTC
	testTime := time.Date(2024, 7, 15, 12, 30, 0, 0, time.UTC)
	col.Append(testTime)

	// Seconds
	secs, _ := col.Seconds(0)
	if secs != testTime.Unix() {
		t.Errorf("Seconds() = %d, want %d", secs, testTime.Unix())
	}

	// Minutes
	mins, _ := col.Minutes(0)
	expectedMins := testTime.Unix() / 60
	if mins != expectedMins {
		t.Errorf("Minutes() = %d, want %d", mins, expectedMins)
	}

	// Hours
	hours, _ := col.Hours(0)
	expectedHours := testTime.Unix() / 3600
	if hours != expectedHours {
		t.Errorf("Hours() = %d, want %d", hours, expectedHours)
	}

	// Days
	days, _ := col.Days(0)
	expectedDays := testTime.Unix() / 86400
	if days != expectedDays {
		t.Errorf("Days() = %d, want %d", days, expectedDays)
	}

	// Weeks
	weeks, _ := col.Weeks(0)
	expectedWeeks := testTime.Unix() / (86400 * 7)
	if weeks != expectedWeeks {
		t.Errorf("Weeks() = %d, want %d", weeks, expectedWeeks)
	}
}

func TestDatetimeColumn_ExactMonths(t *testing.T) {
	colDef := NewColumnDef("date", "Date", "")
	col := NewDatetimeColumn(colDef)

	testCases := []struct {
		date         time.Time
		wantMonths   int64
		wantQuarters int64
		wantYears    int64
	}{
		// Epoch: Jan 1970
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), 0, 0, 0},
		// Feb 1970 = month 1
		{time.Date(1970, 2, 15, 0, 0, 0, 0, time.UTC), 1, 0, 0},
		// Dec 1970 = month 11
		{time.Date(1970, 12, 31, 0, 0, 0, 0, time.UTC), 11, 3, 0},
		// Jan 1971 = month 12
		{time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC), 12, 4, 1},
		// Jan 2024 = (2024-1970)*12 + 0 = 54*12 = 648
		{time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), 648, 216, 54},
		// Jul 2024 = (2024-1970)*12 + 6 = 654
		{time.Date(2024, 7, 15, 0, 0, 0, 0, time.UTC), 654, 218, 54},
		// Q2 = Apr-Jun, so Apr 2024 = quarter (54*4 + 1) = 217
		{time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC), 651, 217, 54},
		// Q3 = Jul-Sep, so Jul 2024 = quarter (54*4 + 2) = 218
		{time.Date(2024, 9, 30, 0, 0, 0, 0, time.UTC), 656, 218, 54},
		// Q4 = Oct-Dec, so Oct 2024 = quarter (54*4 + 3) = 219
		{time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC), 657, 219, 54},
	}

	for _, tc := range testCases {
		col = NewDatetimeColumn(colDef) // Fresh column
		col.Append(tc.date)

		months, err := col.Months(0)
		if err != nil {
			t.Errorf("Months() for %v error: %v", tc.date, err)
		}
		if months != tc.wantMonths {
			t.Errorf("Months() for %v = %d, want %d", tc.date, months, tc.wantMonths)
		}

		quarters, err := col.Quarters(0)
		if err != nil {
			t.Errorf("Quarters() for %v error: %v", tc.date, err)
		}
		if quarters != tc.wantQuarters {
			t.Errorf("Quarters() for %v = %d, want %d", tc.date, quarters, tc.wantQuarters)
		}

		years, err := col.Years(0)
		if err != nil {
			t.Errorf("Years() for %v error: %v", tc.date, err)
		}
		if years != tc.wantYears {
			t.Errorf("Years() for %v = %d, want %d", tc.date, years, tc.wantYears)
		}
	}
}

func TestDatetimeColumn_GroupIndices(t *testing.T) {
	colDef := NewColumnDef("date", "Date", "")
	col := NewDatetimeColumn(colDef)

	// Add dates with duplicates
	t1 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC)

	col.Append(t1)
	col.Append(t2)
	col.Append(t1) // Duplicate of t1
	col.Append(t2) // Duplicate of t2
	col.Append(t1) // Another duplicate

	indices := []uint32{0, 1, 2, 3, 4}
	groups, unmapped := col.GroupIndices(indices, nil)

	if len(unmapped) != 0 {
		t.Errorf("expected no unmapped indices, got %v", unmapped)
	}

	if len(groups) != 2 {
		t.Errorf("expected 2 groups, got %d", len(groups))
	}

	// Count total indices
	total := 0
	for _, g := range groups {
		total += len(g)
	}
	if total != 5 {
		t.Errorf("expected 5 total indices, got %d", total)
	}
}

func TestDatetimeColumn_IsKey(t *testing.T) {
	colDef := NewColumnDef("id", "ID", "timestamp_id")

	// Unique values
	col1 := NewDatetimeColumn(colDef)
	col1.Append(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	col1.Append(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
	col1.Append(time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC))
	col1.FinalizeColumn()

	if !col1.IsKey() {
		t.Error("expected IsKey=true for unique values")
	}

	// Non-unique values
	col2 := NewDatetimeColumn(colDef)
	col2.Append(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	col2.Append(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) // Duplicate
	col2.FinalizeColumn()

	if col2.IsKey() {
		t.Error("expected IsKey=false for duplicate values")
	}
}

func TestParseDatetime(t *testing.T) {
	testCases := []struct {
		input   string
		wantErr bool
		check   func(time.Time) bool
	}{
		{"2024-01-15", false, func(t time.Time) bool { return t.Year() == 2024 && t.Month() == 1 && t.Day() == 15 }},
		{"2024-01-15T10:30:00Z", false, func(t time.Time) bool { return t.Hour() == 10 && t.Minute() == 30 }},
		{"2024-01-15 14:45:30", false, func(t time.Time) bool { return t.Hour() == 14 && t.Minute() == 45 }},
		{"2024/06/20", false, func(t time.Time) bool { return t.Month() == 6 && t.Day() == 20 }},
		{"", false, func(t time.Time) bool { return t.IsZero() }},
		{"null", false, func(t time.Time) bool { return t.IsZero() }},
		{"invalid", true, nil},
		// Unix timestamp (seconds)
		{"1704067200", false, func(t time.Time) bool { return t.Year() == 2024 && t.Month() == 1 && t.Day() == 1 }},
		// Unix timestamp (milliseconds)
		{"1704067200000", false, func(t time.Time) bool { return t.Year() == 2024 && t.Month() == 1 && t.Day() == 1 }},
	}

	for _, tc := range testCases {
		got, err := ParseDatetime(tc.input, time.UTC)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseDatetime(%q) expected error, got %v", tc.input, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseDatetime(%q) error: %v", tc.input, err)
			continue
		}
		if tc.check != nil && !tc.check(got) {
			t.Errorf("ParseDatetime(%q) = %v, failed check", tc.input, got)
		}
	}
}

func TestDatetimeColumn_DisplayFormat(t *testing.T) {
	colDef := NewColumnDef("date", "Date", "")

	// Test date-only format
	col := NewDatetimeColumnWithFormat(colDef, DatetimeFormatDate, time.UTC)
	col.Append(time.Date(2024, 7, 15, 14, 30, 45, 0, time.UTC))

	str, _ := col.GetString(0)
	if str != "2024-07-15" {
		t.Errorf("GetString with DatetimeFormatDate = %q, want %q", str, "2024-07-15")
	}

	// Change format
	col.SetDisplayFormat(DatetimeFormatISO)
	str, _ = col.GetString(0)
	if str != "2024-07-15T14:30:45Z" {
		t.Errorf("GetString with DatetimeFormatISO = %q, want %q", str, "2024-07-15T14:30:45Z")
	}
}
