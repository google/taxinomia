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
	"strconv"
	"strings"
	"time"
)

// Common datetime display formats
const (
	DatetimeFormatISO      = time.RFC3339           // 2006-01-02T15:04:05Z07:00
	DatetimeFormatDate     = "2006-01-02"           // Date only
	DatetimeFormatDateTime = "2006-01-02 15:04:05"  // Date and time
	DatetimeFormatTime     = "15:04:05"             // Time only
)

// dateParseFormats lists formats to try when parsing datetime strings, in order of preference.
var dateParseFormats = []string{
	time.RFC3339Nano,           // 2006-01-02T15:04:05.999999999Z07:00
	time.RFC3339,               // 2006-01-02T15:04:05Z07:00
	"2006-01-02T15:04:05",      // ISO without timezone
	"2006-01-02 15:04:05",      // Space separator
	"2006-01-02",               // Date only (midnight)
	"2006/01/02",               // YYYY/MM/DD
	"02-Jan-2006",              // DD-Mon-YYYY
	"Jan 2, 2006",              // Natural format
	"January 2, 2006",          // Full month name
	"2006-01-02T15:04:05.000",  // ISO with milliseconds no TZ
	"2006-01-02 15:04:05.000",  // Space with milliseconds
}

// DatetimeColumn stores datetime values using Go's time.Time type.
type DatetimeColumn struct {
	columnDef     *ColumnDef
	data          []time.Time
	isKey         bool
	valueIndex    map[int64]int // Unix nanos -> rowIndex for key columns
	displayFormat string        // Format string for GetString()
	location      *time.Location
}

// NewDatetimeColumn creates a new datetime column with default ISO format.
func NewDatetimeColumn(columnDef *ColumnDef) *DatetimeColumn {
	return &DatetimeColumn{
		columnDef:     columnDef,
		data:          make([]time.Time, 0),
		displayFormat: DatetimeFormatDateTime,
		location:      time.UTC,
	}
}

// NewDatetimeColumnWithFormat creates a new datetime column with a custom display format.
func NewDatetimeColumnWithFormat(columnDef *ColumnDef, format string, loc *time.Location) *DatetimeColumn {
	if loc == nil {
		loc = time.UTC
	}
	return &DatetimeColumn{
		columnDef:     columnDef,
		data:          make([]time.Time, 0),
		displayFormat: format,
		location:      loc,
	}
}

// Append adds a time.Time value to the column.
func (c *DatetimeColumn) Append(value time.Time) {
	c.data = append(c.data, value.UTC())
}

// AppendUnix adds a value from Unix seconds.
func (c *DatetimeColumn) AppendUnix(seconds int64) {
	c.data = append(c.data, time.Unix(seconds, 0).UTC())
}

// AppendUnixNano adds a value from Unix nanoseconds.
func (c *DatetimeColumn) AppendUnixNano(nanos int64) {
	c.data = append(c.data, time.Unix(0, nanos).UTC())
}

// AppendString parses a string and appends the datetime value.
// Returns an error if parsing fails.
func (c *DatetimeColumn) AppendString(s string) error {
	t, err := ParseDatetime(s, c.location)
	if err != nil {
		return err
	}
	c.data = append(c.data, t.UTC())
	return nil
}

// Length returns the number of values in the column.
func (c *DatetimeColumn) Length() int {
	return len(c.data)
}

// ColumnDef returns the column definition.
func (c *DatetimeColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

// GetValue returns the time.Time value at index i.
func (c *DatetimeColumn) GetValue(i uint32) (time.Time, error) {
	if i >= uint32(len(c.data)) {
		return time.Time{}, fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	return c.data[i], nil
}

// GetString returns the formatted datetime string at index i.
func (c *DatetimeColumn) GetString(i uint32) (string, error) {
	if i >= uint32(len(c.data)) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.data))
	}
	t := c.data[i]
	if t.IsZero() {
		return "", nil
	}
	return t.In(c.location).Format(c.displayFormat), nil
}

// GetIndex returns the row index for a given time value (for key columns).
func (c *DatetimeColumn) GetIndex(v time.Time) (uint32, error) {
	if c.valueIndex == nil {
		return 0, fmt.Errorf("column %q is not a key column", c.columnDef.Name())
	}
	if idx, exists := c.valueIndex[v.UTC().UnixNano()]; exists {
		return uint32(idx), nil
	}
	return 0, fmt.Errorf("value %v not found in column %q", v, c.columnDef.Name())
}

// IsKey returns whether all values in the column are unique.
func (c *DatetimeColumn) IsKey() bool {
	return c.isKey
}

// FinalizeColumn should be called after all data has been added.
// It detects uniqueness and builds indexes if needed.
func (c *DatetimeColumn) FinalizeColumn() {
	tempIndex := make(map[int64]int)
	isUnique := true

	for i, value := range c.data {
		nanos := value.UnixNano()
		if _, exists := tempIndex[nanos]; exists {
			isUnique = false
			break
		}
		tempIndex[nanos] = i
	}

	c.isKey = isUnique

	if isUnique && c.columnDef.EntityType() != "" {
		c.valueIndex = tempIndex
	} else {
		c.valueIndex = nil
	}
}

// CreateJoinedColumn creates a joined column for this datetime column.
func (c *DatetimeColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedDatetimeColumn(columnDef, joiner, c)
}

// GroupIndices groups the given indices by their datetime value.
func (c *DatetimeColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[int64]uint32{}

	for _, i := range indices {
		if i >= uint32(len(c.data)) {
			continue
		}
		nanos := c.data[i].UnixNano()
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

// SetDisplayFormat changes the display format for GetString().
func (c *DatetimeColumn) SetDisplayFormat(format string) {
	c.displayFormat = format
}

// SetLocation changes the timezone for display.
func (c *DatetimeColumn) SetLocation(loc *time.Location) {
	if loc != nil {
		c.location = loc
	}
}

// --- Epoch-based extraction functions ---

// Seconds returns Unix seconds for the value at index i.
func (c *DatetimeColumn) Seconds(i uint32) (int64, error) {
	if i >= uint32(len(c.data)) {
		return 0, fmt.Errorf("index %d out of bounds", i)
	}
	return c.data[i].Unix(), nil
}

// Minutes returns minutes since Unix epoch for the value at index i.
func (c *DatetimeColumn) Minutes(i uint32) (int64, error) {
	if i >= uint32(len(c.data)) {
		return 0, fmt.Errorf("index %d out of bounds", i)
	}
	return c.data[i].Unix() / 60, nil
}

// Hours returns hours since Unix epoch for the value at index i.
func (c *DatetimeColumn) Hours(i uint32) (int64, error) {
	if i >= uint32(len(c.data)) {
		return 0, fmt.Errorf("index %d out of bounds", i)
	}
	return c.data[i].Unix() / 3600, nil
}

// Days returns days since Unix epoch for the value at index i.
func (c *DatetimeColumn) Days(i uint32) (int64, error) {
	if i >= uint32(len(c.data)) {
		return 0, fmt.Errorf("index %d out of bounds", i)
	}
	return c.data[i].Unix() / 86400, nil
}

// Weeks returns weeks since Unix epoch for the value at index i.
func (c *DatetimeColumn) Weeks(i uint32) (int64, error) {
	if i >= uint32(len(c.data)) {
		return 0, fmt.Errorf("index %d out of bounds", i)
	}
	return c.data[i].Unix() / (86400 * 7), nil
}

// Months returns exact months since Unix epoch (Jan 1970) for the value at index i.
// Calculated as: (year - 1970) * 12 + (month - 1)
func (c *DatetimeColumn) Months(i uint32) (int64, error) {
	if i >= uint32(len(c.data)) {
		return 0, fmt.Errorf("index %d out of bounds", i)
	}
	t := c.data[i].UTC()
	return int64(t.Year()-1970)*12 + int64(t.Month()-1), nil
}

// Quarters returns exact quarters since Unix epoch (Q1 1970) for the value at index i.
// Calculated as: (year - 1970) * 4 + ((month - 1) / 3)
func (c *DatetimeColumn) Quarters(i uint32) (int64, error) {
	if i >= uint32(len(c.data)) {
		return 0, fmt.Errorf("index %d out of bounds", i)
	}
	t := c.data[i].UTC()
	return int64(t.Year()-1970)*4 + int64(t.Month()-1)/3, nil
}

// Years returns years since Unix epoch (1970) for the value at index i.
func (c *DatetimeColumn) Years(i uint32) (int64, error) {
	if i >= uint32(len(c.data)) {
		return 0, fmt.Errorf("index %d out of bounds", i)
	}
	return int64(c.data[i].UTC().Year() - 1970), nil
}

// --- Parsing utilities ---

// ParseDatetime attempts to parse a string as a datetime value.
// Tries multiple formats and returns the first successful parse.
func ParseDatetime(s string, defaultLoc *time.Location) (time.Time, error) {
	s = strings.TrimSpace(s)

	// Handle empty/null values
	if s == "" || s == "null" || s == "nil" || s == "NULL" {
		return time.Time{}, nil
	}

	if defaultLoc == nil {
		defaultLoc = time.UTC
	}

	// Handle Unix timestamp (numeric)
	if isNumericString(s) {
		return parseUnixTimestamp(s)
	}

	// Try each format
	for _, format := range dateParseFormats {
		if t, err := time.ParseInLocation(format, s, defaultLoc); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse datetime: %q", s)
}

// isNumericString checks if a string contains only digits and optional leading minus.
func isNumericString(s string) bool {
	if len(s) == 0 {
		return false
	}
	start := 0
	if s[0] == '-' {
		start = 1
	}
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return start < len(s)
}

// parseUnixTimestamp parses a numeric string as Unix timestamp.
// Handles seconds, milliseconds, and nanoseconds based on magnitude.
// Thresholds:
//   - Seconds: timestamps up to ~3e11 (year ~11000)
//   - Milliseconds: timestamps from ~1e11 to ~1e16
//   - Nanoseconds: timestamps > 1e16
func parseUnixTimestamp(s string) (time.Time, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	absN := n
	if absN < 0 {
		absN = -absN
	}

	// Determine if seconds, milliseconds, or nanoseconds based on magnitude
	switch {
	case absN > 1e16:
		// Nanoseconds (current epoch ~1.7e18)
		return time.Unix(0, n), nil
	case absN > 1e11:
		// Milliseconds (current epoch ~1.7e12)
		return time.Unix(n/1000, (n%1000)*1e6), nil
	default:
		// Seconds (current epoch ~1.7e9)
		return time.Unix(n, 0), nil
	}
}
