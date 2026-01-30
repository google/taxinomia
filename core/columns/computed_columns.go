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
	"time"
)

// ComputeStringFn is a function that computes a string value for a given row index.
// The function should capture any source columns it needs in a closure.
type ComputeStringFn func(i uint32) (string, error)

// ComputeUint32Fn is a function that computes a uint32 value for a given row index.
// The function should capture any source columns it needs in a closure.
type ComputeUint32Fn func(i uint32) (uint32, error)

// ComputeFloat64Fn is a function that computes a float64 value for a given row index.
// The function should capture any source columns it needs in a closure.
type ComputeFloat64Fn func(i uint32) (float64, error)

// ComputeInt64Fn is a function that computes an int64 value for a given row index.
// The function should capture any source columns it needs in a closure.
type ComputeInt64Fn func(i uint32) (int64, error)

// ComputedStringColumn represents a column whose values are computed from other columns.
type ComputedStringColumn struct {
	columnDef *ColumnDef
	computeFn ComputeStringFn
	length    int
}

// NewComputedStringColumn creates a new computed string column.
// The length parameter specifies the number of rows (should match source columns).
// The computeFn should be a closure that captures source columns and computes the value for each row.
func NewComputedStringColumn(columnDef *ColumnDef, length int, computeFn ComputeStringFn) *ComputedStringColumn {
	return &ComputedStringColumn{
		columnDef: columnDef,
		computeFn: computeFn,
		length:    length,
	}
}

func (c *ComputedStringColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ComputedStringColumn) Length() int {
	return c.length
}

func (c *ComputedStringColumn) GetValue(i uint32) (string, error) {
	if i >= uint32(c.length) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, c.length)
	}
	return c.computeFn(i)
}

func (c *ComputedStringColumn) GetString(i uint32) (string, error) {
	return c.GetValue(i)
}

func (c *ComputedStringColumn) GetIndex(value string) (uint32, error) {
	return 0, fmt.Errorf("column %q is a computed column and doesn't support reverse lookups", c.columnDef.Name())
}

func (c *ComputedStringColumn) IsKey() bool {
	return false
}

func (c *ComputedStringColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *ComputedStringColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[string]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		value, err := c.GetValue(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		if groupKey, ok := valueToGroupKey[value]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}

// ComputedUint32Column represents a column whose uint32 values are computed from other columns.
type ComputedUint32Column struct {
	columnDef *ColumnDef
	computeFn ComputeUint32Fn
	length    int
}

// NewComputedUint32Column creates a new computed uint32 column.
// The length parameter specifies the number of rows (should match source columns).
// The computeFn should be a closure that captures source columns and computes the value for each row.
func NewComputedUint32Column(columnDef *ColumnDef, length int, computeFn ComputeUint32Fn) *ComputedUint32Column {
	return &ComputedUint32Column{
		columnDef: columnDef,
		computeFn: computeFn,
		length:    length,
	}
}

func (c *ComputedUint32Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ComputedUint32Column) Length() int {
	return c.length
}

func (c *ComputedUint32Column) GetValue(i uint32) (uint32, error) {
	if i >= uint32(c.length) {
		return 0, fmt.Errorf("index %d out of bounds (length: %d)", i, c.length)
	}
	return c.computeFn(i)
}

func (c *ComputedUint32Column) GetString(i uint32) (string, error) {
	value, err := c.GetValue(i)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", value), nil
}

func (c *ComputedUint32Column) GetIndex(value uint32) (uint32, error) {
	return 0, fmt.Errorf("column %q is a computed column and doesn't support reverse lookups", c.columnDef.Name())
}

func (c *ComputedUint32Column) IsKey() bool {
	return false
}

func (c *ComputedUint32Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *ComputedUint32Column) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[uint32]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		value, err := c.GetValue(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		if groupKey, ok := valueToGroupKey[value]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}

// ComputedFloat64Column represents a column whose float64 values are computed from other columns.
type ComputedFloat64Column struct {
	columnDef *ColumnDef
	computeFn ComputeFloat64Fn
	length    int
}

// NewComputedFloat64Column creates a new computed float64 column.
// The length parameter specifies the number of rows (should match source columns).
// The computeFn should be a closure that captures source columns and computes the value for each row.
func NewComputedFloat64Column(columnDef *ColumnDef, length int, computeFn ComputeFloat64Fn) *ComputedFloat64Column {
	return &ComputedFloat64Column{
		columnDef: columnDef,
		computeFn: computeFn,
		length:    length,
	}
}

func (c *ComputedFloat64Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ComputedFloat64Column) Length() int {
	return c.length
}

func (c *ComputedFloat64Column) GetValue(i uint32) (float64, error) {
	if i >= uint32(c.length) {
		return 0, fmt.Errorf("index %d out of bounds (length: %d)", i, c.length)
	}
	return c.computeFn(i)
}

func (c *ComputedFloat64Column) GetString(i uint32) (string, error) {
	value, err := c.GetValue(i)
	if err != nil {
		return "", err
	}
	// Format without trailing zeros
	return strconv.FormatFloat(value, 'f', -1, 64), nil
}

func (c *ComputedFloat64Column) GetIndex(value float64) (uint32, error) {
	return 0, fmt.Errorf("column %q is a computed column and doesn't support reverse lookups", c.columnDef.Name())
}

func (c *ComputedFloat64Column) IsKey() bool {
	return false
}

func (c *ComputedFloat64Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *ComputedFloat64Column) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[float64]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		value, err := c.GetValue(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		if groupKey, ok := valueToGroupKey[value]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}

// ComputedInt64Column represents a column whose int64 values are computed from other columns.
type ComputedInt64Column struct {
	columnDef *ColumnDef
	computeFn ComputeInt64Fn
	length    int
}

// NewComputedInt64Column creates a new computed int64 column.
// The length parameter specifies the number of rows (should match source columns).
// The computeFn should be a closure that captures source columns and computes the value for each row.
func NewComputedInt64Column(columnDef *ColumnDef, length int, computeFn ComputeInt64Fn) *ComputedInt64Column {
	return &ComputedInt64Column{
		columnDef: columnDef,
		computeFn: computeFn,
		length:    length,
	}
}

func (c *ComputedInt64Column) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ComputedInt64Column) Length() int {
	return c.length
}

func (c *ComputedInt64Column) GetValue(i uint32) (int64, error) {
	if i >= uint32(c.length) {
		return 0, fmt.Errorf("index %d out of bounds (length: %d)", i, c.length)
	}
	return c.computeFn(i)
}

func (c *ComputedInt64Column) GetString(i uint32) (string, error) {
	value, err := c.GetValue(i)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(value, 10), nil
}

func (c *ComputedInt64Column) GetIndex(value int64) (uint32, error) {
	return 0, fmt.Errorf("column %q is a computed column and doesn't support reverse lookups", c.columnDef.Name())
}

func (c *ComputedInt64Column) IsKey() bool {
	return false
}

func (c *ComputedInt64Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *ComputedInt64Column) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[int64]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		value, err := c.GetValue(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		if groupKey, ok := valueToGroupKey[value]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}

// ComputeDatetimeFn is a function that computes a time.Time value for a given row index.
// The function should capture any source columns it needs in a closure.
type ComputeDatetimeFn func(i uint32) (int64, error) // Returns Unix nanoseconds

// ComputedDatetimeColumn represents a column whose datetime values are computed from other columns.
type ComputedDatetimeColumn struct {
	columnDef     *ColumnDef
	computeFn     ComputeDatetimeFn
	length        int
	displayFormat string
}

// NewComputedDatetimeColumn creates a new computed datetime column.
// The length parameter specifies the number of rows (should match source columns).
// The computeFn returns Unix nanoseconds.
func NewComputedDatetimeColumn(columnDef *ColumnDef, length int, computeFn ComputeDatetimeFn) *ComputedDatetimeColumn {
	return &ComputedDatetimeColumn{
		columnDef:     columnDef,
		computeFn:     computeFn,
		length:        length,
		displayFormat: "2006-01-02 15:04:05",
	}
}

// NewComputedDatetimeColumnWithFormat creates a new computed datetime column with a custom display format.
func NewComputedDatetimeColumnWithFormat(columnDef *ColumnDef, length int, computeFn ComputeDatetimeFn, format string) *ComputedDatetimeColumn {
	return &ComputedDatetimeColumn{
		columnDef:     columnDef,
		computeFn:     computeFn,
		length:        length,
		displayFormat: format,
	}
}

func (c *ComputedDatetimeColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ComputedDatetimeColumn) Length() int {
	return c.length
}

func (c *ComputedDatetimeColumn) GetValue(i uint32) (int64, error) {
	if i >= uint32(c.length) {
		return 0, fmt.Errorf("index %d out of bounds (length: %d)", i, c.length)
	}
	return c.computeFn(i)
}

func (c *ComputedDatetimeColumn) GetString(i uint32) (string, error) {
	nanos, err := c.GetValue(i)
	if err != nil {
		return "", err
	}
	if nanos == 0 {
		return "", nil
	}
	t := nanoToTime(nanos)
	return t.UTC().Format(c.displayFormat), nil
}

func (c *ComputedDatetimeColumn) IsKey() bool {
	return false
}

func (c *ComputedDatetimeColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *ComputedDatetimeColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[int64]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		value, err := c.GetValue(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		if groupKey, ok := valueToGroupKey[value]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[value] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}

// SetDisplayFormat changes the display format for GetString().
func (c *ComputedDatetimeColumn) SetDisplayFormat(format string) {
	c.displayFormat = format
}

// nanoToTime converts Unix nanoseconds to time.Time
func nanoToTime(nanos int64) time.Time {
	return time.Unix(0, nanos)
}

// ComputeDurationFn is a function that computes a time.Duration value for a given row index.
// The function should capture any source columns it needs in a closure.
type ComputeDurationFn func(i uint32) (time.Duration, error)

// ComputedDurationColumn represents a column whose duration values are computed from other columns.
type ComputedDurationColumn struct {
	columnDef     *ColumnDef
	computeFn     ComputeDurationFn
	length        int
	displayFormat DurationFormat
}

// NewComputedDurationColumn creates a new computed duration column.
// The length parameter specifies the number of rows (should match source columns).
func NewComputedDurationColumn(columnDef *ColumnDef, length int, computeFn ComputeDurationFn) *ComputedDurationColumn {
	return &ComputedDurationColumn{
		columnDef:     columnDef,
		computeFn:     computeFn,
		length:        length,
		displayFormat: DurationFormatCompact,
	}
}

// NewComputedDurationColumnWithFormat creates a new computed duration column with a custom display format.
func NewComputedDurationColumnWithFormat(columnDef *ColumnDef, length int, computeFn ComputeDurationFn, format DurationFormat) *ComputedDurationColumn {
	return &ComputedDurationColumn{
		columnDef:     columnDef,
		computeFn:     computeFn,
		length:        length,
		displayFormat: format,
	}
}

func (c *ComputedDurationColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ComputedDurationColumn) Length() int {
	return c.length
}

func (c *ComputedDurationColumn) GetValue(i uint32) (time.Duration, error) {
	if i >= uint32(c.length) {
		return 0, fmt.Errorf("index %d out of bounds (length: %d)", i, c.length)
	}
	return c.computeFn(i)
}

func (c *ComputedDurationColumn) GetString(i uint32) (string, error) {
	d, err := c.GetValue(i)
	if err != nil {
		return "", err
	}
	return formatDurationForDisplay(d, c.displayFormat), nil
}

func (c *ComputedDurationColumn) Nanoseconds(i uint32) (int64, error) {
	d, err := c.GetValue(i)
	if err != nil {
		return 0, err
	}
	return int64(d), nil
}

func (c *ComputedDurationColumn) IsKey() bool {
	return false
}

func (c *ComputedDurationColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *ComputedDurationColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[int64]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		value, err := c.GetValue(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		nanos := int64(value)
		if groupKey, ok := valueToGroupKey[nanos]; ok {
			groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
		} else {
			groupKey := uint32(len(valueToGroupKey))
			valueToGroupKey[nanos] = groupKey
			groupedIndices[groupKey] = []uint32{i}
		}
	}
	return groupedIndices, unmapped
}

// SetDisplayFormat changes the display format for GetString().
func (c *ComputedDurationColumn) SetDisplayFormat(format DurationFormat) {
	c.displayFormat = format
}

// formatDurationForDisplay formats a duration according to the specified format.
func formatDurationForDisplay(d time.Duration, format DurationFormat) string {
	switch format {
	case DurationFormatVerbose:
		return formatDurationVerboseComputed(d)
	default:
		return formatDurationCompactComputed(d)
	}
}

// formatDurationCompactComputed returns a compact representation like "2h30m" or "3d4h".
func formatDurationCompactComputed(d time.Duration) string {
	if d == 0 {
		return "0s"
	}

	negative := d < 0
	if negative {
		d = -d
	}

	var result string
	if negative {
		result = "-"
	}

	// Handle days specially (not in standard Go duration)
	days := d / (24 * time.Hour)
	d = d % (24 * time.Hour)

	if days > 0 {
		result += fmt.Sprintf("%dd", days)
	}

	// Use Go's standard formatting for the rest
	if d > 0 || days == 0 {
		remaining := d.String()
		// If we have days and the remaining is just "0s", skip it
		if days > 0 && remaining == "0s" {
			return result
		}
		result += remaining
	}

	return result
}

// formatDurationVerboseComputed returns a human-readable representation like "2 hours 30 minutes".
func formatDurationVerboseComputed(d time.Duration) string {
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
	if seconds > 0 || len(parts) == 0 {
		if seconds == 1 {
			parts = append(parts, "1 second")
		} else {
			parts = append(parts, fmt.Sprintf("%d seconds", seconds))
		}
	}

	result := ""
	for i, part := range parts {
		if i > 0 {
			result += " "
		}
		result += part
	}

	if negative {
		return "-" + result
	}
	return result
}

// ComputeBoolFn is a function that computes a bool value for a given row index.
// The function should capture any source columns it needs in a closure.
type ComputeBoolFn func(i uint32) (bool, error)

// ComputedBoolColumn represents a column whose bool values are computed from other columns.
type ComputedBoolColumn struct {
	columnDef *ColumnDef
	computeFn ComputeBoolFn
	length    int
}

// NewComputedBoolColumn creates a new computed bool column.
// The length parameter specifies the number of rows (should match source columns).
// The computeFn should be a closure that captures source columns and computes the value for each row.
func NewComputedBoolColumn(columnDef *ColumnDef, length int, computeFn ComputeBoolFn) *ComputedBoolColumn {
	return &ComputedBoolColumn{
		columnDef: columnDef,
		computeFn: computeFn,
		length:    length,
	}
}

func (c *ComputedBoolColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ComputedBoolColumn) Length() int {
	return c.length
}

func (c *ComputedBoolColumn) GetValue(i uint32) (bool, error) {
	if i >= uint32(c.length) {
		return false, fmt.Errorf("index %d out of bounds (length: %d)", i, c.length)
	}
	return c.computeFn(i)
}

func (c *ComputedBoolColumn) GetString(i uint32) (string, error) {
	value, err := c.GetValue(i)
	if err != nil {
		return "", err
	}
	if value {
		return "True", nil
	}
	return "False", nil
}

func (c *ComputedBoolColumn) GetIndex(value bool) (uint32, error) {
	return 0, fmt.Errorf("column %q is a computed column and doesn't support reverse lookups", c.columnDef.Name())
}

func (c *ComputedBoolColumn) IsKey() bool {
	return false
}

func (c *ComputedBoolColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return nil
}

func (c *ComputedBoolColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	var unmapped []uint32

	for _, i := range indices {
		value, err := c.GetValue(i)
		if err != nil {
			unmapped = append(unmapped, i)
			continue
		}

		// Use fixed group keys: 0 for false, 1 for true
		var groupKey uint32
		if value {
			groupKey = 1
		} else {
			groupKey = 0
		}
		groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
	}
	return groupedIndices, unmapped
}
