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
	"time"
)

// ChunkedDatetimeColumn is DatetimeColumn with chunked storage. Values are
// normalized to UTC on append, exactly as the plain column does; the display
// format and location apply only at GetString time, so they can be changed
// after finalize without touching the stored data. Grouping, uniqueness and
// reverse lookup key on Unix nanoseconds, mirroring DatetimeColumn.
type ChunkedDatetimeColumn struct {
	chunkedColumn[time.Time, int64]
	displayFormat string
	location      *time.Location
}

// NewChunkedDatetimeColumn creates an empty chunked datetime column with the
// default display format.
func NewChunkedDatetimeColumn(columnDef *ColumnDef) *ChunkedDatetimeColumn {
	return newChunkedDatetimeColumn(columnDef, DefaultChunkSize)
}

// NewChunkedDatetimeColumnWithFormat creates an empty chunked datetime column
// with a custom display format and location.
func NewChunkedDatetimeColumnWithFormat(columnDef *ColumnDef, format string, loc *time.Location) *ChunkedDatetimeColumn {
	c := newChunkedDatetimeColumn(columnDef, DefaultChunkSize)
	c.displayFormat = format
	if loc != nil {
		c.location = loc
	}
	return c
}

func newChunkedDatetimeColumn(columnDef *ColumnDef, chunkSize int) *ChunkedDatetimeColumn {
	c := &ChunkedDatetimeColumn{
		displayFormat: DatetimeFormatDateTime,
		location:      time.UTC,
	}
	c.chunkedColumn = newChunkedColumn[time.Time, int64](
		columnDef, chunkSize, timeGroupKey, c.formatDatetime, compareTimes, nil, true)
	return c
}

// timeGroupKey is the canonical grouping/uniqueness key: Unix nanoseconds,
// the same key DatetimeColumn uses. It is location-independent.
func timeGroupKey(t time.Time) int64 { return t.UnixNano() }

// formatDatetime mirrors DatetimeColumn.GetString: the zero time renders
// empty, everything else in the column's location and display format.
func (c *ChunkedDatetimeColumn) formatDatetime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.In(c.location).Format(c.displayFormat)
}

// Append adds a time.Time value, normalized to UTC.
func (c *ChunkedDatetimeColumn) Append(value time.Time) {
	c.chunkedColumn.Append(value.UTC())
}

// AppendUnix adds a value from Unix seconds.
func (c *ChunkedDatetimeColumn) AppendUnix(seconds int64) {
	c.chunkedColumn.Append(time.Unix(seconds, 0).UTC())
}

// AppendUnixNano adds a value from Unix nanoseconds.
func (c *ChunkedDatetimeColumn) AppendUnixNano(nanos int64) {
	c.chunkedColumn.Append(time.Unix(0, nanos).UTC())
}

// AppendString parses a string and appends the datetime value.
// Returns an error if parsing fails.
func (c *ChunkedDatetimeColumn) AppendString(s string) error {
	t, err := ParseDatetime(s, c.location)
	if err != nil {
		return err
	}
	c.chunkedColumn.Append(t.UTC())
	return nil
}

// SetDisplayFormat changes the display format for GetString().
func (c *ChunkedDatetimeColumn) SetDisplayFormat(format string) {
	c.displayFormat = format
}

// SetLocation changes the timezone for display.
func (c *ChunkedDatetimeColumn) SetLocation(loc *time.Location) {
	if loc != nil {
		c.location = loc
	}
}

// CreateJoinedColumn creates a joined column for this datetime column.
func (c *ChunkedDatetimeColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedDatetimeColumn(columnDef, joiner, c)
}

// --- Epoch-based extraction, mirroring DatetimeColumn ---

func (c *ChunkedDatetimeColumn) timeAt(i uint32) (time.Time, error) {
	if i >= uint32(c.data.len()) {
		return time.Time{}, fmt.Errorf("index %d out of bounds", i)
	}
	return c.data.at(i), nil
}

// Seconds returns Unix seconds for the value at index i.
func (c *ChunkedDatetimeColumn) Seconds(i uint32) (int64, error) {
	t, err := c.timeAt(i)
	if err != nil {
		return 0, err
	}
	return t.Unix(), nil
}

// Minutes returns minutes since Unix epoch for the value at index i.
func (c *ChunkedDatetimeColumn) Minutes(i uint32) (int64, error) {
	t, err := c.timeAt(i)
	if err != nil {
		return 0, err
	}
	return t.Unix() / 60, nil
}

// Hours returns hours since Unix epoch for the value at index i.
func (c *ChunkedDatetimeColumn) Hours(i uint32) (int64, error) {
	t, err := c.timeAt(i)
	if err != nil {
		return 0, err
	}
	return t.Unix() / 3600, nil
}

// Days returns days since Unix epoch for the value at index i.
func (c *ChunkedDatetimeColumn) Days(i uint32) (int64, error) {
	t, err := c.timeAt(i)
	if err != nil {
		return 0, err
	}
	return t.Unix() / 86400, nil
}

// Weeks returns weeks since Unix epoch for the value at index i.
func (c *ChunkedDatetimeColumn) Weeks(i uint32) (int64, error) {
	t, err := c.timeAt(i)
	if err != nil {
		return 0, err
	}
	return t.Unix() / (86400 * 7), nil
}

// Months returns exact months since Unix epoch (Jan 1970) for the value at index i.
func (c *ChunkedDatetimeColumn) Months(i uint32) (int64, error) {
	t, err := c.timeAt(i)
	if err != nil {
		return 0, err
	}
	u := t.UTC()
	return int64(u.Year()-1970)*12 + int64(u.Month()-1), nil
}

// Quarters returns exact quarters since Unix epoch (Q1 1970) for the value at index i.
func (c *ChunkedDatetimeColumn) Quarters(i uint32) (int64, error) {
	t, err := c.timeAt(i)
	if err != nil {
		return 0, err
	}
	u := t.UTC()
	return int64(u.Year()-1970)*4 + int64(u.Month()-1)/3, nil
}

// Years returns years since Unix epoch (1970) for the value at index i.
func (c *ChunkedDatetimeColumn) Years(i uint32) (int64, error) {
	t, err := c.timeAt(i)
	if err != nil {
		return 0, err
	}
	return int64(t.UTC().Year() - 1970), nil
}

// Compile-time checks: the full column surface plus chunk access.
var (
	_ IDataColumn             = (*ChunkedDatetimeColumn)(nil)
	_ IDataColumnT[time.Time] = (*ChunkedDatetimeColumn)(nil)
	_ IGroupOps               = (*ChunkedDatetimeColumn)(nil)
	_ IChunkedColumn          = (*ChunkedDatetimeColumn)(nil)
)
