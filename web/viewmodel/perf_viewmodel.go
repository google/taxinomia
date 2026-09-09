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

package viewmodel

import (
	"fmt"
	"time"

	"github.com/google/safehtml"
)

// TimingEntry is one row of the Performance tab: a request phase, or (Sub)
// a step within the phase above it. Beyond the duration it carries what
// the step processed and which query setting caused it, so a slow request
// explains itself and offers the switch that turns the cost off.
type TimingEntry struct {
	Operation  string        // Name of the operation (e.g., "Parse Query", "Apply Filters")
	DurationMs string        // Duration in milliseconds (formatted)
	Duration   time.Duration // The measured duration (DurationMs is its display form)
	Sub        bool          // A step within the preceding phase (rendered indented; not a phase of its own)

	// Volume: rows the step processed and the derived unit costs. Empty
	// when the step is not per-row (a sort over groups, a cache hit).
	Rows   string // e.g. "1'999'778 rows"
	PerRow string // e.g. "24 ns/row"
	Rate   string // e.g. "41 M rows/s"

	// Settings that caused the step, each with the link that switches it
	// off (when one exists).
	Settings []SettingLink
}

// SettingLink names a query setting behind a cost and, when HasURL, the
// URL that disables it.
type SettingLink struct {
	Text   string       // e.g. "grouped by stage", "Σ on amount"
	Title  string       // tooltip, e.g. "ungroup stage"
	URL    safehtml.URL // the disabling navigation
	HasURL bool
}

// WithVolume fills Rows/PerRow/Rate for a step that processed rows rows in
// d. rows <= 0 leaves them empty.
func (e TimingEntry) WithVolume(d time.Duration, rows int) TimingEntry {
	if rows <= 0 {
		return e
	}
	e.Rows = FormatCount(rows) + " rows"
	ns := float64(d.Nanoseconds()) / float64(rows)
	switch {
	case ns >= 1000:
		e.PerRow = fmt.Sprintf("%.1f µs/row", ns/1000)
	case ns >= 10:
		e.PerRow = fmt.Sprintf("%.0f ns/row", ns)
	default:
		e.PerRow = fmt.Sprintf("%.1f ns/row", ns)
	}
	if d > 0 {
		perSec := float64(rows) / d.Seconds()
		switch {
		case perSec >= 1e9:
			e.Rate = fmt.Sprintf("%.1f G rows/s", perSec/1e9)
		case perSec >= 1e6:
			e.Rate = fmt.Sprintf("%.0f M rows/s", perSec/1e6)
		case perSec >= 1e3:
			e.Rate = fmt.Sprintf("%.0f k rows/s", perSec/1e3)
		default:
			e.Rate = fmt.Sprintf("%.0f rows/s", perSec)
		}
	}
	return e
}

// PerfData is the "Data" section of the Performance tab: the sizes every
// cost on the page is proportional to.
type PerfData struct {
	TableRows    string      // rows in the table
	Columns      string      // columns in the table
	FilteredRows string      // rows after filters (equals TableRows without filters)
	Selectivity  string      // e.g. "20.0%" ("" without filters)
	Levels       []PerfLevel // one per grouping level
	Displayed    string      // rows (or group rows) rendered
	VisibleCols  string      // visible columns
}

// PerfLevel is one grouping level in PerfData.
type PerfLevel struct {
	Column string
	Groups string
}
