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
	"time"

	"github.com/google/taxinomia/core/hrclock"
)

// GroupingStep is one timed step of the most recent grouping build:
// partitioning by the level-0 column, ranking or sorting level 0, each
// deeper level, each leaf column's aggregates (pre-aggregated partials or
// the per-row pass), and the release of membership. A build served from
// the cache records a single "cached" step. The perf tab lists them under
// the Grouping phase so a slow grouping explains itself.
type GroupingStep struct {
	Name     string
	Duration time.Duration
}

// LastGroupingSteps returns the timed steps of the last grouping call, in
// execution order. Empty when the view has never been grouped.
func (t *TableView) LastGroupingSteps() []GroupingStep {
	return append([]GroupingStep(nil), t.groupingSteps...)
}

// SetClock installs the clock the grouping steps are timed with (nil means
// hrclock.System()). The request handler passes its own so the sub-phase
// timings share the resolution of the phase timings around them.
func (t *TableView) SetClock(c hrclock.Clock) {
	t.clock = c
}

func (t *TableView) stepClock() hrclock.Clock {
	if t.clock == nil {
		return hrclock.System()
	}
	return t.clock
}

// stepStart marks the beginning of a step; pair with recordStep.
func (t *TableView) stepStart() hrclock.Stamp { return t.stepClock().Now() }

// recordStep appends a step that started at s.
func (t *TableView) recordStep(name string, s hrclock.Stamp) {
	t.groupingSteps = append(t.groupingSteps, GroupingStep{Name: name, Duration: t.stepClock().Since(s)})
}

// noteStep appends a step that took no measurable time (a cache hit).
func (t *TableView) noteStep(name string) {
	t.groupingSteps = append(t.groupingSteps, GroupingStep{Name: name})
}
