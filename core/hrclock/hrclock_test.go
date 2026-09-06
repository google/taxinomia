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

package hrclock

import (
	"testing"
	"time"
)

// TestSystemResolution asserts the default clock resolves well below the
// ~0.5 ms step of Windows' interrupt-time clock, which made every
// warm-cache phase read "0.00ms" in the perf tab.
func TestSystemResolution(t *testing.T) {
	c := System()
	minStep := time.Hour
	for i := 0; i < 100000; i++ {
		s := c.Now()
		if d := c.Since(s); d > 0 && d < minStep {
			minStep = d
		}
	}
	if minStep > 50*time.Microsecond {
		t.Errorf("smallest measurable step is %v; want ≤ 50µs", minStep)
	}
}

func TestClocksAgreeWithTime(t *testing.T) {
	for name, c := range map[string]Clock{"System": System(), "TimeClock": TimeClock{}} {
		start := c.Now()
		wall := time.Now()
		time.Sleep(20 * time.Millisecond)
		got, want := c.Since(start), time.Since(wall)
		if diff := got - want; diff < -5*time.Millisecond || diff > 5*time.Millisecond {
			t.Errorf("%s: Since = %v, time.Since = %v", name, got, want)
		}
	}
}

// fakeClock shows the seam a client implementation uses: stamps are
// whatever the implementation wants them to be.
type fakeClock struct{ now int64 }

func (f *fakeClock) Now() Stamp                  { return Stamp(f.now) }
func (f *fakeClock) Since(s Stamp) time.Duration { return time.Duration(f.now - int64(s)) }

func TestClientClock(t *testing.T) {
	var c Clock = &fakeClock{now: 100}
	s := c.Now()
	c.(*fakeClock).now = 100 + int64(3*time.Millisecond)
	if got := c.Since(s); got != 3*time.Millisecond {
		t.Errorf("Since = %v, want 3ms", got)
	}
}
