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

package handlers

import (
	"testing"
	"time"
)

// TestHRClockResolution asserts the perf clock resolves well below the
// ~0.5 ms step of the interrupt-time clock that made every warm-cache
// phase read "0.00ms" on Windows.
func TestHRClockResolution(t *testing.T) {
	minStep := time.Hour
	for i := 0; i < 100000; i++ {
		a := hrNow()
		d := hrSince(a)
		if d > 0 && d < minStep {
			minStep = d
		}
	}
	if minStep > 50*time.Microsecond {
		t.Errorf("smallest measurable step is %v; want ≤ 50µs", minStep)
	}
}

func TestHRClockAgreesWithTime(t *testing.T) {
	start := hrNow()
	wall := time.Now()
	time.Sleep(20 * time.Millisecond)
	got, want := hrSince(start), time.Since(wall)
	if diff := got - want; diff < -5*time.Millisecond || diff > 5*time.Millisecond {
		t.Errorf("hrSince = %v, time.Since = %v", got, want)
	}
}

func TestFormatMs(t *testing.T) {
	cases := map[time.Duration]string{
		0:                      "0.00",
		7 * time.Microsecond:   "0.01",
		512 * time.Microsecond: "0.51",
		27*time.Millisecond + 960*time.Microsecond: "27.96",
	}
	for d, want := range cases {
		if got := formatMs(d); got != want {
			t.Errorf("formatMs(%v) = %q, want %q", d, got, want)
		}
	}
}
