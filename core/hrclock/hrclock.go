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

// Package hrclock is the monotonic clock behind the request perf breakdown.
//
// The handler times each request phase with a Clock. The default,
// System(), is the finest monotonic counter the platform offers:
// QueryPerformanceCounter on Windows (~100 ns), where the time package's
// monotonic reading advances in ~0.5 ms steps and would round every
// sub-millisecond phase to zero; time.Now's monotonic reading elsewhere,
// where it is already nanosecond-grained.
//
// Servers embedding taxinomia can install their own implementation with
// handlers.Server.SetClock — a different counter, a clock with injected
// time for tests, or one that also records into their own tracing.
package hrclock

import "time"

// Stamp is a point in time as read from a Clock. It is opaque: only the
// Clock that produced it can interpret it (ticks for a counter clock,
// nanoseconds for a time-based one).
type Stamp int64

// Clock is a monotonic clock for measuring elapsed time.
type Clock interface {
	// Now returns the current instant.
	Now() Stamp
	// Since returns the time elapsed since s, which must come from the
	// same Clock.
	Since(s Stamp) time.Duration
}

// TimeClock is a Clock over the time package's monotonic reading. It is
// the default on every platform except Windows, and a fine choice there
// too when sub-millisecond phases do not matter.
type TimeClock struct{}

// base anchors stamps to the process start so they stay monotonic
// (time.Since uses the monotonic reading of base).
var base = time.Now()

// Now implements Clock.
func (TimeClock) Now() Stamp { return Stamp(time.Since(base)) }

// Since implements Clock.
func (TimeClock) Since(s Stamp) time.Duration { return time.Since(base) - time.Duration(s) }

// System returns the platform's default high-resolution clock.
func System() Clock { return systemClock }
