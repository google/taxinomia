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

//go:build windows

package hrclock

import (
	"syscall"
	"time"
	"unsafe"
)

var (
	kernel32                  = syscall.NewLazyDLL("kernel32.dll")
	procQueryPerformanceCount = kernel32.NewProc("QueryPerformanceCounter")
	procQueryPerformanceFreq  = kernel32.NewProc("QueryPerformanceFrequency")
)

// systemClock is the performance counter when available, else TimeClock.
var systemClock Clock = func() Clock {
	var f int64
	if r, _, _ := procQueryPerformanceFreq.Call(uintptr(unsafe.Pointer(&f))); r == 0 || f <= 0 {
		return TimeClock{}
	}
	return qpcClock{freq: f}
}()

// qpcClock reads QueryPerformanceCounter (~100 ns resolution).
type qpcClock struct {
	freq int64 // ticks per second, fixed at boot (10 MHz on current Windows)
}

func (c qpcClock) Now() Stamp {
	var t int64
	procQueryPerformanceCount.Call(uintptr(unsafe.Pointer(&t)))
	return Stamp(t)
}

func (c qpcClock) Since(s Stamp) time.Duration {
	ticks := int64(c.Now() - s)
	// ticks * 1e9 / freq, split to avoid overflow for long spans
	sec := ticks / c.freq
	rem := ticks % c.freq
	return time.Duration(sec)*time.Second + time.Duration(rem*int64(time.Second)/c.freq)
}
