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

package handlers

import (
	"syscall"
	"time"
	"unsafe"
)

var (
	kernel32                  = syscall.NewLazyDLL("kernel32.dll")
	procQueryPerformanceCount = kernel32.NewProc("QueryPerformanceCounter")
	procQueryPerformanceFreq  = kernel32.NewProc("QueryPerformanceFrequency")

	// qpcFreq is the counter's ticks per second (fixed at boot; 10 MHz on
	// current Windows). Zero if the counter is unavailable, in which case
	// the time package is used.
	qpcFreq = func() int64 {
		var f int64
		if r, _, _ := procQueryPerformanceFreq.Call(uintptr(unsafe.Pointer(&f))); r == 0 {
			return 0
		}
		return f
	}()
)

// hrNow returns the current high-resolution timestamp.
func hrNow() hrTime {
	if qpcFreq == 0 {
		return hrTime(time.Now().UnixNano())
	}
	var c int64
	procQueryPerformanceCount.Call(uintptr(unsafe.Pointer(&c)))
	return hrTime(c)
}

// hrSince returns the time elapsed since t.
func hrSince(t hrTime) time.Duration {
	if qpcFreq == 0 {
		return time.Duration(time.Now().UnixNano() - int64(t))
	}
	ticks := int64(hrNow() - t)
	// ticks * 1e9 / freq, without overflow for any realistic request time
	sec := ticks / qpcFreq
	rem := ticks % qpcFreq
	return time.Duration(sec)*time.Second + time.Duration(rem*int64(time.Second)/qpcFreq)
}
