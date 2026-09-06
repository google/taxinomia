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

// hrTime is a high-resolution monotonic timestamp for the perf breakdown.
//
// time.Now's monotonic reading advances in ~0.5 ms steps on Windows (it is
// the kernel interrupt-time clock), which rounds every sub-millisecond
// request phase to "0.00ms" in the perf tab — on a warm cache that is
// every phase. hrNow/hrSince read the platform's finest monotonic counter
// instead (QueryPerformanceCounter on Windows, ~100 ns); elsewhere they
// are time.Now/time.Since, which are already fine-grained.
type hrTime int64
