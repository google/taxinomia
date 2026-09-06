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

//go:build !windows

package handlers

import "time"

// hrBase anchors hrTime to the process start so values stay monotonic
// (time.Since uses the monotonic reading of hrBase).
var hrBase = time.Now()

// hrNow returns the current high-resolution timestamp.
func hrNow() hrTime { return hrTime(time.Since(hrBase)) }

// hrSince returns the time elapsed since t.
func hrSince(t hrTime) time.Duration { return time.Since(hrBase) - time.Duration(t) }
