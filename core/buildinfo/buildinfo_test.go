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

package buildinfo

import "testing"

func TestUnstamped(t *testing.T) {
	i := Info{Go: "go1.26.3", Platform: "linux/amd64"}
	if i.Stamped() {
		t.Fatal("empty commit must not count as stamped")
	}
	if got := i.Display(); got != "dev" {
		t.Errorf("Display = %q, want dev", got)
	}
	if got := i.Version(); got != "dev" {
		t.Errorf("Version = %q, want dev", got)
	}
	if got := i.Long(); got != "Development build (unstamped), go1.26.3 linux/amd64" {
		t.Errorf("Long = %q", got)
	}
}

func TestStamped(t *testing.T) {
	i := Info{Commit: "1b25cf2ae910ed23728ca4072c8dcac4095b98e2", Revision: "167", Date: "2026-09-02", Go: "go1.26.3", Platform: "windows/amd64"}
	if got := i.Display(); got != "r167 · 1b25cf2" {
		t.Errorf("Display = %q", got)
	}
	if got := i.Version(); got != "r167+1b25cf2" {
		t.Errorf("Version = %q", got)
	}
	if got := i.Long(); got != "Revision 167, commit 1b25cf2ae910ed23728ca4072c8dcac4095b98e2, 2026-09-02, go1.26.3 windows/amd64" {
		t.Errorf("Long = %q", got)
	}
	i.Dirty = true
	if got := i.Display(); got != "r167 · 1b25cf2 +dirty" {
		t.Errorf("dirty Display = %q", got)
	}
	if got := i.Version(); got != "r167+1b25cf2.dirty" {
		t.Errorf("dirty Version = %q", got)
	}
}

func TestGetUsesLinkerValues(t *testing.T) {
	// Unstamped under go test; the runtime fields are always filled.
	i := Get()
	if i.Go == "" || i.Platform == "" {
		t.Errorf("runtime fields missing: %+v", i)
	}
	if i.Stamped() != (commit != "") {
		t.Errorf("Stamped() = %v with commit %q", i.Stamped(), commit)
	}
}
