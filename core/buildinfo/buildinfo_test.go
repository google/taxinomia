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

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestUnknown(t *testing.T) {
	i := Info{Go: "go1.26.3", Platform: "linux/amd64"}
	if i.Known() || i.Stamped() {
		t.Fatal("empty revision must be neither known nor stamped")
	}
	if got := i.Display(); got != "dev" {
		t.Errorf("Display = %q, want dev", got)
	}
	if got := i.Version(); got != "dev" {
		t.Errorf("Version = %q, want dev", got)
	}
	if got := i.Long(); got != "Development build (no version recorded), go1.26.3 linux/amd64" {
		t.Errorf("Long = %q", got)
	}
}

func TestSourceRevisionOnly(t *testing.T) {
	i := Info{Revision: "168", Date: "2026-09-06", Go: "go1.26.3", Platform: "linux/amd64"}
	if !i.Known() || i.Stamped() {
		t.Fatal("source revision must be known but not stamped")
	}
	if got := i.Display(); got != "r168" {
		t.Errorf("Display = %q", got)
	}
	if got := i.Version(); got != "r168" {
		t.Errorf("Version = %q", got)
	}
	if got := i.Long(); got != "Revision 168, 2026-09-06 (source revision; not link-time stamped), go1.26.3 linux/amd64" {
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
	if got := i.Long(); got != "Revision 167, 2026-09-02, commit 1b25cf2ae910ed23728ca4072c8dcac4095b98e2, go1.26.3 windows/amd64" {
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

func TestGetLayersStampOverSource(t *testing.T) {
	// Under go test nothing is linker-set: Get reports the source revision.
	i := Get()
	if i.Go == "" || i.Platform == "" {
		t.Errorf("runtime fields missing: %+v", i)
	}
	if i.Revision != sourceRevision || i.Date != sourceDate {
		t.Errorf("unstamped Get = %+v, want source revision %s/%s", i, sourceRevision, sourceDate)
	}
	if i.Stamped() {
		t.Errorf("unstamped Get reports Stamped: %+v", i)
	}

	// Simulate the linker values: the stamp overrides the source revision.
	defer func(c, r, d, dt string) { commit, revision, date, dirty = c, r, d, dt }(commit, revision, date, dirty)
	commit, revision, date, dirty = "abcdef0123456789", "999", "2030-01-01", "1"
	i = Get()
	if !i.Stamped() || i.Revision != "999" || i.Date != "2030-01-01" || !i.Dirty {
		t.Errorf("stamped Get = %+v", i)
	}
	// A stamp without count/date (a client stamping only the hash) keeps
	// the source revision.
	revision, date = "", ""
	i = Get()
	if !i.Stamped() || i.Revision != sourceRevision || i.Date != sourceDate {
		t.Errorf("hash-only stamp Get = %+v", i)
	}
}

// TestSourceRevisionMatchesGit catches a commit made without the
// pre-commit hook: version.go must describe HEAD. It runs only from a git
// checkout (plain `go test`); under Bazel the test has no repository.
func TestSourceRevisionMatchesGit(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("no caller info")
	}
	dir := filepath.Dir(file)
	if _, err := os.Stat(filepath.Join(dir, "version.go")); err != nil {
		t.Skip("not running from the source tree")
	}
	git := func(args ...string) (string, bool) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		out, err := cmd.Output()
		if err != nil {
			return "", false
		}
		return strings.TrimSpace(string(out)), true
	}
	if _, ok := git("rev-parse", "--is-inside-work-tree"); !ok {
		t.Skip("no git repository")
	}
	count, ok := git("rev-list", "--count", "HEAD")
	if !ok {
		t.Skip("no commits")
	}
	// Staged for the commit in progress (the hook just wrote count+1) is
	// also acceptable.
	staged, _ := git("diff", "--cached", "--name-only", "--", "version.go")
	if staged != "" {
		t.Skip("version.go is staged (commit in progress)")
	}
	if sourceRevision != count {
		t.Errorf("version.go records revision %s but git HEAD has %s commits; enable the hook (git config core.hooksPath tools/hooks) and re-commit", sourceRevision, count)
	}
}
