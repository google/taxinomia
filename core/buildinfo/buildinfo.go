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

// Package buildinfo exposes the version of the running binary.
//
// The version is derived from git at link time, never maintained by hand:
// the //:taxinomia go_binary stamps the package-level variables below via
// x_defs from the workspace status keys emitted by tools/workspace_status.*
// (see .bazelrc, `--stamp`). The display version is the commit count on the
// built revision, so it goes up by exactly one per commit, plus the short
// hash to make it unambiguous across branches. An unstamped build (tests,
// `go build`) reports "dev".
package buildinfo

import (
	"fmt"
	"runtime"
	"strings"
)

// Linker-set values (see package comment). Empty when unstamped.
var (
	commit   string // full git hash
	revision string // commit count on HEAD
	date     string // commit date, YYYY-MM-DD
	dirty    string // "1" when the tree had uncommitted tracked changes
)

// Info describes the build that produced the running binary.
type Info struct {
	Commit   string // full git hash; empty when unstamped
	Revision string // commit count on the built revision; empty when unstamped
	Date     string // commit date (YYYY-MM-DD); empty when unstamped
	Dirty    bool   // tracked files were modified at build time
	Go       string // Go toolchain, e.g. go1.26.3
	Platform string // GOOS/GOARCH
}

// Get returns the build information of the running binary.
func Get() Info {
	return Info{
		Commit:   commit,
		Revision: revision,
		Date:     date,
		Dirty:    dirty == "1",
		Go:       strings.Fields(runtime.Version())[0], // drop "X:<experiments>" suffixes
		Platform: runtime.GOOS + "/" + runtime.GOARCH,
	}
}

// Stamped reports whether the binary carries git-derived version data.
func (i Info) Stamped() bool { return i.Commit != "" }

// ShortCommit is the abbreviated commit hash.
func (i Info) ShortCommit() string {
	if len(i.Commit) > 7 {
		return i.Commit[:7]
	}
	return i.Commit
}

// Display is the version as shown to users: "r168 · 1b25cf2", with a
// "+dirty" suffix for builds from a modified tree, or "dev" when unstamped.
func (i Info) Display() string {
	if !i.Stamped() {
		return "dev"
	}
	s := "r" + i.Revision + " · " + i.ShortCommit()
	if i.Dirty {
		s += " +dirty"
	}
	return s
}

// Version is the ASCII form for headers and logs: "r168+1b25cf2",
// "r168+1b25cf2.dirty", or "dev".
func (i Info) Version() string {
	if !i.Stamped() {
		return "dev"
	}
	s := "r" + i.Revision + "+" + i.ShortCommit()
	if i.Dirty {
		s += ".dirty"
	}
	return s
}

// Long is a one-line description for tooltips.
func (i Info) Long() string {
	if !i.Stamped() {
		return fmt.Sprintf("Development build (unstamped), %s %s", i.Go, i.Platform)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Revision %s, commit %s, %s", i.Revision, i.Commit, i.Date)
	if i.Dirty {
		b.WriteString(", built with uncommitted changes")
	}
	fmt.Fprintf(&b, ", %s %s", i.Go, i.Platform)
	return b.String()
}
