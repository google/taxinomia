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
// The version is the commit count of the source revision, so it goes up by
// exactly one per commit, and it is never maintained by hand. Two sources
// feed it, layered:
//
//  1. version.go, rewritten by the tracked pre-commit hook
//     (tools/hooks/pre-commit) with the count and date of the commit being
//     made. It travels inside the library source, so every importer gets a
//     version with no build-system work.
//  2. The link-time git stamp: the //:taxinomia go_binary sets the
//     package-level variables below via x_defs from the workspace status
//     keys emitted by tools/workspace_status.* (see .bazelrc, `--stamp`).
//     When present it overrides 1 and adds the commit hash and the dirty
//     flag. Importers can set the same variables with -ldflags / x_defs.
//
// With neither source (no hook ever ran, no stamp) the version is "dev".
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
	Revision string // commit count of the source revision; empty only for "dev"
	Date     string // commit date (YYYY-MM-DD) of that revision; empty only for "dev"
	Commit   string // full git hash; empty unless link-time stamped
	Dirty    bool   // tracked files were modified at build time (stamped builds only)
	Go       string // Go toolchain, e.g. go1.26.3
	Platform string // GOOS/GOARCH
}

// Get returns the build information of the running binary.
func Get() Info {
	i := Info{
		Revision: sourceRevision,
		Date:     sourceDate,
		Go:       strings.Fields(runtime.Version())[0], // drop "X:<experiments>" suffixes
		Platform: runtime.GOOS + "/" + runtime.GOARCH,
	}
	if commit != "" {
		i.Commit = commit
		i.Dirty = dirty == "1"
		if revision != "" {
			i.Revision = revision
		}
		if date != "" {
			i.Date = date
		}
	}
	return i
}

// Known reports whether any version source was available.
func (i Info) Known() bool { return i.Revision != "" }

// Stamped reports whether the binary carries the link-time git stamp
// (commit hash and dirty flag), as opposed to only the source revision.
func (i Info) Stamped() bool { return i.Commit != "" }

// ShortCommit is the abbreviated commit hash.
func (i Info) ShortCommit() string {
	if len(i.Commit) > 7 {
		return i.Commit[:7]
	}
	return i.Commit
}

// Display is the version as shown to users: "r168 · 1b25cf2" when
// stamped (with a "+dirty" suffix for builds from a modified tree), "r168"
// from the source revision alone, or "dev".
func (i Info) Display() string {
	if !i.Known() {
		return "dev"
	}
	s := "r" + i.Revision
	if i.Stamped() {
		s += " · " + i.ShortCommit()
		if i.Dirty {
			s += " +dirty"
		}
	}
	return s
}

// Version is the ASCII form for headers and logs: "r168+1b25cf2",
// "r168+1b25cf2.dirty", "r168", or "dev".
func (i Info) Version() string {
	if !i.Known() {
		return "dev"
	}
	s := "r" + i.Revision
	if i.Stamped() {
		s += "+" + i.ShortCommit()
		if i.Dirty {
			s += ".dirty"
		}
	}
	return s
}

// Long is a one-line description for tooltips.
func (i Info) Long() string {
	if !i.Known() {
		return fmt.Sprintf("Development build (no version recorded), %s %s", i.Go, i.Platform)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Revision %s, %s", i.Revision, i.Date)
	if i.Stamped() {
		fmt.Fprintf(&b, ", commit %s", i.Commit)
		if i.Dirty {
			b.WriteString(", built with uncommitted changes")
		}
	} else {
		b.WriteString(" (source revision; not link-time stamped)")
	}
	fmt.Fprintf(&b, ", %s %s", i.Go, i.Platform)
	return b.String()
}
