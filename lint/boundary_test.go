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

// Package lint holds source-level architecture checks that Bazel visibility
// cannot express. Bazel visibility keeps core targets from depending on
// //web/... targets; this test additionally keeps core sources free of
// safehtml (an external repo whose visibility we cannot restrict) and
// double-checks the web-import rule at the source level for plain `go build`
// users.
package lint

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Packages under core/ that are deliberately presentation code parked at
// their pre-split import paths: pure forwarders to the web/ tree, kept for
// import-path compatibility until phase CLEAN deletes them. They are the only
// core packages allowed to import web/ or safehtml.
var forwarderDirs = map[string]bool{
	"core/query":     true,
	"core/rendering": true,
	"core/server":    true,
	"core/views":     true,
}

// forbiddenPrefixes are import paths no non-forwarder core package may use,
// directly or otherwise. Checking direct imports of every core file is
// sufficient for transitive closure within the repo: a chain into web/ has to
// start with a direct core -> web or core -> safehtml import somewhere.
var forbiddenPrefixes = []string{
	"github.com/google/taxinomia/web/",
	"github.com/google/safehtml",
}

// TestCoreDoesNotImportPresentation walks core/**/*.go (excluding _test.go
// files and the forwarding packages) and fails if any file imports the web/
// tree or safehtml. Test files are exempt: cross-boundary tests (e.g. the
// engine URL-translation test) are legitimate, and shipped dependencies are
// what the boundary is about.
func TestCoreDoesNotImportPresentation(t *testing.T) {
	root := repoRoot(t)
	coreDir := filepath.Join(root, "core")

	fset := token.NewFileSet()
	err := filepath.WalkDir(coreDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		if forwarderDirs[filepath.ToSlash(filepath.Dir(relSlash))] {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			importPath, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			for _, prefix := range forbiddenPrefixes {
				if importPath == strings.TrimSuffix(prefix, "/") || strings.HasPrefix(importPath, prefix) {
					t.Errorf("%s imports %q: core packages must not depend on the web/ tree or safehtml", relSlash, importPath)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking core/: %v", err)
	}
}

// repoRoot finds the module root by walking up from the working directory
// until go.mod appears. Under `go test ./lint/` that is one level up. Under a
// Bazel sandbox the source tree is not present as a module; skip there — the
// Bazel side of the boundary is enforced by target visibility instead.
func repoRoot(t *testing.T) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "core")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skip("module root with core/ not found (Bazel sandbox); boundary is enforced by target visibility there")
		}
		dir = parent
	}
}
