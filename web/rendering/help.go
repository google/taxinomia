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

package rendering

import (
	"embed"
	"io"

	"github.com/google/safehtml/uncheckedconversions"
	"github.com/google/taxinomia/web/viewmodel"
)

// The embedded help texts. help/expressions.md is a copy of
// docs/expression_language.md (the reference other docs link to);
// TestHelpExpressionsMatchesDocs fails when the two differ.
//
//go:embed help/*.md
var helpFS embed.FS

// helpSections are the help page's sections, in order.
// The ids are fixed identifiers (letters only), safe as HTML ids.
var helpSections = []struct{ id, title, file string }{
	{"filtering", "Filtering", "help/filtering.md"},
	{"grouping", "Grouping and aggregates", "help/grouping.md"},
	{"expressions", "Computed columns: expressions", "help/expressions.md"},
}

// RenderHelp renders the embedded help page (filter syntax, grouping and
// aggregates, expression language). It shares the table page's stylesheet.
func (r *TableRenderer) RenderHelp(w io.Writer, vm viewmodel.HelpViewModel) error {
	vm.Assets = r.assetsFor()
	if vm.Title == "" {
		vm.Title = "Help: filtering, grouping, expressions"
	}
	for _, s := range helpSections {
		src, err := helpFS.ReadFile(s.file)
		if err != nil {
			return err
		}
		vm.Sections = append(vm.Sections, viewmodel.HelpSection{ID: uncheckedconversions.IdentifierFromStringKnownToSatisfyTypeContract(s.id), Title: s.title, Body: renderMarkdown(string(src))})
	}
	return r.helpTemplate.Execute(w, vm)
}
