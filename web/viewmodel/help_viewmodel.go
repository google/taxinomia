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

package viewmodel

import (
	"github.com/google/safehtml"
	"github.com/google/taxinomia/core/buildinfo"
)

// HelpViewModel is the embedded help page: the filter syntax, grouping and
// aggregates, and the expression language of computed columns. The
// renderer fills Sections from its embedded help texts.
type HelpViewModel struct {
	Title    string
	Sections []HelpSection
	Assets   Assets         // stylesheet (shared with the table page)
	Build    buildinfo.Info // build stamp for the footer
}

// HelpSection is one part of the help page.
type HelpSection struct {
	ID    safehtml.Identifier // anchor: "filtering", "grouping", "expressions"
	Title string              // for the table of contents
	Body  safehtml.HTML       // rendered from the section's help text
}
