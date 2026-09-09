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

import "github.com/google/safehtml"

// Assets is how the table page gets its stylesheet and script: inlined in
// the page (the default, self-contained) or as external files the browser
// caches across navigations (External, set up by the renderer when the
// server mounts its static handler). The renderer fills this in; callers
// building their own view model leave it empty and get the inline form.
type Assets struct {
	External bool
	// External mode: versioned URLs of the stylesheet and the script.
	CSS safehtml.TrustedResourceURL
	JS  safehtml.TrustedResourceURL
	// Inline mode: the contents, embedded in the page.
	InlineCSS    safehtml.StyleSheet
	InlineHeadJS safehtml.Script
	InlineBodyJS safehtml.Script
}
