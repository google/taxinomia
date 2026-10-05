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

import (
	"io"
	"log"

	"github.com/google/taxinomia/core/buildinfo"
	"github.com/google/taxinomia/web/viewmodel"
)

// The embedded help page (filter syntax, grouping and aggregates, the
// expression language) is served on the table route, like the entity
// page, so an app needs no new route: <table path>?help=syntax, with
// #filtering, #grouping or #expressions to jump to a part.

// handleHelpRequest renders the embedded help page.
func (s *Server) handleHelpRequest(w io.Writer, setHeader func(key, value string)) *TableHandlerResult {
	vm := viewmodel.HelpViewModel{Build: buildinfo.Get()}
	setHeader("Content-Type", "text/html; charset=utf-8")
	setHeader(versionHeader, vm.Build.Version())
	if err := s.renderer.RenderHelp(w, vm); err != nil {
		log.Printf("Help page rendering error: %v", err)
		return &TableHandlerResult{Error: err}
	}
	return nil
}
