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
	_ "embed"
	"embed"
	"io"

	"github.com/google/safehtml/template"
	"github.com/google/taxinomia/core/views"
)

//go:embed templates/*
var templateFS embed.FS

// TableRenderer handles rendering of table view models to HTML
type TableRenderer struct {
	tableTemplate   *template.Template
	landingTemplate *template.Template
}

// NewTableRenderer creates a new table renderer
func NewTableRenderer() (*TableRenderer, error) {
	trustedFS := template.TrustedFSFromEmbed(templateFS)

	// Parse the table template
	tableTemplate, err := template.New("table.html").ParseFS(trustedFS, "templates/table.html")
	if err != nil {
		return nil, err
	}

	// Parse the landing page template
	landingTemplate, err := template.New("landing.html").ParseFS(trustedFS, "templates/landing.html")
	if err != nil {
		return nil, err
	}

	return &TableRenderer{
		tableTemplate:   tableTemplate,
		landingTemplate: landingTemplate,
	}, nil
}

// Render renders a TableViewModel to the provided writer
func (r *TableRenderer) Render(w io.Writer, vm views.TableViewModel) error {
	return r.tableTemplate.Execute(w, vm)
}

// RenderLanding renders a LandingViewModel to the provided writer
func (r *TableRenderer) RenderLanding(w io.Writer, vm views.LandingViewModel) error {
	return r.landingTemplate.Execute(w, vm)
}
