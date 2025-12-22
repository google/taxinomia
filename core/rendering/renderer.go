package rendering

import (
	"embed"
	"io"

	"github.com/google/safehtml/template"
	"github.com/google/taxinomia/core/views"
)

//go:embed templates/*
var templateFS embed.FS

// TableRenderer handles rendering of table view models to HTML
type TableRenderer struct {
	tmpl *template.Template
}

// NewTableRenderer creates a new table renderer
func NewTableRenderer() (*TableRenderer, error) {
	trustedFS := template.TrustedFSFromEmbed(templateFS)
	// Use ParseFS to parse embedded templates with safehtml
	// The template name should match the file name for proper execution
	tmpl, err := template.ParseFS(trustedFS, "templates/table.html")
	if err != nil {
		return nil, err
	}

	return &TableRenderer{
		tmpl: tmpl,
	}, nil
}

// Render renders a TableViewModel to the provided writer
func (r *TableRenderer) Render(w io.Writer, vm views.TableViewModel) error {
	return r.tmpl.Execute(w, vm)
}
