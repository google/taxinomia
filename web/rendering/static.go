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
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/google/safehtml"
	"github.com/google/safehtml/uncheckedconversions"
	"github.com/google/taxinomia/core/buildinfo"
	"github.com/google/taxinomia/web/viewmodel"
)

// The table page's stylesheet and script. They ship inside the binary and
// reach the browser either inlined in every page (the default) or as
// versioned static files it caches across navigations (UseStaticAssets).
//
//go:embed static/*
var staticFS embed.FS

// staticAssets holds the embedded files in both delivery forms.
type staticAssets struct {
	css, headJS, bodyJS string // inline parts
	js                  string // external form: head + body script in one deferred file
	version             string // path segment that changes with every build
	etag                string
}

func loadStaticAssets() (*staticAssets, error) {
	read := func(name string) (string, error) {
		b, err := staticFS.ReadFile("static/" + name)
		return string(b), err
	}
	css, err := read("table.css")
	if err != nil {
		return nil, err
	}
	head, err := read("table-head.js")
	if err != nil {
		return nil, err
	}
	body, err := read("table-body.js")
	if err != nil {
		return nil, err
	}
	a := &staticAssets{css: css, headJS: head, bodyJS: body, js: head + "\n" + body}
	sum := sha256.Sum256([]byte(a.css + a.js))
	a.etag = hex.EncodeToString(sum[:8])
	// The version segment of the URL: the build revision, or for unstamped
	// development builds the content hash, so a rebuilt binary never serves
	// a browser's stale cached copy.
	if b := buildinfo.Get(); b.Known() {
		a.version = strings.NewReplacer("+", "-", ".", "-").Replace(b.Version())
	} else {
		a.version = "dev-" + a.etag
	}
	return a, nil
}

// inline returns the assets embedded in the page.
func (a *staticAssets) inline() viewmodel.Assets {
	return viewmodel.Assets{
		InlineCSS:    uncheckedconversions.StyleSheetFromStringKnownToSatisfyTypeContract(a.css),
		InlineHeadJS: uncheckedconversions.ScriptFromStringKnownToSatisfyTypeContract(a.headJS),
		InlineBodyJS: uncheckedconversions.ScriptFromStringKnownToSatisfyTypeContract(a.bodyJS),
	}
}

// external returns the assets as versioned URLs under base.
func (a *staticAssets) external(base string) viewmodel.Assets {
	mk := func(file string) safehtml.TrustedResourceURL {
		u, err := safehtml.TrustedResourceURLFormatFromConstant("%{base}/%{version}/%{file}", map[string]string{"base": base, "version": a.version, "file": file})
		if err != nil {
			// base failed the format's checks; fall back to the root.
			u, _ = safehtml.TrustedResourceURLFormatFromConstant("/static/%{version}/%{file}", map[string]string{"version": a.version, "file": file})
		}
		return u
	}
	return viewmodel.Assets{External: true, CSS: mk("table.css"), JS: mk("table.js")}
}

// UseStaticAssets switches the table page to external, cacheable
// stylesheet and script files served under base (e.g. "/static") — the
// server must route that prefix to StaticHandler. Pages shrink from ~120 KB
// to the data they carry and the browser reuses its parsed script across
// navigations. An empty base restores the inline form.
func (r *TableRenderer) UseStaticAssets(base string) {
	r.staticBase = strings.TrimSuffix(base, "/")
}

// assetsFor fills the delivery form the renderer is configured for.
func (r *TableRenderer) assetsFor() viewmodel.Assets {
	if r.staticBase != "" {
		return r.assets.external(r.staticBase)
	}
	return r.assets.inline()
}

// StaticHandler serves the embedded stylesheet and script at
// <base>/<version>/table.css and <base>/<version>/table.js. Mount it at the
// base passed to UseStaticAssets. Requests carrying the current version
// segment are cached immutably for a year; any other segment (a stale page
// after a redeploy) is served with no-cache so the browser revalidates.
func (r *TableRenderer) StaticHandler() http.Handler {
	a := r.assets
	started := time.Now()
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		file := path.Base(req.URL.Path)
		version := path.Base(path.Dir(req.URL.Path))
		var body, ctype string
		switch file {
		case "table.css":
			body, ctype = a.css, "text/css; charset=utf-8"
		case "table.js":
			body, ctype = a.js, "text/javascript; charset=utf-8"
		default:
			http.NotFound(w, req)
			return
		}
		h := w.Header()
		h.Set("Content-Type", ctype)
		h.Set("ETag", `"`+a.etag+`"`)
		if version == a.version {
			h.Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			h.Set("Cache-Control", "no-cache")
		}
		if match := req.Header.Get("If-None-Match"); match != "" && strings.Contains(match, a.etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		h.Set("X-Taxinomia-Version", buildinfo.Get().Version())
		http.ServeContent(w, req, file, started, strings.NewReader(body))
	})
}

// AssetsVersion is the version segment of the static asset URLs (for
// tests and diagnostics).
func (r *TableRenderer) AssetsVersion() string { return r.assets.version }

var _ = fmt.Sprintf
