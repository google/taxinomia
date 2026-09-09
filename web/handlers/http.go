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
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/google/taxinomia/web/viewmodel"
)

// UseStaticAssets serves the table page's stylesheet and script as
// versioned, immutably cached files under base (e.g. "/static") instead of
// inlining ~117 KB of them into every page. Route base to StaticHandler.
// The default keeps them inline, so servers that do not mount the handler
// are unaffected.
func (s *Server) UseStaticAssets(base string) {
	s.renderer.UseStaticAssets(base)
}

// StaticHandler serves the files UseStaticAssets refers to.
func (s *Server) StaticHandler() http.Handler {
	return s.renderer.StaticHandler()
}

// ServerTimingHeader renders the collector's phases as a Server-Timing
// header value, so browser devtools show the server's breakdown next to
// the network timing: `parse-query;dur=0.03;desc="Parse Query", …`.
func (tc *TimingCollector) ServerTimingHeader() string {
	var parts []string
	for _, e := range tc.entries {
		if e.Sub {
			continue
		}
		name := strings.ToLower(strings.ReplaceAll(e.Operation, " ", "-"))
		parts = append(parts, fmt.Sprintf(`%s;dur=%.2f;desc="%s"`, name, float64(e.Duration.Microseconds())/1000, e.Operation))
	}
	parts = append(parts, fmt.Sprintf("total;dur=%s", tc.TotalMs()))
	return strings.Join(parts, ", ")
}

var gzipPool = sync.Pool{New: func() any {
	w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
	return w
}}

// GzipHandler compresses responses for clients that accept gzip. Pages
// compress 6–19x (a 25-row table page: 122 KB → 18 KB), which is the
// difference between ~100 ms and ~15 ms of transfer on a 10 Mbit/s link.
// Wrap the whole mux with it; responses that already carry a
// Content-Encoding are passed through.
func GzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		gw := &gzipResponseWriter{ResponseWriter: w}
		defer gw.Close()
		next.ServeHTTP(gw, r)
	})
}

// gzipResponseWriter starts compressing on the first write, unless the
// handler set its own Content-Encoding or a status without a body.
type gzipResponseWriter struct {
	http.ResponseWriter
	zw          *gzip.Writer
	wroteHeader bool
	passthrough bool
}

func (g *gzipResponseWriter) WriteHeader(code int) {
	if g.wroteHeader {
		return
	}
	g.wroteHeader = true
	h := g.Header()
	if h.Get("Content-Encoding") != "" || code == http.StatusNotModified || code == http.StatusNoContent {
		g.passthrough = true
	} else {
		h.Set("Content-Encoding", "gzip")
		h.Add("Vary", "Accept-Encoding")
		h.Del("Content-Length")
		g.zw = gzipPool.Get().(*gzip.Writer)
		g.zw.Reset(g.ResponseWriter)
	}
	g.ResponseWriter.WriteHeader(code)
}

func (g *gzipResponseWriter) Write(b []byte) (int, error) {
	if !g.wroteHeader {
		g.WriteHeader(http.StatusOK)
	}
	if g.passthrough {
		return g.ResponseWriter.Write(b)
	}
	return g.zw.Write(b)
}

// Flush lets handlers stream partial output through the compressor.
func (g *gzipResponseWriter) Flush() {
	if g.zw != nil {
		g.zw.Flush()
	}
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (g *gzipResponseWriter) Close() {
	if g.zw != nil {
		g.zw.Close()
		gzipPool.Put(g.zw)
		g.zw = nil
	}
}

var _ = viewmodel.TimingEntry{}
