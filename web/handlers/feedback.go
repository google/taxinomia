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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
)

// Feedback: a "Feedback" button next to "? Help" lets users report a bug or
// ask for a feature. Taxinomia provides the button, the form and the
// decoding; what happens to a report (a ticket, an email, a chat message,
// a log line) is up to the application. The button appears only once the
// application sets the address reports are sent to:
//
//	srv.SetFeedbackURL("/feedback")
//	mux.Handle("/feedback", handlers.FeedbackHandler(func(ctx context.Context, r handlers.FeedbackReport) error {
//	    return fileTicket(ctx, r) // the application's own back end
//	}))

// FeedbackReport is what the feedback form sends.
type FeedbackReport struct {
	Kind  string `json:"kind"`  // "bug" or "feature"
	Text  string `json:"text"`  // what the user wrote
	Page  string `json:"page"`  // the page's address when the form was sent (empty if the user unticked it)
	Build string `json:"build"` // the build the page came from (buildinfo Display)
}

// FeedbackKinds are the accepted values of FeedbackReport.Kind.
var FeedbackKinds = []string{"bug", "feature"}

// MaxFeedbackText is the longest accepted FeedbackReport.Text, in characters.
const MaxFeedbackText = 10000

// SetFeedbackURL sets the address the feedback form posts reports to (JSON,
// see FeedbackReport) and shows the "Feedback" button; "" hides it again.
// Serve the address with FeedbackHandler or an equivalent of your own.
func (s *Server) SetFeedbackURL(url string) {
	s.feedbackURL = url
}

// FeedbackHandler serves the address set with SetFeedbackURL: it accepts a
// POSTed FeedbackReport (JSON, at most 64 KiB), checks it, and hands it to
// submit, the application's back end. It answers 204 when submit succeeds,
// 400 for a malformed or invalid report, 405 for another method and 500
// when submit fails; errors come back as {"error": "..."}.
func FeedbackHandler(submit func(ctx context.Context, report FeedbackReport) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			feedbackError(w, http.StatusMethodNotAllowed, "use POST")
			return
		}
		var report FeedbackReport
		dec := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
		if err := dec.Decode(&report); err != nil {
			feedbackError(w, http.StatusBadRequest, "not a feedback report")
			return
		}
		report.Kind = strings.ToLower(strings.TrimSpace(report.Kind))
		report.Text = strings.TrimSpace(report.Text)
		if !validFeedbackKind(report.Kind) {
			feedbackError(w, http.StatusBadRequest, "kind must be one of "+strings.Join(FeedbackKinds, ", "))
			return
		}
		if report.Text == "" {
			feedbackError(w, http.StatusBadRequest, "the text is empty")
			return
		}
		if utf8.RuneCountInString(report.Text) > MaxFeedbackText {
			feedbackError(w, http.StatusBadRequest, "the text is too long")
			return
		}
		if err := submit(r.Context(), report); err != nil {
			feedbackError(w, http.StatusInternalServerError, "could not send the report")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func validFeedbackKind(kind string) bool {
	for _, k := range FeedbackKinds {
		if kind == k {
			return true
		}
	}
	return false
}

func feedbackError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck // best effort
}
