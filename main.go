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

package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/google/taxinomia/demo"
)

const serverAddress = "127.0.0.1:8097"

func main() {
	srv, products, err := demo.SetupDemoServer()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Handle all requests and route based on product path
	// URL format: /{product}/ or /{product}/table
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Parse product name and action from path
		productName, action := parseProductPath(path)

		// Redirect root to default product
		if productName == "" {
			http.Redirect(w, r, "/default/", http.StatusFound)
			return
		}

		product := products.Get(productName)
		if product == nil {
			http.NotFound(w, r)
			return
		}

		switch action {
		case "table":
			result := srv.HandleTableRequest(w, r.URL, product, w.Header().Set)
			if result != nil {
				http.Error(w, result.Message, result.StatusCode)
			}
		default:
			srv.HandleLandingRequest(w, r.URL, product, w.Header().Set)
		}
	})

	fmt.Printf("\nServer starting on http://%s\n", serverAddress)
	fmt.Printf("Products available:\n")
	for _, p := range products.GetAll() {
		fmt.Printf("  - /%s/\n", p.Name)
	}
	log.Fatal(http.ListenAndServe(serverAddress, nil))
}

// parseProductPath extracts the product name and action from a URL path.
// URL format: /{product}/ or /{product}/table
// Returns (productName, action) where action is "landing" or "table".
// Returns empty productName if path is "/" to trigger redirect.
func parseProductPath(path string) (string, string) {
	// Remove leading slash and split
	path = strings.TrimPrefix(path, "/")
	parts := strings.SplitN(path, "/", 2)

	// Root path - return empty to trigger redirect
	if len(parts) == 0 || parts[0] == "" {
		return "", "landing"
	}

	// First part is always the product name
	productName := parts[0]
	action := "landing"

	if len(parts) > 1 && parts[1] != "" {
		// Remove trailing slash and check for action
		secondPart := strings.TrimSuffix(parts[1], "/")
		if secondPart == "table" {
			action = "table"
		}
	}

	return productName, action
}
