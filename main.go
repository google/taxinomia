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

	"github.com/google/taxinomia/demo"
)

const serverAddress = "127.0.0.1:8097"

func main() {
	srv, err := demo.SetupDemoServer()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	http.HandleFunc("/table", func(w http.ResponseWriter, r *http.Request) {
		result := srv.HandleTableRequest(w, r.URL, w.Header().Set)
		if result != nil {
			http.Error(w, result.Message, result.StatusCode)
		}
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		srv.HandleLandingRequest(w, r.URL, w.Header().Set)
	})

	fmt.Printf("\nServer starting on http://%s\n", serverAddress)
	log.Fatal(http.ListenAndServe(serverAddress, nil))
}
