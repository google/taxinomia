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

package users

import (
	pb "github.com/google/taxinomia/core/users/proto"
)

// UserStore defines the interface for accessing user profiles.
// Implementations handle loading and storing user data.
type UserStore interface {
	// GetUser returns a user profile by name, or nil if not found.
	GetUser(name string) *pb.UserProfile
}

// HasDomain checks if a user has access to a given domain.
func HasDomain(user *pb.UserProfile, domain string) bool {
	if user == nil {
		return false
	}
	for _, d := range user.GetDomains() {
		if d == domain {
			return true
		}
	}
	return false
}

// HasAnyDomain checks if a user has access to any of the given domains.
func HasAnyDomain(user *pb.UserProfile, domains []string) bool {
	if user == nil {
		return false
	}
	userDomains := make(map[string]bool)
	for _, d := range user.GetDomains() {
		userDomains[d] = true
	}
	for _, d := range domains {
		if userDomains[d] {
			return true
		}
	}
	return false
}
