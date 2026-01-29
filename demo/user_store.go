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

package demo

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/taxinomia/core/users"
	"google.golang.org/protobuf/encoding/prototext"
)

// ProfileFileName is the name of the profile file in each user directory.
const ProfileFileName = "profile.textproto"

// UserStore manages user profiles loaded from files.
type UserStore struct {
	users map[string]*users.UserProfile
}

// NewUserStore creates a new empty UserStore.
func NewUserStore() *UserStore {
	return &UserStore{
		users: make(map[string]*users.UserProfile),
	}
}

// LoadFromDirectory loads user profiles from subdirectories.
// Each subdirectory should contain a profile.textproto file.
func (s *UserStore) LoadFromDirectory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read users directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		profilePath := filepath.Join(dir, entry.Name(), ProfileFileName)
		if _, err := os.Stat(profilePath); os.IsNotExist(err) {
			// Skip directories without a profile file
			continue
		}

		profile, err := LoadUserProfile(profilePath)
		if err != nil {
			return fmt.Errorf("failed to load user profile %s: %w", entry.Name(), err)
		}

		// Use folder name as the user identifier
		s.users[entry.Name()] = profile
	}

	return nil
}

// GetUser returns a user profile by name, or nil if not found.
func (s *UserStore) GetUser(name string) *users.UserProfile {
	return s.users[name]
}

// GetAllUsers returns all loaded user profiles.
func (s *UserStore) GetAllUsers() []*users.UserProfile {
	result := make([]*users.UserProfile, 0, len(s.users))
	for _, user := range s.users {
		result = append(result, user)
	}
	return result
}

// LoadUserProfile loads a single user profile from a textproto file.
func LoadUserProfile(filePath string) (*users.UserProfile, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	profile := &users.UserProfile{}
	if err := prototext.Unmarshal(data, profile); err != nil {
		return nil, fmt.Errorf("failed to parse textproto: %w", err)
	}

	return profile, nil
}
