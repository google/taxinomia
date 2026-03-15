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
	"os"

	"github.com/google/taxinomia/datasources"
)

// FileSystem provides an abstraction over OS file operations.
// This is the single point of control for all file access in the application.
type FileSystem struct{}

// NewFileSystem creates a new FileSystem instance.
func NewFileSystem() *FileSystem {
	return &FileSystem{}
}

// ReadFile reads a file and returns its contents.
// Implements datasources.FileReader.
func (fs *FileSystem) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// ReadDir lists directory contents.
// Implements datasources.DirReader.
func (fs *FileSystem) ReadDir(path string) ([]datasources.DirEntry, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	result := make([]datasources.DirEntry, len(entries))
	for i, e := range entries {
		result[i] = datasources.DirEntry{Name: e.Name(), IsDir: e.IsDir()}
	}
	return result, nil
}

// FileReader returns the FileReader function for injection into components.
func (fs *FileSystem) FileReader() datasources.FileReader {
	return fs.ReadFile
}

// DirReader returns the DirReader function for injection into components.
func (fs *FileSystem) DirReader() datasources.DirReader {
	return fs.ReadDir
}
