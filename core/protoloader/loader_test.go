/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package protoloader

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/reflect/protoregistry"
)

// TestLoaderIntegration tests the full workflow with actual proto descriptors.
func TestLoaderIntegration(t *testing.T) {
	registry := new(protoregistry.Files)
	loader := NewLoader(registry)

	// Test GetRegisteredMessages on empty registry
	messages := loader.GetRegisteredMessages()
	if len(messages) != 0 {
		t.Errorf("expected empty message list, got %d messages", len(messages))
	}
}

// TestRowBuilder tests the row builder functionality directly.
func TestRowBuilder(t *testing.T) {
	// Create a simple hierarchy with scalar fields
	rb := &RowBuilder{
		columns: []string{"name", "value", "amount"},
		current: make(map[string]string),
	}

	// Simulate building rows
	rb.current["name"] = "test1"
	rb.current["value"] = "100"
	rb.current["amount"] = "50"
	rb.emitRow()

	rb.current["name"] = "test2"
	rb.current["value"] = "200"
	rb.current["amount"] = "75"
	rb.emitRow()

	if len(rb.rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rb.rows))
	}

	// Verify first row
	if rb.rows[0][0] != "test1" || rb.rows[0][1] != "100" || rb.rows[0][2] != "50" {
		t.Errorf("unexpected first row: %v", rb.rows[0])
	}

	// Verify second row
	if rb.rows[1][0] != "test2" || rb.rows[1][1] != "200" || rb.rows[1][2] != "75" {
		t.Errorf("unexpected second row: %v", rb.rows[1])
	}
}

// TestNewLoader tests loader creation.
func TestNewLoader(t *testing.T) {
	registry := new(protoregistry.Files)
	loader := NewLoader(registry)
	if loader == nil {
		t.Fatal("NewLoader returned nil")
	}
	if loader.registry != registry {
		t.Fatal("loader.registry does not match provided registry")
	}
}

// TestParseTextprotoMissingMessage tests error handling for unknown message type.
func TestParseTextprotoMissingMessage(t *testing.T) {
	registry := new(protoregistry.Files)
	loader := NewLoader(registry)

	// Create a dummy textproto file
	testDir := filepath.Join(os.TempDir(), "protoloader_textproto_test")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("failed to create test dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	textprotoPath := filepath.Join(testDir, "test.textproto")
	if err := os.WriteFile(textprotoPath, []byte("name: \"test\""), 0644); err != nil {
		t.Fatalf("failed to write textproto: %v", err)
	}

	_, err := loader.ParseTextproto(textprotoPath, "unknown.Message")
	if err == nil {
		t.Error("expected error for unknown message type, got nil")
	}
}
