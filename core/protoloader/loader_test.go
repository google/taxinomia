/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

package protoloader

import (
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
		current: make(map[string]any),
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

	// Pass textproto content directly as bytes
	textprotoData := []byte("name: \"test\"")

	_, err := loader.ParseTextproto(textprotoData, "unknown.Message")
	if err == nil {
		t.Error("expected error for unknown message type, got nil")
	}
}
