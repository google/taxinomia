package main

import (
	"fmt"
	"testing"
)

func TestOrderedMap(t *testing.T) {
	// Test basic operations
	om := NewOrderedMap[string, int]()

	// Test Set and Get
	om.Set("first", 1)
	om.Set("second", 2)
	om.Set("third", 3)

	if val, ok := om.Get("second"); !ok || val != 2 {
		t.Errorf("Expected Get('second') to return 2, got %d", val)
	}

	// Test Keys preserve insertion order
	keys := om.Keys()
	expected := []string{"first", "second", "third"}
	if len(keys) != len(expected) {
		t.Errorf("Expected %d keys, got %d", len(expected), len(keys))
	}
	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Expected key[%d] = %s, got %s", i, expected[i], key)
		}
	}

	// Test updating existing key doesn't change order
	om.Set("first", 10)
	keys = om.Keys()
	if keys[0] != "first" {
		t.Errorf("Updating value should not change key order")
	}

	// Test Delete
	om.Delete("second")
	keys = om.Keys()
	expected = []string{"first", "third"}
	if len(keys) != len(expected) {
		t.Errorf("Expected %d keys after delete, got %d", len(expected), len(keys))
	}
	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("After delete, expected key[%d] = %s, got %s", i, expected[i], key)
		}
	}

	// Test Range
	om.Clear()
	om.Set("a", 1)
	om.Set("b", 2)
	om.Set("c", 3)

	var rangeKeys []string
	var rangeVals []int
	om.Range(func(k string, v int) bool {
		rangeKeys = append(rangeKeys, k)
		rangeVals = append(rangeVals, v)
		return true
	})

	expectedKeys := []string{"a", "b", "c"}
	expectedVals := []int{1, 2, 3}
	for i := range expectedKeys {
		if rangeKeys[i] != expectedKeys[i] || rangeVals[i] != expectedVals[i] {
			t.Errorf("Range iteration order incorrect")
		}
	}

	// Test early termination in Range
	count := 0
	om.Range(func(k string, v int) bool {
		count++
		return count < 2 // Stop after 2 iterations
	})
	if count != 2 {
		t.Errorf("Expected Range to stop after 2 iterations, got %d", count)
	}
}

func TestOrderedMapExample(t *testing.T) {
	fmt.Println("=== OrderedMap Example ===")

	// Regular map - order is not preserved
	regularMap := make(map[string]int)
	regularMap["third"] = 3
	regularMap["first"] = 1
	regularMap["second"] = 2

	fmt.Println("Regular map (order not preserved):")
	for k, v := range regularMap {
		fmt.Printf("  %s: %d\n", k, v)
	}

	// OrderedMap - preserves insertion order
	orderedMap := NewOrderedMap[string, int]()
	orderedMap.Set("third", 3)
	orderedMap.Set("first", 1)
	orderedMap.Set("second", 2)

	fmt.Println("\nOrderedMap (insertion order preserved):")
	orderedMap.Range(func(k string, v int) bool {
		fmt.Printf("  %s: %d\n", k, v)
		return true
	})

	fmt.Println("\n✓ OrderedMap implementation complete")
}