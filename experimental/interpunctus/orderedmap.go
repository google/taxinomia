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

// OrderedMap is a map that preserves the order of insertion
type OrderedMap[K comparable, V any] struct {
	keys   []K
	values map[K]V
}

// NewOrderedMap creates a new ordered map
func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		keys:   make([]K, 0),
		values: make(map[K]V),
	}
}

// Set adds or updates a key-value pair
func (om *OrderedMap[K, V]) Set(key K, value V) {
	if _, exists := om.values[key]; !exists {
		om.keys = append(om.keys, key)
	}
	om.values[key] = value
}

// Get retrieves a value by key
func (om *OrderedMap[K, V]) Get(key K) (V, bool) {
	val, exists := om.values[key]
	return val, exists
}

// Delete removes a key-value pair
func (om *OrderedMap[K, V]) Delete(key K) {
	if _, exists := om.values[key]; exists {
		delete(om.values, key)
		// Remove key from slice
		for i, k := range om.keys {
			if k == key {
				om.keys = append(om.keys[:i], om.keys[i+1:]...)
				break
			}
		}
	}
}

// Keys returns all keys in insertion order
func (om *OrderedMap[K, V]) Keys() []K {
	result := make([]K, len(om.keys))
	copy(result, om.keys)
	return result
}

// Values returns all values in insertion order
func (om *OrderedMap[K, V]) Values() []V {
	result := make([]V, len(om.keys))
	for i, k := range om.keys {
		result[i] = om.values[k]
	}
	return result
}

// Len returns the number of key-value pairs
func (om *OrderedMap[K, V]) Len() int {
	return len(om.keys)
}

// Clear removes all key-value pairs
func (om *OrderedMap[K, V]) Clear() {
	om.keys = om.keys[:0]
	om.values = make(map[K]V)
}

// Range iterates over the map in insertion order
// If f returns false, iteration stops
func (om *OrderedMap[K, V]) Range(f func(key K, value V) bool) {
	for _, k := range om.keys {
		if !f(k, om.values[k]) {
			break
		}
	}
}

// Has checks if a key exists
func (om *OrderedMap[K, V]) Has(key K) bool {
	_, exists := om.values[key]
	return exists
}