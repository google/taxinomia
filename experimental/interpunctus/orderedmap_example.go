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
)

// Example showing how OrderedMap could be used in the grouping system
func DemonstrateOrderedMapUsage() {
	fmt.Println("\n=== Demonstrating OrderedMap for Stable Grouping ===")

	// Simulate the groupKeyToFilter map that causes instability
	fmt.Println("\n1. Problem: Regular map iteration is unstable")
	regularMap := make(map[uint32]string)
	regularMap[3] = "Electronics"
	regularMap[1] = "Clothing"
	regularMap[2] = "Food"
	regularMap[4] = "Books"

	fmt.Println("Iterating regular map multiple times:")
	for i := 0; i < 3; i++ {
		fmt.Printf("  Run %d: ", i+1)
		for k, v := range regularMap {
			fmt.Printf("[%d:%s] ", k, v)
		}
		fmt.Println()
	}

	// Solution using OrderedMap
	fmt.Println("\n2. Solution: OrderedMap preserves insertion order")
	orderedMap := NewOrderedMap[uint32, string]()
	orderedMap.Set(3, "Electronics")
	orderedMap.Set(1, "Clothing")
	orderedMap.Set(2, "Food")
	orderedMap.Set(4, "Books")

	fmt.Println("Iterating OrderedMap multiple times:")
	for i := 0; i < 3; i++ {
		fmt.Printf("  Run %d: ", i+1)
		orderedMap.Range(func(k uint32, v string) bool {
			fmt.Printf("[%d:%s] ", k, v)
			return true
		})
		fmt.Println()
	}

	// Show how this would fix the groupKeyToOrder assignment
	fmt.Println("\n3. Impact on groupKeyToOrder assignment:")
	fmt.Println("With regular map - order changes each run:")
	fmt.Println("  Run 1: Electronics=0, Clothing=1, Food=2, Books=3")
	fmt.Println("  Run 2: Clothing=0, Books=1, Electronics=2, Food=3")
	fmt.Println("  Run 3: Food=0, Electronics=1, Books=2, Clothing=3")

	fmt.Println("\nWith OrderedMap - order is consistent:")
	groupKeyToOrder := make(map[uint32]uint32)
	order := uint32(0)
	orderedMap.Range(func(k uint32, v string) bool {
		groupKeyToOrder[k] = order
		fmt.Printf("  %s (key=%d) -> order=%d\n", v, k, order)
		order++
		return true
	})

	fmt.Println("\n✓ OrderedMap ensures stable ordering across refreshes")
}