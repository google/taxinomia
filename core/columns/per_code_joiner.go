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

package columns

import "sync"

// dictCodedColumn is the surface a dictionary-encoded string column exposes to
// the per-code joiner: per-row codes, the code -> value dictionary, and typed
// string access for the fallback path.
type dictCodedColumn[K Unsigned] interface {
	IDataColumnT[string]
	GetCode(i uint32) K
	DictValue(code uint32) string
	Cardinality() int
}

// unmatchedTarget marks a dictionary code whose value has no row in the
// target column. Row indices are uint32 and the design range is 10^9 rows,
// so the top value is free to serve as the sentinel.
const unmatchedTarget = ^uint32(0)

// PerCodeJoiner resolves an FK -> PK join once per distinct dictionary code
// instead of once per row (docs/scaling-to-1b-rows.md §6: joins hit the key
// index d times, not n times). The from side is a dictionary-encoded string
// column; the first Lookup builds a code -> target-row table with one
// GetIndex per distinct value, and every lookup after that is two array
// reads — no string materialization, no map probe, no binary search.
//
// The memo is built once under sync.Once, so concurrent queries share a
// single build; after it the joiner is immutable and lock-free. Codes
// interned after the build (tables are append-only and may grow) fall back
// to a direct per-code resolution rather than growing the memo.
//
// Unmatched values return ErrUnmatched, where the per-row Joiner returned
// the target column's value-not-found error; every consumer treats any
// lookup error as unmatched, so only the message differs.
type PerCodeJoiner[K Unsigned] struct {
	from dictCodedColumn[K]
	to   IDataColumnT[string]

	once    sync.Once
	targets []uint32 // code -> row in to; unmatchedTarget = no match
}

// PerCodeJoinerFor returns a PerCodeJoiner when from is a dictionary-encoded
// string column (plain or chunked, any code width) and to supports typed
// string access, nil otherwise. Callers fall back to the per-row Joiner for
// representations without dictionary codes.
func PerCodeJoinerFor(from, to IDataColumn) IJoiner {
	toT, ok := to.(IDataColumnT[string])
	if !ok {
		return nil
	}
	switch f := from.(type) {
	case *DictStringColumn[uint8]:
		return &PerCodeJoiner[uint8]{from: f, to: toT}
	case *DictStringColumn[uint16]:
		return &PerCodeJoiner[uint16]{from: f, to: toT}
	case *DictStringColumn[uint32]:
		return &PerCodeJoiner[uint32]{from: f, to: toT}
	case *ChunkedDictStringColumn[uint8]:
		return &PerCodeJoiner[uint8]{from: f, to: toT}
	case *ChunkedDictStringColumn[uint16]:
		return &PerCodeJoiner[uint16]{from: f, to: toT}
	case *ChunkedDictStringColumn[uint32]:
		return &PerCodeJoiner[uint32]{from: f, to: toT}
	}
	return nil
}

// Lookup returns the target-table row holding the value of from-table row i.
func (j *PerCodeJoiner[K]) Lookup(i uint32) (uint32, error) {
	if i >= uint32(j.from.Length()) {
		// Out of range: return the exact error the per-row path would.
		_, err := j.from.GetValue(i)
		return 0, err
	}
	j.once.Do(j.build)
	code := uint32(j.from.GetCode(i))
	if code < uint32(len(j.targets)) {
		t := j.targets[code]
		if t == unmatchedTarget {
			return 0, ErrUnmatched
		}
		return t, nil
	}
	// Code interned after the memo was built: resolve directly.
	return j.to.GetIndex(j.from.DictValue(code))
}

func (j *PerCodeJoiner[K]) build() {
	targets := make([]uint32, j.from.Cardinality())
	for code := range targets {
		t, err := j.to.GetIndex(j.from.DictValue(uint32(code)))
		if err != nil {
			targets[code] = unmatchedTarget
			continue
		}
		targets[code] = t
	}
	j.targets = targets
}
