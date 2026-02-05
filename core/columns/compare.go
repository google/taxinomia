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

import (
	"math"
	"strings"
	"time"
)

// CompareAtIndex compares values at indices i and j for the given column.
// Returns -1 if value[i] < value[j], 0 if equal, 1 if value[i] > value[j].
// Uses type-specific comparison for efficiency on base columns.
func CompareAtIndex(col IDataColumn, i, j uint32) int {
	switch c := col.(type) {
	case *StringColumn:
		return strings.Compare(c.data[i], c.data[j])

	case *Uint32Column:
		if c.data[i] < c.data[j] {
			return -1
		}
		if c.data[i] > c.data[j] {
			return 1
		}
		return 0

	case *DatetimeColumn:
		return compareTimes(c.data[i], c.data[j])

	case *DurationColumn:
		return compareDurations(c.data[i], c.data[j])

	case *BoolColumn:
		return compareBools(c.data[i], c.data[j])

	case *Float64Column:
		return compareFloat64s(c.data[i], c.data[j])

	case *Int64Column:
		if c.data[i] < c.data[j] {
			return -1
		}
		if c.data[i] > c.data[j] {
			return 1
		}
		return 0

	case *Uint64Column:
		if c.data[i] < c.data[j] {
			return -1
		}
		if c.data[i] > c.data[j] {
			return 1
		}
		return 0

	// Computed columns - must evaluate on the fly
	case *ComputedStringColumn:
		vi, errI := c.GetValue(i)
		vj, errJ := c.GetValue(j)
		if errI != nil || errJ != nil {
			return compareErrors(errI, errJ)
		}
		return strings.Compare(vi, vj)

	case *ComputedUint32Column:
		vi, errI := c.GetValue(i)
		vj, errJ := c.GetValue(j)
		if errI != nil || errJ != nil {
			return compareErrors(errI, errJ)
		}
		if vi < vj {
			return -1
		}
		if vi > vj {
			return 1
		}
		return 0

	case *ComputedInt64Column:
		vi, errI := c.GetValue(i)
		vj, errJ := c.GetValue(j)
		if errI != nil || errJ != nil {
			return compareErrors(errI, errJ)
		}
		if vi < vj {
			return -1
		}
		if vi > vj {
			return 1
		}
		return 0

	case *ComputedFloat64Column:
		vi, errI := c.GetValue(i)
		vj, errJ := c.GetValue(j)
		if errI != nil || errJ != nil {
			return compareErrors(errI, errJ)
		}
		return compareFloat64s(vi, vj)

	case *ComputedDatetimeColumn:
		vi, errI := c.GetValue(i)
		vj, errJ := c.GetValue(j)
		if errI != nil || errJ != nil {
			return compareErrors(errI, errJ)
		}
		if vi < vj {
			return -1
		}
		if vi > vj {
			return 1
		}
		return 0

	case *ComputedDurationColumn:
		vi, errI := c.GetValue(i)
		vj, errJ := c.GetValue(j)
		if errI != nil || errJ != nil {
			return compareErrors(errI, errJ)
		}
		return compareDurations(vi, vj)

	case *ComputedBoolColumn:
		vi, errI := c.GetValue(i)
		vj, errJ := c.GetValue(j)
		if errI != nil || errJ != nil {
			return compareErrors(errI, errJ)
		}
		return compareBools(vi, vj)

	// Joined columns - use string comparison as fallback
	default:
		// Fallback: use string representation
		si, errI := col.GetString(i)
		sj, errJ := col.GetString(j)
		if errI != nil || errJ != nil {
			return compareErrors(errI, errJ)
		}
		return strings.Compare(si, sj)
	}
}

// compareTimes compares two time.Time values
func compareTimes(a, b time.Time) int {
	if a.Before(b) {
		return -1
	}
	if a.After(b) {
		return 1
	}
	return 0
}

// compareDurations compares two time.Duration values
func compareDurations(a, b time.Duration) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// compareBools compares two bool values (false < true)
func compareBools(a, b bool) int {
	if a == b {
		return 0
	}
	if !a && b {
		return -1
	}
	return 1
}

// compareFloat64s compares two float64 values with NaN handling.
// NaN values are considered greater than all other values (sort to end).
func compareFloat64s(a, b float64) int {
	aNaN := math.IsNaN(a)
	bNaN := math.IsNaN(b)

	if aNaN && bNaN {
		return 0 // Both NaN - equal
	}
	if aNaN {
		return 1 // a is NaN, b isn't - a comes after
	}
	if bNaN {
		return -1 // b is NaN, a isn't - a comes before
	}

	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// compareErrors handles error cases in comparison.
// Errors sort to the end (after valid values).
func compareErrors(errI, errJ error) int {
	if errI != nil && errJ != nil {
		return 0 // Both errors - equal
	}
	if errI != nil {
		return 1 // i has error, j doesn't - i comes after
	}
	return -1 // j has error, i doesn't - i comes before
}
