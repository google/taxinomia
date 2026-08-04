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
	"testing"
	"time"
)

// testTimes returns 21 datetimes with repeats (7 distinct across chunk
// boundaries at chunk size 8), including the zero time.
func testTimes() []time.Time {
	base := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	out := make([]time.Time, 21)
	for i := range out {
		switch i % 7 {
		case 6:
			out[i] = time.Time{}
		default:
			out[i] = base.Add(time.Duration(i%7) * 26 * time.Hour)
		}
	}
	return out
}

func TestChunkedDatetimeColumnParity(t *testing.T) {
	t.Run("repeats", func(t *testing.T) {
		plain := NewDatetimeColumn(NewColumnDef("d", "D", ""))
		chunked := newChunkedDatetimeColumn(NewColumnDef("d", "D", ""), testChunkSize)
		for _, v := range testTimes() {
			plain.Append(v)
			chunked.Append(v)
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		checkColumnParity(t, plain, chunked)
	})

	t.Run("key", func(t *testing.T) {
		plain := NewDatetimeColumn(NewColumnDef("d", "D", "event"))
		chunked := newChunkedDatetimeColumn(NewColumnDef("d", "D", "event"), testChunkSize)
		base := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
		for i := 0; i < 21; i++ {
			v := base.Add(time.Duration(i) * time.Minute)
			plain.Append(v)
			chunked.Append(v)
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		if !chunked.IsKey() {
			t.Fatal("distinct entity datetime column not detected as key")
		}
		checkColumnParity(t, plain, chunked)
		for i := 0; i < 21; i++ {
			v := base.Add(time.Duration(i) * time.Minute)
			pi, pe := plain.GetIndex(v)
			ci, ce := chunked.GetIndex(v)
			if pe != nil || ce != nil || pi != ci {
				t.Fatalf("GetIndex(%v): plain (%d, %v), chunked (%d, %v)", v, pi, pe, ci, ce)
			}
		}
		// Lookup is by instant, not by wall representation: a non-UTC
		// rendering of a stored instant must still resolve.
		shifted := base.In(time.FixedZone("plus2", 2*3600))
		if idx, err := chunked.GetIndex(shifted); err != nil || idx != 0 {
			t.Fatalf("GetIndex of zone-shifted instant: (%d, %v)", idx, err)
		}
	})

	t.Run("display format and location", func(t *testing.T) {
		plain := NewDatetimeColumn(NewColumnDef("d", "D", ""))
		chunked := newChunkedDatetimeColumn(NewColumnDef("d", "D", ""), testChunkSize)
		for _, v := range testTimes() {
			plain.Append(v)
			chunked.Append(v)
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()

		loc := time.FixedZone("plus5", 5*3600)
		plain.SetDisplayFormat(DatetimeFormatISO)
		chunked.SetDisplayFormat(DatetimeFormatISO)
		plain.SetLocation(loc)
		chunked.SetLocation(loc)
		for i := 0; i < plain.Length(); i++ {
			ps, _ := plain.GetString(uint32(i))
			cs, _ := chunked.GetString(uint32(i))
			if ps != cs {
				t.Fatalf("GetString(%d) after format/location change: plain %q, chunked %q", i, ps, cs)
			}
		}
	})

	t.Run("append variants", func(t *testing.T) {
		plain := NewDatetimeColumn(NewColumnDef("d", "D", ""))
		chunked := newChunkedDatetimeColumn(NewColumnDef("d", "D", ""), testChunkSize)
		plain.AppendUnix(1700000000)
		chunked.AppendUnix(1700000000)
		plain.AppendUnixNano(1700000000123456789)
		chunked.AppendUnixNano(1700000000123456789)
		if err := plain.AppendString("2024-03-15 10:30:00"); err != nil {
			t.Fatal(err)
		}
		if err := chunked.AppendString("2024-03-15 10:30:00"); err != nil {
			t.Fatal(err)
		}
		if err := chunked.AppendString("not a date"); err == nil {
			t.Fatal("AppendString of garbage succeeded")
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		checkColumnParity(t, plain, chunked)
	})

	t.Run("epoch extraction", func(t *testing.T) {
		plain := NewDatetimeColumn(NewColumnDef("d", "D", ""))
		chunked := newChunkedDatetimeColumn(NewColumnDef("d", "D", ""), testChunkSize)
		for _, v := range testTimes() {
			plain.Append(v)
			chunked.Append(v)
		}
		type extractor struct {
			name           string
			plain, chunked func(uint32) (int64, error)
		}
		extractors := []extractor{
			{"Seconds", plain.Seconds, chunked.Seconds},
			{"Minutes", plain.Minutes, chunked.Minutes},
			{"Hours", plain.Hours, chunked.Hours},
			{"Days", plain.Days, chunked.Days},
			{"Weeks", plain.Weeks, chunked.Weeks},
			{"Months", plain.Months, chunked.Months},
			{"Quarters", plain.Quarters, chunked.Quarters},
			{"Years", plain.Years, chunked.Years},
		}
		for _, ex := range extractors {
			for i := 0; i < plain.Length(); i++ {
				pv, pe := ex.plain(uint32(i))
				cv, ce := ex.chunked(uint32(i))
				if (pe == nil) != (ce == nil) || pv != cv {
					t.Fatalf("%s(%d): plain (%d, %v), chunked (%d, %v)", ex.name, i, pv, pe, cv, ce)
				}
			}
			if _, err := ex.chunked(uint32(chunked.Length())); err == nil {
				t.Fatalf("%s out of bounds succeeded", ex.name)
			}
		}
	})

	t.Run("structured filters", func(t *testing.T) {
		chunked := newChunkedDatetimeColumn(NewColumnDef("d", "D", ""), testChunkSize)
		times := testTimes()
		for _, v := range times {
			chunked.Append(v)
		}
		chunked.FinalizeColumn()

		target := times[2]
		naive := chunked.FilterSelection(func(v time.Time) bool { return v.Equal(target) })
		checkSelectionsEqual(t, "FilterSelectionEqual", naive, chunked.FilterSelectionEqual(target))

		lo, hi := times[1], times[3]
		naiveRange := chunked.FilterSelection(func(v time.Time) bool {
			return !v.Before(lo) && !v.After(hi)
		})
		checkSelectionsEqual(t, "FilterSelectionRange", naiveRange, chunked.FilterSelectionRange(&lo, &hi))
	})

	t.Run("joined from chunked source", func(t *testing.T) {
		// A chunked datetime column as the source of a joined column, via the
		// widened NewJoinedDatetimeColumn signature.
		source := newChunkedDatetimeColumn(NewColumnDef("d", "D", ""), testChunkSize)
		base := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
		for i := 0; i < 5; i++ {
			source.Append(base.Add(time.Duration(i) * time.Hour))
		}
		source.FinalizeColumn()

		joined := source.CreateJoinedColumn(NewColumnDef("j", "J", ""), identityJoiner{n: 5})
		for i := 0; i < 5; i++ {
			want, _ := source.GetString(uint32(i))
			got, err := joined.GetString(uint32(i))
			if err != nil || got != want {
				t.Fatalf("joined GetString(%d): (%q, %v), want %q", i, got, err, want)
			}
		}
	})
}

// identityJoiner maps each row to itself, for joined-column plumbing tests.
type identityJoiner struct{ n uint32 }

func (j identityJoiner) Lookup(i uint32) (uint32, error) {
	if i >= j.n {
		return 0, ErrUnmatched
	}
	return i, nil
}
