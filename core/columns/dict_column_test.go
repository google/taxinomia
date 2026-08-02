package columns

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
)

// partitionOf reduces a GroupIndices result to a canonical form: the set of
// groups, each identified by its value and its sorted member indices. Group keys
// themselves are opaque (StringColumn uses first-seen ordinals, DictStringColumn
// uses dictionary codes), so only the partition can be compared.
func partitionOf(t *testing.T, col IDataColumn, grouped map[uint32][]uint32) map[string][]uint32 {
	t.Helper()
	out := make(map[string][]uint32, len(grouped))
	for _, indices := range grouped {
		if len(indices) == 0 {
			t.Fatalf("empty group returned")
		}
		value, err := col.GetString(indices[0])
		if err != nil {
			t.Fatalf("GetString(%d): %v", indices[0], err)
		}
		if _, dup := out[value]; dup {
			t.Fatalf("value %q appears in two groups", value)
		}
		members := append([]uint32(nil), indices...)
		sort.Slice(members, func(a, b int) bool { return members[a] < members[b] })
		// Every member must carry the group's value.
		for _, i := range members {
			got, err := col.GetString(i)
			if err != nil {
				t.Fatalf("GetString(%d): %v", i, err)
			}
			if got != value {
				t.Fatalf("group %q contains index %d with value %q", value, i, got)
			}
		}
		out[value] = members
	}
	return out
}

func samePartition(t *testing.T, a, b map[string][]uint32) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("group count differs: %d vs %d", len(a), len(b))
	}
	for value, indicesA := range a {
		indicesB, ok := b[value]
		if !ok {
			t.Fatalf("group %q missing", value)
		}
		if len(indicesA) != len(indicesB) {
			t.Fatalf("group %q size differs: %d vs %d", value, len(indicesA), len(indicesB))
		}
		for i := range indicesA {
			if indicesA[i] != indicesB[i] {
				t.Fatalf("group %q member %d differs: %d vs %d", value, i, indicesA[i], indicesB[i])
			}
		}
	}
}

// buildPair fills a StringColumn and a DictStringColumn with the same values.
func buildPair(values []string) (*StringColumn, *DictStringColumn[uint8]) {
	def := NewColumnDef("zone", "Zone", "")
	plain := NewStringColumn(def)
	dict := NewDictStringColumn[uint8](def)
	for _, v := range values {
		plain.Append(v)
		dict.Append(v)
	}
	plain.FinalizeColumn()
	dict.FinalizeColumn()
	return plain, dict
}

func repetitiveValues(n, distinct int) []string {
	values := make([]string, n)
	for i := range values {
		values[i] = fmt.Sprintf("us-east%d-a", i%distinct)
	}
	return values
}

func TestDictColumn_GetStringMatchesStringColumn(t *testing.T) {
	values := repetitiveValues(1000, 7)
	plain, dict := buildPair(values)

	if plain.Length() != dict.Length() {
		t.Fatalf("length differs: %d vs %d", plain.Length(), dict.Length())
	}
	if dict.Cardinality() != 7 {
		t.Fatalf("cardinality = %d, want 7", dict.Cardinality())
	}
	for i := uint32(0); i < uint32(len(values)); i++ {
		want, err := plain.GetString(i)
		if err != nil {
			t.Fatalf("plain.GetString(%d): %v", i, err)
		}
		got, err := dict.GetString(i)
		if err != nil {
			t.Fatalf("dict.GetString(%d): %v", i, err)
		}
		if got != want {
			t.Fatalf("row %d: got %q, want %q", i, got, want)
		}
	}
}

func TestDictColumn_OutOfBounds(t *testing.T) {
	_, dict := buildPair(repetitiveValues(10, 3))
	if _, err := dict.GetString(10); err == nil {
		t.Fatal("GetString past the end should error")
	}
	if _, err := dict.GetValue(99); err == nil {
		t.Fatal("GetValue past the end should error")
	}
}

func TestDictColumn_GroupIndicesMatchesStringColumn(t *testing.T) {
	cases := []struct {
		name     string
		n        int
		distinct int
	}{
		{"single group", 500, 1},
		{"few groups", 1000, 7},
		{"many groups", 5000, 200},
		{"all distinct", 200, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plain, dict := buildPair(repetitiveValues(tc.n, tc.distinct))
			indices := createBenchIndices(tc.n)

			plainGroups, plainUnmapped := plain.GroupIndices(indices, nil)
			dictGroups, dictUnmapped := dict.GroupIndices(indices, nil)

			if plainUnmapped != nil || dictUnmapped != nil {
				t.Fatalf("unexpected unmapped indices: %v / %v", plainUnmapped, dictUnmapped)
			}
			samePartition(t,
				partitionOf(t, plain, plainGroups),
				partitionOf(t, dict, dictGroups))
		})
	}
}

func TestDictColumn_GroupIndicesOnSubset(t *testing.T) {
	// Grouping runs on filtered subsets, and nested grouping runs on a parent
	// group's indices - neither is dense or ordered.
	plain, dict := buildPair(repetitiveValues(1000, 9))
	subset := []uint32{997, 3, 500, 4, 12, 999, 41, 42, 43, 800}

	plainGroups, _ := plain.GroupIndices(subset, nil)
	dictGroups, _ := dict.GroupIndices(subset, nil)

	samePartition(t,
		partitionOf(t, plain, plainGroups),
		partitionOf(t, dict, dictGroups))
}

func TestDictColumn_GroupIndicesEmpty(t *testing.T) {
	_, dict := buildPair(repetitiveValues(100, 5))
	grouped, unmapped := dict.GroupIndices(nil, nil)
	if len(grouped) != 0 || unmapped != nil {
		t.Fatalf("empty input should produce no groups, got %v / %v", grouped, unmapped)
	}
}

func TestDictColumn_FilterMatchesStringColumn(t *testing.T) {
	plain, dict := buildPair(repetitiveValues(2000, 13))
	predicate := func(v string) bool { return strings.HasSuffix(v, "3-a") }

	want := plain.Filter(predicate)
	got := dict.Filter(predicate)

	if len(want) != len(got) {
		t.Fatalf("match count differs: %d vs %d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("match %d differs: %d vs %d", i, want[i], got[i])
		}
	}
}

func TestDictColumn_IsKey(t *testing.T) {
	_, repeated := buildPair(repetitiveValues(100, 5))
	if repeated.IsKey() {
		t.Fatal("repetitive column should not be a key")
	}
	if _, err := repeated.GetIndex("us-east0-a"); err == nil {
		t.Fatal("GetIndex on a non-key column should error")
	}

	_, unique := buildPair(repetitiveValues(200, 200))
	if !unique.IsKey() {
		t.Fatal("all-distinct column should be a key")
	}
	// When every value is distinct, the code is the row index.
	idx, err := unique.GetIndex("us-east42-a")
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	if idx != 42 {
		t.Fatalf("GetIndex returned row %d, want 42", idx)
	}
}

func TestDictColumn_Ranks(t *testing.T) {
	def := NewColumnDef("zone", "Zone", "")
	dict := NewDictStringColumn[uint8](def)
	for _, v := range []string{"charlie", "alpha", "bravo", "alpha"} {
		dict.Append(v)
	}
	dict.FinalizeColumn()

	ranks := dict.Ranks()
	// Codes are assigned first-seen: charlie=0, alpha=1, bravo=2.
	if ranks[0] != 2 || ranks[1] != 0 || ranks[2] != 1 {
		t.Fatalf("ranks = %v, want [2 0 1]", ranks)
	}
	if second := dict.Ranks(); &second[0] != &ranks[0] {
		t.Fatal("Ranks should be cached, not rebuilt")
	}
}

func TestDictColumn_JoinedColumnReadsThrough(t *testing.T) {
	// A dictionary column must still be usable as the source of a join.
	def := NewColumnDef("zone", "Zone", "zone")
	source := NewDictStringColumn[uint8](def)
	for _, v := range []string{"us-east1-a", "us-west1-b", "eu-west4-c"} {
		source.Append(v)
	}
	source.FinalizeColumn()

	// A joiner that maps row i to row (2 - i) in the source.
	joined := source.CreateJoinedColumn(NewColumnDef("m.zone", "Zone", ""), reverseJoiner{n: 3})
	for i, want := range []string{"eu-west4-c", "us-west1-b", "us-east1-a"} {
		got, err := joined.GetString(uint32(i))
		if err != nil {
			t.Fatalf("joined.GetString(%d): %v", i, err)
		}
		if got != want {
			t.Fatalf("row %d: got %q, want %q", i, got, want)
		}
	}
}

type reverseJoiner struct{ n uint32 }

func (r reverseJoiner) Lookup(index uint32) (uint32, error) {
	if index >= r.n {
		return 0, ErrUnmatched
	}
	return r.n - 1 - index, nil
}

func TestCompactStringColumn(t *testing.T) {
	cases := []struct {
		name       string
		values     []string
		wantCompat bool
		wantType   string
	}{
		{"low cardinality", repetitiveValues(8192, 5), true, "*columns.DictStringColumn[uint8]"},
		{"medium cardinality", repetitiveValues(10000, 300), true, "*columns.DictStringColumn[uint16]"},
		{"above cardinality cap", repetitiveValues(140000, 70000), false, "*columns.StringColumn"},
		{"below row threshold", repetitiveValues(1000, 5), false, "*columns.StringColumn"},
		{"empty", nil, false, "*columns.StringColumn"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plain := NewStringColumn(NewColumnDef("zone", "Zone", ""))
			for _, v := range tc.values {
				plain.Append(v)
			}
			plain.FinalizeColumn()

			compacted, ok := CompactStringColumn(plain)
			if ok != tc.wantCompat {
				t.Fatalf("compacted = %v, want %v", ok, tc.wantCompat)
			}
			if got := fmt.Sprintf("%T", compacted); got != tc.wantType {
				t.Fatalf("type = %s, want %s", got, tc.wantType)
			}
			if compacted.Length() != len(tc.values) {
				t.Fatalf("length = %d, want %d", compacted.Length(), len(tc.values))
			}
			// Values must survive the conversion unchanged.
			for i := range tc.values {
				got, err := compacted.GetString(uint32(i))
				if err != nil {
					t.Fatalf("GetString(%d): %v", i, err)
				}
				if got != tc.values[i] {
					t.Fatalf("row %d: got %q, want %q", i, got, tc.values[i])
				}
			}
		})
	}
}

func TestCompactStringColumn_KeyColumnDeclined(t *testing.T) {
	// A primary key is all-distinct by definition; compaction must decline
	// without scanning.
	plain := NewStringColumn(NewColumnDef("id", "ID", ""))
	for _, v := range repetitiveValues(8192, 8192) {
		plain.Append(v)
	}
	plain.FinalizeColumn()
	if !plain.IsKey() {
		t.Fatal("all-distinct column should be a key")
	}

	compacted, ok := CompactStringColumn(plain)
	if ok {
		t.Fatal("key column should not be compacted")
	}
	if compacted != IDataColumn(plain) {
		t.Fatal("declining should return the original column")
	}
}

func TestDictColumn_RanksConcurrent(t *testing.T) {
	_, dict := buildPair(repetitiveValues(1000, 9))

	var wg sync.WaitGroup
	results := make([][]uint8, 8)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			results[g] = dict.Ranks()
		}(g)
	}
	wg.Wait()

	for g, ranks := range results {
		if &ranks[0] != &results[0][0] {
			t.Fatalf("goroutine %d got a different ranks slice", g)
		}
	}
}

func TestDictColumn_AppendOverflowPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("appending a 257th distinct value to a uint8 column should panic")
		}
	}()
	col := NewDictStringColumn[uint8](NewColumnDef("zone", "Zone", ""))
	for i := 0; i < 257; i++ {
		col.Append(fmt.Sprintf("value_%d", i))
	}
}
