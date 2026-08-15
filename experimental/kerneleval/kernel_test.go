package kerneleval

import (
	"fmt"
	"math/rand"
	"testing"
)

func genKeys(n, d int) []int64 {
	r := rand.New(rand.NewSource(42))
	keys := make([]int64, n)
	for i := range keys {
		// Spread key values so hashes are not trivially sequential.
		keys[i] = int64(r.Intn(d))*7919 + 13
	}
	return keys
}

func capacityFor(d int) int {
	c := 1
	for c < d*2 {
		c <<= 1
	}
	return c
}

func TestParity(t *testing.T) {
	for _, d := range []int{1, 1000, 100000} {
		keys := genKeys(1_000_000, d)
		mc, mcnt, mnd := PartitionGoMap(keys, d)
		oc, ocnt, ond := PartitionGoOpenAddr(keys, capacityFor(d))
		cc, ccnt, cnd := PartitionCXX(keys, 65536, capacityFor(d))
		if mnd != ond || mnd != cnd {
			t.Fatalf("d=%d ndistinct: map=%d go=%d cxx=%d", d, mnd, ond, cnd)
		}
		for i := range mc {
			if mc[i] != oc[i] || mc[i] != cc[i] {
				t.Fatalf("d=%d codes differ at %d: map=%d go=%d cxx=%d", d, i, mc[i], oc[i], cc[i])
			}
		}
		for c := 0; c < mnd; c++ {
			if mcnt[c] != ocnt[c] || mcnt[c] != ccnt[c] {
				t.Fatalf("d=%d counts differ at code %d", d, c)
			}
		}
	}
}

func BenchmarkPartition(b *testing.B) {
	const n = 1_000_000
	for _, d := range []int{1000, 100000} {
		keys := genKeys(n, d)
		cap := capacityFor(d)
		b.Run(fmt.Sprintf("d=%d/gomap", d), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				PartitionGoMap(keys, d)
			}
		})
		b.Run(fmt.Sprintf("d=%d/go-openaddr", d), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				PartitionGoOpenAddr(keys, cap)
			}
		})
		b.Run(fmt.Sprintf("d=%d/cxx-64k", d), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				PartitionCXX(keys, 65536, cap)
			}
		})
	}
}

func BenchmarkCgoCallOverhead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Noop()
	}
}
