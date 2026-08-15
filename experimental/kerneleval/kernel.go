package kerneleval

// #cgo CXXFLAGS: -O2 -std=c++17
// #include <stdint.h>
// int64_t hash_partition_i64(const int64_t* keys, int64_t n, uint32_t* codes_out,
//                            int64_t* slot_keys, uint32_t* slot_codes,
//                            uint8_t* slot_state, int64_t cap_mask,
//                            int64_t ndistinct, uint64_t* counts);
// void noop_kernel(void);
import "C"
import "unsafe"

// PartitionCXX partitions keys in batches of batchSize through the C++ kernel.
// Table capacity must be a power of two comfortably above the distinct count.
func PartitionCXX(keys []int64, batchSize int, capacity int) (codes []uint32, counts []uint64, ndistinct int) {
	codes = make([]uint32, len(keys))
	counts = make([]uint64, capacity)
	slotKeys := make([]int64, capacity)
	slotCodes := make([]uint32, capacity)
	slotState := make([]uint8, capacity)
	capMask := int64(capacity - 1)
	nd := C.int64_t(0)
	for off := 0; off < len(keys); off += batchSize {
		end := off + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		n := end - off
		nd = C.hash_partition_i64(
			(*C.int64_t)(unsafe.Pointer(&keys[off])), C.int64_t(n),
			(*C.uint32_t)(unsafe.Pointer(&codes[off])),
			(*C.int64_t)(unsafe.Pointer(&slotKeys[0])),
			(*C.uint32_t)(unsafe.Pointer(&slotCodes[0])),
			(*C.uint8_t)(unsafe.Pointer(&slotState[0])),
			C.int64_t(capMask), nd,
			(*C.uint64_t)(unsafe.Pointer(&counts[0])))
		if nd < 0 {
			panic("table overflow")
		}
	}
	return codes, counts, int(nd)
}

// Noop measures raw cgo call overhead.
func Noop() { C.noop_kernel() }

func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// PartitionGoOpenAddr is the same algorithm as the C++ kernel in pure Go.
func PartitionGoOpenAddr(keys []int64, capacity int) (codes []uint32, counts []uint64, ndistinct int) {
	codes = make([]uint32, len(keys))
	counts = make([]uint64, capacity)
	slotKeys := make([]int64, capacity)
	slotCodes := make([]uint32, capacity)
	slotState := make([]uint8, capacity)
	capMask := uint64(capacity - 1)
	nd := uint32(0)
	for i, k := range keys {
		h := mix64(uint64(k)) & capMask
		var code uint32
		for {
			if slotState[h] == 0 {
				slotState[h] = 1
				slotKeys[h] = k
				code = nd
				slotCodes[h] = code
				nd++
				break
			}
			if slotKeys[h] == k {
				code = slotCodes[h]
				break
			}
			h = (h + 1) & capMask
		}
		codes[i] = code
		counts[code]++
	}
	return codes, counts, int(nd)
}

// PartitionGoMap is the current in-repo shape: a Go map from key to code.
func PartitionGoMap(keys []int64, distinctHint int) (codes []uint32, counts []uint64, ndistinct int) {
	codes = make([]uint32, len(keys))
	m := make(map[int64]uint32, distinctHint)
	counts = make([]uint64, 0, distinctHint)
	for i, k := range keys {
		code, ok := m[k]
		if !ok {
			code = uint32(len(m))
			m[k] = code
			counts = append(counts, 0)
		}
		codes[i] = code
		counts[code]++
	}
	return codes, counts, len(m)
}
