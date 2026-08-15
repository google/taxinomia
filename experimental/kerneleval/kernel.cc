// C++ side of the phase-7 kernel evaluation.
// Hash-partition a batch of int64 keys into dense first-appearance codes,
// accumulating per-code counts. Open addressing, linear probing, power-of-two
// capacity, splitmix64 finalizer as the hash. Table state lives in caller
// (Go) memory so calls are batch-granularity and stateless on the C++ side.
#include <cstdint>

static inline uint64_t mix64(uint64_t x) {
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

extern "C" {

// slot_state: 0 = empty, 1 = used. cap_mask = capacity-1 (capacity a power of
// two, > n_distinct_max). Returns the updated number of distinct codes, or -1
// if the table overflows.
int64_t hash_partition_i64(const int64_t* keys, int64_t n, uint32_t* codes_out,
                           int64_t* slot_keys, uint32_t* slot_codes,
                           uint8_t* slot_state, int64_t cap_mask,
                           int64_t ndistinct, uint64_t* counts) {
  for (int64_t i = 0; i < n; i++) {
    int64_t k = keys[i];
    uint64_t h = mix64((uint64_t)k) & (uint64_t)cap_mask;
    uint32_t code;
    for (;;) {
      if (!slot_state[h]) {
        if (ndistinct > cap_mask) return -1;
        slot_state[h] = 1;
        slot_keys[h] = k;
        code = (uint32_t)ndistinct++;
        slot_codes[h] = code;
        break;
      }
      if (slot_keys[h] == k) {
        code = slot_codes[h];
        break;
      }
      h = (h + 1) & (uint64_t)cap_mask;
    }
    codes_out[i] = code;
    counts[code]++;
  }
  return ndistinct;
}

// Empty kernel to measure raw cgo call overhead.
void noop_kernel(void) {}

}  // extern "C"
