//go:build !race

package tables

// raceEnabled reports whether the race detector is active; some tests that
// measure memory skip themselves under it (shadow allocations skew the
// numbers and exhaust the Windows pagefile at 1M rows).
const raceEnabled = false
