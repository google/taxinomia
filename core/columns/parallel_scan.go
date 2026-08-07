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
	"context"

	"github.com/google/taxinomia/core/executor"
)

// maxSpanChunks caps how many chunks one work item covers. Batching chunks
// into spans keeps the per-item dispatch overhead negligible on tables with
// thousands of chunks, while the cap keeps one item's work small enough
// (~1M rows) that cancellation, observed between items, stays prompt.
const maxSpanChunks = 16

// disableParallelScan forces forEachChunk sequential. Tests and benchmarks
// only, to compare the two paths on identical data; never set in production
// code.
var disableParallelScan = false

// forEachChunk runs body(ci) for every chunk index in [0, nc), on the global
// executor pool when the column is large enough to benefit, else on the
// calling goroutine. It returns early with ctx.Err() when ctx is cancelled;
// a nil error means body ran for every chunk.
//
// The parallel path requires chunkSize to be a multiple of 64: each chunk's
// rows then cover whole words of a Selection bitmap, so concurrent bodies
// writing their own chunks' rows into one shared Selection touch disjoint
// words and need no locking (the property recorded on FilterSelection since
// chunking was introduced). Every public constructor uses DefaultChunkSize,
// which qualifies; tiny test-only chunk sizes fall back to the sequential
// path.
func forEachChunk(ctx context.Context, nc int, chunkSize int, body func(ci int)) error {
	if nc >= 2 && chunkSize%64 == 0 && !disableParallelScan {
		pool := executor.Default()
		spanLen := (nc + 4*pool.Parallelism() - 1) / (4 * pool.Parallelism())
		if spanLen > maxSpanChunks {
			spanLen = maxSpanChunks
		}
		spans := (nc + spanLen - 1) / spanLen
		return pool.Run(ctx, spans, func(si int) {
			lo := si * spanLen
			hi := lo + spanLen
			if hi > nc {
				hi = nc
			}
			for ci := lo; ci < hi; ci++ {
				body(ci)
			}
		})
	}
	for ci := 0; ci < nc; ci++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		body(ci)
	}
	return nil
}
