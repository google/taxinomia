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

package executor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRunAllItemsExactlyOnce: every item index runs exactly once.
func TestRunAllItemsExactlyOnce(t *testing.T) {
	p := NewPool(3)
	defer p.Close()
	const n = 1000
	counts := make([]int32, n)
	if err := p.Run(context.Background(), n, func(i int) {
		atomic.AddInt32(&counts[i], 1)
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, c := range counts {
		if c != 1 {
			t.Fatalf("item %d ran %d times, want 1", i, c)
		}
	}
}

// TestRunZeroItems: n=0 completes immediately without error.
func TestRunZeroItems(t *testing.T) {
	p := NewPool(2)
	defer p.Close()
	if err := p.Run(context.Background(), 0, func(i int) {
		t.Error("task ran for n=0")
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestConcurrencyBounded: a pool with w workers runs at most w+1 items of a
// single job at once (workers plus the submitting goroutine).
func TestConcurrencyBounded(t *testing.T) {
	const workers = 2
	p := NewPool(workers)
	defer p.Close()
	var cur, max int32
	if err := p.Run(context.Background(), 64, func(i int) {
		c := atomic.AddInt32(&cur, 1)
		for {
			m := atomic.LoadInt32(&max)
			if c <= m || atomic.CompareAndSwapInt32(&max, m, c) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		atomic.AddInt32(&cur, -1)
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := atomic.LoadInt32(&max); got > workers+1 {
		t.Fatalf("observed %d concurrent items, want <= %d", got, workers+1)
	}
}

// TestPerJobFairness: a short job submitted while a long job occupies the
// pool is served before the long job finishes — workers rotate over jobs
// instead of draining the first one.
func TestPerJobFairness(t *testing.T) {
	p := NewPool(2)
	defer p.Close()

	var aDone, bDoneBeforeAFinished int32
	aStarted := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		p.Run(context.Background(), 200, func(i int) {
			once.Do(func() { close(aStarted) })
			time.Sleep(time.Millisecond)
			atomic.AddInt32(&aDone, 1)
		})
	}()
	go func() {
		defer wg.Done()
		<-aStarted
		p.Run(context.Background(), 4, func(i int) {
			time.Sleep(time.Millisecond)
		})
		if atomic.LoadInt32(&aDone) < 200 {
			atomic.StoreInt32(&bDoneBeforeAFinished, 1)
		}
	}()
	wg.Wait()
	if bDoneBeforeAFinished != 1 {
		t.Fatal("short job B finished only after long job A drained completely; want interleaving")
	}
}

// TestCancellationStopsNewItems: after the context is cancelled, no new
// items start, Run returns ctx.Err(), and in-flight items finish.
func TestCancellationStopsNewItems(t *testing.T) {
	p := NewPool(2)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	var started, finished int32
	const n = 1000
	err := p.Run(ctx, n, func(i int) {
		atomic.AddInt32(&started, 1)
		if atomic.LoadInt32(&started) == 3 {
			cancel()
		}
		time.Sleep(time.Millisecond)
		atomic.AddInt32(&finished, 1)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
	s, f := atomic.LoadInt32(&started), atomic.LoadInt32(&finished)
	if s != f {
		t.Fatalf("started %d != finished %d: Run returned with items in flight", s, f)
	}
	if s >= n {
		t.Fatalf("all %d items ran despite cancellation", n)
	}
}

// TestPreCancelledContext: Run with an already-cancelled context runs
// nothing.
func TestPreCancelledContext(t *testing.T) {
	p := NewPool(2)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.Run(ctx, 10, func(i int) {
		t.Error("task ran under a pre-cancelled context")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

// TestPanicPropagates: a task panic re-panics in the Run caller with the
// original value, stops the job, and leaves the pool usable.
func TestPanicPropagates(t *testing.T) {
	p := NewPool(2)
	defer p.Close()
	func() {
		defer func() {
			r := recover()
			if r != "boom" {
				t.Fatalf("recovered %v, want \"boom\"", r)
			}
		}()
		p.Run(context.Background(), 100, func(i int) {
			if i == 5 {
				panic("boom")
			}
			time.Sleep(100 * time.Microsecond)
		})
		t.Fatal("Run returned instead of panicking")
	}()
	// The pool survives and serves the next job.
	var ran int32
	if err := p.Run(context.Background(), 8, func(i int) {
		atomic.AddInt32(&ran, 1)
	}); err != nil {
		t.Fatalf("Run after panic: %v", err)
	}
	if ran != 8 {
		t.Fatalf("job after panic ran %d of 8 items", ran)
	}
}

// TestNestedRun: a task may itself call Run — the submitting goroutine
// drains its own job, so this cannot deadlock even on a single-worker pool.
func TestNestedRun(t *testing.T) {
	p := NewPool(1)
	defer p.Close()
	var inner int32
	done := make(chan error, 1)
	go func() {
		done <- p.Run(context.Background(), 4, func(i int) {
			p.Run(context.Background(), 4, func(k int) {
				atomic.AddInt32(&inner, 1)
			})
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nested Run deadlocked")
	}
	if inner != 16 {
		t.Fatalf("inner items ran %d times, want 16", inner)
	}
}

// TestManyConcurrentRuns: many goroutines submitting jobs at once all
// complete with every item run (exercised further under -race).
func TestManyConcurrentRuns(t *testing.T) {
	p := NewPool(3)
	defer p.Close()
	const jobs, items = 16, 100
	var total int32
	var wg sync.WaitGroup
	wg.Add(jobs)
	for g := 0; g < jobs; g++ {
		go func() {
			defer wg.Done()
			p.Run(context.Background(), items, func(i int) {
				atomic.AddInt32(&total, 1)
			})
		}()
	}
	wg.Wait()
	if total != jobs*items {
		t.Fatalf("ran %d items, want %d", total, jobs*items)
	}
}

// TestDefaultPool: the process pool exists, has at least one worker, and
// works.
func TestDefaultPool(t *testing.T) {
	p := Default()
	if p.Parallelism() < 2 {
		t.Fatalf("Default().Parallelism() = %d, want >= 2", p.Parallelism())
	}
	if p != Default() {
		t.Fatal("Default() returned two different pools")
	}
	var ran int32
	if err := p.Run(context.Background(), 5, func(i int) {
		atomic.AddInt32(&ran, 1)
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ran != 5 {
		t.Fatalf("ran %d of 5 items", ran)
	}
}
