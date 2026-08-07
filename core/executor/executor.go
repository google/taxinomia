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

// Package executor provides the process-wide worker pool for per-chunk query
// work (docs/scaling-to-1b-rows.md §8). The rules it exists to enforce:
//
//   - One global pool, not one per query. Concurrent queries share a fixed
//     set of workers instead of each spawning GOMAXPROCS goroutines and
//     oversubscribing the machine.
//   - Per-query fairness. Workers hand out items round-robin across the
//     active jobs, so a short query is served while a long one is running,
//     and every submitting goroutine additionally works on its own job, so
//     no query is starved however busy the pool is.
//   - Everything is cancellable. The pool checks a job's context before
//     handing out each work item, so a superseded query stops competing for
//     cores at chunk granularity rather than running to completion.
package executor

import (
	"context"
	"runtime"
	"sync"
)

// Pool is a fixed set of worker goroutines shared by all queries. Use
// Default for the process-wide pool; NewPool exists for tests that need a
// pool with a known size or lifetime.
type Pool struct {
	mu      sync.Mutex
	cond    *sync.Cond // signals workers: new work or Close
	jobs    []*job     // active jobs, served round-robin
	cursor  int        // next job index to serve (fairness rotation)
	workers int
	closed  bool
	wg      sync.WaitGroup // running workers, for Close
}

// job is one Run call: n items of one task, one context.
type job struct {
	ctx  context.Context
	task func(int)
	n    int // total items

	// All below are guarded by the pool mutex.
	next     int  // next item index to hand out
	done     int  // handed-out items that have finished
	stopped  bool // context cancelled or task panicked: hand out no more items
	panicked bool
	panicVal any
	finished bool          // waiter closed
	waiter   chan struct{} // closed when no more items will start and all started items finished
}

// NewPool returns a pool running the given number of worker goroutines
// (minimum one). Callers of Run participate in their own jobs, so the
// concurrency of a single Run is workers+1.
func NewPool(workers int) *Pool {
	if workers < 1 {
		workers = 1
	}
	p := &Pool{workers: workers}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

var (
	defaultPool *Pool
	defaultOnce sync.Once
)

// Default returns the process-wide pool, created on first use with
// GOMAXPROCS-1 workers (minimum one): with the submitting goroutine working
// too, one query saturates the machine without oversubscribing it.
func Default() *Pool {
	defaultOnce.Do(func() {
		defaultPool = NewPool(runtime.GOMAXPROCS(0) - 1)
	})
	return defaultPool
}

// Parallelism returns the number of goroutines that can work on one Run
// call: the pool's workers plus the submitting goroutine.
func (p *Pool) Parallelism() int {
	return p.workers + 1
}

// Run executes task(0) … task(n-1) on the pool and the calling goroutine,
// returning when every started item has finished. Before each item starts,
// the job's context is checked: once it is cancelled no further items start
// (items already running are not interrupted — tasks that run long per item
// observe ctx themselves) and Run returns ctx.Err(). If a task panics, no
// further items start and Run re-panics with the task's panic value after
// the in-flight items finish.
//
// Items of concurrent Run calls are interleaved fairly; the calling
// goroutine works only on its own job, so a Run call makes progress even
// when every worker is busy elsewhere — which also makes Run safe to call
// from inside a task.
func (p *Pool) Run(ctx context.Context, n int, task func(int)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n <= 0 {
		return nil
	}
	j := &job{ctx: ctx, task: task, n: n, waiter: make(chan struct{})}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		panic("executor: Run on a closed Pool")
	}
	p.jobs = append(p.jobs, j)
	p.mu.Unlock()
	p.cond.Broadcast()

	// Work on our own job until it has no more items to hand out.
	p.mu.Lock()
	for !j.stopped && j.next < j.n {
		if j.ctx.Err() != nil {
			j.stopped = true
			break
		}
		i := j.next
		j.next++
		p.mu.Unlock()
		p.runItem(j, i)
		p.mu.Lock()
	}
	p.maybeFinish(j)
	p.mu.Unlock()

	<-j.waiter

	p.mu.Lock()
	p.removeJob(j)
	p.mu.Unlock()

	if j.panicked {
		panic(j.panicVal)
	}
	if j.stopped {
		if err := j.ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

// worker is the loop of one pool goroutine: take the next item round-robin
// across active jobs, run it, repeat; block when there is nothing to do.
func (p *Pool) worker() {
	defer p.wg.Done()
	p.mu.Lock()
	for {
		if p.closed {
			p.mu.Unlock()
			return
		}
		j, i, ok := p.take()
		if !ok {
			p.cond.Wait()
			continue
		}
		p.mu.Unlock()
		p.runItem(j, i)
		p.mu.Lock()
	}
}

// take hands out the next work item, rotating over the active jobs so that
// every job gets service (per-query fairness). Jobs whose context is
// cancelled are stopped instead of served. Called with the mutex held.
func (p *Pool) take() (*job, int, bool) {
	for k := 0; k < len(p.jobs); k++ {
		j := p.jobs[(p.cursor+k)%len(p.jobs)]
		if !j.stopped && j.ctx.Err() != nil {
			j.stopped = true
			p.maybeFinish(j)
		}
		if j.stopped || j.next >= j.n {
			continue
		}
		i := j.next
		j.next++
		p.cursor = (p.cursor + k + 1) % len(p.jobs)
		return j, i, true
	}
	return nil, 0, false
}

// runItem runs one item and records its completion, capturing a panic into
// the job (the first one wins) and stopping the job on panic. Called
// without the mutex.
func (p *Pool) runItem(j *job, i int) {
	defer func() {
		r := recover()
		p.mu.Lock()
		if r != nil {
			if !j.panicked {
				j.panicked = true
				j.panicVal = r
			}
			j.stopped = true
		}
		j.done++
		p.maybeFinish(j)
		p.mu.Unlock()
	}()
	j.task(i)
}

// maybeFinish closes the job's waiter once no more items will be handed out
// (all handed out, or the job stopped) and every handed-out item has
// finished. Called with the mutex held.
func (p *Pool) maybeFinish(j *job) {
	if j.finished {
		return
	}
	if (j.stopped || j.next >= j.n) && j.done == j.next {
		j.finished = true
		close(j.waiter)
	}
}

// removeJob drops a finished job from the active list. Called with the
// mutex held.
func (p *Pool) removeJob(j *job) {
	for k, other := range p.jobs {
		if other == j {
			p.jobs = append(p.jobs[:k], p.jobs[k+1:]...)
			if p.cursor > k {
				p.cursor--
			}
			if len(p.jobs) > 0 {
				p.cursor %= len(p.jobs)
			} else {
				p.cursor = 0
			}
			return
		}
	}
}

// Close stops the pool's workers after their current items finish. Only
// tests need it — the Default pool lives for the process. Run must not be
// called after Close; jobs already submitted are drained by their callers.
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cond.Broadcast()
	p.wg.Wait()
}
