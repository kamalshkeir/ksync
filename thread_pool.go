package ksync

import (
	"sync"
)

// Task represents a function to be executed by the thread pool.
type Task[T any] func() T

// ThreadPool represents a pool of goroutines that can execute tasks concurrently.
type ThreadPool[T any] struct {
	size    int
	workers int
	queue   chan Task[T]
	wg      sync.WaitGroup
	mu      sync.RWMutex
	results []T
}

// NewThreadPool creates a new ThreadPool with the specified number of workers and queue size.
func NewThreadPool[T any](workers, queueSize int) *ThreadPool[T] {
	if queueSize <= 0 {
		queueSize = 5
	}
	if workers <= 0 {
		workers = 1
	}
	pool := &ThreadPool[T]{
		size:    queueSize,
		workers: workers,
		queue:   make(chan Task[T], queueSize),
	}
	pool.start()
	return pool
}

// Submit submits a task to the thread pool for execution.
func (p *ThreadPool[T]) Submit(task Task[T]) {
	p.queue <- task
}

// Wait waits for all tasks to be completed.
func (p *ThreadPool[T]) Wait() {
	close(p.queue)
	p.wg.Wait()
}

// Data returns the results of all executed tasks.
func (p *ThreadPool[T]) Data() []T {
	return p.results
}

// start starts the worker goroutines.
func (p *ThreadPool[T]) start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for task := range p.queue {
				result := task()
				if (any(result)) != nil {
					p.mu.Lock()
					p.results = append(p.results, result)
					p.mu.Unlock()
				}
			}
		}()
	}
}

// EXAMPLE:

// func main() {
// 	// Create a worker pool with 3 workers
// 	tp := ksync.NewThreadPool[ksync.JobResult[string]](10, 10)

// 	for i := 0; i < 10; i++ {
// 		in := i
// 		tp.Submit(func() ksync.JobResult[string] {
// 			time.Sleep(time.Second)
// 			fmt.Println(in, "finished")
// 			return ksync.JobResult[string]{
// 				WorkerID: in,
// 				JobID:    fmt.Sprintf("Job--%d", in),
// 				Result:   "done",
// 			}
// 		})
// 	}
// 	tp.Wait()
// 	for _, res := range tp.Data() {
// 		fmt.Printf("got:: %+v\n", res)
// 	}
// }
