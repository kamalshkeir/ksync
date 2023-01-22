package ksync

import (
	"sync"
)

type ThreadPool[T any] struct {
	chData       chan T
	taskCh       chan func() T
	workerNum    int
	wg           sync.WaitGroup
	returnedList []T
}

// NewPool creates a new thread pool
func NewPool[T any](workerNum int, taskBufferSize ...int) *ThreadPool[T] {
	bufSize := 5
	if len(taskBufferSize) > 0 {
		bufSize = taskBufferSize[0]
	}
	pool := &ThreadPool[T]{
		taskCh:       make(chan func() T, bufSize),
		workerNum:    workerNum,
		chData:       make(chan T),
		returnedList: make([]T, 0),
	}

	pool.wg.Add(workerNum)
	go func() {
		for v := range pool.chData {
			pool.returnedList = append(pool.returnedList, v)
		}
		close(pool.chData)
	}()
	for i := 0; i < workerNum; i++ {
		go pool.worker()
	}
	return pool
}

func (p *ThreadPool[T]) Data() []T {
	return p.returnedList
}

// worker function that listens for tasks and executes them
func (p *ThreadPool[T]) worker() {
	defer p.wg.Done()
	for task := range p.taskCh {
		v := task()
		p.chData <- v
	}
}

// Submit adds a new task to the thread pool
func (p *ThreadPool[T]) Submit(task func() T) {
	p.taskCh <- task
}

// Wait waits for all goroutines to complete
func (p *ThreadPool[T]) Wait() {
	close(p.taskCh)
	p.wg.Wait()
}

// Example

// func main() {
// 	pool := NewPool[error](10)
// 	for i := 0; i < 100; i++ {
// 		in := i
// 		pool.Submit(func() error {
// 			fmt.Println("TASK", in)
// 			return nil
// 		})
// 	}
// 	pool.Wait()
// 	errs := pool.Data()
// 	if len(errs) > 0 {
// 		fmt.Println("err:", len(errs))
// 		fmt.Println("err list:", errs)
// 	}
// }
