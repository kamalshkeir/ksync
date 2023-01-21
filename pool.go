package ksync

import (
	"sync"
)

type ThreadPool[T any] struct {
	chErr      chan T
	taskCh     chan func() T
	workerNum  int
	wg         sync.WaitGroup
	errorsList []T
}

// NewPool creates a new thread pool
func NewPool[T any](workerNum int, taskBufferSize ...int) *ThreadPool[T] {
	bufSize := 5
	if len(taskBufferSize) > 0 {
		bufSize = taskBufferSize[0]
	}
	pool := &ThreadPool[T]{
		taskCh:     make(chan func() T, bufSize),
		workerNum:  workerNum,
		chErr:      make(chan T),
		errorsList: make([]T, 0),
	}

	pool.wg.Add(workerNum)
	go func() {
		for v := range pool.chErr {
			pool.errorsList = append(pool.errorsList, v)
		}
		close(pool.chErr)
	}()
	for i := 0; i < workerNum; i++ {
		go pool.worker()
	}
	return pool
}

func (p *ThreadPool[T]) Errors() []T {
	return p.errorsList
}

// worker function that listens for tasks and executes them
func (p *ThreadPool[T]) worker() {
	defer p.wg.Done()
	for task := range p.taskCh {
		v := task()
		p.chErr <- v
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
// 	pool := New[error](10)
// 	for i := 0; i < 100; i++ {
// 		in := i
// 		pool.Submit(func() error {
// 			ss := 0
// 			fmt.Println("TASK", in)
// 			if in%5 == 0 {
// 				ss = 2
// 			}
// 			time.Sleep(time.Duration(ss) * time.Second)
// 			if in%5 == 0 {
// 				fmt.Println("FINISH SLEEP", in)
// 			}

// 			if in%2 != 0 {
// 				return fmt.Errorf("-------- error %d", in)
// 			}

// 			return nil
// 		})
// 	}
// 	pool.Wait()
// 	errs := pool.Errors()
// 	if len(errs) > 0 {
// 		fmt.Println("err:", len(errs))
// 		fmt.Println("err list:", errs)
// 	}
// }
