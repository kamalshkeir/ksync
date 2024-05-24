package ksync

import (
	"sync"
)

type CapWait struct {
	sem chan struct{}
	wg  sync.WaitGroup
}

func NewCapWait(max int) *CapWait {
	return &CapWait{
		sem: make(chan struct{}, max),
	}
}

func (cw *CapWait) Add(i int) {
	for j := 0; j < i; j++ {
		cw.sem <- struct{}{} // Acquire a semaphore slot
		cw.wg.Add(1)
	}
}

func (cw *CapWait) Done() {
	<-cw.sem // Release a semaphore slot
	cw.wg.Done()
}

func (cw *CapWait) Wait() {
	cw.wg.Wait()
}

// EXAMPLE:

// func main() {
// 	// Create a worker pool with 3 workers
// 	wg := ksync.NewCapWait(4)

// 	for i := 0; i < 10; i++ {
// 		wg.Add(1)
// 		in := i
// 		go func(wg *ksync.CapWait) {
// 			time.Sleep(time.Second)
// 			fmt.Println(in, "finished")
// 			wg.Done()
// 		}(wg)
// 	}
// 	wg.Wait()
// }
