package ksync

import (
	"fmt"
	"sync"
	"time"
)

// Completer is a channel that the future expects to receive
// a result on.  The future only receives on this channel.
type Completer <-chan interface{}

// Future represents an object that can be used to perform asynchronous
// tasks.  The constructor of the future will complete it, and listeners
// will block on getresult until a result is received.  This is different
// from a channel in that the future is only completed once, and anyone
// listening on the future will get the result, regardless of the number
// of listeners.
type Future struct {
	triggered bool // because item can technically be nil and still be valid
	item      interface{}
	err       error
	lock      sync.Mutex
	wg        sync.WaitGroup
}

// AwaitResult will immediately fetch the result if it exists
// or wait on the result until it is ready.
func (f *Future) AwaitResult() (interface{}, error) {
	f.lock.Lock()
	if f.triggered {
		f.lock.Unlock()
		return f.item, f.err
	}
	f.lock.Unlock()

	f.wg.Wait()
	return f.item, f.err
}

// HasResult will return true iff the result exists
func (f *Future) HasResult() bool {
	f.lock.Lock()
	hasResult := f.triggered
	f.lock.Unlock()
	return hasResult
}

func (f *Future) setItem(item interface{}, err error) {
	f.lock.Lock()
	f.triggered = true
	f.item = item
	f.err = err
	f.lock.Unlock()
	f.wg.Done()
}

func listenForResult(f *Future, ch Completer, timeout time.Duration, wg *sync.WaitGroup) {
	wg.Done()
	t := time.NewTimer(timeout)
	select {
	case item := <-ch:
		f.setItem(item, nil)
		t.Stop() // we want to trigger GC of this timer as soon as it's no longer needed
	case <-t.C:
		f.setItem(nil, fmt.Errorf(`timeout after %f seconds`, timeout.Seconds()))
	}
}

// New is the constructor to generate a new future.  Pass the completed
// item to the toComplete channel and any listeners will get
// notified.  If timeout is hit before toComplete is called,
// any listeners will get passed an error.
func NewFuture(completer Completer, timeout time.Duration) *Future {
	f := &Future{}
	f.wg.Add(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go listenForResult(f, completer, timeout, &wg)
	wg.Wait()
	return f
}

// EXAMPLE:

// func main() {
// 	// Create a completer channel
// 	completer := make(chan interface{})

// 	// Create a new Future with a 1-second timeout
// 	future := ksync.NewFuture(completer, time.Second)

// 	// Perform some asynchronous task
// 	go func() {
// 		// Simulate some work
// 		time.Sleep(500 * time.Millisecond)

// 		// Complete the completer channel with a result
// 		completer <- "Hello, Future!"
// 	}()

// 	// Wait for the result
// 	result, err := future.GetResult()
// 	if err != nil {
// 		fmt.Println("Error:", err)
// 	} else {
// 		fmt.Println("Result:", result)
// 	}
// }
