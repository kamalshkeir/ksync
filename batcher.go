/*
Copyright 2015 Workiva, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package ksync

import (
	"errors"
	"sync/atomic"
	"time"
)

type mutex struct {
	state int32 // 0 for unlocked, 1 for locked
}

func NewTryMutex() *mutex {
	return &mutex{}
}

func (m *mutex) Lock() {
	for !atomic.CompareAndSwapInt32(&m.state, 0, 1) {
		// Wait until the lock is acquired
	}
}

func (m *mutex) Unlock() {
	atomic.StoreInt32(&m.state, 0)
}

func (m *mutex) TryLock() bool {
	return atomic.CompareAndSwapInt32(&m.state, 0, 1)
}

// Batcher provides an API for accumulating items into a batch for processing.
type Batcher interface {
	// Put adds items to the batcher.
	Put(interface{}) error

	// Get retrieves a batch from the batcher. This call will block until
	// one of the conditions for a "complete" batch is reached.
	Get() ([]interface{}, error)

	// Flush forcibly completes the batch currently being built
	Flush() error

	// Dispose will dispose of the batcher. Any calls to Put or Flush
	// will return ErrDisposed, calls to Get will return an error iff
	// there are no more ready batches.
	Dispose()

	// IsDisposed will determine if the batcher is disposed
	IsDisposed() bool
}

// ErrDisposed is the error returned for a disposed Batcher
var ErrDisposed = errors.New("batcher: disposed")

// CalculateBytes evaluates the number of bytes in an item added to a Batcher.
type CalculateBytes func(interface{}) uint

type basicBatcher struct {
	maxTime        time.Duration
	maxItems       uint
	maxBytes       uint
	calculateBytes CalculateBytes
	disposed       bool
	items          []interface{}
	batchChan      chan []interface{}
	availableBytes uint
	lock           *mutex
}

// NewBatcher creates a new Batcher using the provided arguments.
// Batch readiness can be determined in three ways:
//   - Maximum number of bytes per batch
//   - Maximum number of items per batch
//   - Maximum amount of time waiting for a batch
//
// Values of zero for one of these fields indicate they should not be
// taken into account when evaluating the readiness of a batch.
// This provides an ordering guarantee for any given thread such that if a
// thread places two items in the batcher, Get will guarantee the first
// item is returned before the second, whether before the second in the same
// batch, or in an earlier batch.
func NewBatcher(maxTime time.Duration, maxItems, maxBytes, queueLen uint, calculate CalculateBytes) (Batcher, error) {
	if maxBytes > 0 && calculate == nil {
		return nil, errors.New("batcher: must provide CalculateBytes function")
	}

	return &basicBatcher{
		maxTime:        maxTime,
		maxItems:       maxItems,
		maxBytes:       maxBytes,
		calculateBytes: calculate,
		items:          make([]interface{}, 0, maxItems),
		batchChan:      make(chan []interface{}, queueLen),
		lock:           NewTryMutex(),
	}, nil
}

// Put adds items to the batcher.
func (b *basicBatcher) Put(item interface{}) error {
	b.lock.Lock()
	if b.disposed {
		b.lock.Unlock()
		return ErrDisposed
	}

	b.items = append(b.items, item)
	if b.calculateBytes != nil {
		b.availableBytes += b.calculateBytes(item)
	}
	if b.ready() {
		// To guarantee ordering this MUST be in the lock, otherwise multiple
		// flush calls could be blocked at the same time, in which case
		// there's no guarantee each batch is placed into the channel in
		// the proper order
		b.flush()
	}

	b.lock.Unlock()
	return nil
}

// Get retrieves a batch from the batcher. This call will block until
// one of the conditions for a "complete" batch is reached.
func (b *basicBatcher) Get() ([]interface{}, error) {
	// Don't check disposed yet so any items remaining in the queue
	// will be returned properly.

	var timeout <-chan time.Time
	if b.maxTime > 0 {
		timeout = time.After(b.maxTime)
	}

	select {
	case items, ok := <-b.batchChan:
		// If there's something on the batch channel, we definitely want that.
		if !ok {
			return nil, ErrDisposed
		}
		return items, nil
	case <-timeout:
		// It's possible something was added to the channel after something
		// was received on the timeout channel, in which case that must
		// be returned first to satisfy our ordering guarantees.
		// We can't just grab the lock here in case the batch channel is full,
		// in which case a Put or Flush will be blocked and holding
		// onto the lock. In that case, there should be something on the
		// batch channel
		for {
			if b.lock.TryLock() {
				// We have a lock, try to read from channel first in case
				// something snuck in
				select {
				case items, ok := <-b.batchChan:
					b.lock.Unlock()
					if !ok {
						return nil, ErrDisposed
					}
					return items, nil
				default:
				}

				// If that is unsuccessful, nothing was added to the channel,
				// and the temp buffer can't have changed because of the lock,
				// so grab that
				items := b.items
				b.items = make([]interface{}, 0, b.maxItems)
				b.availableBytes = 0
				b.lock.Unlock()
				return items, nil
			} else {
				// If we didn't get a lock, there are two cases:
				// 1) The batch chan is full.
				// 2) A Put or Flush temporarily has the lock.
				// In either case, trying to read something off the batch chan,
				// and going back to trying to get a lock if unsuccessful
				// works.
				select {
				case items, ok := <-b.batchChan:
					if !ok {
						return nil, ErrDisposed
					}
					return items, nil
				default:
				}
			}
		}
	}
}

// Flush forcibly completes the batch currently being built
func (b *basicBatcher) Flush() error {
	// This is the same pattern as a Put
	b.lock.Lock()
	if b.disposed {
		b.lock.Unlock()
		return ErrDisposed
	}
	b.flush()
	b.lock.Unlock()
	return nil
}

// Dispose will dispose of the batcher. Any calls to Put or Flush
// will return ErrDisposed, calls to Get will return an error iff
// there are no more ready batches. Any items not flushed and retrieved
// by a Get may or may not be retrievable after calling this.
func (b *basicBatcher) Dispose() {
	for {
		if b.lock.TryLock() {
			// We've got a lock
			if b.disposed {
				b.lock.Unlock()
				return
			}

			b.disposed = true
			b.items = nil
			b.drainBatchChan()
			close(b.batchChan)
			b.lock.Unlock()
		} else {
			// Two cases here:
			// 1) Something is blocked and holding onto the lock
			// 2) Something temporarily has a lock
			// For case 1, we have to clear at least some space so the blocked
			// Put/Flush can release the lock. For case 2, nothing bad
			// will happen here
			b.drainBatchChan()
		}

	}
}

// IsDisposed will determine if the batcher is disposed
func (b *basicBatcher) IsDisposed() bool {
	b.lock.Lock()
	disposed := b.disposed
	b.lock.Unlock()
	return disposed
}

// flush adds the batch currently being built to the queue of completed batches.
// flush is not threadsafe, so should be synchronized externally.
func (b *basicBatcher) flush() {
	b.batchChan <- b.items
	b.items = make([]interface{}, 0, b.maxItems)
	b.availableBytes = 0
}

func (b *basicBatcher) ready() bool {
	if b.maxItems != 0 && uint(len(b.items)) >= b.maxItems {
		return true
	}
	if b.maxBytes != 0 && b.availableBytes >= b.maxBytes {
		return true
	}
	return false
}

func (b *basicBatcher) drainBatchChan() {
	for {
		select {
		case <-b.batchChan:
		default:
			return
		}
	}
}

// Example:

// func processRequest(req interface{}) {
// 	// Simulate processing time
// 	time.Sleep(1 * time.Second)
// 	fmt.Println("Processed request:", req)
// }

// func main() {
// 	// Create a new Batcher with a maximum time of 100 milliseconds,
// 	// maximum 5 items per batch, maximum 500 bytes per batch, and a function to calculate the number of bytes for each item.
// 	b, err := batcher.New(100*time.Millisecond, 10, 50, 10, func(item interface{}) uint {
// 		// For simplicity, assume each item has a fixed size of 100 bytes.
// 		return 100
// 	})
// 	if err != nil {
// 		// Handle error
// 		fmt.Println("Error:", err)
// 		return
// 	}

// 	// Channel to wait for batches to be processed
// 	var wg sync.WaitGroup

// 	// Goroutine to continuously process batches
// 	go func() {
// 		for {
// 			// Wait for a batch to be ready
// 			items, err := b.Get()
// 			if err != nil {
// 				// Handle error
// 				fmt.Println("Error:", err)
// 				return
// 			}

// 			// Process the batch concurrently
// 			wg.Add(1)
// 			go func(batch []interface{}) {
// 				defer wg.Done()
// 				for _, item := range batch {
// 					processRequest(item)
// 				}
// 			}(items)
// 		}
// 	}()

// 	// Simulate receiving requests and putting them into the batcher
// 	for i := 1; i <= 20; i++ {
// 		req := fmt.Sprintf("Request %d", i)
// 		fmt.Println("Received request:", req)
// 		err := b.Put(req)
// 		if err != nil {
// 			// Handle error
// 			fmt.Println("Error:", err)
// 		}
// 	}

// 	// Wait for all batches to be processed
// 	wg.Wait()

// 	// Dispose the batcher when done
// 	b.Dispose()
// }
