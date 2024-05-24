package ksync

import "sync"

var pool = sync.Pool{}

// Set is an implementation of ISet using the builtin map type. Set is threadsafe.
type Set struct {
	items     map[interface{}]struct{}
	lock      sync.RWMutex
	flattened []interface{}
}

// Add will add the provided items to the set.
func (set *Set) Add(items ...interface{}) {
	set.lock.Lock()
	defer set.lock.Unlock()

	set.flattened = nil
	for _, item := range items {
		set.items[item] = struct{}{}
	}
}

// Remove will remove the given items from the set.
func (set *Set) Remove(items ...interface{}) {
	set.lock.Lock()
	defer set.lock.Unlock()

	set.flattened = nil
	for _, item := range items {
		delete(set.items, item)
	}
}

// Exists returns a bool indicating if the given item exists in the set.
func (set *Set) Exists(item interface{}) bool {
	set.lock.RLock()

	_, ok := set.items[item]

	set.lock.RUnlock()

	return ok
}

// Flatten will return a list of the items in the set.
func (set *Set) Flatten() []interface{} {
	set.lock.Lock()
	defer set.lock.Unlock()

	if set.flattened != nil {
		return set.flattened
	}

	set.flattened = make([]interface{}, 0, len(set.items))
	for item := range set.items {
		set.flattened = append(set.flattened, item)
	}
	return set.flattened
}

// Len returns the number of items in the set.
func (set *Set) Len() int64 {
	set.lock.RLock()

	size := int64(len(set.items))

	set.lock.RUnlock()

	return size
}

// Clear will remove all items from the set.
func (set *Set) Clear() {
	set.lock.Lock()

	set.items = map[interface{}]struct{}{}

	set.lock.Unlock()
}

// All returns a bool indicating if all of the supplied items exist in the set.
func (set *Set) All(items ...interface{}) bool {
	set.lock.RLock()
	defer set.lock.RUnlock()

	for _, item := range items {
		if _, ok := set.items[item]; !ok {
			return false
		}
	}

	return true
}

// Dispose will add this set back into the pool.
func (set *Set) Dispose() {
	set.lock.Lock()
	defer set.lock.Unlock()

	for k := range set.items {
		delete(set.items, k)
	}

	//this is so we don't hang onto any references
	for i := 0; i < len(set.flattened); i++ {
		set.flattened[i] = nil
	}

	set.flattened = set.flattened[:0]
	pool.Put(set)
}

// NewSet is the constructor for sets. It will pull from a reuseable memory pool if it can.
// Takes a list of items to initialize the set with.
func NewSet(items ...interface{}) *Set {
	set := pool.Get().(*Set)
	for _, item := range items {
		set.items[item] = struct{}{}
	}

	if len(items) > 0 {
		set.flattened = nil
	}

	return set
}

func init() {
	pool.New = func() interface{} {
		return &Set{
			items: make(map[interface{}]struct{}, 10),
		}
	}
}

// EXAMPLE:

// func main() {
// 	// Create a new set with initial values
// 	mySet := set.NewSet("apple", "banana", "cherry")

// 	// Adding new items to the set
// 	mySet.Add("orange")
// 	fmt.Println(mySet.Flatten()) // Output: ["apple", "banana", "cherry", "orange"]

// 	// Checking if an item exists
// 	exists := mySet.Exists("banana")
// 	fmt.Println(exists) // Output: true

// 	// Removing an item
// 	mySet.Remove("cherry")
// 	fmt.Println(mySet.Flatten()) // Output: ["apple", "banana", "orange"]

// 	// Concurrent modification example
// 	var wg sync.WaitGroup
// 	wg.Add(2)

// 	go func() {
// 		defer wg.Done()
// 		mySet.Add("grape") // Adding an item concurrently
// 	}()

// 	go func() {
// 		defer wg.Done()
// 		mySet.Remove("banana") // Removing an item concurrently
// 	}()

// 	wg.Wait()

// 	fmt.Println(mySet.Flatten()) // Output: ["apple", "orange", "grape"]
// 	fmt.Println(mySet.Len())     // Output: 3

// 	// Disposing the set back into the pool
// 	mySet.Dispose()
// 	fmt.Println("disposed")
// 	fmt.Scanln()
// }
