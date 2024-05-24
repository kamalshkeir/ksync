package ksync

import (
	"hash/fnv"
	"sync"
)

// Job represents the task to be executed by a worker
type Job[T any] struct {
	Debug    bool
	ID       string
	Job      func(workerID int) (T, error)
	OnStart  func(workerID int, jobID string)
	OnFinish func(res T, err error, workerID int, jobID string)
}

// Worker represents a worker that executes jobs
type Worker[T any] struct {
	active   bool
	ID       int
	stopOnce sync.Once
	resChan  chan JobResult[T]
	jobs     chan Job[T]
	stop     chan struct{}
	wg       *sync.WaitGroup
}

// NewWorkerPool creates a new worker pool with the specified number of workers
func NewWorkerPool[T any](workerCount int) *WorkerPool[T] {
	if workerCount <= 0 {
		workerCount = 1
	}
	resChan := make(chan JobResult[T], workerCount)
	pool := &WorkerPool[T]{
		stop:    make(chan struct{}),
		resChan: resChan,
	}
	for i := 1; i <= workerCount; i++ {
		worker := NewWorker[T](i, resChan)
		pool.workers = append(pool.workers, worker)
		worker.Run()
	}
	return pool
}

func (wp *WorkerPool[T]) OnResult(fn func(res JobResult[T])) {
	go func() {
		for v := range wp.resChan {
			fn(v)
		}
	}()
}

// NewWorker creates a new worker
func NewWorker[T any](ID int, resChan chan JobResult[T]) *Worker[T] {
	return &Worker[T]{
		ID:      ID,
		jobs:    make(chan Job[T]),
		stop:    make(chan struct{}),
		active:  true,
		resChan: resChan,
		wg:      &sync.WaitGroup{},
	}
}

type JobResult[T any] struct {
	WorkerID int
	Err      error
	JobID    string
	Result   T
}

// Run starts the worker
func (w *Worker[T]) Run() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case job := <-w.jobs:
				if job.OnStart != nil {
					job.OnStart(w.ID, job.ID)
				}
				res, err := job.Job(w.ID)
				if w.resChan != nil {
					go func() {
						w.resChan <- JobResult[T]{
							Result:   res,
							Err:      err,
							WorkerID: w.ID,
							JobID:    job.ID,
						}
					}()
				}
				if job.OnFinish != nil {
					job.OnFinish(res, err, w.ID, job.ID)
				}
			case <-w.stop:
				return
			}
		}
	}()
}

// Stop stops the worker
func (w *Worker[T]) Stop() {
	w.stopOnce.Do(func() {
		w.active = false
		close(w.stop)
	})
}

// WorkerPool represents a pool of workers
type WorkerPool[T any] struct {
	workers []*Worker[T]
	resChan chan JobResult[T]
	stop    chan struct{} // Channel to signal stopping the worker pool
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (wp *WorkerPool[T]) AddJob(job Job[T]) {
	// Hash the job ID and use the result to determine the worker index
	workerIndex := hash(job.ID) % uint32(len(wp.workers))
	wp.workers[workerIndex].jobs <- job
}

// Run starts all workers in the pool and keeps the pool running until stopped
func (wp *WorkerPool[T]) Run() {
	go func() {
		for _, worker := range wp.workers {
			worker.Run()
		}
		<-wp.stop // Wait until stop signal received
	}()
}

// Stop stops all workers in the pool
func (wp *WorkerPool[T]) Stop() {
	close(wp.stop) // Signal to stop the worker pool
	for _, worker := range wp.workers {
		worker.Stop()
	}
}

// EXAMPLE

// func main() {
// 	// Create a worker pool with 3 workers
// 	wp := ksync.NewWorkerPool[string](9)
// 	defer wp.Stop()
// 	// Add some jobs to the pool
// 	for i := 1; ; i++ {
// 		id := i
// 		wp.AddJob(ksync.Job[string]{
// 			ID: fmt.Sprintf("Job--%d", id),
// 			Job: func(workerID int) (string, error) {
// 				fmt.Printf("Executing job %d with worker %d\n", id, workerID)
// 				time.Sleep(100 * time.Millisecond) // Simulating some work
// 				return "done", nil
// 			},
// 		})
// 		if i == 10 {
// 			break
// 		}
// 	}
// 	wp.OnResult(func(res ksync.JobResult[string]) {
// 		fmt.Printf("got:: %+v\n", res)
// 	})

// 	wp.Run()

// 	fmt.Scanln()
// }
