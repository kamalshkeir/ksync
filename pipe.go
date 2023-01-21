package ksync

import (
	"fmt"
	"sync"
)

// Stage represents a processing stage in the pipeline
type Stage[T any] struct {
	name string
	f    func(T) (T, error)
}

// Pipeline represents a pipeline of stages
type Pipeline[T any] struct {
	stages []Stage[T]
	wg     sync.WaitGroup
}

// NewPipeline creates a new pipeline
func NewPipeline[T any]() *Pipeline[T] {
	return &Pipeline[T]{}
}

// AddStage adds a new stage to the pipeline
func (p *Pipeline[T]) AddStage(name string, f func(T) (T, error)) error {
	if f == nil {
		return fmt.Errorf("function should not be nil")
	}
	p.stages = append(p.stages, Stage[T]{name: name, f: f})
	return nil
}

// Process processes the input data through all the stages of the pipeline
func (p *Pipeline[T]) Process(data T) error {
	var errOut error
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for _, stage := range p.stages {
			var err error
			data, err = stage.f(data)
			if err != nil {
				errOut = fmt.Errorf("error at stage %s: %v", stage.name, err)
			}
		}
	}()
	if errOut != nil {
		return errOut
	}
	return nil
}

// Wait waits for all goroutines to complete
func (p *Pipeline[T]) Wait() {
	p.wg.Wait()
}

// Example

// func main() {
// 	pipeline := NewPipeline[int]()
// 	// add stages
// 	for i := 0; i < 5; i++ {
// 		in := i
// 		err := pipeline.AddStage("stage-"+strconv.Itoa(in), func(data int) (int, error) {
// 			fmt.Printf("stage-%d : %v\n", in, data)
// 			time.Sleep(time.Second * time.Duration(in))
// 			return data + 1, nil
// 		})
// 		if err != nil {
// 			fmt.Println(err)
// 			return
// 		}
// 	}
// 	// process the 5 stages with 1
// 	err := pipeline.Process(1)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println("--------------------------")
// 	err = pipeline.Process(2)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Println("--------------------------")
// 	err = pipeline.Process(3)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	pipeline.Wait()
// }
