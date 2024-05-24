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
// 	// Create a worker pool with 3 workers
// 	pipe := ksync.NewPipeline[string]()

// 	err := pipe.AddStage("remove-space", func(s string) (string, error) {
// 		return strings.ReplaceAll(s, " ", ""), nil
// 	})
// 	if err != nil {
// 		fmt.Println("err:", err)
// 	}

// 	err = pipe.AddStage("uppercase", func(s string) (string, error) {
// 		return strings.ToUpper(s), nil
// 	})
// 	if err != nil {
// 		fmt.Println("err:", err)
// 	}
// 	err = pipe.AddStage("reverse", func(s string) (string, error) {
// 		return string(reverse([]rune(s))), nil
// 	})
// 	if err != nil {
// 		fmt.Println("err:", err)
// 	}
// 	err = pipe.AddStage("print", func(s string) (string, error) {
// 		fmt.Println(s)
// 		return "", nil
// 	})
// 	if err != nil {
// 		fmt.Println("err:", err)
// 	}

// 	err = pipe.Process("kamal shkeir")
// 	if err != nil {
// 		fmt.Println("err Process:", err)
// 	}
// 	err = pipe.Process("another one")
// 	if err != nil {
// 		fmt.Println("err Process:", err)
// 	}
// 	pipe.Wait()
// }

// func reverse(s []rune) []rune {
// 	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
// 		s[i], s[j] = s[j], s[i]
// 	}
// 	return s
// }
