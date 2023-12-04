package main

import (
	"sync"
)

// Task 定义了工作池中的任务类型
type Task func()

// NewWorkerPool 创建并返回一个工作池
func NewWorkerPool(workerCount int) (chan<- Task, *sync.WaitGroup) {
	var wg sync.WaitGroup
	taskQueue := make(chan Task)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskQueue {
				task()
			}
		}()
	}

	return taskQueue, &wg
}
