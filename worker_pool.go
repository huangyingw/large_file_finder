// worker_pool.go
package main

import (
	"sync"
)

// Task 定义了工作池中的任务类型
type Task func()

func NewWorkerPool(workerCount int) (chan<- Task, *sync.WaitGroup, func()) {
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

	stopFunc := func() {
		close(taskQueue) // 关闭任务队列
	}

	return taskQueue, &wg, stopFunc
}
