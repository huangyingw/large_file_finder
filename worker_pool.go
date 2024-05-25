// worker_pool.go
package main

import (
	"sync"
)

// Task 定义了工作池中的任务类型
type Task func()

func NewWorkerPool(workerCount int) (chan<- Task, *sync.WaitGroup, func(), chan struct{}) {
	var wg sync.WaitGroup
	taskQueue := make(chan Task)
	stopProcessing := make(chan struct{})

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopProcessing:
					return
				case task, ok := <-taskQueue:
					if !ok {
						return
					}
					task()
				}
			}
		}()
	}

	stopFunc := func() {
		close(stopProcessing) // 发送停止信号
		close(taskQueue)      // 关闭任务队列
	}

	return taskQueue, &wg, stopFunc, stopProcessing
}
