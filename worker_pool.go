// worker_pool.go
package main

import (
	"sync"
	"sync/atomic"
)

// Task 定义了工作池中的任务类型
type Task func()

func NewWorkerPool(workerCount int, stopProcessing *int32) (chan<- Task, *sync.WaitGroup, func(), chan struct{}) {
	var wg sync.WaitGroup
	taskQueue := make(chan Task)
	stopSignal := make(chan struct{})

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopSignal:
					return
				case task, ok := <-taskQueue:
					if !ok || atomic.LoadInt32(stopProcessing) != 0 {
						return
					}
					task()
				}
			}
		}()
	}

	stopFunc := func() {
		close(stopSignal) // 发送停止信号
		close(taskQueue)  // 关闭任务队列
	}

	return taskQueue, &wg, stopFunc, stopSignal
}
