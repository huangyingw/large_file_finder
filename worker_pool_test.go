package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewWorkerPool(t *testing.T) {
	workerCount := 3
	var stopProcessing bool
	taskQueue, wg, stopFunc, stopSignal := NewWorkerPool(workerCount, &stopProcessing)

	// Test that the worker pool is created correctly
	assert.NotNil(t, taskQueue)
	assert.NotNil(t, wg)
	assert.NotNil(t, stopFunc)
	assert.NotNil(t, stopSignal)

	// Test that tasks can be added and processed
	done := make(chan bool)
	taskQueue <- func() {
		time.Sleep(100 * time.Millisecond)
		done <- true
	}

	select {
	case <-done:
		// Task completed successfully
	case <-time.After(1 * time.Second):
		t.Fatal("Task did not complete in time")
	}

	// Test stopping the worker pool
	stopFunc()
	wg.Wait()

	// Ensure the channels are closed
	_, ok := <-stopSignal
	assert.False(t, ok, "stopSignal should be closed")
}
