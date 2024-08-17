package main

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestNewWorkerPool(t *testing.T) {
	workerCount := 3
	var stopProcessing bool
	taskQueue, wg, stopFunc, stopSignal := NewWorkerPool
}
