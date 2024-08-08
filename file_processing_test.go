// file_processing_test.go
package main

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
)

func TestExtractTimestamp(t *testing.T) {
	testCases := []struct {
		name     string
		filePath string
		expected string
	}{
		{"With timestamp", "/path/to/file_12:34:56.txt", "12:34:56"},
		{"Without timestamp", "/path/to/file.txt", ""},
		{"With date", "/path/to/2021-05-01_file.txt", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := extractTimestamp(tc.filePath)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCalculateScore(t *testing.T) {
	testCases := []struct {
		name           string
		timestamp      string
		fileNameLength int
		expected       float64
	}{
		{"Long timestamp", "12:34:56", 10, -8000010},
		{"Short timestamp", "12:34", 5, -5000005},
		{"No timestamp", "", 15, -15},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := calculateScore(tc.timestamp, tc.fileNameLength)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSaveDuplicateFileInfoToRedis(t *testing.T) {
	rdb, mock := redismock.NewClientMock()
	ctx := context.Background()

	testCases := []struct {
		name     string
		fullHash string
		info     FileInfo
		filePath string
		expected float64
	}{
		{
			name:     "File with timestamp",
			fullHash: "testhash1",
			info:     FileInfo{Size: 1000, ModTime: time.Now().Unix()},
			filePath: "/path/to/file_12:34:56.txt",
			expected: -8000023,
		},
		{
			name:     "File without timestamp",
			fullHash: "testhash2",
			info:     FileInfo{Size: 2000, ModTime: time.Now().Unix()},
			filePath: "/path/to/file.txt",
			expected: -13,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock.ExpectZAdd("duplicateFiles:"+tc.fullHash, &redis.Z{
				Score:  tc.expected,
				Member: tc.filePath,
			}).SetVal(1)

			err := saveDuplicateFileInfoToRedis(rdb, ctx, tc.fullHash, tc.info, tc.filePath)

			assert.NoError(t, err)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// Add more tests for other functions...
