package main

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGenerateHash(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple string",
			input:    "hello",
			expected: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:     "Long string",
			input:    "This is a very long string that we will use to test the generateHash function",
			expected: "d24160249ac798efe059cdc40edb21f4fc7a5627852fa0313e98ab6f35446a83",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := generateHash(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSaveFileInfoToRedis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	testPath := "/path/to/testfile.txt"
	testInfo := FileInfo{
		Size:    1024,
		ModTime: time.Now(),
	}
	testFileHash := "testfilehash"
	testFullHash := "testfullhash"

	err = saveFileInfoToRedis(rdb, ctx, testPath, testInfo, testFileHash, testFullHash, true)
	assert.NoError(t, err)

	// Verify the data was saved correctly
	hashedKey := generateHash(testPath)
	assert.True(t, mr.Exists("fileInfo:"+hashedKey))
	assert.True(t, mr.Exists("hashedKeyToPath:"+hashedKey))

	isMember, err := mr.SIsMember("fileHashToPathSet:"+testFileHash, testPath)
	assert.NoError(t, err)
	assert.True(t, isMember)

	assert.True(t, mr.Exists("hashedKeyToFullHash:"+hashedKey))
	assert.True(t, mr.Exists("pathToHashedKey:"+testPath))
	assert.True(t, mr.Exists("hashedKeyToFileHash:"+hashedKey))
}

func TestCleanUpHashKeys(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	fullHash := "testfullhash"
	duplicateFilesKey := "duplicateFiles:" + fullHash
	fileHashKey := "fileHashToPathSet:" + fullHash

	// Set up test data
	_ = rdb.Set(ctx, duplicateFilesKey, "dummy_data", 0)
	_ = rdb.Set(ctx, fileHashKey, "dummy_data", 0)

	err = cleanUpHashKeys(rdb, ctx, fullHash, duplicateFilesKey)
	assert.NoError(t, err)

	// Check if keys were deleted
	assert.False(t, mr.Exists(duplicateFilesKey))
	assert.False(t, mr.Exists(fileHashKey))
}
