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
	input := "test string"
	hash := generateHash(input)
	assert.NotEmpty(t, hash)
	assert.Len(t, hash, 64) // SHA-256 hash is 64 characters long
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

	err = saveFileInfoToRedis(rdb, ctx, testPath, testInfo, testFileHash, testFullHash)
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
