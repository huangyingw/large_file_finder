package main

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"os"
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
	assert.True(t, mr.SIsMember("fileHashToPathSet:"+testFileHash, testPath))
	assert.True(t, mr.Exists("hashedKeyToFullHash:"+hashedKey))
	assert.True(t, mr.Exists("pathToHashedKey:"+testPath))
	assert.True(t, mr.Exists("hashedKeyToFileHash:"+hashedKey))
}

func TestSaveDuplicateFileInfoToRedis(t *testing.T) {
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
	info := FileInfo{
		Size:    1024,
		ModTime: time.Now(),
		Path:    "/path/to/testfile_01:23:45.txt",
	}

	err = SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
	assert.NoError(t, err)

	// Verify the data was saved correctly
	assert.True(t, mr.Exists("duplicateFiles:"+fullHash))
	members, err := rdb.ZRange(ctx, "duplicateFiles:"+fullHash, 0, -1).Result()
	assert.NoError(t, err)
	assert.Contains(t, members, info.Path)
}

func TestCleanUpOldRecords(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	// Create test data
	existingFile := "/path/to/existing.txt"
	nonExistingFile := "/path/to/non_existing.txt"

	// Create the existing file
	_, err = os.Create(existingFile)
	assert.NoError(t, err)
	defer os.Remove(existingFile)

	// Set up Redis data
	for _, path := range []string{existingFile, nonExistingFile} {
		hashedKey := generateHash(path)
		_ = rdb.Set(ctx, "pathToHashedKey:"+path, hashedKey, 0)
		_ = rdb.Set(ctx, "hashedKeyToPath:"+hashedKey, path, 0)
		_ = rdb.Set(ctx, "fileInfo:"+hashedKey, "dummy_data", 0)
		_ = rdb.Set(ctx, "hashedKeyToFileHash:"+hashedKey, "dummy_hash", 0)
	}

	err = CleanUpOldRecords(rdb, ctx)
	assert.NoError(t, err)

	// Check if records for non-existing file were deleted
	assert.False(t, mr.Exists("pathToHashedKey:"+nonExistingFile))

	// Check if records for existing file were retained
	assert.True(t, mr.Exists("pathToHashedKey:"+existingFile))
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
