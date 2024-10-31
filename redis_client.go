// redis_client.go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Redis key 前缀
const (
	keyPrefixFileInfo        = "fileInfo:"
	keyPrefixHashedKeyToPath = "hashedKeyToPath:"
	keyPrefixPathToHashedKey = "pathToHashedKey:"
	keyPrefixFileHash        = "fileHashToPathSet:"
	keyPrefixDuplicateFiles  = "duplicateFiles:"
	keyPrefixHashCache       = "hashedKeyToFileHash:"
	keyPrefixFullHashCache   = "hashedKeyToFullHash:"
	keyPrefixCalculating     = "calculating:"
)

// Generate a SHA-256 hash for the given string
func generateHash(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return hex.EncodeToString(hasher.Sum(nil))
}

func getFileInfoKey(hashedKey string) string {
	return keyPrefixFileInfo + hashedKey
}

func getHashedKeyToPathKey(hashedKey string) string {
	return keyPrefixHashedKeyToPath + hashedKey
}

func getPathToHashedKeyKey(path string) string {
	return keyPrefixPathToHashedKey + path
}

func getFileHashKey(fileHash string) string {
	return keyPrefixFileHash + fileHash
}

func getDuplicateFilesKey(fullHash string) string {
	return keyPrefixDuplicateFiles + fullHash
}

func getHashCacheKey(hashedKey string) string {
	return keyPrefixHashCache + hashedKey
}

func getFullHashCacheKey(hashedKey string) string {
	return keyPrefixFullHashCache + hashedKey
}

func getCalculatingKey(path string, limit int64) string {
	return fmt.Sprintf("%s%s:%d", keyPrefixCalculating, path, limit)
}

func saveFileInfoToRedis(rdb *redis.Client, ctx context.Context, fullPath string, info FileInfo, fileHash, fullHash string, calculateHashes bool) error {
	hashedKey := generateHash(fullPath)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(info); err != nil {
		return fmt.Errorf("error encoding file info: %w", err)
	}

	pipe := rdb.Pipeline()

	pipe.Set(ctx, getFileInfoKey(hashedKey), buf.Bytes(), 0)
	pipe.Set(ctx, getHashedKeyToPathKey(hashedKey), fullPath, 0)
	pipe.Set(ctx, getPathToHashedKeyKey(fullPath), hashedKey, 0)

	if calculateHashes {
		pipe.SAdd(ctx, getFileHashKey(fileHash), fullPath)
		pipe.Set(ctx, getHashCacheKey(hashedKey), fileHash, 0)
		if fullHash != "" {
			pipe.Set(ctx, getFullHashCacheKey(hashedKey), fullHash, 0)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error executing pipeline for file: %s: %w", fullPath, err)
	}

	return nil
}

// 将重复文件的信息存储到 Redis
func SaveDuplicateFileInfoToRedis(rdb *redis.Client, ctx context.Context, fullHash string, info FileInfo) error {
	log.Printf("Saving duplicate file info to Redis for hash: %s, path: %s", fullHash, info.Path)
	timestamps := ExtractTimestamps(info.Path)
	fileNameLength := len(filepath.Base(info.Path))
	score := CalculateScore(timestamps, fileNameLength)

	_, err := rdb.ZAdd(ctx, getDuplicateFilesKey(fullHash), &redis.Z{
		Score:  score,
		Member: info.Path,
	}).Result()

	if err != nil {
		log.Printf("Error adding duplicate file to Redis: %v", err)
		return fmt.Errorf("error adding duplicate file to Redis: %w", err)
	}

	log.Printf("Successfully saved duplicate file info to Redis for hash: %s, path: %s", fullHash, info.Path)
	return nil
}

func CleanUpOldRecords(rdb *redis.Client, ctx context.Context) error {
	log.Println("Starting to clean up old records")
	iter := rdb.Scan(ctx, 0, "pathToHashedKey:*", 0).Iterator()
	for iter.Next(ctx) {
		pathToHashKey := iter.Val()
		filePath := strings.TrimPrefix(pathToHashKey, "pathToHashedKey:")

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			err := cleanUpRecordsByFilePath(rdb, ctx, filePath)
			if err != nil && err != redis.Nil {
				log.Printf("Error cleaning up records for file %s: %s\n", filePath, err)
			}
		}
	}

	if err := iter.Err(); err != nil {
		log.Printf("Error during iteration: %s\n", err)
		return err
	}

	return nil
}

func cleanUpRecordsByFilePath(rdb *redis.Client, ctx context.Context, fullPath string) error {
	hashedKey := generateHash(fullPath)

	pipe := rdb.Pipeline()

	pipe.Del(ctx, getFileInfoKey(hashedKey))
	pipe.Del(ctx, getHashedKeyToPathKey(hashedKey))
	pipe.Del(ctx, getPathToHashedKeyKey(fullPath))

	fileHashCmd := pipe.Get(ctx, getHashCacheKey(hashedKey))
	pipe.Del(ctx, getHashCacheKey(hashedKey))

	fullHashCmd := pipe.Get(ctx, getFullHashCacheKey(hashedKey))
	pipe.Del(ctx, getFullHashCacheKey(hashedKey))

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("error executing pipeline for cleanup of %s: %v", fullPath, err)
	}

	fileHash, err := fileHashCmd.Result()
	if err == nil {
		pipe.SRem(ctx, getFileHashKey(fileHash), fullPath)
	}

	fullHash, err := fullHashCmd.Result()
	if err == nil {
		pipe.ZRem(ctx, getDuplicateFilesKey(fullHash), fullPath)
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("error executing second pipeline for cleanup of %s: %v", fullPath, err)
	}

	log.Printf("Successfully cleaned up records for file: %s", fullPath)
	return nil
}

func cleanUpHashKeys(rdb *redis.Client, ctx context.Context, fullHash, duplicateFilesKey string) error {
	fileHashKey := getFileHashKey(fullHash)

	// 使用管道批量删除 Redis 键
	pipe := rdb.TxPipeline()
	pipe.Del(ctx, duplicateFilesKey)
	pipe.Del(ctx, fileHashKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error executing pipeline for cleaning up hash keys: %w", err)
	}

	log.Printf("Cleaned up Redis keys: %s and %s\n", duplicateFilesKey, fileHashKey)
	return nil
}
