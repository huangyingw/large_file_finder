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

// Generate a SHA-256 hash for the given string
func generateHash(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return hex.EncodeToString(hasher.Sum(nil))
}

func saveFileInfoToRedis(rdb *redis.Client, ctx context.Context, fullPath string, info FileInfo, fileHash, fullHash string, calculateHashes bool) error {
	hashedKey := generateHash(fullPath)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(info); err != nil {
		return fmt.Errorf("error encoding file info: %w", err)
	}

	pipe := rdb.Pipeline()

	pipe.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0)
	pipe.Set(ctx, "hashedKeyToPath:"+hashedKey, fullPath, 0)
	pipe.Set(ctx, "pathToHashedKey:"+fullPath, hashedKey, 0)

	if calculateHashes {
		pipe.SAdd(ctx, "fileHashToPathSet:"+fileHash, fullPath)
		pipe.Set(ctx, "hashedKeyToFileHash:"+hashedKey, fileHash, 0)
		if fullHash != "" {
			pipe.Set(ctx, "hashedKeyToFullHash:"+hashedKey, fullHash, 0)
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
	timestamps := ExtractTimestamps(info.Path)
	fileNameLength := len(filepath.Base(info.Path))
	score := CalculateScore(timestamps, fileNameLength)

	_, err := rdb.ZAdd(ctx, "duplicateFiles:"+fullHash, &redis.Z{
		Score:  score,
		Member: info.Path,
	}).Result()

	if err != nil {
		return fmt.Errorf("error adding duplicate file to Redis: %w", err)
	}

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

	pipe.Del(ctx, "fileInfo:"+hashedKey)
	pipe.Del(ctx, "hashedKeyToPath:"+hashedKey)
	pipe.Del(ctx, "pathToHashedKey:"+fullPath)

	fileHashCmd := pipe.Get(ctx, "hashedKeyToFileHash:"+hashedKey)
	pipe.Del(ctx, "hashedKeyToFileHash:"+hashedKey)

	fullHashCmd := pipe.Get(ctx, "hashedKeyToFullHash:"+hashedKey)
	pipe.Del(ctx, "hashedKeyToFullHash:"+hashedKey)

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("error executing pipeline for cleanup of %s: %v", fullPath, err)
	}

	fileHash, err := fileHashCmd.Result()
	if err == nil {
		pipe.SRem(ctx, "fileHashToPathSet:"+fileHash, fullPath)
	}

	fullHash, err := fullHashCmd.Result()
	if err == nil {
		pipe.ZRem(ctx, "duplicateFiles:"+fullHash, fullPath)
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return fmt.Errorf("error executing second pipeline for cleanup of %s: %v", fullPath, err)
	}

	log.Printf("Successfully cleaned up records for file: %s", fullPath)
	return nil
}

func cleanUpHashKeys(rdb *redis.Client, ctx context.Context, fullHash, duplicateFilesKey string) error {
	fileHashKey := "fileHashToPathSet:" + fullHash

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
