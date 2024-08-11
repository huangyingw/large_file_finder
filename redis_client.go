// redis_client.go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"path/filepath" // 添加导入
	"regexp"
	"strings"
)

// Generate a SHA-256 hash for the given string
func generateHash(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	hash := hex.EncodeToString(hasher.Sum(nil))
	return hash
}

// 使用给定的正则表达式匹配时间戳
func extractTimestamp(filePath string) string {
	// 匹配冒号或逗号后的 MM:SS 或 H:MM:SS 格式的时间戳
	re := regexp.MustCompile(`[:,/](\d{1,2}:\d{2}(?::\d{2})?)`)
	match := re.FindStringSubmatch(filePath)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

// 将重复文件的信息存储到 Redis
func saveDuplicateFileInfoToRedis(rdb *redis.Client, ctx context.Context, fullHash string, info FileInfo) error {
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

func saveFileInfoToRedis(rdb *redis.Client, ctx context.Context, hashedKey string, path string, buf bytes.Buffer, fileHash string, fullHash string) error {
	// 规范化路径
	normalizedPath := filepath.Clean(path)

	// 使用管道批量处理Redis命令
	pipe := rdb.Pipeline()

	// 这里我们添加命令到管道，但不立即检查错误
	pipe.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0)
	pipe.Set(ctx, "hashedKeyToPath:"+hashedKey, normalizedPath, 0)
	pipe.SAdd(ctx, "fileHashToPathSet:"+fileHash, normalizedPath) // 将文件路径存储为集合
	if fullHash != "" {
		pipe.Set(ctx, "hashedKeyToFullHash:"+hashedKey, fullHash, 0) // 存储完整文件哈希值
	}
	// 存储从路径到hashedKey的映射
	pipe.Set(ctx, "pathToHashedKey:"+normalizedPath, hashedKey, 0)
	// 存储hashedKey到fileHash的映射
	pipe.Set(ctx, "hashedKeyToFileHash:"+hashedKey, fileHash, 0)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("error executing pipeline for file: %s: %w", path, err)
	}

	return nil
}

func cleanUpOldRecords(rdb *redis.Client, ctx context.Context) error {
	log.Println("Starting to clean up old records")
	iter := rdb.Scan(ctx, 0, "pathToHashedKey:*", 0).Iterator()
	for iter.Next(ctx) {
		pathToHashKey := iter.Val()
		filePath := strings.TrimPrefix(pathToHashKey, "pathToHashedKey:")

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			err := cleanUpRecordsByFilePath(rdb, ctx, filePath)
			if err != nil {
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

func cleanUpRecordsByFilePath(rdb *redis.Client, ctx context.Context, filePath string) error {
	hashedKey, err := rdb.Get(ctx, "pathToHashedKey:"+filePath).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("error retrieving hashedKey for path %s: %v", filePath, err)
	}

	fileHash, err := rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Result()
	if err != nil {
		return fmt.Errorf("error retrieving fileHash for key %s: %v", hashedKey, err)
	}

	fullHash, err := rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("error retrieving fullHash for key %s: %v", hashedKey, err)
	}

	// 删除记录
	pipe := rdb.TxPipeline()
	pipe.Del(ctx, "fileInfo:"+hashedKey)            // 删除 fileInfo 相关数据
	pipe.Del(ctx, "hashedKeyToPath:"+hashedKey)     // 删除 path 相关数据
	pipe.Del(ctx, "pathToHashedKey:"+filePath)      // 删除从路径到 hashedKey 的映射
	pipe.Del(ctx, "hashedKeyToFileHash:"+hashedKey) // 删除 hashedKey 到 fileHash 的映射
	if fullHash != "" {
		pipe.Del(ctx, "hashedKeyToFullHash:"+hashedKey)      // 删除完整文件哈希相关数据
		pipe.ZRem(ctx, "duplicateFiles:"+fullHash, filePath) // 从 duplicateFiles 有序集合中移除路径
	}
	pipe.SRem(ctx, "fileHashToPathSet:"+fileHash, filePath) // 从 hash 集合中移除文件路径

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error deleting keys for outdated record %s: %v", hashedKey, err)
	}

	log.Printf("Deleted outdated record: path=%s\n", filePath)
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
