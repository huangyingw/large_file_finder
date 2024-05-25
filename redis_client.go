// redis_client.go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"path/filepath" // 添加导入
	"strings"
)

// Generate a SHA-256 hash for the given string
func generateHash(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return hex.EncodeToString(hasher.Sum(nil))
}

// 将重复文件的信息存储到 Redis
func saveDuplicateFileInfoToRedis(rdb *redis.Client, ctx context.Context, fullHash string, info fileInfo) error {
	// 使用管道批量处理 Redis 命令
	pipe := rdb.Pipeline()

	// 将路径添加到有序集合 duplicateFiles:<fullHash> 中，并使用文件名长度的负值作为分数
	fileNameLength := len(filepath.Base(info.path))
	pipe.ZAdd(ctx, "duplicateFiles:"+fullHash, &redis.Z{
		Score:  float64(-fileNameLength), // 取负值
		Member: info.path,
	})

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("error executing pipeline for duplicate file: %s: %w", info.path, err)
	}
	return nil
}

func saveFileInfoToRedis(rdb *redis.Client, ctx context.Context, hashedKey string, path string, buf bytes.Buffer, startTime int64, fileHash string, fullHash string) error {
	// 使用管道批量处理Redis命令
	pipe := rdb.Pipeline()

	// 这里我们添加命令到管道，但不立即检查错误
	pipe.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0)
	pipe.Set(ctx, "hashedKeyToPath:"+hashedKey, path, 0)
	pipe.Set(ctx, "updateTime:"+hashedKey, startTime, 0)
	pipe.SAdd(ctx, "hash:"+fileHash, path) // 将文件路径存储为集合
	if fullHash != "" {
		pipe.Set(ctx, "hashedKeyToFullHash:"+hashedKey, fullHash, 0) // 存储完整文件哈希值
	}
	// 存储从路径到hashedKey的映射
	pipe.Set(ctx, "pathToHashedKey:"+path, hashedKey, 0)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("error executing pipeline for file: %s: %w", path, err)
	}
	return nil
}

func cleanUpOldRecords(rdb *redis.Client, ctx context.Context) error {
	fmt.Println("Starting to clean up old records")
	iter := rdb.Scan(ctx, 0, "pathToHashedKey:*", 0).Iterator()
	for iter.Next(ctx) {
		pathToHashKey := iter.Val()

		// 解析出文件路径
		filePath := strings.TrimPrefix(pathToHashKey, "pathToHashedKey:")

		// 检查文件是否存在
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			err := cleanUpRecordsByFilePath(rdb, ctx, filePath)
			if err != nil {
				fmt.Printf("Error cleaning up records for file %s: %s\n", filePath, err)
			}
		}
	}

	if err := iter.Err(); err != nil {
		fmt.Printf("Error during iteration: %s\n", err)
		return err
	}

	return nil
}

func cleanUpRecordsByFilePath(rdb *redis.Client, ctx context.Context, filePath string) error {
	// 获取 hashedKey
	hashedKey, err := rdb.Get(ctx, "pathToHashedKey:"+filePath).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("error retrieving hashedKey for path %s: %v", filePath, err)
	}

	// 获取 fileHash
	fileHash, err := rdb.Get(ctx, "hash:"+hashedKey).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("error retrieving fileHash for key %s: %v", hashedKey, err)
	}

	// 获取 fullHash
	fullHash, err := rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("error retrieving fullHash for key %s: %v", hashedKey, err)
	}

	// 构造 duplicateFilesKey
	duplicateFilesKey := "duplicateFiles:" + fullHash

	// 删除记录
	pipe := rdb.TxPipeline()
	pipe.Del(ctx, "updateTime:"+hashedKey)      // 删除 updateTime 键
	pipe.Del(ctx, "fileInfo:"+hashedKey)        // 删除 fileInfo 相关数据
	pipe.Del(ctx, "hashedKeyToPath:"+hashedKey)            // 删除 path 相关数据
	pipe.Del(ctx, "pathToHashedKey:"+filePath)       // 删除从路径到 hashedKey 的映射
	pipe.Del(ctx, "hashedKeyToFullHash:"+hashedKey)        // 删除完整文件哈希相关数据
	pipe.ZRem(ctx, duplicateFilesKey, filePath) // 从 duplicateFiles 有序集合中移除路径
	pipe.SRem(ctx, "hash:"+fileHash, filePath)  // 从 hash 集合中移除文件路径

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error deleting keys for outdated record %s: %v", hashedKey, err)
	}

	fmt.Printf("Deleted outdated record: path=%s\n", filePath)
	return nil
}
