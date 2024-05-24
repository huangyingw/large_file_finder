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
	pipe.Set(ctx, "path:"+hashedKey, path, 0)
	pipe.Set(ctx, "updateTime:"+hashedKey, startTime, 0)
	pipe.SAdd(ctx, "hash:"+fileHash, path) // 将文件路径存储为集合
	if fullHash != "" {
		pipe.Set(ctx, "fullHash:"+hashedKey, fullHash, 0) // 存储完整文件哈希值
	}
	// 存储从路径到hashedKey的映射
	pipe.Set(ctx, "pathToHash:"+path, hashedKey, 0)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("error executing pipeline for file: %s: %w", path, err)
	}
	return nil
}

func cleanUpOldRecords(rdb *redis.Client, ctx context.Context, startTime int64) error {
	fmt.Println("Starting to clean up old records")
	iter := rdb.Scan(ctx, 0, "updateTime:*", 0).Iterator()
	for iter.Next(ctx) {
		updateTimeKey := iter.Val()

		// 解析出原始的hashedKey
		hashedKey := strings.TrimPrefix(updateTimeKey, "updateTime:")

		// 获取文件路径和文件哈希值
		filePath, err := rdb.Get(ctx, "path:"+hashedKey).Result()
		if err != nil && err != redis.Nil {
			fmt.Printf("Error retrieving filePath for key %s: %s\n", hashedKey, err)
			continue
		}

		fileHash, err := calculateFileHash(filePath, false)
		if err != nil {
			fmt.Printf("Error calculating hash for file %s: %s\n", filePath, err)
			continue
		}

		filePaths, errHash := rdb.SMembers(ctx, "hash:"+fileHash).Result()
		if errHash != nil && errHash != redis.Nil {
			fmt.Printf("Error retrieving file paths for key %s: %s\n", hashedKey, errHash)
			continue
		}

		// 检查文件是否存在
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			fmt.Printf("File does not exist, cleaning up records: %s\n", filePath)

			// 获取文件信息
			fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
			if err != nil && err != redis.Nil {
				fmt.Printf("Error retrieving fileInfo for key %s: %s\n", hashedKey, err)
				continue
			}

			// 解码文件信息
			var fileInfo FileInfo
			if len(fileInfoData) > 0 {
				buf := bytes.NewBuffer(fileInfoData)
				dec := gob.NewDecoder(buf)
				if err := dec.Decode(&fileInfo); err != nil {
					fmt.Printf("Error decoding fileInfo for key %s: %s\n", hashedKey, err)
					continue
				}
			}

			// 构造 duplicateFilesKey
			for _, filePath := range filePaths {
				fmt.Printf("Calculating hash for file: %s\n", filePath)
				fileHash, err := calculateFileHash(filePath, false)
				if err != nil {
					fmt.Printf("Error calculating hash for file %s: %s\n", filePath, err)
					continue
				}

				duplicateFilesKey := "duplicateFiles:" + fileHash

				// 删除记录
				pipe := rdb.TxPipeline()
				pipe.Del(ctx, updateTimeKey)                // 删除updateTime键
				pipe.Del(ctx, "fileInfo:"+hashedKey)        // 删除fileInfo相关数据
				pipe.Del(ctx, "path:"+hashedKey)            // 删除path相关数据
				pipe.SRem(ctx, "hash:"+fileHash, filePath)  // 从集合中移除文件路径
				pipe.Del(ctx, "pathToHash:"+filePath)       // 删除从路径到hashedKey的映射
				pipe.Del(ctx, "fullHash:"+hashedKey)        // 删除完整文件哈希相关数据
				pipe.ZRem(ctx, duplicateFilesKey, filePath) // 从 duplicateFiles 有序集合中移除路径

				_, err = pipe.Exec(ctx)
				if err != nil {
					fmt.Printf("Error deleting keys for outdated record %s: %s\n", hashedKey, err)
				} else {
					fmt.Printf("Deleted outdated record: path=%s, size=%d\n", filePath, fileInfo.Size)
				}
			}
		}
	}

	if err := iter.Err(); err != nil {
		fmt.Printf("Error during iteration: %s\n", err)
		return err
	}

	return nil
}
