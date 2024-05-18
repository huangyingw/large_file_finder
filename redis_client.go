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
	"strconv"
	"strings"
)

// Generate a SHA-256 hash for the given string
func generateHash(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return hex.EncodeToString(hasher.Sum(nil))
}

func saveFileInfoToRedis(rdb *redis.Client, ctx context.Context, hashedKey string, path string, buf bytes.Buffer, startTime int64, fileHash string, hashSizeKey string, fullHash string) error {
	// 使用管道批量处理Redis命令
	pipe := rdb.Pipeline()

	// 这里我们添加命令到管道，但不立即检查错误
	pipe.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0)
	pipe.Set(ctx, "path:"+hashedKey, path, 0)
	pipe.Set(ctx, "updateTime:"+hashedKey, startTime, 0)
	pipe.Set(ctx, "hash:"+hashedKey, fileHash, 0) // 存储文件哈希值
	if fullHash != "" {
		pipe.Set(ctx, "fullHash:"+hashedKey, fullHash, 0) // 存储完整文件哈希值
	}
	// 存储从路径到hashedKey的映射
	pipe.Set(ctx, "pathToHash:"+path, hashedKey, 0)
	// 使用SAdd而不是Set，将路径添加到集合中
	pipe.SAdd(ctx, hashSizeKey, path)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("error executing pipeline for file: %s: %w", path, err)
	}
	return nil
}

func cleanUpOldRecords(rdb *redis.Client, ctx context.Context, startTime int64) error {
	iter := rdb.Scan(ctx, 0, "updateTime:*", 0).Iterator()
	for iter.Next(ctx) {
		updateTimeKey := iter.Val()
		updateTime, err := rdb.Get(ctx, updateTimeKey).Int64()
		if err != nil {
			fmt.Printf("Error retrieving updateTime for key %s: %s\n", updateTimeKey, err)
			continue
		}

		if updateTime < startTime {
			// 解析出原始的hashedKey
			hashedKey := strings.TrimPrefix(updateTimeKey, "updateTime:")

			// 获取文件路径和文件哈希值
			filePath, err := rdb.Get(ctx, "path:"+hashedKey).Result()
			fileHash, errHash := rdb.Get(ctx, "hash:"+hashedKey).Result()
			if err != nil || errHash != nil {
				fmt.Printf("Error retrieving filePath or file hash for key %s: %s, %s\n", hashedKey, err, errHash)
				continue
			}

			// 获取文件信息
			fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
			if err != nil {
				fmt.Printf("Error retrieving fileInfo for key %s: %s\n", hashedKey, err)
				continue
			}

			// 解码文件信息
			var fileInfo FileInfo
			buf := bytes.NewBuffer(fileInfoData)
			dec := gob.NewDecoder(buf)
			if err := dec.Decode(&fileInfo); err != nil {
				fmt.Printf("Error decoding fileInfo for key %s: %s\n", hashedKey, err)
				continue
			}

			// 构造hashSizeKey
			hashSizeKey := "fileHashSize:" + fileHash + "_" + strconv.FormatInt(fileInfo.Size, 10)

			// 删除记录
			pipe := rdb.Pipeline()
			pipe.Del(ctx, updateTimeKey)          // 删除updateTime键
			pipe.Del(ctx, "fileInfo:"+hashedKey)  // 删除fileInfo相关数据
			pipe.Del(ctx, "path:"+hashedKey)      // 删除path相关数据
			pipe.Del(ctx, "hash:"+hashedKey)      // 删除hash相关数据
			pipe.Del(ctx, "pathToHash:"+filePath) // 删除从路径到hashedKey的映射
			pipe.SRem(ctx, hashSizeKey, filePath) // 从fileHashSize集合中移除路径

			_, err = pipe.Exec(ctx)
			if err != nil {
				fmt.Printf("Error deleting keys for outdated record %s: %s\n", hashedKey, err)
			} else {
				fmt.Printf("Deleted outdated record: path=%s, hash=%s, size=%d\n", filePath, fileHash, fileInfo.Size)
			}
		}
	}
	return nil
}
