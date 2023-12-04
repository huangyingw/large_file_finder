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
	"strings"
)

// Generate a SHA-256 hash for the given string
func generateHash(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return hex.EncodeToString(hasher.Sum(nil))
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

			// 获取与文件相关的数据
			fileInfoData, err := rdb.Get(ctx, hashedKey).Bytes()
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

			// 获取文件路径
			filePath, err := rdb.Get(ctx, "path:"+hashedKey).Result()
			if err != nil {
				fmt.Printf("Error retrieving filePath for key %s: %s\n", hashedKey, err)
				continue
			}

			// 删除记录
			pipe := rdb.Pipeline()
			pipe.Del(ctx, updateTimeKey)     // 删除updateTime键
			pipe.Del(ctx, hashedKey)         // 删除与hashedKey相关的数据
			pipe.Del(ctx, "path:"+hashedKey) // 删除与path:hashedKey相关的数据

			_, err = pipe.Exec(ctx)
			if err != nil {
				fmt.Printf("Error deleting keys for outdated record %s: %s\n", hashedKey, err)
			} else {
				fmt.Printf("Deleted outdated record: path=%s, size=%d, modTime=%s\n", filePath, fileInfo.Size, fileInfo.ModTime)
			}
		}
	}
	return nil
}

// getAllFileHashes 从Redis中检索所有文件的哈希值及其对应的路径。
func getAllFileHashes(rdb *redis.Client, ctx context.Context) (map[string][]string, error) {
	fileHashes := make(map[string][]string)

	// Scan用于查找所有哈希键
	iter := rdb.Scan(ctx, 0, "hash:*", 0).Iterator()
	for iter.Next(ctx) {
		hashKey := iter.Val()
		// 获取原始的hashedKey
		hashedKey := strings.TrimPrefix(hashKey, "hash:")

		// 获取与hashedKey相关的文件路径
		filePath, err := rdb.Get(ctx, "path:"+hashedKey).Result()
		if err != nil {
			// 处理错误
			continue
		}

		// 提取哈希值
		hashValue := strings.TrimPrefix(hashKey, "hash:")
		fileHashes[hashValue] = append(fileHashes[hashValue], filePath)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return fileHashes, nil
}
