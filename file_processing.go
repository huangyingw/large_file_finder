// file_processing.go
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

func getFileInfoFromRedis(rdb *redis.Client, ctx context.Context, hashedKey string) (FileInfo, error) {
	var fileInfo FileInfo
	value, err := rdb.Get(ctx, hashedKey).Bytes()
	if err != nil {
		return fileInfo, err
	}

	buf := bytes.NewBuffer(value)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&fileInfo)
	return fileInfo, err
}

func saveToFile(dir, filename string, sortByModTime bool, rdb *redis.Client, ctx context.Context) error {
	iter := rdb.Scan(ctx, 0, "*", 0).Iterator()
	var data = make(map[string]FileInfo)
	for iter.Next(ctx) {
		hashedKey := iter.Val()
		originalPath, err := rdb.Get(ctx, "path:"+hashedKey).Result()
		if err != nil {
			continue
		}

		fileInfo, err := getFileInfoFromRedis(rdb, ctx, hashedKey)
		if err == nil {
			data[originalPath] = fileInfo
		}
	}

	var lines []string
	var keys []string
	for k := range data {
		keys = append(keys, k)
	}
	sortKeys(keys, data, sortByModTime)
	for _, k := range keys {
		relativePath, _ := filepath.Rel(dir, k)
		line := formatFileInfoLine(data[k], relativePath, sortByModTime)
		lines = append(lines, line)
	}

	return writeLinesToFile(filepath.Join(dir, filename), lines)
}

func processFile(path string, typ os.FileMode, rdb *redis.Client, ctx context.Context, startTime int64) {
	// Update progress counter atomically
	atomic.AddInt32(&progressCounter, 1)

	// 生成文件路径的哈希作为键
	hashedKey := generateHash(path)

	// 检查Redis中是否已存在该文件的信息
	exists, err := rdb.Exists(ctx, hashedKey).Result()
	if err != nil {
		fmt.Printf("Error checking existence in Redis for file %s: %s\n", path, err)
		return
	}
	if exists > 0 {
		// 文件已存在于Redis中，更新其更新时间戳
		_, err := rdb.Set(ctx, "updateTime:"+hashedKey, startTime, 0).Result()
		if err != nil {
			fmt.Printf("Error updating updateTime for file %s: %s\n", path, err)
		}
		return
	}

	info, err := os.Stat(path)
	if err != nil {
		fmt.Printf("Error stating file: %s, Error: %s\n", path, err)
		return
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(FileInfo{Size: info.Size(), ModTime: info.ModTime()}); err != nil {
		fmt.Printf("Error encoding: %s, File: %s\n", err, path)
		return
	}

	// 计算文件的SHA-256哈希值
	fileHash, err := calculateFileHash(path)
	if err != nil {
		fmt.Printf("Error calculating hash for file %s: %s\n", path, err)
		return
	}

	// 使用管道批量处理Redis命令
	pipe := rdb.Pipeline()

	// 这里我们添加命令到管道，但不立即检查错误
	pipe.Set(ctx, hashedKey, buf.Bytes(), 0)
	pipe.Set(ctx, "path:"+hashedKey, path, 0)
	pipe.Set(ctx, "updateTime:"+hashedKey, startTime, 0)
	pipe.Set(ctx, "hash:"+hashedKey, fileHash, 0) // 存储文件哈希值

	if _, err = pipe.Exec(ctx); err != nil {
		fmt.Printf("Error executing pipeline for file: %s: %s\n", path, err)
		return
	}
}

func formatFileInfoLine(fileInfo FileInfo, relativePath string, sortByModTime bool) string {
	if sortByModTime {
		utcTimestamp := fileInfo.ModTime.UTC().Unix()
		return fmt.Sprintf("%d,\"./%s\"", utcTimestamp, relativePath)
	}
	return fmt.Sprintf("%d,\"./%s\"", fileInfo.Size, relativePath)
}

// calculateFileHash 计算文件的SHA-256哈希值
func calculateFileHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	const readLimit = 4 * 1024 // 限制读取的数据量为 4 KB
	reader := bufio.NewReaderSize(file, readLimit)
	limitedReader := io.LimitReader(reader, readLimit)

	hasher := sha256.New()
	if _, err := io.Copy(hasher, limitedReader); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func processDirectory(path string) {
	// 处理目录的逻辑
	fmt.Printf("Processing directory: %s\n", path)
	// 可能的操作：遍历目录下的文件等
}
func processSymlink(path string) {
	// 处理软链接的逻辑
	fmt.Printf("Processing symlink: %s\n", path)
	// 可能的操作：解析软链接，获取实际文件等
}
