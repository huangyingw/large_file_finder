// file_processing.go
package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"path/filepath"
	"sync/atomic"
)

func saveToFile(dir, filename string, sortByModTime bool, rdb *redis.Client, ctx context.Context) error {
	file, err := os.Create(filepath.Join(dir, filename))
	if err != nil {
		return err
	}
	defer file.Close()

	iter := rdb.Scan(ctx, 0, "*", 0).Iterator()
	var data = make(map[string]FileInfo)
	for iter.Next(ctx) {
		hashedKey := iter.Val()
		originalPath, err := rdb.Get(ctx, "path:"+hashedKey).Result()
		if err != nil {
			continue
		}
		value, err := rdb.Get(ctx, hashedKey).Bytes()
		if err != nil {
			continue
		}
		var fileInfo FileInfo
		buf := bytes.NewBuffer(value)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&fileInfo); err == nil {
			data[originalPath] = fileInfo
		}
	}

	var keys []string
	for k := range data {
		keys = append(keys, k)
	}

	sortKeys(keys, data, sortByModTime)

	for _, k := range keys {
		relativePath, _ := filepath.Rel(dir, k)
		if sortByModTime {
			utcTimestamp := data[k].ModTime.UTC().Unix()
			fmt.Fprintf(file, "%d,\"./%s\"\n", utcTimestamp, relativePath)
		} else {
			fmt.Fprintf(file, "%d,\"./%s\"\n", data[k].Size, relativePath)
		}
	}
	return nil
}

func processFile(path string, typ os.FileMode, rdb *redis.Client, ctx context.Context) {
	// Update progress counter atomically
	atomic.AddInt32(&progressCounter, 1)
	// 生成文件路径的哈希作为键
	hashedKey := generateHash(path)

	// 检查Redis中是否已存在该文件的信息
	exists, err := rdb.Exists(ctx, hashedKey).Result()
	if err != nil {
		// 处理错误情况
		return
	}
	if exists > 0 {
		// 文件已存在于Redis中，跳过处理
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

	// Generate hash for the file path
	hashedKey = generateHash(path)

	// 使用管道批量处理Redis命令
	pipe := rdb.Pipeline()

	// 这里我们添加命令到管道，但不立即检查错误
	pipe.Set(ctx, hashedKey, buf.Bytes(), 0)
	pipe.Set(ctx, "path:"+hashedKey, path, 0)

	if _, err = pipe.Exec(ctx); err != nil {
		fmt.Printf("Error executing pipeline for file: %s: %s\n", path, err)
		return
	}

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
