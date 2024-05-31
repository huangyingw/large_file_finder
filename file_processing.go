// file_processing.go
package main

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/gob"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
)

// 获取文件信息
func getFileInfoFromRedis(rdb *redis.Client, ctx context.Context, hashedKey string) (FileInfo, error) {
	var fileInfo FileInfo
	value, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
	if err != nil {
		return fileInfo, err
	}

	buf := bytes.NewBuffer(value)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&fileInfo)
	return fileInfo, err
}

// 保存文件信息到文件
func saveToFile(dir, filename string, sortByModTime bool, rdb *redis.Client, ctx context.Context) error {
	iter := rdb.Scan(ctx, 0, "fileInfo:*", 0).Iterator()
	var data = make(map[string]FileInfo)

	foundData := false

	for iter.Next(ctx) {
		hashedKey := iter.Val()
		hashedKey = strings.TrimPrefix(hashedKey, "fileInfo:")

		originalPath, err := rdb.Get(ctx, "hashedKeyToPath:"+hashedKey).Result()
		if err != nil {
			continue
		}

		fileInfo, err := getFileInfoFromRedis(rdb, ctx, hashedKey)
		if err != nil {
			continue
		}

		data[originalPath] = fileInfo
		foundData = true
	}

	if !foundData {
		return nil
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

	if len(lines) == 0 {
		return nil
	}

	return writeLinesToFile(filepath.Join(dir, filename), lines)
}

// 处理文件
func processFile(path string, typ os.FileMode, rdb *redis.Client, ctx context.Context, startTime int64) {
	defer atomic.AddInt32(&progressCounter, 1)


	hashedKey := generateHash(path)

	// 检查Redis中是否已存在该文件的信息
	exists, err := rdb.Exists(ctx, "fileInfo:"+hashedKey).Result()
	if err != nil || exists > 0 {
		return
	}

	info, err := os.Stat(path)
	if err != nil {
		return
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(FileInfo{Size: info.Size(), ModTime: info.ModTime()}); err != nil {
		return
	}

	// 计算文件的SHA-512哈希值（只读取前4KB）
	fileHash, err := getFileHash(path, rdb, ctx)
	if err != nil {
		return
	}

	// 调用saveFileInfoToRedis函数来保存文件信息到Redis，不需要完整文件的哈希值
	if err := saveFileInfoToRedis(rdb, ctx, hashedKey, path, buf, fileHash, ""); err != nil {
		return
	}
}

// 格式化文件信息行
func formatFileInfoLine(fileInfo FileInfo, relativePath string, sortByModTime bool) string {
	if sortByModTime {
		return fmt.Sprintf("\"./%s\"", relativePath)
	}
	return fmt.Sprintf("%d,\"./%s\"", fileInfo.Size, relativePath)
}

const readLimit = 100 * 1024 // 100KB

// 处理目录
func processDirectory(path string) {
}

// 处理符号链接
func processSymlink(path string) {
}

// 处理关键词
func processKeyword(keyword string, keywordFiles []string, rdb *redis.Client, ctx context.Context, rootDir string) {
	// 对 keywordFiles 进行排序
	sort.Slice(keywordFiles, func(i, j int) bool {
		sizeI, _ := getFileSize(rdb, ctx, filepath.Join(rootDir, cleanPath(keywordFiles[i])))
		sizeJ, _ := getFileSize(rdb, ctx, filepath.Join(rootDir, cleanPath(keywordFiles[j])))
		return sizeI > sizeJ
	})

	// 准备要写入的数据
	var outputData strings.Builder
	outputData.WriteString(keyword + "\n")
	for _, filePath := range keywordFiles {
		fileSize, _ := getFileSize(rdb, ctx, filepath.Join(rootDir, cleanPath(filePath)))
		outputData.WriteString(fmt.Sprintf("%d,%s\n", fileSize, filePath))
	}

	// 创建并写入文件
	outputFilePath := filepath.Join(rootDir, keyword+".txt")
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return
	}
	defer outputFile.Close()

	_, err = outputFile.WriteString(outputData.String())
	if err != nil {
	}
}

// 清理和标准化路径
func cleanPath(path string) string {
	path = strings.Trim(path, `"`)
	if strings.HasPrefix(path, "./") {
		path = strings.TrimPrefix(path, "./")
	}
	return path
}

// 获取文件大小
func getFileSize(rdb *redis.Client, ctx context.Context, fullPath string) (int64, error) {
	size, err := getFileSizeFromRedis(rdb, ctx, fullPath)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// 获取文件哈希值
func getHash(path string, rdb *redis.Client, ctx context.Context, keyPrefix string, limit int64) (string, error) {
	hashedKey := generateHash(path)
	hashKey := keyPrefix + hashedKey

	// 尝试从Redis获取哈希值
	hash, err := rdb.Get(ctx, hashKey).Result()
	if err == nil && hash != "" {
		return hash, nil
	}

	if err != nil && err != redis.Nil {
		return "", err
	}

	// 计算哈希值
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha512.New()

	if limit > 0 {
		// 只读取前 limit 字节
		reader := io.LimitReader(file, limit)
		if _, err := io.Copy(hasher, reader); err != nil {
			return "", err
		}
	} else {
		// 读取整个文件
		buf := make([]byte, 4*1024*1024) // 每次读取 4MB 数据
		for {
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				return "", err
			}
			if n == 0 {
				break
			}
			if _, err := hasher.Write(buf[:n]); err != nil {
				return "", err
			}
		}
	}

	hashBytes := hasher.Sum(nil)
	hash = fmt.Sprintf("%x", hashBytes)

	// 将计算出的哈希值保存到Redis
	err = rdb.Set(ctx, hashKey, hash, 0).Err()
	if err != nil {
		return "", err
	}
	return hash, nil
}

// 获取文件哈希
func getFileHash(path string, rdb *redis.Client, ctx context.Context) (string, error) {
	const readLimit = 100 * 1024 // 100KB
	return getHash(path, rdb, ctx, "hashedKeyToFileHash:", readLimit)
}

// 获取完整文件哈希
func getFullFileHash(path string, rdb *redis.Client, ctx context.Context) (string, error) {
	const noLimit = -1 // No limit for full file hash
	hash, err := getHash(path, rdb, ctx, "hashedKeyToFullHash:", noLimit)
	if err != nil {
		log.Printf("Error calculating full hash for file %s: %v", path, err)
	} else {
		log.Printf("Full hash for file %s: %s", path, hash)
	}
	return hash, err
}
