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

var debugFile = "9 uncensored あやみ旬果 ABP-557 & ABP-566.mp4"

// 获取文件信息
func getFileInfoFromRedis(rdb *redis.Client, ctx context.Context, hashedKey string) (FileInfo, error) {
	var fileInfo FileInfo
	value, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
	if err != nil {
		fmt.Printf("Error retrieving file info for hashedKey %s: %v\n", hashedKey, err)
		return fileInfo, err
	}

	buf := bytes.NewBuffer(value)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&fileInfo)
	fmt.Printf("Retrieved file info for hashedKey %s: %+v\n", hashedKey, fileInfo)
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
			log.Printf("Error getting file info from Redis for key %s: %s\n", hashedKey, err)
			continue
		}

		data[originalPath] = fileInfo
		foundData = true
	}

	if !foundData {
		log.Println("No data found in Redis.")
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
		log.Println("No lines to write to file.")
		return nil
	}

	log.Printf("Writing %d lines to file %s\n", len(lines), filepath.Join(dir, filename))
	return writeLinesToFile(filepath.Join(dir, filename), lines)
}

// 处理文件
func processFile(path string, typ os.FileMode, rdb *redis.Client, ctx context.Context, startTime int64) {
	defer atomic.AddInt32(&progressCounter, 1)

	// 生成文件路径的哈希作为键
	hashedKey := generateHash(path)

	// 检查Redis中是否已存在该文件的信息
	exists, err := rdb.Exists(ctx, "fileInfo:"+hashedKey).Result()
	if err != nil {
		log.Printf("Error checking existence in Redis for file %s: %s\n", path, err)
		return
	}
	if exists > 0 {
		return
	}

	log.Printf("File %s not found in Redis, processing.\n", path)

	info, err := os.Stat(path)
	if err != nil {
		log.Printf("Error stating file: %s, Error: %s\n", path, err)
		return
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(FileInfo{Size: info.Size(), ModTime: info.ModTime()}); err != nil {
		log.Printf("Error encoding: %s, File: %s\n", err, path)
		return
	}

	// 计算文件的SHA-512哈希值（只读取前4KB）
	fileHash, err := getFileHash(path, rdb, ctx)
	if err != nil {
		log.Printf("Error calculating hash for file %s: %s\n", path, err)
		return
	}

	// 调用saveFileInfoToRedis函数来保存文件信息到Redis，不需要完整文件的哈希值
	if err := saveFileInfoToRedis(rdb, ctx, hashedKey, path, buf, fileHash, ""); err != nil {
		log.Printf("Error saving file info to Redis for file %s: %s\n", path, err)
		return
	}

	if strings.Contains(path, debugFile) {
		log.Printf("Debug Info: Processed file %s with hash %s\n", path, fileHash)
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
	log.Printf("Processing directory: %s\n", path)
}

// 处理符号链接
func processSymlink(path string) {
	log.Printf("Processing symlink: %s\n", path)
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
		log.Println("Error creating file:", err)
		return
	}
	defer outputFile.Close()

	_, err = outputFile.WriteString(outputData.String())
	if err != nil {
		log.Println("Error writing to file:", err)
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
		log.Printf("Error getting size for %s: %v\n", fullPath, err)
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
	if err == nil && hash != "" && hash != "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e" {
		log.Printf("Retrieved hash from Redis for path: %s, key: %s, hash: %s", path, hashKey, hash)
		return hash, nil
	}

	if err != nil && err != redis.Nil {
		return "", err
	}

	// 计算哈希值
	log.Printf("Hash miss for path: %s, key: %s", path, hashKey)
	file, err := os.Open(path)
	if err != nil {
		log.Printf("Error opening file %s: %s", path, err)
		return "", err
	}
	defer file.Close()

	hasher := sha512.New()

	if limit > 0 {
		// 只读取前 limit 字节
		reader := io.LimitReader(file, limit)
		if _, err := io.Copy(hasher, reader); err != nil {
			log.Printf("Error copying file content for hash calculation %s: %s", path, err)
			return "", err
		}
	} else {
		// 读取整个文件
		buf := make([]byte, 4*1024*1024) // 每次读取 4MB 数据
		for {
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				log.Printf("Error reading file content for hash calculation %s: %s", path, err)
				return "", err
			}
			if n == 0 {
				break
			}
			if _, err := hasher.Write(buf[:n]); err != nil {
				log.Printf("Error writing to hasher for file %s: %s", path, err)
				return "", err
			}
		}
	}

	hashBytes := hasher.Sum(nil)
	hash = fmt.Sprintf("%x", hashBytes)

	log.Printf("Calculated hash for path %s: %x", path, hashBytes)

	// 将计算出的哈希值保存到Redis
	err = rdb.Set(ctx, hashKey, hash, 0).Err()
	if err != nil {
		log.Printf("Error setting hash in Redis for path %s: %s", path, err)
		return "", err
	}
	log.Printf("Computed and saved hash for path: %s, key: %s, hash: %s", path, hashKey, hash)
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
		log.Printf("Error calculating full hash for file %s: %s\n", path, err)
	} else {
		log.Printf("Full hash for file %s: %s\n", path, hash)
	}
	return hash, err
}
