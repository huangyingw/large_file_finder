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
		log.Printf("File %s already exists in Redis, skipping processing.\n", path)
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
	fileHash, err := calculateFileHash(path, false)
	if err != nil {
		log.Printf("Error calculating hash for file %s: %s\n", path, err)
		return
	}

	// 调用saveFileInfoToRedis函数来保存文件信息到Redis，不需要完整文件的哈希值
	if err := saveFileInfoToRedis(rdb, ctx, hashedKey, path, buf, fileHash, ""); err != nil {
		log.Printf("Error saving file info to Redis for file %s: %s\n", path, err)
		return
	}
}

func formatFileInfoLine(fileInfo FileInfo, relativePath string, sortByModTime bool) string {
	if sortByModTime {
		return fmt.Sprintf("\"./%s\"", relativePath)
	}
	return fmt.Sprintf("%d,\"./%s\"", fileInfo.Size, relativePath)
}

const readLimit = 100 * 1024 // 100KB

func calculateFileHash(path string, fullRead bool) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha512.New()
	if fullRead {
		log.Printf("Calculating full hash for file: %s\n", path)
		if _, err := io.Copy(hasher, file); err != nil {
			return "", err
		}
	} else {
		reader := io.LimitReader(file, readLimit)
		if _, err := io.Copy(hasher, reader); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func processDirectory(path string) {
	log.Printf("Processing directory: %s\n", path)
}

func processSymlink(path string) {
	log.Printf("Processing symlink: %s\n", path)
}

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
	defer outputFile.Close() // 确保文件会被关闭

	_, err = outputFile.WriteString(outputData.String())
	if err != nil {
		log.Println("Error writing to file:", err)
	}
}

// cleanPath 用于清理和标准化路径
func cleanPath(path string) string {
	path = strings.Trim(path, `"`)
	if strings.HasPrefix(path, "./") {
		path = strings.TrimPrefix(path, "./")
	}
	return path
}

func getFileSize(rdb *redis.Client, ctx context.Context, fullPath string) (int64, error) {
	size, err := getFileSizeFromRedis(rdb, ctx, fullPath)
	if err != nil {
		log.Printf("Error getting size for %s: %v\n", fullPath, err)
		return 0, err
	}
	return size, nil
}
