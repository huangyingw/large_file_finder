// utils.go
// 该文件包含用于整个应用程序的通用工具函数。

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func loadExcludePatterns(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var patterns []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		pattern := scanner.Text()
		fmt.Printf("Loaded exclude pattern: %s\n", pattern) // 打印每个加载的模式
		patterns = append(patterns, pattern)
	}
	return patterns, scanner.Err()
}

func sortKeys(keys []string, data map[string]FileInfo, sortByModTime bool) {
	if sortByModTime {
		sort.Slice(keys, func(i, j int) bool {
			return data[keys[i]].ModTime.After(data[keys[j]].ModTime)
		})
	} else {
		sort.Slice(keys, func(i, j int) bool {
			return data[keys[i]].Size > data[keys[j]].Size
		})
	}
}

func compileExcludePatterns(filename string) ([]*regexp.Regexp, error) {
	excludePatterns, err := loadExcludePatterns(filename)
	if err != nil {
		return nil, err
	}

	excludeRegexps := make([]*regexp.Regexp, len(excludePatterns))
	for i, pattern := range excludePatterns {
		regexPattern := strings.Replace(pattern, "*", ".*", -1)
		excludeRegexps[i], err = regexp.Compile(regexPattern)
		if err != nil {
			return nil, fmt.Errorf("Invalid regex pattern '%s': %v", regexPattern, err)
		}
	}
	return excludeRegexps, nil
}

func performSaveOperation(rootDir, filename string, sortByModTime bool, rdb *redis.Client, ctx context.Context) {
	if err := saveToFile(rootDir, filename, sortByModTime, rdb, ctx); err != nil {
		fmt.Printf("Error saving to %s: %s\n", filepath.Join(rootDir, filename), err)
	} else {
		fmt.Printf("Saved data to %s\n", filepath.Join(rootDir, filename))
	}
}

func writeLinesToFile(filename string, lines []string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, line := range lines {
		if _, err := fmt.Fprintln(file, line); err != nil {
			return err
		}
	}
	return nil
}

func findAndLogDuplicates(rootDir string, outputFile string, rdb *redis.Client, ctx context.Context) error {
	fmt.Println("Starting to find duplicates...")

	hashes, err := getAllFileHashes(rdb, ctx)
	if err != nil {
		fmt.Printf("Error getting file hashes: %s\n", err)
		return err
	}

	// 创建一个切片来存储哈希和对应的文件大小
	type hashSize struct {
		hash string
		size int64
	}
	var hashSizes []hashSize
	for hash, paths := range hashes {
		if len(paths) > 1 {
			// 假设每个哈希下的第一个路径代表整个组
			size, _ := getFileSizeFromRedis(rdb, ctx, generateHash(paths[0])) // 直接使用第一个路径
			hashSizes = append(hashSizes, hashSize{hash: hash, size: size})
		}
	}

	// 根据文件大小排序哈希
	sort.Slice(hashSizes, func(i, j int) bool {
		return hashSizes[i].size < hashSizes[j].size
	})

	var lines []string
	for _, hs := range hashSizes {
		paths := hashes[hs.hash]
		lines = append(lines, fmt.Sprintf("Duplicate files for hash %s:", hs.hash))
		lines = append(lines, paths...)
	}

	if len(lines) == 0 {
		fmt.Println("No duplicates found.")
		return nil
	}

	outputFile = filepath.Join(rootDir, outputFile)
	fmt.Printf("Writing duplicates to file '%s'...\n", outputFile)
	err = writeLinesToFile(outputFile, lines)
	if err != nil {
		fmt.Printf("Error writing to file '%s': %s\n", outputFile, err)
		return err
	}

	fmt.Printf("Duplicates written to '%s'\n", outputFile)
	return nil
}

func getFileSizeFromRedis(rdb *redis.Client, ctx context.Context, hashedKey string) (int64, error) {
	fileInfoData, err := rdb.Get(ctx, hashedKey).Bytes()
	if err != nil {
		return 0, err
	}

	var fileInfo FileInfo
	err = json.Unmarshal(fileInfoData, &fileInfo)
	if err != nil {
		return 0, err
	}

	return fileInfo.Size, nil
}
