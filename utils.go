// utils.go
// 该文件包含用于整个应用程序的通用工具函数。

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
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

	// Scan用于查找所有fileHashSize键
	iter := rdb.Scan(ctx, 0, "fileHashSize:*", 0).Iterator()
	type hashSize struct {
		key  string
		size int64
	}
	type fileInfo struct {
		name   string
		line   string
		header string
	}

	var hashSizes []hashSize
	keyCount := 0

	for iter.Next(ctx) {
		fileHashSizeKey := iter.Val()
		parts := strings.Split(fileHashSizeKey, "_")
		if len(parts) != 2 {
			fmt.Printf("Invalid key format: %s\n", fileHashSizeKey)
			continue
		}
		size, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing size from key %s: %s\n", fileHashSizeKey, err)
			continue
		}

		hashSizes = append(hashSizes, hashSize{key: fileHashSizeKey, size: size})
		keyCount++
	}

	if err := iter.Err(); err != nil {
		fmt.Println("Iterator error:", err)
		return err
	}

	fmt.Printf("Scanned %d keys\n", keyCount)

	// 根据文件大小排序哈希
	sort.Slice(hashSizes, func(i, j int) bool {
		return hashSizes[i].size > hashSizes[j].size
	})

	var duplicateGroups [][]fileInfo
	fileCount := 0

	for _, hs := range hashSizes {
		filePaths, err := rdb.SMembers(ctx, hs.key).Result()
		if err != nil {
			fmt.Printf("Error getting file paths for key %s: %s\n", hs.key, err)
			continue
		}

		if len(filePaths) > 1 {
			header := fmt.Sprintf("Duplicate files for fileHashSizeKey %s:", hs.key)
			group := []fileInfo{}
			hashes := make(map[string][]fileInfo)
			for _, fullPath := range filePaths {
				relativePath, err := filepath.Rel(rootDir, fullPath)
				if err != nil {
					fmt.Printf("Error converting to relative path: %s\n", err)
					continue
				}
				fileName := filepath.Base(relativePath)

				// 获取或计算完整文件的SHA-256哈希值
				hashedKey := generateHash(fullPath)
				fullHash, err := rdb.Get(ctx, "fullHash:"+hashedKey).Result()
				if err == redis.Nil {
					fullHash, err = calculateFileHash(fullPath, true)
					if err != nil {
						fmt.Printf("Error calculating full hash for file %s: %s\n", fullPath, err)
						continue
					}

					// 获取文件信息并编码
					info, err := os.Stat(fullPath)
					if err != nil {
						fmt.Printf("Error stating file: %s, Error: %s\n", fullPath, err)
						continue
					}

					var buf bytes.Buffer
					enc := gob.NewEncoder(&buf)
					if err := enc.Encode(FileInfo{Size: info.Size(), ModTime: info.ModTime()}); err != nil {
						fmt.Printf("Error encoding: %s, File: %s\n", err, fullPath)
						continue
					}

					// 构造包含前缀的hashSizeKey
					hashSizeKey := "fileHashSize:" + fullHash + "_" + strconv.FormatInt(info.Size(), 10)

					// 调用saveFileInfoToRedis函数来保存文件信息到Redis
					if err := saveFileInfoToRedis(rdb, ctx, hashedKey, fullPath, buf, time.Now().Unix(), fullHash, hashSizeKey, fullHash); err != nil {
						fmt.Printf("Error saving file info to Redis for file %s: %s\n", fullPath, err)
						continue
					}
				} else if err != nil {
					fmt.Printf("Error getting full hash for file %s from Redis: %s\n", fullPath, err)
					continue
				}

				infoStruct := fileInfo{
					name: fileName,
					line: fmt.Sprintf("%d,\"./%s\"", hs.size, relativePath),
				}
				hashes[fullHash] = append(hashes[fullHash], infoStruct)
				fileCount++
			}
			for _, infos := range hashes {
				if len(infos) > 1 {
					group = append(group, infos...)
				}
			}
			if len(group) > 0 {
				// 在分组前添加组标题
				duplicateGroups = append(duplicateGroups, append([]fileInfo{{line: header}}, group...))
			}
		}
	}

	fmt.Printf("Processed %d files\n", fileCount)

	if len(duplicateGroups) == 0 {
		fmt.Println("No duplicates found.")
		return nil
	}

	// 按文件名长度排序每个组内的文件，并将标题放在前面
	for _, group := range duplicateGroups {
		sort.Slice(group[1:], func(i, j int) bool {
			return len(group[i+1].name) > len(group[j+1].name)
		})
	}

	var sortedLines []string
	for _, group := range duplicateGroups {
		for _, fi := range group {
			sortedLines = append(sortedLines, fi.line)
		}
	}

	outputFile = filepath.Join(rootDir, outputFile)
	err := writeLinesToFile(outputFile, sortedLines)
	if err != nil {
		fmt.Printf("Error writing to file %s: %s\n", outputFile, err)
		return err
	}

	fmt.Printf("Duplicates written to %s\n", outputFile)
	return nil
}

func getFileSizeFromRedis(rdb *redis.Client, ctx context.Context, fullPath string) (int64, error) {
	// 首先从 fullPath 获取 hashedKey
	hashedKey, err := getHashedKeyFromPath(rdb, ctx, fullPath)
	if err != nil {
		return 0, fmt.Errorf("error getting hashed key for %s: %w", fullPath, err)
	}

	// 然后使用 hashedKey 从 Redis 获取文件信息
	fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
	if err != nil {
		return 0, fmt.Errorf("error getting file info for hashed key %s: %w", hashedKey, err)
	}

	var fileInfo FileInfo
	buf := bytes.NewBuffer(fileInfoData)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&fileInfo); err != nil {
		return 0, fmt.Errorf("error decoding file info for hashed key %s: %w", hashedKey, err)
	}

	return fileInfo.Size, nil
}

func getHashedKeyFromPath(rdb *redis.Client, ctx context.Context, path string) (string, error) {
	return rdb.Get(ctx, "pathToHash:"+path).Result()
}

// extractFileName extracts the file name from a given file path.
func extractFileName(filePath string) string {
	return strings.ToLower(filepath.Base(filePath))
}

var pattern = regexp.MustCompile(`\b(?:\d{2}\.\d{2}\.\d{2}|(?:\d+|[a-z]+(?:\d+[a-z]*)?))\b`)

func extractKeywords(fileNames []string) []string {
	workerCount := 100
	// 创建自己的工作池
	taskQueue, poolWg, stopPool := NewWorkerPool(workerCount)

	keywordsCh := make(chan string, len(fileNames)*10) // 假设每个文件名大约有10个关键词

	for _, fileName := range fileNames {
		poolWg.Add(1) // 确保在任务开始前递增计数
		taskQueue <- func(name string) Task {
			return func() {
				defer func() {
					poolWg.Done() // 任务结束时递减计数
				}()

				nameWithoutExt := strings.TrimSuffix(name, filepath.Ext(name))
				matches := pattern.FindAllString(nameWithoutExt, -1)
				for _, match := range matches {
					keywordsCh <- match
				}
			}
		}(fileName)
	}

	stopPool() // 使用停止函数来关闭任务队列

	// 关闭通道的逻辑保持不变
	go func() {
		poolWg.Wait()
		close(keywordsCh)
	}()

	// 收集关键词
	keywordsMap := make(map[string]struct{})
	for keyword := range keywordsCh {
		keywordsMap[keyword] = struct{}{}
	}

	// 将map转换为slice
	var keywords []string
	for keyword := range keywordsMap {
		keywords = append(keywords, keyword)
	}

	return keywords
}

func findCloseFiles(fileNames, filePaths, keywords []string) map[string][]string {
	closeFiles := make(map[string][]string)

	for _, kw := range keywords {
		for i, fileName := range fileNames {
			if strings.Contains(strings.ToLower(fileName), strings.ToLower(kw)) {
				closeFiles[kw] = append(closeFiles[kw], filePaths[i])
			}
		}
	}

	return closeFiles
}
