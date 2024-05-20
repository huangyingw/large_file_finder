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
	"sync"
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

// hashSize 和 fileInfo 结构体定义
type hashSize struct {
	key  string
	size int64
}

// FileInfo holds file information
type FileInfo struct {
	Size    int64
	ModTime time.Time
}

type fileInfo struct {
	name        string
	path        string
	buf         bytes.Buffer
	startTime   int64
	fileHash    string
	hashSizeKey string
	fullHash    string
	line        string
	header      string
	FileInfo    // 嵌入已有的FileInfo结构体
}

// 扫描Redis中的fileHashSize键
func scanFileHashSizeKeys(rdb *redis.Client, ctx context.Context) ([]hashSize, error) {
	iter := rdb.Scan(ctx, 0, "fileHashSize:*", 0).Iterator()
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
		return nil, err
	}

	fmt.Printf("Scanned %d keys\n", keyCount)
	return hashSizes, nil
}

// 处理每个fileHashSize键并查找重复文件
func processFileHashSizeKey(rootDir string, hs hashSize, rdb *redis.Client, ctx context.Context, processedFullHashes map[string]bool) (int, error) {
	filePaths, err := rdb.SMembers(ctx, hs.key).Result()
	if err != nil {
		return 0, fmt.Errorf("Error getting file paths for key %s: %s", hs.key, err)
	}

	fileCount := 0

	if len(filePaths) > 1 {
		header := fmt.Sprintf("Duplicate files for fileHashSizeKey %s:", hs.key)
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
			var buf bytes.Buffer
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

			info, err := os.Stat(fullPath) // 获取文件信息
			if err != nil {
				fmt.Printf("Error stating file %s: %s\n", fullPath, err)
				continue
			}

			infoStruct := fileInfo{
				name:        fileName,
				path:        fullPath,
				buf:         buf,
				startTime:   time.Now().Unix(),
				fileHash:    hashedKey,
				hashSizeKey: "fileHashSize:" + fullHash + "_" + strconv.FormatInt(info.Size(), 10),
				fullHash:    fullHash,
				line:        fmt.Sprintf("%d,\"./%s\"", hs.size, relativePath),
				header:      header,
				FileInfo:    FileInfo{Size: info.Size(), ModTime: info.ModTime()},
			}
			hashes[fullHash] = append(hashes[fullHash], infoStruct)
			fileCount++
		}
		for fullHash, infos := range hashes {
			if len(infos) > 1 {
				// 检查fullHash是否已处理过
				if !processedFullHashes[fullHash] {
					// 将重复文件的信息存储到Redis集合
					processedFullHashes[fullHash] = true
					for _, info := range infos {
						err := saveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
						if err != nil {
							fmt.Printf("Error saving duplicate file info to Redis for hash %s: %s\n", fullHash, err)
						}
					}
				}
			}
		}
	}
	return fileCount, nil
}

// 主函数
func findAndLogDuplicates(rootDir string, outputFile string, rdb *redis.Client, ctx context.Context) error {
	fmt.Println("Starting to find duplicates...")

	hashSizes, err := scanFileHashSizeKeys(rdb, ctx)
	if err != nil {
		return err
	}

	fileCount := 0

	// 用于存储已处理的文件哈希
	processedFullHashes := make(map[string]bool)

	var wg sync.WaitGroup // 等待 goroutine 完成
	var mu sync.Mutex     // 用于保护对 fileCount 的并发访问

	for _, hs := range hashSizes {
		wg.Add(1)
		go func(hs hashSize) {
			defer wg.Done()

			count, err := processFileHashSizeKey(rootDir, hs, rdb, ctx, processedFullHashes)
			if err != nil {
				fmt.Printf("Error processing fileHashSize key %s: %s\n", hs.key, err)
				return
			}
			mu.Lock()
			fileCount += count
			mu.Unlock()
		}(hs)
	}

	wg.Wait()

	fmt.Printf("Processed %d files\n", fileCount)

	if fileCount == 0 {
		fmt.Println("No duplicates found.")
		return nil
	}

	// 从 Redis 中读取重复文件信息并输出到文件
	return writeDuplicateFilesToFile(rootDir, outputFile, rdb, ctx)
}

func writeDuplicateFilesToFile(rootDir string, outputFile string, rdb *redis.Client, ctx context.Context) error {
	file, err := os.Create(filepath.Join(rootDir, outputFile))
	if err != nil {
		return fmt.Errorf("Error creating output file: %s", err)
	}
	defer file.Close()

	iter := rdb.Scan(ctx, 0, "duplicateFiles:*", 0).Iterator()
	for iter.Next(ctx) {
		duplicateFilesKey := iter.Val()

		// 获取 fullHash
		fullHash := strings.TrimPrefix(duplicateFilesKey, "duplicateFiles:")

		// 获取重复文件列表，按文件名长度排序
		duplicateFiles, err := rdb.ZRevRange(ctx, duplicateFilesKey, 0, -1).Result()
		if err != nil {
			fmt.Printf("Error retrieving duplicate files for key %s: %s\n", duplicateFilesKey, err)
			continue
		}

		if len(duplicateFiles) > 1 {
			header := fmt.Sprintf("Duplicate files for hash %s:\n", fullHash)
			if _, err := file.WriteString(header); err != nil {
				fmt.Printf("Error writing header to file %s: %s\n", outputFile, err)
				continue
			}
			for _, duplicateFile := range duplicateFiles {
				// 获取文件信息
				hashedKey, err := getHashedKeyFromPath(rdb, ctx, duplicateFile)
				if err != nil {
					fmt.Printf("Error retrieving hashed key for path %s: %s\n", duplicateFile, err)
					continue
				}
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

				// 获取相对路径
				relativePath, err := filepath.Rel(rootDir, duplicateFile)
				if err != nil {
					fmt.Printf("Error getting relative path for %s: %s\n", duplicateFile, err)
					continue
				}

				// 使用 filepath.Clean 来规范化路径
				cleanPath := filepath.Clean(relativePath)
				line := fmt.Sprintf("%d,\"./%s\"\n", fileInfo.Size, cleanPath)
				if _, err := file.WriteString(line); err != nil {
					fmt.Printf("Error writing file path to file %s: %s\n", outputFile, err)
					continue
				}
			}
			if _, err := file.WriteString("\n"); err != nil {
				fmt.Printf("Error writing newline to file %s: %s\n", outputFile, err)
				continue
			}
		}
	}

	if err := iter.Err(); err != nil {
		fmt.Printf("Error during iteration: %s\n", err)
		return err
	}

	fmt.Printf("Duplicates written to %s\n", filepath.Join(rootDir, outputFile))
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
