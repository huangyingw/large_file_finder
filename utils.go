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
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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
	log.Printf("Starting save operation to %s\n", filepath.Join(rootDir, filename))
	if err := saveToFile(rootDir, filename, sortByModTime, rdb, ctx); err != nil {
		log.Printf("Error saving to %s: %s\n", filepath.Join(rootDir, filename), err)
	} else {
		log.Printf("Saved data to %s\n", filepath.Join(rootDir, filename))
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

// FileInfo holds file information
type FileInfo struct {
	Size    int64
	ModTime time.Time
}

type fileInfo struct {
	name      string
	path      string
	buf       bytes.Buffer
	startTime int64
	fileHash  string
	fullHash  string
	line      string
	header    string
	FileInfo  // 嵌入已有的FileInfo结构体
}

func processFileHash(rootDir string, fileHash string, filePaths []string, rdb *redis.Client, ctx context.Context, processedFullHashes map[string]bool) (int, error) {
	fileCount := 0

	if len(filePaths) > 1 {
		header := fmt.Sprintf("Duplicate files for hash %s:", fileHash)
		hashes := make(map[string][]fileInfo)
		for _, fullPath := range filePaths {
			// 确保只处理rootDir下的文件
			if !strings.HasPrefix(fullPath, rootDir) {
				continue
			}

			// 检查文件是否存在
			if _, err := os.Stat(fullPath); os.IsNotExist(err) {
				continue
			}

			semaphore <- struct{}{} // 获取一个信号量
			relativePath, err := filepath.Rel(rootDir, fullPath)
			if err != nil {
				<-semaphore // 释放信号量
				continue
			}
			fileName := filepath.Base(relativePath)

			// 获取或计算完整文件的SHA-512哈希值
			fullHash, err := getFullFileHash(fullPath, rdb, ctx)
			if err != nil {
				<-semaphore // 释放信号量
				continue
			}

			// 计算文件的SHA-512哈希值（只读取前4KB）
			fileHash, err := getFileHash(fullPath, rdb, ctx)
			if err != nil {
				<-semaphore // 释放信号量
				continue
			}

			// 获取文件信息并编码
			info, err := os.Stat(fullPath)
			if err != nil {
				<-semaphore // 释放信号量
				continue
			}

			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			if err := enc.Encode(FileInfo{Size: info.Size(), ModTime: info.ModTime()}); err != nil {
				<-semaphore // 释放信号量
				continue
			}

			// 调用saveFileInfoToRedis函数来保存文件信息到Redis
			if err := saveFileInfoToRedis(rdb, ctx, generateHash(fullPath), fullPath, buf, fileHash, fullHash); err != nil {
				<-semaphore // 释放信号量
				continue
			}

			infoStruct := fileInfo{
				name:      fileName,
				path:      fullPath,
				buf:       buf,
				startTime: time.Now().Unix(),
				fileHash:  fileHash,
				fullHash:  fullHash,
				line:      fmt.Sprintf("%d,\"./%s\"", info.Size(), relativePath),
				header:    header,
				FileInfo:  FileInfo{Size: info.Size(), ModTime: info.ModTime()},
			}
			hashes[fullHash] = append(hashes[fullHash], infoStruct)
			fileCount++
			<-semaphore // 释放信号量
		}

		var saveErr error
		for fullHash, infos := range hashes {
			if len(infos) > 1 {
				mu.Lock()
				if !processedFullHashes[fullHash] {
					for _, info := range infos {
						log.Printf("Saving duplicate file info to Redis for file: %s", info.path)
						err := saveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
						if err != nil {
							log.Printf("Error saving duplicate file info to Redis for file: %s, error: %v", info.path, err)
							saveErr = err
						} else {
							log.Printf("Successfully saved duplicate file info to Redis for file: %s", info.path)
						}
					}
					processedFullHashes[fullHash] = true
				}
				mu.Unlock()
			}
		}

		if saveErr != nil {
			return fileCount, saveErr
		}
	}

	return fileCount, nil
}

// 主函数
func findAndLogDuplicates(rootDir string, rdb *redis.Client, ctx context.Context, maxDuplicates int) error {
	log.Println("Scanning file hashes")
	fileHashes, err := scanFileHashes(rdb, ctx)
	if err != nil {
		return err
	}

	fileCount := 0

	// 用于存储已处理的文件哈希
	processedFullHashes := make(map[string]bool)

	workerCount := 500
	var stopProcessing bool
	taskQueue, poolWg, stopFunc, _ := NewWorkerPool(workerCount, &stopProcessing)

	for fileHash, filePaths := range fileHashes {
		fileCount += 1
		if fileCount >= maxDuplicates {
			stopProcessing = true
			break
		}

		semaphore <- struct{}{} // 获取一个信号量

		taskQueue <- func(fileHash string, filePaths []string) Task {
			return func() {
				defer func() {
					<-semaphore // 释放信号量
				}()

				if stopProcessing {
					return
				}

				_, err := processFileHash(rootDir, fileHash, filePaths, rdb, ctx, processedFullHashes)
				if err != nil {
					log.Printf("Error processing file hash %s: %s\n", fileHash, err)
					return
				}
			}
		}(fileHash, filePaths)
	}

	stopFunc()
	poolWg.Wait()

	log.Printf("Total duplicates found: %d\n", fileCount)

	if fileCount == 0 {
		return nil
	}

	return nil
}

func scanFileHashes(rdb *redis.Client, ctx context.Context) (map[string][]string, error) {
	iter := rdb.Scan(ctx, 0, "fileHashToPathSet:*", 0).Iterator()
	fileHashes := make(map[string][]string)
	for iter.Next(ctx) {
		hashKey := iter.Val()
		fileHash := strings.TrimPrefix(hashKey, "fileHashToPathSet:")
		duplicateFiles, err := rdb.SMembers(ctx, hashKey).Result()
		if err != nil {
			return nil, fmt.Errorf("error retrieving duplicate files for key %s: %w", hashKey, err)
		}
		// 只添加包含多个文件路径的条目
		if len(duplicateFiles) > 1 {
			fileHashes[fileHash] = duplicateFiles
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("error during iteration: %w", err)
	}
	return fileHashes, nil
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

		// 获取重复文件列表，按文件名长度（score）排序
		duplicateFiles, err := rdb.ZRange(ctx, duplicateFilesKey, 0, -1).Result()
		if err != nil {
			continue
		}

		if len(duplicateFiles) > 1 {
			header := fmt.Sprintf("Duplicate files for fullHash %s:\n", fullHash)
			if _, err := file.WriteString(header); err != nil {
				continue
			}
			for _, duplicateFile := range duplicateFiles {
				// 获取文件信息
				hashedKey, err := getHashedKeyFromPath(rdb, ctx, duplicateFile)
				if err != nil {
					continue
				}
				fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
				if err != nil {
					continue
				}

				// 解码文件信息
				var fileInfo FileInfo
				buf := bytes.NewBuffer(fileInfoData)
				dec := gob.NewDecoder(buf)
				if err := dec.Decode(&fileInfo); err != nil {
					continue
				}

				// 获取相对路径
				relativePath, err := filepath.Rel(rootDir, duplicateFile)
				if err != nil {
					continue
				}

				// 使用 filepath.Clean 来规范化路径
				cleanPath := filepath.Clean(relativePath)
				line := fmt.Sprintf("%d,\"./%s\"\n", fileInfo.Size, cleanPath)
				if _, err := file.WriteString(line); err != nil {
					continue
				}
			}
			if _, err := file.WriteString("\n"); err != nil {
				continue
			}
		}
	}

	if err := iter.Err(); err != nil {
		return err
	}

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
	hashedKey, err := rdb.Get(ctx, "pathToHashedKey:"+filepath.Clean(path)).Result()
	return hashedKey, err
}

// extractFileName extracts the file name from a given file path.
func extractFileName(filePath string) string {
	return strings.ToLower(filepath.Base(filePath))
}

var pattern = regexp.MustCompile(`\b(?:\d{2}\.\d{2}\.\d{2}|(?:\d+|[a-z]+(?:\d+[a-z]*)?))\b`)

func extractKeywords(fileNames []string, stopProcessing *bool) []string {
	workerCount := 100
	// 创建自己的工作池
	taskQueue, poolWg, stopFunc, _ := NewWorkerPool(workerCount, stopProcessing)

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

	stopFunc() // 使用停止函数来关闭任务队列

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

func deleteDuplicateFiles(rootDir string, rdb *redis.Client, ctx context.Context) error {
	iter := rdb.Scan(ctx, 0, "duplicateFiles:*", 0).Iterator()
	for iter.Next(ctx) {
		duplicateFilesKey := iter.Val()

		// 获取 fullHash
		fullHash := strings.TrimPrefix(duplicateFilesKey, "duplicateFiles:")

		// 获取重复文件列表
		duplicateFiles, err := rdb.ZRange(ctx, duplicateFilesKey, 0, -1).Result()
		if err != nil {
			continue
		}

		if len(duplicateFiles) > 1 {
			// 保留第一个文件（你可以根据自己的需求修改保留策略）
			fileToKeep := duplicateFiles[0]

			// 检查第一个文件是否存在
			if _, err := os.Stat(fileToKeep); os.IsNotExist(err) {
				continue
			}

			filesToDelete := duplicateFiles[1:]

			for _, duplicateFile := range filesToDelete {
				// 先从 Redis 中删除相关记录
				err := cleanUpRecordsByFilePath(rdb, ctx, duplicateFile)
				if err != nil {
					continue
				}

				// 然后删除文件
				err = os.Remove(duplicateFile)
				if err != nil {
					continue
				}
			}

			// 清理 Redis 键
			err = cleanUpHashKeys(rdb, ctx, fullHash, duplicateFilesKey)
			if err != nil {
			}
		}
	}

	if err := iter.Err(); err != nil {
		return err
	}

	return nil
}

func shouldStopDuplicateFileSearch(duplicateCount int, maxDuplicateFiles int) bool {
	return duplicateCount >= maxDuplicateFiles
}
