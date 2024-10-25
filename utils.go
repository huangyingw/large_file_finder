// utils.go
package main

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/gob"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/afero"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex

func findAndLogDuplicates(rootDir string, rdb *redis.Client, ctx context.Context, maxDuplicates int, excludeRegexps []*regexp.Regexp, fs afero.Fs) error {
	log.Println("Starting findAndLogDuplicates function")
	fileHashes, err := scanFileHashes(rdb, ctx)
	if err != nil {
		log.Printf("Error scanning file hashes: %v", err)
		return err
	}
	log.Printf("Found %d file hashes", len(fileHashes))

	fileCount := 0
	processedFullHashes := &sync.Map{}
	var stopProcessing bool
	taskQueue, poolWg, stopFunc, _ := NewWorkerPool(workerCount, &stopProcessing)

	for fileHash, filePaths := range fileHashes {
		if len(filePaths) > 1 {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled, stopping processing")
				stopProcessing = true
				break
			default:
				fileCount++
				if fileCount >= maxDuplicates {
					log.Println("Reached max duplicates, stopping processing")
					stopProcessing = true
					break
				}

				taskQueue <- func(fileHash string, filePaths []string) Task {
					return func() {
						log.Printf("Processing hash %s with %d files\n", fileHash, len(filePaths))
						_, err := processFileHash(rootDir, fileHash, filePaths, rdb, ctx, processedFullHashes, fs)
						if err != nil {
							log.Printf("Error processing file hash %s: %s\n", fileHash, err)
						}
					}
				}(fileHash, filePaths)
			}
		}
		if stopProcessing {
			break
		}
	}

	stopFunc()
	log.Println("Stopped worker pool, waiting for tasks to complete")

	// 使用带超时的 context 等待
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	waitCh := make(chan struct{})
	go func() {
		poolWg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		log.Println("All tasks completed successfully")
	case <-waitCtx.Done():
		log.Println("Timeout waiting for tasks to complete")
	}

	log.Printf("Total duplicates found: %d\n", fileCount)
	return nil
}

func getFileSizeFromRedis(rdb *redis.Client, ctx context.Context, rootDir, relativePath string, excludeRegexps []*regexp.Regexp) (int64, error) {
	fp := CreateFileProcessor(rdb, ctx, excludeRegexps)
	fullPath := filepath.Join(rootDir, relativePath)
	hashedKey, err := fp.getHashedKeyFromPath(fullPath)
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

func getFullFileHash(fs afero.Fs, path string, rdb *redis.Client, ctx context.Context) (string, error) {
	return calculateFileHash(fs, path, -1)
}

func getFileHash(fs afero.Fs, path string, rdb *redis.Client, ctx context.Context) (string, error) {
	return calculateFileHash(fs, path, 100*1024) // 100KB
}

func calculateFileHash(fs afero.Fs, path string, limit int64) (string, error) {
	f, err := fs.Open(path)
	if err != nil {
		return "", fmt.Errorf("error opening file %q: %w", path, err)
	}
	defer f.Close()

	h := sha512.New()
	if limit == -1 {
		if _, err := io.Copy(h, f); err != nil {
			return "", fmt.Errorf("error reading full file %q: %w", path, err)
		}
	} else {
		if _, err := io.CopyN(h, f, limit); err != nil && err != io.EOF {
			return "", fmt.Errorf("error reading file %q: %w", path, err)
		}
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func ExtractTimestamps(filePath string) []string {
	pattern := regexp.MustCompile(`[:,/](\d{1,2}(?::\d{1,2}){1,2})`)
	matches := pattern.FindAllStringSubmatch(filePath, -1)

	timestamps := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			timestamps = append(timestamps, FormatTimestamp(match[1]))
		}
	}

	uniqueTimestamps := make([]string, 0, len(timestamps))
	seen := make(map[string]bool)
	for _, ts := range timestamps {
		if !seen[ts] {
			seen[ts] = true
			uniqueTimestamps = append(uniqueTimestamps, ts)
		}
	}

	sort.Slice(uniqueTimestamps, func(i, j int) bool {
		return TimestampToSeconds(uniqueTimestamps[i]) < TimestampToSeconds(uniqueTimestamps[j])
	})

	return uniqueTimestamps
}

func cleanRelativePath(rootDir, fullPath string) string {
	rootDir, _ = filepath.Abs(rootDir)
	fullPath, _ = filepath.Abs(fullPath)

	rel, err := filepath.Rel(rootDir, fullPath)
	if err != nil {
		return fullPath
	}

	rel = strings.TrimPrefix(rel, "./")
	for strings.HasPrefix(rel, "../") {
		rel = strings.TrimPrefix(rel, "../")
	}

	if !strings.HasPrefix(rel, "./") {
		rel = "./" + rel
	}

	return filepath.ToSlash(rel)
}

func FormatTimestamp(timestamp string) string {
	parts := strings.Split(timestamp, ":")
	formattedParts := make([]string, len(parts))
	for i, part := range parts {
		num, _ := strconv.Atoi(part)
		formattedParts[i] = fmt.Sprintf("%02d", num)
	}
	return strings.Join(formattedParts, ":")
}

func TimestampToSeconds(timestamp string) int {
	parts := strings.Split(timestamp, ":")
	var totalSeconds int
	if len(parts) == 2 {
		minutes, _ := strconv.Atoi(parts[0])
		seconds, _ := strconv.Atoi(parts[1])
		totalSeconds = minutes*60 + seconds
	} else if len(parts) == 3 {
		hours, _ := strconv.Atoi(parts[0])
		minutes, _ := strconv.Atoi(parts[1])
		seconds, _ := strconv.Atoi(parts[2])
		totalSeconds = hours*3600 + minutes*60 + seconds
	}
	return totalSeconds
}

func sortKeys(keys []string, data map[string]FileInfo, sortByModTime bool) {
	if sortByModTime {
		sort.Slice(keys, func(i, j int) bool {
			return data[keys[i]].ModTime.After(data[keys[j]].ModTime)
		})
	} else {
		sort.Slice(keys, func(i, j int) bool {
			if data[keys[i]].Size == data[keys[j]].Size {
				return keys[i] < keys[j] // 如果大小相同，按路径字母顺序排序
			}
			return data[keys[i]].Size > data[keys[j]].Size
		})
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

func processFileHash(rootDir string, fileHash string, filePaths []string, rdb *redis.Client, ctx context.Context, processedFullHashes *sync.Map, fs afero.Fs) (int, error) {
	log.Printf("Starting processFileHash for hash: %s", fileHash)
	fileCount := 0
	hashes := make(map[string][]fileInfo)
	for _, fullPath := range filePaths {
		info, err := fs.Stat(fullPath)
		if err != nil {
			log.Printf("File does not exist: %s", fullPath)
			continue
		}
		relativePath, err := filepath.Rel(rootDir, fullPath)
		if err != nil {
			log.Printf("Error getting relative path: %v", err)
			continue
		}
		fileName := filepath.Base(relativePath)

		fullHash, err := getFullFileHash(fs, fullPath, rdb, ctx)
		if err != nil {
			log.Printf("Error getting full hash for file %s: %v", fullPath, err)
			continue
		}
		localFileHash, err := getFileHash(fs, fullPath, rdb, ctx) // 使用不同的变量名，避免与参数冲突
		if err != nil {
			continue
		}
		info, err = fs.Stat(fullPath)
		if err != nil {
			log.Printf("Error getting file info for %s: %v", fullPath, err)
			continue
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(FileInfo{Size: info.Size(), ModTime: info.ModTime(), Path: fullPath}); err != nil { // 确保 Path 被正确设置
			continue
		}
		if err := saveFileInfoToRedis(rdb, ctx, fullPath, FileInfo{
			Size:    info.Size(),
			ModTime: info.ModTime(),
			Path:    fullPath,
		}, localFileHash, fullHash, true); err != nil {
			continue
		}
		infoStruct := fileInfo{
			name:      fileName,
			path:      fullPath,
			buf:       buf,
			startTime: time.Now().Unix(),
			fileHash:  localFileHash,
			fullHash:  fullHash,
			line:      fmt.Sprintf("%d,\"./%s\"", info.Size(), relativePath),
			FileInfo:  FileInfo{Size: info.Size(), ModTime: info.ModTime(), Path: fullPath}, // 确保 Path 被正确设置
		}
		hashes[fullHash] = append(hashes[fullHash], infoStruct)
		fileCount++
	}
	var saveErr error
	for fullHash, infos := range hashes {
		if len(infos) > 1 {
			if _, loaded := processedFullHashes.LoadOrStore(fullHash, true); !loaded {
				for _, info := range infos {
					log.Printf("Saving duplicate file info to Redis for file: %s", info.path)
					err := SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info.FileInfo)
					if err != nil {
						log.Printf("Error saving duplicate file info to Redis for file: %s, error: %v", info.path, err)
						saveErr = err
					} else {
						log.Printf("Successfully saved duplicate file info to Redis for file: %s", info.path)
					}
				}
			}
		}
	}
	if saveErr != nil {
		return fileCount, saveErr
	}
	return fileCount, nil
}

// 主函数

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

func writeDuplicateFilesToFile(rootDir string, outputFile string, rdb *redis.Client, ctx context.Context, excludeRegexps []*regexp.Regexp) error {
	file, err := os.Create(filepath.Join(rootDir, outputFile))
	if err != nil {
		return fmt.Errorf("Error creating output file: %s", err)
	}
	defer file.Close()

	iter := rdb.Scan(ctx, 0, "duplicateFiles:*", 0).Iterator()
	for iter.Next(ctx) {
		duplicateFilesKey := iter.Val()
		fullHash := strings.TrimPrefix(duplicateFilesKey, "duplicateFiles:")
		duplicateFiles, err := rdb.ZRange(ctx, duplicateFilesKey, 0, -1).Result()
		if err != nil {
			log.Printf("Error getting duplicate files for key %s: %v", duplicateFilesKey, err)
			continue
		}

		if len(duplicateFiles) > 1 {
			header := fmt.Sprintf("Duplicate files for fullHash %s:\n", fullHash)
			if _, err := file.WriteString(header); err != nil {
				log.Printf("Error writing header: %v", err)
				continue
			}

			fp := CreateFileProcessor(rdb, ctx, excludeRegexps)

			for i, duplicateFile := range duplicateFiles {
				hashedKey, err := fp.getHashedKeyFromPath(duplicateFile)
				if err != nil {
					log.Printf("Error getting hashed key for path %s: %v", duplicateFile, err)
					continue
				}

				fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
				if err != nil {
					log.Printf("Error getting file info for key %s: %v", hashedKey, err)
					continue
				}

				var fileInfo FileInfo
				buf := bytes.NewBuffer(fileInfoData)
				dec := gob.NewDecoder(buf)
				if err := dec.Decode(&fileInfo); err != nil {
					log.Printf("Error decoding file info: %v", err)
					continue
				}

				cleanedPath := cleanRelativePath(rootDir, duplicateFile)
				var line string
				if i == 0 {
					line = fmt.Sprintf("\t[+] %d,\"%s\"\n", fileInfo.Size, cleanedPath)
				} else {
					line = fmt.Sprintf("\t[-] %d,\"%s\"\n", fileInfo.Size, cleanedPath)
				}

				if _, err := file.WriteString(line); err != nil {
					log.Printf("Error writing line: %v", err)
					continue
				}
			}
		} else {
			log.Printf("No duplicates found for hash %s", fullHash)
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("error during iteration: %w", err)
	}

	return nil
}

// extractFileName extracts the file name from a given file path.
func extractFileName(filePath string) string {
	return strings.ToLower(filepath.Base(filePath))
}

var pattern = regexp.MustCompile(`\b(?:\d{2}\.\d{2}\.\d{2}|(?:\d+|[a-z]+(?:\d+[a-z]*)?)|[a-z]+|[0-9]+)\b`)

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

func deleteDuplicateFiles(rootDir string, rdb *redis.Client, ctx context.Context, fs afero.Fs) error {
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
			if _, err := fs.Stat(fileToKeep); os.IsNotExist(err) {
				continue
			}

			filesToDelete := duplicateFiles[1:]

			if _, err := fs.Stat(fileToKeep); os.IsNotExist(err) {
				continue
			}

			for _, filePath := range filesToDelete {
				if err := fs.Remove(filePath); err != nil { // 使用 fs.Remove
					log.Printf("Error deleting file %s: %v", filePath, err)
				} else {
					log.Printf("Deleted duplicate file: %s", filePath)
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

func writeDataToFile(rootDir, filename string, data map[string]FileInfo, sortByModTime bool) error {
	outputPath := filepath.Join(rootDir, filename)
	outputDir := filepath.Dir(outputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("error creating output directory: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	sortKeys(keys, data, sortByModTime)

	for _, k := range keys {
		fileInfo := data[k]
		cleanedPath := cleanRelativePath(rootDir, k) // 使用 k 而不是 fileInfo.Path
		line := formatFileInfoLine(fileInfo, cleanedPath, sortByModTime)
		if _, err := fmt.Fprint(file, line); err != nil {
			return fmt.Errorf("error writing to file: %w", err)
		}
	}

	return nil
}

func formatFileInfoLine(fileInfo FileInfo, relativePath string, sortByModTime bool) string {
	if sortByModTime {
		return fmt.Sprintf("\"%s\"\n", relativePath)
	}
	return fmt.Sprintf("%d,\"%s\"\n", fileInfo.Size, relativePath)
}

// decodeGob decodes gob-encoded data into the provided interface
func decodeGob(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}
