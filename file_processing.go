// file_processing.go
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
	"time"
)

type FileProcessor struct {
	Rdb                     *redis.Client
	Ctx                     context.Context
	generateHashFunc        func(string) string
	calculateFileHashFunc   func(path string, limit int64) (string, error)
	saveFileInfoToRedisFunc func(*redis.Client, context.Context, string, FileInfo, string, string, bool) error
	fs                      afero.Fs
	fileInfoRetriever       FileInfoRetriever
	excludeRegexps          []*regexp.Regexp
}

func CreateFileProcessor(rdb *redis.Client, ctx context.Context, excludeRegexps []*regexp.Regexp, options ...func(*FileProcessor)) *FileProcessor {
	fp := &FileProcessor{
		Rdb:            rdb,
		Ctx:            ctx,
		fs:             afero.NewOsFs(),
		excludeRegexps: excludeRegexps,
	}

	// 设置默认值
	fp.generateHashFunc = generateHash
	fp.calculateFileHashFunc = fp.calculateFileHash
	fp.saveFileInfoToRedisFunc = saveFileInfoToRedis
	fp.fileInfoRetriever = &RedisFileInfoRetriever{Rdb: rdb, Ctx: ctx}

	// 应用选项
	for _, option := range options {
		option(fp)
	}

	return fp
}

// 修改 saveToFile 方法
func (fp *FileProcessor) saveToFile(dir, filename string, sortByModTime bool) error {
	outputPath := filepath.Join(dir, filename)
	outputDir := filepath.Dir(outputPath)
	if err := fp.fs.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("error creating output directory: %w", err)
	}

	file, err := fp.fs.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	iter := fp.Rdb.Scan(fp.Ctx, 0, "fileInfo:*", 0).Iterator()
	data := make(map[string]FileInfo)

	for iter.Next(fp.Ctx) {
		hashedKey := strings.TrimPrefix(iter.Val(), "fileInfo:")

		originalPath, err := fp.Rdb.Get(fp.Ctx, "hashedKeyToPath:"+hashedKey).Result()
		if err != nil {
			log.Printf("Error getting original path for key %s: %v", hashedKey, err)
			continue
		}

		fileInfo, err := fp.getFileInfoFromRedis(hashedKey)
		if err != nil {
			log.Printf("Error getting file info for key %s: %v", hashedKey, err)
			continue
		}

		data[originalPath] = fileInfo
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("error iterating over Redis keys: %w", err)
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	sortKeys(keys, data, sortByModTime)

	for _, k := range keys {
		fileInfo := data[k]
		cleanedPath := cleanRelativePath(dir, k)
		line := formatFileInfoLine(fileInfo, cleanedPath, sortByModTime)
		if _, err := file.WriteString(line); err != nil {
			return fmt.Errorf("error writing to file: %w", err)
		}
	}

	return nil
}

type timestamp struct {
	hour, minute, second int
}

func parseTimestamp(ts string) timestamp {
	parts := strings.Split(ts, ":")
	hour, _ := strconv.Atoi(parts[0])
	minute, _ := strconv.Atoi(parts[1])
	second := 0
	if len(parts) > 2 {
		second, _ = strconv.Atoi(parts[2])
	}
	return timestamp{hour, minute, second}
}

const (
	ReadLimit       = 100 * 1024 // 100KB
	FullFileReadCmd = -1
)

type FileInfo struct {
	Size    int64
	ModTime time.Time
	Path    string // 新增 Path 字段
}

func (fp *FileProcessor) ProcessFile(rootDir, relativePath string, calculateHashes bool) error {
	fullPath := filepath.Join(rootDir, relativePath)
	log.Printf("Processing file: %s", fullPath)

	info, err := fp.fs.Stat(fullPath)
	if err != nil {
		return fmt.Errorf("error getting file info: %w", err)
	}

	hashedKey := fp.generateHashFunc(fullPath)
	log.Printf("Generated hashed key: %s", hashedKey)

	fileInfo := FileInfo{
		Size:    info.Size(),
		ModTime: info.ModTime(),
		Path:    fullPath, // 存储绝对路径
	}

	var fileHash, fullHash string
	if calculateHashes {
		fileHash, err = fp.calculateFileHashFunc(fullPath, ReadLimit)
		if err != nil {
			return fmt.Errorf("error calculating file hash: %w", err)
		}
		log.Printf("Calculated file hash: %s", fileHash)

		fullHash, err = fp.calculateFileHashFunc(fullPath, FullFileReadCmd)
		if err != nil {
			return fmt.Errorf("error calculating full file hash: %w", err)
		}
		log.Printf("Calculated full hash: %s", fullHash)
	}

	err = fp.saveFileInfoToRedisFunc(fp.Rdb, fp.Ctx, fullPath, fileInfo, fileHash, fullHash, calculateHashes)
	if err != nil {
		return fmt.Errorf("error saving file info to Redis: %w", err)
	}
	log.Printf("Saved file info to Redis")

	// 检查文件是否应该被排除
	if fp.ShouldExclude(fullPath) {
		return nil
	}

	return nil
}

type FileInfoRetriever interface {
	getFileInfoFromRedis(hashedKey string) (FileInfo, error)
}

func (fp *FileProcessor) WriteDuplicateFilesToFile(rootDir string, outputFile string, rdb *redis.Client, ctx context.Context) error {
	outputPath := filepath.Join(rootDir, outputFile)
	outputDir := filepath.Dir(outputPath)
	if err := fp.fs.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("error creating output directory: %w", err)
	}
	file, err := fp.fs.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
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
			fmt.Fprintf(file, "Duplicate files for fullHash %s:\n", fullHash)
			for i, duplicateFile := range duplicateFiles {
				hashedKey, err := fp.getHashedKeyFromPath(duplicateFile)
				if err != nil {
					log.Printf("Error getting hashed key for path %s: %v", duplicateFile, err)
					continue
				}
				cleanedPath := cleanRelativePath(rootDir, duplicateFile)
				var line string
				if i == 0 {
					fileSize, err := getFileSizeFromRedis(rdb, ctx, duplicateFile, fp.excludeRegexps)
					if err != nil {
						log.Printf("Error getting file size for key %s: %v", hashedKey, err)
						continue
					}
					line = fmt.Sprintf("[+] %d,\"%s\"\n", fileSize, cleanedPath)
				} else {
					fileSize, err := getFileSizeFromRedis(rdb, ctx, duplicateFile, fp.excludeRegexps)
					if err != nil {
						log.Printf("Error getting file size for key %s: %v", hashedKey, err)
						continue
					}
					line = fmt.Sprintf("[-] %d,\"%s\"\n", fileSize, cleanedPath)
				}
				if _, err := file.WriteString(line); err != nil {
					log.Printf("Error writing line: %v", err)
					continue
				}
			}
			// Add a single newline after each group of duplicate files
			if _, err := file.WriteString("\n"); err != nil {
				log.Printf("Error writing newline: %v", err)
			}
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("error during iteration: %w", err)
	}
	return nil
}

func (fp *FileProcessor) ShouldExclude(path string) bool {
	for _, re := range fp.excludeRegexps {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

func (fp *FileProcessor) getFileInfoFromRedis(hashedKey string) (FileInfo, error) {
	return fp.fileInfoRetriever.getFileInfoFromRedis(hashedKey)
}

type RedisFileInfoRetriever struct {
	Rdb *redis.Client
	Ctx context.Context
}

func (r *RedisFileInfoRetriever) getFileInfoFromRedis(hashedKey string) (FileInfo, error) {
	var fileInfo FileInfo
	value, err := r.Rdb.Get(r.Ctx, "fileInfo:"+hashedKey).Bytes()
	if err != nil {
		return fileInfo, err // 直接返回错误，包括 redis.Nil
	}

	buf := bytes.NewBuffer(value)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&fileInfo)
	if err != nil {
		return fileInfo, fmt.Errorf("error decoding file info: %w", err)
	}
	return fileInfo, nil
}

func (fp *FileProcessor) calculateFileHash(path string, limit int64) (string, error) {
	f, err := fp.fs.Open(path)
	if err != nil {
		return "", fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	h := sha512.New()
	if limit == FullFileReadCmd {
		if _, err := io.Copy(h, f); err != nil {
			return "", fmt.Errorf("error reading full file: %w", err)
		}
	} else {
		if _, err := io.CopyN(h, f, limit); err != nil && err != io.EOF {
			return "", fmt.Errorf("error reading file: %w", err)
		}
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

const readLimit = 100 * 1024 // 100KB

// 处理目录
func processDirectory(path string) {
}

// 处理符号链接
func processSymlink(path string) {
}

// 处理关键词
func processKeyword(keyword string, keywordFiles []string, Rdb *redis.Client, Ctx context.Context, rootDir string, excludeRegexps []*regexp.Regexp) {
	// 对 keywordFiles 进行排序
	sort.Slice(keywordFiles, func(i, j int) bool {
		sizeI, err := getFileSizeFromRedis(Rdb, Ctx, filepath.Join(rootDir, cleanRelativePath(rootDir, keywordFiles[i])), excludeRegexps)
		if err != nil {
			log.Printf("Error getting file size for %s: %v", keywordFiles[i], err)
			return false
		}
		sizeJ, err := getFileSizeFromRedis(Rdb, Ctx, filepath.Join(rootDir, cleanRelativePath(rootDir, keywordFiles[j])), excludeRegexps)
		if err != nil {
			log.Printf("Error getting file size for %s: %v", keywordFiles[j], err)
			return false
		}
		return sizeI > sizeJ
	})

	// 准备要写入的数据
	var outputData strings.Builder
	outputData.WriteString(keyword + "\n")
	for _, filePath := range keywordFiles {
		fileSize, err := getFileSizeFromRedis(Rdb, Ctx, filepath.Join(rootDir, cleanRelativePath(rootDir, filePath)), excludeRegexps)
		if err != nil {
			log.Printf("Error getting file size for %s: %v", filePath, err)
			continue
		}
		outputData.WriteString(fmt.Sprintf("%d,%s\n", fileSize, filePath))
	}

	// 创建并写入文件
	outputFilePath := filepath.Join(rootDir, keyword+".txt")
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Printf("Error creating output file %s: %v", outputFilePath, err)
		return
	}
	defer outputFile.Close()

	_, err = outputFile.WriteString(outputData.String())
	if err != nil {
		log.Printf("Error writing to output file %s: %v", outputFilePath, err)
	}
}

func getFileInfo(rdb *redis.Client, ctx context.Context, filePath string) (FileInfo, error) {
	hashedKey, err := rdb.Get(ctx, "pathToHashedKey:"+filePath).Result()
	if err != nil {
		return FileInfo{}, err
	}

	fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
	if err != nil {
		return FileInfo{}, err
	}

	var fileInfo FileInfo
	buf := bytes.NewBuffer(fileInfoData)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&fileInfo); err != nil {
		return FileInfo{}, err
	}

	return fileInfo, nil
}

const timestampWeight = 1000000000 // 使用一个非常大的数字

func CalculateScore(timestamps []string, fileNameLength int) float64 {
	timestampCount := len(timestamps)
	return float64(-(timestampCount*timestampWeight + fileNameLength))
}

func (fp *FileProcessor) getHashedKeyFromPath(path string) (string, error) {
	return fp.Rdb.Get(fp.Ctx, "pathToHashedKey:"+filepath.Clean(path)).Result()
}
