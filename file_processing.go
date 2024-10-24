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

	// 应用选项
	for _, option := range options {
		option(fp)
	}

	return fp
}

// 修改 saveToFile 方法
func (fp *FileProcessor) saveToFile(rootDir, filename string, sortByModTime bool) error {
	outputPath := filepath.Join(rootDir, filename)
	absOutputPath, err := filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("error getting absolute path: %w", err)
	}

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
		cleanedPath := cleanRelativePath(rootDir, k)
		line := formatFileInfoLine(fileInfo, cleanedPath, sortByModTime)
		if _, err := file.WriteString(line); err != nil {
			return fmt.Errorf("error writing to file: %w", err)
		}
	}

	log.Printf("File updated successfully: %s", absOutputPath)
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

	return nil
}

type FileInfoRetriever interface {
	getFileInfoFromRedis(hashedKey string) (FileInfo, error)
}

func (fp *FileProcessor) WriteDuplicateFilesToFile(rootDir string, outputFile string, rdb *redis.Client, ctx context.Context) error {
	outputPath := filepath.Join(rootDir, outputFile)
	absOutputPath, err := filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("error getting absolute path: %w", err)
	}

	file, err := fp.fs.Create(outputPath)
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
			fmt.Fprintf(file, "Duplicate files for fullHash %s:\n", fullHash)
			for i, duplicateFile := range duplicateFiles {
				hashedKey, err := rdb.Get(ctx, "pathToHashedKey:"+duplicateFile).Result()
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
				err = gob.NewDecoder(bytes.NewReader(fileInfoData)).Decode(&fileInfo)
				if err != nil {
					log.Printf("Error decoding file info: %v", err)
					continue
				}

				cleanedPath := cleanRelativePath(rootDir, duplicateFile)
				formattedLine := formatFileInfoLine(fileInfo, cleanedPath, false)
				prefix := "[-]"
				if i == 0 {
					prefix = "[+]"
				}
				line := fmt.Sprintf("%s %s", prefix, formattedLine)

				if _, err := file.WriteString(line); err != nil {
					log.Printf("Error writing line: %v", err)
				}
			}
			file.WriteString("\n")
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("error during iteration: %w", err)
	}

	log.Printf("Duplicate files written successfully: %s", absOutputPath)
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

type RedisFileInfoRetriever struct {
	Rdb *redis.Client
	Ctx context.Context
}

func (fp *FileProcessor) getFileInfoFromRedis(hashedKey string) (FileInfo, error) {
	var fileInfo FileInfo
	value, err := fp.Rdb.Get(fp.Ctx, "fileInfo:"+hashedKey).Bytes()
	if err != nil {
		return fileInfo, err
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
	// 首先获取文件的 hashedKey
	hashedKey := fp.generateHashFunc(path)

	// 检查Redis缓存中是否已存在对应的hash
	var cacheKey string
	if limit == FullFileReadCmd {
		cacheKey = "hashedKeyToFullHash:" + hashedKey
	} else {
		cacheKey = "hashedKeyToFileHash:" + hashedKey
	}

	// 尝试从缓存获取
	cachedHash, err := fp.Rdb.Get(fp.Ctx, cacheKey).Result()
	if err == nil {
		return cachedHash, nil
	} else if err != redis.Nil {
		return "", fmt.Errorf("redis error: %w", err)
	}

	// 缓存未命中，计算新的hash
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

	hash := fmt.Sprintf("%x", h.Sum(nil))

	// 将新计算的hash保存到Redis
	if err := fp.Rdb.Set(fp.Ctx, cacheKey, hash, 0).Err(); err != nil {
		log.Printf("Warning: Failed to cache hash for %s: %v", path, err)
	}

	return hash, nil
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
		relativePathI := cleanRelativePath(rootDir, keywordFiles[i])
		relativePathJ := cleanRelativePath(rootDir, keywordFiles[j])
		sizeI, err := getFileSizeFromRedis(Rdb, Ctx, rootDir, relativePathI, excludeRegexps)
		if err != nil {
			log.Printf("Error getting file size for %s: %v", keywordFiles[i], err)
			return false
		}
		sizeJ, err := getFileSizeFromRedis(Rdb, Ctx, rootDir, relativePathJ, excludeRegexps)
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
		relativePath := cleanRelativePath(rootDir, filePath)
		fileSize, err := getFileSizeFromRedis(Rdb, Ctx, rootDir, relativePath, excludeRegexps)
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

type FileInfo struct {
	Size    int64
	ModTime time.Time
	Path    string // 新增 Path 字段
}

func (fp *FileProcessor) getHashedKeyFromPath(path string) (string, error) {
	return fp.Rdb.Get(fp.Ctx, "pathToHashedKey:"+filepath.Clean(path)).Result()
}
