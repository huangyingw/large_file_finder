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
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ReadLimit       = 100 * 1024 // 100KB
	FullFileReadCmd = -1
)

type FileInfo struct {
	Size    int64
	ModTime time.Time
}

type FileProcessor struct {
	Rdb                   *redis.Client
	Ctx                   context.Context
	generateHashFunc      func(string) string                            // 新增字段
	calculateFileHashFunc func(path string, limit int64) (string, error) // 重命名字段
}

func NewFileProcessor(Rdb *redis.Client, Ctx context.Context) *FileProcessor {
	return &FileProcessor{
		Rdb:              Rdb,
		Ctx:              Ctx,
		generateHashFunc: generateHash, // 使用默认的 generateHash 函数
	}
}

var TimestampRegex = regexp.MustCompile(`(\d{1,2}:\d{2}(?::\d{2})?)`)

func ExtractTimestamp(filePath string) string {
	match := TimestampRegex.FindStringSubmatch(filePath)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func CalculateScore(timestamp string, fileNameLength int) float64 {
	timestampLength := len(timestamp)
	return float64(-(timestampLength*1000 + fileNameLength))
}

func (fp *FileProcessor) SaveDuplicateFileInfoToRedis(fullHash string, info FileInfo, filePath string) error {
	timestamp := ExtractTimestamp(filePath)
	fileNameLength := len(filepath.Base(filePath))
	score := CalculateScore(timestamp, fileNameLength)

	_, err := fp.Rdb.ZAdd(fp.Ctx, "duplicateFiles:"+fullHash, &redis.Z{
		Score:  score,
		Member: filePath,
	}).Result()

	if err != nil {
		return fmt.Errorf("error adding duplicate file to Redis: %w", err)
	}

	return nil
}

// getFileInfoFromRedis retrieves file info from Redis
func (fp *FileProcessor) getFileInfoFromRedis(hashedKey string) (FileInfo, error) {
	var fileInfo FileInfo
	value, err := fp.Rdb.Get(fp.Ctx, "fileInfo:"+hashedKey).Bytes()
	if err != nil {
		return fileInfo, fmt.Errorf("error getting file info from Redis: %w", err)
	}

	buf := bytes.NewBuffer(value)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&fileInfo)
	if err != nil {
		return fileInfo, fmt.Errorf("error decoding file info: %w", err)
	}
	return fileInfo, nil
}

// saveToFile saves file information to a file
func (fp *FileProcessor) saveToFile(dir, filename string, sortByModTime bool) error {
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

	if len(data) == 0 {
		return nil
	}

	return fp.writeDataToFile(dir, filename, data, sortByModTime)
}

// writeDataToFile writes the processed data to a file
func (fp *FileProcessor) writeDataToFile(dir, filename string, data map[string]FileInfo, sortByModTime bool) error {
	var lines []string
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	sortKeys(keys, data, sortByModTime)

	for _, k := range keys {
		relativePath, err := filepath.Rel(dir, k)
		if err != nil {
			log.Printf("Error getting relative path for %s: %v", k, err)
			continue
		}
		line := formatFileInfoLine(data[k], relativePath, sortByModTime)
		lines = append(lines, line)
	}

	return writeLinesToFile(filepath.Join(dir, filename), lines)
}

// ProcessFile processes a single file
func (fp *FileProcessor) ProcessFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("error getting file info: %w", err)
	}

	fileHash, err := fp.calculateFileHashFunc(path, ReadLimit) // 使用新的字段名
	if err != nil {
		return fmt.Errorf("error calculating file hash: %w", err)
	}

	fullHash, err := fp.calculateFileHashFunc(path, FullFileReadCmd) // 使用新的字段名
	if err != nil {
		return fmt.Errorf("error calculating full file hash: %w", err)
	}

	fileInfo := FileInfo{
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}

	err = fp.saveFileInfoToRedis(path, fileInfo, fileHash, fullHash)
	if err != nil {
		return fmt.Errorf("error saving file info to Redis: %w", err)
	}

	return nil
}

// calculateFileHash calculates the hash of a file
func (fp *FileProcessor) calculateFileHash(path string, limit int64) (string, error) {
	f, err := os.Open(path)
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

// saveFileInfoToRedis saves file info to Redis
func (fp *FileProcessor) saveFileInfoToRedis(path string, info FileInfo, fileHash, fullHash string) error {
	hashedKey := fp.generateHashFunc(path)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(info); err != nil {
		return fmt.Errorf("error encoding file info: %w", err)
	}

	pipe := fp.Rdb.Pipeline()
	pipe.SetNX(fp.Ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0)
	pipe.Set(fp.Ctx, "hashedKeyToPath:"+hashedKey, path, 0)
	pipe.SAdd(fp.Ctx, "fileHashToPathSet:"+fileHash, path)
	pipe.Set(fp.Ctx, "hashedKeyToFullHash:"+hashedKey, fullHash, 0)
	pipe.Set(fp.Ctx, "pathToHashedKey:"+path, hashedKey, 0)
	pipe.Set(fp.Ctx, "hashedKeyToFileHash:"+hashedKey, fileHash, 0)

	_, err := pipe.Exec(fp.Ctx)
	if err != nil {
		return fmt.Errorf("error executing Redis pipeline: %w", err)
	}
	return nil
}

const readLimit = 100 * 1024 // 100KB

// 处理目录
func processDirectory(path string) {
}

// 处理符号链接
func processSymlink(path string) {
}

// 处理关键词
func processKeyword(keyword string, keywordFiles []string, Rdb *redis.Client, Ctx context.Context, rootDir string) {
	// 对 keywordFiles 进行排序
	sort.Slice(keywordFiles, func(i, j int) bool {
		sizeI, _ := getFileSize(Rdb, Ctx, filepath.Join(rootDir, cleanPath(keywordFiles[i])))
		sizeJ, _ := getFileSize(Rdb, Ctx, filepath.Join(rootDir, cleanPath(keywordFiles[j])))
		return sizeI > sizeJ
	})

	// 准备要写入的数据
	var outputData strings.Builder
	outputData.WriteString(keyword + "\n")
	for _, filePath := range keywordFiles {
		fileSize, _ := getFileSize(Rdb, Ctx, filepath.Join(rootDir, cleanPath(filePath)))
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
func getFileSize(Rdb *redis.Client, Ctx context.Context, fullPath string) (int64, error) {
	size, err := getFileSizeFromRedis(Rdb, Ctx, fullPath)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// 获取文件哈希值
func getHash(path string, Rdb *redis.Client, Ctx context.Context, keyPrefix string, limit int64) (string, error) {
	hashedKey := generateHash(path) // 使用全局的 generateHash 函数
	hashKey := keyPrefix + hashedKey

	// 尝试从Redis获取哈希值
	hash, err := Rdb.Get(Ctx, hashKey).Result()
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
	err = Rdb.Set(Ctx, hashKey, hash, 0).Err()
	if err != nil {
		return "", err
	}
	return hash, nil
}

// 获取文件哈希
func getFileHash(path string, Rdb *redis.Client, Ctx context.Context) (string, error) {
	const readLimit = 100 * 1024 // 100KB
	return getHash(path, Rdb, Ctx, "hashedKeyToFileHash:", readLimit)
}

// 获取完整文件哈希
func getFullFileHash(path string, Rdb *redis.Client, Ctx context.Context) (string, error) {
	const noLimit = -1 // No limit for full file hash
	hash, err := getHash(path, Rdb, Ctx, "hashedKeyToFullHash:", noLimit)
	if err != nil {
		log.Printf("Error calculating full hash for file %s: %v", path, err)
	} else {
		log.Printf("Full hash for file %s: %s", path, hash)
	}
	return hash, err
}
