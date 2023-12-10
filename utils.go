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

	var lines []string
	for _, paths := range hashes {
		if len(paths) > 1 {
			for _, path := range paths {
				relPath, err := filepath.Rel(rootDir, path)
				if err != nil {
					fmt.Printf("Error converting to relative path: %s\n", err)
					continue
				}
				lines = append(lines, fmt.Sprintf("\"./%s\"", relPath))
			}
		}
	}

	if len(lines) == 0 {
		fmt.Println("No duplicates found.")
		return nil
	}

	outputFile = filepath.Join(rootDir, outputFile)
	err = writeLinesToFile(outputFile, lines)
	if err != nil {
		fmt.Printf("Error writing to file %s: %s\n", outputFile, err)
		return err
	}

	fmt.Printf("Duplicates written to %s\n", outputFile)
	return nil
}

func getFileSizeFromRedis(rdb *redis.Client, ctx context.Context, hashedKey string) (int64, error) {
	fileInfoData, err := rdb.Get(ctx, hashedKey).Bytes()
	if err != nil {
		return 0, err
	}

	var fileInfo FileInfo
	buf := bytes.NewBuffer(fileInfoData)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&fileInfo); err != nil {
		return 0, err
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

// extractKeywords extracts keywords from a slice of file names.
func extractKeywords(fileNames []string) []string {
	keywords := make(map[string]struct{})
	pattern := regexp.MustCompile(`\b(?:\d{2}\.\d{2}\.\d{2}|(?:\d+|[a-z]+(?:\d+[a-z]*)?))\b`)

	for _, fileName := range fileNames {
		nameWithoutExt := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		matches := pattern.FindAllString(nameWithoutExt, -1)
		for _, match := range matches {
			keywords[match] = struct{}{}
		}
	}

	var keywordList []string
	for keyword := range keywords {
		keywordList = append(keywordList, keyword)
	}

	return keywordList
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
