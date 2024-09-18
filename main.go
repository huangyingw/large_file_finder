// main.go
// 此文件是Go程序的主要入口点，包含了文件处理程序的核心逻辑和初始化代码。

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/afero"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"
)

var (
	rootDir          string
	redisAddr        string
	workerCount      int
	minSizeBytes     int64
	deleteDuplicates bool
	findDuplicates   bool
	outputDuplicates bool
	maxDuplicates    int
	semaphore        chan struct{}
)

var excludeRegexps []*regexp.Regexp

func init() {
	flag.StringVar(&rootDir, "rootDir", "", "Root directory to start the search")
	flag.StringVar(&redisAddr, "redisAddr", "localhost:6379", "Redis server address")
	flag.IntVar(&workerCount, "workers", runtime.NumCPU(), "Number of worker goroutines")
	flag.Int64Var(&minSizeBytes, "minSize", 200*1024*1024, "Minimum file size in bytes")
	flag.BoolVar(&deleteDuplicates, "delete-duplicates", false, "Delete duplicate files")
	flag.BoolVar(&findDuplicates, "find-duplicates", false, "Find duplicate files")
	flag.BoolVar(&outputDuplicates, "output-duplicates", false, "Output duplicate files")
	flag.IntVar(&maxDuplicates, "max-duplicates", 50, "Maximum number of duplicates to process")
}

func loadExcludePatterns(filename string, fs afero.Fs) ([]*regexp.Regexp, error) {
	file, err := fs.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening exclude patterns file: %w", err)
	}
	defer file.Close()

	var patterns []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		patterns = append(patterns, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading exclude patterns file: %w", err)
	}

	return compileExcludePatterns(patterns)
}

func compileExcludePatterns(patterns []string) ([]*regexp.Regexp, error) {
	var regexps []*regexp.Regexp
	for _, pattern := range patterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern '%s': %v", pattern, err)
		}
		regexps = append(regexps, re)
	}
	return regexps, nil
}

func main() {
	flag.Parse()

	if rootDir == "" {
		log.Fatal("rootDir must be specified")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	semaphore = make(chan struct{}, runtime.NumCPU())

	// Load exclude patterns
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("No caller information")
	}
	mainDir := filepath.Dir(filename)
	excludePatternsFile := filepath.Join(mainDir, "exclude_patterns.txt")
	fs := afero.NewOsFs()
	var err error
	excludeRegexps, err = loadExcludePatterns(excludePatternsFile, fs)
	if err != nil {
		log.Fatalf("Error loading exclude patterns: %v", err)
	}
	log.Printf("Loaded %d exclude patterns from %s", len(excludeRegexps), excludePatternsFile)

	fp := CreateFileProcessor(rdb, ctx, excludeRegexps)
	fp.fs = afero.NewOsFs() // 使用实际文件系统

	if err := CleanUpOldRecords(rdb, ctx); err != nil {
		log.Printf("Error cleaning up old records: %v", err)
	}

	// 确定是否需要计算哈希值
	calculateHashes := findDuplicates || outputDuplicates || deleteDuplicates

	// 处理文件
	fileChan := make(chan string, workerCount)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for relativePath := range fileChan {
				fullPath := filepath.Join(rootDir, relativePath)
				if !fp.ShouldExclude(fullPath) {
					if err := fp.ProcessFile(rootDir, relativePath, calculateHashes); err != nil {
						log.Printf("Error processing file %s: %v", fullPath, err)
					}
				}
			}
		}()
	}

	// Start progress monitoring
	go monitorProgress(ctx)

	err = walkFiles(rootDir, minSizeBytes, fileChan, fp)
	close(fileChan)
	if err != nil {
		log.Printf("Error walking files: %v", err)
	}

	wg.Wait()

	// 在处理完文件后，根据标志执行相应操作
	if findDuplicates {
		if err := findAndLogDuplicates(rootDir, rdb, ctx, maxDuplicates, excludeRegexps, fp.fs); err != nil {
			log.Fatalf("Error finding duplicates: %v", err)
		}
	}

	if outputDuplicates {
		if err := fp.WriteDuplicateFilesToFile(rootDir, "fav.log.dup", rdb, ctx); err != nil {
			log.Fatalf("Error writing duplicates to file: %v", err)
		}
	}

	if deleteDuplicates {
		if err := deleteDuplicateFiles(rootDir, rdb, ctx, fp.fs); err != nil {
			log.Fatalf("Error deleting duplicate files: %v", err)
		}
	}

	log.Println("Processing complete")
}

// 更新 walkFiles 函数
func walkFiles(rootDir string, minSizeBytes int64, fileChan chan<- string, fp *FileProcessor) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %q: %v", path, err)
			return filepath.SkipDir
		}

		if fp.ShouldExclude(path) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if info.IsDir() {
			return nil
		}

		if info.Mode()&os.ModeSymlink != 0 {
			log.Printf("Skipping symlink: %q", path)
			return nil
		}

		if info.Size() >= minSizeBytes {
			relPath, err := filepath.Rel(rootDir, path)
			if err != nil {
				log.Printf("Error getting relative path for %q: %v", path, err)
				return nil
			}
			fileChan <- relPath
		}

		return nil
	})
}

// 初始化Redis客户端
func newRedisClient(ctx context.Context) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // 该地址应从配置中获取
	})
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Println("Error connecting to Redis:", err)
		os.Exit(1)
	}
	return rdb
}

func monitorProgress(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// You might want to implement a way to track progress
			log.Println("Processing files...")
		}
	}
}
