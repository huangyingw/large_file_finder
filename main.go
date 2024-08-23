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
	"sort"
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

func shouldExclude(path string, regexps []*regexp.Regexp) bool {
	for _, re := range regexps {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

func walkFiles(rootDir string, minSizeBytes int64, fileChan chan<- string, regexps []*regexp.Regexp) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %q: %v", path, err)
			return filepath.SkipDir
		}

		relPath, err := filepath.Rel(rootDir, path)
		if err != nil {
			log.Printf("Error getting relative path for %q: %v", path, err)
			return nil
		}

		if shouldExclude(relPath, regexps) {
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
			// 使用 filepath.ToSlash 来标准化路径分隔符
			relPath = filepath.ToSlash(relPath)
			fileChan <- relPath
		}

		return nil
	})
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

	fp := CreateFileProcessor(rdb, ctx)

	if err := CleanUpOldRecords(rdb, ctx); err != nil {
		log.Printf("Error cleaning up old records: %v", err)
	}

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

	if findDuplicates {
		if err := findAndLogDuplicates(rootDir, rdb, ctx, maxDuplicates); err != nil {
			log.Fatalf("Error finding duplicates: %v", err)
		}
		return
	}

	if outputDuplicates {
		if err := fp.WriteDuplicateFilesToFile(rootDir, "fav.log.dup", rdb, ctx); err != nil {
			log.Fatalf("Error writing duplicates to file: %v", err)
		}
		return
	}

	if deleteDuplicates {
		if err := deleteDuplicateFiles(rootDir, rdb, ctx); err != nil {
			log.Fatalf("Error deleting duplicate files: %v", err)
		}
		return
	}

	fileChan := make(chan string, workerCount)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for relativePath := range fileChan {
				if !shouldExclude(relativePath, excludeRegexps) {
					if err := fp.ProcessFile(rootDir, relativePath, findDuplicates); err != nil {
						log.Printf("Error processing file %s: %v", filepath.Join(rootDir, relativePath), err)
					}
				}
			}
		}()
	}

	// Start progress monitoring
	go monitorProgress(ctx)

	// Walk through files
	err = walkFiles(rootDir, minSizeBytes, fileChan, excludeRegexps)
	close(fileChan)
	if err != nil {
		log.Printf("Error walking files: %v", err)
	}

	wg.Wait()

	// Save results
	if err := fp.saveToFile(rootDir, "fav.log", false); err != nil {
		log.Printf("Error saving to fav.log: %v", err)
	}
	if err := fp.saveToFile(rootDir, "fav.log.sort", true); err != nil {
		log.Printf("Error saving to fav.log.sort: %v", err)
	}

	log.Println("Processing complete")
}

func processFavLog(filePath string, rootDir string, rdb *redis.Client, ctx context.Context) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	var fileNames, filePaths []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = regexp.MustCompile(`^\d+,`).ReplaceAllString(line, "")
		filePaths = append(filePaths, line)
		fileNames = append(fileNames, extractFileName(line))
	}

	// 确定工作池的大小并调用 extractKeywords
	var stopProcessing bool
	keywords := extractKeywords(fileNames, &stopProcessing)

	closeFiles := findCloseFiles(fileNames, filePaths, keywords)

	// 排序关键词
	sort.Slice(keywords, func(i, j int) bool {
		return len(closeFiles[keywords[i]]) > len(closeFiles[keywords[j]])
	})

	workerCount := 500
	taskQueue, poolWg, stopFunc, _ := NewWorkerPool(workerCount, &stopProcessing)

	for i, keyword := range keywords {
		keywordFiles := closeFiles[keyword]
		if len(keywordFiles) >= 2 {
			poolWg.Add(1) // 在将任务发送到队列之前增加计数
			taskQueue <- func(kw string, kf []string, idx int) Task {
				return func() {
					defer poolWg.Done()
					log.Printf("Processing keyword %d of %d: %s\n", idx+1, len(keywords), kw)
					processKeyword(kw, kf, rdb, ctx, rootDir)
				}
			}(keyword, keywordFiles, i)
		}
	}

	stopFunc() // 使用停止函数来关闭任务队列
	poolWg.Wait()
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

func initializeApp() (string, int64, []*regexp.Regexp, *redis.Client, context.Context, bool, bool, bool, int, error) {
	rootDir := flag.String("rootDir", "", "Root directory to start the search")
	deleteDuplicates := flag.Bool("delete-duplicates", false, "Delete duplicate files")
	findDuplicates := flag.Bool("find-duplicates", false, "Find duplicate files")
	outputDuplicates := flag.Bool("output-duplicates", false, "Output duplicate files")
	maxDuplicates := flag.Int("max-duplicates", 50, "Maximum number of duplicates to process")
	flag.Parse()

	if *rootDir == "" {
		return "", 0, nil, nil, nil, false, false, false, 0, fmt.Errorf("rootDir must be specified")
	}

	// Minimum file size in bytes
	minSize := 200 // Default size is 200MB
	minSizeBytes := int64(minSize * 1024 * 1024)

	// 获取当前运行目录
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current directory: %v", err)
	}

	// 拼接当前目录和文件名
	excludePatternsFilePath := filepath.Join(currentDir, "exclude_patterns.txt")

	excludeRegexps, err := loadAndCompileExcludePatterns(excludePatternsFilePath)
	if err != nil {
		log.Printf("Error loading exclude patterns: %v", err)
	}

	// 创建 Redis 客户端
	ctx := context.Background()
	rdb := newRedisClient(ctx)

	return *rootDir, minSizeBytes, excludeRegexps, rdb, ctx, *deleteDuplicates, *findDuplicates, *outputDuplicates, *maxDuplicates, nil
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
