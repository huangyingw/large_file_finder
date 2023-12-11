// main.go
// 此文件是Go程序的主要入口点，包含了文件处理程序的核心逻辑和初始化代码。

package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/karrick/godirwalk"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var progressCounter int32 // Progress counter

func main() {
	startTime := time.Now().Unix()
	rootDir, minSizeBytes, excludeRegexps, rdb, ctx, err := initializeApp(os.Args)
	if err != nil {
		fmt.Println(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go monitorProgress(ctx, &progressCounter)

	workerCount := 500
	taskQueue, poolWg := NewWorkerPool(workerCount)

	walkFiles(rootDir, minSizeBytes, excludeRegexps, taskQueue, rdb, ctx, startTime)

	close(taskQueue)
	poolWg.Wait()
	fmt.Printf("Final progress: %d files processed.\n", atomic.LoadInt32(&progressCounter))

	err = cleanUpOldRecords(rdb, ctx, startTime)
	if err != nil {
		fmt.Println("Error cleaning up old records:", err)
	}

	// 文件处理完成后的保存操作
	performSaveOperation(rootDir, "fav.log", false, rdb, ctx)
	performSaveOperation(rootDir, "fav.log.sort", true, rdb, ctx)
	findAndLogDuplicates(rootDir, "fav.log.dup", rdb, ctx)

	// 新增逻辑：处理 fav.log 文件，类似于 find_sort_similar_filenames 函数的操作
	favLogPath := filepath.Join(rootDir, "fav.log") // 假设 fav.log 在 rootDir 目录下
	processFavLog(favLogPath, rootDir, rdb, ctx, taskQueue, poolWg)
}

func processFavLog(filePath string, rootDir string, rdb *redis.Client, ctx context.Context, taskQueue chan<- Task, poolWg *sync.WaitGroup) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
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

	keywords := extractKeywords(fileNames)
	closeFiles := findCloseFiles(fileNames, filePaths, keywords)

	sort.Slice(keywords, func(i, j int) bool {
		return len(closeFiles[keywords[i]]) > len(closeFiles[keywords[j]])
	})

	totalKeywords := len(keywords)
	for i, keyword := range keywords {
		keywordFiles := closeFiles[keyword]
		if len(keywordFiles) >= 2 {
			poolWg.Add(1) // 在提交任务之前增加等待组的计数
			go func(kw string, kf []string) {
				defer poolWg.Done() // 任务完成后调用 Done
				fmt.Printf("Processing keyword %d of %d: %s\n", i+1, totalKeywords, kw)
				processKeyword(kw, kf, rdb, ctx, rootDir)
			}(keyword, keywordFiles)
		}
	}
}

// 初始化Redis客户端
func newRedisClient(ctx context.Context) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // 该地址应从配置中获取
	})
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Println("Error connecting to Redis:", err)
		os.Exit(1)
	}
	return rdb
}

// initializeApp 初始化应用程序设置
func initializeApp(args []string) (string, int64, []*regexp.Regexp, *redis.Client, context.Context, error) {
	if len(args) < 2 {
		return "", 0, nil, nil, nil, fmt.Errorf("usage: ./find_large_files_with_cache <directory>")
	}

	// Root directory to start the search
	rootDir := args[1]

	// Minimum file size in bytes
	minSize := 200 // Default size is 200MB
	minSizeBytes := int64(minSize * 1024 * 1024)

	excludeRegexps, _ := compileExcludePatterns(filepath.Join(rootDir, "exclude_patterns.txt"))

	// 创建 Redis 客户端
	ctx := context.Background()
	rdb := newRedisClient(ctx)

	return rootDir, minSizeBytes, excludeRegexps, rdb, ctx, nil
}

// walkFiles 遍历指定目录下的文件，并根据条件进行处理
func walkFiles(rootDir string, minSizeBytes int64, excludeRegexps []*regexp.Regexp, taskQueue chan<- Task, rdb *redis.Client, ctx context.Context, startTime int64) error {
	return godirwalk.Walk(rootDir, &godirwalk.Options{
		Callback: func(osPathname string, dirent *godirwalk.Dirent) error {
			// 排除模式匹配
			for _, re := range excludeRegexps {
				if re.MatchString(osPathname) {
					return nil
				}
			}

			fileInfo, err := os.Lstat(osPathname)
			if err != nil {
				fmt.Printf("Error getting file info: %s\n", err)
				return err
			}

			// 检查文件大小是否满足最小阈值
			if fileInfo.Size() < minSizeBytes {
				return nil
			}

			// 将任务发送到工作池
			taskQueue <- func() {
				if fileInfo.Mode().IsDir() {
					processDirectory(osPathname)
				} else if fileInfo.Mode().IsRegular() {
					processFile(osPathname, fileInfo.Mode(), rdb, ctx, startTime)
				} else if fileInfo.Mode()&os.ModeSymlink != 0 {
					processSymlink(osPathname)
				} else {
					fmt.Printf("Skipping unknown type: %s\n", osPathname)
				}
			}
			return nil
		},
		Unsorted: true, // 设置为true以提高性能
	})
}

// monitorProgress 在给定的上下文中定期打印处理进度
func monitorProgress(ctx context.Context, progressCounter *int32) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done(): // 检查上下文是否被取消
			return
		case <-ticker.C: // 每秒触发一次
			processed := atomic.LoadInt32(progressCounter)
			fmt.Printf("Progress: %d files processed.\n", processed)
		}
	}
}
