// main.go
// 此文件是Go程序的主要入口点，包含了文件处理程序的核心逻辑和初始化代码。

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/karrick/godirwalk"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	semaphore        = make(chan struct{}, 100)
	mu               sync.Mutex
	duplicateCounter int32
	stopProcessing   int32
	progressCounter  int32
)

func main() {
	// 初始化全局计数器
	atomic.StoreInt32(&duplicateCounter, 0)
	startTime := time.Now().Unix()

	// 解析命令行参数
	rootDir, minSizeBytes, excludeRegexps, rdb, ctx, deleteDuplicates, findDuplicates, outputDuplicates, maxDuplicates, err := initializeApp()
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("Starting to clean up old records")
	err = cleanUpOldRecords(rdb, ctx)
	if err != nil {
		log.Println("Error cleaning up old records:", err)
	}

	// 根据参数决定是否进行重复文件查找
	if findDuplicates {
		log.Println("Finding duplicates")
		err = findAndLogDuplicates(rootDir, rdb, ctx, maxDuplicates) // 查找重复文件
		if err != nil {
			log.Println("Error finding duplicates:", err)
			return
		}
	}

	// 根据参数决定是否输出重复文件
	if outputDuplicates {
		log.Println("Writing duplicates to file")
		err = writeDuplicateFilesToFile(rootDir, "fav.log.dup", rdb, ctx) // 输出结果
		if err != nil {
			log.Println("Error writing duplicates to file:", err)
		}
		return // 如果进行重复查找并输出结果，则结束程序
	}

	// 根据参数决定是否删除重复文件
	if deleteDuplicates {
		log.Println("Deleting duplicate files")
		err = deleteDuplicateFiles(rootDir, rdb, ctx)
		if err != nil {
			log.Println("Error deleting duplicate files:", err)
		}
		return // 如果删除重复文件，则结束程序
	}

	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()

	// 启动进度监控 Goroutine
	go monitorProgress(progressCtx, &progressCounter)

	workerCount := 500
	taskQueue, poolWg, stopFunc, _ := NewWorkerPool(workerCount, &stopProcessing)

	log.Printf("Starting to walk files in directory: %s\n", rootDir)
	err = walkFiles(rootDir, minSizeBytes, excludeRegexps, taskQueue, rdb, ctx, startTime, &stopProcessing)
	if err != nil {
		log.Printf("Error walking files: %s\n", err)
	}

	stopFunc()
	poolWg.Wait()

	// 此时所有任务已经完成，取消进度监控上下文
	progressCancel()

	log.Printf("Final progress: %d files processed.\n", atomic.LoadInt32(&progressCounter))

	// 文件处理完成后的保存操作
	log.Println("Saving data to fav.log")
	performSaveOperation(rootDir, "fav.log", false, rdb, ctx)
	log.Println("Saving data to fav.log.sort")
	performSaveOperation(rootDir, "fav.log.sort", true, rdb, ctx)

	// 新增逻辑：处理 fav.log 文件，类似于 find_sort_similar_filenames 函数的操作
	// favLogPath := filepath.Join(rootDir, "fav.log") // 假设 fav.log 在 rootDir 目录下
	// processFavLog(favLogPath, rootDir, rdb, ctx)
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

	excludeRegexps, _ := compileExcludePatterns(filepath.Join(*rootDir, "exclude_patterns.txt"))

	// 创建 Redis 客户端
	ctx := context.Background()
	rdb := newRedisClient(ctx)

	return *rootDir, minSizeBytes, excludeRegexps, rdb, ctx, *deleteDuplicates, *findDuplicates, *outputDuplicates, *maxDuplicates, nil
}

// walkFiles 遍历指定目录下的文件，并根据条件进行处理
func walkFiles(rootDir string, minSizeBytes int64, excludeRegexps []*regexp.Regexp, taskQueue chan<- Task, rdb *redis.Client, ctx context.Context, startTime int64, stopProcessing *int32) error {
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
				log.Printf("Error getting file info: %s\n", err)
				return err
			}

			// 检查文件大小是否满足最小阈值
			if fileInfo.Size() < minSizeBytes {
				return nil
			}

			// 将任务发送到工作池
			taskQueue <- func() {
				if fileInfo.Mode().IsDir() {
					log.Printf("Processing directory: %s\n", osPathname)
					processDirectory(osPathname)
				} else if fileInfo.Mode().IsRegular() {
					log.Printf("Processing file: %s\n", osPathname)
					processFile(osPathname, fileInfo.Mode(), rdb, ctx, startTime, stopProcessing)
				} else if fileInfo.Mode()&os.ModeSymlink != 0 {
					log.Printf("Processing symlink: %s\n", osPathname)
					processSymlink(osPathname)
				} else {
					log.Printf("Skipping unknown type: %s\n", osPathname)
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
			log.Printf("Progress: %d files processed.\n", processed)
		}
	}
}
