// main.go
// 此文件是Go程序的主要入口点，包含了文件处理程序的核心逻辑和初始化代码。

package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/karrick/godirwalk"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

var progressCounter int32 // Progress counter

func main() {
	startTime := time.Now().Unix()
	rootDir, minSizeBytes, excludeRegexps, rdb, ctx, deleteDuplicates, findDuplicates, err := initializeApp(os.Args)
	if err != nil {
		log.Println(err)
		return
	}

	// 先清理旧记录
	err = cleanUpOldRecords(rdb, ctx)
	if err != nil {
		log.Println("Error cleaning up old records:", err)
	}

	// 根据参数决定是否进行重复文件查找并输出结果
	if findDuplicates {
		err = findAndLogDuplicates(rootDir, "fav.log.dup", rdb, ctx) // 先查找重复文件
		if err != nil {
			log.Println("Error finding and logging duplicates:", err)
			return
		}

		err = writeDuplicateFilesToFile(rootDir, "fav.log.dup", rdb, ctx) // 再输出结果
		if err != nil {
			log.Println("Error writing duplicates to file:", err)
		}
		return // 如果进行重复查找并输出结果，则结束程序
	}

	// 根据参数决定是否删除重复文件
	if deleteDuplicates {
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
	taskQueue, poolWg, stopPool := NewWorkerPool(workerCount)

	walkFiles(rootDir, minSizeBytes, excludeRegexps, taskQueue, rdb, ctx, startTime)

	stopPool() // 使用停止函数来关闭任务队列
	poolWg.Wait()

	// 此时所有任务已经完成，取消进度监控上下文
	progressCancel()

	log.Printf("Final progress: %d files processed.\n", atomic.LoadInt32(&progressCounter))

	// 文件处理完成后的保存操作
	performSaveOperation(rootDir, "fav.log", false, rdb, ctx)
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
	keywords := extractKeywords(fileNames)

	closeFiles := findCloseFiles(fileNames, filePaths, keywords)

	// 排序关键词
	sort.Slice(keywords, func(i, j int) bool {
		return len(closeFiles[keywords[i]]) > len(closeFiles[keywords[j]])
	})

	totalKeywords := len(keywords)

	workerCount := 500
	taskQueue, poolWg, stopPool := NewWorkerPool(workerCount)

	for i, keyword := range keywords {
		keywordFiles := closeFiles[keyword]
		if len(keywordFiles) >= 2 {
			poolWg.Add(1) // 在将任务发送到队列之前增加计数
			taskQueue <- func(kw string, kf []string, idx int) Task {
				return func() {
					defer poolWg.Done()
					processKeyword(kw, kf, rdb, ctx, rootDir)
				}
			}(keyword, keywordFiles, i)
		}
	}

	stopPool() // 使用停止函数来关闭任务队列
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

// initializeApp 初始化应用程序设置
func initializeApp(args []string) (string, int64, []*regexp.Regexp, *redis.Client, context.Context, bool, bool, error) {
	if len(args) < 2 {
		return "", 0, nil, nil, nil, false, false, fmt.Errorf("Usage: %s <rootDir> [--delete-duplicates] [--find-duplicates]", args[0])
	}

	// Root directory to start the search
	rootDir := args[1]
	deleteDuplicates := false
	findDuplicates := false

	// 解析参数
	for _, arg := range args {
		if arg == "--delete-duplicates" {
			deleteDuplicates = true
		} else if arg == "--find-duplicates" {
			findDuplicates = true
		}
	}

	// Minimum file size in bytes
	minSize := 200 // Default size is 200MB
	minSizeBytes := int64(minSize * 1024 * 1024)

	excludeRegexps, _ := compileExcludePatterns(filepath.Join(rootDir, "exclude_patterns.txt"))

	// 创建 Redis 客户端
	ctx := context.Background()
	rdb := newRedisClient(ctx)

	return rootDir, minSizeBytes, excludeRegexps, rdb, ctx, deleteDuplicates, findDuplicates, nil
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
