// main.go
// 此文件是Go程序的主要入口点，包含了文件处理程序的核心逻辑和初始化代码。

package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/karrick/godirwalk"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

var progressCounter int32 // Progress counter
var rdb *redis.Client     // Redis client
var ctx = context.Background()

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./find_large_files_with_cache <directory>")
		return
	}

	// Root directory to start the search
	rootDir := os.Args[1]

	// Minimum file size in bytes
	minSize := 200 // Default size is 200MB
	minSizeBytes := int64(minSize * 1024 * 1024)

	excludePatterns, err := loadExcludePatterns(filepath.Join(rootDir, "exclude_patterns.txt"))
	if err != nil {
		fmt.Println("Warning: Could not read exclude patterns:", err)
	}

	excludeRegexps := make([]*regexp.Regexp, len(excludePatterns))
	for i, pattern := range excludePatterns {
		// 将通配符模式转换为正则表达式
		regexPattern := strings.Replace(pattern, "*", ".*", -1)
		excludeRegexps[i], err = regexp.Compile(regexPattern)
		if err != nil {
			fmt.Printf("Invalid regex pattern '%s': %s\n", regexPattern, err)
			return
		}
	}

	// Start a goroutine to periodically print progress
	go func() {
		for {
			time.Sleep(1 * time.Second)
			fmt.Printf("Progress: %d files processed.\n", atomic.LoadInt32(&progressCounter))
		}
	}()

	// Use godirwalk.Walk instead of fastwalk.Walk or filepath.Walk
	// 初始化工作池
	workerCount := 20 // 可以根据需要调整工作池的大小
	taskQueue, poolWg := NewWorkerPool(workerCount)

	// 使用 godirwalk.Walk 遍历文件
	err = godirwalk.Walk(rootDir, &godirwalk.Options{
		Callback: func(osPathname string, de *godirwalk.Dirent) error {
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
					processFile(osPathname, fileInfo.Mode())
				} else if fileInfo.Mode()&os.ModeSymlink != 0 {
					processSymlink(osPathname)
				} else {
					fmt.Printf("Skipping unknown type: %s\n", osPathname)
				}
			}

			return nil
		},
		Unsorted: true,
	})

	// 关闭任务队列，并等待所有任务完成
	close(taskQueue)
	poolWg.Wait()
	fmt.Printf("Final progress: %d files processed.\n", atomic.LoadInt32(&progressCounter))

	// 文件处理完成后的保存操作
	if err := saveToFile(rootDir, "fav.log", false); err != nil {
		fmt.Printf("Error saving to fav.log: %s\n", err)
	} else {
		fmt.Printf("Saved data to %s\n", filepath.Join(rootDir, "fav.log"))
	}

	if err := saveToFile(rootDir, "fav.log.sort", true); err != nil {
		fmt.Printf("Error saving to fav.log.sort: %s\n", err)
	} else {
		fmt.Printf("Saved sorted data to %s\n", filepath.Join(rootDir, "fav.log.sort"))
	}
}

// Initialize Redis client
func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Println("Error connecting to Redis:", err)
		os.Exit(1)
	}
}
