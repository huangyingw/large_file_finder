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
	"sync/atomic"
	"time"
)

var progressCounter int32 // Progress counter

func main() {
	startTime := time.Now().Unix() // 记录当前时间的Unix时间戳
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./find_large_files_with_cache <directory>")
		return
	}

	// 将context和redis客户端设置为局部变量
	ctx := context.Background()
	rdb := newRedisClient(ctx)

	// Root directory to start the search
	rootDir := os.Args[1]

	// Minimum file size in bytes
	minSize := 200 // Default size is 200MB
	minSizeBytes := int64(minSize * 1024 * 1024)

	excludeRegexps, err := compileExcludePatterns(filepath.Join(rootDir, "exclude_patterns.txt"))
	if err != nil {
		fmt.Println(err)
		return
	}

	// 创建一个可以取消的context
	progressCtx, cancel := context.WithCancel(ctx)
	defer cancel() // 确保在main函数结束时调用cancel

	go func() {
		for {
			select {
			case <-progressCtx.Done(): // 检查context是否被取消
				return
			case <-time.After(1 * time.Second):
				fmt.Printf("Progress: %d files processed.\n", atomic.LoadInt32(&progressCounter))
			}
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
					processFile(osPathname, fileInfo.Mode(), rdb, ctx, startTime)
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

	err = cleanUpOldRecords(rdb, ctx, startTime)
	if err != nil {
		fmt.Println("Error cleaning up old records:", err)
	}

	// 文件处理完成后的保存操作
	performSaveOperation(rootDir, "fav.log", false, rdb, ctx)
	performSaveOperation(rootDir, "fav.log.sort", true, rdb, ctx)
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
