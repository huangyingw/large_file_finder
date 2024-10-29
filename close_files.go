package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

type CloseFileFinder struct {
	rootDir     string
	workerCount int
	minScore    float64
}

func NewCloseFileFinder(rootDir string) *CloseFileFinder {
	return &CloseFileFinder{
		rootDir:     rootDir,
		workerCount: runtime.NumCPU(),
		minScore:    0.6, // 相似度阈值，可配置
	}
}

// 处理 fav.log 文件并生成 fav.log.close
func (cf *CloseFileFinder) ProcessCloseFiles() error {
	// 读取 fav.log 文件
	files, err := cf.readFavLog()
	if err != nil {
		return fmt.Errorf("读取fav.log失败: %w", err)
	}

	// 查找相似文件
	results := cf.findCloseFiles(files)

	// 写入结果到 fav.log.close
	return cf.writeResults(results)
}

// 从 fav.log 读取文件信息
func (cf *CloseFileFinder) readFavLog() ([]string, error) {
	favLogPath := filepath.Join(cf.rootDir, "fav.log")
	file, err := os.Open(favLogPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var files []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// 跳过空行
		if line == "" {
			continue
		}
		// 解析行内容，提取文件名
		parts := strings.Split(line, ",")
		if len(parts) >= 2 {
			fileName := strings.Trim(parts[1], "\"")
			files = append(files, fileName)
		}
	}
	return files, scanner.Err()
}

type similarityResult struct {
	file1 string
	file2 string
	score float64
}

// 查找相似文件
func (cf *CloseFileFinder) findCloseFiles(files []string) []similarityResult {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		results  []similarityResult
		taskChan = make(chan [2]int, len(files)*len(files)/2)
	)

	// 启动工作协程
	for i := 0; i < cf.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskChan {
				i, j := task[0], task[1]
				score := calculateSimilarity(
					filepath.Base(files[i]),
					filepath.Base(files[j]),
				)

				if score >= cf.minScore {
					mu.Lock()
					results = append(results, similarityResult{
						file1: files[i],
						file2: files[j],
						score: score,
					})
					mu.Unlock()
				}
			}
		}()
	}

	// 分发任务
	for i := 0; i < len(files); i++ {
		for j := i + 1; j < len(files); j++ {
			taskChan <- [2]int{i, j}
		}
	}
	close(taskChan)
	wg.Wait()

	return results
}

// 计算两个文件名的相似度
func calculateSimilarity(name1, name2 string) float64 {
	// 移除扩展名
	name1 = strings.TrimSuffix(name1, filepath.Ext(name1))
	name2 = strings.TrimSuffix(name2, filepath.Ext(name2))

	// 转换为小写进行比较
	name1 = strings.ToLower(name1)
	name2 = strings.ToLower(name2)

	// 使用 Levenshtein 距离计算相似度
	distance := levenshteinDistance(name1, name2)
	maxLen := float64(max(len(name1), len(name2)))

	if maxLen == 0 {
		return 0
	}

	return 1 - float64(distance)/maxLen
}

// 写入结果到 fav.log.close
func (cf *CloseFileFinder) writeResults(results []similarityResult) error {
	outputPath := filepath.Join(cf.rootDir, "fav.log.close")
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, result := range results {
		_, err := fmt.Fprintf(writer,
			"相似度: %.2f\n文件1: %s\n文件2: %s\n\n",
			result.score,
			result.file1,
			result.file2,
		)
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
