package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 创建测试辅助函数
func setupCloseFileTest(t *testing.T) (string, *CloseFileFinder, func()) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "closefiles_test")
	require.NoError(t, err)

	// 创建测试用的 fav.log 文件
	favLog := `100,"test_file_1.txt"
200,"test_file_2.txt"
300,"similar_name_1.mp4"
400,"similar_name_2.mp4"
500,"totally_different.txt"
`
	err = os.WriteFile(filepath.Join(tempDir, "fav.log"), []byte(favLog), 0644)
	require.NoError(t, err)

	// 创建 CloseFileFinder 实例
	finder := NewCloseFileFinder(tempDir)

	// 返回清理函数
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return tempDir, finder, cleanup
}

// 使用测试辅助函数重写测试用例
func TestCloseFileFinder(t *testing.T) {
	tempDir, finder, cleanup := setupCloseFileTest(t)
	defer cleanup()

	// 测试处理文件
	err := finder.ProcessCloseFiles()
	require.NoError(t, err)

	// 验证输出文件存在
	outputPath := filepath.Join(tempDir, "fav.log.close")
	_, err = os.Stat(outputPath)
	assert.NoError(t, err)

	// 读取并验证输出内容
	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	// 验证相似文件被正确识别
	contentStr := string(content)
	assert.Contains(t, contentStr, "similar_name_1.mp4")
	assert.Contains(t, contentStr, "similar_name_2.mp4")
	assert.Contains(t, contentStr, "相似度:")

	// 验证不相似的文件没有被错误匹配
	assert.NotContains(t, contentStr, "totally_different.txt")
}

func TestCalculateSimilarity(t *testing.T) {
	testCases := []struct {
		name     string
		file1    string
		file2    string
		expected float64
	}{
		{
			name:     "完全相同",
			file1:    "test.txt",
			file2:    "test.txt",
			expected: 1.0,
		},
		{
			name:     "相似名称",
			file1:    "similar_name_1.mp4",
			file2:    "similar_name_2.mp4",
			expected: 0.8,
		},
		{
			name:     "完全不同",
			file1:    "abc.txt",
			file2:    "xyz.txt",
			expected: 0.0,
		},
		{
			name:     "不同扩展名",
			file1:    "test.txt",
			file2:    "test.mp4",
			expected: 1.0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			score := calculateSimilarity(tc.file1, tc.file2)
			assert.InDelta(t, tc.expected, score, 0.2)
		})
	}
}

func TestCalculateSimilarityEdgeCases(t *testing.T) {
	testCases := []struct {
		name     string
		file1    string
		file2    string
		expected float64
	}{
		{
			name:     "空文件名",
			file1:    "",
			file2:    "",
			expected: 0.0,
		},
		{
			name:     "特殊字符",
			file1:    "file!@#$%^&*.txt",
			file2:    "file!@#$%^&*.mp4",
			expected: 1.0,
		},
		{
			name:     "中文文件名",
			file1:    "测试文件1.txt",
			file2:    "测试文件2.txt",
			expected: 0.9,
		},
		{
			name:     "混合字符",
			file1:    "test文件1.txt",
			file2:    "test文件2.txt",
			expected: 0.9,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			score := calculateSimilarity(tc.file1, tc.file2)
			assert.InDelta(t, tc.expected, score, 0.1)
		})
	}
}

func TestCloseFileFinderConcurrency(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "closefiles_concurrent_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建大量测试文件名
	var fileNames []string
	for i := 0; i < 1000; i++ {
		fileNames = append(fileNames, fmt.Sprintf("test_file_%d.txt", i))
	}

	// 创建fav.log
	var content strings.Builder
	for _, name := range fileNames {
		content.WriteString(fmt.Sprintf("100,\"%s\"\n", name))
	}
	err = os.WriteFile(filepath.Join(tempDir, "fav.log"), []byte(content.String()), 0644)
	require.NoError(t, err)

	// 测试并发处理
	finder := NewCloseFileFinder(tempDir)
	start := time.Now()
	err = finder.ProcessCloseFiles()
	duration := time.Since(start)

	require.NoError(t, err)
	assert.Less(t, duration, 9*time.Second, "并发处理应该在合理时间内完成")

	// 验证输出文件
	outputContent, err := os.ReadFile(filepath.Join(tempDir, "fav.log.close"))
	require.NoError(t, err)
	assert.NotEmpty(t, outputContent)
}

func TestCloseFileFinderErrors(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "closefiles_error_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Run("不存在的fav.log文件", func(t *testing.T) {
		finder := NewCloseFileFinder(tempDir)
		err := finder.ProcessCloseFiles()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no such file or directory")
	})

	t.Run("无效的fav.log内容", func(t *testing.T) {
		// 创建格式错误的fav.log
		invalidContent := "invalid content\nwrong format\n"
		err := os.WriteFile(filepath.Join(tempDir, "fav.log"), []byte(invalidContent), 0644)
		require.NoError(t, err)

		finder := NewCloseFileFinder(tempDir)
		err = finder.ProcessCloseFiles()
		assert.NoError(t, err) // 应该优雅处理格式错误
	})
}

// 如果需要测试其他场景，可以继续使用相同的辅助函数
func TestCloseFileFinderWithEmptyFile(t *testing.T) {
	tempDir, finder, cleanup := setupCloseFileTest(t)
	defer cleanup()

	// 清空 fav.log 文件
	err := os.WriteFile(filepath.Join(tempDir, "fav.log"), []byte(""), 0644)
	require.NoError(t, err)

	// 测试处理空文件
	err = finder.ProcessCloseFiles()
	require.NoError(t, err)

	// 验证输出文件存在但为空
	outputPath := filepath.Join(tempDir, "fav.log.close")
	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	assert.Empty(t, string(content))
}
