package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloseFileFinder(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "closefiles_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

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

	// 测试处理文件
	err = finder.ProcessCloseFiles()
	require.NoError(t, err)

	// 验证输出文件存在
	outputPath := filepath.Join(tempDir, "fav.log.close")
	_, err = os.Stat(outputPath)
	assert.NoError(t, err)

	// 读取并验证输出内容
	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	
	// 验证相似文件被正确识别
	assert.Contains(t, string(content), "similar_name_1.mp4")
	assert.Contains(t, string(content), "similar_name_2.mp4")
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
