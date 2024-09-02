package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadAndCompileExcludePatterns(t *testing.T) {
	// Create a temporary file with test patterns
	tmpfile, err := os.CreateTemp("", "test_patterns")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	testPatterns := []string{
		"pattern1*",
		"pattern2?",
		"[a-z]+pattern3",
	}

	for _, pattern := range testPatterns {
		if _, err := tmpfile.WriteString(pattern + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	tmpfile.Close()

	// Test loadAndCompileExcludePatterns
	regexps, err := loadAndCompileExcludePatterns(tmpfile.Name())
	assert.NoError(t, err)
	assert.Len(t, regexps, len(testPatterns))

	// Test compiled patterns
	assert.True(t, regexps[0].MatchString("pattern1abc"))
	assert.True(t, regexps[1].MatchString("pattern2a"))
	assert.True(t, regexps[2].MatchString("abcpattern3"))
}

func TestSortKeys(t *testing.T) {
	data := map[string]FileInfo{
		"file1": {Size: 100, ModTime: time.Now().Add(-1 * time.Hour)},
		"file2": {Size: 200, ModTime: time.Now()},
		"file3": {Size: 150, ModTime: time.Now().Add(-2 * time.Hour)},
	}
	keys := []string{"file1", "file2", "file3"}

	// Test sorting by size
	sortKeys(keys, data, false)
	assert.Equal(t, []string{"file2", "file3", "file1"}, keys)

	// Test sorting by mod time
	sortKeys(keys, data, true)
	assert.Equal(t, []string{"file2", "file1", "file3"}, keys)
}

func TestExtractFileName(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"/path/to/file.txt", "file.txt"},
		{"file.txt", "file.txt"},
		{"/path/with/spaces/file name.txt", "file name.txt"},
		{"/path/to/file/without/extension", "extension"},
	}

	for _, tc := range testCases {
		result := extractFileName(tc.input)
		assert.Equal(t, tc.expected, result)
	}
}

func TestExtractKeywords(t *testing.T) {
	fileNames := []string{
		"file01.02.03.txt",
		"document123abc.pdf",
		"image_20210515.jpg",
	}

	var stopProcessing bool
	keywords := extractKeywords(fileNames, &stopProcessing)

	// 使用 sort.Strings 对结果进行排序，以确保比较的一致性
	sort.Strings(keywords)

	expectedKeywords := []string{"02", "03", "document123abc", "file01"}
	sort.Strings(expectedKeywords)

	assert.Equal(t, expectedKeywords, keywords, "Extracted keywords do not match expected keywords")
}

func TestFindCloseFiles(t *testing.T) {
	fileNames := []string{"file1.txt", "file2.txt", "document3.pdf"}
	filePaths := []string{"/path/to/file1.txt", "/path/to/file2.txt", "/path/to/document3.pdf"}
	keywords := []string{"file", "document"}

	result := findCloseFiles(fileNames, filePaths, keywords)

	expected := map[string][]string{
		"file":     {"/path/to/file1.txt", "/path/to/file2.txt"},
		"document": {"/path/to/document3.pdf"},
	}

	assert.Equal(t, expected, result)
}

func TestWalkFiles(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "test_walk_files")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dirs := []string{
		"dir1",
		"dir with spaces",
		filepath.Join("dir2", "subdir"),
		"dir3",
	}
	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(tempDir, dir), 0755)
		assert.NoError(t, err)
	}

	files := map[string]int64{
		filepath.Join("dir1", "file1.txt"):                       100,
		filepath.Join("dir with spaces", "file with spaces.txt"): 200,
		filepath.Join("dir2", "sub dir", "file3_特殊字符.txt"):       300,
		filepath.Join("dir3", "file4!@#$%.txt"):                  400,
		filepath.Join("dir3", "small_file.txt"):                  50,
	}
	for file, size := range files {
		fullPath := filepath.Join(tempDir, file)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		assert.NoError(t, err)
		err = ioutil.WriteFile(fullPath, make([]byte, size), 0644)
		assert.NoError(t, err)
	}

	err = os.Symlink(filepath.Join(tempDir, "dir2"), filepath.Join(tempDir, "symlink dir"))
	assert.NoError(t, err)

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	// 创建一个模拟的 Redis 客户端
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()

	// 创建一个 FileProcessor 实例，不包含任何排除规则
	fp := &FileProcessor{
		Rdb:            rdb,
		Ctx:            context.Background(),
		excludeRegexps: []*regexp.Regexp{},
	}

	fileChan := make(chan string, 10)
	go func() {
		err := walkFiles(tempDir, 100, fileChan, fp)
		assert.NoError(t, err)
		close(fileChan)
	}()

	var result []string
	for file := range fileChan {
		result = append(result, file)
	}

	expected := []string{
		filepath.Join("dir1", "file1.txt"),
		filepath.Join("dir with spaces", "file with spaces.txt"),
		filepath.Join("dir2", "sub dir", "file3_特殊字符.txt"),
		filepath.Join("dir3", "file4!@#$%.txt"),
	}

	// 使用 filepath.ToSlash 来标准化路径
	for i, path := range result {
		result[i] = filepath.ToSlash(path)
	}
	for i, path := range expected {
		expected[i] = filepath.ToSlash(path)
	}

	// 排序结果和期望值，以确保比较的一致性
	sort.Strings(result)
	sort.Strings(expected)

	assert.Equal(t, expected, result)

	logOutput := logBuf.String()
	assert.Contains(t, logOutput, "Skipping symlink:")
	assert.NotContains(t, logOutput, "no such file or directory")
	assert.Equal(t, 1, strings.Count(logOutput, "Skipping symlink:"), "Symlink should be logged only once")

	for _, file := range result {
		assert.NotContains(t, file, "symlink dir", "Symlink should be skipped")
	}
}

func TestFormatFileInfoLine(t *testing.T) {
	testCases := []struct {
		name           string
		fileInfo       FileInfo
		relativePath   string
		sortByModTime  bool
		expectedOutput string
	}{
		{
			name:           "Normal path",
			fileInfo:       FileInfo{Size: 1000},
			relativePath:   "./normal/path.txt",
			sortByModTime:  false,
			expectedOutput: "1000,\"./normal/path.txt\"\n",
		},
		{
			name:           "Path with spaces",
			fileInfo:       FileInfo{Size: 2000},
			relativePath:   "./path with spaces/file.txt",
			sortByModTime:  false,
			expectedOutput: "2000,\"./path with spaces/file.txt\"\n",
		},
		{
			name:           "Path with special characters",
			fileInfo:       FileInfo{Size: 3000},
			relativePath:   "./special_字符/file!@#.txt",
			sortByModTime:  false,
			expectedOutput: "3000,\"./special_字符/file!@#.txt\"\n",
		},
		{
			name:           "Sort by mod time",
			fileInfo:       FileInfo{Size: 4000},
			relativePath:   "./mod_time/file.txt",
			sortByModTime:  true,
			expectedOutput: "\"./mod_time/file.txt\"\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output := formatFileInfoLine(tc.fileInfo, tc.relativePath, tc.sortByModTime)
			assert.Equal(t, tc.expectedOutput, output)
		})
	}
}

func TestGetFileSizeFromRedis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("An error occurred while creating miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()
	ctx := context.Background()

	tempDir, err := ioutil.TempDir("", "test_redis_file_size")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	specialFiles := []struct {
		name    string
		content string
	}{
		{"normal_file.txt", "content1"},
		{"file with spaces.txt", "content2"},
		{"file_with_特殊字符.txt", "content3"},
		{"file!@#$%^&*().txt", "content4"},
	}

	for _, sf := range specialFiles {
		filePath := filepath.Join(tempDir, sf.name)
		err := ioutil.WriteFile(filePath, []byte(sf.content), 0644)
		require.NoError(t, err)

		info, err := os.Stat(filePath)
		require.NoError(t, err)

		fileInfo := FileInfo{
			Size:    info.Size(),
			ModTime: info.ModTime(),
			Path:    filePath,
		}

		err = saveFileInfoToRedis(rdb, ctx, filePath, fileInfo, "dummyhash", "dummyfullhash", true)
		require.NoError(t, err)

		t.Run("GetSize_"+sf.name, func(t *testing.T) {
			relativePath, err := filepath.Rel(tempDir, filePath)
			require.NoError(t, err)

			size, err := getFileSizeFromRedis(rdb, ctx, tempDir, relativePath, testExcludeRegexps)
			assert.NoError(t, err)
			assert.Equal(t, int64(len(sf.content)), size)
		})
	}

	// Test with non-existent file
	t.Run("GetSize_NonExistentFile", func(t *testing.T) {
		_, err := getFileSizeFromRedis(rdb, ctx, tempDir, "non-existent-file.txt", testExcludeRegexps)
		assert.Error(t, err)
	})
}

func TestWalkFilesWithExcludePatterns(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "test_walk_files_exclude")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dirs := []string{
		"dir1",
		"dir with spaces",
		filepath.Join("dir2", "subdir"),
		"dir3",
		".git",
	}
	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(tempDir, dir), 0755)
		assert.NoError(t, err)
	}

	files := map[string]int64{
		filepath.Join("dir1", "file1.txt"):                       100,
		filepath.Join("dir with spaces", "file with spaces.txt"): 200,
		filepath.Join("dir2", "subdir", "file3_特殊字符.txt"):        300,
		filepath.Join("dir3", "file4!@#$%.txt"):                  400,
		filepath.Join("dir3", "small_file.txt"):                  50,
		filepath.Join(".git", "config"):                          100,
	}
	for file, size := range files {
		fullPath := filepath.Join(tempDir, file)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		assert.NoError(t, err)
		err = ioutil.WriteFile(fullPath, make([]byte, size), 0644)
		assert.NoError(t, err)
	}

	excludePatterns := []string{
		`.*\.git/.*`,
		`.*small_file\.txt$`,
	}
	excludeRegexps, err := compileExcludePatterns(excludePatterns)
	require.NoError(t, err)

	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	// 创建一个模拟的 Redis 客户端
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()

	fileChan := make(chan string, 10)
	fp := CreateFileProcessor(rdb, context.Background(), excludeRegexps)

	go func() {
		err := walkFiles(tempDir, 100, fileChan, fp)
		assert.NoError(t, err)
		close(fileChan)
	}()

	var result []string
	for file := range fileChan {
		result = append(result, file)
	}

	expected := []string{
		filepath.Join("dir1", "file1.txt"),
		filepath.Join("dir with spaces", "file with spaces.txt"),
		filepath.Join("dir2", "subdir", "file3_特殊字符.txt"),
		filepath.Join("dir3", "file4!@#$%.txt"),
	}

	// 使用 filepath.ToSlash 来标准化路径
	for i, path := range result {
		result[i] = filepath.ToSlash(path)
	}
	for i, path := range expected {
		expected[i] = filepath.ToSlash(path)
	}

	// 排序结果和期望值，���确保比较的一致性
	sort.Strings(result)
	sort.Strings(expected)

	assert.Equal(t, expected, result)

	logOutput := logBuf.String()
	assert.NotContains(t, logOutput, ".git/config")
	assert.NotContains(t, logOutput, "small_file.txt")
}

func TestCleanRelativePath(t *testing.T) {
	testCases := []struct {
		name     string
		rootDir  string
		fullPath string
		expected string
	}{
		{
			name:     "Simple case",
			rootDir:  "/home/user",
			fullPath: "/home/user/documents/file.txt",
			expected: "./documents/file.txt",
		},
		{
			name:     "Path with parent directory",
			rootDir:  "/home/user",
			fullPath: "/home/user/../user/documents/file.txt",
			expected: "./documents/file.txt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := cleanRelativePath(tc.rootDir, tc.fullPath)
			assert.Equal(t, filepath.ToSlash(tc.expected), filepath.ToSlash(result))
		})
	}
}

func TestFindAndLogDuplicates(t *testing.T) {
	// 初始化 Redis 客户端和上下文
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()

	// 清理 Redis 中的测试数据
	rdb.FlushDB(ctx)

	// 模拟写入重复文件信息到 Redis
	rdb.SAdd(ctx, "duplicateFiles:hash1", "/path/to/file1", "/path/to/file1_duplicate")
	rdb.SAdd(ctx, "duplicateFiles:hash2", "/path/to/file2", "/path/to/file2_duplicate")

	// 假设根目录为 "/testroot"
	rootDir := "/testroot"
	// 假设 workerCount 为 10
	workerCount := 10
	// 假设没有排除规则
	excludeRegexps := []*regexp.Regexp{}

	// 调用被测试的函数
	findAndLogDuplicates(rootDir, rdb, ctx, workerCount, excludeRegexps)

	// 检查 Redis 中是否存在期望的键
	exists, err := rdb.Exists(ctx, "duplicateFiles:hash1").Result()
	if err != nil {
		t.Fatalf("Error checking existence of key: %v", err)
	}
	if exists != 1 {
		t.Errorf("Expected key duplicateFiles:hash1 to exist")
	}

	exists, err = rdb.Exists(ctx, "duplicateFiles:hash2").Result()
	if err != nil {
		t.Fatalf("Error checking existence of key: %v", err)
	}
	if exists != 1 {
		t.Errorf("Expected key duplicateFiles:hash2 to exist")
	}
}
