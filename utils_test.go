package main

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadExcludePatterns(t *testing.T) {
	// Create a temporary file with test patterns
	tmpfile, err := os.CreateTemp("", "test_patterns")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	testPatterns := []string{
		"pattern1",
		"pattern2",
		"pattern3",
	}

	for _, pattern := range testPatterns {
		if _, err := tmpfile.WriteString(pattern + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	tmpfile.Close()

	// Test loadExcludePatterns
	patterns, err := loadExcludePatterns(tmpfile.Name())
	assert.NoError(t, err)
	assert.Equal(t, testPatterns, patterns)
}

func TestCompileExcludePatterns(t *testing.T) {
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

	// Test compileExcludePatterns
	regexps, err := compileExcludePatterns(tmpfile.Name())
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

func TestGetFileSizeFromRedis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	// Prepare test data
	testPath := "/path/to/testfile.txt"
	testSize := int64(1024)
	testInfo := FileInfo{
		Size:    testSize,
		ModTime: time.Now(),
	}

	hashedKey := generateHash(testPath)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(testInfo)
	assert.NoError(t, err)

	err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
	assert.NoError(t, err)
	err = rdb.Set(ctx, "pathToHashedKey:"+testPath, hashedKey, 0).Err()
	assert.NoError(t, err)

	// Test getFileSizeFromRedis
	size, err := getFileSizeFromRedis(rdb, ctx, testPath)
	assert.NoError(t, err)
	assert.Equal(t, testSize, size)
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

	expectedKeywords := []string{"01", "02", "03", "123", "abc", "20210515"}
	assert.ElementsMatch(t, expectedKeywords, keywords)
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
