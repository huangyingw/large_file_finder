package main

import (
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestLoadExcludePatterns(t *testing.T) {
	fs := afero.NewMemMapFs()
	testFile := "/test_exclude_patterns.txt"
	err := afero.WriteFile(fs, testFile, []byte("*/.git/*\n*/snapraid.parity/*\n./sda1/*"), 0644)
	require.NoError(t, err)

	patterns, err := loadExcludePatterns(testFile)
	require.NoError(t, err)

	expectedPatterns := []string{
		"*/.git/*",
		"*/snapraid.parity/*",
		"./sda1/*",
	}
	assert.Equal(t, expectedPatterns, patterns)
}

func TestShouldExclude(t *testing.T) {
	patterns := []string{
		"*/.git/*",
		"*/snapraid.parity/*",
		"./sda1/*",
	}

	testCases := []struct {
		path     string
		expected bool
	}{
		{"project/.git/config", true},
		{"data/snapraid.parity/file", true},
		{"./sda1/somefile", true},
		{"normal/file.txt", false},
		{"snapraid.parity", false},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			result := shouldExclude(tc.path, patterns)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestWalkFiles(t *testing.T) {
	fs := afero.NewMemMapFs()
	rootDir := "/testroot"

	// Create test directory structure
	testFiles := []string{
		"/testroot/file1.txt",
		"/testroot/.git/config",
		"/testroot/snapraid.parity/data",
		"/testroot/sda1/file2.txt",
		"/testroot/normaldir/file3.txt",
	}

	for _, file := range testFiles {
		err := fs.MkdirAll(filepath.Dir(file), 0755)
		require.NoError(t, err)
		err = afero.WriteFile(fs, file, []byte("test content"), 0644)
		require.NoError(t, err)
	}

	excludePatterns := []string{
		"*/.git/*",
		"*/snapraid.parity/*",
		"./sda1/*",
	}

	fileChan := make(chan string, len(testFiles))
	err := walkFiles(rootDir, 1, fileChan, excludePatterns)
	require.NoError(t, err)
	close(fileChan)

	var collectedFiles []string
	for file := range fileChan {
		collectedFiles = append(collectedFiles, file)
	}

	expectedFiles := []string{
		"file1.txt",
		"normaldir/file3.txt",
	}

	assert.ElementsMatch(t, expectedFiles, collectedFiles)
}
