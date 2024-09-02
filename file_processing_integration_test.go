package main

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestFileProcessorIntegration(t *testing.T) {
	// Set up test environment
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()
	fs := afero.NewMemMapFs()

	fp := CreateFileProcessor(rdb, ctx, testExcludeRegexps)
	fp.fs = fs

	// Create test directory structure
	tempDir, err := afero.TempDir(fs, "", "test")
	require.NoError(t, err)

	testFiles := []struct {
		path    string
		content string
		size    int64
	}{
		{filepath.Join(tempDir, "file1.txt"), "content1", 8},
		{filepath.Join(tempDir, "file2.txt"), "content2", 8},
		{filepath.Join(tempDir, "subdir", "file3.txt"), "content3", 8},
	}

	for _, tf := range testFiles {
		err := fs.MkdirAll(filepath.Dir(tf.path), 0755)
		require.NoError(t, err)
		err = afero.WriteFile(fs, tf.path, []byte(tf.content), 0644)
		require.NoError(t, err)
	}

	// Process files
	for _, tf := range testFiles {
		relPath, err := filepath.Rel(tempDir, tf.path)
		require.NoError(t, err)
		err = fp.ProcessFile(tempDir, relPath, true)
		require.NoError(t, err)
	}

	// Test saveToFile
	t.Run("SaveToFile", func(t *testing.T) {
		err := fp.saveToFile(tempDir, "fav.log", false)
		require.NoError(t, err)

		content, err := afero.ReadFile(fs, filepath.Join(tempDir, "fav.log"))
		require.NoError(t, err)

		assert.Contains(t, string(content), "8,\"./file1.txt\"")
		assert.Contains(t, string(content), "8,\"./file2.txt\"")
		assert.Contains(t, string(content), "8,\"./subdir/file3.txt\"")
	})

	// Test WriteDuplicateFilesToFile
	t.Run("WriteDuplicateFilesToFile", func(t *testing.T) {
		// Create duplicate files in Redis
		fullHash := "testhash"
		for _, tf := range testFiles {
			err := SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, FileInfo{
				Size: tf.size,
				Path: tf.path,
			})
			require.NoError(t, err)
		}

		err := fp.WriteDuplicateFilesToFile(tempDir, "fav.log.dup", rdb, ctx)
		require.NoError(t, err)

		content, err := afero.ReadFile(fs, filepath.Join(tempDir, "fav.log.dup"))
		require.NoError(t, err)

		assert.Contains(t, string(content), "Duplicate files for fullHash testhash:")
		assert.Contains(t, string(content), "[+] 8,\"./file1.txt\"")
		assert.Contains(t, string(content), "[-] 8,\"./file2.txt\"")
		assert.Contains(t, string(content), "[-] 8,\"./subdir/file3.txt\"")
	})
}
