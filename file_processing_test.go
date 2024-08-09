// file_processing_test.go
package main

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestProcessFile(t *testing.T) {
	// 创建一个临时文件
	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // 清理

	// 写入一些数据
	if _, err := tmpfile.Write([]byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	// 创建一个 miniredis 实例
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	// 创建一个真实的 Redis 客户端，连接到 miniredis
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	// 创建一个 FileProcessor
	fp := NewFileProcessor(rdb, ctx)

	// 保存原始的 generateHash 函数
	originalGenerateHash := fp.generateHashFunc

	// 创建一个新的 generateHash 函数
	fp.generateHashFunc = func(s string) string {
		return "mockedHash"
	}

	// 在测试结束时恢复原始函数
	defer func() { fp.generateHashFunc = originalGenerateHash }()

	// 模拟 calculateFileHash 方法
	calculateFileHashCalls := 0
	fp.calculateFileHashFunc = func(path string, limit int64) (string, error) {
		calculateFileHashCalls++
		if calculateFileHashCalls == 1 {
			return "file_hash", nil
		}
		return "full_hash", nil
	}

	// 处理文件
	err = fp.ProcessFile(tmpfile.Name())
	if err != nil {
		t.Errorf("ProcessFile() error = %v", err)
	}

	// 验证 Redis 中的数据
	fileInfoData, err := rdb.Get(ctx, "fileInfo:mockedHash").Bytes()
	assert.NoError(t, err)

	var storedFileInfo FileInfo
	err = decodeGob(fileInfoData, &storedFileInfo)
	assert.NoError(t, err)

	assert.Equal(t, int64(11), storedFileInfo.Size) // "hello world" 的长度

	// 验证其他 Redis 键
	assert.Equal(t, tmpfile.Name(), rdb.Get(ctx, "hashedKeyToPath:mockedHash").Val())
	assert.True(t, rdb.SIsMember(ctx, "fileHashToPathSet:file_hash", tmpfile.Name()).Val())
	assert.Equal(t, "full_hash", rdb.Get(ctx, "hashedKeyToFullHash:mockedHash").Val())
	assert.Equal(t, "mockedHash", rdb.Get(ctx, "pathToHashedKey:"+tmpfile.Name()).Val())
	assert.Equal(t, "file_hash", rdb.Get(ctx, "hashedKeyToFileHash:mockedHash").Val())

	// 验证 calculateFileHash 被调用了两次
	assert.Equal(t, 2, calculateFileHashCalls)
}

func TestExtractTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		want     string
	}{
		{"With timestamp", "/path/to/file_12:34:56.txt", "12:34:56"},
		{"Without timestamp", "/path/to/file.txt", ""},
		{"With date", "/path/2021-05-01/file.txt", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractTimestamp(tt.filePath)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCalculateScore(t *testing.T) {
	tests := []struct {
		name           string
		timestamp      string
		fileNameLength int
		want           float64
	}{
		{"With timestamp", "12:34:56", 10, -8010},
		{"Without timestamp", "", 10, -10},
		{"Short timestamp", "12:34", 5, -5005},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateScore(tt.timestamp, tt.fileNameLength)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSaveDuplicateFileInfoToRedis(t *testing.T) {
	rdb, mock := redismock.NewClientMock()
	ctx := context.Background()
	fp := NewFileProcessor(rdb, ctx)

	fullHash := "testhash"
	info := FileInfo{Size: 1000, ModTime: time.Unix(1620000000, 0)}
	filePath := "/path/to/file_12:34:56.txt"

	mock.ExpectZAdd("duplicateFiles:"+fullHash, &redis.Z{
		Score:  -8017, // 更新期望的分数
		Member: filePath,
	}).SetVal(1)

	err := fp.SaveDuplicateFileInfoToRedis(fullHash, info, filePath)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// Add more tests for other functions...
