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
	"path/filepath" // 添加这行
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

func TestCalculateScore(t *testing.T) {
	tests := []struct {
		name           string
		timestamps     []string
		fileNameLength int
		want           float64
	}{
		{"Basic timestamp", []string{"12:34:56"}, 10, -1010},
		{"Multiple timestamps", []string{"02:43", "07:34", "10:26"}, 15, -3015},
		{"Many timestamps", []string{"24:30", "01:11:27", "01:40:56", "02:35:52"}, 20, -4020},
		{"No timestamp", []string{}, 30, -30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateScore(tt.timestamps, tt.fileNameLength)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractTimestamps(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		want     []string
	}{
		{
			"Multiple timestamps",
			"/Users/huangyingw/mini/media/usb_backup_crypt_8T_1/cartoon/dragonball/第一部/龙珠 第一部 日语配音/七龙珠146.mp4:24:30,:1:11:27,1:40:56,:02:35:52",
			[]string{"24:30", "01:11:27", "01:40:56", "02:35:52"},
		},
		{
			"Timestamps with different formats",
			"/Users/huangyingw/mini/media/usb_backup_crypt_8T_1/cartoon/dragonball/第一部/龙珠 第一部 日语配音/七龙珠146.rmvb:24:30,1:11:27,:02:35:52",
			[]string{"24:30", "01:11:27", "02:35:52"},
		},
		{
			"Short timestamps",
			"/Users/huangyingw/mini/media/usb_backup_crypt_8T_1/cartoon/dragonball/第一部/龙珠 第一部 日语配音/七龙珠146.mp4:02:43,07:34,10:26",
			[]string{"02:43", "07:34", "10:26"},
		},
		{
			"Many timestamps",
			"/Users/huangyingw/mini/media/usb_backup_crypt_8T_1/cartoon/dragonball/第一部/龙珠 第一部 日语配音/七龙珠146.mp4:24:30,:1:11:27,1:40:56,:02:35:52,:02:36:03,:2:39:25,:2:43:06,:2:48:24,:2:53:16,:3:08:41,:3:58:08,:4:00:38,5:12:14,5:24:58,5:36:54,5:41:01,:6:16:21,:6:20:03",
			[]string{"24:30", "01:11:27", "01:40:56", "02:35:52", "02:36:03", "02:39:25", "02:43:06", "02:48:24", "02:53:16", "03:08:41", "03:58:08", "04:00:38", "05:12:14", "05:24:58", "05:36:54", "05:41:01", "06:16:21", "06:20:03"},
		},
		{
			"Timestamps in folder names",
			"/Users/huangyingw/mini/media/usb_backup_crypt_8T_1/cartoon/dragonball/第一部:24:30,/龙珠 第一部 日语配音:1:11:27,/七龙珠146.mp4:1:40:56,/更多文件:02:35:52",
			[]string{"24:30", "01:11:27", "01:40:56", "02:35:52"},
		},
		{
			"Mixed format timestamps in path",
			"/path/to/video/24:30,15:24,/subfolder:1:11:27,3:45,1:7/anotherfolder:02:35:52,/finalfile.mp4:03:45",
			[]string{"01:07", "03:45", "15:24", "24:30", "01:11:27", "02:35:52"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractTimestamps(tt.filePath)
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

	expectedScore := float64(-(1*1000 + len(filepath.Base(filePath)))) // 更新期望的分数计算

	mock.ExpectZAdd("duplicateFiles:"+fullHash, &redis.Z{
		Score:  expectedScore,
		Member: filePath,
	}).SetVal(1)

	err := fp.SaveDuplicateFileInfoToRedis(fullHash, info, filePath)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// Add more tests for other functions...
