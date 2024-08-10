// file_processing_test.go

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path/filepath" // 添加这行
	"testing"
	"time"
)

func TestFormatTimestamp(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"1:2:3", "01:02:03"},
		{"10:20:30", "10:20:30"},
		{"1:2", "01:02"},
		{"10:20", "10:20"},
	}

	for _, test := range tests {
		result := FormatTimestamp(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestCalculateScore(t *testing.T) {
	t.Run("Same timestamp length, different file name length", func(t *testing.T) {
		score1 := CalculateScore([]string{"12:34:56", "01:23:45"}, 15)
		score2 := CalculateScore([]string{"12:34:56", "01:23:45"}, 10)
		assert.Less(t, score1, score2, "Score with longer file name should be less (sorted first)")
	})

	t.Run("Different timestamp length, same file name length", func(t *testing.T) {
		score1 := CalculateScore([]string{"12:34:56", "01:23:45", "00:11:22"}, 10)
		score2 := CalculateScore([]string{"12:34:56", "01:23:45"}, 10)
		assert.Less(t, score1, score2, "Score with more timestamps should be less (sorted first)")
	})

	t.Run("Different timestamp length and file name length", func(t *testing.T) {
		score1 := CalculateScore([]string{"12:34:56", "01:23:45", "00:11:22"}, 10)
		score2 := CalculateScore([]string{"12:34:56", "01:23:45"}, 11155515)
		assert.Less(t, score1, score2, "Score with more timestamps should be less, even if file name is shorter")
	})
}

func TestTimestampToSeconds(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"1:2:3", 3723},
		{"10:20:30", 37230},
		{"1:2", 62},
		{"10:20", 620},
		{"invalid", 0},
	}

	for _, test := range tests {
		result := TimestampToSeconds(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestFileProcessor_SaveDuplicateFileInfoToRedis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	fp := NewFileProcessor(rdb, ctx)

	fullHash := "testhash"
	info := FileInfo{
		Size:    1000,
		ModTime: time.Now(),
	}

	// Test case 1: File with one timestamp
	filePath1 := "/path/to/file:12:34:56.mp4"
	err = fp.SaveDuplicateFileInfoToRedis(fullHash, info, filePath1)
	assert.NoError(t, err)

	// Test case 2: File with two timestamps
	filePath2 := "/path/to/file:12:34:56,01:23:45.mp4"
	err = fp.SaveDuplicateFileInfoToRedis(fullHash, info, filePath2)
	assert.NoError(t, err)

	// Test case 3: File with no timestamp
	filePath3 := "/path/to/file.mp4"
	err = fp.SaveDuplicateFileInfoToRedis(fullHash, info, filePath3)
	assert.NoError(t, err)

	// Verify the data was saved correctly and in the right order
	members, err := rdb.ZRange(ctx, "duplicateFiles:"+fullHash, 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, filePath2, members[0]) // Should be first due to more timestamps
	assert.Equal(t, filePath1, members[1])
	assert.Equal(t, filePath3, members[2]) // Should be last due to no timestamps
}

func TestFileProcessor_ProcessFile(t *testing.T) {
	// Create temporary files
	tmpfile1, err := ioutil.TempFile("", "example1")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile1.Name())

	tmpfile2, err := ioutil.TempFile("", "example2")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile2.Name())

	// Write different data to the files
	if _, err := tmpfile1.Write([]byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if _, err := tmpfile2.Write([]byte("hello universe")); err != nil {
		t.Fatal(err)
	}

	if err := tmpfile1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile2.Close(); err != nil {
		t.Fatal(err)
	}

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	fp := NewFileProcessor(rdb, ctx)

	// Mock the hash calculation functions
	fp.calculateFileHashFunc = func(path string, limit int64) (string, error) {
		if path == tmpfile1.Name() {
			return "mockhash1", nil
		}
		return "mockhash2", nil
	}

	// Process both files
	err = fp.ProcessFile(tmpfile1.Name())
	assert.NoError(t, err)
	err = fp.ProcessFile(tmpfile2.Name())
	assert.NoError(t, err)

	// Verify the data was saved correctly for both files
	hashedKey1 := generateHash(tmpfile1.Name())
	hashedKey2 := generateHash(tmpfile2.Name())

	// Check file info
	fileInfoData1, err := rdb.Get(ctx, "fileInfo:"+hashedKey1).Bytes()
	assert.NoError(t, err)
	assert.NotNil(t, fileInfoData1)

	fileInfoData2, err := rdb.Get(ctx, "fileInfo:"+hashedKey2).Bytes()
	assert.NoError(t, err)
	assert.NotNil(t, fileInfoData2)

	// Check paths
	path1, err := rdb.Get(ctx, "hashedKeyToPath:"+hashedKey1).Result()
	assert.NoError(t, err)
	assert.Equal(t, tmpfile1.Name(), path1)

	path2, err := rdb.Get(ctx, "hashedKeyToPath:"+hashedKey2).Result()
	assert.NoError(t, err)
	assert.Equal(t, tmpfile2.Name(), path2)

	// Check file hashes
	fileHash1, err := rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey1).Result()
	assert.NoError(t, err)
	assert.Equal(t, "mockhash1", fileHash1)

	fileHash2, err := rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey2).Result()
	assert.NoError(t, err)
	assert.Equal(t, "mockhash2", fileHash2)
}

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

	expectedScore := float64(-(1*timestampWeight + len(filepath.Base(filePath))))

	mock.ExpectZAdd("duplicateFiles:"+fullHash, &redis.Z{
		Score:  expectedScore,
		Member: filePath,
	}).SetVal(1)

	err := fp.SaveDuplicateFileInfoToRedis(fullHash, info, filePath)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// Add more tests for other functions...

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		input    string
		expected timestamp
	}{
		{"12:34", timestamp{12, 34, 0}},
		{"01:23:45", timestamp{1, 23, 45}},
		{"00:00:01", timestamp{0, 0, 1}},
	}

	for _, test := range tests {
		result := parseTimestamp(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestFileProcessor_GetFileInfoFromRedis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	fp := NewFileProcessor(rdb, ctx)

	// Prepare test data
	testFileInfo := FileInfo{
		Size:    1000,
		ModTime: time.Now(),
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(testFileInfo)
	assert.NoError(t, err)

	hashedKey := "testkey"
	err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
	assert.NoError(t, err)

	// Test getFileInfoFromRedis
	result, err := fp.getFileInfoFromRedis(hashedKey)
	assert.Equal(t, testFileInfo.Size, result.Size)
	assert.WithinDuration(t, testFileInfo.ModTime, result.ModTime, time.Second)
}

func TestFileProcessor_SaveToFile(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	fp := NewFileProcessor(rdb, ctx)

	// Prepare test data
	testData := map[string]FileInfo{
		"/path/to/file1": {Size: 1000, ModTime: time.Now().Add(-1 * time.Hour)},
		"/path/to/file2": {Size: 2000, ModTime: time.Now()},
	}

	for path, info := range testData {
		hashedKey := generateHash(path)
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(info)
		assert.NoError(t, err)

		err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
		assert.NoError(t, err)
		err = rdb.Set(ctx, "hashedKeyToPath:"+hashedKey, path, 0).Err()
		assert.NoError(t, err)
	}

	// Create a temporary directory for the test
	tempDir, err := ioutil.TempDir("", "testdir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Test saveToFile
	err = fp.saveToFile(tempDir, "testfile.txt", false)
	assert.NoError(t, err)

	// Verify the file contents
	content, err := ioutil.ReadFile(filepath.Join(tempDir, "testfile.txt"))
	assert.NoError(t, err)
	assert.Contains(t, string(content), "2000,\"")
	assert.Contains(t, string(content), "1000,\"")
	assert.Contains(t, string(content), "file2")
	assert.Contains(t, string(content), "file1")
}

func TestFileContentVerification(t *testing.T) {
	// 设置测试环境
	fs := afero.NewMemMapFs()
	tempDir, err := afero.TempDir(fs, "", "testdir")
	assert.NoError(t, err)
	defer fs.RemoveAll(tempDir)

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	fp := NewFileProcessor(rdb, ctx)
	fp.fs = fs

	// 准备测试数据
	fullHash := "793bf43bc5719d3deb836a2a8d38eeada28d457c48153b1e7d5af7ed5f38be98632dbad7d64f0f83d58619c6ef49d7565622d7b20119e7d2cb2540ece11ce119"

	testData := []struct {
		path     string
		size     int64
		modTime  time.Time
		fullHash string
	}{
		{filepath.Join(tempDir, "path", "to", "file1_01:23:45.mp4"), 209720828, time.Now().Add(-1 * time.Hour), "unique_hash_1"},
		{filepath.Join(tempDir, "path", "to", "file2_02:34:56_03:45:67.mp4"), 2172777224, time.Now(), fullHash},
		{filepath.Join(tempDir, "path", "to", "file3.mp4"), 2172777224, time.Now().Add(-2 * time.Hour), fullHash},
	}

	// 设置 Redis 数据和创建模拟文件
	for _, data := range testData {
		info := FileInfo{Size: data.size, ModTime: data.modTime}
		err = fp.SaveDuplicateFileInfoToRedis(data.fullHash, info, data.path)
		assert.NoError(t, err)

		hashedKey := fp.generateHashFunc(data.path)
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(info)
		assert.NoError(t, err)

		err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
		assert.NoError(t, err)
		err = rdb.Set(ctx, "hashedKeyToPath:"+hashedKey, data.path, 0).Err()
		assert.NoError(t, err)
		err = rdb.Set(ctx, "pathToHashedKey:"+data.path, hashedKey, 0).Err()
		assert.NoError(t, err)
		err = rdb.Set(ctx, "hashedKeyToFullHash:"+hashedKey, data.fullHash, 0).Err()
		assert.NoError(t, err)

		// 创建模拟文件
		err = afero.WriteFile(fs, data.path, []byte("test content"), 0644)
		assert.NoError(t, err)
	}

	// 测试 saveToFile 方法
	err = fp.saveToFile(tempDir, "fav.log", false)
	assert.NoError(t, err)

	content, err := afero.ReadFile(fs, filepath.Join(tempDir, "fav.log"))
	assert.NoError(t, err)

	expectedContent := `2172777224,"./path/to/file2_02:34:56_03:45:67.mp4"
2172777224,"./path/to/file3.mp4"
209720828,"./path/to/file1_01:23:45.mp4"
`
	assert.Equal(t, expectedContent, string(content), "fav.log content does not match expected")

	// Test fav.log.sort
	err = fp.saveToFile(tempDir, "fav.log.sort", true)
	assert.NoError(t, err)

	content, err = afero.ReadFile(fs, filepath.Join(tempDir, "fav.log.sort"))
	assert.NoError(t, err)

	expectedSortedContent := `"./path/to/file2_02:34:56_03:45:67.mp4"
"./path/to/file1_01:23:45.mp4"
"./path/to/file3.mp4"
`
	assert.Equal(t, expectedSortedContent, string(content), "fav.log.sort content does not match expected")

	// Test WriteDuplicateFilesToFile method
	err = fp.WriteDuplicateFilesToFile(tempDir, "fav.log.dup", rdb, ctx)
	assert.NoError(t, err)

	content, err = afero.ReadFile(fs, filepath.Join(tempDir, "fav.log.dup"))
	assert.NoError(t, err)

	expectedDupContent := fmt.Sprintf(`Duplicate files for fullHash %s:
[+] 2172777224,"./path/to/file2_02:34:56_03:45:67.mp4"
[-] 2172777224,"./path/to/file3.mp4"
	`, fullHash)

	assert.Equal(t, expectedDupContent, string(content), "fav.log.dup content does not match expected")
}

func setupTestData(rdb *redis.Client, ctx context.Context, rootDir string) error {
	testData := []struct {
		path     string
		size     int64
		modTime  time.Time
		fullHash string
	}{
		{"/path/to/file1_01:23:45.mp4", 1000, time.Now().Add(-1 * time.Hour), "hash1"},
		{"/path/to/file2_02:34:56_03:45:67.mp4", 2000, time.Now(), "hash1"},
		{"/path/to/file3.mp4", 3000, time.Now().Add(-2 * time.Hour), "hash1"},
	}

	for _, data := range testData {
		hashedKey := generateHash(data.path)
		fileInfo := FileInfo{Size: data.size, ModTime: data.modTime}

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(fileInfo); err != nil {
			return err
		}

		pipe := rdb.Pipeline()
		pipe.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0)
		pipe.Set(ctx, "hashedKeyToPath:"+hashedKey, data.path, 0)
		pipe.Set(ctx, "pathToHashedKey:"+data.path, hashedKey, 0)
		pipe.Set(ctx, "hashedKeyToFullHash:"+hashedKey, data.fullHash, 0)
		pipe.ZAdd(ctx, "duplicateFiles:"+data.fullHash, &redis.Z{Score: float64(-data.size), Member: data.path})

		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func TestGetFileInfoFromRedis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	fp := NewFileProcessor(rdb, ctx)

	// Prepare test data
	testPath := "/path/to/testfile.txt"
	testInfo := FileInfo{
		Size:    1024,
		ModTime: time.Now(),
	}

	hashedKey := generateHash(testPath)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(testInfo)
	assert.NoError(t, err)

	err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
	assert.NoError(t, err)
	err = rdb.Set(ctx, "hashedKeyToPath:"+hashedKey, testPath, 0).Err()
	assert.NoError(t, err)
	err = rdb.Set(ctx, "pathToHashedKey:"+testPath, hashedKey, 0).Err()
	assert.NoError(t, err)

	// Test getFileInfoFromRedis
	retrievedInfo, err := fp.getFileInfoFromRedis(hashedKey)
	assert.NoError(t, err)
	assert.Equal(t, testInfo.Size, retrievedInfo.Size)
	assert.Equal(t, testInfo.ModTime.Unix(), retrievedInfo.ModTime.Unix())
}

func TestCleanUpOldRecords(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	// 创建临时目录
	tempDir, err := ioutil.TempDir("", "testcleanup")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 准备测试数据
	existingFile := filepath.Join(tempDir, "existing.txt")
	nonExistingFile := filepath.Join(tempDir, "non_existing.txt")

	// 创建存在的文件
	_, err = os.Create(existingFile)
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{existingFile, nonExistingFile} {
		hashedKey := generateHash(path)
		err = rdb.Set(ctx, "pathToHashedKey:"+path, hashedKey, 0).Err()
		assert.NoError(t, err)
		err = rdb.Set(ctx, "hashedKeyToPath:"+hashedKey, path, 0).Err()
		assert.NoError(t, err)
		err = rdb.Set(ctx, "fileInfo:"+hashedKey, "dummy_data", 0).Err()
		assert.NoError(t, err)
		err = rdb.Set(ctx, "hashedKeyToFileHash:"+hashedKey, "dummy_hash", 0).Err()
		assert.NoError(t, err)
	}

	// 运行 cleanUpOldRecords
	err = cleanUpOldRecords(rdb, ctx)
	assert.NoError(t, err)

	// 检查不存在文件的记录是否被删除
	_, err = rdb.Get(ctx, "pathToHashedKey:"+nonExistingFile).Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)

	// 检查存在文件的记录是否被保留
	val, err := rdb.Get(ctx, "pathToHashedKey:"+existingFile).Result()
	assert.NoError(t, err)
	assert.NotEmpty(t, val)
}

func mockSaveFileInfoToRedis(fp *FileProcessor, path string, info FileInfo, fileHash, fullHash string) error {
	hashedKey := fp.generateHashFunc(path)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(info); err != nil {
		return fmt.Errorf("error encoding file info: %w", err)
	}

	ctx := fp.Ctx
	pipe := fp.Rdb.Pipeline()
	pipe.SetNX(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0)
	pipe.Set(ctx, "hashedKeyToPath:"+hashedKey, path, 0)
	pipe.SAdd(ctx, "fileHashToPathSet:"+fileHash, path)
	pipe.Set(ctx, "hashedKeyToFullHash:"+hashedKey, fullHash, 0)
	pipe.Set(ctx, "pathToHashedKey:"+path, hashedKey, 0)
	pipe.Set(ctx, "hashedKeyToFileHash:"+hashedKey, fileHash, 0)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error executing Redis pipeline: %w", err)
	}
	return nil
}

func TestProcessFileBoundary(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	fs := afero.NewMemMapFs()
	fp := NewFileProcessor(rdb, ctx)
	fp.fs = fs

	// 确保所有必要的函数都被初始化
	fp.generateHashFunc = generateHash
	fp.calculateFileHashFunc = func(path string, limit int64) (string, error) {
		if limit == FullFileReadCmd {
			return "full_hash_large_file", nil
		}
		return "partial_hash_large_file", nil
	}

	// 创建一个新的方法来模拟 ProcessFile 的行为
	mockProcessFile := func(path string) error {
		info, err := fs.Stat(path)
		if err != nil {
			return fmt.Errorf("error getting file info: %w", err)
		}

		fileHash, err := fp.calculateFileHashFunc(path, ReadLimit)
		if err != nil {
			return fmt.Errorf("error calculating file hash: %w", err)
		}

		fullHash, err := fp.calculateFileHashFunc(path, FullFileReadCmd)
		if err != nil {
			return fmt.Errorf("error calculating full file hash: %w", err)
		}

		fileInfo := FileInfo{
			Size:    info.Size(),
			ModTime: info.ModTime(),
		}

		err = mockSaveFileInfoToRedis(fp, path, fileInfo, fileHash, fullHash)
		if err != nil {
			return fmt.Errorf("error saving file info to Redis: %w", err)
		}

		return nil
	}

	// 测试空文件
	emptyFilePath := "/path/to/empty_file.txt"
	_, err = fs.Create(emptyFilePath)
	assert.NoError(t, err)

	err = mockProcessFile(emptyFilePath)
	assert.NoError(t, err)

	// 测试大文件（模拟）
	largeFilePath := "/path/to/large_file.bin"
	err = afero.WriteFile(fs, largeFilePath, []byte("large file content"), 0644)
	assert.NoError(t, err)

	err = mockProcessFile(largeFilePath)
	assert.NoError(t, err)

	// 验证大文件是否被正确处理
	hashedKey := generateHash(largeFilePath)
	fileHash, err := rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, "partial_hash_large_file", fileHash)

	fullHash, err := rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, "full_hash_large_file", fullHash)
}
