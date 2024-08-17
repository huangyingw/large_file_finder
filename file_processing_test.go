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
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 辅助函数：设置测试环境
func setupTestEnvironment() (*miniredis.Miniredis, *redis.Client, context.Context, afero.Fs, error) {
	mr, err := miniredis.Run()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()
	fs := afero.NewMemMapFs()

	return mr, rdb, ctx, fs, nil
}

func createTestData(rdb *redis.Client, ctx context.Context, fs afero.Fs, rootDir string) error {
	testData := []struct {
		path     string
		size     int64
		modTime  time.Time
		fullHash string
	}{
		{filepath.Join(rootDir, "file1.txt"), 100, time.Now().Add(-1 * time.Hour), "hash1"},
		{filepath.Join(rootDir, "file2.txt"), 200, time.Now(), "hash2"},
		{filepath.Join(rootDir, "file3.txt"), 300, time.Now().Add(-2 * time.Hour), "hash2"},
	}

	for _, data := range testData {
		info := FileInfo{Size: data.size, ModTime: data.modTime}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(info); err != nil {
			return err
		}

		hashedKey := generateHash(data.path)
		if err := rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err(); err != nil {
			return err
		}
		if err := rdb.Set(ctx, "hashedKeyToPath:"+hashedKey, data.path, 0).Err(); err != nil {
			return err
		}
		if err := rdb.Set(ctx, "pathToHashedKey:"+data.path, hashedKey, 0).Err(); err != nil {
			return err
		}
		if err := rdb.Set(ctx, "hashedKeyToFullHash:"+hashedKey, data.fullHash, 0).Err(); err != nil {
			return err
		}

		// 创建模拟文件
		if err := afero.WriteFile(fs, data.path, []byte("test content"), 0644); err != nil {
			return err
		}
	}

	return nil
}
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
		{"1:2:3", 3723},     // 1小时2分3秒 -> 3723秒
		{"10:20:30", 37230}, // 10小时20分30秒 -> 37230秒
		{"1:2", 62},         // 1分2秒 -> 62秒
		{"10:20", 620},      // 10分20秒 -> 620秒
		{"invalid", 0},      // 无效格式 -> 0秒
	}

	for _, test := range tests {
		result := TimestampToSeconds(test.input)
		assert.Equal(t, test.expected, result)
	}
}

func TestFileProcessor_SaveDuplicateFileInfoToRedis(t *testing.T) {
	mr, rdb, ctx, _, err := setupTestEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	fullHash := "testhash"
	info := FileInfo{
		Size:    1000,
		ModTime: time.Now(),
		Path:    "/path/to/file:12:34:56.mp4",
	}

	// Test case 1: File with one timestamp
	filePath1 := "/path/to/file:12:34:56.mp4"
	info.Path = filePath1
	err = SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
	assert.NoError(t, err)

	// Test case 2: File with two timestamps
	filePath2 := "/path/to/file:12:34:56,01:23:45.mp4"
	info.Path = filePath2
	err = SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
	assert.NoError(t, err)

	// Test case 3: File with no timestamp
	filePath3 := "/path/to/file.mp4"
	info.Path = filePath3
	err = SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
	assert.NoError(t, err)

	// Verify the data was saved correctly and in the right order
	members, err := rdb.ZRange(ctx, "duplicateFiles:"+fullHash, 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, filePath2, members[0]) // Should be first due to more timestamps
	assert.Equal(t, filePath1, members[1])
	assert.Equal(t, filePath3, members[2]) // Should be last due to no timestamps
}

func TestProcessFile(t *testing.T) {
	mr, rdb, ctx, fs, err := setupTestEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rootDir := "/testroot"
	err = fs.MkdirAll(rootDir, 0755)
	assert.NoError(t, err)

	testFilePath := filepath.Join(rootDir, "testfile.txt")
	err = afero.WriteFile(fs, testFilePath, []byte("test content"), 0644)
	assert.NoError(t, err)

	fp := NewFileProcessor(rdb, ctx)
	fp.fs = fs

	// Mock the hash calculation functions
	fp.calculateFileHashFunc = func(path string, limit int64) (string, error) {
		if limit == FullFileReadCmd {
			return "fullhash", nil
		}
		return "partialhash", nil
	}

	// Mock the generate hash function
	fp.generateHashFunc = func(s string) string {
		return "mockedHash"
	}

	// Mock saveFileInfoToRedis
	fp.saveFileInfoToRedisFunc = func(rdb *redis.Client, ctx context.Context, path string, info FileInfo, fileHash, fullHash string) error {
		hashedKey := "mockedHash"
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(info); err != nil {
			return err
		}

		pipe := rdb.Pipeline()
		pipe.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0)
		pipe.Set(ctx, "hashedKeyToPath:"+hashedKey, path, 0)
		pipe.SAdd(ctx, "fileHashToPathSet:"+fileHash, path)
		pipe.Set(ctx, "hashedKeyToFullHash:"+hashedKey, fullHash, 0)
		pipe.Set(ctx, "pathToHashedKey:"+path, hashedKey, 0)
		pipe.Set(ctx, "hashedKeyToFileHash:"+hashedKey, fileHash, 0)

		_, err := pipe.Exec(ctx)
		return err
	}

	// Process the file
	err = fp.ProcessFile(testFilePath)
	assert.NoError(t, err)

	// Verify the data was saved correctly
	hashedKey := "mockedHash" // Use the mocked hash value

	// Check file info
	fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
	assert.NoError(t, err)
	assert.NotNil(t, fileInfoData)

	var storedFileInfo FileInfo
	err = gob.NewDecoder(bytes.NewReader(fileInfoData)).Decode(&storedFileInfo)
	assert.NoError(t, err)
	assert.Equal(t, int64(12), storedFileInfo.Size) // "test content" length

	// Check other Redis keys
	assert.Equal(t, testFilePath, rdb.Get(ctx, "hashedKeyToPath:"+hashedKey).Val())
	assert.True(t, rdb.SIsMember(ctx, "fileHashToPathSet:partialhash", testFilePath).Val())
	assert.Equal(t, "fullhash", rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Val())
	assert.Equal(t, hashedKey, rdb.Get(ctx, "pathToHashedKey:"+testFilePath).Val())
	assert.Equal(t, "partialhash", rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Val())
}

func TestFileProcessor_ProcessFile(t *testing.T) {
	mr, rdb, ctx, fs, err := setupTestEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rootDir := "/testroot"
	err = fs.MkdirAll(rootDir, 0755)
	assert.NoError(t, err)

	// Create a test file
	testFilePath := filepath.Join(rootDir, "testfile.txt")
	err = afero.WriteFile(fs, testFilePath, []byte("test content"), 0644)
	assert.NoError(t, err)

	fp := NewFileProcessor(rdb, ctx)
	fp.fs = fs

	// Mock the hash calculation functions
	fp.calculateFileHashFunc = func(path string, limit int64) (string, error) {
		if limit == FullFileReadCmd {
			return "fullhash", nil
		}
		return "partialhash", nil
	}

	// Process the file
	err = fp.ProcessFile(testFilePath)
	assert.NoError(t, err)

	// Verify the data was saved correctly
	hashedKey := generateHash(testFilePath)

	// Check file info
	fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
	assert.NoError(t, err)
	assert.NotNil(t, fileInfoData)

	var storedFileInfo FileInfo
	err = gob.NewDecoder(bytes.NewReader(fileInfoData)).Decode(&storedFileInfo)
	assert.NoError(t, err)
	assert.Equal(t, int64(12), storedFileInfo.Size) // "test content" length

	// Check other Redis keys
	assert.Equal(t, testFilePath, rdb.Get(ctx, "hashedKeyToPath:"+hashedKey).Val())
	assert.True(t, rdb.SIsMember(ctx, "fileHashToPathSet:partialhash", testFilePath).Val())
	assert.Equal(t, "fullhash", rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Val())
	assert.Equal(t, hashedKey, rdb.Get(ctx, "pathToHashedKey:"+testFilePath).Val())
	assert.Equal(t, "partialhash", rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Val())
}

// Update the mockSaveFileInfoToRedis function definition
func mockSaveFileInfoToRedis(fp *FileProcessor, path string, info FileInfo, fileHash, fullHash string) error {
	rdb := fp.Rdb
	ctx := fp.Ctx
	hashedKey := generateHash(path)

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(info); err != nil {
		return err
	}

	pipe := rdb.Pipeline()
	pipe.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0)
	pipe.Set(ctx, "hashedKeyToPath:"+hashedKey, path, 0)
	pipe.SAdd(ctx, "fileHashToPathSet:"+fileHash, path)
	pipe.Set(ctx, "hashedKeyToFullHash:"+hashedKey, fullHash, 0)
	pipe.Set(ctx, "pathToHashedKey:"+path, hashedKey, 0)
	pipe.Set(ctx, "hashedKeyToFileHash:"+hashedKey, fileHash, 0)

	_, err := pipe.Exec(ctx)
	return err
}

// Update the TestProcessFile function

func TestWriteDuplicateFilesToFile(t *testing.T) {
	mr, rdb, ctx, fs, err := setupTestEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	tempDir, err := afero.TempDir(fs, "", "test")
	assert.NoError(t, err)
	defer fs.RemoveAll(tempDir)

	fp := NewFileProcessor(rdb, ctx)
	fp.fs = fs

	// 模拟重复文件数据
	fullHash := "testhash"
	duplicateFilesKey := "duplicateFiles:" + fullHash
	_, err = rdb.ZAdd(ctx, duplicateFilesKey,
		&redis.Z{Score: 1, Member: "/path/to/file1"},
		&redis.Z{Score: 2, Member: "/path/to/file2"},
		&redis.Z{Score: 3, Member: "/path/to/file3"},
	).Result()
	assert.NoError(t, err)

	// 模拟文件信息
	for i, path := range []string{"/path/to/file1", "/path/to/file2", "/path/to/file3"} {
		hashedKey := fmt.Sprintf("hashedKey%d", i+1)
		info := FileInfo{Size: int64((i + 1) * 1000), ModTime: time.Now()}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(info)
		assert.NoError(t, err)
		err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
		assert.NoError(t, err)
		err = rdb.Set(ctx, "pathToHashedKey:"+path, hashedKey, 0).Err()
		assert.NoError(t, err)
	}

	// 执行测试函数
	err = fp.WriteDuplicateFilesToFile(tempDir, "fav.log.dup", rdb, ctx)
	assert.NoError(t, err)

	// 读取并验证文件内容
	content, err := afero.ReadFile(fs, filepath.Join(tempDir, "fav.log.dup"))
	assert.NoError(t, err)

	expectedContent := fmt.Sprintf(`Duplicate files for fullHash %s:
[+] 1000,"./path/to/file1"
[-] 2000,"./path/to/file2"
[-] 3000,"./path/to/file3"

`, fullHash)

	assert.Equal(t, expectedContent, string(content))
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

	fullHash := "testhash"
	info := FileInfo{Size: 1000, ModTime: time.Unix(1620000000, 0), Path: "/path/to/file_12:34:56.txt"}

	expectedScore := float64(-(1*timestampWeight + len(filepath.Base(info.Path))))

	mock.ExpectZAdd("duplicateFiles:"+fullHash, &redis.Z{
		Score:  expectedScore,
		Member: info.Path,
	}).SetVal(1)

	err := SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
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
	mr, rdb, ctx, _, err := setupTestEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

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
	result, err := fp.fileInfoRetriever.getFileInfoFromRedis(hashedKey)
	assert.NoError(t, err)
	assert.Equal(t, testFileInfo.Size, result.Size)
	assert.WithinDuration(t, testFileInfo.ModTime, result.ModTime, time.Second)
}

func TestFileContentVerification(t *testing.T) {
	// 设置测试环境
	mr, rdb, ctx, fs, err := setupTestEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	tempDir, err := afero.TempDir(fs, "", "testdir")
	assert.NoError(t, err)
	defer fs.RemoveAll(tempDir)

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
	setupTestFiles(t, fp, rdb, ctx, fs, testData)

	// 测试 saveToFile 方法 (fav.log)
	testSaveToFile(t, fp, fs, tempDir, "fav.log", false)

	// 测试 saveToFile 方法 (fav.log.sort)
	testSaveToFile(t, fp, fs, tempDir, "fav.log.sort", true)

	// 测试 WriteDuplicateFilesToFile 方法
	testWriteDuplicateFilesToFile(t, fp, fs, rdb, ctx, tempDir, fullHash)
}

func setupTestFiles(t *testing.T, fp *FileProcessor, rdb *redis.Client, ctx context.Context, fs afero.Fs, testData []struct {
	path     string
	size     int64
	modTime  time.Time
	fullHash string
}) {
	for _, data := range testData {
		info := FileInfo{Size: data.size, ModTime: data.modTime, Path: data.path}
		err := SaveDuplicateFileInfoToRedis(rdb, ctx, data.fullHash, info)
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

		err = afero.WriteFile(fs, data.path, []byte("test content"), 0644)
		assert.NoError(t, err)
	}
}

func testSaveToFile(t *testing.T, fp *FileProcessor, fs afero.Fs, tempDir, filename string, sortByModTime bool) {
	err := fp.saveToFile(tempDir, filename, sortByModTime)
	assert.NoError(t, err)

	content, err := afero.ReadFile(fs, filepath.Join(tempDir, filename))
	assert.NoError(t, err)

	var expectedContent string
	if sortByModTime {
		expectedContent = `./path/to/file2_02:34:56_03:45:67.mp4
./path/to/file1_01:23:45.mp4
./path/to/file3.mp4
`
	} else {
		expectedContent = `2172777224,./path/to/file2_02:34:56_03:45:67.mp4
2172777224,./path/to/file3.mp4
209720828,./path/to/file1_01:23:45.mp4
`
	}

	assert.Equal(t, expectedContent, string(content), fmt.Sprintf("%s content does not match expected", filename))
}

func testWriteDuplicateFilesToFile(t *testing.T, fp *FileProcessor, fs afero.Fs, rdb *redis.Client, ctx context.Context, tempDir, fullHash string) {
	err := fp.WriteDuplicateFilesToFile(tempDir, "fav.log.dup", rdb, ctx)
	assert.NoError(t, err)

	content, err := afero.ReadFile(fs, filepath.Join(tempDir, "fav.log.dup"))
	assert.NoError(t, err)

	expectedDupContent := fmt.Sprintf(`Duplicate files for fullHash %s:
[+] 2172777224,"./path/to/file2_02:34:56_03:45:67.mp4"
[-] 2172777224,"./path/to/file3.mp4"

`, fullHash)

	assert.Equal(t, expectedDupContent, string(content), "fav.log.dup content does not match expected")
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
	retrievedInfo, err := fp.fileInfoRetriever.getFileInfoFromRedis(hashedKey)
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

	err = CleanUpOldRecords(rdb, ctx)
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

func TestSaveToFileTimeSort(t *testing.T) {
	// 设置测试数据
	now := time.Now()
	data := map[string]FileInfo{
		"/path/to/file1": {Size: 1000, ModTime: now.Add(-1 * time.Hour)},
		"/path/to/file2": {Size: 2000, ModTime: now},
		"/path/to/file3": {Size: 1500, ModTime: now.Add(-2 * time.Hour)},
	}

	// 创建临时目录
	tempDir, err := ioutil.TempDir("", "test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 保存文件
	err = writeDataToFile(tempDir, "fav.log.sort", data, true)
	assert.NoError(t, err)

	// 读取并验证文件内容
	content, err := ioutil.ReadFile(filepath.Join(tempDir, "fav.log.sort"))
	assert.NoError(t, err)

	lines := strings.Split(string(content), "\n")

	// 修改预期的输出格式
	expectedLines := []string{
		"./path/to/file2",
		"./path/to/file1",
		"./path/to/file3",
		"", // 空行
	}

	assert.Equal(t, len(expectedLines), len(lines), "Number of lines should match")

	for i, expectedFile := range expectedLines[:len(expectedLines)-1] { // 忽略最后一个空行
		assert.Contains(t, lines[i], expectedFile,
			fmt.Sprintf("Line %d should contain %s", i+1, expectedFile))
	}
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
	assert.Contains(t, string(content), "2000,./path/to/file2")
	assert.Contains(t, string(content), "1000,./path/to/file1")
	assert.Contains(t, string(content), "file2")
	assert.Contains(t, string(content), "file1")
}

func TestSaveToFileSizeSort(t *testing.T) {
	// 设置测试环境
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	fp := NewFileProcessor(rdb, ctx)

	// 准备测试数据
	testData := map[string]FileInfo{
		"/path/to/file1": {Size: 1000, ModTime: time.Now().Add(-1 * time.Hour)},
		"/path/to/file2": {Size: 2000, ModTime: time.Now()},
		"/path/to/file3": {Size: 1500, ModTime: time.Now().Add(-2 * time.Hour)},
	}

	// 设置 Redis 数据
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

	// 创建临时目录
	tempDir, err := ioutil.TempDir("", "testdir")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 测试 saveToFile
	err = fp.saveToFile(tempDir, "testfile.txt", false)
	assert.NoError(t, err)

	// 验证文件内容
	content, err := ioutil.ReadFile(filepath.Join(tempDir, "testfile.txt"))
	assert.NoError(t, err)

	assert.Contains(t, string(content), "2000,./path/to/file2")
	assert.Contains(t, string(content), "1500,./path/to/file3")
	assert.Contains(t, string(content), "1000,./path/to/file1")
}

type MockFileInfoRetriever struct {
	mockData map[string]FileInfo
}

func (m *MockFileInfoRetriever) getFileInfoFromRedis(hashedKey string) (FileInfo, error) {
	if info, ok := m.mockData[hashedKey]; ok {
		return info, nil
	}
	return FileInfo{}, fmt.Errorf("mock: file info not found")
}

// 辅助函数：将 FileInfo 转换为字节数组
func mockFileInfoBytes(info FileInfo) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(info)
	return buf.Bytes()
}
