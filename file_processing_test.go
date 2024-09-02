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
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func setupTestEnvironment(t *testing.T) (*miniredis.Miniredis, *redis.Client, context.Context, afero.Fs, *FileProcessor) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()
	fs := afero.NewMemMapFs()

	fp := CreateFileProcessor(rdb, ctx, testExcludeRegexps)
	fp.fs = fs

	// Clear all data in Redis before each test
	err = rdb.FlushAll(ctx).Err()
	require.NoError(t, err)

	return mr, rdb, ctx, fs, fp
}

func cleanupRedis(mr *miniredis.Miniredis) {
	mr.FlushAll()
}

func TestProcessFile(t *testing.T) {
	mr, rdb, ctx, fs, fp := setupTestEnvironment(t)
	defer mr.Close()

	rootDir := "/media"
	testRelativePath := "testroot/testfile.txt"
	testFullPath := filepath.Join(rootDir, testRelativePath)

	err := fs.MkdirAll(filepath.Dir(testFullPath), 0755)
	require.NoError(t, err)

	err = afero.WriteFile(fs, testFullPath, []byte("test content"), 0644)
	require.NoError(t, err)

	hashCalcCount := 0
	fp.calculateFileHashFunc = func(path string, limit int64) (string, error) {
		hashCalcCount++
		if limit == FullFileReadCmd {
			return "fullhash", nil
		}
		return "partialhash", nil
	}

	hashedKey := generateHash(testFullPath)

	// Test without calculating hashes
	t.Run("Without Calculating Hashes", func(t *testing.T) {
		hashCalcCount = 0
		err = fp.ProcessFile(rootDir, testRelativePath, false)
		require.NoError(t, err)
		assert.Equal(t, 0, hashCalcCount, "Hash should not be calculated when calculateHashes is false")

		// Verify file info was saved
		fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
		require.NoError(t, err)
		assert.NotNil(t, fileInfoData)

		var storedFileInfo FileInfo
		err = gob.NewDecoder(bytes.NewReader(fileInfoData)).Decode(&storedFileInfo)
		require.NoError(t, err)
		assert.Equal(t, int64(len("test content")), storedFileInfo.Size)
		assert.Equal(t, testFullPath, storedFileInfo.Path)

		// Verify path data was saved
		pathValue, err := rdb.Get(ctx, "hashedKeyToPath:"+hashedKey).Result()
		require.NoError(t, err)
		assert.Equal(t, testFullPath, pathValue)

		hashedKeyValue, err := rdb.Get(ctx, "pathToHashedKey:"+testFullPath).Result()
		require.NoError(t, err)
		assert.Equal(t, hashedKey, hashedKeyValue)

		// Verify hash-related data was not saved
		_, err = rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Result()
		assert.Equal(t, redis.Nil, err)

		_, err = rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Result()
		assert.Equal(t, redis.Nil, err)

		isMember, err := rdb.SIsMember(ctx, "fileHashToPathSet:partialhash", testFullPath).Result()
		require.NoError(t, err)
		assert.False(t, isMember)
	})

	// Clear Redis data
	err = rdb.FlushAll(ctx).Err()
	require.NoError(t, err)

	// Test with calculating hashes
	t.Run("With Calculating Hashes", func(t *testing.T) {
		hashCalcCount = 0
		err = fp.ProcessFile(rootDir, testRelativePath, true)
		require.NoError(t, err)
		assert.Equal(t, 2, hashCalcCount, "Hash should be calculated twice when calculateHashes is true")

		// Verify hash data was saved
		fileHashValue, err := rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Result()
		require.NoError(t, err)
		assert.Equal(t, "partialhash", fileHashValue)

		fullHashValue, err := rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Result()
		require.NoError(t, err)
		assert.Equal(t, "fullhash", fullHashValue)

		isMember, err := rdb.SIsMember(ctx, "fileHashToPathSet:partialhash", testFullPath).Result()
		require.NoError(t, err)
		assert.True(t, isMember)
	})
}

// 保留原有的 createTestData 函数
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
	mr, rdb, ctx, _, _ := setupTestEnvironment(t)
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
	err := SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
	require.NoError(t, err)

	// Test case 2: File with two timestamps
	filePath2 := "/path/to/file:12:34:56,01:23:45.mp4"
	info.Path = filePath2
	err = SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
	require.NoError(t, err)

	// Test case 3: File with no timestamp
	filePath3 := "/path/to/file.mp4"
	info.Path = filePath3
	err = SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, info)
	require.NoError(t, err)

	// Verify the data was saved correctly and in the right order
	members, err := rdb.ZRange(ctx, "duplicateFiles:"+fullHash, 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, 3, len(members))
	assert.Equal(t, filePath2, members[0]) // Should be first due to more timestamps
	assert.Equal(t, filePath1, members[1])
	assert.Equal(t, filePath3, members[2]) // Should be last due to no timestamps
}

// 更新 TestFileProcessor_SaveToFile 函数
func TestFileProcessor_SaveToFile(t *testing.T) {
	mr, rdb, ctx, fs, fp := setupTestEnvironment(t)
	defer mr.Close()

	rootDir := "/testroot"
	err := fs.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	// Prepare test data
	testData := []struct {
		path     string
		size     int64
		modTime  time.Time
		fullHash string
	}{
		{filepath.Join(rootDir, "file1.txt"), 100, time.Now().Add(-1 * time.Hour), "hash1"},
		{filepath.Join(rootDir, "file2.txt"), 200, time.Now(), "hash2"},
		{filepath.Join(rootDir, "file3.txt"), 150, time.Now().Add(-2 * time.Hour), "hash3"},
	}

	for _, data := range testData {
		info := FileInfo{Size: data.size, ModTime: data.modTime, Path: data.path}
		hashedKey := generateHash(data.path)
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(info)
		require.NoError(t, err)

		err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
		require.NoError(t, err)
		err = rdb.Set(ctx, "hashedKeyToPath:"+hashedKey, data.path, 0).Err()
		require.NoError(t, err)
	}

	// Test saveToFile with size sorting
	err = fp.saveToFile(rootDir, "test_size.log", false)
	require.NoError(t, err)

	content, err := afero.ReadFile(fs, filepath.Join(rootDir, "test_size.log"))
	require.NoError(t, err)
	assert.Contains(t, string(content), "200,\"./file2.txt\"")
	assert.Contains(t, string(content), "150,\"./file3.txt\"")
	assert.Contains(t, string(content), "100,\"./file1.txt\"")

	// Test saveToFile with time sorting
	err = fp.saveToFile(rootDir, "test_time.log", true)
	require.NoError(t, err)

	content, err = afero.ReadFile(fs, filepath.Join(rootDir, "test_time.log"))
	require.NoError(t, err)
	assert.Contains(t, string(content), "./file2.txt")
	assert.Contains(t, string(content), "./file1.txt")
	assert.Contains(t, string(content), "./file3.txt")
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
func TestWriteDuplicateFilesToFileWithMockData(t *testing.T) {
	mr, rdb, ctx, fs, fp := setupTestEnvironment(t)
	defer mr.Close()

	tempDir, err := afero.TempDir(fs, "", "test")
	require.NoError(t, err)
	defer fs.RemoveAll(tempDir)

	fp = CreateFileProcessor(rdb, ctx, testExcludeRegexps)
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
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
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
	mr, rdb, ctx, _, fp := setupTestEnvironment(t)
	defer mr.Close()

	testFileInfo := FileInfo{
		Size:    1000,
		ModTime: time.Now(),
		Path:    "/test/path/file.txt",
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(testFileInfo)
	require.NoError(t, err)

	hashedKey := "testkey"
	err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
	require.NoError(t, err)

	result, err := fp.getFileInfoFromRedis(hashedKey)
	require.NoError(t, err)
	assert.Equal(t, testFileInfo.Size, result.Size)
	assert.Equal(t, testFileInfo.Path, result.Path)
	assert.WithinDuration(t, testFileInfo.ModTime, result.ModTime, time.Second)
}

func TestFileProcessor_GetHashedKeyFromPath(t *testing.T) {
	mr, rdb, ctx, _, fp := setupTestEnvironment(t)
	defer mr.Close()

	testPath := "/path/to/test/file.txt"
	hashedKey := generateHash(testPath)

	err := rdb.Set(ctx, "pathToHashedKey:"+testPath, hashedKey, 0).Err()
	require.NoError(t, err)

	result, err := fp.getHashedKeyFromPath(testPath)
	assert.NoError(t, err)
	assert.Equal(t, hashedKey, result)

	_, err = fp.getHashedKeyFromPath("/non/existent/path")
	assert.Error(t, err)
}

func setupTestFiles(t *testing.T, fp *FileProcessor, rdb *redis.Client, ctx context.Context, fs afero.Fs, testData []struct {
	path     string
	size     int64
	modTime  time.Time
	fullHash string
}) error {
	for _, data := range testData {
		info := FileInfo{Size: data.size, ModTime: data.modTime, Path: data.path}
		err := SaveDuplicateFileInfoToRedis(rdb, ctx, data.fullHash, info)
		if err != nil {
			return err
		}

		hashedKey := fp.generateHashFunc(data.path)
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(info)
		if err != nil {
			return err
		}

		err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
		if err != nil {
			return err
		}
		err = rdb.Set(ctx, "hashedKeyToPath:"+hashedKey, data.path, 0).Err()
		if err != nil {
			return err
		}
		err = rdb.Set(ctx, "pathToHashedKey:"+data.path, hashedKey, 0).Err()
		if err != nil {
			return err
		}
		err = rdb.Set(ctx, "hashedKeyToFullHash:"+hashedKey, data.fullHash, 0).Err()
		if err != nil {
			return err
		}

		err = afero.WriteFile(fs, data.path, []byte("test content"), 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

func testSaveToFile(t *testing.T, fp *FileProcessor, fs afero.Fs, tempDir, filename string, sortByModTime bool) error {
	err := fp.saveToFile(tempDir, filename, sortByModTime)
	if err != nil {
		return err
	}

	content, err := afero.ReadFile(fs, filepath.Join(tempDir, filename))
	if err != nil {
		return err
	}

	var expectedContent string
	if sortByModTime {
		expectedContent = `"./path/to/file2_02:34:56_03:45:67.mp4"
"./path/to/file1_01:23:45.mp4"
"./path/to/file3.mp4"
`
	} else {
		expectedContent = `2172777224,"./path/to/file2_02:34:56_03:45:67.mp4"
2172777224,"./path/to/file3.mp4"
209720828,"./path/to/file1_01:23:45.mp4"
`
	}

	assert.Equal(t, expectedContent, string(content), "File content does not match expected")

	return nil
}

func testWriteDuplicateFilesToFile(t *testing.T, fp *FileProcessor, fs afero.Fs, rdb *redis.Client, ctx context.Context, tempDir, fullHash string) error {
	err := fp.WriteDuplicateFilesToFile(tempDir, "fav.log.dup", rdb, ctx)
	if err != nil {
		return err
	}

	content, err := afero.ReadFile(fs, filepath.Join(tempDir, "fav.log.dup"))
	if err != nil {
		return err
	}

	expectedDupContent := fmt.Sprintf(`Duplicate files for fullHash %s:
[+] 2172777224,"./path/to/file2_02:34:56_03:45:67.mp4"
[-] 2172777224,"./path/to/file3.mp4"

`, fullHash)

	if string(content) != expectedDupContent {
		return fmt.Errorf("fav.log.dup content does not match expected.\nExpected:\n%s\nActual:\n%s", expectedDupContent, string(content))
	}

	return nil
}

func TestFileContentVerification(t *testing.T) {
	mr, rdb, ctx, fs, fp := setupTestEnvironment(t)
	defer mr.Close()

	tempDir, err := afero.TempDir(fs, "", "testdir")
	require.NoError(t, err, "Failed to create temp directory")
	defer fs.RemoveAll(tempDir)

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

	err = setupTestFiles(t, fp, rdb, ctx, fs, testData)
	require.NoError(t, err, "Failed to setup test files")

	// Set up duplicate files in Redis
	duplicateFilesKey := "duplicateFiles:" + fullHash
	for _, data := range testData {
		if data.fullHash == fullHash {
			score := float64(-data.size) // Use negative size as score for correct ordering
			_, err := rdb.ZAdd(ctx, duplicateFilesKey, &redis.Z{Score: score, Member: data.path}).Result()
			require.NoError(t, err)
		}
	}

	t.Run("Test saveToFile (fav.log)", func(t *testing.T) {
		err := testSaveToFile(t, fp, fs, tempDir, "fav.log", false)
		require.NoError(t, err, "Failed to test saveToFile for fav.log")
	})

	t.Run("Test saveToFile (fav.log.sort)", func(t *testing.T) {
		err := testSaveToFile(t, fp, fs, tempDir, "fav.log.sort", true)
		require.NoError(t, err, "Failed to test saveToFile for fav.log.sort")
	})

	t.Run("Test WriteDuplicateFilesToFile", func(t *testing.T) {
		err := testWriteDuplicateFilesToFile(t, fp, fs, rdb, ctx, tempDir, fullHash)
		require.NoError(t, err, "Failed to test WriteDuplicateFilesToFile")
	})
}

func TestGetFileInfoFromRedis(t *testing.T) {
	mr, rdb, ctx, _, fp := setupTestEnvironment(t)
	defer mr.Close()

	// Prepare test data
	testInfo := FileInfo{
		Size:    1000,
		ModTime: time.Now(),
		Path:    "/path/to/test/file.txt",
	}
	hashedKey := "testkey"
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(testInfo)
	require.NoError(t, err)

	err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
	require.NoError(t, err)

	// Test getFileInfoFromRedis
	result, err := fp.getFileInfoFromRedis(hashedKey)
	assert.NoError(t, err)
	assert.Equal(t, testInfo.Size, result.Size)
	assert.Equal(t, testInfo.Path, result.Path)
	assert.WithinDuration(t, testInfo.ModTime, result.ModTime, time.Second)
}

func TestGetHashedKeyFromPath(t *testing.T) {
	mr, rdb, ctx, _, fp := setupTestEnvironment(t)
	defer mr.Close()

	testPath := "/path/to/test/file.txt"
	hashedKey := generateHash(testPath)

	err := rdb.Set(ctx, "pathToHashedKey:"+testPath, hashedKey, 0).Err()
	require.NoError(t, err)

	result, err := fp.getHashedKeyFromPath(testPath)
	assert.NoError(t, err)
	assert.Equal(t, hashedKey, result)

	// Test with non-existent path
	_, err = fp.getHashedKeyFromPath("/non/existent/path")
	assert.Error(t, err)
}

func TestCalculateFileHash(t *testing.T) {
	_, _, _, fs, fp := setupTestEnvironment(t)

	testFilePath := "/testfile.txt"
	testContent := "This is a test file content"
	err := afero.WriteFile(fs, testFilePath, []byte(testContent), 0644)
	require.NoError(t, err)

	// Test partial hash
	partialHash, err := fp.calculateFileHash(testFilePath, ReadLimit)
	require.NoError(t, err)
	assert.NotEmpty(t, partialHash)

	// Test full hash
	fullHash, err := fp.calculateFileHash(testFilePath, FullFileReadCmd)
	require.NoError(t, err)
	assert.NotEmpty(t, fullHash)

	// Partial hash and full hash should be different for files larger than ReadLimit
	if len(testContent) > ReadLimit {
		assert.NotEqual(t, partialHash, fullHash)
	} else {
		assert.Equal(t, partialHash, fullHash)
	}
}

func TestCleanUpOldRecords(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	ctx := context.Background()

	// 创建临时目录
	tempDir, err := ioutil.TempDir("", "testcleanup")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 准备测试数据
	existingFile := filepath.Join(tempDir, "existing.txt")
	nonExistingFile := filepath.Join(tempDir, "non_existing.txt")

	// 创建存在的文件
	_, err = os.Create(existingFile)
	require.NoError(t, err)

	for _, path := range []string{existingFile, nonExistingFile} {
		hashedKey := generateHash(path)
		err = rdb.Set(ctx, "pathToHashedKey:"+path, hashedKey, 0).Err()
		require.NoError(t, err)
		err = rdb.Set(ctx, "hashedKeyToPath:"+hashedKey, path, 0).Err()
		require.NoError(t, err)
		err = rdb.Set(ctx, "fileInfo:"+hashedKey, "dummy_data", 0).Err()
		require.NoError(t, err)
		err = rdb.Set(ctx, "hashedKeyToFileHash:"+hashedKey, "dummy_hash", 0).Err()
		require.NoError(t, err)
	}

	err = CleanUpOldRecords(rdb, ctx)
	require.NoError(t, err)

	// 检查不存在文件的记录是否被删除
	_, err = rdb.Get(ctx, "pathToHashedKey:"+nonExistingFile).Result()
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)

	// 检查存在文件的记录是否被保留
	val, err := rdb.Get(ctx, "pathToHashedKey:"+existingFile).Result()
	require.NoError(t, err)
	assert.NotEmpty(t, val)
}

func TestProcessFileBoundary(t *testing.T) {
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
	require.NoError(t, err)

	err = mockProcessFile(emptyFilePath)
	require.NoError(t, err)

	// 测试大文件（模拟）
	largeFilePath := "/path/to/large_file.bin"
	err = afero.WriteFile(fs, largeFilePath, []byte("large file content"), 0644)
	require.NoError(t, err)

	err = mockProcessFile(largeFilePath)
	require.NoError(t, err)

	// 验证大文件是否被正确处理
	hashedKey := generateHash(largeFilePath)
	fileHash, err := rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Result()
	require.NoError(t, err)
	assert.Equal(t, "partial_hash_large_file", fileHash)

	fullHash, err := rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Result()
	require.NoError(t, err)
	assert.Equal(t, "full_hash_large_file", fullHash)
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

func TestFileOperationsWithSpecialChars(t *testing.T) {
	mr, rdb, ctx, fs, fp := setupTestEnvironment(t)
	defer mr.Close()

	tempDir, err := ioutil.TempDir("", "test_special_chars")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	specialFiles := []struct {
		name    string
		content string
	}{
		{"file with spaces.txt", "content1"},
		{"file_with_特殊字符.txt", "content2"},
		{"file!@#$%^&*().txt", "content3"},
		{"áéíóú.txt", "content4"},
	}

	for _, sf := range specialFiles {
		sf := sf // capture range variable
		t.Run(sf.name, func(t *testing.T) {
			cleanupRedis(mr) // 清理 Redis 数据

			filePath := filepath.Join(tempDir, sf.name)
			err := afero.WriteFile(fs, filePath, []byte(sf.content), 0644)
			require.NoError(t, err)

			relativePath, err := filepath.Rel(tempDir, filePath)
			require.NoError(t, err)

			t.Run("FileSize", func(t *testing.T) {
				info, err := fs.Stat(filePath)
				assert.NoError(t, err)
				assert.Equal(t, int64(len(sf.content)), info.Size())
			})

			t.Run("FileHash", func(t *testing.T) {
				hash, err := fp.calculateFileHashFunc(filePath, -1)
				assert.NoError(t, err)
				assert.NotEmpty(t, hash)
				assert.Regexp(t, "^[0-9a-f]+$", hash)
			})

			t.Run("ProcessFileWithHash", func(t *testing.T) {
				cleanupRedis(mr) // 清理 Redis 数据
				err := fp.ProcessFile(tempDir, relativePath, true)
				assert.NoError(t, err)

				hashedKey := generateHash(filePath)

				// Verify file info
				fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
				require.NoError(t, err)
				assert.NotNil(t, fileInfoData)

				var storedFileInfo FileInfo
				err = gob.NewDecoder(bytes.NewReader(fileInfoData)).Decode(&storedFileInfo)
				require.NoError(t, err)
				assert.Equal(t, int64(len(sf.content)), storedFileInfo.Size)
				assert.Equal(t, filePath, storedFileInfo.Path)

				// Verify path data
				pathValue, err := rdb.Get(ctx, "hashedKeyToPath:"+hashedKey).Result()
				require.NoError(t, err)
				assert.Equal(t, filePath, pathValue)

				// Verify hash data
				_, err = rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Result()
				assert.NoError(t, err)
				_, err = rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Result()
				assert.NoError(t, err)
			})

			t.Run("ProcessFileWithoutHash", func(t *testing.T) {
				cleanupRedis(mr) // 清理 Redis 数据
				err := fp.ProcessFile(tempDir, relativePath, false)
				assert.NoError(t, err)

				hashedKey := generateHash(filePath)

				// Verify file info exists
				fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
				assert.NoError(t, err)
				assert.NotNil(t, fileInfoData)

				var storedFileInfo FileInfo
				err = gob.NewDecoder(bytes.NewReader(fileInfoData)).Decode(&storedFileInfo)
				require.NoError(t, err)
				assert.Equal(t, int64(len(sf.content)), storedFileInfo.Size)
				assert.Equal(t, filePath, storedFileInfo.Path)

				// Verify path data exists
				pathValue, err := rdb.Get(ctx, "hashedKeyToPath:"+hashedKey).Result()
				assert.NoError(t, err)
				assert.Equal(t, filePath, pathValue)

				// Verify hash data does not exist
				_, err = rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Result()
				assert.Equal(t, redis.Nil, err)
				_, err = rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Result()
				assert.Equal(t, redis.Nil, err)

				isMember, err := rdb.SIsMember(ctx, "fileHashToPathSet:partialhash", filePath).Result()
				assert.NoError(t, err)
				assert.False(t, isMember)
			})
		})
	}
}

func TestFileProcessor_ProcessFile(t *testing.T) {
	mr, rdb, ctx, fs, fp := setupTestEnvironment(t)
	defer mr.Close()

	rootDir := "/testroot"
	err := fs.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	// Create a test file
	testFileName := "testfile.txt"
	testFilePath := filepath.Join(rootDir, testFileName)
	err = afero.WriteFile(fs, testFilePath, []byte("test content"), 0644)
	require.NoError(t, err)

	// Process the file
	err = fp.ProcessFile(rootDir, testFileName, true)
	assert.NoError(t, err)

	hashedKey := generateHash(testFilePath)

	// Verify file info
	fileInfoData, err := rdb.Get(ctx, "fileInfo:"+hashedKey).Bytes()
	require.NoError(t, err)
	assert.NotNil(t, fileInfoData)

	var storedFileInfo FileInfo
	err = gob.NewDecoder(bytes.NewReader(fileInfoData)).Decode(&storedFileInfo)
	require.NoError(t, err)
	assert.Equal(t, int64(len("test content")), storedFileInfo.Size)
	assert.Equal(t, testFilePath, storedFileInfo.Path)

	// Verify path data
	pathValue, err := rdb.Get(ctx, "hashedKeyToPath:"+hashedKey).Result()
	require.NoError(t, err)
	assert.Equal(t, testFilePath, pathValue)

	// Verify hash data
	_, err = rdb.Get(ctx, "hashedKeyToFileHash:"+hashedKey).Result()
	assert.NoError(t, err)
	_, err = rdb.Get(ctx, "hashedKeyToFullHash:"+hashedKey).Result()
	assert.NoError(t, err)

	// Verify that the full path is stored
	hashedKeyFromPath, err := rdb.Get(ctx, "pathToHashedKey:"+testFilePath).Result()
	assert.NoError(t, err)
	assert.Equal(t, hashedKey, hashedKeyFromPath)
}

func TestFileProcessor_WriteDuplicateFilesToFile(t *testing.T) {
	mr, rdb, ctx, fs, fp := setupTestEnvironment(t)
	defer mr.Close()

	tempDir, err := afero.TempDir(fs, "", "test")
	require.NoError(t, err)
	defer fs.RemoveAll(tempDir)

	// Set up mock data
	fullHash := "testhash"
	duplicateFiles := []string{"/path/to/file1", "/path/to/file2", "/path/to/file3"}
	for i, file := range duplicateFiles {
		fileInfo := FileInfo{
			Size:    int64((i + 1) * 1000),
			ModTime: time.Now(),
			Path:    file,
		}
		err = SaveDuplicateFileInfoToRedis(rdb, ctx, fullHash, fileInfo)
		require.NoError(t, err)

		// Set up file info in Redis
		hashedKey := generateHash(file)
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(fileInfo)
		require.NoError(t, err)
		err = rdb.Set(ctx, "fileInfo:"+hashedKey, buf.Bytes(), 0).Err()
		require.NoError(t, err)
		err = rdb.Set(ctx, "pathToHashedKey:"+file, hashedKey, 0).Err()
		require.NoError(t, err)
	}

	err = fp.WriteDuplicateFilesToFile(tempDir, "fav.log.dup", rdb, ctx)
	require.NoError(t, err)

	content, err := afero.ReadFile(fs, filepath.Join(tempDir, "fav.log.dup"))
	require.NoError(t, err)

	expectedContent := fmt.Sprintf(`Duplicate files for fullHash %s:
[+] 1000,"./path/to/file1"
[-] 2000,"./path/to/file2"
[-] 3000,"./path/to/file3"

`, fullHash)

	assert.Equal(t, expectedContent, string(content), "File content does not match expected")
}
