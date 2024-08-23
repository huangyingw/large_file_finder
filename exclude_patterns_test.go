package main

import (
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLoadExcludePatterns(t *testing.T) {
	fs := afero.NewMemMapFs()
	testFile := "/test_exclude_patterns.txt"
	content := `.*\.git(/.*)?$
.*snapraid\.parity(/.*)?$
^./sda1/.*
^\.git$
^\.git/.*`
	err := afero.WriteFile(fs, testFile, []byte(content), 0644)
	require.NoError(t, err)

	regexps, err := loadExcludePatterns(testFile, fs)
	require.NoError(t, err)
	assert.Len(t, regexps, 5)

	// Test for non-existent file
	nonExistentFile := "/non_existent_file.txt"
	_, err = loadExcludePatterns(nonExistentFile, fs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error opening exclude patterns file")
}

func TestShouldExclude(t *testing.T) {
	patterns := []string{
		`.*\.git(/.*)?$`,
		`.*snapraid\.parity(/.*)?$`,
		`^./sda1/.*`,
		`^\.git$`,
		`^\.git/.*`,
	}

	regexps, err := compileExcludePatterns(patterns)
	require.NoError(t, err)

	testCases := []struct {
		path     string
		expected bool
	}{
		{"project/.git/config", true},
		{"data/snapraid.parity/file", true},
		{"./sda1/somefile", true},
		{"sda1/somefile", false},
		{"normal/file.txt", false},
		{"snapraid.parity", true}, // Changed this to true
		{".git/config", true},
		{".git", true},
		{"subdir/.git", true},
		{"subdir/.git/config", true},
		{"deep/nested/subdir/.git", true},
		{"deep/nested/subdir/.git/config", true},
		{"normal/.gitignore", false},
		{"normal/git/file.txt", false},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			result := shouldExclude(tc.path, regexps)
			assert.Equal(t, tc.expected, result)
		})
	}
}
