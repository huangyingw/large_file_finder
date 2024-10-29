package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLevenshteinDistance(t *testing.T) {
	testCases := []struct {
		name     string
		str1     string
		str2     string
		expected int
	}{
		{
			name:     "完全相同",
			str1:     "hello",
			str2:     "hello",
			expected: 0,
		},
		{
			name:     "一个字符不同",
			str1:     "hello",
			str2:     "hallo",
			expected: 1,
		},
		{
			name:     "长度不同",
			str1:     "hello",
			str2:     "hell",
			expected: 1,
		},
		{
			name:     "完全不同",
			str1:     "hello",
			str2:     "world",
			expected: 4,
		},
		{
			name:     "空字符串",
			str1:     "",
			str2:     "hello",
			expected: 5,
		},
		{
			name:     "中文字符",
			str1:     "你好",
			str2:     "你们好",
			expected: 1,
		},
		{
			name:     "中文单字差异",
			str1:     "你好",
			str2:     "你们",
			expected: 1,
		},
		{
			name:     "混合字符",
			str1:     "hello你好",
			str2:     "hello再见",
			expected: 2,
		},
		{
			name:     "日文字符",
			str1:     "こんにちは",
			str2:     "さようなら",
			expected: 5,
		},
		{
			name:     "韩文字符",
			str1:     "안녕하세요",
			str2:     "안녕히가세요",
			expected: 2,
		},
		{
			name:     "俄文字符",
			str1:     "привет",
			str2:     "пока",
			expected: 5,
		},
		{
			name:     "表情符号",
			str1:     "hello😊",
			str2:     "hello😄",
			expected: 1,
		},
		{
			name:     "混合多语言",
			str1:     "hello你好こんにちは",
			str2:     "hello再见さようなら",
			expected: 7,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := levenshteinDistance(tc.str1, tc.str2)
			assert.Equal(t, tc.expected, result, "对于输入 '%s' 和 '%s'", tc.str1, tc.str2)
		})
	}
}
