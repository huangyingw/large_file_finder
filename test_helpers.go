// test_helpers.go

package main

import "regexp"

var testExcludeRegexps []*regexp.Regexp

func init() {
	// 初始化测试用的 exclude patterns
	patterns := []string{
		`.*\.git(/.*)?$`,
		`.*snapraid\.parity(/.*)?$`,
		`^./sda1/.*`,
	}
	var err error
	testExcludeRegexps, err = compileExcludePatterns(patterns)
	if err != nil {
		panic("Failed to compile test exclude patterns: " + err.Error())
	}
}
