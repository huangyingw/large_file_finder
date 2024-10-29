package main

func levenshteinDistance(str1, str2 string) int {
	// 将字符串转换为 rune 切片，这样可以正确处理 Unicode 字符
	s1 := []rune(str1)
	s2 := []rune(str2)

	// 获取字符串长度（以字符为单位，而不是字节）
	len1 := len(s1)
	len2 := len(s2)

	// 创建矩阵
	matrix := make([][]int, len1+1)
	for i := range matrix {
		matrix[i] = make([]int, len2+1)
	}

	// 初始化第一行和第一列
	for i := 0; i <= len1; i++ {
		matrix[i][0] = i
	}
	for j := 0; j <= len2; j++ {
		matrix[0][j] = j
	}

	// 填充矩阵
	for i := 1; i <= len1; i++ {
		for j := 1; j <= len2; j++ {
			cost := 1
			if s1[i-1] == s2[j-1] {
				cost = 0
			}
			matrix[i][j] = min(
				matrix[i-1][j]+1,      // 删除
				matrix[i][j-1]+1,      // 插入
				matrix[i-1][j-1]+cost, // 替换
			)
		}
	}

	return matrix[len1][len2]
}

func min(nums ...int) int {
	if len(nums) == 0 {
		return 0
	}
	res := nums[0]
	for _, num := range nums[1:] {
		if num < res {
			res = num
		}
	}
	return res
}
