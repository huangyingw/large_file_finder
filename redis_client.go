package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/go-redis/redis/v8"
)

// Generate a SHA-256 hash for the given string
func generateHash(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return hex.EncodeToString(hasher.Sum(nil))
}

func cleanUpOldRecords(rdb *redis.Client, ctx context.Context, startTime int64) error {
	iter := rdb.Scan(ctx, 0, "updateTime:*", 0).Iterator()
	for iter.Next(ctx) {
		updateTimeKey := iter.Val()
		updateTime, err := rdb.Get(ctx, updateTimeKey).Int64()
		if err != nil {
			// 处理错误
			continue
		}
		if updateTime < startTime {
			_, err := rdb.Del(ctx, updateTimeKey).Result()
			if err != nil {
				// 处理错误
			}
			// 可能还需要删除与这个updateTimeKey相关联的其他数据
		}
	}
	return nil
}
