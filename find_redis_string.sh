#!/bin/bash

# 定义要查找的字符串
search_string="/media/av162/av/旬果/test/test.mp4"

# 使用 Redis SCAN 命令查找所有包含特定字符串的键和值
redis-cli --scan --pattern "*" | while read -r key; do
value=$(redis-cli GET "$key" | tr -d '\000')
if [[ "$value" == *"$search_string"* ]]; then
    echo "Key: $key, Value: $value"
fi
done
