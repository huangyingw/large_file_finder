#!/bin/bash

# 定义要查找的字符串
search_string="/media/av162/av/旬果/test/test.mp4"

# 使用 Redis SCAN 命令查找所有包含特定字符串的键和值
redis-cli --scan --pattern "*" | while read -r key; do
# 获取键的类型
key_type=$(redis-cli TYPE "$key" | tr -d '\000')

# 检查键是否包含指定的字符串
if [[ "$key" == *"$search_string"* ]]; then
    echo "Key: $key, Value: (key contains search string)"
    continue
fi

# 根据键的类型分别处理
case "$key_type" in
    string)
        value=$(redis-cli GET "$key" | tr -d '\000')
        if [[ "$value" == *"$search_string"* ]]; then
            echo "Key: $key, Value: $value"
        fi
        ;;
    set)
        members=$(redis-cli SMEMBERS "$key" | tr -d '\000')
        for member in $members; do
            if [[ "$member" == *"$search_string"* ]]; then
                echo "Key: $key, Set Member: $member"
            fi
        done
        ;;
    zset)
        members=$(redis-cli ZRANGE "$key" 0 -1 | tr -d '\000')
        for member in $members; do
            if [[ "$member" == *"$search_string"* ]]; then
                echo "Key: $key, ZSet Member: $member"
            fi
        done
        ;;
    *)
        echo "Key: $key is of unsupported type: $key_type"
        ;;
esac
done
