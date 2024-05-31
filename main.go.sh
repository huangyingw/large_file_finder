#!/bin/zsh
SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd "$SCRIPTPATH"


go mod init github.com/huangyingw/FileSorter
go get -u github.com/allegro/bigcache
go get -u github.com/go-redis/redis/v8
go get -u github.com/mattn/go-zglob/fastwalk
go get -u github.com/karrick/godirwalk

docker-compose up -d

# 定义路径变量，确保处理包含空格和特殊字符的情况
rootDir="/media/"

# 正常运行
# go run . --rootDir="$rootDir"

# 输出重复文件结果
go run . --rootDir="$rootDir" --find-duplicates --max-duplicates=4
# go run . --rootDir="$rootDir" --output-duplicates

# 删除重复文件（示例，实际运行时取消注释）
# go run . --rootDir="$rootDir" --delete-duplicates
