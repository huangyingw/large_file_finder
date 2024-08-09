#!/bin/zsh
SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd "$SCRIPTPATH"


go mod init github.com/huangyingw/FileSorter
go mod download github.com/stretchr/testify
go get github.com/go-redis/redismock/v8
go get github.com/golang/mock/gomock
go get -u github.com/allegro/bigcache
go get -u github.com/go-redis/redis/v8
go get -u github.com/mattn/go-zglob/fastwalk
go get -u github.com/karrick/godirwalk

go test ./... || exit 1

docker-compose up -d

# 定义路径变量，确保处理包含空格和特殊字符的情况
rootDir="/media/"

go run . --rootDir="$rootDir"
#go run . --rootDir="$rootDir" --find-duplicates --max-duplicates=10000000000000
#go run . --rootDir="$rootDir" --output-duplicates
#go run . --rootDir="$rootDir" --delete-duplicates
pm-suspend
