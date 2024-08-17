#!/bin/zsh
SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd "$SCRIPTPATH"

go test ./... || exit 1
go test ./... -cover

#go mod init github.com/huangyingw/FileSorter
#go mod download github.com/stretchr/testify
#go get -u github.com/allegro/bigcache
#go get -u github.com/go-redis/redis/v8
#go get -u github.com/karrick/godirwalk
#go get github.com/spf13/afero
#go get -u github.com/mattn/go-zglob/fastwalk
#go get github.com/alicebob/miniredis/v2
#go get github.com/stretchr/testify/assert@v1.9.0
#go get github.com/go-redis/redismock/v8
#go get github.com/golang/mock
#go get github.com/golang/mock/gomock
#go get github.com/stretchr/testify
#
#docker-compose up -d
#
## 定义路径变量，确保处理包含空格和特殊字符的情况
#rootDir="/media/"
#
#go run . --rootDir="$rootDir"
##go run . --rootDir="$rootDir" --find-duplicates --max-duplicates=10000000000000
##go run . --rootDir="$rootDir" --output-duplicates
##go run . --rootDir="$rootDir" --delete-duplicates
#pm-suspend
