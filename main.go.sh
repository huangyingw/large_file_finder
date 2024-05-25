#!/bin/zsh
SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd "$SCRIPTPATH"


go mod init github.com/huangyingw/FileSorter
go get -u github.com/allegro/bigcache
go get -u github.com/go-redis/redis/v8
go get -u github.com/mattn/go-zglob/fastwalk
go get -u github.com/karrick/godirwalk

cp -v /media/secure_bcache/test/test.mp4.bak.bak /media/secure_bcache/test/test.mp4.bak
cp -v /media/secure_bcache/test/test.mp4.bak.bak /media/secure_bcache/test/test.mp4
docker-compose down -v
docker-compose restart
docker-compose up -d

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
rm /media/av162/cartoon/dragonball/test/*.txt
>>>>>>> 80d9d45 (n)
go run . /media/av162/cartoon/dragonball/test/
ls -al /media/av162/cartoon/dragonball/test/*.txt
#go run . /media
=======
#go run . /media/secure_bcache/av/onlyfans/
go run . /media
>>>>>>> a2c1f16 (n)
=======
#rm /media/av162/cartoon/dragonball/test/*.txt
#go run . /media/av162/cartoon/dragonball/test/
#ls -al /media/av162/cartoon/dragonball/test/*.txt
<<<<<<< HEAD
go run . /media
>>>>>>> 11de013 (n)
=======

# 定义路径变量，确保处理包含空格和特殊字符的情况
rootDir="/media/secure_bcache/test/"

# 正常运行
go run . "$rootDir"

# 输出重复文件结果
go run . "$rootDir" --find-duplicates

# 删除重复文件（示例，实际运行时取消注释）
<<<<<<< HEAD
# go run . "$rootDir" --delete-duplicates
>>>>>>> 1c14d61 (n)
=======
go run . "$rootDir" --delete-duplicates
>>>>>>> 2f382eb (n)
