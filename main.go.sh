#!/bin/zsh
SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd "$SCRIPTPATH"


go mod init github.com/huangyingw/FileSorter
go get -u github.com/allegro/bigcache
go get -u github.com/go-redis/redis/v8
go get -u github.com/mattn/go-zglob/fastwalk
go get -u github.com/karrick/godirwalk

#docker-compose down -v
#docker-compose restart
docker-compose up -d

rm /media/av162/cartoon/dragonball/test/*.txt
go run . /media/av162/cartoon/dragonball/test/
ls -al /media/av162/cartoon/dragonball/test/*.txt
#go run . /media
