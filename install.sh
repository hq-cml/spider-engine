#!/bin/sh
go build ./
spider_path=$SPIDER_PATH

#校验
if [ -d $spider_path ]; then
    echo "The path $spider_path already exist!"
    exit
fi

#目录创建
mkdir $spider_path 
mkdir $spider_path/log 
mkdir $spider_path/data
mkdir $spider_path/conf

#文本替换
sed "s#TODO_REPLACE#$spider_path#" ./conf/spider.conf > $spider_path/conf/spider.conf

#执行文件copy
cp ./spider-engine $spider_path
