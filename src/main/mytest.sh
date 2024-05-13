#!/bin/bash


go build -buildmode=plugin ../mrapps/wc.go

rm mr-out*

rm mr-tmp-*
# 启动协调器
go run mrcoordinator.go pg-*.txt &

# 等待一段时间以确保协调器已经启动
sleep 2

# 启动 5 个工作器
for i in {1..1}
do
   go run mrworker.go wc.so &
done

# 等待所有后台任务完成
wait
