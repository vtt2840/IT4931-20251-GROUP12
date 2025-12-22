#!/bin/bash
# Khởi động HDFS
start-dfs.sh

# Chờ DataNode & NameNode sẵn sàng
sleep 10

# Tạo folder air_quality & set quyền
hdfs dfs -mkdir -p /data/air_quality
hdfs dfs -chmod 777 /data/air_quality
echo "HDFS /data/air_quality ready."

# Giữ container chạy
tail -f /dev/null
