#!/bin/bash
echo "Waiting for HDFS NameNode to exit safe mode..."
until hdfs dfsadmin -safemode get | grep -q "Safe mode is OFF"
do
  echo "HDFS is still in safe mode. Waiting..."
  sleep 5
done
echo "HDFS has exited safe mode. Ready to proceed."
echo "Creating initial HDFS directories..."
hdfs dfs -mkdir -p /data/air_quality
hdfs dfs -mkdir -p /clean-data/air_quality
echo "Setting permissions for data directories..."
hdfs dfs -chmod -R 777 /data
hdfs dfs -chmod -R 777 /clean-data
echo "HDFS is ready and initialized."
