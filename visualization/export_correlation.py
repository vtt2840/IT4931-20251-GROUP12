import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import make_timestamp

HDFS_ROOT = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:9000")

spark = SparkSession.builder \
    .appName("ExportHourlyRelationToES") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.es.net.ssl", "true") \
    .config("spark.es.cloud.id", os.getenv("ES_CLOUD_ID")) \
    .config("spark.es.api.key", os.getenv("ES_API_KEY")) \
    .getOrCreate()

# 🔹 Đọc dữ liệu hourly
df_hourly = spark.read.parquet(
    f"{HDFS_ROOT}/user/hadoop/batch/hourly"
)

# 🔹 Tạo timestamp theo giờ
df_hourly = df_hourly.withColumn(
    "timestamp",
    make_timestamp("year", "month", "day", "hour", 0, 0)
)

# 🔹 Chỉ giữ các cột phục vụ relationship
df_hourly = df_hourly.select(
    "city",
    "timestamp",
    "pm25_mean",
    "avg_temp",
    "avg_hum",
    "avg_wind"
)

# 🔹 Ghi sang Elasticsearch
df_hourly.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("append") \
    .option("es.resource", os.getenv("ES_INDEX_CORRELATION")) \
    .save()

spark.stop()
