import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_timestamp

HDFS_ROOT = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:9000")

def get_spark():
    return SparkSession.builder \
        .appName("ExportHourlyPM25ToES") \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.es.net.ssl", "true") \
        .config("spark.es.cloud.id", os.getenv("ES_CLOUD_ID")) \
        .config("spark.es.api.key", os.getenv("ES_API_KEY")) \
        .getOrCreate()

spark = get_spark()

df = spark.read.parquet(
    f"{HDFS_ROOT}/user/hadoop/batch/hourly"
)

df_es = df.withColumn(
    "timestamp",
    to_timestamp(concat_ws(
        " ",
        concat_ws("-", col("year"), col("month"), col("day")),
        concat_ws(":", col("hour"), col("local_hour"), "00")
    ))
).select(
    "city",
    "timestamp",
    col("pm25_mean").alias("pm25"),
    "aqi_mean",
    "avg_temp",
    "avg_hum",
    "avg_wind"
)

df_es.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("append") \
    .option("es.resource", os.getenv("ES_INDEX_HOURLY_PM25")) \
    .save()

spark.stop()
