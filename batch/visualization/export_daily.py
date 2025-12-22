import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, make_timestamp, sha2, concat_ws

# 1. Cấu hình HDFS
HDFS_ROOT = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:9000")
HDFS_PATH = f"{HDFS_ROOT}/user/hadoop/batch/hourly"

# 2. Cấu hình ElasticSearch (Ưu tiên lấy từ Env)
raw_es_nodes = os.getenv("ES_NODES")
ES_API_KEY = os.getenv("ES_API_KEY")
ES_INDEX_PM25 = os.getenv("ES_INDEX_DAILY_PM25")

ES_NODES = raw_es_nodes.replace("https://", "").replace("http://", "")

if not ES_NODES or not ES_API_KEY:
    raise RuntimeError("Chưa thiết lập biến môi trường ES_NODES hoặc ES_API_KEY")

spark = (
    SparkSession.builder
    .appName("Export_Hourly_PM25_Stats")
    .config(
        "spark.jars.packages",
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def main():
    print(f"--- STARTING PM25 EXPORT ---")
    print(f"Source: {HDFS_PATH}")
    print(f"Target Index: {ES_INDEX_PM25}")

    # 1. Đọc dữ liệu từ HDFS
    try:
        df = spark.read.parquet(HDFS_PATH)
    except Exception as e:
        print(f"ERROR: Không thể đọc dữ liệu từ {HDFS_PATH}. Lỗi: {e}")
        sys.exit(1)

    # 2. Transform dữ liệu
    df_transformed = df.withColumn(
        "timestamp",
        make_timestamp(col("year"), col("month"), col("day"), col("hour"), lit(0), lit(0))
    )

    df_transformed = df_transformed.withColumn(
        "doc_id",
        sha2(concat_ws("_", col("city"), col("timestamp").cast("string"), lit("pm25")), 256)
    )

    df_final = df_transformed.select(
        "doc_id", 
        "city",
        "timestamp",
        col("pm25_mean").alias("pm25"),
        "aqi_mean",
        "avg_temp",
        "avg_hum",
        "avg_wind"
    )

    auth_header = f"ApiKey {ES_API_KEY}"

    try:
        (
            df_final.write
            .format("org.elasticsearch.spark.sql")
            .mode("append")
            .option("es.nodes", ES_NODES)
            .option("es.port", "443")
            .option("es.nodes.wan.only", "true")
            .option("es.net.ssl", "true")
            .option("es.nodes.discovery", "false")
            .option("es.net.http.header.Authorization", auth_header)
            .option("es.resource", ES_INDEX_PM25)
            .option("es.write.operation", "upsert")
            .option("es.mapping.id", "doc_id")
            .save()
        )
        print(f"--- SUCCESS: Đã ghi dữ liệu vào '{ES_INDEX_PM25}' thành công! ---")

    except Exception as e:
        print(f"--- FAILED: Lỗi khi ghi vào ES. Chi tiết: {str(e)}")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()