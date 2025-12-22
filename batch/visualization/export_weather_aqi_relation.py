import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, make_timestamp, lit, sha2, concat_ws

HDFS_ROOT = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:9000")
HDFS_PATH = f"{HDFS_ROOT}/user/hadoop/batch/hourly"

raw_es_nodes = os.getenv("ES_NODES")
ES_API_KEY = os.getenv("ES_API_KEY")
ES_INDEX_CORRELATION = os.getenv("ES_INDEX_CORRELATION")

ES_NODES = raw_es_nodes.replace("https://", "").replace("http://", "")

if not ES_NODES or not ES_API_KEY:
    raise RuntimeError("Chưa thiết lập biến môi trường ES_NODES hoặc ES_API_KEY")

spark = (
    SparkSession.builder
    .appName("Export_Hourly_Correlation_To_ES")
    .config(
        "spark.jars.packages",
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def main():
    print(f"--- STARTING CORRELATION EXPORT ---")
    print(f"Source: {HDFS_PATH}")
    print(f"Target Index: {ES_INDEX_CORRELATION}")

    # 1. Đọc dữ liệu từ HDFS
    try:
        df_hourly = spark.read.parquet(HDFS_PATH)
    except Exception as e:
        print(f"ERROR: Không tìm thấy dữ liệu tại {HDFS_PATH}. Lỗi: {e}")
        sys.exit(1)

    # 2. Xử lý dữ liệu & Tạo ID
    df_transformed = df_hourly.withColumn(
        "timestamp",
        make_timestamp(col("year"), col("month"), col("day"), col("hour"), lit(0), lit(0))
    )

    df_transformed = df_transformed.withColumn(
        "doc_id",
        sha2(concat_ws("_", col("city"), col("timestamp").cast("string"), lit("corr")), 256)
    )

    target_columns = [
        "doc_id",       
        "city",
        "timestamp",
        "pm25_mean",
        "avg_temp",
        "avg_hum",
        "avg_wind"
    ]
    
    available_cols = [c for c in target_columns if c in df_transformed.columns]
    df_final = df_transformed.select(*available_cols)

    # 4. Ghi vào ElasticSearch
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
            .option("es.resource", ES_INDEX_CORRELATION)
            .option("es.write.operation", "upsert")
            .option("es.mapping.id", "doc_id")
            .save()
        )
        print(f"--- SUCCESS: Đã export {df_final.count()} bản ghi vào '{ES_INDEX_CORRELATION}' ---")

    except Exception as e:
        print(f"--- FAILED: Lỗi khi ghi vào ES. Chi tiết: {str(e)}")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()