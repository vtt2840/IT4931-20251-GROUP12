import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, make_timestamp, lit, sha2, concat_ws

# Cấu hình HDFS
HDFS_ROOT = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:9000")
HDFS_PATH = f"{HDFS_ROOT}/user/hadoop/batch/hourly"

raw_es_nodes = os.getenv("ES_NODES")
ES_API_KEY = os.getenv("ES_API_KEY")
ES_INDEX = os.getenv("ES_INDEX_HOURLY")

ES_NODES = raw_es_nodes.replace("https://", "").replace("http://", "")

if not ES_NODES or not ES_API_KEY:
    raise RuntimeError("ES_NODES or ES_API_KEY is not set properly")

spark = (
    SparkSession.builder
    .appName("Hanoi_AirQuality_Export_Batch_Hourly")
    .config(
        "spark.jars.packages",
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def main():
    print(f"--- STARTING BATCH EXPORT ---")
    print(f"Source: {HDFS_PATH}")
    print(f"Dest:   ES Cloud [{ES_INDEX}]")

    # 1. Đọc dữ liệu từ HDFS
    try:
        df = spark.read.parquet(HDFS_PATH)
    except Exception as e:
        print(f"ERROR: Không thể đọc dữ liệu từ {HDFS_PATH}. Chi tiết: {e}")
        sys.exit(1)

    # 2. Chuẩn hóa dữ liệu & Tạo ID
    df_es = df.withColumn(
        "@timestamp", 
        make_timestamp(col("year"), col("month"), col("day"), col("hour"), lit(0), lit(0))
    ).withColumn(
        "doc_id",
        sha2(concat_ws("_", col("city"), col("@timestamp").cast("string"), lit("hourly")), 256)
    )

    # 3. Ghi vào Elastic Cloud
    auth_header = f"ApiKey {ES_API_KEY}"

    try:
        (
            df_es.write
            .format("org.elasticsearch.spark.sql")
            .mode("append") 
            .option("es.nodes", ES_NODES)
            .option("es.port", "443")
            .option("es.nodes.wan.only", "true")
            .option("es.net.ssl", "true")
            .option("es.nodes.discovery", "false")
            .option("es.net.http.header.Authorization", auth_header)
            .option("es.resource", ES_INDEX)
            .option("es.write.operation", "upsert")
            .option("es.mapping.id", "doc_id")
            .save()
        )
        print(f"--- SUCCESS: Đã ghi dữ liệu vào index '{ES_INDEX}' thành công! ---")
    
    except Exception as e:
        print(f"--- FAILED: Lỗi khi ghi vào ES. Chi tiết: {str(e)}")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()