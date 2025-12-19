from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, from_json, avg, max as spark_max,
    to_timestamp, when, window
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os

# ================= CONFIG =================
kafka_servers = os.getenv("KAFKA_BROKER", "kafka:9092")
kafka_topic = os.getenv("TOPIC", "air-quality")
spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

# ===== Elasticsearch Cloud Config =====
raw_es_nodes = os.getenv("ES_NODES")  # Ví dụ: https://my-deployment.es.us-central1.gcp.cloud.es.io
ES_INDEX = os.getenv("ES_INDEX", "aqi_data")
ES_API_KEY = os.getenv("ES_API_KEY") # Chuỗi Base64 dài (được tạo từ Kibana)

if not raw_es_nodes or not ES_API_KEY:
    raise RuntimeError("❌ ES_NODES or ES_API_KEY is not set")

# --- Xử lý URL ES_NODES (QUAN TRỌNG) ---
# Spark ES Connector cần Hostname thuần, không chứa 'https://' hay ':port'
ES_NODES = raw_es_nodes.replace("https://", "").replace("http://", "").split(":")[0]

print("===== CONFIG =====")
print("Kafka:", kafka_servers)
print("ES Host (Cleaned):", ES_NODES)
print("ES Index:", ES_INDEX)
print("==================")

CHECKPOINT_LOCATION = "/app/checkpoints/aqi_streaming"

# ================= SPARK SESSION =================
spark = (
    SparkSession.builder
    .appName("Hanoi_AirQuality_Streaming_10min")
    .master(spark_master)
    # Lưu ý: Phiên bản connector phải tương thích với Spark và Scala
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ================= KAFKA JSON SCHEMA =================
schema = StructType([
    StructField("city", StringType(), True),
    StructField("aqi", IntegerType(), True),
    StructField("co", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("timestamp_local", StringType(), True),
    StructField("timestamp_utc", StringType(), True)
])

# ================= STATIC DATA =================
static_df = spark.createDataFrame(
    [("HANOI", 21.0285, 105.8542, "Vietnam")],
    ["city", "lat", "lon", "country"]
)

# ================= 1. READ FROM KAFKA =================
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# ================= 2. PARSE JSON =================
df_parsed = (
    df_raw.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("d"))
    .select("d.*")
    .withColumn(
        "timestamp_utc",
        to_timestamp(col("timestamp_utc"), "yyyy-MM-dd'T'HH:mm:ss")
    )
)

# ================= 3. CLEAN & CATEGORY =================
df_clean = (
    df_parsed
    .withWatermark("timestamp_utc", "10 minutes")
    .dropDuplicates(["city", "timestamp_utc"])
    .na.drop(subset=["city", "aqi"])
    .withColumn("city", trim(upper(col("city"))))
    .filter((col("aqi") >= 0) & (col("aqi") <= 1000))
    .withColumn(
        "aqi_category",
        when(col("aqi") <= 50, "Good")
        .when(col("aqi") <= 100, "Moderate")
        .when(col("aqi") <= 150, "Unhealthy for Sensitive Groups")
        .when(col("aqi") <= 200, "Unhealthy")
        .when(col("aqi") <= 300, "Very Unhealthy")
        .otherwise("Hazardous")
    )
)

# ================= 4. ENRICH =================
df_enriched = (
    df_clean.alias("c")
    .join(static_df.alias("s"), col("c.city") == col("s.city"), "left")
    .select(col("c.*"), col("s.lat"), col("s.lon"), col("s.country"))
)

# ================= 5. 10-MIN AGGREGATION =================
df_10min = (
    df_enriched
    .groupBy(window("timestamp_utc", "10 minutes"), col("city"))
    .agg(
        avg("aqi").alias("avg_aqi"),
        avg("co").alias("avg_co"),
        avg("no2").alias("avg_no2"),
        avg("o3").alias("avg_o3"),
        avg("pm10").alias("avg_pm10"),
        avg("pm25").alias("avg_pm25"),
        avg("so2").alias("avg_so2"),
        spark_max("lat").alias("lat"),
        spark_max("lon").alias("lon"),
        spark_max("country").alias("country")
    )
    .selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "city",
        "avg_aqi",
        "avg_co",
        "avg_no2",
        "avg_o3",
        "avg_pm10",
        "avg_pm25",
        "avg_so2",
        "lat",
        "lon",
        "country"
    )
)

# ================= WRITER FUNCTION =================
def write_elasticsearch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return

    print(f"===== BATCH {batch_id} | ROWS = {count} =====")

    # Sử dụng API Key trong Header Authorization là cách an toàn nhất cho mọi version
    # ES_API_KEY phải là chuỗi Base64 đầy đủ
    auth_header_value = f"ApiKey {ES_API_KEY}"

    (
        batch_df.write
        .format("org.elasticsearch.spark.sql")
        # --- Network Config ---
        .option("es.nodes", ES_NODES)          
        .option("es.port", "443")              
        .option("es.nodes.wan.only", "true")   # Bắt buộc cho Cloud
        .option("es.nodes.discovery", "false") # Tắt discovery trên Cloud để tránh lỗi private IP
        .option("es.net.ssl", "true")          
        
        # --- Authentication (Quan trọng) ---
        # Thay vì dùng es.net.http.auth.api.key, ta bơm thẳng vào Header
        .option("es.net.http.header.Authorization", auth_header_value)
        
        # --- Headers cho ES 8.x ---
        .option("es.net.http.header.Accept", "application/vnd.elasticsearch+json;compatible-with=8")
        .option("es.net.http.header.Content-Type", "application/vnd.elasticsearch+json;compatible-with=8")
        
        # --- Data & Settings ---
        .option("es.resource", ES_INDEX)
        .option("es.input.json", "false")      # DataFrame tự convert sang JSON
        .option("es.mapping.date.rich", "false") # Tránh lỗi format ngày tháng phức tạp
        .option("es.write.operation", "index") # Hoặc "upsert" nếu có ID
        
        # --- Timeout & Batching ---
        .option("es.batch.size.entries", "1000")
        .option("es.http.timeout", "5m")
        .option("es.http.retries", "3")
        
        .mode("append")
        .save()
    )

    print(f"✅ Batch {batch_id}: Written to Elasticsearch Cloud")


# ================= START STREAM =================
query = (
    df_10min.writeStream
    .outputMode("update") # update hoặc append tùy logic (aggregate dùng update tốt hơn)
    .foreachBatch(write_elasticsearch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="120 seconds")
    .start()
)

print("🚀 Streaming job running...")
spark.streams.awaitAnyTermination()