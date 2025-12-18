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

# ===== Elasticsearch Cloud (from ENV) =====
ES_NODES = os.getenv("ES_NODES")
ES_PORT = os.getenv("ES_PORT", "443")
ES_INDEX = os.getenv("ES_INDEX", "aqi_data")
ES_API_KEY = os.getenv("ES_API_KEY")
ES_USER = os.getenv("username")
ES_PASS = os.getenv("password")

if not ES_NODES or not ES_API_KEY:
    raise RuntimeError("❌ ES_NODES or ES_API_KEY is not set")

print(f"DEBUG: ES_NODES = {ES_NODES}")
print(f"DEBUG: API_KEY starts with = {ES_API_KEY[:5]}...")


CHECKPOINT_LOCATION = "/app/checkpoints/aqi_streaming"

print("===== CONFIG =====")
print("Kafka:", kafka_servers)
print("Topic:", kafka_topic)
print("Elasticsearch index:", ES_INDEX)
print("==================")

# ================= SPARK SESSION =================
spark = (
    SparkSession.builder
    .appName("Hanoi_AirQuality_Streaming_10min")
    .master(spark_master)
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
    .option("startingOffsets", "earliest") # Luôn đọc từ đầu nếu là checkpoint mới
    .option("failOnDataLoss", "false")     # Thêm dòng này
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

def write_elasticsearch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0: return
    print(f"===== BATCH {batch_id} | ROWS = {count} =====")

    (
        batch_df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_NODES)
        .option("es.port", ES_PORT)
        .option("es.resource", ES_INDEX)
        .option("es.nodes.wan.only", "true")
        .option("es.net.ssl", "true")
        .option("es.net.http.auth.user", ES_USER)
        .option("es.net.http.auth.pass", ES_PASS)
        # BẮT BUỘC cho Cloud: Tắt tự động dò tìm node để tránh request không hợp lệ
        .option("es.nodes.discovery", "false")
        .option("es.nodes.client.only", "false")
        # Thêm cấu hình này để tránh lỗi handshake trên một số môi trường
        .option("es.net.ssl.cert.allow.self.signed", "true")
        .mode("append")
        .save()
    )
    print(f"✅ Batch {batch_id}: Written to Elasticsearch Cloud")


# ================= START STREAM =================
query = (
    df_10min.writeStream
    .outputMode("update")
    .foreachBatch(write_elasticsearch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="30 seconds")
    .start()
)

print("🚀 Streaming job running...")
spark.streams.awaitAnyTermination()
