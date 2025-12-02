from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, from_json, avg, max as spark_max,
    to_timestamp, when, window
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os, sys

# === CONFIG ===
kafka_servers = os.getenv("KAFKA_BROKER", "kafka:9092")
kafka_topic = os.getenv("TOPIC", "air-quality")
spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
hdfs_url = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:9000")
mongo_uri = os.getenv("MONGO_URI")


if not mongo_uri:
    print("MONGO_URI is not set. Exiting...")
    sys.exit(1)

CHECKPOINT_MONGO = f"{hdfs_url}/checkpoints/streaming_10min"

print("===== CONFIG =====")
print("Kafka:", kafka_servers)
print("Topic:", kafka_topic)
print("Checkpoint:", CHECKPOINT_MONGO)
print("==================")

# === SPARK SESSION ===
spark = (
    SparkSession.builder
    .appName("Hanoi_AirQuality_Streaming_10min")
    .master(spark_master)
    .config("spark.mongodb.connection.uri", mongo_uri)
    .config("spark.mongodb.output.uri", mongo_uri)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# === SCHEMA KAFKA JSON ===
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

# Data tĩnh tham chiếu
static_df = spark.createDataFrame([
    ("HANOI", 21.0285, 105.8542, "Vietnam"),
], ["city", "lat", "lon", "country"])

# === 1. READ STREAM FROM KAFKA ===
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# === 2. PARSE JSON ===
df_parsed = df_raw.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), schema).alias("d")) \
    .select("d.*") \
    .withColumn("timestamp_utc", to_timestamp(col("timestamp_utc"), "yyyy-MM-dd'T'HH:mm:ss"))

# === 3. CLEAN & CATEGORY ===
df_clean = df_parsed \
    .withWatermark("timestamp_utc", "10 minutes") \
    .dropDuplicates(["city", "timestamp_utc"]) \
    .na.drop(subset=["city", "aqi"]) \
    .withColumn("city", trim(upper(col("city")))) \
    .filter((col("aqi") >= 0) & (col("aqi") <= 1000)) \
    .withColumn(
        "aqi_category",
        when(col("aqi") <= 50, "Good")
        .when(col("aqi") <= 100, "Moderate")
        .when(col("aqi") <= 150, "Unhealthy for Sensitive Groups")
        .when(col("aqi") <= 200, "Unhealthy")
        .when(col("aqi") <= 300, "Very Unhealthy")
        .otherwise("Hazardous")
    )

# === 4. ENRICH STATIC DATA ===
df_enriched = df_clean.alias("c") \
    .join(static_df.alias("s"), col("c.city") == col("s.city"), "left") \
    .select(col("c.*"), col("s.lat"), col("s.lon"), col("s.country"))

# === 5. 10-MINUTES AGGREGATION ===
df_10min = df_enriched.groupBy(
    window("timestamp_utc", "10 minutes"),
    col("city")
).agg(
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
).selectExpr(
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

# === 6. FOREACH BATCH TO MONGO ===
def write_mongo(batch_df, batch_id):
    count = batch_df.count()
    print(f"\n===== BATCH {batch_id} | ROWS = {count} =====")
    if count == 0:
        print("Empty batch.")
        return

    batch_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", "aqi_db") \
        .option("collection", "summary_10min") \
        .option("operationType", "replace") \
        .option("idFieldList", "city,window_start") \
        .save()
    print("MongoDB updated")

query = df_10min.writeStream \
    .outputMode("update") \
    .foreachBatch(write_mongo) \
    .option("checkpointLocation", CHECKPOINT_MONGO) \
    .trigger(processingTime="30 seconds") \
    .start()

print("Streaming job running...")
spark.streams.awaitAnyTermination()
