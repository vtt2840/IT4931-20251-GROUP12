from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, from_json, year, month, dayofmonth, window, 
    avg, max, first, to_timestamp, when, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import sys


# Environment variables

kafka_servers = os.getenv("KAFKA_BROKER", "kafka:9092")
kafka_topic = os.getenv("TOPIC", "air-quality")
spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
hdfs_url = os.getenv("HDFS_URL")
hdfs_path_root = os.getenv("HDFS_PATH", "/data/air-quality/")
mongo_uri = os.getenv("MONGO_URI")

if not mongo_uri:
    print("MONGO_URI is not set. Exiting...")
    sys.exit(1)

FULL_OUTPUT_PATH = f"{hdfs_url}{hdfs_path_root}/cleaned/"
CHECKPOINT_HDFS = f"{hdfs_url}/spark/checkpoints/streaming_aqi"
CHECKPOINT_MONGO = f"{hdfs_url}/checkpoints/aqi_mongo"

print(f"Kafka = {kafka_servers}, topic = {kafka_topic}")
print(f"HDFS = {hdfs_url}")


spark = SparkSession.builder \
    .appName("Hanoi_AirQuality_Streaming") \
    .master(spark_master) \
    .config("spark.mongodb.connection.uri", mongo_uri) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("city_name", StringType(), True),
    StructField("aqi", IntegerType(), True),
    StructField("co", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("timestamp_local", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
])


static_df = spark.createDataFrame([
    ("HANOI", "21.0285", "105.8542", "Vietnam"),
], ["city_name", "lat", "lon", "country"])


df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp_utc", to_timestamp("timestamp_utc"))


df_clean = (
    df_parsed
    .withWatermark("timestamp_utc", "5 minutes")
    .dropDuplicates(["city_name", "timestamp_utc"])
    .na.drop(subset=["city_name", "aqi"])
    .withColumn("city_name", trim(upper(col("city_name"))))
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
    .withColumn("year", year(col("timestamp_utc")))
    .withColumn("month", month(col("timestamp_utc")))
    .withColumn("day", dayofmonth(col("timestamp_utc")))
)


df_enriched = df_clean.alias("c").join(
    broadcast(static_df.alias("s")),
    col("c.city_name") == col("s.city_name"),
    "left"
).select(
    col("c.*"),
    col("s.lat"),
    col("s.lon"),
    col("s.country")
)


df_aggregated = df_enriched.groupBy(
    window("timestamp_utc", "1 hour"),
    col("city_name")
).agg(
    avg("aqi").alias("avg_aqi_1h"),
    max("pm25").alias("max_pm25_1h"),
    first("lat").alias("lat"),
    first("lon").alias("lon"),
    first("country").alias("country")
).withColumn("window_start", col("window.start")) \
 .withColumn("window_end", col("window.end")) \
 .drop("window")


query_mongo = df_aggregated.writeStream \
    .outputMode("update") \
    .format("mongodb") \
    .option("checkpointLocation", CHECKPOINT_MONGO) \
    .option("database", "aqi_db") \
    .option("collection", "hourly_summary") \
    .trigger(processingTime="30 seconds") \
    .start()


query_hdfs = df_clean.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", FULL_OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_HDFS) \
    .partitionBy("year", "month", "day") \
    .start()

print("Streaming job started...")
spark.streams.awaitAnyTermination()
