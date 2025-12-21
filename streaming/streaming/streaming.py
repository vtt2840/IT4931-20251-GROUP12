from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, from_json, avg, max as spark_max,
    to_timestamp, when, window, stddev, percentile_approx,
    count, sha2, concat_ws, broadcast, lit  
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)
from pyspark.sql.functions import udf
import os

# ================= CONFIG =================
kafka_servers = os.getenv("KAFKA_BROKER", "kafka:9092")
kafka_topic = os.getenv("TOPIC", "air-quality")
spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

raw_es_nodes = os.getenv("ES_NODES")
ES_INDEX = os.getenv("ES_INDEX", "aqi_data")
ES_INDEX_PIVOT = os.getenv("ES_INDEX_PIVOT", "aqi_category_stats")
ES_API_KEY = os.getenv("ES_API_KEY")

if not raw_es_nodes or not ES_API_KEY:
    raise RuntimeError("ES_NODES or ES_API_KEY is not set")


ES_NODES = raw_es_nodes.replace("https://", "").replace("http://", "")

CHECKPOINT_LOCATION = "/app/checkpoints/aqi_streaming"

# ================= SPARK SESSION =================
spark = (
    SparkSession.builder
    .appName("Hanoi_AirQuality_Streaming_Optimized")
    .master(spark_master)
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0"
    )
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===== PERFORMANCE TUNING =====
spark.conf.set("spark.sql.shuffle.partitions", "4")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# ================= SCHEMA =================
schema = StructType([
    StructField("city", StringType()),
    StructField("aqi", IntegerType()),
    StructField("co", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("o3", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("pm25", DoubleType()),
    StructField("so2", DoubleType()),
    StructField("timestamp_local", StringType()),
    StructField("timestamp_utc", StringType())
])

# ================= STATIC DATA =================
static_df = spark.createDataFrame(
    [("HANOI", 21.0285, 105.8542, "Vietnam")],
    ["city", "lat", "lon", "country"]
).cache()

# ================= CUSTOM UDF =================
def aqi_risk_score(aqi):
    if aqi is None:
        return None
    if aqi <= 50:
        return 0.1
    elif aqi <= 100:
        return 0.3
    elif aqi <= 150:
        return 0.6
    elif aqi <= 200:
        return 0.8
    else:
        return 1.0

aqi_risk_udf = udf(aqi_risk_score, DoubleType())

# ================= 1. READ KAFKA =================
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
        to_timestamp("timestamp_utc", "yyyy-MM-dd'T'HH:mm:ss")
    )
)

# ================= 3. CLEAN + WATERMARK =================

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
        .when(col("aqi") <= 150, "Unhealthy")
        .when(col("aqi") <= 200, "Very Unhealthy")
        .otherwise("Hazardous")
    )
    .withColumn("aqi_risk_score", aqi_risk_udf(col("aqi")))
)

# ================= 4. ENRICH (BROADCAST JOIN) =================
df_enriched = (
    df_clean
    .join(broadcast(static_df), "city", "left")
)

# ================= 5. ADVANCED WINDOW AGG (MAIN STREAM) =================

df_10min = (
    df_enriched
    .groupBy(window("timestamp_utc", "10 minutes"), "city")
    .agg(
        avg("aqi").alias("avg_aqi"),
        stddev("aqi").alias("stddev_aqi"),
        percentile_approx("aqi", 0.95).alias("p95_aqi"),
        count("*").alias("event_count"),
        avg("pm25").alias("avg_pm25"),
        avg("pm10").alias("avg_pm10"),
        spark_max("lat").alias("lat"),
        spark_max("lon").alias("lon"),
        spark_max("country").alias("country")
    )
    .selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "city",
        "avg_aqi",
        "stddev_aqi",
        "p95_aqi",
        "event_count",
        "avg_pm25",
        "avg_pm10",
        "lat",
        "lon",
        "country"
    )
)

df_10min = df_10min.withColumn(
    "doc_id",
    sha2(
        concat_ws("_", col("city"), col("window_start").cast("string")),
        256
    )
)

# ================= 6. PIVOT (PIVOT STREAM) =================
df_pivot = (
    df_clean
    .groupBy(window("timestamp_utc", "10 minutes"), "city")
    .pivot(
        "aqi_category",
        ["Good", "Moderate", "Unhealthy", "Very Unhealthy", "Hazardous"]
    )
    .count()
    .selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "city",
        "coalesce(Good, 0) as Good",            
        "coalesce(Moderate, 0) as Moderate",
        "coalesce(Unhealthy, 0) as Unhealthy",
        "coalesce(`Very Unhealthy`, 0) as `Very_Unhealthy`",
        "coalesce(Hazardous, 0) as Hazardous"
    )
)


df_pivot = df_pivot.withColumn(
    "pivot_doc_id",
    sha2(
        concat_ws("_", col("city"), col("window_start").cast("string"), lit("pivot")),
        256
    )
)

# ================= ES WRITER (OPTIMIZED) =================
def write_es(batch_df, batch_id, index_name, id_field=None):

    
    auth_header = f"ApiKey {ES_API_KEY}"
    
    writer = (
        batch_df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_NODES)
        .option("es.port", "443")
        .option("es.nodes.wan.only", "true")
        .option("es.net.ssl", "true")
        .option("es.nodes.discovery", "false")
        .option("es.net.http.header.Authorization", auth_header)
        .option("es.write.operation", "upsert") 
        .option("es.resource", index_name)
    )
    
    # Chỉ định ID field để ES biết record nào cần update
    if id_field:
        writer = writer.option("es.mapping.id", id_field)

    try:
        writer.mode("append").save()
        print(f"Batch {batch_id} wrote to {index_name}")
    except Exception as e:
        print(f"ERROR: Batch {batch_id} failed writing to {index_name}. Error: {e}")

# ================= START STREAMS =================

# Query 1: Main Aggregation
query_main = (
    df_10min.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, bid: write_es(df, bid, ES_INDEX, "doc_id"))
    .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/main")
    .trigger(processingTime="120 seconds")
    .start()
)

# Query 2: Pivot Aggregation
query_pivot = (
    df_pivot.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, bid: write_es(df, bid, ES_INDEX_PIVOT, "pivot_doc_id"))
    .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/pivot")
    .trigger(processingTime="120 seconds")
    .start()
)

spark.streams.awaitAnyTermination()