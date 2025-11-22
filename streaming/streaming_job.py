from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, from_json, year, month, dayofmonth, broadcast, window, avg, max, first
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os

# --- CẤU HÌNH ---
spark_master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "air_quality")

# --- LOGIC FIX LỖI HDFS (QUAN TRỌNG) ---
# Lấy giá trị từ biến môi trường, nếu không có thì dùng mặc định
raw_namenode = os.getenv("HDFS_NAMENODE", "hdfs://hadoop-namenode:9000")

# Ép buộc phải có hdfs:// ở đầu
if not raw_namenode.startswith("hdfs://"):
    # Nếu người dùng quên nhập hdfs://, tự động thêm vào
    if raw_namenode.startswith("http://"):
        # Nếu lỡ nhập http:// thì đổi thành hdfs://
        hdfs_namenode = raw_namenode.replace("http://", "hdfs://")
    else:
        hdfs_namenode = f"hdfs://{raw_namenode}"
else:
    hdfs_namenode = raw_namenode

print(f"🔍 HDFS URI CHUẨN: {hdfs_namenode}")

# Cấu hình đường dẫn Output và Checkpoint
# Đảm bảo đường dẫn bắt đầu bằng /
output_path = "/data/cleaned_air_quality"
checkpoint_path = "/checkpoints/air_quality"

full_static_path = f"{hdfs_namenode}/data/reference/hanoi_info.csv"
full_output_path = f"{hdfs_namenode}{output_path}"
full_checkpoint_path = f"{hdfs_namenode}{checkpoint_path}"

# Lấy Mongo URI
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("❌ LỖI: Chưa có MONGO_URI trong biến môi trường!")

# 1. KHỞI TẠO SPARK
spark = SparkSession.builder \
    .appName("Hanoi_AirQuality_Streaming") \
    .master(spark_master) \
    .config("spark.sql.streaming.checkpointLocation", full_checkpoint_path) \
    .config("spark.mongodb.connection.uri", mongo_uri) \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. STATIC DATA
try:
    print(f"📥 Đang đọc Static Data từ: {full_static_path}")
    static_df = spark.read.option("header", "true").csv(full_static_path)
except Exception as e:
    print(f"⚠️ Không đọc được file CSV ({e}). Đang dùng dữ liệu giả lập.")
    static_df = spark.createDataFrame([("HANOI", "21.0285", "105.8542")], ["city_name", "lat", "lon"])

# 3. SCHEMA
schema = StructType([
    StructField("city", StringType(), True),
    StructField("aqi", IntegerType(), True),
    StructField("co", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("timestamp_utc", TimestampType(), True),
    StructField("timestamp_local", StringType(), True),
    StructField("source", StringType(), True)
])

# 4. ĐỌC KAFKA
print("📥 Đang đọc Kafka...")
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# 5. XỬ LÝ
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

df_clean = (
    df_parsed
    .withWatermark("timestamp_utc", "10 minutes")
    .dropDuplicates(["city", "timestamp_utc"])
    .na.drop(subset=["city", "aqi"])
    .withColumn("city", trim(upper(col("city"))))
    .filter((col("aqi") >= 0) & (col("aqi") <= 1000))
)

df_final = df_clean.withColumn("year", year(col("timestamp_utc"))) \
                   .withColumn("month", month(col("timestamp_utc"))) \
                   .withColumn("day", dayofmonth(col("timestamp_utc")))

# Join
df_enriched = df_final.join(broadcast(static_df), df_final.city == trim(upper(static_df.city_name)), "left").drop(static_df.city_name)

# Aggregate
df_aggregated = df_enriched \
    .groupBy(window(col("timestamp_utc"), "1 hour"), col("city")) \
    .agg(avg("aqi").alias("avg_aqi"), max("pm25").alias("max_pm25"), first("lat").alias("lat"), first("lon").alias("lon"))

# 6. OUTPUT
print(f"🚀 Ghi MongoDB và HDFS tại: {full_output_path}")

query_mongo = df_aggregated.writeStream \
    .outputMode("update") \
    .format("mongodb") \
    .option("checkpointLocation", f"{full_checkpoint_path}/mongo") \
    .trigger(processingTime='30 seconds') \
    .start()

query_hdfs = df_final.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", full_output_path) \
    .option("checkpointLocation", f"{full_checkpoint_path}/hdfs") \
    .partitionBy("year", "month", "day") \
    .start()

spark.streams.awaitAnyTermination()