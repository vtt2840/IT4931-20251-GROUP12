import sys
import os
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, date_trunc, hour as spark_hour
)

# Chuẩn hóa log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_ROOT = "hdfs://hadoop-namenode:9000"

def get_spark_session(job_name):
    return SparkSession.builder \
        .appName(job_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--day", type=int)
    parser.add_argument("--hour", type=int)
    args = parser.parse_args()

    # Tự động lấy giờ hiện tại nếu không truyền tham số
    if not args.year:
        import datetime
        now = datetime.datetime.utcnow()
        args.year, args.month, args.day, args.hour = now.year, now.month, now.day, now.hour

    logger.info(f"STARTING COMPACT JOB (AQI + PM2.5 + Weather): {args.year}-{args.month}-{args.day} Hour: {args.hour}")
    spark = get_spark_session(f"Batch Processing {args.hour}h")
    spark.sparkContext.setLogLevel("ERROR")

    # --- 1. ĐỌC DỮ LIỆU ---
    def read_source(subpath):
        try:
            path = f"{HDFS_ROOT}/user/hadoop/clean-data/{subpath}"
            return spark.read.parquet(path).filter(
                (col("year") == args.year) & 
                (col("month") == args.month) & 
                (col("day") == args.day)
            )
        except Exception:
            return None

    df_air = read_source("air_quality")
    df_wind = read_source("weather_wind")
    df_temp = read_source("weather_temperature")
    df_hum = read_source("weather_humidity")

    if not df_air:
        logger.error("No Air Quality data found. Exiting.")
        return

    # --- 2. CHUẨN BỊ JOIN ---
    def prep_df(df, metric_col):
        if df is None: return None
        return df.withColumn("join_time", date_trunc("minute", col("timestamp_utc"))) \
                 .select("city", "join_time", metric_col)

    df_air = df_air.withColumn("join_time", date_trunc("minute", col("timestamp_utc")))
    df_wind_clean = prep_df(df_wind, "wind_speed")
    df_temp_clean = prep_df(df_temp, "temperature")
    df_hum_clean  = prep_df(df_hum, "humidity")

    # --- 3. JOIN DATA ---
    master_df = df_air
    if df_wind_clean: master_df = master_df.join(df_wind_clean, ["city", "join_time"], "left")
    if df_temp_clean: master_df = master_df.join(df_temp_clean, ["city", "join_time"], "left")
    if df_hum_clean:  master_df = master_df.join(df_hum_clean, ["city", "join_time"], "left")

    # Lọc đúng giờ cần xử lý
    master_df = master_df.filter(spark_hour("join_time") == args.hour)

    if master_df.rdd.isEmpty():
        logger.warning(f"No data for hour {args.hour} UTC.")
        return

    # --- 4. AGGREGATION (CHỈ LẤY CÁI CẦN THIẾT) ---
    final_report = master_df.groupBy("city").agg(
        lit(args.year).alias("year"),
        lit(args.month).alias("month"),
        lit(args.day).alias("day"),
        lit(args.hour).alias("hour"),
        
        avg("aqi").alias("aqi_mean"),          
        avg("pm25").alias("pm25_mean"),        
        
        # Chỉ số thời tiết (Correlation)
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_hum"),      
        avg("wind_speed").alias("avg_wind")   
    )

    # --- 5. SAVE ---
    output_path = f"{HDFS_ROOT}/user/hadoop/batch/hourly"
    
    logger.info(f"Writing Compact data to: {output_path}")
    final_report.coalesce(1).write \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)

    logger.info("Job Finished Successfully.")
    spark.stop()

if __name__ == "__main__":
    main()