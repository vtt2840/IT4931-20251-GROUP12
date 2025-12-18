import sys
import os
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, max as spark_max, min as spark_min, 
    stddev, count, when, first, date_trunc, udf, 
    dayofweek, expr, percentile_approx, skewness
)
from pyspark.sql.types import StringType
from pyspark.sql.functions import from_utc_timestamp, hour as spark_hour

# Chuẩn hóa log khi chạy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_ROOT = "hdfs://hadoop-namenode:9000"

def get_spark_session(job_name):
    os.environ["HADOOP_USER_NAME"] = "hadoop"
    return SparkSession.builder \
        .appName(job_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

# --- UDF: Phân loại AQI ---
@udf(returnType=StringType())
def categorize_aqi(aqi):
    if aqi is None: return "Unknown"
    if aqi <= 50: return "Good"
    elif aqi <= 100: return "Moderate"
    elif aqi <= 150: return "Unhealthy for Sensitive"
    elif aqi <= 200: return "Unhealthy"
    elif aqi <= 300: return "Very Unhealthy"
    else: return "Hazardous"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--day", type=int)
    parser.add_argument("--hour", type=int)
    args = parser.parse_args()

    if not args.year:
        import datetime
        now = datetime.datetime.now()
        args.year, args.month, args.day, args.hour = now.year, now.month, now.day, now.hour

    logger.info(f"STARTING EDA FEATURE JOB (AQI + PM2.5): {args.year}-{args.month}-{args.day} Hour: {args.hour}")
    spark = get_spark_session(f"EDA Features {args.hour}h")
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

    # Chuẩn hóa thời gian về cùng phút để join

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

    
    master_df = master_df.withColumn("local_time", from_utc_timestamp(col("join_time"), "Asia/Ho_Chi_Minh"))
    master_df = master_df.filter(spark_hour("join_time") == args.hour) # Lọc theo giờ UTC

    if master_df.rdd.isEmpty():
        logger.warning(f"No data for hour {args.hour} UTC.")
        return

    master_df = master_df.withColumn("day_of_week", dayofweek("local_time")) \
                         .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0)) \
                         .withColumn("local_hour", spark_hour("local_time"))

    # --- 5. AGGREGATION (FULL METRICS: AQI, PM2.5, PM10) ---
    final_report = master_df.groupBy("city").agg(

        lit(args.year).alias("year"),
        lit(args.month).alias("month"),
        lit(args.day).alias("day"),
        lit(args.hour).alias("hour"), 
        first("local_hour").alias("local_hour"),
        first("day_of_week").alias("day_of_week"),
        first("is_weekend").alias("is_weekend"),
        
        # === AQI STATS ===
        avg("aqi").alias("aqi_mean"),
        percentile_approx("aqi", 0.5).alias("aqi_median"),
        percentile_approx("aqi", 0.25).alias("aqi_q1"),
        percentile_approx("aqi", 0.75).alias("aqi_q3"),
        spark_max("aqi").alias("aqi_max"),
        
        # === PM2.5 STATS ===
        # Dùng cho Boxplot PM2.5 và Time Series
        avg("pm25").alias("pm25_mean"),
        percentile_approx("pm25", 0.5).alias("pm25_median"), # Trung vị PM2.5
        percentile_approx("pm25", 0.25).alias("pm25_q1"),    # Q1(Tứ phân vị thứ nhất) -> 25% dữ liệu <=Q1 , Q1~Q3 ổn định , Q3-Q1 lớn biến động mạnh
        percentile_approx("pm25", 0.75).alias("pm25_q3"),    # Q3(Tứ phân vị thứ ba) -> 75% dữ liệu <= Q3 
        stddev("pm25").alias("pm25_stddev"),                 # Độ biến động của bụi mịn
        spark_max("pm25").alias("pm25_max"),
        spark_min("pm25").alias("pm25_min"),

        # === PM10 STATS (Phân tích bụi thô ) ===
        avg("pm10").alias("pm10_mean"),
        percentile_approx("pm10", 0.5).alias("pm10_median"),
        spark_max("pm10").alias("pm10_max"),
        
        # Weather Stats (Cho Correlation)
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_hum"),
        avg("wind_speed").alias("avg_wind"),
        
        # Categorization(Phân loại AQI theo giờ )
        categorize_aqi(avg("aqi")).alias("aqi_category_hourly")
    )

    # --- 6. SAVE ---
    output_path = f"{HDFS_ROOT}/user/hadoop/batch/hourly"
    
    logger.info(f"Writing Full EDA data to: {output_path}")
    final_report.coalesce(1).write \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)

    logger.info("Job Finished Successfully.")
    spark.stop()

if __name__ == "__main__":
    main()