from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, min as spark_min, count, 
    percentile_approx, to_date, hour, stddev, year, month, dayofmonth,
    dayofweek, sin, cos, lit, from_utc_timestamp, when
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.window import Window
import logging
import sys
import math
import os 

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


# Hàm phân loại chất lượng không khí
def aqi_category(aqi_col):
    return when(aqi_col <= 50, lit("Good")) \
        .when((aqi_col > 50) & (aqi_col <= 100), lit("Moderate")) \
        .when((aqi_col > 100) & (aqi_col <= 150), lit("Unhealthy for Sensitive Groups")) \
        .when((aqi_col > 150) & (aqi_col <= 200), lit("Unhealthy")) \
        .when((aqi_col > 200) & (aqi_col <= 300), lit("Very Unhealthy")) \
        .otherwise(lit("Hazardous"))

def main():
    os.environ["HADOOP_USER_NAME"] = "root"
    
    spark = SparkSession.builder \
        .appName("batch-hourly-aggregates") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    logger.info("=" * 100)
    logger.info("Starting Hourly Aggregates Batch Job")
    logger.info("=" * 100)
    
    try:
        input_path = "hdfs://hadoop-namenode:9000/clean-data/air-quality"
        output_path = "hdfs://hadoop-namenode:9000/batch/air-quality/hourly"
        
        logger.info(f"Reading cleaned data from: {input_path}")
        
        # SỬ DỤNG SCHEMA CỨNG
        df = spark.read.parquet(input_path)
        
        initial_count = df.count()
        logger.info(f"Initial record count: {initial_count}")

        # Chuyển UTC sang giờ Việt Nam
        df = df.withColumn("timestamp_local", from_utc_timestamp(col("timestamp_utc"), "Asia/Ho_Chi_Minh"))

        # Preprocessing & Feature Engineering
        df = (
            df.withColumn("date", to_date(col("timestamp_local")))
              .withColumn("hour_of_day", hour(col("timestamp_local")))
              .withColumn("year", year(col("timestamp_local")))
              .withColumn("month", month(col("timestamp_local")))
              .withColumn("day", dayofmonth(col("timestamp_local")))
              .withColumn("day_of_week", dayofweek(col("timestamp_local")))
              .withColumn("is_weekend", ((col("day_of_week") == 1) | (col("day_of_week") == 7)).cast("int"))
              .withColumn("month_sin", sin(2 * math.pi * col("month") / 12))
              .withColumn("month_cos", cos(2 * math.pi * col("month") / 12))
        )

        # Drop duplicates
        df = df.dropDuplicates(["city", "timestamp_utc"])
        
        # Range validation
        df = df.filter(
            (col("aqi").between(0, 500)) &
            (col("pm25").between(0, 500)) &
            (col("pm10").between(0, 600)) &
            (col("co").between(0, 10000)) &
            (col("no2").between(0, 500)) &
            (col("o3").between(0, 500)) &
            (col("so2").between(0, 500))
        )
        
        # --- 4. HOURLY AGGREGATION ---
        logger.info("Computing hourly aggregates...")
        
        hourly_agg = df.groupBy(
            "city", "date", "hour_of_day",
            "year", "month", "day",
            "day_of_week", "is_weekend",
            "month_sin", "month_cos"              
        ).agg(
            count("*").alias("n_records"),
            
            # AQI
            avg("aqi").alias("aqi_avg"),
            stddev("aqi").alias("aqi_stddev"),
            spark_max("aqi").alias("aqi_max"),
            spark_min("aqi").alias("aqi_min"),
            percentile_approx("aqi", 0.50).alias("aqi_p50"),
            
            # PM2.5
            avg("pm25").alias("pm25_avg"),
            stddev("pm25").alias("pm25_stddev"),
            spark_max("pm25").alias("pm25_max"),
            spark_min("pm25").alias("pm25_min"),
            percentile_approx("pm25", 0.50).alias("pm25_p50"),
            
            avg("pm10").alias("pm10_avg"),
            stddev("pm10").alias("pm10_stddev"),
            spark_max("pm10").alias("pm10_max"),
            spark_min("pm10").alias("pm10_min"),
            percentile_approx("pm10", 0.50).alias("pm10_p50"),
            
            # Others
            avg("co").alias("co_avg"),      stddev("co").alias("co_stddev"),
            avg("no2").alias("no2_avg"),    stddev("no2").alias("no2_stddev"),
            avg("o3").alias("o3_avg"),      stddev("o3").alias("o3_stddev"),
            avg("so2").alias("so2_avg"),    stddev("so2").alias("so2_stddev"),
        )

        # WHO Status
        hourly_agg = hourly_agg.withColumn("who_status", aqi_category(col("aqi_avg")))

        # --- 5. SPIKE DETECTION ---
        logger.info("Computing rolling statistics...")
        window_24h = Window.partitionBy("city").orderBy("year", "month", "day", "hour_of_day").rowsBetween(-23, -1)
        
        hourly_agg = hourly_agg.withColumn("aqi_rolling_avg_24h", avg("aqi_avg").over(window_24h))
        
        hourly_agg = hourly_agg.withColumn(
            "spike_flag",
            when(
                (col("aqi_avg") > col("aqi_rolling_avg_24h") * 1.5) & 
                col("aqi_rolling_avg_24h").isNotNull(), 
                1
            ).otherwise(0)
        )

        logger.info(f"Writing to {output_path}...")
        
        hourly_agg.repartition(1).write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)
        
        logger.info("Hourly aggregates written successfully")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"Error in batch job: {e}", exc_info=True)
        return False
        
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)