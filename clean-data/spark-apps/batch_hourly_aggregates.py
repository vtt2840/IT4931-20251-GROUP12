from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, min as spark_min, count, 
    percentile_approx, to_date, hour, stddev, year, month, dayofmonth,
    dayofweek, sin, cos, lit
)
from pyspark.sql.window import Window
import logging
import sys
import math

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

def main():
    """
    Hourly aggregates job:
    - Đọc dữ liệu từ /clean-data/air_quality (Parquet đã làm sạch)
    - Preprocessing: loại bỏ trùng lặp, kiểm tra ranges, xử lý nulls
    - Tính aggregates theo city/date/hour: avg, max, percentiles cho AQI/PM2.5/PM10
    - Thêm feature engineering: day_of_week, is_weekend, month sin/cos
    - Ghi Parquet partitioned vào /batch/air_quality/hourly
    """
    
    spark = SparkSession.builder \
        .appName("batch-hourly-aggregates") \
        .getOrCreate()
    
    # Set Parquet compression
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    
    logger.info("=" * 80)
    logger.info("Starting Hourly Aggregates Batch Job")
    logger.info("=" * 80)
    
    try:
        # Paths
        input_path = "hdfs://hadoop-namenode:9000/clean-data/air_quality"
        output_path = "hdfs://hadoop-namenode:9000/batch/air_quality/hourly"
        
        logger.info(f"Reading cleaned data from: {input_path}")
        df = spark.read.parquet(input_path)
        
        initial_count = df.count()
        logger.info(f"Initial record count: {initial_count}")
        
        logger.info("Starting preprocessing...")
        
        # 1. Ensure timestamp is properly converted to date and time features
        df = df.withColumn("date", to_date(col("timestamp_utc")))
        df = df.withColumn("hour_of_day", hour(col("timestamp_utc")))
        # explicit time-based features
        df = df.withColumn("year", year(col("timestamp_utc")))
        df = df.withColumn("month", month(col("timestamp_utc")))
        df = df.withColumn("day", dayofmonth(col("timestamp_utc")))
        
        # Feature engineering: day_of_week, is_weekend, month sin/cos
        df = df.withColumn("day_of_week", dayofweek(col("timestamp_utc")))  # 1=Sunday, 7=Saturday
        df = df.withColumn("is_weekend", 
            ((col("day_of_week") == 1) | (col("day_of_week") == 7)).cast("int"))
        df = df.withColumn("month_sin", sin(2 * math.pi * col("month") / 12))
        df = df.withColumn("month_cos", cos(2 * math.pi * col("month") / 12))
        
        # 2. Drop duplicates (keep first occurrence per city/timestamp)
        df_no_dup = df.dropDuplicates(["city", "timestamp_utc"])
        dup_count = initial_count - df_no_dup.count()
        logger.info(f"Duplicates removed: {dup_count}")
        df = df_no_dup
        
        # 3. Check and report nulls before filtering
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                null_counts[col_name] = null_count
                logger.warning(f"Column '{col_name}' has {null_count} nulls")
        
        # 4. Range validation & filter out-of-range values
        df = df.filter(
            (col("aqi") >= 0) & (col("aqi") <= 500) &
            (col("pm25") >= 0) & (col("pm25") <= 500) &
            (col("pm10") >= 0) & (col("pm10") <= 600) &
            (col("co") >= 0) & (col("co") <= 10000) &
            (col("no2") >= 0) & (col("no2") <= 500) &
            (col("o3") >= 0) & (col("o3") <= 500) &
            (col("so2") >= 0) & (col("so2") <= 500)
        )
        
        after_filter_count = df.count()
        logger.info(f"Records after range filtering: {after_filter_count}")
        logger.info(f"Records filtered out (out of range): {initial_count - after_filter_count}")
        
        logger.info("Computing hourly aggregates...")
        
        hourly_agg = (
            df.groupBy("city", "date", "hour_of_day", "year", "month", "day", "day_of_week", "is_weekend", "month_sin", "month_cos")
              .agg(
                  count("*").alias("n_records"),
                  
                  # AQI aggregates
                  avg("aqi").alias("aqi_avg"),
                  stddev("aqi").alias("aqi_stddev"),
                  spark_max("aqi").alias("aqi_max"),
                  spark_min("aqi").alias("aqi_min"),
                  percentile_approx("aqi", 0.50).alias("aqi_p50"),
                  
                  # PM2.5 aggregates
                  avg("pm25").alias("pm25_avg"),
                  stddev("pm25").alias("pm25_stddev"),
                  spark_max("pm25").alias("pm25_max"),
                  spark_min("pm25").alias("pm25_min"),
                  percentile_approx("pm25", 0.50).alias("pm25_p50"),
                  
                  # PM10 aggregates
                  avg("pm10").alias("pm10_avg"),
                  stddev("pm10").alias("pm10_stddev"),
                  spark_max("pm10").alias("pm10_max"),
                  spark_min("pm10").alias("pm10_min"),
                  percentile_approx("pm10", 0.50).alias("pm10_p50"),
                  
                  # Other pollutants
                  avg("co").alias("co_avg"),
                  stddev("co").alias("co_stddev"),
                  avg("no2").alias("no2_avg"),
                  stddev("no2").alias("no2_stddev"),
                  avg("o3").alias("o3_avg"),
                  stddev("o3").alias("o3_stddev"),
                  avg("so2").alias("so2_avg"),
                  stddev("so2").alias("so2_stddev"),
              )
        )
        
        agg_count = hourly_agg.count()
        logger.info(f"Hourly aggregates computed: {agg_count} city/date/hour combinations")
        
        # ============ SPIKE DETECTION (HOURLY) ============
        logger.info("Computing rolling statistics for spike detection...")
        
        # Calculate 24-hour rolling average (1-day window)
        window_24h = Window.partitionBy("city").orderBy("date", "hour_of_day").rowsBetween(-23, 0)
        hourly_agg = hourly_agg.withColumn("aqi_rolling_avg_24h", avg("aqi_avg").over(window_24h))
        
        # Flag spikes (>50% increase from rolling average)
        hourly_agg = hourly_agg.withColumn(
            "spike_flag",
            ((col("aqi_avg") > col("aqi_rolling_avg_24h") * 1.5) & (col("aqi_rolling_avg_24h").isNotNull())).cast("int")
        )
        
        spike_count = hourly_agg.filter(col("spike_flag") == 1).count()
        logger.info(f"Spikes detected (>50% increase): {spike_count}")
        
        logger.info(f"Writing hourly aggregates to: {output_path}")
        
        hourly_agg.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day", "hour_of_day") \
            .parquet(output_path)
        
        logger.info("✅ Hourly aggregates written successfully")
        
        logger.info("=" * 80)
        logger.info("BATCH JOB SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Input records (initial): {initial_count}")
        logger.info(f"Duplicates removed: {dup_count}")
        logger.info(f"Out-of-range records filtered: {initial_count - dup_count - after_filter_count}")
        logger.info(f"Records after preprocessing: {after_filter_count}")
        logger.info(f"Hourly aggregates created: {agg_count}")
        logger.info(f"Spike detections: {spike_count}")
        logger.info(f"Output location: {output_path}")
        logger.info(f"Partitioning: year/month/day/hour_of_day")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in batch job: {e}", exc_info=True)
        return False
        
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
