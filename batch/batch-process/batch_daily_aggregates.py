from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, max as spark_max, min as spark_min, 
    count, lit, when, first, round as spark_round
)
from pyspark.sql.window import Window
import sys
import os 

def aqi_category(aqi_col):
    return when(aqi_col <= 50, lit("Good")) \
        .when((aqi_col > 50) & (aqi_col <= 100), lit("Moderate")) \
        .when((aqi_col > 100) & (aqi_col <= 150), lit("Unhealthy for Sensitive Groups")) \
        .when((aqi_col > 150) & (aqi_col <= 200), lit("Unhealthy")) \
        .when((aqi_col > 200) & (aqi_col <= 300), lit("Very Unhealthy")) \
        .otherwise(lit("Hazardous"))

def main():
    """
    Daily Aggregates Job (Safe Mode)
    - Tự động bỏ qua các cột thiếu để tránh lỗi crash.
    """
    os.environ["HADOOP_USER_NAME"] = "root"
    
    spark = SparkSession.builder \
        .appName("batch-daily-aggregates-rollup") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Log4j Setup
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger("DAILY_BATCH_JOB")
    
    logger.info("=" * 50)
    logger.info(">>> STARTING DAILY AGGREGATES JOB (SAFE MODE)")
    
    try:
        input_path = "hdfs://hadoop-namenode:9000/batch/air-quality/hourly"
        output_path = "hdfs://hadoop-namenode:9000/batch/air-quality/daily"
        
        logger.info(f">>> Reading hourly data from: {input_path}")
        
        try:
            df = spark.read.parquet(input_path)
        except Exception as e:
            logger.warn(f">>> DATA NOT FOUND: {e}")
            return True 

        existing_columns = df.columns
        logger.info(f">>> Columns found in Hourly data: {existing_columns}")

        logger.info(">>> Computing Daily Roll-up...")

        metrics = ["aqi", "pm25", "pm10", "co", "no2", "o3", "so2"]
        
        # Các cột cơ bản luôn phải có
        agg_exprs = [
            spark_sum("n_records").alias("n_records"),
            first("day_of_week").alias("day_of_week"),
            first("is_weekend").alias("is_weekend"),
            first("month_sin").alias("month_sin"),
            first("month_cos").alias("month_cos")
        ]
        
        for m in metrics:
            # Luôn ưu tiên tính Average (Trung bình)
            if f"{m}_avg" in existing_columns:
                df = df.withColumn(f"{m}_weight", col(f"{m}_avg") * col("n_records"))
                agg_exprs.append(
                    spark_round(spark_sum(f"{m}_weight") / spark_sum("n_records"), 2).alias(f"{m}_avg")
                )
                
            if f"{m}_max" in existing_columns:
                agg_exprs.append(spark_max(f"{m}_max").alias(f"{m}_max"))
                
            if f"{m}_stddev" in existing_columns:
                agg_exprs.append(spark_round(avg(f"{m}_stddev"), 2).alias(f"{m}_stddev"))

        daily_agg = df.groupBy("city", "date", "year", "month", "day").agg(*agg_exprs)
        
        if "aqi_avg" in daily_agg.columns:
            daily_agg = daily_agg.withColumn("who_status", aqi_category(col("aqi_avg")))
            
            # Spike Detection
            window_7d = Window.partitionBy("city").orderBy("year", "month", "day").rowsBetween(-7, -1)
            daily_agg = daily_agg.withColumn("rolling_avg_7d", spark_round(avg("aqi_avg").over(window_7d), 2))
            daily_agg = daily_agg.withColumn("spike_flag", 
                when((col("aqi_avg") > col("rolling_avg_7d") * 1.5) & col("rolling_avg_7d").isNotNull(), 1).otherwise(0)
            )

        logger.info(f">>> Writing to {output_path}")
        
        daily_agg.repartition(1).write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)
            
        logger.info(">>> DAILY JOB SUCCESS")
        return True

    except Exception as e:
        logger.error(f"!!! JOB ERROR: {e}")
        raise e 
    finally:
        spark.stop()

if __name__ == "__main__":
    main()