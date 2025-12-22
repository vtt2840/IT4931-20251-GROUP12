import sys
import os
import argparse
import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, 
    count, round as spark_round, 
    lag, when, make_date, lit
)
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_ROOT = os.getenv("HDFS_URL", "hdfs://hadoop-namenode:9000")

def get_spark_session(job_name):
    os.environ["HADOOP_USER_NAME"] = "hadoop"
    return SparkSession.builder \
        .appName(job_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    args = parser.parse_args()

    now = datetime.datetime.now()
    if not args.year: args.year = now.year
    if not args.month: args.month = now.month

    logger.info(f"--- DAILY ANALYTICS: Processing {args.month}/{args.year} ---")
    spark = get_spark_session(f"Daily_Analytics_{args.year}_{args.month}")
    spark.sparkContext.setLogLevel("ERROR")

    base_path = f"{HDFS_ROOT}/user/hadoop/batch/hourly"
    
    prev_month = args.month - 1 if args.month > 1 else 12
    prev_year = args.year if args.month > 1 else args.year - 1

    path_curr = f"{base_path}/year={args.year}/month={args.month}"
    path_prev = f"{base_path}/year={prev_year}/month={prev_month}"

    logger.info(f"Reading data from:\n - {path_curr}\n - {path_prev}")

    try:
        df_hourly = spark.read \
            .option("basePath", base_path) \
            .parquet(path_curr, path_prev)
            
    except Exception as e:
        logger.warning(f"Không thể đọc cả 2 tháng. Thử chỉ đọc tháng hiện tại...")
        try:
            df_hourly = spark.read \
                .option("basePath", base_path) \
                .parquet(path_curr)
        except Exception as e2:
            logger.error(f"FATAL: Không tìm thấy dữ liệu tháng hiện tại ({path_curr}). Dừng Job.")
            spark.stop()
            return

    df_hourly = df_hourly.select(
        "city", "year", "month", "day", 
        "pm25_mean", "aqi_mean", "avg_temp", "avg_hum", "avg_wind"
    )

    daily_agg = df_hourly.groupBy("city", "year", "month", "day").agg(
        spark_round(avg("pm25_mean"), 2).alias("pm25_avg"),
        spark_round(avg("aqi_mean"), 0).alias("aqi_avg"),
        spark_round(avg("avg_temp"), 1).alias("temp_avg"),
        spark_round(avg("avg_hum"), 1).alias("hum_avg"),
        spark_round(avg("avg_wind"), 1).alias("wind_avg"),
        spark_round(spark_max("pm25_mean"), 2).alias("pm25_peak_hour"),
        count(when(col("pm25_mean") > 50, 1)).alias("polluted_hours_count")
    )

    daily_agg = daily_agg.withColumn("date", make_date("year", "month", "day"))
    w_daily = Window.partitionBy("city").orderBy("date")

    df_trend = daily_agg.withColumn(
        "prev_day_pm25", lag("pm25_avg", 1).over(w_daily)
    ).withColumn(
        "growth_pct", 
        spark_round(((col("pm25_avg") - col("prev_day_pm25")) / col("prev_day_pm25")) * 100, 2)
    ).withColumn(
        "trend_status",
        when(col("growth_pct") > 10, "WORSENING")
        .when(col("growth_pct") < -10, "IMPROVING")
        .otherwise("STABLE")
    ).drop("prev_day_pm25")

    df_final_write = df_trend.filter(
        (col("year") == args.year) & (col("month") == args.month)
    )

    output_path = f"{HDFS_ROOT}/user/hadoop/batch/daily"
    logger.info(f"Writing Data to partition year={args.year}/month={args.month}")

    df_final_write.coalesce(1).write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)

    logger.info("Job Finished Successfully.")
    spark.stop()

if __name__ == "__main__":
    main()