import sys
import os
import argparse
import logging
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, min as spark_min, 
    sum as spark_sum, count, round as spark_round, 
    lag, when, lit, first
)
from pyspark.sql.window import Window
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_ROOT = "hdfs://hadoop-namenode:9000"

def get_spark_session(job_name):
    os.environ["HADOOP_USER_NAME"] = "hadoop"
    return SparkSession.builder \
        .appName(job_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    # Daily thường chạy đè lại dữ liệu phân tích của cả năm/tháng để cập nhật xu hướng mới nhất
    args = parser.parse_args()

    if not args.year:
        args.year = datetime.datetime.now().year

    logger.info(f"--- STARTING DAILY ANALYTICAL JOB FOR YEAR {args.year} ---")
    spark = get_spark_session(f"Daily_Analytics_{args.year}")
    spark.sparkContext.setLogLevel("ERROR")

    input_path = f"{HDFS_ROOT}/user/hadoop/batch/hourly"
    
    try:
        df_hourly = spark.read.parquet(input_path).filter(col("year") == args.year)
    except Exception as e:
        logger.error(f"Data not found: {e}")
        return
    
    #Lưu DataFrame vào bộ nhớ (RAM) của Spark để tái sử dụng nhanh hơn.
    df_hourly.cache()

    # 2. ANALYSIS 1: DAILY TREND WITH GROWTH RATE (Window Functions)
    #  Vẽ biểu đồ đường PM2.5 theo ngày & Xem xu hướng tăng/giảm
    logger.info("Computing Daily Trends...")

    # Gom nhóm theo ngày
    daily_agg = df_hourly.groupBy("city", "year", "month", "day").agg(
        avg("pm25_mean").alias("day_pm25"),
        spark_max("pm25_max").alias("day_max_pm25"),
        avg("aqi_mean").alias("day_aqi"),
        avg("avg_temp").alias("day_temp"),
        avg("avg_wind").alias("day_wind"),
        # Đếm số giờ ô nhiễm trong ngày (PM2.5 > 50 là mốc xấu)
        count(when(col("pm25_mean") > 50, 1)).alias("polluted_hours")
    )

    # Sắp xếp theo ngày để so sánh với ngày hôm trước
    w_daily = Window.partitionBy("city").orderBy("year", "month", "day")

    daily_trend = daily_agg.withColumn(
        "prev_day_pm25", lag("day_pm25", 1).over(w_daily)
    ).withColumn(
        # Tính % thay đổi
        "pm25_growth_pct", 
        spark_round(((col("day_pm25") - col("prev_day_pm25")) / col("prev_day_pm25")) * 100, 2)
    ).withColumn(
        "trend_status",
        when(col("pm25_growth_pct") > 10, "WORSENING")
        .when(col("pm25_growth_pct") < -10, "IMPROVING")
        .otherwise("STABLE")
    )

    daily_trend.coalesce(1).write.mode("overwrite").parquet(f"{HDFS_ROOT}/user/hadoop/batch/daily/trend")

    # 3. ANALYSIS 2: HOURLY HEATMAP (Pivot & Unpivot)
    # Vẽ Heatmap [Trục dọc: Thứ 2-CN] x [Trục ngang: 0h-23h]
    # Giá trị ô: Trung bình PM2.5

    logger.info("Computing Heatmap Data (Pivot)...")

    # Pivot: Biến giờ (0..23) thành cột
    heatmap_df = df_hourly.groupBy("city", "day_of_week") \
        .pivot("local_hour", list(range(24))) \
        .agg(spark_round(avg("pm25_mean"), 1)) \
        .orderBy("city", "day_of_week")

    heatmap_df.coalesce(1).write.mode("overwrite").parquet(f"{HDFS_ROOT}/user/hadoop/batch/daily/heatmap_pm25")

    # 4. ANALYSIS 3: CORRELATION MATRIX (Advanced MLlib)
    # Xem PM2.5 tương quan thế nào với Temp, Hum, Wind
    
    logger.info("Computing Correlation Matrix...")

    cols_corr = ["pm25_mean", "pm10_mean", "aqi_mean", "avg_temp", "avg_hum", "avg_wind"]
    
    df_clean = df_hourly.select(cols_corr).na.drop()

    if not df_clean.rdd.isEmpty():
        # Gom các cột thành Vector
        assembler = VectorAssembler(inputCols=cols_corr, outputCol="features")
        df_vector = assembler.transform(df_clean)

        # Tính tương quan Pearson
        pearson_corr = Correlation.corr(df_vector, "features", "pearson").collect()[0][0]

        rows = pearson_corr.toArray().tolist()
        corr_data = []
        for i, row in enumerate(rows):
            corr_dict = {"variable": cols_corr[i]}
            for j, val in enumerate(row):
                corr_dict[cols_corr[j]] = float(val)
            corr_data.append(corr_dict)
        
        spark.createDataFrame(corr_data).coalesce(1).write.mode("overwrite").json(f"{HDFS_ROOT}/user/hadoop/batch/daily/correlation")

    logger.info("Daily Analytical Job Finished.")
    spark.stop()

if __name__ == "__main__":
    main()