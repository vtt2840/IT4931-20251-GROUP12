from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, when, isnan, to_date, hour,
    avg, max as spark_max, min as spark_min, percentile_approx,
    row_number, lit, to_json, struct, stddev, dayofweek, sin, cos, year, month, dayofmonth
)
from pyspark.sql.window import Window
import json
import logging
import sys
import math
from datetime import datetime
import os 

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

def main():
    """
    Data Quality Report job:
    - Đọc dữ liệu từ /clean-data/air_quality (Parquet đã làm sạch)
    - Tính: null ratios, duplicate counts, range violations, spikes
    - Lưu báo cáo JSON vào /reports/data-quality/YYYY/MM/DD/
    - (Tùy chọn) Publish alerts to Kafka
    """
    os.environ["HADOOP_USER_NAME"] = "root"
    spark = (
        SparkSession.builder
        .appName("dq-report")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    
    logger.info("=" * 80)
    logger.info("Starting Data Quality Report Job")
    logger.info("=" * 80)
    
    try:
        # Paths
        input_path = "hdfs://hadoop-namenode:9000/clean-data/air-quality"
        output_base = "hdfs://hadoop-namenode:9000/reports/data-quality"
        
        logger.info(f"Reading cleaned data from: {input_path}")
        df = spark.read.parquet(input_path)
        
        total_records = df.count()
        logger.info(f"Total records: {total_records}")
        
        # ============ PREPARE DATE COLUMNS ============
        df = df.withColumn("date", to_date(col("timestamp_utc")))
        df = df.withColumn("hour", hour(col("timestamp_utc")))
        
        # Feature engineering: day_of_week, is_weekend, month sin/cos
        df = df.withColumn("day_of_week", dayofweek(col("timestamp_utc")))  # 1=Sunday, 7=Saturday
        df = df.withColumn("is_weekend", 
            ((col("day_of_week") == 1) | (col("day_of_week") == 7)).cast("int"))
        df = df.withColumn("month", month(col("timestamp_utc")))
        df = df.withColumn("month_sin", sin(2 * math.pi * col("month") / 12))
        df = df.withColumn("month_cos", cos(2 * math.pi * col("month") / 12))
        
        # ============ GLOBAL QUALITY METRICS ============
        logger.info("Computing global data quality metrics...")
        
        # Null counts per column
        null_report = {}
        for col_name in ["city", "aqi", "co", "no2", "o3", "pm10", "pm25", "so2", "timestamp_utc"]:
            null_count = df.filter(col(col_name).isNull()).count()
            null_ratio = (null_count / total_records * 100) if total_records > 0 else 0
            null_report[col_name] = {
                "null_count": int(null_count),
                "null_ratio_percent": round(null_ratio, 2)
            }
        
        logger.info("Null counts:")
        for col_name, stats in null_report.items():
            logger.info(f"  {col_name}: {stats['null_count']} nulls ({stats['null_ratio_percent']}%)")
        
        # ============ DUPLICATE ANALYSIS ============
        logger.info("Analyzing duplicates...")
        unique_records = df.dropDuplicates(["city", "timestamp_utc"]).count()
        duplicate_count = total_records - unique_records
        duplicate_ratio = (duplicate_count / total_records * 100) if total_records > 0 else 0
        
        logger.info(f"Total duplicates: {duplicate_count} ({duplicate_ratio:.2f}%)")
        
        # ============ RANGE VALIDATION ============
        logger.info("Validating value ranges...")
        
        # Define valid ranges
        ranges = {
            "aqi": (0, 500),
            "pm25": (0, 500),
            "pm10": (0, 600),
            "co": (0, 10000),
            "no2": (0, 500),
            "o3": (0, 500),
            "so2": (0, 500)
        }
        
        range_violations = {}
        for col_name, (min_val, max_val) in ranges.items():
            violation_count = df.filter(
                (col(col_name) < min_val) | (col(col_name) > max_val)
            ).count()
            violation_ratio = (violation_count / total_records * 100) if total_records > 0 else 0
            range_violations[col_name] = {
                "range": f"[{min_val}, {max_val}]",
                "violations": int(violation_count),
                "violation_ratio_percent": round(violation_ratio, 2)
            }
            if violation_count > 0:
                logger.warning(f"  {col_name}: {violation_count} violations ({violation_ratio:.2f}%)")
        
        # ============ SPIKE DETECTION (PER CITY/DATE) ============
        logger.info("Detecting spikes by city/date...")
        
        # Calculate daily stats per city
        daily_stats = (
            df.groupBy("city", "date")
              .agg(
                  count("*").alias("n_records"),
                  avg("aqi").alias("aqi_avg"),
                  percentile_approx("aqi", 0.5).alias("aqi_p50")
              )
        )
        
        # Calculate 7-day rolling average
        window_7d = Window.partitionBy("city").orderBy("date").rowsBetween(-7, 0)
        daily_stats = daily_stats.withColumn(
            "aqi_rolling_avg_7d",
            avg("aqi_avg").over(window_7d)
        )
        
        # Flag spikes (>50% increase from rolling average)
        daily_stats = daily_stats.withColumn(
            "is_spike",
            when(
                (col("aqi_avg") > col("aqi_rolling_avg_7d") * 1.5) & (col("aqi_rolling_avg_7d").isNotNull()),
                1
            ).otherwise(0)
        )
        
        spike_records = daily_stats.filter(col("is_spike") == 1).collect()
        spike_count = len(spike_records)
        logger.info(f"Spike detections (>50% increase): {spike_count}")
        if spike_count > 0:
            logger.warning("Spike details:")
            for row in spike_records[:5]:  # Show first 5
                logger.warning(
                    f"  City={row['city']}, Date={row['date']}, "
                    f"Avg_AQI={row['aqi_avg']:.2f}, Rolling_Avg={row['aqi_rolling_avg_7d']:.2f}"
                )
        
        # ============ DAILY CITY-LEVEL REPORT ============
        logger.info("Generating daily city-level data quality report...")
        
        daily_dq = (
            df.groupBy("city", "date")
              .agg(
                  count("*").alias("n_records"),
                  count(when(col("aqi").isNull(), 1)).alias("aqi_nulls"),
                  count(when(col("pm25").isNull(), 1)).alias("pm25_nulls"),
                  count(when(col("pm10").isNull(), 1)).alias("pm10_nulls"),
                  
                  # Range violations per day/city
                  spark_sum(when((col("aqi") < 0) | (col("aqi") > 500), 1).otherwise(0)).alias("aqi_out_of_range"),
                  spark_sum(when((col("pm25") < 0) | (col("pm25") > 500), 1).otherwise(0)).alias("pm25_out_of_range"),
                  spark_sum(when((col("pm10") < 0) | (col("pm10") > 600), 1).otherwise(0)).alias("pm10_out_of_range"),
                  
                  # Stats
                  avg("aqi").alias("aqi_avg"),
                  stddev("aqi").alias("aqi_stddev"),
                  percentile_approx("aqi", 0.5).alias("aqi_p50"),
              )
        )
        
        daily_dq_rows = daily_dq.collect()
        logger.info(f"Daily city-level DQ records: {len(daily_dq_rows)}")
        
        # ============ GENERATE REPORT JSON ============
        logger.info("Generating report JSON...")
        
        # Get current date from data
        current_date = df.select(to_date(col("timestamp_utc")).alias("date")).distinct().collect()
        report_date = current_date[0]['date'].isoformat() if current_date else datetime.now().strftime("%Y-%m-%d")
        
        # Build report structure
        report = {
            "report_date": report_date,
            "report_timestamp": datetime.now().isoformat(),
            "summary": {
                "total_records": int(total_records),
                "unique_records": int(unique_records),
                "duplicate_records": int(duplicate_count),
                "duplicate_ratio_percent": round(duplicate_ratio, 2),
                "null_columns": null_report,
                "range_violations": range_violations,
                "spike_detections": spike_count
            },
            "daily_city_details": []
        }
        
        # Add daily city details
        for row in daily_dq_rows:
            detail = {
                "city": row['city'],
                "date": str(row['date']),
                "n_records": int(row['n_records']),
                "aqi_nulls": int(row['aqi_nulls']),
                "pm25_nulls": int(row['pm25_nulls']),
                "pm10_nulls": int(row['pm10_nulls']),
                "aqi_out_of_range": int(row['aqi_out_of_range']),
                "pm25_out_of_range": int(row['pm25_out_of_range']),
                "pm10_out_of_range": int(row['pm10_out_of_range']),
                "aqi_avg": round(row['aqi_avg'], 2) if row['aqi_avg'] else None,
                "aqi_stddev": round(row['aqi_stddev'], 2) if row['aqi_stddev'] else None,
                "aqi_p50": round(row['aqi_p50'], 2) if row['aqi_p50'] else None,
                "status": "WARNING" if (row['aqi_out_of_range'] > 0 or row['aqi_nulls'] > 0) else "OK"
            }
            report["daily_city_details"].append(detail)
        
        # Add spike details
        report["spikes"] = []
        for row in spike_records:
            spike_detail = {
                "city": row['city'],
                "date": str(row['date']),
                "aqi_avg": round(row['aqi_avg'], 2),
                "rolling_avg_7d": round(row['aqi_rolling_avg_7d'], 2),
                "increase_percent": round(
                    ((row['aqi_avg'] - row['aqi_rolling_avg_7d']) / row['aqi_rolling_avg_7d'] * 100),
                    2
                ) if row['aqi_rolling_avg_7d'] else 0
            }
            report["spikes"].append(spike_detail)

        # ============ CORRELATION MATRIX (PAIRWISE Pearson) ============
        logger.info("Computing pairwise Pearson correlations between pollutants...")
        pollutants = ["aqi", "pm25", "pm10", "co", "no2", "o3", "so2"]
        correlations = {}
        for i, a in enumerate(pollutants):
            correlations[a] = {}
            for b in pollutants[i+1:]:
                try:
                    corr_val = df.stat.corr(a, b)
                except Exception:
                    corr_val = None
                correlations[a][b] = None if corr_val is None else round(corr_val, 4)

        report["correlations"] = correlations
        
        # ============ WRITE REPORT ============
        # Determine output path based on report date
        year_str, month_str, day_str = report_date.split('-')
        output_path = f"{output_base}/{year_str}/{month_str}/{day_str}"
        
        logger.info(f"Writing report to: {output_path}/dq_report.json")

        # Remove existing folder for idempotent writes
        hadoop_conf = spark._jsc.hadoopConfiguration()
        output_path_obj = spark._jvm.org.apache.hadoop.fs.Path(output_path)
        fs = output_path_obj.getFileSystem(hadoop_conf)
        if fs.exists(output_path_obj):
            logger.info("Existing report folder found — deleting before overwrite")
            fs.delete(output_path_obj, True)
        
        # Write JSON report to HDFS
        report_json_str = json.dumps(report, indent=2, ensure_ascii=False)
        
        # Create RDD from report and save as text
        report_rdd = spark.sparkContext.parallelize([report_json_str])
        report_rdd.saveAsTextFile(output_path)
        
        logger.info("✅ Data quality report written successfully")
        
        # Also save to console for immediate visibility
        logger.info("=" * 80)
        logger.info("DATA QUALITY REPORT (JSON)")
        logger.info("=" * 80)
        logger.info(report_json_str)
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in DQ report job: {e}", exc_info=True)
        return False
        
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
