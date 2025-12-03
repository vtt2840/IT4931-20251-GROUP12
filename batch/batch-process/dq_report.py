from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, when, isnan, to_date, hour,
    avg, max as spark_max, min as spark_min, percentile_approx,
    lit, to_json, struct, stddev, from_utc_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.window import Window
import json
import sys
import math
from datetime import datetime
import os 



def main():
    os.environ["HADOOP_USER_NAME"] = "root"
    
    spark = SparkSession.builder.appName("dq-report").getOrCreate()
    
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger("DQ_REPORT_JOB")
    
    logger.info("=" * 50)
    logger.info(">>> STARTING DATA QUALITY REPORT JOB")
    logger.info("=" * 50)
    
    try:
        input_path = "hdfs://hadoop-namenode:9000/clean-data/air-quality"
        output_base = "hdfs://hadoop-namenode:9000/reports/data-quality"
        
        input_path_obj = spark._jvm.org.apache.hadoop.fs.Path(input_path)
        fs_input = input_path_obj.getFileSystem(spark._jsc.hadoopConfiguration())
        
        if not fs_input.exists(input_path_obj):
            logger.warn(f">>> INPUT PATH NOT FOUND: {input_path}")
            logger.warn(">>> Please run the 'clean_data' task first!")
            return True 

        logger.info(f">>> Reading cleaned data from: {input_path}")
        
        df = spark.read.parquet(input_path)
        
        df = df.withColumn("timestamp_local", from_utc_timestamp(col("timestamp_utc"), "Asia/Ho_Chi_Minh"))
        df = df.withColumn("date", to_date(col("timestamp_local")))
        
        total_records = df.count()
        logger.info(f">>> Total records processed: {total_records}")

        if total_records == 0:
            logger.warn(">>> Data is empty. Exiting.")
            return True

        logger.info(">>> Computing Quality Metrics...")
        
        check_cols = ["city", "aqi", "co", "no2", "o3", "pm10", "pm25", "so2"]
        null_exprs = [count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in check_cols]
        null_counts_row = df.select(null_exprs).collect()[0].asDict()
        null_report = {k: {"count": v, "ratio": round((v/total_records)*100, 2)} for k, v in null_counts_row.items()}

        duplicate_count = total_records - df.dropDuplicates(["city", "timestamp_utc"]).count()
        
        ranges = {"aqi": (0, 500), "pm25": (0, 500), "pm10": (0, 600), "co": (0, 10000), "no2": (0, 500)}
        range_violations = {}
        for c, (min_v, max_v) in ranges.items():
            vio = df.filter((col(c) < min_v) | (col(c) > max_v)).count()
            if vio > 0: range_violations[c] = {"count": vio}

        daily_stats = df.groupBy("city", "date").agg(
            count("*").alias("n_records"),
            avg("aqi").alias("aqi_avg"),
            stddev("aqi").alias("aqi_stddev"),
            spark_sum(when((col("aqi") < 0) | (col("aqi") > 500), 1).otherwise(0)).alias("aqi_outliers")
        )
        
        w = Window.partitionBy("city").orderBy("date").rowsBetween(-7, -1)
        daily_stats = daily_stats.withColumn("rolling_avg", avg("aqi_avg").over(w))
        spike_df = daily_stats.filter((col("rolling_avg").isNotNull()) & (col("aqi_avg") > col("rolling_avg") * 1.5))
        
        spikes = [row.asDict() for row in spike_df.select("city", "date", "aqi_avg", "rolling_avg").collect()]
        for s in spikes: s['date'] = str(s['date'])
        
        daily_details = [row.asDict() for row in daily_stats.select("city", "date", "n_records", "aqi_avg", "aqi_outliers").collect()]
        for d in daily_details: d['date'] = str(d['date'])

        correlations = {}
        pollutants = ["aqi", "pm25", "pm10", "co", "no2", "o3", "so2"]
        try:
            for i, a in enumerate(pollutants):
                correlations[a] = {}
                for b in pollutants[i+1:]:
                    val = df.stat.corr(a, b)
                    correlations[a][b] = round(val, 4) if val is not None else None
        except Exception: pass

        # --- WRITE REPORT ---
        today_str = datetime.now().strftime("%Y-%m-%d")
        report_data = {
            "meta": {"report_date": today_str, "generated_at": datetime.now().isoformat()},
            "summary": {
                "total_records": total_records,
                "duplicates": duplicate_count,
                "null_analysis": null_report,
                "range_violations": range_violations,
                "spikes_detected": len(spikes)
            },
            "correlations": correlations,
            "spikes_details": spikes,
            "daily_city_summary": daily_details 
        }

        y, m, d = today_str.split('-')
        output_path = f"{output_base}/{y}/{m}/{d}"
        
        logger.info(f">>> Writing Report to HDFS: {output_path}")
        
        output_path_obj = spark._jvm.org.apache.hadoop.fs.Path(output_path)
        
        fs = output_path_obj.getFileSystem(spark._jsc.hadoopConfiguration())
        
        if fs.exists(output_path_obj):
            fs.delete(output_path_obj, True)

        spark.sparkContext.parallelize([json.dumps(report_data, ensure_ascii=False)]).saveAsTextFile(output_path)
        
        logger.info(">>> DQ REPORT COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        return True

    except Exception as e:
        logger.error(f"!!! DQ JOB FAILED: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    if main(): sys.exit(0)
    else: sys.exit(1)