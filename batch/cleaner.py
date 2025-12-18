import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, year, month, dayofmonth,
    to_timestamp
)
from pyspark.sql.types import (
    IntegerType, StringType, DoubleType,
    StructType, StructField, LongType
)

HDFS_ROOT = "hdfs://hadoop-namenode:9000/user/hadoop"

def get_spark_session():
    os.environ["HADOOP_USER_NAME"] = "hadoop"
    return (
        SparkSession.builder
        .appName("Clean Weather Data Hourly")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def clean_and_save(spark, data_name, schema, input_subpath, output_subpath, target_path_suffix, value_col=None, min_val=None, max_val=None):
    
    raw_path = f"{HDFS_ROOT}/collect-data/{input_subpath}/{target_path_suffix}/*.json"
    clean_path = f"{HDFS_ROOT}/clean-data/{output_subpath}"
    
    print(f"\n[{data_name.upper()}] Processing path: {raw_path}")

    try:
        df = spark.read.schema(schema).option("mode", "DROPMALFORMED").json(raw_path)
        if df.rdd.isEmpty():
            print("Không có dữ liệu để xử lý.")
            spark.stop()
            sys.exit(0)
    except Exception as e:
       print(f"Lỗi đọc dữ liệu: {e}")
       spark.stop()
       sys.exit(0)

    df = df.withColumn("timestamp_obj", to_timestamp(col("timestamp_utc"), "yyyy-MM-dd'T'HH:mm:ss"))
    
    subset = ["city", "timestamp_utc"]
    if value_col: subset.append(value_col)
    df_clean = df.na.drop(subset=subset)

    if "city" in df.columns:
        df_clean = df_clean.withColumn("city", trim(upper(col("city"))))

    df_clean = df_clean.dropDuplicates(["city", "ts"])

    if value_col and min_val is not None: df_clean = df_clean.filter(col(value_col) >= min_val)
    if value_col and max_val is not None: df_clean = df_clean.filter(col(value_col) <= max_val)

    df_final = (
        df_clean
        .withColumn("year", year(col("timestamp_obj")))
        .withColumn("month", month(col("timestamp_obj")))
        .withColumn("day", dayofmonth(col("timestamp_obj")))
        .drop("timestamp_obj")
    )

    count = df_final.count()
    print(f"   -> Valid records: {count}")

    if count == 0: return

    try:
        (
            df_final.coalesce(1).write.mode("append")
            .partitionBy("year", "month", "day")
            .parquet(clean_path)
        )
        print(f"   -> SUCCESS.")
    except Exception as e:
        print(f"   -> ERROR writing: {e}")

def main():
    # --- XỬ LÝ THAM SỐ TỪ AIRFLOW ---
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=str, help="Year (YYYY)")
    parser.add_argument("--month", type=str, help="Month (MM)")
    parser.add_argument("--day", type=str, help="Day (DD)")
    parser.add_argument("--hour", type=str, help="Hour (HH)")
    args = parser.parse_args()

    if args.year and args.month and args.day and args.hour:
        path_suffix = f"{args.year}/{args.month}/{args.day}/{args.hour}"
        print(f"Mode: HOURLY RUN -> {path_suffix}")
    else:
        path_suffix = "*/*/*/*"
        print("Mode: FULL SCAN (Manual Run)")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    schema_air = StructType([
        StructField("city", StringType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("co", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("o3", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("so2", DoubleType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("ts", LongType(), True)
    ])

    schema_wind = StructType([
        StructField("city", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("ts", LongType(), True)
    ])

    schema_temp = StructType([
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("ts", LongType(), True)
    ])

    schema_hum = StructType([
        StructField("city", StringType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("ts", LongType(), True)
    ])

    clean_and_save(spark, "Air", schema_air, "air_quality", "air_quality", path_suffix, "aqi", 0, 1000)
    clean_and_save(spark, "Wind", schema_wind, "weather_wind", "weather_wind", path_suffix, "wind_speed", 0, 200)
    clean_and_save(spark, "Temp", schema_temp, "weather_temperature", "weather_temperature", path_suffix, "temperature", -60, 60)
    clean_and_save(spark, "Hum", schema_hum, "weather_humidity", "weather_humidity", path_suffix, "humidity", 0, 100)

    spark.stop()

if __name__ == "__main__":
    main()