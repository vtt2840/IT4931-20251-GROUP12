import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, year, month, dayofmonth,
    to_timestamp
)
from pyspark.sql.types import (
    IntegerType, StringType, DoubleType,
    StructType, StructField, TimestampType, LongType
)

def main():
    os.environ["HADOOP_USER_NAME"] = "root"

    spark = (
        SparkSession.builder
        .appName("Air Quality Cleaner")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("city", StringType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("co", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("o3", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("so2", DoubleType(), True),
        StructField("timestamp_local", StringType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("ts", LongType(), True)
    ])

    raw_data_path = "hdfs://hadoop-namenode:9000/data/air-quality/*/*/*/*.json"
    clean_data_path = "hdfs://hadoop-namenode:9000/clean-data/air-quality"

    print(f"Đang đọc dữ liệu từ: {raw_data_path}")

    try:
        df_raw = spark.read.schema(schema).json(raw_data_path)
        
        if df_raw.rdd.isEmpty():
            print("Không có dữ liệu để xử lý.")
            spark.stop()
            sys.exit(1)
    except Exception as e:
        print(f"Lỗi đọc dữ liệu: {e}")
        spark.stop()
        sys.exit(1)

    print(f"Số bản ghi ban đầu: {df_raw.count()}")

    df_raw = df_raw.withColumn(
        "timestamp_utc",
        to_timestamp(col("timestamp_utc"), "yyyy-MM-dd'T'HH:mm:ss")
    )

    df_clean = (
        df_raw
        .dropDuplicates()
        .na.drop(subset=["city", "aqi", "timestamp_utc"])
        .withColumn("city", trim(upper(col("city"))))
        .filter((col("aqi") >= 0) & (col("aqi") <= 500))
    )

    df_final = (
        df_clean
        .withColumn("year", year(col("timestamp_utc")))
        .withColumn("month", month(col("timestamp_utc")))
        .withColumn("day", dayofmonth(col("timestamp_utc")))
    )

    cleaned_count = df_clean.count()
    print(f"Số bản ghi sau khi làm sạch: {cleaned_count}")

    if cleaned_count == 0:
        print("Không có bản ghi hợp lệ.")
        spark.stop()
        return

    print(f"Đang ghi dữ liệu vào: {clean_data_path}")

    try:
        (
            df_final
            .write
            .mode("append")
            .partitionBy("year", "month", "day")
            .parquet(clean_data_path)
        )
        print("Job completed successfully!")
    except Exception as e:
        print(f"Lỗi ghi dữ liệu: {e}")
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
