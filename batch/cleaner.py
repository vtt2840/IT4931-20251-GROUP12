import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, year, month, dayofmonth
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField, TimestampType

def main():
    # Cấu hình HDFS User
    os.environ["HADOOP_USER_NAME"] = "root"
    
    spark = (
        SparkSession.builder
        .appName("Air Quality Cleaner")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Định nghĩa Schema
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("co", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("o3", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("so2", DoubleType(), True),
        StructField("timestamp_utc", TimestampType(), True) 
    ])

    raw_data_path = "hdfs://hadoop-namenode:9000/data/air-quality/*/*/*/*.json"
    clean_data_path = "hdfs://hadoop-namenode:9000/clean-data/air-quality"
    
    print(f"Đang đọc dữ liệu từ: {raw_data_path}")
    
    try:
        # Đọc dữ liệu
        df_raw = spark.read.schema(schema).json(raw_data_path, multiLine=True)
        
        # Kiểm tra nếu không có dữ liệu
        if df_raw.rdd.isEmpty():
            print("File rỗng hoặc không tìm thấy bản ghi nào.")
            spark.stop()
            sys.exit(1) # Trả về exit code 1 để Airflow biết là Fail và có thể Retry
            
    except Exception as e:
        print(f"Lỗi khi đọc dữ liệu: {e}")
        spark.stop()
        sys.exit(1) # Báo lỗi để Airflow biết

    # --- XỬ LÝ DỮ LIỆU ---
    print(f"Số bản ghi ban đầu: {df_raw.count()}")

    df_clean = (
        df_raw
        .dropDuplicates()
        .na.drop(subset=["city", "aqi"])  
        .withColumn("city", trim(upper(col("city"))))
        .filter((col("aqi") >= 0) & (col("aqi") <= 500))
    )

    df_final = df_clean.withColumn("year", year(col("timestamp_utc"))) \
                   .withColumn("month", month(col("timestamp_utc"))) \
                   .withColumn("day", dayofmonth(col("timestamp_utc")))

    cleaned_count = df_clean.count()
    print(f" Số bản ghi sau khi làm sạch: {cleaned_count}")
    
    if cleaned_count == 0:
        print("Không có bản ghi hợp lệ. Kết thúc job.")
        spark.stop()
        return

    # Ghi dữ liệu
    print(f"Đang ghi dữ liệu vào: {clean_data_path}")
    try:
        (
            df_final
            .coalesce(1)
            .write
            .mode("append")
            .partitionBy("year", "month", "day")
            .parquet(clean_data_path)
        )
        print("Job completed successfully!")
    except Exception as e:
        print(f"Lỗi khi ghi dữ liệu: {e}")
        sys.exit(1) # Báo lỗi để Airflow biết
    spark.stop()

if __name__ == "__main__":
    main()