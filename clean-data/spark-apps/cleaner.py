from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper,year,month,dayofmonth
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField, TimestampType

def main():
    spark = (
        SparkSession.builder
        .appName("Air Quality Cleaner")
        .getOrCreate()
    )

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

    raw_data_path = "hdfs://hadoop-namenode:9000/data/air_quality/*/*/*/*.json"
    clean_data_path = "hdfs://hadoop-namenode:9000/clean-data/air_quality"

    df_raw= spark.read.schema(schema).json(raw_data_path,multiLine=True)
    df_raw.printSchema()

    initial_count = df_raw.count()
    print(f"📥 Số bản ghi ban đầu: {initial_count}")

    df_clean = (
        df_raw
        .dropDuplicates()
        .na.drop(subset=["city", "aqi"])  
        .withColumn("city", trim(upper(col("city"))))  # chuẩn hóa tên
        .filter((col("aqi") >= 0) & (col("aqi") <= 500))  # lọc giá trị hợp lệ
    )

    df_final = df_clean.withColumn("year", year(col("timestamp_utc"))) \
                   .withColumn("month", month(col("timestamp_utc"))) \
                   .withColumn("day", dayofmonth(col("timestamp_utc")))

    cleaned_count = df_clean.count()
    print(f"✅ Số bản ghi sau khi làm sạch: {cleaned_count}")
    print("Data cleaning and transformation complete.")

    # Ghi dữ liệu đã xử lý vào HDFS
    print(f"Writing cleaned data to: {clean_data_path}")
    df_final.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(clean_data_path)

    print("Job completed successfully!")
        
    spark.stop()

if __name__ == "__main__":
    main()
