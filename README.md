# Big Data Project - Hanoi Air Quality Analysis

## Kiến trúc tổng quát
- **collection**: Python producer gọi Weatherbit API, đẩy dữ liệu vào Kafka và lưu JSON thô vào HDFS theo ngày.
- **batch**: Các job PySpark (`cleaner`, `batch_hourly_aggregates`, `batch_daily_aggregates`, `dq_report`) đọc dữ liệu từ HDFS, làm sạch, tổng hợp và xuất báo cáo chất lượng dữ liệu.
- **hạ tầng**: Kafka + Zookeeper + Hadoop NameNode/DataNode được orchestration bằng `docker-compose.yml`.

## Chuẩn bị môi trường
1. Cài Docker + Docker Compose.
2. Tạo file `.env` ở thư mục gốc (tham khảo block dưới):
   ```
   CITY=Hanoi
   API_KEY=735aa4b584ba48b0b68dc312f2a272e4
   KAFKA_BROKER=kafka:9092
   TOPIC=air_quality
   HDFS_URL=http://hadoop-namenode:9870
   HDFS_PATH=/data/air_quality
   ```
   > Repo ignore `.env`, bạn có thể sao chép nội dung này vào `.env` thủ công.

## Chạy toàn bộ stack
```bash
docker-compose up -d --build zookeeper kafka hadoop-namenode hadoop-datanode collection
```
- Kiểm tra log collection: `docker logs -f collection` (phải thấy log `Sent data: ...`).
- Kiểm tra Kafka topic: dùng `kafka-console-consumer` trong container Kafka hoặc tool bất kỳ.
- Kiểm tra dữ liệu thô trên HDFS: 
  ```bash
  docker exec -it hadoop-namenode hdfs dfs -ls -R /data/air_quality | head
  ```

## Chạy batch pipeline
Chạy full pipeline một lần:
```bash
docker-compose up --build batch
```

Hoặc từng bước (phù hợp CI/CD):
```bash
docker-compose run --rm batch python cleaner.py
docker-compose run --rm batch python batch_hourly_aggregates.py
docker-compose run --rm batch python batch_daily_aggregates.py
docker-compose run --rm batch python dq_report.py
```

## Vị trí dữ liệu trên HDFS
- Raw JSON: `/data/air_quality/YYYY/MM/DD/*.json`
- Clean parquet: `/clean-data/air_quality/`
- Hourly aggregates: `/batch/air_quality/hourly/`
- Daily aggregates: `/batch/air_quality/daily/`
- Data quality report: `/reports/data-quality/YYYY/MM/DD/`

## Checklist chạy và kiểm tra từng giai đoạn
1. **Ingestion (collection → Kafka + raw HDFS)**
   ```bash
   docker logs --tail 5 collection
   docker exec hadoop-namenode hdfs dfs -ls /data/air_quality/2025/11/18
   docker exec hadoop-namenode hdfs dfs -cat /data/air_quality/2025/11/18/data_<epoch>.json
   ```
   > Kỳ vọng: log “Sent data …” mỗi ~60 s và trong HDFS có các file JSON ~200 B.

2. **Cleaner**
   ```bash
   docker-compose run --rm batch python cleaner.py
   docker exec hadoop-namenode hdfs dfs -ls -R /clean-data/air_quality
   ```
   > Thấy `_SUCCESS` và file parquet partition `year=<yyyy>/month=<mm>/day=<dd>`.

3. **Hourly aggregates**
   ```bash
   docker-compose run --rm batch python batch_hourly_aggregates.py
   docker exec hadoop-namenode hdfs dfs -ls -R /batch/air_quality/hourly
   ```

4. **Daily aggregates**
   ```bash
   docker-compose run --rm batch python batch_daily_aggregates.py
   docker exec hadoop-namenode hdfs dfs -ls -R /batch/air_quality/daily
   ```

5. **Data Quality report**
   ```bash
   docker-compose run --rm batch python dq_report.py
   docker exec hadoop-namenode hdfs dfs -ls -R /reports/data-quality/2025/11/18
   docker exec hadoop-namenode hdfs dfs -cat /reports/data-quality/2025/11/18/part-00011
   ```
   > Script tự xóa thư mục cũ trước khi ghi nên có thể chạy nhiều lần trong ngày.

## Dữ liệu mẫu (để test nhanh)
- **Raw ingestion JSON**
  ```json
  {
    "city": "Hanoi",
    "aqi": 29,
    "co": 1989,
    "no2": 41,
    "o3": 6,
    "pm10": 9,
    "pm25": 7,
    "so2": 11,
    "timestamp_local": "2025-11-18T11:58:06",
    "timestamp_utc": "2025-11-18T11:58:06",
    "ts": 1763467086
  }
  ```
- **Data Quality report output (rút gọn)**
  ```json
  {
    "report_date": "2025-11-18",
    "summary": {
      "total_records": 3,
      "duplicate_records": 0,
      "null_columns": { "city": { "null_count": 0 }, "...": "..." },
      "range_violations": { "aqi": { "violations": 0 }, "...": "..." },
      "spike_detections": 0
    },
    "daily_city_details": [
      {
        "city": "HANOI",
        "n_records": 3,
        "aqi_avg": 29.0,
        "status": "OK"
      }
    ]
  }
  ```
> Khi chạy test, đối chiếu kết quả thực tế với mẫu trên để xác nhận pipeline hoạt động đúng.

## Troubleshooting nhanh
- Kafka chưa sẵn sàng → producer retry 5s/lần (log `Kafka not ready`).
- HDFS chưa có thư mục → producer tự tạo và set quyền 777.
- Không có dữ liệu mới → kiểm tra API key hoặc quota Weatherbit.
