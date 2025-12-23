# Hanoi Air Quality – Big Data Analysis (Group 12)

Dự án xây dựng **hệ thống pipeline xử lý dữ liệu lớn** nhằm theo dõi và phân tích **chất lượng không khí tại Hà Nội**.
---

## Kiến trúc hệ thống & Công nghệ

### Data Ingestion
- **Python Producer** thu thập dữ liệu từ **Weatherbit API**
- Đẩy dữ liệu vào **Apache Kafka**
- Đồng thời lưu **JSON thô** vào **HDFS**

### Storage
- **HDFS**
  - Lưu trữ Data Lake
  - Bao gồm **Raw JSON** và **Cleaned Parquet**
- **Elasticsearch**
  - Lưu trữ dữ liệu sau xử lý
  - Phục vụ truy vấn tốc độ cao (Kibana)

### Processing
- **PySpark**
  - Làm sạch dữ liệu
  - Tổng hợp chỉ số AQI
  - Kiểm tra và đánh giá **Data Quality**

### Orchestration
- **Apache Airflow**
  - Điều phối **Batch Processing**
  - Tự động kích hoạt các Spark Job theo lịch

---

## Cấu trúc thư mục chính

```plaintext
.          
├── batch/              # Các script PySpark và định nghĩa các DAGs điều phối Batch Process
├── ingestion/          # Thu thập dữ liệu (API -> Kafka/HDFS)
├── streaming/          # Xử lý dữ liệu dòng từ Kafka
└── README.md
```

##  Hướng dẫn vận hành

### 1. Khởi động hạ tầng
Khởi chạy toàn bộ các service bằng Docker Compose:

```bash
docker compose up -d
```

### 2. Điều phối Batch Process (Airflow)

Truy cập giao diện Airflow: http://localhost:8080

Airflow sẽ tự động điều phối các bước:

Cleaner

Đọc dữ liệu thô từ HDFS

Làm sạch và lưu dưới dạng Parquet

Aggregation

Tổng hợp chỉ số AQI theo Giờ / Ngày

DQ Report

Kiểm tra lỗi dữ liệu

Xuất dữ liệu sau xử lý

Elasticsearch Indexing

Đẩy dữ liệu cuối cùng vào Elasticsearch

### Vị trí lưu trữ dữ liệu trong HDFS

Dữ liệu thô

```plaintext
/collect-data/air_quality/YYYY/MM/DD/HH
/collect-data/weather_humidity/YYYY/MM/DD/HH
/collect-data/weather_temperature/YYYY/MM/DD/HH
/collect-data/weather_wind/YYYY/MM/DD/HH
```

Dữ liệu làm sạch

```plaintext
/clean-data/air_quality/YYYY/MM/DD/HH
/clean-data/weather_humidity/YYYY/MM/DD/HH
/clean-data/weather_temperature/YYYY/MM/DD/HH
/clean-data/weather_wind/YYYY/MM/DD/HH
```

Dữ liệu sau phân tích batch

```plaintext
batch/hourly/YY/MM/DD
batch/dailyly/YY/MM/DD
```

### Real-time Processing (Streaming)
Thành phần này đảm nhận vai trò xử lý dữ liệu dòng (Stream Processing) từ Kafka, thực hiện các phép biến đổi phức tạp và đẩy kết quả phân tích tức thời lên Elasticsearch Cloud.

1. Luồng xử lý dữ liệu (Streaming Logic)
Dịch vụ streaming thực hiện chuỗi thao tác sau:


Tiêu thụ dữ liệu: Đọc luồng JSON từ Kafka topic air-quality.

Làm sạch & Chuẩn hóa:

Sử dụng Watermarking (10 phút) để xử lý dữ liệu đến trễ và giới hạn trạng thái lưu trữ.

Loại bỏ các bản ghi trùng lặp dựa trên city và timestamp_utc.

Lọc các giá trị AQI không hợp lệ (ngoài khoảng 0 - 1000).

Làm giàu dữ liệu (Enrichment): Sử dụng kỹ thuật Broadcast Join với dữ liệu tĩnh (tọa độ GPS, quốc gia) để tối ưu hiệu suất xử lý.

Phân loại & Đánh giá rủi ro:

Tự động phân cấp chỉ số AQI (Good, Moderate, Unhealthy,...).

Tính toán điểm rủi ro sức khỏe (aqi_risk_score) thông qua Spark UDF tùy chỉnh.

2. Các chỉ số tổng hợp (Aggregations)
Hệ thống chạy song song hai luồng phân tích với chu kỳ 120 giây (Trigger):

Main Aggregation: Tính toán trung bình (avg), độ lệch chuẩn (stddev), và phân vị 95 (p95) của các chỉ số AQI, PM2.5, PM10 theo cửa sổ 10 phút.

Pivot Analysis: Thống kê số lượng bản ghi theo từng phân cấp chất lượng không khí trong mỗi khung giờ.

3. Lưu trữ & Tối ưu hóa

Sink: Ghi dữ liệu vào Elasticsearch dưới dạng upsert thông qua foreachBatch để tránh trùng lặp dữ liệu.

Tối ưu hiệu suất:

Bật Adaptive Query Execution (AQE) để tự động điều chỉnh kế hoạch thực thi.

Cấu hình spark.sql.shuffle.partitions = 4 phù hợp với tài nguyên Docker.


Fault Tolerance: Sử dụng cơ chế Checkpointing lưu tại /app/checkpoints/aqi_streaming để đảm bảo hệ thống có thể phục hồi khi xảy ra sự cố.

### Thành viên thực hiện
Nguyễn Thị Thùy Linh (Trưởng nhóm)

Nguyễn Phương Thảo

Vũ Thu Trang

Ngô Minh Thu

Vũ Thúy Hằng
