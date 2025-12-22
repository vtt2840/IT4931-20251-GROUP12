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
  - Chỉ dùng để điều phối **Batch Processing**
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
docker-compose up -d
```

2. Điều phối Batch Process (Airflow)

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

### Thành viên thực hiện 

Nguyễn Thị Thùy Linh (Trưởng nhóm)

Nguyễn Phương Thảo

Vũ Thu Trang

Ngô Thu Minh

Vũ Thúy Hằng