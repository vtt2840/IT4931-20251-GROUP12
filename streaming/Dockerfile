FROM apache/spark:3.5.3

# Chuyển sang user root để cài đặt pip
USER root

WORKDIR /app

# Copy file requirements và cài đặt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY . .

# Chuyển lại về user mặc định của Spark (thường là 185 hoặc spark)
# Nhưng để tránh lỗi quyền hạn khi ghi file logs/checkpoints,
# trong môi trường học tập có thể giữ root (không khuyến khích production)
# USER 185