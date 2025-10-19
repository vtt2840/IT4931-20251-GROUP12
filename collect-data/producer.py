from kafka import KafkaProducer
import pandas as pd
import time
import json
import os

# Đọc file CSV từ thư mục data (nằm bên ngoài collect-data)
csv_path = os.path.join(os.path.dirname(__file__), '../data/diemchuan.csv')
df = pd.read_csv(csv_path)

# Cấu hình Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# Gửi từng dòng dữ liệu lên topic
for _, row in df.iterrows():
    data = row.to_dict()
    producer.send('diemchuan-topic', value=data)
    print("Sent:", data)
    time.sleep(0.01)

producer.flush()
producer.close()
