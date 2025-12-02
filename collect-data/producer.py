import os
import time
import json
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from hdfs import InsecureClient
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv()

# CONFIG
CITY = os.getenv("CITY")
API_KEY = os.getenv("API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("TOPIC")
HDFS_URL = os.getenv("HDFS_URL")
HDFS_PATH = os.getenv("HDFS_PATH")

try:
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    # Tăng Partitions và Replication Factor để tối ưu
    topic = NewTopic(name=TOPIC, num_partitions=3, replication_factor=3)
    admin.create_topics(new_topics=[topic], validate_only=False)
    print(f"Created topic: {TOPIC} with 3 partitions and 3 replicas.")
except TopicAlreadyExistsError:
    print(f"Topic {TOPIC} already exists. Please verify its configuration.")
except Exception as e:
    print(f"Error creating topic: {e}")
finally:
    if 'admin' in locals() and admin:
        admin.close()

# SETUP 
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    acks='all', 
    retries=5
)

hdfs_client = InsecureClient(HDFS_URL, user='hadoop')

print("Connect to kafka successfully")


# Config BATCH 
HDFS_BUFFER = []
LAST_HOUR_SAVED = datetime.now(timezone.utc).hour 

def fetch_latest_data():
    url = f"https://api.weatherbit.io/v2.0/current/airquality?city={CITY}&key={API_KEY}"
    response = requests.get(url)
    data = response.json()
    if "data" not in data:
        return None

    record = data["data"][0]
    now_utc = datetime.now(timezone.utc)
    now_local = datetime.now()

    doc = {
        "city": CITY,
        "aqi": record["aqi"],
        "co": record["co"],
        "no2": record["no2"],
        "o3": record["o3"],
        "pm10": record["pm10"],
        "pm25": record["pm25"],
        "so2": record["so2"],
        "timestamp_local": now_local.strftime("%Y-%m-%dT%H:%M:%S"),
        "timestamp_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%S"),
        "ts": int(now_utc.timestamp()),
    }
    return doc

def write_buffer_to_hdfs(hour_to_save):
    global HDFS_BUFFER
    
    if not HDFS_BUFFER:
        return

    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime("%Y/%m/%d")
    hour_str = str(hour_to_save).zfill(2) 
    
    hdfs_dir = os.path.join(HDFS_PATH, date_str, hour_str)
    
    try:
        hdfs_client.makedirs(hdfs_dir, permission=755)
        
        # Tên file sử dụng timestamp hiện tại
        file_name = f"airquality_batch_{int(time.time())}.json"
        file_path = os.path.join(hdfs_dir, file_name)
        
        # Gom tất cả bản ghi thành một chuỗi JSON Lines (mỗi dòng là một JSON object)
        data_to_write = "".join([json.dumps(r) + "\n" for r in HDFS_BUFFER])
        
        with hdfs_client.write(file_path, encoding="utf-8") as writer:
            writer.write(data_to_write)
            
        print(f"Batch saved: {len(HDFS_BUFFER)} records to HDFS: {file_path}")
        HDFS_BUFFER = [] # Xóa buffer sau khi ghi thành công
        
    except Exception as e:
        print(f"Error saving batch to HDFS: {e}")

def send_to_kafka(record):
    producer.send(TOPIC, value=record)

if __name__ == "__main__":
    while True:
        try:
            # Xử lý Batch
            now_utc = datetime.now(timezone.utc)
            current_hour = now_utc.hour
            
            # Kiểm tra nếu giờ hiện tại khác giờ đã lưu batch cuối cùng
            if current_hour != LAST_HOUR_SAVED:
                print(f"--- Triggering batch save: hour {LAST_HOUR_SAVED} -> {current_hour} ---")
                # Ghi dữ liệu của giờ đã qua 
                write_buffer_to_hdfs(LAST_HOUR_SAVED) 
                LAST_HOUR_SAVED = current_hour
            
            # Thu thập và Đưa dữ liệu vào HDFS Buffer và Kafka 
            data = fetch_latest_data()
            if data:
                # 1. Gửi lên Kafka 
                send_to_kafka(data)
                # 2. Thêm vào HDFS Buffer 
                HDFS_BUFFER.append(data)
                
                print(f"Sent data: {data['timestamp_utc']} | Buffer size: {len(HDFS_BUFFER)}")
            
        except Exception as e:
            print("Error:", e)
            
        # Tần suất lấy dữ liệu
        time.sleep(60)