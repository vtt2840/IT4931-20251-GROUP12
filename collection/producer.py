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

# --- CONFIG ---
def require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        print(f"WARNING: Missing env var {key}")
        return ""
    return value

CITY = require_env("CITY")
API_KEY = require_env("API_KEY")
KAFKA_BROKER = require_env("KAFKA_BROKER") or "kafka:9092"
TOPIC = require_env("TOPIC") or "air-quality"
HDFS_URL_WEB = require_env("HDFS_URL_WEB") or "http://hadoop-namenode:9870"
HDFS_PATH = require_env("HDFS_PATH") or "/data/air-quality"

print(f"Config: Broker={KAFKA_BROKER}, HDFS Web={HDFS_URL_WEB}")

# --- KAFKA TOPIC SETUP ---
def create_topic():
    admin = None
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        topic = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
        admin.create_topics(new_topics=[topic], validate_only=False)
        print(f"Created topic: {TOPIC}")
    except TopicAlreadyExistsError:
        print(f"Topic {TOPIC} exists.")
    except Exception as e:
        print(f"Kafka Admin Error: {e}")
    finally:
        if admin: admin.close()

create_topic()

# --- PRODUCER SETUP ---
producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        print("Kafka Producer Connected")
    except Exception as e:
        print(f"Waiting for Kafka... {e}")
        time.sleep(5)

# --- HDFS CLIENT ---
hdfs_client = InsecureClient(HDFS_URL_WEB, user='root')

def ensure_hdfs_root():
    try:
        if not hdfs_client.status(HDFS_PATH, strict=False):
            hdfs_client.makedirs(HDFS_PATH)
            hdfs_client.set_permission(HDFS_PATH, permission=0o777)
            print(f"Created HDFS path: {HDFS_PATH}")
    except Exception as e:
        print(f"HDFS Connection Warning: {e}")

ensure_hdfs_root()

from datetime import timedelta

# --- FETCH DATA ---
def fetch_latest_data():
    if not API_KEY: 
        return None
    url = f"https://api.weatherbit.io/v2.0/current/airquality?city={CITY}&key={API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200: 
            print(f"API Error: {response.status_code}")
            return None
        data = response.json()
        if "data" not in data or not data["data"]: 
            return None
        
        record = data["data"][0]
        now_utc = datetime.now(timezone.utc)
        now_hanoi = now_utc + timedelta(hours=7)  # Hà Nội UTC+7

        return {
            "city": CITY,
            "aqi": record.get("aqi"),
            "co": record.get("co"),
            "no2": record.get("no2"),
            "o3": record.get("o3"),
            "pm10": record.get("pm10"),
            "pm25": record.get("pm25"),
            "so2": record.get("so2"),
            "timestamp_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%S"),
            "timestamp_local": now_hanoi.strftime("%Y-%m-%dT%H:%M:%S"),
            "ts": int(now_utc.timestamp())
        }
    except Exception as e:
        print(f"Fetch Error: {e}")
        return None

# --- SAVE TO HDFS ---
def save_to_hdfs(record):
    try:
        now = datetime.now(timezone.utc)
        # Đường dẫn: /data/air-quality/2023/11/30/
        daily_path = f"{HDFS_PATH}/{now.year}/{now.month:02d}/{now.day:02d}"
        
        if not hdfs_client.status(daily_path, strict=False):
            hdfs_client.makedirs(daily_path)
            
        filename = f"data_{int(time.time())}.json"
        full_path = f"{daily_path}/{filename}"
        
        with hdfs_client.write(full_path, encoding='utf-8') as writer:
            writer.write(json.dumps(record) + "\n") # Thêm xuống dòng
            
        print(f"Saved to HDFS: {full_path}")
    except Exception as e:
        print(f"HDFS Write Error: {e}")

# --- MAIN LOOP ---
if __name__ == "__main__":
    print(f"Started. Fetching every 60s...")
    while True:
        try:
            data = fetch_latest_data()
            if data:
                # 1. Gửi Kafka
                producer.send(TOPIC, value=data)
                producer.flush()
                print(f"Sent to Kafka: AQI={data['aqi']}")
                
                # 2. Lưu HDFS
                save_to_hdfs(data)
        except Exception as e:
            print(f"Main loop error: {e}")
        
        time.sleep(60)