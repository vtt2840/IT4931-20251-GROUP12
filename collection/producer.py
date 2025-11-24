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
def require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise ValueError(f"Missing required env var: {key}")
    return value

CITY = require_env("CITY")
API_KEY = require_env("API_KEY")
KAFKA_BROKER = require_env("KAFKA_BROKER")
TOPIC = require_env("TOPIC")
HDFS_URL_WEB = require_env("HDFS_URL_WEB")
HDFS_PATH = require_env("HDFS_PATH")

print("KAFKA_BROKER =", KAFKA_BROKER)
print("HDFS_PATH =", HDFS_PATH)

# CREATE TOPIC
def create_topic():
    admin = None
    for _ in range(10):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            topic = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
            admin.create_topics(new_topics=[topic], validate_only=False)
            print(f"Created topic: {TOPIC}")
            return
        except TopicAlreadyExistsError:
            print(f"Topic {TOPIC} already exists.")
            return
        except Exception as e:
            print(f"Kafka not ready, retrying topic creation in 5s: {e}")
            time.sleep(5)
        finally:
            if admin:
                admin.close()

create_topic()

# CREATE PRODUCER

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            )
            print("Connected to Kafka successfully!")
            return producer
        except Exception as e:
            print("Kafka not ready, retrying producer in 5s:", e)
            time.sleep(5)

producer = create_producer()


# HDFS CLIENT

hdfs_client = InsecureClient(HDFS_URL_WEB, user='hadoop')

# Tạo thư mục gốc HDFS_PATH nếu chưa tồn tại, với quyền 777
def ensure_hdfs_root():
    try:
        if not hdfs_client.status(HDFS_PATH, strict=False):
            print(f"HDFS root {HDFS_PATH} not found. Creating...")
            hdfs_client.makedirs(HDFS_PATH)
            hdfs_client.set_permission(HDFS_PATH, permission=0o777)
            print(f"Created HDFS root {HDFS_PATH} with 777 permissions.")
    except Exception as e:
        print(f"Error ensuring HDFS root: {e}")

ensure_hdfs_root()

print("Connected to HDFS & Kafka — starting loop")

# FETCH DATA

def fetch_latest_data():
    url = f"https://api.weatherbit.io/v2.0/current/airquality?city={CITY}&key={API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "data" not in data or len(data["data"]) == 0:
            return None
        record = data["data"][0]
        now_utc = datetime.now(timezone.utc)
        now_local = datetime.now()
        return {
            "city": CITY,
            "aqi": record.get("aqi"),
            "co": record.get("co"),
            "no2": record.get("no2"),
            "o3": record.get("o3"),
            "pm10": record.get("pm10"),
            "pm25": record.get("pm25"),
            "so2": record.get("so2"),
            "timestamp_local": now_local.strftime("%Y-%m-%dT%H:%M:%S"),
            "timestamp_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%S"),
            "ts": int(now_utc.timestamp())
        }
    except Exception as e:
        print("Error fetching data:", e)
        return None


# SAVE TO HDFS

def save_to_hdfs(record):
    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime("%Y/%m/%d")
    hdfs_dir = os.path.join(HDFS_PATH, date_str)

    try:
        hdfs_client.makedirs(hdfs_dir)  # tạo thư mục con nếu chưa tồn tại
    except Exception as e:
        print(f"Error creating HDFS directory {hdfs_dir}: {e}")

    file_path = os.path.join(hdfs_dir, f"data_{int(time.time())}.json")
    try:
        with hdfs_client.write(file_path, encoding="utf-8") as writer:
            writer.write(json.dumps(record) + "\n")
    except Exception as e:
        print(f"Error writing to HDFS {file_path}: {e}")


# SEND TO KAFKA

def send_to_kafka(record):
    try:
        producer.send(TOPIC, value=record)
    except Exception as e:
        print("Error sending to Kafka:", e)


# MAIN LOOP

if __name__ == "__main__":
    while True:
        try:
            data = fetch_latest_data()
            if data:
                send_to_kafka(data)
                save_to_hdfs(data)
                print(f"Sent data: {data['timestamp_utc']}")
        except Exception as e:
            print("Error in main loop:", e)
        time.sleep(60)