import os
import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer
from hdfs import InsecureClient
import os
from dotenv import load_dotenv

load_dotenv()

# CONFIG
CITY = os.getenv("CITY")
API_KEY = os.getenv("API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("TOPIC")
HDFS_URL = os.getenv("HDFS_URL")
HDFS_PATH = os.getenv("HDFS_PATH")


# SETUP 
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)
print("✅ Connected to Kafka successfully!")
hdfs_client = InsecureClient(HDFS_URL, user='hadoop')

def fetch_latest_data():
    url = f"https://api.weatherbit.io/v2.0/current/airquality?city={CITY}&key={API_KEY}"
    response = requests.get(url)
    data = response.json()
    if "data" not in data:
        return None

    record = data["data"][0]
    now_utc = datetime.utcnow()
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


def save_to_hdfs(record):
    date_str = datetime.utcnow().strftime("%Y/%m/%d")
    hdfs_dir = os.path.join(HDFS_PATH, date_str)
    hdfs_client.makedirs(hdfs_dir)

    file_path = os.path.join(hdfs_dir, f"data_{int(time.time())}.json")
    with hdfs_client.write(file_path, encoding="utf-8") as writer:
        writer.write(json.dumps(record) + "\n")


def send_to_kafka(record):
    producer.send(TOPIC, value=record)


if __name__ == "__main__":
    while True:
        try:
            data = fetch_latest_data()
            if data:
                send_to_kafka(data)
                save_to_hdfs(data)
        except Exception as e:
            print("Error:", e)
        time.sleep(60) 
