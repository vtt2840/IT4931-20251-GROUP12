import os
import time
import json
import requests
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

from hdfs import InsecureClient
from dotenv import load_dotenv

load_dotenv()

# CONFIG
CITY = os.getenv("CITY")
API_KEY = os.getenv("API_KEY")

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("TOPIC")
WIND_TOPIC = os.getenv("WIND_TOPIC")
TEMP_TOPIC = os.getenv("TEMP_TOPIC")
HUM_TOPIC = os.getenv("HUM_TOPIC")

HDFS_URL = os.getenv("HDFS_URL_WEB")
HDFS_PATH = os.getenv("HDFS_PATH")

# WAIT FOR KAFKA
def wait_for_kafka(broker, retry_sec=5):
    while True:
        try:
            print("Waiting for Kafka...")
            admin = KafkaAdminClient(bootstrap_servers=broker)
            print("Kafka is ready")
            return admin
        except NoBrokersAvailable:
            time.sleep(retry_sec)

topics = [
    NewTopic(name=TOPIC, num_partitions=3, replication_factor=1),
    NewTopic(name=WIND_TOPIC, num_partitions=3, replication_factor=1),
    NewTopic(name=TEMP_TOPIC, num_partitions=3, replication_factor=1),
    NewTopic(name=HUM_TOPIC, num_partitions=3, replication_factor=1),
]

admin = wait_for_kafka(KAFKA_BROKER)

try:
    admin.create_topics(new_topics=topics)
    print("Kafka topics created")
except TopicAlreadyExistsError:
    print("Some topics already exist")
finally:
    admin.close()

# KAFKA PRODUCER
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=500,
    request_timeout_ms=30000,
    retry_backoff_ms=5000
)

print("Connected to Kafka producer")

# =======================
# HDFS
# =======================
hdfs_client = InsecureClient(HDFS_URL, user="hadoop")
print("Connected to HDFS")

# BUFFERS (BATCH BY HOUR)
AIR_BUFFER = []
WIND_BUFFER = []
TEMP_BUFFER = []
HUM_BUFFER = []

LAST_HOUR_SAVED = datetime.now(timezone.utc).hour

# FETCH AIR QUALITY
def fetch_latest_data():
    url = f"https://api.weatherbit.io/v2.0/current/airquality?city={CITY}&key={API_KEY}"
    response = requests.get(url, timeout=10)
    data = response.json()
    print("AIR RAW:", data)

    if "data" not in data:
        return None

    record = data["data"][0]
    now_utc = datetime.now(timezone.utc)
    now_local = datetime.now()

    return {
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

# FETCH WEATHER
def fetch_weather_data():
    url = f"https://api.weatherbit.io/v2.0/current?city={CITY}&key={API_KEY}"
    response = requests.get(url, timeout=10)
    data = response.json()
    print("WEATHER RAW:", data)

    if "data" not in data:
        return None

    record = data["data"][0]
    now_utc = datetime.now(timezone.utc)

    base = {
        "city": CITY,
        "timestamp_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%S"),
        "ts": int(now_utc.timestamp())
    }

    return (
        {**base, "wind_speed": record["wind_spd"]},
        {**base, "temperature": record["temp"]},
        {**base, "humidity": record["rh"]},
    )

# HDFS WRITE
def write_buffer_to_hdfs(buffer, sub_dir, hour_to_save):
    if not buffer:
        return

    date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    hour_str = str(hour_to_save).zfill(2)

    hdfs_dir = os.path.join(HDFS_PATH,"collect-data",sub_dir, date_str, hour_str)

    try:
        hdfs_client.makedirs(hdfs_dir, permission=755)

        file_path = os.path.join(
            hdfs_dir, f"{sub_dir}_batch_{int(time.time())}.json"
        )

        with hdfs_client.write(file_path, encoding="utf-8") as writer:
            for r in buffer:
                writer.write(json.dumps(r, ensure_ascii=False) + "\n")

        print(f"[HDFS] Saved {len(buffer)} records → {file_path}")
        buffer.clear()

    except Exception as e:
        print(f"[HDFS ERROR] {sub_dir}: {e}")

# SEND KAFKA
def send_to_kafka(topic, record):
    producer.send(topic, value=record)

if __name__ == "__main__":
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            current_hour = now_utc.hour

            # Save batch when hour changes
            if current_hour != LAST_HOUR_SAVED:

                success = True
                try: 

                    print(f"--- Saving batch hour {LAST_HOUR_SAVED} ---")

                    write_buffer_to_hdfs(AIR_BUFFER, "air_quality", LAST_HOUR_SAVED)
                    write_buffer_to_hdfs(WIND_BUFFER, "weather_wind", LAST_HOUR_SAVED)
                    write_buffer_to_hdfs(TEMP_BUFFER, "weather_temperature", LAST_HOUR_SAVED)
                    write_buffer_to_hdfs(HUM_BUFFER, "weather_humidity", LAST_HOUR_SAVED)
                except: 
                    success = False
                if success:
                   LAST_HOUR_SAVED = current_hour
                else:
                   print("HDFS write failed, will retry next loop...")
            # Air quality
            air_data = fetch_latest_data()
            if air_data:
                send_to_kafka(TOPIC, air_data)
                AIR_BUFFER.append(air_data)

            # Weather
            weather = fetch_weather_data()
            if weather:
                wind_doc, temp_doc, hum_doc = weather

                send_to_kafka(WIND_TOPIC, wind_doc)
                send_to_kafka(TEMP_TOPIC, temp_doc)
                send_to_kafka(HUM_TOPIC, hum_doc)

                WIND_BUFFER.append(wind_doc)
                TEMP_BUFFER.append(temp_doc)
                HUM_BUFFER.append(hum_doc)

            print(
                f"AIR:{len(AIR_BUFFER)} | "
                f"WIND:{len(WIND_BUFFER)} | "
                f"TEMP:{len(TEMP_BUFFER)} | "
                f"HUM:{len(HUM_BUFFER)}"
            )

        except Exception as e:
            print("Error:", e)

        time.sleep(60)