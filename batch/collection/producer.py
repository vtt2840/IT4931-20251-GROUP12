import os
import time
import json
import requests
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from hdfs import InsecureClient
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
CITY = os.getenv("CITY")
API_KEY = os.getenv("API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("TOPIC")
WIND_TOPIC = os.getenv("WIND_TOPIC")
TEMP_TOPIC = os.getenv("TEMP_TOPIC")
HUM_TOPIC = os.getenv("HUM_TOPIC")
HDFS_URL = os.getenv("HDFS_URL_WEB")
HDFS_PATH = os.getenv("HDFS_PATH")

# --- KAFKA SETUP ---
def setup_kafka():
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            topics = [
                NewTopic(name=TOPIC, num_partitions=3, replication_factor=1),
                NewTopic(name=WIND_TOPIC, num_partitions=3, replication_factor=1),
                NewTopic(name=TEMP_TOPIC, num_partitions=3, replication_factor=1),
                NewTopic(name=HUM_TOPIC, num_partitions=3, replication_factor=1),
            ]
            admin.create_topics(new_topics=topics)
            admin.close()
            print("Kafka topics ready")
            break
        except TopicAlreadyExistsError:
            break
        except Exception as e:
            print(f"Waiting for Kafka: {e}")
            time.sleep(5)

    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        acks="all",
        retries=5
    )

# --- HDFS WORKER (CONSUMER) ---
def hdfs_sink_worker(topic_name, sub_dir):
    """Luồng tiêu thụ dữ liệu từ Kafka và ghi vào HDFS theo đúng cấu trúc cũ"""
    print(f"Worker started for topic: {topic_name} -> {sub_dir}")
    hdfs_client = InsecureClient(HDFS_URL, user="hadoop")
    
    while True:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id=f"hdfs_group_{sub_dir}", 
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            for message in consumer:
                record = message.value
                success = False
                
                while not success:
                    try:
                        now_utc = datetime.now(timezone.utc)
                        date_str = now_utc.strftime("%Y/%m/%d")
                        hour_str = now_utc.strftime("%H")
                        hdfs_dir = os.path.join(HDFS_PATH,"collect-data", sub_dir, date_str, hour_str)
                        
                        if not hdfs_client.status(hdfs_dir, strict=False):
                            hdfs_client.makedirs(hdfs_dir, permission=755)
                        
                        file_path = os.path.join(hdfs_dir, f"{sub_dir}_{int(time.time())}.json")
                        with hdfs_client.write(file_path, encoding="utf-8") as writer:
                            writer.write(json.dumps(record, ensure_ascii=False) + "\n")
                        
                        consumer.commit()
                        success = True
                    except Exception as e:
                        if "safe mode" in str(e).lower():
                            print(f" [HDFS SafeMode] {sub_dir} retrying in 15s...")
                        else:
                            print(f" [HDFS Error] {sub_dir}: {e}")
                        time.sleep(15)
        except Exception as e:
            print(f"Consumer {topic_name} error: {e}")
            time.sleep(5)

# --- DATA FETCHING ---
def fetch_latest_data():
    try:
        url = f"https://api.weatherbit.io/v2.0/current/airquality?city={CITY}&key={API_KEY}"
        r = requests.get(url, timeout=10)
        data = r.json()
        if "data" not in data: return None
        record = data["data"][0]
        return {
            "city": CITY, "aqi": record["aqi"], "co": record["co"], "no2": record["no2"],
            "o3": record["o3"], "pm10": record["pm10"], "pm25": record["pm25"], "so2": record["so2"],
            "timestamp_local": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "ts": int(time.time()),
        }
    except: return None

def fetch_weather_data():
    try:
        url = f"https://api.weatherbit.io/v2.0/current?city={CITY}&key={API_KEY}"
        r = requests.get(url, timeout=10)
        data = r.json()
        if "data" not in data: return None
        record = data["data"][0]
        base = {"city": CITY, "ts": int(time.time()), "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}
        return (
            {**base, "wind_speed": record["wind_spd"]},
            {**base, "temperature": record["temp"]},
            {**base, "humidity": record["rh"]},
        )
    except: return None

# --- MAIN ---
if __name__ == "__main__":
    setup_kafka()
    producer = setup_kafka() # Khởi tạo lại producer sau khi check topic

    # Danh sách mapping Topic Kafka -> Thư mục HDFS tương ứng
    workers = [
        (TOPIC, "air_quality"),
        (WIND_TOPIC, "weather_wind"),
        (TEMP_TOPIC, "weather_temperature"),
        (HUM_TOPIC, "weather_humidity")
    ]

    # Khởi chạy 4 luồng ghi HDFS
    for t_name, s_dir in workers:
        t = threading.Thread(target=hdfs_sink_worker, args=(t_name, s_dir), daemon=True)
        t.start()

    print("Main Loop: Fetching API and sending to Kafka...")
    while True:
        try:
            # Air
            air = fetch_latest_data()
            if air: producer.send(TOPIC, air)
            
            # Weather
            weather = fetch_weather_data()
            if weather:
                w_doc, t_doc, h_doc = weather
                producer.send(WIND_TOPIC, w_doc)
                producer.send(TEMP_TOPIC, t_doc)
                producer.send(HUM_TOPIC, h_doc)
            
            producer.flush()
            print(f" [Kafka] Data sent at {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"Main Error: {e}")
        
        time.sleep(60)