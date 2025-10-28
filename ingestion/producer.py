import os
import time
import json
import requests
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from hdfs import InsecureClient
from dotenv import load_dotenv
from data_cleaner import clean_air_quality_record  


load_dotenv()


CITY = os.getenv("CITY")
API_KEY = os.getenv("API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("TOPIC")
HDFS_URL = os.getenv("HDFS_URL")
HDFS_PATH = os.getenv("HDFS_PATH")

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# KAFKA SETUP 
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)
logging.info(" Connected to Kafka successfully!")

# HDFS SETUP 
hdfs_client = InsecureClient(HDFS_URL, user="hadoop")


# FETCH DATA 
def fetch_latest_data(max_retries=3, delay=5):
    
    url = f"https://api.weatherbit.io/v2.0/current/airquality?city={CITY}&key={API_KEY}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if "data" not in data:
                    logging.warning("API không trả về trường 'data'")
                    return None

                raw_record = data["data"][0]
                clean_record = clean_air_quality_record(raw_record, CITY)
                if clean_record:
                    clean_record["source"] = "weatherbit"  
                return clean_record

            elif response.status_code in [429, 500, 502, 503]:
                logging.warning(f"API lỗi {response.status_code}, thử lại ({attempt+1}/{max_retries})...")
                time.sleep(delay)

            else:
                logging.error(f" API trả về mã lỗi {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Lỗi khi gọi API: {e}")
            time.sleep(delay)

    logging.error("Gọi API thất bại sau nhiều lần thử")
    return None



def save_to_hdfs(record):
    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime("%Y/%m/%d")
    hdfs_dir = os.path.join(HDFS_PATH, date_str)
    hdfs_client.makedirs(hdfs_dir)

    file_path = os.path.join(hdfs_dir, f"data_{int(time.time())}.json")
    with hdfs_client.write(file_path, encoding="utf-8") as writer:
        writer.write(json.dumps(record) + "\n")
    logging.info(f"💾 Saved record to HDFS: {file_path}")


# SEND TO KAFKA 
def send_to_kafka(record):
    producer.send(TOPIC, value=record)
    logging.info(f"📤 Sent data to Kafka topic '{TOPIC}'")


# MAIN LOOP 
if __name__ == "__main__":
    while True:
        try:
            data = fetch_latest_data()
            if data:
                send_to_kafka(data)
                save_to_hdfs(data)
                logging.info(f" Sent data for {CITY} at {data['timestamp_utc']}")
            else:
                logging.warning(" Không có dữ liệu hợp lệ để gửi")
        except Exception as e:
            logging.error(f" Lỗi trong vòng lặp chính: {e}")
        time.sleep(60)
