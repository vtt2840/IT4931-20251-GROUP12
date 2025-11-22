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

# --- CẤU HÌNH ---
def require_env(key: str) -> str:
    value = os.getenv(key)
    if not value: raise ValueError(f"Missing env: {key}")
    return value

TOKEN = require_env("API_KEY") # Token WAQI
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "air_quality")
HDFS_URL = os.getenv("HDFS_URL", "http://hadoop-namenode:9870")
HDFS_PATH = os.getenv("HDFS_PATH", "/data/air_quality")
CITY = "Hanoi"

# URL API WAQI cho Hà Nội
API_URL = f"https://api.waqi.info/feed/hanoi/?token={TOKEN}"

print(f"🚀 STARTING WAQI PRODUCER FOR {CITY}")

# 1. TẠO TOPIC
def create_topic():
    admin = None
    for _ in range(10):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            topic = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
            admin.create_topics(new_topics=[topic], validate_only=False)
            print(f"✅ Topic created: {TOPIC}")
            return
        except TopicAlreadyExistsError:
            print(f"⚠️ Topic exists.")
            return
        except Exception:
            time.sleep(5)
        finally:
            if admin: admin.close()
create_topic()

# 2. KẾT NỐI PRODUCER
def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode("utf-8")
            )
        except:
            print("⏳ Connecting to Kafka...")
            time.sleep(5)
producer = create_producer()

# 3. KẾT NỐI HDFS
hdfs_client = InsecureClient(HDFS_URL, user='hadoop')
try:
    if not hdfs_client.status(HDFS_PATH, strict=False):
        hdfs_client.makedirs(HDFS_PATH)
except: pass

# 4. HÀM LẤY DỮ LIỆU TỪ WAQI
def fetch_data():
    try:
        print(f"🌍 Fetching WAQI Data...")
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data['status'] != 'ok':
            print(f"❌ WAQI Error: {data.get('data')}")
            return None

        # Lấy dữ liệu thô
        result = data['data']
        iaqi = result.get('iaqi', {})
        
        now_utc = datetime.now(timezone.utc)
        now_local = datetime.now()

        # --- QUAN TRỌNG: Mapping sang định dạng Spark cần ---
        record = {
            "city": "HANOI",
            "aqi": int(result.get('aqi', 0)),
            # Lấy an toàn các chỉ số, nếu thiếu thì gán 0
            "co": float(iaqi.get('co', {}).get('v', 0)),
            "no2": float(iaqi.get('no2', {}).get('v', 0)),
            "o3": float(iaqi.get('o3', {}).get('v', 0)),
            "pm10": float(iaqi.get('pm10', {}).get('v', 0)),
            "pm25": float(iaqi.get('pm25', {}).get('v', 0)),
            "so2": float(iaqi.get('so2', {}).get('v', 0)),
            "timestamp_local": now_local.strftime("%Y-%m-%dT%H:%M:%S"),
            "timestamp_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%S"),
            "source": "WAQI_Embassy"
        }
        return record
    except Exception as e:
        print(f"❌ Error fetching: {e}")
        return None

# 5. GHI HDFS
def save_to_hdfs(record):
    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime("%Y/%m/%d")
    hdfs_dir = os.path.join(HDFS_PATH, date_str)
    file_path = os.path.join(hdfs_dir, f"data_{int(time.time())}.json")
    try:
        try: hdfs_client.makedirs(hdfs_dir)
        except: pass
        with hdfs_client.write(file_path, encoding="utf-8") as writer:
            writer.write(json.dumps(record) + "\n")
    except Exception as e:
        print(f"⚠️ HDFS Error: {e}")

# 6. MAIN LOOP
if __name__ == "__main__":
    while True:
        data = fetch_data()
        if data:
            producer.send(TOPIC, value=data)
            save_to_hdfs(data)
            print(f"📤 Sent: AQI={data['aqi']} | PM2.5={data['pm25']} | Time={data['timestamp_utc']}")
        
        # WAQI update mỗi giờ, nhưng  gọi mỗi phút để demo
        time.sleep(60)