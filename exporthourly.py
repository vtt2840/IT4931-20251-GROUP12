import os
import json
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from hdfs import InsecureClient

import requests
from requests.adapters import HTTPAdapter

# 1. Tạo một Adapter để "ép" mọi request gửi tới hostname lạ về localhost
class LocalhostAdapter(HTTPAdapter):
    def send(self, request, **kwargs):
        # Kiểm tra nếu URL chứa hostname lạ (a5d2dad14b06) hoặc bất kỳ host nào không phải localhost
        # Ta sẽ ép nó về localhost để Windows có thể hiểu được (qua port mapping của Docker)
        if "localhost" not in request.url and "127.0.0.1" not in request.url:
            import re
            # Thay thế phần host giữa http:// và :port/ bằng localhost
            request.url = re.sub(r'(https?://)[^/:]+(:[0-9]+)', r'\1localhost\2', request.url)
        return super().send(request, **kwargs)

# 2. Khởi tạo session và gắn adapter
session = requests.Session()
adapter = LocalhostAdapter()
session.mount("http://", adapter)
session.mount("https://", adapter)

# 3. Kết nối HDFS sử dụng session này
hdfs = InsecureClient(
    "http://localhost:9870",
    user="hadoop",
    session=session  # Truyền session đã tùy chỉnh vào đây
)
# =====================
# CONFIG
# =====================

HDFS_URL = "http://hadoop-namenode:9870"
HDFS_BASE = "/user/hadoop"

CITY = "Hanoi"

ELASTIC_CLOUD_ID = "7a5b36f0e18348e989b69bf84acc0b82:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRiNDNiODdjNmVjY2Q0MDc3YjU4MGMzZDYyMDg1Y2IwZCRlOTEzMjFhOWZmMjc0MjgzOWJiYjEzZjM5ZGZjMGIxZQ=="
ELASTIC_API_KEY = "N050Q1Bac0JPZUFuUlRCMFRMLXI6VHVIVlZvV0ZwcXdyN0N3TDh6MndHQQ=="

INDEX_NAME = "pm25_hourly_relationship"

# =====================
# CONNECT HDFS
# =====================

#hdfs = InsecureClient(HDFS_URL, user="hadoop")



# =====================
# CONNECT ELASTIC
# =====================

es = Elasticsearch(
    cloud_id=ELASTIC_CLOUD_ID,
    api_key=ELASTIC_API_KEY
)

assert es.ping(), "❌ Cannot connect to Elastic Cloud"
print("✅ Connected to Elastic Cloud")

# =====================
# READ JSON FROM HDFS
# =====================
def read_json_recursive(path):
    records = []
    for root, _, files in hdfs.walk(path):
        for f in files:
            if f.endswith(".json"):
                # Option A: Manual join to force forward slash
                file_path = f"{root}/{f}".replace("\\", "/") 
                
                # Option B: Use posixpath for cross-platform HDFS paths
                # import posixpath
                # file_path = posixpath.join(root, f)

                with hdfs.read(file_path, encoding='utf-8') as reader:
                    for line in reader:
                        # Safety check for empty lines
                        if line.strip():
                            records.append(json.loads(line))
    return pd.DataFrame(records)

print("📥 Reading data from HDFS...")

df_air = read_json_recursive(f"{HDFS_BASE}/air_quality")
df_temp = read_json_recursive(f"{HDFS_BASE}/weather_temperature")
df_hum  = read_json_recursive(f"{HDFS_BASE}/weather_humidity")
df_wind = read_json_recursive(f"{HDFS_BASE}/weather_wind")

print(f"AIR: {len(df_air)} | TEMP: {len(df_temp)} | HUM: {len(df_hum)} | WIND: {len(df_wind)}")

# =====================
# PREPARE TIME
# =====================

for df in [df_air, df_temp, df_hum, df_wind]:
    df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    df["date"] = df["ts"].dt.date
    df["hour"] = df["ts"].dt.hour

# =====================
# HOURLY AGGREGATION
# =====================

air_hourly = df_air.groupby(["date", "hour"]).agg(
    pm25_mean=("pm25", "mean")
).reset_index()

temp_hourly = df_temp.groupby(["date", "hour"]).agg(
    temperature_mean=("temperature", "mean")
).reset_index()

hum_hourly = df_hum.groupby(["date", "hour"]).agg(
    humidity_mean=("humidity", "mean")
).reset_index()

wind_hourly = df_wind.groupby(["date", "hour"]).agg(
    wind_speed_mean=("wind_speed", "mean")
).reset_index()

# =====================
# MERGE → RELATIONSHIP DATA
# =====================

df = air_hourly \
    .merge(temp_hourly, on=["date", "hour"], how="left") \
    .merge(hum_hourly,  on=["date", "hour"], how="left") \
    .merge(wind_hourly, on=["date", "hour"], how="left")

df["city"] = CITY
df["date"] = df["date"].astype(str)

print(f"✅ Hourly records after merge: {len(df)}")

# =====================
# PUSH TO ELASTIC
# =====================
# =====================
# PUSH TO ELASTIC (ĐÃ SỬA ĐỂ NHẤP NHÔ CHI TIẾT)
# =====================

print("📤 Sending data to Elastic Cloud...")

for _, row in df.iterrows():
    # TẠO TIMESTAMP KẾT HỢP NGÀY VÀ GIỜ
    # Thay vì chỉ có "2025-12-12", ta sẽ tạo "2025-12-12T01:00:00"
    full_timestamp = f"{row['date']}T{int(row['hour']):02d}:00:00"

    doc = {
        "city": row["city"],
        "date": full_timestamp, # Đổi thành timestamp đầy đủ ở đây
        "hour": int(row["hour"]),
        "pm25_mean": round(row["pm25_mean"], 2),
        "temperature_mean": round(row["temperature_mean"], 2) if pd.notna(row["temperature_mean"]) else None,
        "humidity_mean": round(row["humidity_mean"], 2) if pd.notna(row["humidity_mean"]) else None,
        "wind_speed_mean": round(row["wind_speed_mean"], 2) if pd.notna(row["wind_speed_mean"]) else None
    }

    es.index(index=INDEX_NAME, document=doc)

print(f"🎉 DONE! {len(df)} documents indexed with hourly timestamps.")