import pandas as pd
import numpy as np
from datetime import datetime, timezone

def clean_air_quality_record(record, city_name):

    if record is None or not isinstance(record, dict):
        return None

    required_fields = ["aqi", "co", "no2", "o3", "pm10", "pm25", "so2"]
    for field in required_fields:
        if field not in record or record[field] is None:
            record[field] = np.nan 

    for field in required_fields:
        try:
            record[field] = float(record[field])
        except (ValueError, TypeError):
            record[field] = np.nan

    limits = {
        "aqi": (0, 500),
        "pm25": (0, 500),
        "pm10": (0, 600),
        "co": (0, 50),
        "no2": (0, 400),
        "so2": (0, 400),
        "o3": (0, 400),
    }

    for field, (min_val, max_val) in limits.items():
        val = record.get(field, np.nan)
        if pd.isna(val) or val < min_val or val > max_val:
            record[field] = np.nan 

    now_utc = datetime.now(timezone.utc)
    now_local = datetime.now()

    clean_record = {
        "city": city_name,
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

    valid_values = [v for k, v in clean_record.items() if k in required_fields and not pd.isna(v)]
    if len(valid_values) < len(required_fields) / 2:
        return None

    return clean_record
