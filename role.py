import requests
from requests.auth import HTTPBasicAuth
import json

# ==== Cấu hình ====
CLOUD_ENDPOINT = "https://8afd93090abf46f1a8f6d9617db51f44.us-central1.gcp.cloud.es.io:443"  # Thay bằng endpoint Cloud của bạn
ADMIN_USER = "elastic"       # Tài khoản admin của bạn
ADMIN_PASS = "4BxyuccS9xygfkwwP3jATqt9"  # Mật khẩu admin
ROLE_NAME = "streaming_writer_monitor"
API_KEY_NAME = "streaming-ingest-key"
INDEX_PATTERN = "streaming-data-*"

# ==== 1. Tạo Role với cluster monitor + index write ====
role_url = f"{CLOUD_ENDPOINT}/_security/role/{ROLE_NAME}"
role_payload = {
    "cluster": ["monitor"],
    "indices": [
        {
            "names": [INDEX_PATTERN],
            "privileges": ["write", "create", "create_index"]
        }
    ]
}

print("Creating role...")
r = requests.post(role_url,
                  auth=HTTPBasicAuth(ADMIN_USER, ADMIN_PASS),
                  headers={"Content-Type": "application/json"},
                  data=json.dumps(role_payload))
if r.status_code in [200, 201]:
    print(f"Role '{ROLE_NAME}' created successfully.")
else:
    print(f"Failed to create role: {r.status_code}, {r.text}")

# ==== 2. Tạo API Key gán role vừa tạo ====
api_key_url = f"{CLOUD_ENDPOINT}/_security/api_key"
api_key_payload = {
    "name": API_KEY_NAME,
    "role_descriptors": {
        ROLE_NAME: {
            "cluster": ["monitor"],
            "index": [
                {
                    "names": [INDEX_PATTERN],
                    "privileges": ["write", "create", "create_index"]
                }
            ]
        }
    }
}

print("Creating API key...")
r = requests.post(api_key_url,
                  auth=HTTPBasicAuth(ADMIN_USER, ADMIN_PASS),
                  headers={"Content-Type": "application/json"},
                  data=json.dumps(api_key_payload))

if r.status_code in [200, 201]:
    response_json = r.json()
    print("API Key created successfully!")
    print(f"API Key ID: {response_json['id']}")
    print(f"API Key: {response_json['api_key']}")
    print("\nUse this API key in Spark job as es.net.http.auth.pass")
else:
    print(f"Failed to create API key: {r.status_code}, {r.text}")
{
  "id": "i1Z6NJsBRxzi-7lbXqht",
  "name": "aqi",
  "expiration": 1771295993454,
  "api_key": "LBV5Qhem6bw-41k-eyfCfA",
  "encoded": "aTFaNk5Kc0JSeHppLTdsYlhxaHQ6TEJWNVFoZW02YnctNDFrLWV5ZkNmQQ==",
  "beats_logstash_format": "i1Z6NJsBRxzi-7lbXqht:LBV5Qhem6bw-41k-eyfCfA"
}