from kafka import KafkaConsumer
from hdfs import InsecureClient
import json

consumer = KafkaConsumer(
    'diemchuan-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = InsecureClient('http://localhost:9870', user='hadoop')
hdfs_path = '/user/hadoop/data_from_kafka.csv'

if not client.status(hdfs_path, strict=False):
    print("File chưa tồn tại, tạo mới:", hdfs_path)
    client.write(hdfs_path, data='', encoding='utf-8')

count = 0
for msg in consumer:
    record = msg.value
    line = ','.join(map(str, record.values())) + '\n'

    with client.write(hdfs_path, append=True, encoding='utf-8') as writer:
        writer.write(line)

    count += 1
    print(f"Đã ghi bản ghi {count} vào HDFS:", record)
