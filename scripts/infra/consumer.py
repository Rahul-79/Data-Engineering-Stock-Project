import json
import boto3
import time
import os
from kafka import KafkaConsumer


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9002")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password123")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# Initialize S3 Client (MinIO)
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

BUCKET_NAME = "bronze-transactions"

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    group_id="bronze-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"--- Consumer started, listening on {KAFKA_BOOTSTRAP_SERVERS} ---")

try:
    for message in consumer:
        record = message.value
        symbol = record.get('symbol', 'unknown')
        ts = record.get("fetched_at", int(time.time()))
        
       
        key = f"{symbol}/{ts}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(record),
            ContentType="application/json"
        )
        print(f"Saved record for {symbol} at {key}")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
