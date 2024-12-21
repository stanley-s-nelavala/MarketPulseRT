# Created by Stanley Sujith Nelavala
--------------------------------------------------------------------------------
import json
from kafka import KafkaConsumer
from s3fs import S3FileSystem

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'stock_market'
S3_BUCKET = os.getenv('S3_BUCKET_NAME', 'my-stock-data-pipeline')  # Ensure this matches your actual S3 bucket name

# Initialize Kafka Consumer
try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
    )
    print("✅ Kafka Consumer connected successfully.")

except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    exit(1)

# Initialize S3 File System
s3 = S3FileSystem()

print("🔄 Kafka Consumer is running and listening for messages...")

try:
    file_path = f"s3://{S3_BUCKET}/stock_market.jsonl"
    
    with s3.open(file_path, 'a') as file:  
        for message in consumer:
            if message and message.value:
                try:
                    # Convert bytes to string
                    raw_message = message.value.decode('utf-8').strip()
                    if not raw_message:
                        print("⚠️ Skipping empty message...")
                        continue
                    
                    # Decode JSON
                    decoded_message = json.loads(raw_message)
                    
                    # **Write message as a JSONL record**
                    file.write(json.dumps(decoded_message) + "\n")

                    print(f"✅ Saved message to S3: {decoded_message}")

                except json.JSONDecodeError as e:
                    print(f"⚠️ JSON decoding error: {e}, skipping message...")

except KeyboardInterrupt:
    print("\n🛑 Kafka Consumer stopped by user.")

finally:
    consumer.close()
    print("🔒 Kafka Consumer closed successfully.")