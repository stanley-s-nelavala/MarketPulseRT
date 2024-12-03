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
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None  # Prevents decoding empty messages
    )
    print("✅ Kafka Consumer connected successfully.")

except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    exit(1)

# Initialize S3 File System
s3 = S3FileSystem()

print("🔄 Kafka Consumer is running and listening for messages...")

try:
    for count, message in enumerate(consumer):
        if message and message.value:  # Ignore empty messages
            try:
                file_path = f"s3://{S3_BUCKET}/stock_market_{count}.json"

                # Save message to S3
                with s3.open(file_path, 'w') as file:
                    json.dump(message.value, file, indent=4)  # Pretty-print JSON

                print(f"✅ Saved message {count} to S3: {message.value}")

            except json.JSONDecodeError as e:
                print(f"⚠️ JSON decoding error: {e}, skipping message...")

        else:
            print("⚠️ Received empty or None message, skipping...")

except KeyboardInterrupt:
    print("\n🛑 Kafka Consumer stopped by user.")

finally:
    consumer.close()
    print("🔒 Kafka Consumer closed successfully.")# Refactor Kafka producer functions
# Update pipeline architecture diagram
# Fix consumer crash on empty messages
