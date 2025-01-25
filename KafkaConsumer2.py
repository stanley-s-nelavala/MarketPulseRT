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
        if not message or not message.value:  
            print(f"⚠️ Received an empty message at index {count}, skipping...")
            continue  # Skip empty messages

        try:
            # Decode message safely
            decoded_message = message.value
            print(f"📥 Received message: {decoded_message}")

            # Save message to S3
            file_path = f"s3://{S3_BUCKET}/stock_market_{count}.json"
            with s3.open(file_path, 'w') as file:
                json.dump(decoded_message, file, indent=4)  # Pretty-print JSON

            print(f"✅ Saved message {count} to S3: {file_path}")

        except (json.JSONDecodeError, TypeError) as e:
            print(f"⚠️ JSON decoding error: {e}, skipping message...")

except KeyboardInterrupt:
    print("\n🛑 Kafka Consumer stopped by user.")

finally:
    consumer.close()
    print("🔒 Kafka Consumer closed successfully.")# Add error handling to Kafka producer
# Add environment variables support
# Add debug flags and CLI arguments
# Fix dead-letter topic config
# Update README with usage examples
