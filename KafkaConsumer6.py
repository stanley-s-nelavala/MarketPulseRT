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
        value_deserializer=lambda x: x  
    )
    print("✅ Kafka Consumer connected successfully.")

except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    exit(1)

print("🔄 Kafka Consumer is running and listening for messages...")

try:
    for count, message in enumerate(consumer):
        if not message or not message.value:
            print(f"⚠️ Received an empty message at index {count}, skipping...")
            continue  # Skip empty messages

        try:
            # Print raw message before deserializing
            print(f"🛠 RAW MESSAGE RECEIVED ({count}): {message.value}")

            # Attempt decoding
            decoded_message = json.loads(message.value.decode('utf-8'))

            print(f"📥 Successfully Decoded Message {count}: {decoded_message}")

        except Exception as e:
            print(f"⚠️ JSON decoding error at index {count}: {e}")
            print(f"⚠️ Problematic raw message: {message.value}")

except KeyboardInterrupt:
    print("\n🛑 Kafka Consumer stopped by user.")

finally:
    consumer.close()
    print("🔒 Kafka Consumer closed successfully.")