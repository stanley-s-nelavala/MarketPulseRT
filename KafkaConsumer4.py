# Created by Stanley Sujith Nelavala
--------------------------------------------------------------------------------
import json
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'stock_market'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x  
)

print("✅ Kafka Consumer connected successfully.")
print("🔄 Listening for messages...")

try:
    for message in consumer:
        print(f"🛠 RAW MESSAGE RECEIVED: {message.value}")  # Print raw data
except KeyboardInterrupt:
    print("\n🛑 Kafka Consumer stopped by user.")
finally:
    consumer.close()
    print("🔒 Kafka Consumer closed successfully.")