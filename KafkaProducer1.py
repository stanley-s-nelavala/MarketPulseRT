# Created by Stanley Sujith Nelavala
--------------------------------------------------------------------------------
import pandas as pd
from kafka import KafkaProducer
from time import sleep
import json

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'stock_market'

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Ensure JSON serialization
    )
    print("✅ Kafka Producer connected successfully.")

except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    exit(1)

# Load stock market data from CSV
try:
    df = pd.read_csv("stockmarket_data.csv")
    print(f"✅ Loaded {len(df)} records from CSV.")

except FileNotFoundError:
    print("❌ CSV file not found! Ensure 'stockmarket_data.csv' exists in the same directory.")
    exit(1)

except pd.errors.EmptyDataError:
    print("❌ CSV file is empty!")
    exit(1)

print("🔄 Starting Kafka Producer...")

try:
    while True:
        # Pick a random stock record
        stock_record = df.sample(1).to_dict(orient="records")[0]

        # Ensure there are no NaN/None values (convert to string)
        stock_record = {k: (v if pd.notna(v) else "NULL") for k, v in stock_record.items()}

        # Send data to Kafka topic
        producer.send(TOPIC_NAME, value=stock_record)
        print(f"📤 Sent: {json.dumps(stock_record)}")

        # Sleep for 3 seconds before sending the next data
        sleep(3)

except KeyboardInterrupt:
    print("\n🛑 Kafka Producer stopped by user.")

finally:
    producer.flush()  # Ensure all messages are sent
    producer.close()
    print("🔒 Kafka Producer closed successfully.")# Update README with architecture overview
