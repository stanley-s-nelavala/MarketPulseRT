# Created by Stanley Sujith Nelavala
--------------------------------------------------------------------------------
import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps

# Define Kafka broker (Change this to your EC2 Public IP)
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Load stock market data from CSV
df = pd.read_csv("stockmarket_data.csv")

print("Starting Kafka Producer...")

try:
    while True:
        # Pick a random stock record
        stock_record = df.sample(1).to_dict(orient="records")[0]
        
        # Send data to Kafka topic 'stock_market'
        producer.send('stock_market', value=stock_record)
        print(f"Sent: {stock_record}")

        # Sleep for 3 seconds before sending the next data
        sleep(3)

except KeyboardInterrupt:
    print("Stopping Producer...")

finally:
    producer.flush()  # Ensure all messages are sent
    producer.close()# Fix bug in message serialization
# Implement message retry mechanism
# Update LICENSE
# Enhance topic subscription logic
# Update schema structure in consumer
