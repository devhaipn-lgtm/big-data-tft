
import json
import os
import time
from kafka import KafkaProducer

# --- CONFIG ---
# This must match the topic your Spark/HDFS scripts are listening to
KAFKA_TOPIC = 'tft-match-stream'

# The folder where your extracted JSONs are sitting
# (Change this if you saved them somewhere else)
DATA_DIR = 'data_factory/real'

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    # This handles the JSON serialization automatically
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


def feed_kafka():
    # 1. Get list of all JSON files
    if not os.path.exists(DATA_DIR):
        print(f"❌ Error: Folder '{DATA_DIR}' not found.")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
    print(f"--- 📂 Found {len(files)} matches in '{DATA_DIR}' ---")
    print(f"--- 🚀 Starting Ingestion to Topic: {KAFKA_TOPIC} ---")

    count = 0
    for filename in files:
        filepath = os.path.join(DATA_DIR, filename)

        try:
            # 2. Read the local file
            with open(filepath, 'r') as f:
                match_data = json.load(f)

            # 3. Send to Kafka
            producer.send(KAFKA_TOPIC, value=match_data)

            count += 1
            if count % 10 == 0:
                print(f"  -> Sent {count}/{len(files)}: {filename}")

            # Optional: Add a tiny delay so you don't crash Kafka with 100k messages instantly
            # time.sleep(0.01)

        except Exception as e:
            print(f"Failed to process {filename}: {e}")

    # 4. Flush ensures all messages are sent before script exits
    producer.flush()
    print("--- ✅ DONE. All extracted data is now in Kafka. ---")


if __name__ == "__main__":
    feed_kafka()