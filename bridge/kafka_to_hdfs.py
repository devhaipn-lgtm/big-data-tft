import json
import time
from kafka import KafkaConsumer
from hdfs import InsecureClient

# --- CONFIG FOR INTERNAL DOCKER USE ---
# --- CONFIG FOR INTERNAL DOCKER USE ---
KAFKA_TOPIC = 'tft-match-stream'

# 1. CHANGE THIS: Use the internal listener name and port
KAFKA_BOOTSTRAP = 'kafka:29092'  # Was 'bigdataenv-kafka-1:9092'

# 2. CHANGE THIS: Use the service name defined in docker-compose
HDFS_URL = 'http://namenode:9870' # Was 'http://bigdataenv-namenode-1:9870'

HDFS_USER = 'root'
BATCH_SIZE = 100


def connect_hdfs():
    print(f"Connecting to HDFS at {HDFS_URL}...")
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        # Create the folder if it doesn't exist
        client.makedirs('/tft/raw')
        return client
    except Exception as e:
        print(f"❌ Could not connect to HDFS: {e}")
        return None


def run_bridge():
    hdfs_client = connect_hdfs()
    if not hdfs_client: return

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',  # Read from the beginning if we missed data
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='hdfs_archiver_group'  # Important: Keeps track of what we've read
    )

    print(f"--- 🌉 BRIDGE STARTED: Kafka ({KAFKA_TOPIC}) -> HDFS (/tft/raw/) ---")

    batch = []

    for message in consumer:
        match_data = message.value
        print(f"Message received")
        batch.append(match_data)

        # If batch is full, save to HDFS
        if len(batch) >= BATCH_SIZE:
            save_to_hdfs(hdfs_client, batch)
            batch = []  # Reset


def save_to_hdfs(client, data_batch):
    # Create a unique filename based on time
    filename = f"/tft/raw/batch_{int(time.time())}.json"

    try:
        # Convert list of JSONs to a single string (Newline Delimited JSON)
        # Spark loves this format!
        json_str = "\n".join([json.dumps(record) for record in data_batch])

        with client.write(filename, encoding='utf-8') as writer:
            writer.write(json_str)

        print(f"  -> ✅ Saved {len(data_batch)} matches to {filename}")

    except Exception as e:
        print(f"  -> ❌ Failed to save to HDFS: {e}")


if __name__ == "__main__":
    run_bridge()