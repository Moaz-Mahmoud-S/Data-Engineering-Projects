from kafka import KafkaConsumer
import json
import os
from datetime import datetime
import pandas as pd

# Output folder for saved tweets
OUTPUT_DIR = "/home/muaz/tweets_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Kafka Consumer
consumer = KafkaConsumer(
    "twitter-stream",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("📡 Listening to tweets...")

file_index = 0
batch = []

for message in consumer:
    tweet = message.value
    batch.append(tweet)

    # Save every 10 tweets into a parquet file
    if len(batch) >= 10:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(OUTPUT_DIR, f"tweets_{file_index}_{timestamp}.parquet")

        # Convert batch to DataFrame and save as Parquet
        df = pd.DataFrame(batch)
        df.to_parquet(filename, engine="pyarrow", index=False)

        print(f"💾 Saved {len(batch)} tweets to {filename}")

        batch = []  # reset batch
        file_index += 1
