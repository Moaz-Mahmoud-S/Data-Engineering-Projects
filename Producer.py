import tweepy
from kafka import KafkaProducer
import json
import time

# Twitter API Bearer Token (make sure it has + and =, not %2B or %3D)
BEARER_TOKEN = "Add-Your-Token"

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# Tweepy client
client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

def fetch_tweets():
    #query = "from:moazmah66824642 lang:en"
    query = "the lang:en -is:retweet"


    # only English quoted tweets   # Example query: English quoted tweets, no retweets
    tweets = client.search_recent_tweets(
        query=query,
        tweet_fields=["created_at", "text", "id", "author_id"],
        max_results=10   # Max 100 per request
    )

    if tweets.data:
        for tweet in tweets.data:
            tweet_data = {
                "id": tweet.id,
                "text": tweet.text,
                "timestamp": str(tweet.created_at),
                "author_id": tweet.author_id
            }
            # Send to Kafka topic
            producer.send("twitter-stream", value=tweet_data)
            print(f"✅ Tweet sent: {tweet_data}")

# Polling loop
print("🚀 Starting Twitter -> Kafka producer (polling mode)...")
while True:
    fetch_tweets()
    time.sleep(900)  # Wait 1 minute between calls (avoid hitting rate limit)

