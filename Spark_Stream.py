from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import requests
import pandas as pd

# Initialize Spark
spark = (SparkSession.builder
         .appName("TweetClassificationFromFiles_API")
         .master("local[*]")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# Read from parquet instead of json
INPUT_PATH = "/home/muaz/tweets_output/*.parquet"

tweets_df = (spark.read
             .parquet(INPUT_PATH))

API_URL = "https://api-inference.huggingface.co/models/facebook/bart-large-mnli"
HF_TOKEN = "Add your token"   # 🔑 replace with your Hugging Face token
HEADERS = {"Authorization": f"Bearer {HF_TOKEN}"}

labels = ["politics", "art", "sports", "technology", "entertainment"]

def classify_with_api(text):
    """Call Hugging Face Inference API to classify text"""
    if text is None or text.strip() == "":
        return "Unknown"
    try:
        payload = {"inputs": text, "parameters": {"candidate_labels": labels}}
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=10)
        result = response.json()
        if "labels" in result:
            return result["labels"][0]   # Top predicted label
        else:
            return "Error"
    except Exception as e:
        return f"Error: {str(e)}"

# Register UDF
classify_udf = udf(classify_with_api, StringType())

# Apply classification
classified_df = tweets_df.withColumn("category", classify_udf(col("text")))

# Save output as parquet instead of json
OUTPUT_PATH = "/home/muaz/tweets_classified"

(classified_df
    .select("id", "text", "category","created_at")   # keep only needed columns
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH))

# Show sample output
classified_df.select("id", "text", "category").show(truncate=False)
