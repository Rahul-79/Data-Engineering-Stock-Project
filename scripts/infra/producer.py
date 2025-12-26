import time 
import json
import requests
import os
from kafka import KafkaProducer

# SECURITY: Fetch sensitive info from environment variables
API_KEY = os.getenv("FINNHUB_API_KEY", "your_api_key_here")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")

BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_quote(symbol):
    """Fetches real-time stock quotes from Finnhub API."""
    params = {
        'symbol': symbol,
        'token': API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Enriching the data with metadata for the downstream Bronze layer
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

print(f"--- Producer started, sending data to {KAFKA_TOPIC} ---")

try:
    while True:
        for symbol in SYMBOLS:
            quote = fetch_quote(symbol)
            if quote:
                print(f"Producing record for {symbol}: {quote['c']}") # Printing 'c' (current price)
                producer.send(KAFKA_TOPIC, value=quote)
        
        # Wait 6 seconds before next poll to stay within API rate limits
        time.sleep(6)
except KeyboardInterrupt:
    print("Producer stopped by user.")
finally:
    producer.flush()
    producer.close()
