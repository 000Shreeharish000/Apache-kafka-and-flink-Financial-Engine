import os
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "market_ticks")
TICK_INTERVAL_SEC = float(os.getenv("TICK_INTERVAL_SEC", "0.5"))

# Universe of symbols with initial prices
SYMBOLS = {
    "AAPL": 227.35,
    "MSFT": 445.10,
    "NVDA": 128.50,
    "AMZN": 185.20,
    "GOOGL": 178.40
}

current_prices = dict(SYMBOLS)

def create_producer():
    """Attempt connecting to Kafka with exponential backoff."""
    retries = 30
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(2, 5, 0)
            )
            print(f"[SIMULATOR] Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[SIMULATOR] Kafka not available yet (attempt {i+1}/{retries}). Retrying in 2 seconds...")
            time.sleep(2)
    raise Exception("Could not connect to Kafka brokers")

def generate_tick(symbol: str) -> dict:
    """Generate realistic price movement, bid/ask, and volume for a symbol."""
    base_price = current_prices[symbol]
    
    # 95% chance of normal small random movement (-0.2% to +0.2%)
    # 5% chance of larger spike/jump (-1.5% to +1.5%)
    if random.random() < 0.05:
        pct_change = random.uniform(-0.015, 0.015)
    else:
        pct_change = random.uniform(-0.002, 0.002)

    new_price = round(max(1.0, base_price * (1.0 + pct_change)), 2)
    current_prices[symbol] = new_price

    # Bid/Ask spread (0.01 to 0.05 around current price)
    spread = round(random.uniform(0.01, 0.05), 2)
    bid = round(new_price - (spread / 2), 2)
    ask = round(new_price + (spread / 2), 2)

    # Volume (500 to 5000)
    volume = random.randint(500, 5000)

    tick = {
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": new_price,
        "volume": volume,
        "bid": bid,
        "ask": ask
    }
    return tick

def main():
    print("[SIMULATOR] Starting Market Data Simulator...")
    producer = create_producer()

    symbols_list = list(SYMBOLS.keys())
    counter = 0

    while True:
        try:
            # Round-robin or random symbol selection
            symbol = symbols_list[counter % len(symbols_list)]
            tick = generate_tick(symbol)

            producer.send(TOPIC, value=tick)
            producer.flush()

            print(f"[SIMULATOR] Published {symbol} tick -> Price: {tick['price']}, Vol: {tick['volume']}, Time: {tick['timestamp']}")

            counter += 1
            time.sleep(TICK_INTERVAL_SEC)
        except Exception as e:
            print(f"[SIMULATOR] Error producing tick: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()
