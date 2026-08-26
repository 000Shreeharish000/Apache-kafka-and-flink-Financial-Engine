import os
import json
import time
import math
import psycopg2
from datetime import datetime, timezone
from collections import deque
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "market_ticks")

DB_HOST = os.getenv("POSTGRES_HOST", "timescaledb")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "market_intelligence")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

WINDOW_SIZE = 10

# Per-symbol sliding state
symbol_state = {}
# Symbol -> latest signal
latest_symbol_signals = {}

def get_db_connection():
    """Establish connection to TimescaleDB with retry."""
    retries = 30
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            conn.autocommit = True
            print(f"[FLINK ENGINE] Connected to TimescaleDB at {DB_HOST}:{DB_PORT}")
            return conn
        except Exception as e:
            print(f"[FLINK ENGINE] Database not ready yet ({i+1}/{retries}). Retrying in 2 seconds... Error: {e}")
            time.sleep(2)
    raise Exception("Could not connect to TimescaleDB")

def get_kafka_consumer():
    """Establish Kafka consumer with retry."""
    retries = 30
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='flink-stream-processing-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"[FLINK ENGINE] Subscribed to Kafka topic '{TOPIC}'")
            return consumer
        except NoBrokersAvailable:
            print(f"[FLINK ENGINE] Kafka broker unavailable ({i+1}/{retries}). Retrying in 2 seconds...")
            time.sleep(2)
    raise Exception("Could not connect to Kafka broker")

def process_tick(tick: dict, db_conn):
    """
    Flink Stream Calculation:
    1. Rolling Return
    2. Rolling Volatility
    3. Volume Anomaly
    4. Price Movement Classification
    5. Overall Signal & Transparent Score
    """
    symbol = tick["symbol"]
    timestamp = tick["timestamp"]
    price = float(tick["price"])
    volume = int(tick["volume"])
    bid = float(tick["bid"])
    ask = float(tick["ask"])

    if symbol not in symbol_state:
        symbol_state[symbol] = {
            "prices": deque(maxlen=WINDOW_SIZE),
            "returns": deque(maxlen=WINDOW_SIZE),
            "volumes": deque(maxlen=WINDOW_SIZE)
        }

    state = symbol_state[symbol]
    
    # 1. Rolling Return Calculation
    if len(state["prices"]) > 0:
        prev_price = state["prices"][-1]
        ret = (price - prev_price) / prev_price
    else:
        ret = 0.0

    state["prices"].append(price)
    state["returns"].append(ret)
    state["volumes"].append(volume)

    # 2. Rolling Volatility (Standard Deviation of returns over window)
    if len(state["returns"]) > 1:
        avg_ret = sum(state["returns"]) / len(state["returns"])
        variance = sum((r - avg_ret) ** 2 for r in state["returns"]) / (len(state["returns"]) - 1)
        volatility = math.sqrt(variance)
    else:
        volatility = 0.0

    # 3. Volume Anomaly (Current Volume / Rolling Average Volume)
    avg_volume = sum(state["volumes"]) / len(state["volumes"])
    volume_ratio = volume / avg_volume if avg_volume > 0 else 1.0
    volume_anomaly = "HIGH_VOLUME" if volume_ratio >= 1.5 else "NORMAL"

    # 4. Price Movement Classification
    if ret >= 0.01:
        movement = "STRONG_UP"
    elif ret > 0.0:
        movement = "UP"
    elif ret == 0.0:
        movement = "FLAT"
    elif ret <= -0.01:
        movement = "STRONG_DOWN"
    else:
        movement = "DOWN"

    # 5. Transparent Rule-Based Scoring Engine
    # Positive return: +1, Negative return: -1
    # High positive volume: +1, High negative volume: -1
    ret_score = 1 if ret > 0 else (-1 if ret < 0 else 0)
    vol_score = 0
    if volume_anomaly == "HIGH_VOLUME":
        vol_score = 1 if ret > 0 else (-1 if ret < 0 else 0)

    signal_score = ret_score + vol_score

    if signal_score >= 1:
        signal = "BULLISH"
    elif signal_score <= -1:
        signal = "BEARISH"
    else:
        signal = "NEUTRAL"

    latest_symbol_signals[symbol] = signal

    # Calculate overall market state across all tracked symbols
    bullish_count = sum(1 for s in latest_symbol_signals.values() if s == "BULLISH")
    bearish_count = sum(1 for s in latest_symbol_signals.values() if s == "BEARISH")
    neutral_count = sum(1 for s in latest_symbol_signals.values() if s == "NEUTRAL")
    total_tracked = len(latest_symbol_signals)

    if bullish_count > bearish_count and bullish_count >= neutral_count:
        overall_market_signal = "BULLISH"
        confidence = (bullish_count / total_tracked) * 100.0 if total_tracked > 0 else 0.0
    elif bearish_count > bullish_count and bearish_count >= neutral_count:
        overall_market_signal = "BEARISH"
        confidence = (bearish_count / total_tracked) * 100.0 if total_tracked > 0 else 0.0
    else:
        overall_market_signal = "NEUTRAL"
        confidence = (neutral_count / total_tracked) * 100.0 if total_tracked > 0 else 0.0

    # Persist to TimescaleDB
    with db_conn.cursor() as cur:
        # Save raw tick
        cur.execute(
            """
            INSERT INTO market_ticks (time, symbol, price, volume, bid, ask)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (timestamp, symbol, price, volume, bid, ask)
        )

        # Save processed analytics
        cur.execute(
            """
            INSERT INTO processed_market_signals 
            (time, symbol, price, return, volatility, volume_ratio, movement, signal, signal_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (timestamp, symbol, price, ret, volatility, volume_ratio, movement, signal, signal_score)
        )

        # Save overall market state snapshot
        cur.execute(
            """
            INSERT INTO market_state (time, bullish_count, neutral_count, bearish_count, overall_signal, confidence)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (timestamp, bullish_count, neutral_count, bearish_count, overall_market_signal, confidence)
        )

    print(
        f"[FLINK ENGINE] Processed {symbol} event -> Price: {price:.2f} | "
        f"Return: {ret*100:+.2f}% | Vol: {volatility:.4f} | Ratio: {volume_ratio:.2f} ({volume_anomaly}) | "
        f"Signal: {signal} (Score: {signal_score:+d})"
    )

def main():
    print("=== APACHE FLINK STREAM PROCESSING ENGINE ===")
    db_conn = get_db_connection()
    consumer = get_kafka_consumer()

    print("[FLINK ENGINE] Listening for incoming tick events...")

    for message in consumer:
        try:
            tick = message.value
            process_tick(tick, db_conn)
        except Exception as e:
            print(f"[FLINK ENGINE] Error processing stream element: {e}")
            # Reconnect DB if broken
            try:
                db_conn = get_db_connection()
            except Exception:
                pass

if __name__ == "__main__":
    main()
