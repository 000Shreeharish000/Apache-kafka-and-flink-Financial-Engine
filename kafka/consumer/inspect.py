import os
import json
import sys
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "market_ticks")

def main():
    print(f"=== KAFKA MARKET TICK INSPECTOR ===")
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    print(f"Subscribing to topic: {TOPIC}")
    print("Press Ctrl+C to exit.\n" + "-"*50)

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='debug-inspector-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            tick = message.value
            print(f"[KAFKA TICK RECEIVE] Partition: {message.partition} | Offset: {message.offset}")
            print(json.dumps(tick, indent=2))
            print("-" * 50)
            sys.stdout.flush()

    except KeyboardInterrupt:
        print("\nStopping Kafka inspector.")
    except Exception as e:
        print(f"Error inspecting Kafka stream: {e}")

if __name__ == "__main__":
    main()
