#!/usr/bin/env python3
"""
Kafka consumer for Phase 1 verification.

Reads messages from topic crypto.trades and prints them as JSON.
Run: python scripts/kafka_consumer.py
     python scripts/kafka_consumer.py --from latest   # new messages only
"""

import argparse
import json
import os
import signal
import sys

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "crypto.trades")

running = True


def shutdown(signum, frame):
    global running
    print("\nShutting down...")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def main():
    parser = argparse.ArgumentParser(description="Read crypto.trades from Kafka")
    parser.add_argument(
        "--from",
        dest="offset",
        choices=["earliest", "latest"],
        default="earliest",
        help="earliest = all stored messages; latest = only new (default: earliest)",
    )
    args = parser.parse_args()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset=args.offset,
        value_deserializer=lambda m: json.loads(m.decode()),
        key_deserializer=lambda k: k.decode() if k else None,
    )

    print(f"Reading {KAFKA_TOPIC} from {args.offset} (Ctrl+C to stop)\n")

    try:
        for msg in consumer:
            if not running:
                break
            trade = msg.value
            print(
                f"offset={msg.offset} key={msg.key} | "
                f"{trade.get('s')} @ {trade.get('p')} qty={trade.get('q')}"
            )
    finally:
        consumer.close()
        print("Done.")


if __name__ == "__main__":
    main()
