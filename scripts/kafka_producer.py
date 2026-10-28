#!/usr/bin/env python3
"""
Binance WebSocket → Kafka producer (Phase 1).

Reads live BTC/USDT trades from Binance and publishes to topic crypto.trades.
Run: python scripts/kafka_producer.py
"""

import json
import os
import signal
import sys

import websocket
from kafka import KafkaProducer

BINANCE_WS_URL = os.environ.get("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "crypto.trades")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
    enable_idempotence=True,
)

ws_app = None


def shutdown(signum, frame):
    print("\nShutting down...")
    if ws_app is not None:
        ws_app.close()
    producer.flush()
    producer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def on_message(ws, message):
    trade = json.loads(message)
    trade_id = trade["t"]
    producer.send(KAFKA_TOPIC, key=trade_id, value=trade)
    print(f"Sent trade {trade_id} | {trade['s']} @ {trade['p']} qty={trade['q']}")


def on_error(ws, error):
    print(f"WebSocket error: {error}", file=sys.stderr)


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} {close_msg}")


def on_open(ws):
    print(f"Connected to Binance → publishing to {KAFKA_TOPIC}")


def main():
    global ws_app
    ws_app = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws_app.run_forever()
    producer.flush()
    producer.close()
    print("Done.")


if __name__ == "__main__":
    main()
