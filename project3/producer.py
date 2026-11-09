"""Coinbase trades -> Kafka topic "quakes".

  python quakes_producer.py history 7   # PULL: the last 7 days from the EMSC catalogue
  python quakes_producer.py live        # PUSH: new and updated events from the EMSC WebSocket

Adapted from Professor Magee's stream-seismic.py (learn-kafka).
"""
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "coinbase_trades"
WSS_URL = "wss://ws-feed.exchange.coinbase.com"

def send(producer, message):
    """Part 2: one Kafka record per message, keyed as on your card."""
    key = message["data"].get("product_id")   # .get(): no crash if missing
    if key is None:
        logging.warning("No product_id: sent without a key")
    producer.produce(topic=TOPIC, key=key, value=json.dumps(message))

WSS_URL = "wss://ws-feed.exchange.coinbase.com"
SUBSCRIBE = {"type": "subscribe",
             "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD",
                             "XRP-USD", "DOGE-USD", "ADA-USD"],
             "channels": ["matches"]}

async def live(producer):
    """PUSH over a WebSocket, with a reconnect loop."""
    while True:
        try:
            ws = await websocket_connect(WSS_URL, ping_interval=15)
            if SUBSCRIBE:  # Coinbase closes the connection after 5 s without it
                await ws.write_message(json.dumps(SUBSCRIBE))
            while True:
                text = await ws.read_message()
                if text is None:
                    break  # the server closed the connection
                try:
                    data = json.loads(text)
                    if data.get("type") != "match":
                        continue 
                    send(producer, {"action": "live", "data": data})
                except (ValueError, KeyError, TypeError, AttributeError) as e:
                    logging.warning("Skipped a message: %r", e)
        except Exception as e:
            logging.warning("Connection failed: %r", e)
        logging.info("Reconnecting in 5 s")
        await asyncio.sleep(5)

def main():
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        asyncio.run(live(producer))

if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass
