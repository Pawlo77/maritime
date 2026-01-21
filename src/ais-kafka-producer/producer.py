"""
AIS Kafka Producer.

Connects to AIS data stream via WebSocket, processes messages, and sends them to a Kafka topic.
"""

import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timezone

import dotenv
import websockets
from kafka import KafkaProducer

dotenv.load_dotenv()

API_KEY = os.getenv("API_KEY")
BBOX = json.loads(os.getenv("BBOX"))

TOPIC_NAME = os.getenv("TOPIC_NAME", "ais-stream")
BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER", "kafka:29092")


async def connect_ais_stream():
    producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])
    print("Created Kafka producer at time ", datetime.now(timezone.utc))

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {
            "APIKey": API_KEY,
            "BoundingBoxes": BBOX,
        }

        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)
        print("Subscribed to AIS stream with bbox:", BBOX)

        counter = Counter()
        t0 = datetime.now(timezone.utc)

        async for message_json in websocket:
            message = json.loads(message_json)

            if "error" in message:
                print(f"Error from server: {message['error']}")
                continue

            message_type = message.get("MessageType", "Unknown")
            counter[message_type] += 1

            if (datetime.now(timezone.utc) - t0).total_seconds() >= 900:
                print(f"Last 15 minutes message counts: {dict(counter)}")
                counter.clear()
                t0 = datetime.now(timezone.utc)

            producer.send(TOPIC_NAME, value=message_json)


if __name__ == "__main__":
    # asyncio.run(connect_ais_stream())
    pass
