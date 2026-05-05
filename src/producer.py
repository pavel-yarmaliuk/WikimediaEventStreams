"""
Reads the Wikimedia SSE stream and publishes each edit event to a Kafka topic.
"""
import asyncio
import json
import logging
import os
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.stream_reader import read_stream

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "wikimedia-recentchange"


def get_producer(retries: int = 15, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
                batch_size=16_384,
            )
        except NoBrokersAvailable:
            logger.warning("Kafka not ready (%d/%d), retrying in %ds…", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts")


async def _run(producer: KafkaProducer) -> None:
    count = 0
    async for event in read_stream():
        if event.get("type") not in ("edit", "new"):
            continue
        producer.send(TOPIC, event)
        count += 1
        if count % 200 == 0:
            logger.info("Published %d events", count)


def main() -> None:
    producer = get_producer()
    logger.info("Connected to Kafka. Publishing to topic '%s'…", TOPIC)
    asyncio.run(_run(producer))


if __name__ == "__main__":
    main()
