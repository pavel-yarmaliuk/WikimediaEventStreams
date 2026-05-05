"""
Entry point. Reads the Wikimedia SSE stream and writes each edit to Neo4j.

Usage:
    python -m src.pipeline [--limit N]

Set NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD in a .env file (copy .env.example).
"""

import argparse
import asyncio
import logging
import os
from dotenv import load_dotenv

from src.stream_reader import read_stream
from src.neo4j_writer import Neo4jWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()


async def _run(limit: int | None, writer: Neo4jWriter) -> None:
    count = 0
    async for event in read_stream():
        event_type = event.get("type")
        if event_type not in ("edit", "new"):
            continue

        writer.write_event(event)
        count += 1

        if count % 100 == 0:
            logger.info("Processed %d events", count)

        if limit and count >= limit:
            logger.info("Reached limit of %d events. Stopping.", limit)
            break


def main(limit: int | None = None) -> None:
    uri      = os.environ["NEO4J_URI"]
    user     = os.environ["NEO4J_USER"]
    password = os.environ["NEO4J_PASSWORD"]

    writer = Neo4jWriter(uri, user, password)
    logger.info("Connected to Neo4j. Starting stream…")

    try:
        asyncio.run(_run(limit, writer))
    finally:
        writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N events (omit for continuous mode)")
    args = parser.parse_args()
    main(limit=args.limit)