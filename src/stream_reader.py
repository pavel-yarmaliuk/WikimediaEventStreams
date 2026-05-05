import json
import logging
import requests
from typing import Iterator

logger = logging.getLogger(__name__)

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

HEADERS = {
    "User-Agent": "WikimediaEditGraph/1.0 (portfolio-project; https://github.com/PavelYarmaliuk/WikimediaEventStreams)"
}


def parse_event(line: str) -> dict | None:
    if not line.startswith("data:"):
        return None
    try:
        return json.loads(line[5:].strip())
    except json.JSONDecodeError:
        logger.warning("Failed to parse line: %s", line[:120])
        return None


def read_stream(url: str = STREAM_URL) -> Iterator[dict]:
    """Yields parsed recentchange events from the Wikimedia SSE stream."""
    while True:
        try:
            with requests.get(url, stream=True, timeout=30, headers=HEADERS) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines(decode_unicode=True):
                    event = parse_event(line)
                    if event:
                        yield event
        except requests.RequestException as exc:
            logger.error("Stream connection error, reconnecting: %s", exc)