import asyncio
import json
import logging
import httpx
from typing import AsyncIterator

logger = logging.getLogger(__name__)

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

HEADERS = {
    "User-Agent": "WikimediaEditGraph/1.0 (portfolio-project; https://github.com/PavelYarmaliuk/WikimediaEventStreams)"
}

_TIMEOUT = httpx.Timeout(connect=10.0, read=None, write=None, pool=10.0)
_LIMITS = httpx.Limits(max_connections=1, max_keepalive_connections=1, keepalive_expiry=30.0)


def parse_event(line: str) -> dict | None:
    if not line.startswith("data:"):
        return None
    try:
        return json.loads(line[5:].strip())
    except json.JSONDecodeError:
        logger.warning("Failed to parse line: %s", line[:120])
        return None


async def read_stream(url: str = STREAM_URL) -> AsyncIterator[dict]:
    """Yields parsed recentchange events from the Wikimedia SSE stream."""
    async with httpx.AsyncClient(headers=HEADERS, timeout=_TIMEOUT, limits=_LIMITS) as client:
        while True:
            try:
                async with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    async for line in resp.aiter_lines():
                        event = parse_event(line)
                        if event:
                            yield event
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code < 500:
                    raise
                logger.error("Server error %d, reconnecting: %s", exc.response.status_code, exc)
                await asyncio.sleep(1)
            except httpx.HTTPError as exc:
                logger.error("Stream connection error, reconnecting: %s", exc)
                await asyncio.sleep(1)