# WikimediaEventStreams

Real-time pipeline that consumes Wikipedia edit events from the Wikimedia SSE API and stores them in a Neo4j graph database, modeling `User`, `Page`, and `Revision` nodes with edit relationships.

## Codebase Overview

There are two write paths: a **Kafka/Spark path** (`producer.py` → Kafka → `spark_consumer.py` → Neo4j) for production use, and a **direct path** (`pipeline.py` → Neo4j) for local development. Both share `stream_reader.py` as the SSE abstraction. Analytics queries run via `queries.py` against the populated graph.

**Stack**: Python 3.12, httpx (async SSE streaming), Apache Kafka (Confluent 7.6.1), Apache Spark Structured Streaming 3.5.8, Neo4j 5.18 Community, Docker Compose.

**Structure**: All application code is in `src/`. Infrastructure is fully Docker-composed (`docker-compose.yml`). Convenience commands are in `Makefile`.

For detailed architecture, see [docs/CODEBASE_MAP.md](docs/CODEBASE_MAP.md).

## Common Commands

```bash
make build        # Build images and start all services
make up           # Start services (no rebuild)
make down         # Stop services
make logs         # Tail all logs
make queries      # Run analytics queries against Neo4j
make clean        # Destroy everything including volumes

# Direct path (no Kafka/Spark):
python -m src.pipeline --limit 500
```

## Environment Setup

Copy `.env.example` to `.env` and fill in values (or use defaults for local Docker-based dev — defaults already match `docker-compose.yml`).

## Logging

All entrypoints share `src/logger.py` (`configure_logging`). Each writes to `logs/<entrypoint>.log` (console output unchanged). Files rotate at 10 MB and 5 backups are kept; tune via `LOG_DIR`, `LOG_LEVEL`, `LOG_MAX_BYTES`, `LOG_BACKUP_COUNT`. Docker producer/spark-consumer bind-mount `./logs` to `/app/logs`.
