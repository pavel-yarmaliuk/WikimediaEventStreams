"""
Spark Structured Streaming job: reads from Kafka, writes to Neo4j.

Run via spark-submit (see docker-compose.yml spark-consumer service).
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType,
)
from neo4j import GraphDatabase

from src.logger import configure_logging

logger = configure_logging("spark_consumer")

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
NEO4J_URI     = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER    = os.environ.get("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")
TOPIC         = "wikimedia-recentchange"
CHECKPOINT    = "/tmp/wikimedia-checkpoint"

# Single round-trip for the whole batch via UNWIND
_UPSERT_BATCH = """
UNWIND $events AS e
MERGE (u:User {user_id: e.user_id})
  ON CREATE SET u.username = e.username, u.edit_count = 1
  ON MATCH  SET u.edit_count = u.edit_count + 1
MERGE (p:Page {page_id: e.page_id})
  ON CREATE SET p.title = e.title, p.namespace = e.namespace
MERGE (r:Revision {rev_id: e.rev_id})
  ON CREATE SET r.timestamp = e.timestamp, r.comment = e.comment, r.type = e.type
MERGE (u)-[:EDITED]->(r)
MERGE (r)-[:MODIFIES]->(p)
"""

_REVERT_BATCH = """
UNWIND $pairs AS p
MATCH (r_new:Revision {rev_id: p.new_id})
MATCH (r_old:Revision {rev_id: p.old_id})
MERGE (r_new)-[:REVERTED]->(r_old)
"""

event_schema = StructType([
    StructField("user",      StringType(),  True),
    StructField("title",     StringType(),  True),
    StructField("page_id",   LongType(),    True),
    StructField("namespace", IntegerType(), True),
    StructField("type",      StringType(),  True),
    StructField("timestamp", LongType(),    True),
    StructField("comment",   StringType(),  True),
    StructField("revision", StructType([
        StructField("old", LongType(), True),
        StructField("new", LongType(), True),
    ]), True),
])


_driver = None


def _get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return _driver


def write_batch(batch_df, epoch_id):
    rows = batch_df.collect()
    if not rows:
        return

    events, revert_pairs = [], []
    for row in rows:
        rev_id = (
            str(row.revision.new)
            if row.revision and row.revision.new
            else f"epoch-{epoch_id}"
        )
        events.append({
            "user_id":   row.user or "anonymous",
            "username":  row.user or "anonymous",
            "page_id":   str(row.page_id or row.title or ""),
            "title":     row.title or "",
            "namespace": row.namespace or 0,
            "rev_id":    rev_id,
            "timestamp": row.timestamp or 0,
            "comment":   (row.comment or "")[:500],
            "type":      row.type or "edit",
        })
        if row.type == "edit" and row.revision and row.revision.old:
            old_rev = str(row.revision.old)
            if old_rev != rev_id:
                revert_pairs.append({"new_id": rev_id, "old_id": old_rev})

    driver = _get_driver()
    with driver.session() as session:
        session.run(_UPSERT_BATCH, events=events)
        if revert_pairs:
            session.run(_REVERT_BATCH, pairs=revert_pairs)


spark = (
    SparkSession.builder
    .appName("WikimediaEditGraph")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), event_schema).alias("d"))
    .select("d.*")
    .filter(col("type").isin("edit", "new"))
)

query = (
    parsed.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="15 seconds")
    .start()
)

query.awaitTermination()
