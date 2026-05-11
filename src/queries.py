"""
Run the three portfolio analytics queries against Neo4j and print results.

Usage:
    python -m src.queries
"""

import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

from src.logger import configure_logging

logger = configure_logging("queries")
load_dotenv()

TOP_EDITORS = """
MATCH (u:User)-[:EDITED]->(r:Revision)
RETURN u.username AS user, count(r) AS edits
ORDER BY edits DESC LIMIT 5
"""

MOST_REVERTED_PAGES = """
MATCH (r1:Revision)-[:REVERTED]->(r2:Revision)-[:MODIFIES]->(p:Page)
RETURN p.title AS page, count(r2) AS revert_count
ORDER BY revert_count DESC LIMIT 10
"""

COLLABORATION_PAIRS = """
MATCH (u1:User)-[:EDITED]->(:Revision)-[:MODIFIES]->(p:Page)
      <-[:MODIFIES]-(:Revision)<-[:EDITED]-(u2:User)
WHERE u1.username < u2.username
RETURN u1.username AS user1, u2.username AS user2, count(p) AS common_pages
ORDER BY common_pages DESC LIMIT 10
"""


def run_query(driver, title: str, cypher: str) -> None:
    logger.info("=" * 60)
    logger.info("  %s", title)
    logger.info("=" * 60)
    with driver.session() as session:
        for record in session.run(cypher):
            logger.info("%s", dict(record))


def main() -> None:
    driver = GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
    )
    run_query(driver, "Top-5 most active editors",     TOP_EDITORS)
    run_query(driver, "Most reverted pages",           MOST_REVERTED_PAGES)
    run_query(driver, "User collaboration pairs",      COLLABORATION_PAIRS)
    driver.close()


if __name__ == "__main__":
    main()
