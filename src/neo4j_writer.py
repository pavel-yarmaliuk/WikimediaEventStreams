import logging
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

# MERGE on rev_id prevents duplicate nodes when the pipeline restarts.
_UPSERT_EDIT = """
MERGE (u:User {user_id: $user_id})
  ON CREATE SET u.username = $username, u.edit_count = 1
  ON MATCH  SET u.edit_count = u.edit_count + 1

MERGE (p:Page {page_id: $page_id})
  ON CREATE SET p.title = $title, p.namespace = $namespace

MERGE (r:Revision {rev_id: $rev_id})
  ON CREATE SET r.timestamp = $timestamp,
               r.comment   = $comment,
               r.type      = $type

MERGE (u)-[:EDITED]->(r)
MERGE (r)-[:MODIFIES]->(p)
"""

_MARK_REVERT = """
MATCH (r_new:Revision {rev_id: $rev_new_id})
MATCH (r_old:Revision {rev_id: $rev_old_id})
MERGE (r_new)-[:REVERTED]->(r_old)
"""


class Neo4jWriter:
    def __init__(self, uri: str, user: str, password: str) -> None:
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self._driver.close()

    def write_event(self, event: dict) -> None:
        meta = event.get("meta", {})
        rev_id = event.get("revision", {}).get("new") or meta.get("id", "")

        params = {
            "user_id":   event.get("user", "anonymous"),
            "username":  event.get("user", "anonymous"),
            "page_id":   str(event.get("page_id", event.get("title", ""))),
            "title":     event.get("title", ""),
            "namespace": event.get("namespace", 0),
            "rev_id":    str(rev_id),
            "timestamp": event.get("timestamp", 0),
            "comment":   event.get("comment", "")[:500],
            "type":      event.get("type", "edit"),
        }

        with self._driver.session() as session:
            session.run(_UPSERT_EDIT, **params)

            # Wire up revert relationship when revision data is present
            if event.get("type") == "edit":
                old_rev = event.get("revision", {}).get("old")
                if old_rev and str(old_rev) != str(rev_id):
                    session.run(
                        _MARK_REVERT,
                        rev_new_id=str(rev_id),
                        rev_old_id=str(old_rev),
                    )