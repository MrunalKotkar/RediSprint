"""
Creates the two Redis Vector Search indices used by RediSprint:
  - tickets_idx: stores past Jira tickets for semantic context retrieval
  - code_idx:    stores codebase chunks for code-aware ticket enrichment

Run this ONCE before running ingest_code_and_tickets.py.
"""
import os
from redisvl.index import SearchIndex
from redisvl.schema import IndexSchema
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.environ["REDIS_URL"]

TICKETS_SCHEMA_DICT = {
    "index": {
        "name": "tickets_idx",
        "prefix": "ticket:",
        "storage_type": "hash"
    },
    "fields": [
        {"name": "id", "type": "tag"},
        {"name": "title", "type": "text", "attrs": {"weight": 2.0}},
        {"name": "body", "type": "text"},
        {"name": "issuetype", "type": "tag"},
        {"name": "component", "type": "tag"},
        {"name": "assignee", "type": "tag"},
        {"name": "sprint", "type": "tag"},
        {
            "name": "embedding",
            "type": "vector",
            "attrs": {
                "dims": 384,
                "algorithm": "hnsw",
                "distance_metric": "cosine",
                "datatype": "float32",
                "m": 24,
                "ef_construction": 200,
                "ef_runtime": 50,
            },
        },
    ],
}

CODE_SCHEMA_DICT = {
    "index": {
        "name": "code_idx",
        "prefix": "code:",
        "storage_type": "hash"
    },
    "fields": [
        {"name": "id", "type": "tag"},
        {"name": "repo", "type": "tag"},
        {"name": "path", "type": "text"},
        {"name": "lang", "type": "tag"},
        {"name": "body", "type": "text"},
        {
            "name": "embedding",
            "type": "vector",
            "attrs": {
                "dims": 384,
                "algorithm": "hnsw",
                "distance_metric": "cosine",
                "datatype": "float32",
                "m": 24,
                "ef_construction": 200,
                "ef_runtime": 50,
            },
        },
    ],
}

if __name__ == "__main__":
    print("\n[*] Creating Redis Vector Search indices...\n")
    for schema_dict in [TICKETS_SCHEMA_DICT, CODE_SCHEMA_DICT]:
        name = schema_dict["index"]["name"]
        schema = IndexSchema.from_dict(schema_dict)
        idx = SearchIndex(schema=schema, redis_url=REDIS_URL)
        idx.create(overwrite=True)
        print(f"   [OK] Created index: {name}")
    print("\n[DONE] All indices ready. Run ingest_code_and_tickets.py next.\n")
