from __future__ import annotations
from typing import Optional
import os
from functools import lru_cache
from dotenv import load_dotenv
from pymongo import MongoClient

# Ensure environment variables are loaded if available
load_dotenv()


_DEFAULT_DB = os.environ.get("DATA_DB_NAME", os.environ.get("MONGO_DB", "quant_data"))
_DEFAULT_URI = os.environ.get("MONGO_URI") or "mongodb://localhost:27017"


def pool_options() -> dict:
    """Connection pool bounds, overridable per deployment.

    pymongo defaults to ``maxIdleTimeMS=None``, which never reclaims an idle
    connection: a long-lived process climbs to ``maxPoolSize`` and stays there.
    That ratchet exhausted the mongod container's file descriptors on
    2026-08-23, so both bounds are set explicitly rather than left to pymongo.
    Env overrides exist so the ceiling can be tuned without rebuilding images.
    """
    return {
        "maxIdleTimeMS": int(os.environ.get("MONGO_MAX_IDLE_TIME_MS", "60000")),
        "maxPoolSize": int(os.environ.get("MONGO_MAX_POOL_SIZE", "50")),
    }


def _build_client(uri: Optional[str] = None) -> MongoClient:
    use_mock = os.getenv("USE_MOCK_MONGO") in ("1", "true", "True")
    if use_mock:
        try:
            import mongomock  # type: ignore
            return mongomock.MongoClient()
        except Exception:
            # Fallback to real client if mongomock unavailable
            pass
    mongo_uri = uri or _DEFAULT_URI
    return MongoClient(mongo_uri, **pool_options())

@lru_cache(maxsize=1)
def get_mongo_client(uri: Optional[str] = None) -> MongoClient:
    """Cached `MongoClient`. Tests can call `get_mongo_client.cache_clear()`."""
    return _build_client(uri)

@lru_cache(maxsize=1)
def get_db(uri: Optional[str] = None, db_name: Optional[str] = None):
    """Return a cached DB handle using `MONGO_DB` or provided name."""
    name = db_name or _DEFAULT_DB
    client = get_mongo_client(uri)
    return client.get_database(name)