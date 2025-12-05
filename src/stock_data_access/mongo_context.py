from __future__ import annotations

import os
from typing import Optional
from pymongo import MongoClient

_DEFAULT_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
_DEFAULT_DB = os.environ.get("MONGO_DB", "finance")

_client: Optional[MongoClient] = None


def get_db(uri: Optional[str] = None, db_name: Optional[str] = None):
    global _client
    if _client is None:
        _client = MongoClient(uri or _DEFAULT_URI)
    return _client[(db_name or _DEFAULT_DB)]
