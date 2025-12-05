from __future__ import annotations
from typing import Any, Dict, List, Optional
from .mongo_context import get_db

class UserDataAccess:
    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self.user_coll = self.db["users"]
        self.watch_coll = self.db["user_watchlists"]

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        return self.user_coll.find_one({"username": username})

    def extract_user_id(self, user_doc: Dict[str, Any]) -> str:
        return str(user_doc.get("id") or user_doc.get("_id"))

    def extract_email(self, user_doc: Dict[str, Any]) -> Optional[str]:
        return user_doc.get("email") or user_doc.get("mail") or user_doc.get("contact_email")

    def get_watchlist_symbols(self, user_id: str) -> List[str]:
        wl = self.watch_coll.find_one({"user_id": user_id})
        if not wl:
            return []
        symbols = wl.get("symbols") or []
        return [s for s in symbols if isinstance(s, str) and s.strip()]
