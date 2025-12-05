from __future__ import annotations
from typing import List
from .mongo_context import get_db

class ScoreDataAccess:
    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self.scores_coll = self.db["stock_scores"]
        self._composite_styles = {
            "balanced",
            "aggressive",
            "conservative",
            "defensive",
            "value_oriented",
            "trading_oriented",
            "growth_oriented",
            "cycle_oriented",
        }

    def _resolve_nearest_score_date(self, score_date: str) -> str:
        if self.scores_coll.find_one({"score_date": score_date}, {"_id": 1}):
            return score_date
        prev_doc = self.scores_coll.find({"score_date": {"$lte": score_date}}, {"score_date": 1}).sort("score_date", -1).limit(1)
        prev_list = list(prev_doc)
        if prev_list:
            return prev_list[0]["score_date"]
        latest = self.scores_coll.find({}, {"score_date": 1}).sort("score_date", -1).limit(1)
        latest_list = list(latest)
        if latest_list:
            return latest_list[0]["score_date"]
        return score_date

    def select_top_symbols(self, score_date: str, dimension: str, top_n: int, auto_resolve_date: bool = True) -> List[str]:
        if auto_resolve_date:
            score_date = self._resolve_nearest_score_date(score_date)
        if dimension in self._composite_styles:
            sort_field = f"composite_score.{dimension}"
            query = {"score_date": score_date, sort_field: {"$exists": True}}
        else:
            sort_field = f"{dimension}_score"
            query = {"score_date": score_date, sort_field: {"$exists": True}}
        cursor = self.scores_coll.find(query, {"symbol": 1, sort_field: 1}).sort(sort_field, -1).limit(top_n)
        return [doc["symbol"] for doc in cursor]

    def select_top_with_date(self, score_date: str, dimension: str, top_n: int, auto_resolve_date: bool = True) -> tuple[str, List[str]]:
        used_date = self._resolve_nearest_score_date(score_date) if auto_resolve_date else score_date
        if dimension in self._composite_styles:
            sort_field = f"composite_score.{dimension}"
            query = {"score_date": used_date, sort_field: {"$exists": True}}
        else:
            sort_field = f"{dimension}_score"
            query = {"score_date": used_date, sort_field: {"$exists": True}}
        cursor = self.scores_coll.find(query, {"symbol": 1, sort_field: 1}).sort(sort_field, -1).limit(top_n)
        return used_date, [doc["symbol"] for doc in cursor]
