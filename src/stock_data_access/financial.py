from __future__ import annotations
from typing import Optional, Any, List

class FinancialDataAccess:
    def __init__(self, db):
        self.db = db
        self.cash_flow = db["financial_cashflow"]
        self.income = db["financial_income"]
        self.balancesheet = db["financial_balance"]
        self.fina_indicator = db["financial_indicator"]
        self.daily_basic = db["financial_daily_basic"]
        self.index_constituents = db["index_constituents"]

    def fetch_docs(self, coll, query: dict, periods: Optional[int], sort_field: str = "end_date") -> List[dict]:
        try:
            raw = coll.find(query)
            try:
                if hasattr(raw, "sort") and hasattr(raw, "limit") and periods is not None:
                    raw = raw.sort(sort_field, -1).limit(int(periods))
                    docs = list(raw)
                else:
                    docs = list(raw)
            except Exception:
                docs = list(raw)
        except Exception:
            return []
        try:
            docs = sorted(docs, key=lambda d: d.get(sort_field) or "", reverse=True)
        except Exception:
            pass
        return docs[: int(periods)] if periods else docs
