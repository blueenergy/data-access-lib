from __future__ import annotations
import os
from typing import List
from .mongo_context import get_db

try:
    import tushare as ts  # type: ignore
except Exception:  # pragma: no cover
    ts = None

TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")


def _tushare_trading_days(start_date: str, end_date: str) -> List[str]:
    if ts is None or not TUSHARE_TOKEN:
        return []
    try:
        pro = ts.pro_api(TUSHARE_TOKEN)
        df = pro.trade_cal(exchange="", start_date=start_date, end_date=end_date, is_open=1)
        if df is None or df.empty:
            return []
        return sorted(df["cal_date"].tolist())
    except Exception:
        return []


def _mongo_trading_days(start_date: str, end_date: str) -> List[str]:
    db = get_db()
    coll = db["volume_price"]
    try:
        days = coll.distinct("trade_date", {"trade_date": {"$gte": start_date, "$lte": end_date}})
        days = [d for d in days if isinstance(d, str)]
        return sorted(days)
    except Exception:
        cursor = coll.find({"trade_date": {"$gte": start_date, "$lte": end_date}}, {"trade_date": 1})
        uniq = {doc.get("trade_date") for doc in cursor if doc.get("trade_date")}
        return sorted(uniq)


def get_trading_dates(start_date: str, end_date: str, prefer: str = "tushare") -> List[str]:
    if prefer == "tushare":
        days = _tushare_trading_days(start_date, end_date)
        if days:
            return days
        return _mongo_trading_days(start_date, end_date)
    else:
        days = _mongo_trading_days(start_date, end_date)
        if days:
            return days
        return _tushare_trading_days(start_date, end_date)
