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


_MARKET_CALENDAR = {
    "A": "trade_calendar",
    "SSE": "trade_calendar",
    "CN": "trade_calendar",
    "HK": "hk_trade_calendar",
    "US": "us_trade_calendar",
}


def get_market_trading_dates(start_date: str, end_date: str, market: str = "A") -> List[str]:
    """Open days from Mongo calendars. ``market`` is A/HK/US (A-share stays SSE)."""
    key = (market or "A").upper()
    if key in ("A", "SSE", "CN"):
        return get_trading_dates(start_date, end_date, prefer="mongo")
    coll_name = _MARKET_CALENDAR.get(key)
    if not coll_name:
        raise ValueError(f"unknown market {market!r}; expected A|HK|US")
    db = get_db()
    coll = db[coll_name]
    query = {
        "cal_date": {"$gte": start_date, "$lte": end_date},
        "is_open": 1,
    }
    try:
        days = coll.distinct("cal_date", query)
        days = [str(d).replace("-", "") for d in days if d]
        if days:
            return sorted(days)
    except Exception:
        pass
    cursor = coll.find(query, {"cal_date": 1, "trade_date": 1})
    uniq = set()
    for doc in cursor:
        d = doc.get("cal_date") or doc.get("trade_date")
        if d:
            uniq.add(str(d).replace("-", "")[:8])
    return sorted(uniq)
