from __future__ import annotations
import pandas as pd
from .mongo_context import get_db

class IndexDataAccess:
    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self.index_coll = self.db["index_prices"]

    def load_normalized(self, ts_code: str, start_date: str, end_date: str) -> pd.Series:
        cursor = self.index_coll.find(
            {"ts_code": ts_code, "trade_date": {"$gte": start_date, "$lte": end_date}},
            {"trade_date": 1, "close": 1},
        ).sort("trade_date", 1)
        docs = list(cursor)
        if not docs:
            stock_coll = self.db["volume_price"]
            symbol = ts_code[:-3]
            stock_cursor = stock_coll.find(
                {"symbol": symbol, "trade_date": {"$gte": start_date, "$lte": end_date}},
                {"trade_date": 1, "close": 1},
            ).sort("trade_date", 1)
            docs = list(stock_cursor)
            if not docs:
                return pd.Series(dtype=float, name=f"{ts_code}_norm")
        df = pd.DataFrame(docs)
        ser = pd.Series(df["close"].values, index=pd.to_datetime(df["trade_date"], format="%Y%m%d"), name=ts_code)
        base = ser.iloc[0] if not ser.empty else None
        if base in (None, 0):
            return pd.Series(dtype=float, name=f"{ts_code}_norm")
        return (ser / base).rename(f"{ts_code}_norm")

    def load_raw(self, ts_code: str, start_date: str, end_date: str) -> pd.Series:
        cursor = self.index_coll.find(
            {"ts_code": ts_code, "trade_date": {"$gte": start_date, "$lte": end_date}},
            {"trade_date": 1, "close": 1},
        ).sort("trade_date", 1)
        docs = list(cursor)
        if not docs:
            return pd.Series(dtype=float, name=ts_code)
        df = pd.DataFrame(docs)
        return pd.Series(df["close"].values, index=pd.to_datetime(df["trade_date"], format="%Y%m%d"), name=ts_code)
