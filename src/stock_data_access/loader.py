"""Reusable Stock price data access layer.
Provides unified batch/single retrieval from Mongo collections.
This module is self-contained and reads Mongo connection from env by default.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Union

import pandas as pd

from .mongo_context import get_db


class StockPriceDataAccess:
    def __init__(self, db=None, minute: bool = False):
        # Explicit None check to prevent pymongo Database truthiness NotImplementedError
        self.db = db if db is not None else get_db()
        self.price_coll = self.db["minute_bars" if minute else "volume_price"]
        self.info_coll = self.db["stock_info"]
        self._sym_ts_cache: Dict[str, str] = {}

    # -------- symbol <-> ts_code resolution --------
    def resolve_ts_code(self, symbol: str) -> Optional[str]:
        if symbol in self._sym_ts_cache:
            return self._sym_ts_cache[symbol]
        doc = self.info_coll.find_one({"symbol": symbol}, {"ts_code": 1})
        ts = doc.get("ts_code") if doc else None
        if ts:
            self._sym_ts_cache[symbol] = ts
        return ts

    def resolve_many(self, symbols: List[str]) -> Dict[str, Optional[str]]:
        missing = [s for s in symbols if s not in self._sym_ts_cache]
        if missing:
            docs = list(self.info_coll.find({"symbol": {"$in": missing}}, {"symbol": 1, "ts_code": 1}))
            for d in docs:
                ts = d.get("ts_code")
                if ts:
                    self._sym_ts_cache[d["symbol"]] = ts
        return {s: self._sym_ts_cache.get(s) for s in symbols}

    # -------- batch retrieval --------
    def fetch_names(self, symbols: List[str]) -> Dict[str, str]:
        """Return mapping symbol -> name for provided symbols.
        Missing names will map to empty string. Uses single query with $in.
        """
        if not symbols:
            return {}
        docs = list(self.info_coll.find({"symbol": {"$in": symbols}}, {"symbol": 1, "name": 1}))
        return {d.get("symbol"): d.get("name", "") for d in docs if d.get("symbol")}

    def fetch_batch(self, symbols: List[str], start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """Return dict(symbol -> OHLCV DataFrame) using symbol and trade_date range.
        Supports minute bars (e.g., trade_date like 202511301430) and daily bars.
        """
        if not symbols:
            return {}
        query = {
            "symbol": {"$in": symbols},
            "trade_date": {"$gte": start_date, "$lte": end_date},
        }
        cursor = self.price_coll.find(
            query,
            {"symbol": 1, "trade_date": 1, "open": 1, "close": 1, "high": 1, "low": 1, "volume": 1},
        ).sort([("symbol", 1), ("trade_date", 1)])
        docs = list(cursor)
        grouped: Dict[str, List[dict]] = {}
        for d in docs:
            sym = d.get("symbol")
            if not sym:
                continue
            grouped.setdefault(sym, []).append(d)
        out: Dict[str, pd.DataFrame] = {}
        for sym, gdocs in grouped.items():
            df = pd.DataFrame(gdocs)
            # mixed will handle pure YYYYMMDD and YYYYMMDDHHMM by auto-detection
            dt_index = pd.to_datetime(df["trade_date"], format="mixed")
            df = df.set_index(dt_index).sort_index()
            out[sym] = df[["open", "high", "low", "close", "volume"]]
        return out

    def fetch_frame(self, symbols: List[str], start_date: str, end_date: str, forward_fill: bool = True) -> pd.DataFrame:
        price_map = self.fetch_batch(symbols, start_date, end_date)
        if not price_map:
            return pd.DataFrame()
        df = pd.concat(price_map.values(), axis=1)
        if forward_fill:
            df = df.ffill()
        return df.dropna(how="all")

    def fetch_latest_close(self, symbols: List[str], date_str: str) -> Dict[str, float]:
        sym_ts = self.resolve_many(symbols)
        ts_list = [ts for ts in sym_ts.values() if ts]
        if not ts_list:
            return {}
        cursor = self.price_coll.find(
            {
                "ts_code": {"$in": ts_list},
                "trade_date": date_str,
            },
            {"ts_code": 1, "close": 1},
        )
        docs = list(cursor)
        ts_to_symbol = {v: k for k, v in sym_ts.items() if v}
        result = {}
        for d in docs:
            sym = ts_to_symbol.get(d.get("ts_code"))
            if sym:
                result[sym] = d.get("close")
        return result


__all__ = ["StockPriceDataAccess"]
