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

    # -------- symbol resolution (now using symbol with suffix) --------
    def resolve_ts_code(self, symbol: str) -> Optional[str]:
        if symbol in self._sym_ts_cache:
            return self._sym_ts_cache[symbol]
        doc = self.info_coll.find_one({"symbol": symbol}, {"symbol": 1})
        # Now symbol field contains exchange suffix, so return the symbol itself
        resolved_symbol = doc.get("symbol") if doc else None
        if resolved_symbol:
            self._sym_ts_cache[symbol] = resolved_symbol
        return resolved_symbol

    def resolve_many(self, symbols: List[str]) -> Dict[str, Optional[str]]:
        missing = [s for s in symbols if s not in self._sym_ts_cache]
        if missing:
            docs = list(self.info_coll.find({"symbol": {"$in": missing}}, {"symbol": 1}))
            for d in docs:
                resolved_symbol = d.get("symbol")
                if resolved_symbol:
                    self._sym_ts_cache[d["symbol"]] = resolved_symbol
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
        # Check if pct_chg field exists in the collection
        sample_doc = self.price_coll.find_one(query)
        has_pct_chg = sample_doc and "pct_chg" in sample_doc
        
        # Define projection based on whether pct_chg field exists
        projection = {"symbol": 1, "trade_date": 1, "open": 1, "close": 1, "high": 1, "low": 1, "volume": 1}
        if has_pct_chg:
            projection["pct_chg"] = 1
        
        cursor = self.price_coll.find(
            query,
            projection,
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
            # Include pct_chg column if it exists
            columns = ["open", "high", "low", "close", "volume"]
            if "pct_chg" in df.columns:
                columns.append("pct_chg")
            out[sym] = df[columns]
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
        # Try to fetch with symbols (now with suffixes) first
        cursor = self.price_coll.find(
            {
                "symbol": {"$in": symbols},
                "trade_date": date_str,
            },
            {"symbol": 1, "close": 1},
        )
        docs = list(cursor)
        result = {}
        for d in docs:
            sym = d.get("symbol")
            if sym:
                result[sym] = d.get("close")
        
        # For symbols not found, try with stripped suffixes (backward compatibility)
        found_symbols = set(result.keys())
        symbols_not_found = [s for s in symbols if s not in found_symbols]
        
        if symbols_not_found:
            # Strip suffixes from symbols not found
            symbols_stripped = [s.split('.')[0] for s in symbols_not_found if '.' in s]
            symbols_stripped.extend([s for s in symbols_not_found if '.' not in s])  # Add symbols without suffix
            
            cursor = self.price_coll.find(
                {
                    "symbol": {"$in": symbols_stripped},
                    "trade_date": date_str,
                },
                {"symbol": 1, "close": 1},
            )
            docs = list(cursor)
            for d in docs:
                sym = d.get("symbol")
                if sym:
                    result[sym] = d.get("close")
        
        return result


    def fetch_market_spectrum(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch market spectrum (Yin/Yang) data.
        Returns DataFrame set index to trade_date (datetime).
        Columns: yin_spectrum, yang_spectrum, total_stocks
        """
        coll = self.db["market_spectrum"]
        cursor = coll.find(
            {"trade_date": {"$gte": start_date, "$lte": end_date}},
            {"trade_date": 1, "yin_spectrum": 1, "yang_spectrum": 1, "total_stocks": 1, "_id": 0}
        ).sort("trade_date", 1)
        
        docs = list(cursor)
        if not docs:
            return pd.DataFrame()
            
        df = pd.DataFrame(docs)
        # Convert YYYYMMDD string to datetime
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="mixed")
        df = df.set_index("trade_date").sort_index()
        return df


__all__ = ["StockPriceDataAccess"]
