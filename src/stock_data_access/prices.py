"""Adjusted price access: join raw ``volume_price`` with ``stock_adj_factor``.

``volume_price`` stores **raw (unadjusted)** OHLCV. ``stock_adj_factor`` stores
Tushare ``adj_factor`` per ``(symbol, trade_date)``. Adjusted prices are computed
at read time so we never physicalize an adjusted table and never store negatives:

    hfq_price = raw_price * adj_factor                       (backward-adjusted, positive)
    qfq_price = raw_price * adj_factor / latest_adj_factor   (forward-adjusted, for display)

Daily returns are identical for hfq and qfq. Volume/amount are left raw; consumers
that need turnover-consistent volume can derive it from ``adj_factor``.

When a symbol has no ``adj_factor`` at all, the loader degrades to raw prices and
flags it via ``df.attrs["adj_degraded"] = True`` (and ``adjust`` recorded in attrs).
"""

from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

from .mongo_context import get_db

PRICE_COLS = ("open", "high", "low", "close", "pre_close")
VALID_ADJUST = ("hfq", "qfq", "none")


def apply_adjustment(
    df: pd.DataFrame,
    adjust: str = "hfq",
    latest_factor: Optional[float] = None,
) -> pd.DataFrame:
    """Apply price adjustment to a raw OHLC frame that already carries ``adj_factor``.

    Pure function (no I/O) so it is trivially unit-testable.

    ``df`` must contain an ``adj_factor`` column plus any subset of ``PRICE_COLS``.
    Rows keep their order/index. Volume and other columns pass through unchanged.
    For ``adjust="qfq"`` a ``latest_factor`` must be provided (the symbol's most
    recent ``adj_factor``); otherwise it falls back to the max factor present.
    """
    if adjust not in VALID_ADJUST:
        raise ValueError(f"adjust must be one of {VALID_ADJUST}, got {adjust!r}")

    out = df.copy()
    if adjust == "none" or "adj_factor" not in out.columns:
        return out

    factor = pd.to_numeric(out["adj_factor"], errors="coerce")
    if factor.notna().sum() == 0:
        # No usable factor -> leave raw, mark degraded.
        out.attrs["adj_degraded"] = True
        return out

    if adjust == "qfq":
        denom = latest_factor if latest_factor else factor.dropna().iloc[-1]
        multiplier = factor / denom
    else:  # hfq
        multiplier = factor

    for col in PRICE_COLS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce") * multiplier

    return out


class AdjustedPriceDataAccess:
    """Read raw prices + adjustment factors and compute hfq/qfq at read time."""

    def __init__(self, db=None):
        # Explicit None check to avoid pymongo Database truthiness NotImplementedError.
        self.db = db if db is not None else get_db()
        self.price_coll = self.db["volume_price"]
        self.adj_coll = self.db["stock_adj_factor"]

    # -------- raw loaders --------
    def _load_raw(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        cursor = self.price_coll.find(
            {"symbol": symbol, "trade_date": {"$gte": start_date, "$lte": end_date}},
            {
                "_id": 0,
                "symbol": 1,
                "trade_date": 1,
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "pre_close": 1,
                "volume": 1,
                "amount": 1,
            },
        ).sort("trade_date", 1)
        return pd.DataFrame(list(cursor))

    def _load_factors(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        cursor = self.adj_coll.find(
            {"symbol": symbol, "trade_date": {"$gte": start_date, "$lte": end_date}},
            {"_id": 0, "trade_date": 1, "adj_factor": 1},
        ).sort("trade_date", 1)
        return pd.DataFrame(list(cursor))

    def latest_factor(self, symbol: str, as_of_date: Optional[str] = None) -> Optional[float]:
        """Return the symbol's most recent ``adj_factor`` on or before ``as_of_date``.

        When ``as_of_date`` is omitted, uses the latest factor across all dates.
        For qfq display on a historical window, pass the query ``end_date`` so the
        anchor matches Tushare ``pro_bar(adj='qfq', end_date=...)``.
        """
        query: Dict[str, object] = {"symbol": symbol}
        if as_of_date:
            query["trade_date"] = {"$lte": as_of_date}
        doc = self.adj_coll.find_one(
            query,
            {"_id": 0, "adj_factor": 1},
            sort=[("trade_date", -1)],
        )
        if not doc:
            return None
        try:
            return float(doc["adj_factor"])
        except (TypeError, ValueError, KeyError):
            return None

    # -------- public API --------
    def load_adjusted_ohlc(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = "hfq",
    ) -> pd.DataFrame:
        """Return adjusted OHLC for ``symbol`` in ``[start_date, end_date]``.

        Returns a DataFrame indexed by ``trade_date`` (datetime) with adjusted
        price columns, raw ``volume``/``amount``, and the joined ``adj_factor``.
        Empty DataFrame if no raw prices exist. Sets ``df.attrs["adjust"]`` and
        ``df.attrs["adj_degraded"]``.
        """
        if adjust not in VALID_ADJUST:
            raise ValueError(f"adjust must be one of {VALID_ADJUST}, got {adjust!r}")

        raw = self._load_raw(symbol, start_date, end_date)
        if raw.empty:
            empty = pd.DataFrame()
            empty.attrs["adjust"] = adjust
            empty.attrs["adj_degraded"] = False
            return empty

        factors = self._load_factors(symbol, start_date, end_date)
        if factors.empty:
            merged = raw.copy()
            merged["adj_factor"] = pd.NA
        else:
            merged = raw.merge(factors, on="trade_date", how="left")
            # adj_factor is piecewise-constant; fill gaps within the window.
            merged["adj_factor"] = merged["adj_factor"].ffill().bfill()

        latest = self.latest_factor(symbol, as_of_date=end_date) if adjust == "qfq" else None
        adjusted = apply_adjustment(merged, adjust=adjust, latest_factor=latest)

        adjusted = adjusted.set_index(
            pd.to_datetime(adjusted["trade_date"], format="mixed")
        ).sort_index()
        adjusted.attrs["adjust"] = adjust
        adjusted.attrs["adj_degraded"] = bool(adjusted.attrs.get("adj_degraded", False))
        return adjusted

    def load_adjusted_batch(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        adjust: str = "hfq",
    ) -> Dict[str, pd.DataFrame]:
        """Convenience batch wrapper returning ``{symbol: adjusted DataFrame}``."""
        return {
            s: self.load_adjusted_ohlc(s, start_date, end_date, adjust=adjust)
            for s in symbols
        }


def load_adjusted_ohlc(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust: str = "hfq",
    db=None,
) -> pd.DataFrame:
    """Module-level convenience wrapper around :class:`AdjustedPriceDataAccess`."""
    return AdjustedPriceDataAccess(db=db).load_adjusted_ohlc(
        symbol, start_date, end_date, adjust=adjust
    )


__all__ = [
    "AdjustedPriceDataAccess",
    "apply_adjustment",
    "load_adjusted_ohlc",
    "PRICE_COLS",
    "VALID_ADJUST",
]
