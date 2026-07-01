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

from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .mongo_context import get_db

PRICE_COLS = ("open", "high", "low", "close", "pre_close")
VALID_ADJUST = ("hfq", "qfq", "none")
FactorPair = Tuple[str, str]


def normalize_trade_date(trade_date) -> str:
    """Normalize ``trade_date`` to YYYYMMDD string."""
    if trade_date is None:
        return ""
    if hasattr(trade_date, "strftime"):
        return trade_date.strftime("%Y%m%d")
    s = str(trade_date).replace("-", "").strip()
    return s[:8] if len(s) >= 8 else s


def symbol_variants(symbol: str) -> List[str]:
    """Return symbol lookup variants (with/without exchange suffix)."""
    if not symbol:
        return []
    out: List[str] = [symbol]
    if "." in symbol:
        base = symbol.split(".", 1)[0]
        if base not in out:
            out.append(base)
    else:
        for suffix in (".SH", ".SZ", ".BJ"):
            candidate = f"{symbol}{suffix}"
            if candidate not in out:
                out.append(candidate)
    return out


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
        self._factor_cache: Dict[FactorPair, Optional[float]] = {}

    def factor_for(
        self,
        symbol: str,
        trade_date: str,
        variants: Optional[Sequence[str]] = None,
    ) -> Optional[float]:
        """Return ``adj_factor`` for ``(symbol, trade_date)`` with optional symbol variants."""
        td = normalize_trade_date(trade_date)
        if not symbol or not td:
            return None
        cache_key = (symbol, td)
        if cache_key in self._factor_cache:
            return self._factor_cache[cache_key]

        lookup_syms = list(variants) if variants else symbol_variants(symbol)
        for sym in lookup_syms:
            doc = self.adj_coll.find_one(
                {"symbol": sym, "trade_date": td},
                {"_id": 0, "adj_factor": 1},
            )
            if not doc:
                continue
            try:
                value = float(doc["adj_factor"])
                self._factor_cache[cache_key] = value
                return value
            except (TypeError, ValueError, KeyError):
                continue
        self._factor_cache[cache_key] = None
        return None

    def factor_map_for_pairs(
        self,
        pairs: Sequence[FactorPair],
        *,
        variants_by_symbol: Optional[Dict[str, Sequence[str]]] = None,
    ) -> Dict[FactorPair, float]:
        """Batch lookup ``adj_factor`` for ``(symbol, trade_date)`` pairs.

        Missing factors are omitted from the result (callers may degrade to raw
        close ratios). Uses per-instance cache to avoid duplicate queries.
        """
        from collections import defaultdict

        variants_by_symbol = variants_by_symbol or {}
        out: Dict[FactorPair, float] = {}
        pending: List[FactorPair] = []
        seen_pending: set = set()
        for symbol, trade_date in pairs:
            td = normalize_trade_date(trade_date)
            if not symbol or not td:
                continue
            key: FactorPair = (symbol, td)
            if key in self._factor_cache:
                cached = self._factor_cache[key]
                if cached is not None:
                    out[key] = cached
                continue
            if key in seen_pending:
                continue
            seen_pending.add(key)
            pending.append(key)

        if not pending:
            return out

        by_symbol: Dict[str, set] = defaultdict(set)
        for symbol, td in pending:
            by_symbol[symbol].add(td)

        for symbol, dates in by_symbol.items():
            lookup_syms = list(variants_by_symbol.get(symbol) or symbol_variants(symbol))
            if not lookup_syms:
                for td in dates:
                    self._factor_cache[(symbol, td)] = None
                continue

            cursor = self.adj_coll.find(
                {
                    "symbol": {"$in": lookup_syms},
                    "trade_date": {"$in": sorted(dates)},
                },
                {"_id": 0, "symbol": 1, "trade_date": 1, "adj_factor": 1},
            )
            raw_hits: Dict[Tuple[str, str], float] = {}
            for doc in cursor:
                td = normalize_trade_date(doc.get("trade_date"))
                doc_sym = str(doc.get("symbol") or "")
                if not td or not doc_sym:
                    continue
                try:
                    raw_hits[(doc_sym, td)] = float(doc["adj_factor"])
                except (TypeError, ValueError, KeyError):
                    continue

            for td in dates:
                pair: FactorPair = (symbol, td)
                factor: Optional[float] = None
                for sym in lookup_syms:
                    hit = raw_hits.get((sym, td))
                    if hit is not None:
                        factor = hit
                        break
                self._factor_cache[pair] = factor
                if factor is not None:
                    out[pair] = factor

        return out

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
    "normalize_trade_date",
    "symbol_variants",
    "PRICE_COLS",
    "VALID_ADJUST",
]
