"""Resample A-share 1m bars to 30m / 60m without crossing lunch.

QMT / Tonghuashun 60m labels are session buckets, not clock hours:
10:30, 11:30, 14:00, 15:00. 30m labels are 10:00 / 10:30 / 11:00 / 11:30
then 13:30 / 14:00 / 14:30 / 15:00.

``is_partial`` is true only for the last bucket in the series when its last
1m close is still before the bucket end. Earlier buckets with a later 1m
(even in the next bucket) are treated as closed so small historical gaps
do not suppress G/S.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

SUPPORTED_INTRADAY_TF = ("30m", "60m")

# (start_hhmm inclusive, end_hhmm inclusive, label_hhmm)
_BUCKETS: Dict[str, Tuple[Tuple[int, int, int], ...]] = {
    "30m": (
        (930, 1000, 1000),
        (1001, 1030, 1030),
        (1031, 1100, 1100),
        (1101, 1130, 1130),
        (1300, 1330, 1330),
        (1331, 1400, 1400),
        (1401, 1430, 1430),
        (1431, 1500, 1500),
    ),
    "60m": (
        (930, 1030, 1030),
        (1031, 1130, 1130),
        (1300, 1400, 1400),
        (1401, 1500, 1500),
    ),
}


def normalize_intraday_tf(tf: Optional[str]) -> str:
    """Map UI / API aliases to ``1m`` / ``30m`` / ``60m``."""
    text = str(tf or "1m").strip().lower()
    if text in ("1m", "minute", "min"):
        return "1m"
    if text in ("30m", "30min"):
        return "30m"
    if text in ("60m", "60min", "1h"):
        return "60m"
    raise ValueError(f"unsupported intraday tf: {tf!r}")


def bucket_end_hhmm(hhmm: int, tf: str) -> Optional[int]:
    """Return the session-bucket close HHMM for a 1m bar, or None if outside hours."""
    normalized = normalize_intraday_tf(tf)
    if normalized == "1m":
        return None
    for start, end, label in _BUCKETS[normalized]:
        if start <= hhmm <= end:
            return label
    return None


def _to_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _digits(raw: Any) -> str:
    return "".join(ch for ch in str(raw or "").strip() if ch.isdigit())


def parse_minute_trade_date(raw: Any) -> Optional[Tuple[str, int]]:
    """Return ``(YYYYMMDD, HHMM)`` from ``YYYYMMDDHHMM`` (int or str)."""
    digits = _digits(raw)
    if len(digits) < 12:
        return None
    ymd = digits[:8]
    try:
        hhmm = int(digits[8:12])
    except ValueError:
        return None
    return ymd, hhmm


def _bar_volume(row: Dict[str, Any]) -> float:
    volume = _to_float(row.get("volume"))
    if volume is None:
        volume = _to_float(row.get("vol"))
    return volume if volume is not None else 0.0


def resample_ashare_intraday(
    bars: Iterable[Dict[str, Any]],
    tf: str,
) -> List[Dict[str, Any]]:
    """Aggregate chronological 1m OHLCV into 30m or 60m session buckets.

    Lunch 11:31–12:59 is dropped (not crossed). Output ``trade_date`` is
    ``YYYYMMDDHHMM`` at the bucket close. Order is oldest-first.
    """
    normalized = normalize_intraday_tf(tf)
    if normalized == "1m":
        return [dict(row) for row in bars]
    if normalized not in _BUCKETS:
        raise ValueError(f"unsupported intraday tf: {tf!r}")

    grouped: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    order: List[Tuple[str, int]] = []
    for row in bars:
        parsed = parse_minute_trade_date(row.get("trade_date") or row.get("date"))
        if parsed is None:
            continue
        if _to_float(row.get("close")) is None:
            continue
        ymd, hhmm = parsed
        label = bucket_end_hhmm(hhmm, normalized)
        if label is None:
            continue
        key = (ymd, label)
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(row)

    out: List[Dict[str, Any]] = []
    last_index = len(order) - 1
    for idx, key in enumerate(order):
        members = grouped[key]
        members.sort(key=lambda item: _digits(item.get("trade_date") or item.get("date")))
        ymd, label = key
        last_parsed = parse_minute_trade_date(members[-1].get("trade_date"))
        last_hhmm = last_parsed[1] if last_parsed else 0
        reached_end = last_hhmm >= label
        has_later_bucket = idx < last_index
        is_partial = not reached_end and not has_later_bucket

        highs = [_to_float(row.get("high")) for row in members]
        lows = [_to_float(row.get("low")) for row in members]
        high_vals = [value for value in highs if value is not None]
        low_vals = [value for value in lows if value is not None]
        first = members[0]
        last = members[-1]
        out.append(
            {
                "symbol": first.get("symbol"),
                "trade_date": f"{ymd}{label:04d}",
                "open": _to_float(first.get("open")),
                "high": max(high_vals) if high_vals else _to_float(first.get("high")),
                "low": min(low_vals) if low_vals else _to_float(first.get("low")),
                "close": _to_float(last.get("close")),
                "volume": sum(_bar_volume(row) for row in members),
                "is_partial": is_partial,
                "source_bars": len(members),
            }
        )
    return out


def filter_bars_by_ymd(
    bars: Sequence[Dict[str, Any]],
    start_ymd: Optional[str] = None,
    end_ymd: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Keep bars whose ``trade_date`` day is inside ``[start_ymd, end_ymd]``."""
    start = _digits(start_ymd)[:8] if start_ymd else ""
    end = _digits(end_ymd)[:8] if end_ymd else ""
    if not start and not end:
        return list(bars)
    out: List[Dict[str, Any]] = []
    for row in bars:
        parsed = parse_minute_trade_date(row.get("trade_date"))
        if parsed is None:
            day = _digits(row.get("trade_date"))[:8]
        else:
            day = parsed[0]
        if start and day < start:
            continue
        if end and day > end:
            continue
        out.append(row)
    return out
