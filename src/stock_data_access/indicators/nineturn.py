"""Nine-turn (TD Sequential style) signal enrichment helpers."""
from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Optional


def _normalize_date(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else text


def _to_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _is_nine_up(value: Any) -> bool:
    text = str(value or "").strip()
    number = _to_float(value)
    return text == "+9" or text == "9" or number == 9


def _is_nine_down(value: Any) -> bool:
    text = str(value or "").strip()
    number = _to_float(value)
    return text == "-9" or number == -9


def _moving_average(rows: List[Dict[str, Any]], idx: int, field: str, window: int) -> Optional[float]:
    if idx + 1 < window:
        return None
    values = [_to_float(item.get(field)) for item in rows[idx + 1 - window : idx + 1]]
    if any(value is None for value in values):
        return None
    return sum(value for value in values if value is not None) / window


def _perfect_down(window_rows: List[Dict[str, Any]]) -> bool:
    day6, day7, day8, day9 = window_rows[5], window_rows[6], window_rows[7], window_rows[8]
    lows = [_to_float(row.get("low")) for row in (day6, day7, day8, day9)]
    if any(value is None for value in lows):
        return False
    low6, low7, low8, low9 = lows
    return bool((low8 < low6 and low8 < low7) or (low9 < low6 and low9 < low7))


def _perfect_up(window_rows: List[Dict[str, Any]]) -> bool:
    day6, day7, day8, day9 = window_rows[5], window_rows[6], window_rows[7], window_rows[8]
    highs = [_to_float(row.get("high")) for row in (day6, day7, day8, day9)]
    if any(value is None for value in highs):
        return False
    high6, high7, high8, high9 = highs
    return bool((high8 > high6 and high8 > high7) or (high9 > high6 and high9 > high7))


def _merge_rows(
    bars: Iterable[Dict[str, Any]],
    counts: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows_by_date: Dict[str, Dict[str, Any]] = {}
    for bar in bars:
        date = _normalize_date(bar.get("trade_date") or bar.get("date"))
        if date:
            rows_by_date[date] = {**bar, "trade_date": date}
    for count in counts:
        date = _normalize_date(count.get("trade_date") or count.get("date"))
        if date:
            rows_by_date[date] = {**rows_by_date.get(date, {}), **count, "trade_date": date}
    return sorted(rows_by_date.values(), key=lambda item: str(item.get("trade_date") or ""))


def enrich_nineturn_signals(
    bars: Iterable[Dict[str, Any]],
    counts: Iterable[Dict[str, Any]],
    *,
    vol_ma: int = 5,
    trend_ma: int = 60,
    vol_bottom_ratio: float = 0.8,
    vol_top_ratio: float = 1.2,
) -> List[Dict[str, Any]]:
    """Return enriched completed nine-turn signals.

    ``bars`` and ``counts`` are both expected in daily granularity. They may be
    passed separately and are joined by normalized ``trade_date``.
    """
    rows = _merge_rows(bars, counts)
    signals: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows):
        direction: Optional[str] = None
        if _is_nine_down(row.get("nine_down_turn")) or _to_float(row.get("down_count")) == 9:
            direction = "down"
        elif _is_nine_up(row.get("nine_up_turn")) or _to_float(row.get("up_count")) == 9:
            direction = "up"
        if direction is None:
            continue

        window_rows = rows[idx - 8 : idx + 1] if idx >= 8 else []
        perfect = False
        if len(window_rows) == 9:
            perfect = _perfect_down(window_rows) if direction == "down" else _perfect_up(window_rows)

        vol = _to_float(row.get("vol") if row.get("vol") is not None else row.get("volume"))
        ma_vol = _moving_average(rows, idx, "vol", vol_ma)
        if ma_vol is None:
            ma_vol = _moving_average(rows, idx, "volume", vol_ma)
        close = _to_float(row.get("close"))
        ma_trend = _moving_average(rows, idx, "close", trend_ma)

        if direction == "down":
            vol_filter_pass = None if vol is None or ma_vol in (None, 0) else vol <= ma_vol * vol_bottom_ratio
            trend_filter_pass = None if close is None or ma_trend is None else close > ma_trend
            strong = bool(perfect and vol_filter_pass and trend_filter_pass)
            label = "下九转"
            signal_type = "bottom"
        else:
            vol_filter_pass = None if vol is None or ma_vol in (None, 0) else vol >= ma_vol * vol_top_ratio
            trend_filter_pass = None
            strong = bool(perfect and vol_filter_pass)
            label = "上九转"
            signal_type = "top"

        grade = "strong" if strong else ("perfect" if perfect else "normal")
        strength = 1 + (1 if perfect else 0) + (1 if vol_filter_pass else 0) + (
            1 if direction == "down" and trend_filter_pass else 0
        )

        signals.append(
            {
                "trade_date": row.get("trade_date"),
                "direction": direction,
                "signal_type": signal_type,
                "label": label,
                "count": 9,
                "perfect": perfect,
                "vol_filter_pass": vol_filter_pass,
                "trend_filter_pass": trend_filter_pass,
                "volume": vol,
                "volume_ma": ma_vol,
                "trend_ma": ma_trend,
                "close": close,
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "grade": grade,
                "grade_label": {"normal": "普通", "perfect": "完美", "strong": "强"}.get(grade, "普通"),
                "strength": strength,
                "raw": {
                    "up_count": row.get("up_count"),
                    "down_count": row.get("down_count"),
                    "nine_up_turn": row.get("nine_up_turn"),
                    "nine_down_turn": row.get("nine_down_turn"),
                },
            }
        )

    return signals


def latest_signal_summary(signals: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    ordered = sorted(signals, key=lambda item: str(item.get("trade_date") or ""), reverse=True)
    if not ordered:
        return None
    latest = dict(ordered[0])
    latest["summary"] = f"{latest.get('label', '九转')}·{latest.get('grade_label', '普通')}"
    return latest


__all__ = ["enrich_nineturn_signals", "latest_signal_summary"]
