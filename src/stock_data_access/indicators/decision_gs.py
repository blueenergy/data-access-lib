"""Causal approximation of Tonghuashun 决策先锋 G/S overlays.

This is **not** the closed-source 决策先锋 formula. v1 is a published, causal
skeleton: weighted typical price → EMA(39)/EMA(99) → slope-turn G/S.

Signals are events on *closed* bars only. Partial / in-progress bars still
receive overlay values but never emit G, S, or ``s_watch``, so historical
marks do not repaint when later bars arrive.

``s_watch`` is an early warning, not a replacement for S: close is already
below ``mj20`` while the ``mj20`` slope has not turned negative yet. Official
S still requires the slope turn.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Optional, Sequence

FORMULA_ID = "decision_gs_v1"
DEFAULT_MJ20_SPAN = 39
DEFAULT_MJ30_SPAN = 99
DEFAULT_SLOPE_WINDOW = 8


def _to_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _trade_date(row: Dict[str, Any]) -> str:
    raw = row.get("trade_date") or row.get("date") or ""
    text = str(raw).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits if digits else text


def typical_price(row: Dict[str, Any]) -> Optional[float]:
    """Weighted typical price ``(H + L + O + 2C) / 5``."""
    high = _to_float(row.get("high"))
    low = _to_float(row.get("low"))
    open_ = _to_float(row.get("open"))
    close = _to_float(row.get("close"))
    if None in (high, low, open_, close):
        return None
    return (high + low + open_ + 2.0 * close) / 5.0


def _ema(values: Sequence[Optional[float]], span: int) -> List[Optional[float]]:
    """Causal EMA matching ``Series.ewm(span=span, adjust=False)``.

    The first finite value seeds the EMA. Subsequent missing inputs carry the
    previous EMA forward so overlay lines do not drop out on a single gap.
    """
    if span < 1:
        raise ValueError(f"span must be >= 1, got {span}")
    alpha = 2.0 / (span + 1)
    out: List[Optional[float]] = []
    prev: Optional[float] = None
    for value in values:
        if value is None:
            out.append(prev)
            continue
        if prev is None:
            prev = value
        else:
            prev = alpha * value + (1.0 - alpha) * prev
        out.append(prev)
    return out


def _linear_slope(window: Sequence[float]) -> Optional[float]:
    """OLS slope of ``window`` against ``0..n-1`` (talib.SLOPE-style)."""
    n = len(window)
    if n < 2:
        return None
    sum_x = (n - 1) * n / 2.0
    sum_x2 = (n - 1) * n * (2 * n - 1) / 6.0
    sum_y = 0.0
    sum_xy = 0.0
    for idx, value in enumerate(window):
        sum_y += value
        sum_xy += idx * value
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return None
    return (n * sum_xy - sum_x * sum_y) / denom


def compute_decision_gs(
    bars: Iterable[Dict[str, Any]],
    *,
    mj20_span: int = DEFAULT_MJ20_SPAN,
    mj30_span: int = DEFAULT_MJ30_SPAN,
    slope_window: int = DEFAULT_SLOPE_WINDOW,
    formula_id: str = FORMULA_ID,
) -> List[Dict[str, Any]]:
    """Return per-bar overlay + G/S flags in the same order as ``bars``.

    ``bars`` must already be chronological (oldest first). G/S require
    ``mj30_span`` bars of warmup plus a defined slope turn, and are suppressed
    when ``is_partial`` is true. ``watch='s'`` can fire earlier: close below
    ``mj20`` while slope is still non-negative in a bull overlay.
    """
    rows = list(bars)
    jcx = [typical_price(row) for row in rows]
    mj20 = _ema(jcx, mj20_span)
    mj30 = _ema(jcx, mj30_span)
    warmup_idx = max(mj30_span, slope_window) - 1

    slopes: List[Optional[float]] = []
    for idx, value in enumerate(mj20):
        if idx + 1 < slope_window:
            slopes.append(None)
            continue
        window = mj20[idx + 1 - slope_window : idx + 1]
        if any(item is None for item in window):
            slopes.append(None)
            continue
        slopes.append(_linear_slope([item for item in window if item is not None]))

    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        close = _to_float(row.get("close"))
        line20 = mj20[idx]
        line30 = mj30[idx]
        slope = slopes[idx]
        prev_slope = slopes[idx - 1] if idx > 0 else None
        is_partial = bool(row.get("is_partial"))
        warmed = idx >= warmup_idx and line20 is not None and line30 is not None
        is_bull: Optional[bool] = None
        if line20 is not None and line30 is not None:
            is_bull = line20 >= line30

        signal: Optional[str] = None
        watch: Optional[str] = None
        closed_ready = (
            warmed
            and not is_partial
            and close is not None
            and line20 is not None
            and slope is not None
        )
        if closed_ready and prev_slope is not None:
            if slope > 0 and prev_slope <= 0 and close > line20:
                signal = "g"
            elif slope < 0 and prev_slope >= 0 and close < line20:
                signal = "s"
        if closed_ready and is_bull is True and close < line20 and slope >= 0:
            watch = "s"

        out.append(
            {
                "trade_date": _trade_date(row) or row.get("trade_date"),
                "jcx": jcx[idx],
                "mj20": line20,
                "mj30": line30,
                "slope": slope,
                "is_bull": is_bull,
                "signal": signal,
                "watch": watch,
                "warmup": not warmed,
                "formula_id": formula_id,
            }
        )
    return out


def attach_decision_gs(
    rows: Sequence[Dict[str, Any]],
    **params: Any,
) -> List[Dict[str, Any]]:
    """Copy ``rows`` (any order) and stamp v1 overlay / G/S fields.

    Computation always runs on chronological OHLC; the returned list keeps the
    input order so newest-first workbench payloads stay intact.
    """
    if not rows:
        return []
    indexed = list(enumerate(rows))
    chrono = sorted(
        indexed,
        key=lambda item: (_trade_date(item[1]), item[0]),
    )
    computed = compute_decision_gs((row for _, row in chrono), **params)
    by_index = {origin_idx: extra for (origin_idx, _), extra in zip(chrono, computed)}
    attached: List[Dict[str, Any]] = []
    for origin_idx, row in indexed:
        extra = by_index[origin_idx]
        merged = dict(row)
        merged["mj20"] = extra.get("mj20")
        merged["mj30"] = extra.get("mj30")
        merged["gs_is_bull"] = extra.get("is_bull")
        merged["gs_signal"] = extra.get("signal")
        merged["gs_watch"] = extra.get("watch")
        merged["gs_warmup"] = extra.get("warmup")
        merged["gs_formula_id"] = extra.get("formula_id")
        attached.append(merged)
    return attached


__all__ = [
    "DEFAULT_MJ20_SPAN",
    "DEFAULT_MJ30_SPAN",
    "DEFAULT_SLOPE_WINDOW",
    "FORMULA_ID",
    "attach_decision_gs",
    "compute_decision_gs",
    "typical_price",
]
