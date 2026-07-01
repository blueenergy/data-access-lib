"""Stock volatility indicators (unadjusted close OHLCV).

All functions use raw (unadjusted) prices, consistent with ``quant_data.volume_price``.
Volatility definition matches ``cycle_scorer._calculate_volatility``:
daily pct-change std, optionally annualized with sqrt(252).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252


def _as_series(values: Union[pd.Series, Sequence[float]]) -> pd.Series:
    if isinstance(values, pd.Series):
        return values.astype(float)
    return pd.Series(list(values), dtype=float)


def _positive_closes(closes: pd.Series) -> pd.Series:
    return closes.replace(0, np.nan).dropna()


def realized_vol(
    closes: Union[pd.Series, Sequence[float]],
    window: int = 20,
    *,
    annualize: bool = True,
) -> Optional[float]:
    """Rolling realized volatility from close-to-close pct returns."""
    if window < 2:
        return None
    series = _positive_closes(_as_series(closes))
    if len(series) < window + 1:
        return None
    tail = series.tail(window + 1)
    returns = tail.pct_change().dropna()
    if returns.empty:
        return None
    vol = float(returns.std(ddof=1))
    if vol != vol:  # NaN
        return None
    if annualize:
        vol *= np.sqrt(TRADING_DAYS_PER_YEAR)
    return vol


def true_range_series(
    high: Union[pd.Series, Sequence[float]],
    low: Union[pd.Series, Sequence[float]],
    close: Union[pd.Series, Sequence[float]],
) -> pd.Series:
    """Wilder true range for each bar (needs aligned high/low/close)."""
    h = _as_series(high)
    low_s = _as_series(low)
    c = _as_series(close)
    prev_close = c.shift(1)
    tr = pd.concat(
        [
            (h - low_s).abs(),
            (h - prev_close).abs(),
            (low_s - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr


def atr(
    high: Union[pd.Series, Sequence[float]],
    low: Union[pd.Series, Sequence[float]],
    close: Union[pd.Series, Sequence[float]],
    window: int = 20,
) -> Optional[float]:
    """Average true range over the last ``window`` bars."""
    if window < 1:
        return None
    tr = true_range_series(high, low, close).dropna()
    if len(tr) < window:
        return None
    value = float(tr.tail(window).mean())
    return None if value != value else value


def atr_pct(
    high: Union[pd.Series, Sequence[float]],
    low: Union[pd.Series, Sequence[float]],
    close: Union[pd.Series, Sequence[float]],
    window: int = 20,
) -> Optional[float]:
    """ATR divided by the latest close (fraction, e.g. 0.08 = 8%)."""
    series = _positive_closes(_as_series(close))
    if series.empty:
        return None
    latest_close = float(series.iloc[-1])
    if latest_close <= 0:
        return None
    atr_value = atr(high, low, close, window=window)
    if atr_value is None:
        return None
    return atr_value / latest_close


def vol_percentile(
    closes: Union[pd.Series, Sequence[float]],
    window: int = 20,
    lookback: int = 250,
    *,
    annualize: bool = True,
) -> Optional[float]:
    """Percentile rank (0-100) of current ``window``-day vol vs rolling history."""
    series = _positive_closes(_as_series(closes))
    min_len = window + lookback
    if len(series) < min_len:
        return None
    tail = series.tail(min_len)
    rolling_vols: List[float] = []
    for end_idx in range(window, len(tail) + 1):
        chunk = tail.iloc[end_idx - window - 1 : end_idx]
        vol = realized_vol(chunk, window=window, annualize=annualize)
        if vol is not None:
            rolling_vols.append(vol)
    if len(rolling_vols) < 2:
        return None
    current = rolling_vols[-1]
    history = rolling_vols[:-1]
    rank = sum(1 for v in history if v <= current)
    return 100.0 * rank / len(history)


def abnormal_range_flag(
    high: Union[pd.Series, Sequence[float]],
    low: Union[pd.Series, Sequence[float]],
    close: Union[pd.Series, Sequence[float]],
    window: int = 20,
    multiplier: float = 2.0,
) -> bool:
    """True when the latest bar range exceeds ``multiplier`` * ATR."""
    h = _as_series(high)
    low_s = _as_series(low)
    c = _as_series(close)
    if len(c) < window + 1:
        return False
    atr_value = atr(h, low_s, c, window=window)
    if atr_value is None or atr_value <= 0:
        return False
    latest_range = float(h.iloc[-1] - low_s.iloc[-1])
    if latest_range != latest_range:
        return False
    return latest_range > multiplier * atr_value


def compute_volatility_features(
    rows: Sequence[Dict[str, Any]],
    *,
    vol_windows: Sequence[int] = (20, 60),
    atr_window: int = 20,
    percentile_lookback: int = 250,
) -> Dict[str, Any]:
    """Compute a feature dict from ascending OHLCV row dicts (``volume_price`` shape)."""
    if not rows:
        return {}
    frame = pd.DataFrame(rows)
    for col in ("open", "high", "low", "close"):
        if col not in frame.columns:
            return {}
    frame = frame.sort_values("trade_date")
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    close = frame["close"].astype(float)

    out: Dict[str, Any] = {
        "trade_date": str(frame["trade_date"].iloc[-1]) if "trade_date" in frame.columns else None,
        "close": float(close.iloc[-1]) if len(close) else None,
    }
    for window in vol_windows:
        vol = realized_vol(close, window=window, annualize=True)
        out[f"vol_{window}d_annual"] = vol
    out[f"atr_pct_{atr_window}d"] = atr_pct(high, low, close, window=atr_window)
    vol_pctl_window = vol_windows[0] if vol_windows else 20
    vol_pctl = vol_percentile(
        close,
        window=vol_pctl_window,
        lookback=percentile_lookback,
        annualize=True,
    )
    out[f"vol_percentile_{percentile_lookback}d"] = vol_pctl
    out["vol_percentile"] = vol_pctl
    out["abnormal_range_flag"] = abnormal_range_flag(high, low, close, window=atr_window)
    return out
