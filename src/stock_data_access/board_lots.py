"""Board-aware share sizing for A-share order construction.

Different A-share boards impose different per-order (单笔申报) size rules:

* Main board (沪/深主板), ChiNext (创业板, 300/301), BSE (北交所): orders trade in
  round lots of ``lot_size`` (typically 100 shares).
* STAR market (科创板, 688/689 on SSE): a single limit/market order must be at
  least ``STAR_MIN_ORDER_SHARES`` (200) shares; above that it may increase in
  1-share increments. When a position drops below the minimum, the remaining
  odd lot may only be cleared in a single full-liquidation order.

Placing a sub-200 STAR order gets rejected (废单) by the broker counter, so
sizing must enforce these rules before a signal is published. This module is
the single source of truth shared by the plan generator (stock-scoring-system)
and the live-signal builder (quantFinance).
"""

from __future__ import annotations

from typing import List, Optional, Tuple

__all__ = [
    "STAR_MIN_ORDER_SHARES",
    "STAR_ADDON_BUMP_THRESHOLD",
    "DEFAULT_LOT_SIZE",
    "BELOW_LOT_SIZE_BLOCKER",
    "BELOW_STAR_MIN_BLOCKER",
    "is_star_market",
    "board_min_order_shares",
    "round_board_lot_shares",
    "normalize_board_target_shares",
]

# Minimum single-order quantity for STAR market (科创板) limit/market orders.
STAR_MIN_ORDER_SHARES = 200
# Sub-minimum STAR buys are only bumped up to the 200-share minimum when the
# intended delta is at least this many shares; smaller deltas are skipped so a
# tiny weight tweak does not get inflated into a >100% overshoot.
STAR_ADDON_BUMP_THRESHOLD = 100
DEFAULT_LOT_SIZE = 100

BELOW_LOT_SIZE_BLOCKER = "below_lot_size"
BELOW_STAR_MIN_BLOCKER = "below_star_min_order"


def _symbol_code(symbol: object) -> str:
    """Return the numeric code portion of a symbol like ``688627.SH``."""
    return str(symbol or "").strip().split(".")[0]


def is_star_market(symbol: object) -> bool:
    """True for STAR market (科创板) symbols, i.e. SSE codes ``688``/``689``."""
    return _symbol_code(symbol).startswith(("688", "689"))


def board_min_order_shares(symbol: object, lot_size: int = DEFAULT_LOT_SIZE) -> int:
    """Minimum tradable shares for a single order on the symbol's board."""
    if is_star_market(symbol):
        return STAR_MIN_ORDER_SHARES
    return max(1, int(lot_size or 1))


def round_board_lot_shares(
    amount: float,
    price: Optional[float],
    lot_size: int,
    symbol: object,
) -> int:
    """Snap a target notional to a board-tradable share count.

    STAR names use 1-share granularity (order-min is enforced separately by
    :func:`normalize_board_target_shares`); every other board floors to
    ``lot_size`` round lots. Returns ``0`` when the notional or price is
    non-positive.
    """
    if not price or price <= 0 or amount <= 0:
        return 0
    raw = int(amount // price)
    if is_star_market(symbol):
        return raw
    lot = max(1, int(lot_size or 1))
    return (raw // lot) * lot


def normalize_board_target_shares(
    current_shares: int,
    desired_target_shares: int,
    price: Optional[float],
    lot_size: int,
    symbol: object,
    available_cash: Optional[float] = None,
) -> Tuple[int, List[str]]:
    """Adjust a desired target position to a board-legal target.

    Returns ``(final_target_shares, blockers)``. The resulting *order* is the
    difference between ``final_target_shares`` and ``current_shares``.

    Non-STAR boards keep the legacy round-lot delta behavior. STAR (科创板)
    orders honor the 200-share single-order minimum:

    * Buy delta >= 200: kept as-is (1-share increments allowed).
    * Buy delta in [100, 200): bumped up to a 200-share order when
      ``available_cash`` covers it (``available_cash is None`` means unknown =>
      allowed); otherwise the buy is skipped with ``below_star_min_order``.
    * Buy delta in (0, 100): skipped with ``below_star_min_order`` (too small to
      justify inflating into a 200-share order).
    * Sell that would leave an odd lot < 200 (or targets a full exit): the whole
      position is liquidated in one order.
    * Sell delta in (0, 200) that still leaves >= 200: skipped with
      ``below_star_min_order`` (too small to submit, position kept).
    """
    current = max(0, int(current_shares or 0))
    desired = max(0, int(desired_target_shares or 0))
    lot = max(1, int(lot_size or 1))
    blockers: List[str] = []

    if not is_star_market(symbol):
        if desired == current or desired == 0:
            return desired, blockers
        delta = desired - current
        if delta > 0:
            rounded_delta = (delta // lot) * lot
            if rounded_delta <= 0:
                blockers.append(BELOW_LOT_SIZE_BLOCKER)
                return current, blockers
            return current + rounded_delta, blockers
        sell_qty = (abs(delta) // lot) * lot
        if sell_qty <= 0:
            blockers.append(BELOW_LOT_SIZE_BLOCKER)
            return current, blockers
        return current - sell_qty, blockers

    # --- STAR market (688/689) ---
    if desired == current:
        return current, blockers
    delta = desired - current
    if delta > 0:
        if delta >= STAR_MIN_ORDER_SHARES:
            return desired, blockers
        if delta < STAR_ADDON_BUMP_THRESHOLD:
            # Too small to justify inflating into a full 200-share order.
            blockers.append(BELOW_STAR_MIN_BLOCKER)
            return current, blockers
        # Sub-minimum buy: bump to a 200-share order if the cash budget allows.
        priced = price is not None and price > 0
        need_cash = STAR_MIN_ORDER_SHARES * float(price) if priced else None
        affordable = available_cash is None or (
            need_cash is not None and float(available_cash) >= need_cash
        )
        if priced and affordable:
            return current + STAR_MIN_ORDER_SHARES, blockers
        blockers.append(BELOW_STAR_MIN_BLOCKER)
        return current, blockers

    # Sell (delta < 0).
    remaining = desired
    if remaining < STAR_MIN_ORDER_SHARES:
        # Full exit, or clearing a sub-200 odd lot in one liquidation order.
        return 0, blockers
    sell_qty = current - desired
    if sell_qty >= STAR_MIN_ORDER_SHARES:
        return desired, blockers
    blockers.append(BELOW_STAR_MIN_BLOCKER)
    return current, blockers
