from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


_PROB_MIN = Decimal("0")
_PROB_MAX = Decimal("1")


def _as_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        candidate = value
    elif isinstance(value, (int, float)):
        candidate = Decimal(str(value))
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            candidate = Decimal(text)
        except InvalidOperation:
            return None
    if candidate.is_nan() or candidate.is_infinite():
        return None
    return candidate


def _as_float(value: Decimal | None, *, places: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), places)


def clamp_probability_price(price: Decimal) -> Decimal:
    if price < _PROB_MIN:
        return _PROB_MIN
    if price > _PROB_MAX:
        return _PROB_MAX
    return price


def reciprocal_price(price: Decimal) -> Decimal:
    return clamp_probability_price(_PROB_MAX - clamp_probability_price(price))


def _parse_book_levels(levels: Any) -> list[tuple[Decimal, Decimal | None]]:
    parsed: list[tuple[Decimal, Decimal | None]] = []
    if not isinstance(levels, list):
        return parsed
    for level in levels:
        if not isinstance(level, list) or len(level) < 2:
            continue
        price = _as_decimal(level[0])
        size = _as_decimal(level[1])
        if price is None:
            continue
        parsed.append((clamp_probability_price(price), size))
    return parsed


def _best_bid(levels: list[tuple[Decimal, Decimal | None]]) -> tuple[Decimal | None, Decimal | None]:
    if not levels:
        return None, None
    price, size = max(levels, key=lambda item: item[0])
    return price, size


def derive_top_of_book(orderbook_fp: dict[str, Any]) -> dict[str, float | None]:
    yes_levels = _parse_book_levels(orderbook_fp.get("yes_dollars"))
    no_levels = _parse_book_levels(orderbook_fp.get("no_dollars"))

    best_yes_bid, best_yes_bid_size = _best_bid(yes_levels)
    best_no_bid, best_no_bid_size = _best_bid(no_levels)

    best_yes_ask = reciprocal_price(best_no_bid) if best_no_bid is not None else None
    best_no_ask = reciprocal_price(best_yes_bid) if best_yes_bid is not None else None

    yes_spread = None
    if best_yes_bid is not None and best_yes_ask is not None and best_yes_ask >= best_yes_bid:
        yes_spread = best_yes_ask - best_yes_bid

    yes_midpoint = None
    if best_yes_bid is not None and best_yes_ask is not None:
        yes_midpoint = (best_yes_bid + best_yes_ask) / Decimal("2")

    yes_microprice = None
    if (
        best_yes_bid is not None
        and best_yes_ask is not None
        and best_yes_bid_size is not None
        and best_no_bid_size is not None
    ):
        total_depth = best_yes_bid_size + best_no_bid_size
        if total_depth > Decimal("0"):
            yes_microprice = ((best_yes_ask * best_yes_bid_size) + (best_yes_bid * best_no_bid_size)) / total_depth

    return {
        "best_yes_bid_dollars": _as_float(best_yes_bid, places=6),
        "best_yes_bid_size_contracts": _as_float(best_yes_bid_size, places=6),
        "best_yes_ask_dollars": _as_float(best_yes_ask, places=6),
        "best_no_bid_dollars": _as_float(best_no_bid, places=6),
        "best_no_bid_size_contracts": _as_float(best_no_bid_size, places=6),
        "best_no_ask_dollars": _as_float(best_no_ask, places=6),
        "yes_spread_dollars": _as_float(yes_spread, places=6),
        "yes_midpoint_dollars": _as_float(yes_midpoint, places=6),
        "yes_microprice_dollars": _as_float(yes_microprice, places=6),
    }
