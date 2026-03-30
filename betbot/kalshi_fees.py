from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP


DEFAULT_TAKER_FEE_MULTIPLIER = 0.07
DEFAULT_INDEX_TAKER_FEE_MULTIPLIER = 0.035
DEFAULT_MAKER_FEE_MULTIPLIER = 0.0175
INDEX_FEE_SCHEDULE_PREFIXES = ("INX", "NASDAQ100", "KXINX", "KXNASDAQ100")
_DECIMAL_ZERO = Decimal("0")
_DECIMAL_ONE = Decimal("1")
_DECIMAL_CENT = Decimal("0.01")


@dataclass(frozen=True)
class FeeEstimate:
    raw_fee_dollars: float
    rounded_fee_dollars: float
    fee_per_contract_dollars: float


def _clamp_probability_price(price_dollars: float) -> float:
    return min(1.0, max(0.0, price_dollars))


def _as_decimal(value: float | int) -> Decimal:
    return Decimal(str(value))


def _uses_index_fee_schedule(market_ticker: str | None) -> bool:
    if not market_ticker:
        return False
    ticker = str(market_ticker).strip().upper()
    return any(ticker.startswith(prefix) for prefix in INDEX_FEE_SCHEDULE_PREFIXES)


def raw_trade_fee_dollars(
    *,
    price_dollars: float,
    contract_count: int,
    fee_multiplier: float,
) -> float:
    if contract_count <= 0:
        return 0.0
    price = _as_decimal(_clamp_probability_price(price_dollars))
    multiplier = _as_decimal(float(fee_multiplier))
    contracts = _as_decimal(int(contract_count))
    # Kalshi's fee curve scales with p * (1 - p), contracts, and fee multiplier.
    raw = multiplier * price * (_DECIMAL_ONE - price) * contracts
    if raw <= _DECIMAL_ZERO:
        return 0.0
    return float(raw)


def rounded_fee_dollars(raw_fee_dollars: float, *, conservative: bool = True) -> float:
    if raw_fee_dollars <= 0:
        return 0.0
    fee = _as_decimal(raw_fee_dollars)
    if conservative:
        rounded = fee.quantize(_DECIMAL_CENT, rounding=ROUND_CEILING)
    else:
        rounded = fee.quantize(_DECIMAL_CENT, rounding=ROUND_HALF_UP)
    if rounded < _DECIMAL_ZERO:
        return 0.0
    return round(float(rounded), 6)


def estimate_trade_fee(
    *,
    price_dollars: float,
    contract_count: int,
    is_maker: bool,
    market_ticker: str | None = None,
    fee_multiplier_override: float | None = None,
    conservative_rounding: bool = True,
) -> FeeEstimate:
    if fee_multiplier_override is not None:
        multiplier = fee_multiplier_override
    elif is_maker:
        multiplier = DEFAULT_MAKER_FEE_MULTIPLIER
    elif _uses_index_fee_schedule(market_ticker):
        multiplier = DEFAULT_INDEX_TAKER_FEE_MULTIPLIER
    else:
        multiplier = DEFAULT_TAKER_FEE_MULTIPLIER
    raw_fee = raw_trade_fee_dollars(
        price_dollars=price_dollars,
        contract_count=contract_count,
        fee_multiplier=multiplier,
    )
    rounded_fee = rounded_fee_dollars(raw_fee, conservative=conservative_rounding)
    per_contract = rounded_fee / float(contract_count) if contract_count > 0 else 0.0
    return FeeEstimate(
        raw_fee_dollars=round(raw_fee, 8),
        rounded_fee_dollars=round(rounded_fee, 6),
        fee_per_contract_dollars=round(per_contract, 8),
    )


def fee_adjusted_edge_per_contract(
    *,
    fair_probability: float,
    entry_price_dollars: float,
    contract_count: int,
    is_maker: bool,
    market_ticker: str | None = None,
    fee_multiplier_override: float | None = None,
    conservative_rounding: bool = True,
) -> float:
    gross_edge = float(fair_probability) - float(entry_price_dollars)
    fee = estimate_trade_fee(
        price_dollars=entry_price_dollars,
        contract_count=contract_count,
        is_maker=is_maker,
        market_ticker=market_ticker,
        fee_multiplier_override=fee_multiplier_override,
        conservative_rounding=conservative_rounding,
    )
    return round(gross_edge - fee.fee_per_contract_dollars, 8)
