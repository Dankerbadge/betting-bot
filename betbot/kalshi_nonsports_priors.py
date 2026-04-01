from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_fees import estimate_trade_fee, fee_adjusted_edge_per_contract
from betbot.kalshi_nonsports_quality import _parse_float, _parse_timestamp, load_history_rows


PRIOR_FIELDNAMES = [
    "market_ticker",
    "fair_yes_probability",
    "fair_yes_probability_low",
    "fair_yes_probability_high",
    "confidence",
    "thesis",
    "source_note",
    "updated_at",
    "evidence_count",
    "evidence_quality",
    "source_type",
    "last_evidence_at",
]

_WEATHER_HISTORY_PASSTHROUGH_FIELDS = (
    "contract_family",
    "weather_station_history_status",
    "weather_station_history_cache_hit",
    "weather_station_history_cache_fallback_used",
    "weather_station_history_cache_fresh",
    "weather_station_history_cache_age_seconds",
    "weather_station_history_sample_metric",
    "weather_station_history_sample_years",
    "weather_station_history_sample_years_total",
    "weather_station_history_sample_years_precip",
    "weather_station_history_sample_years_tmax",
    "weather_station_history_sample_years_tmin",
    "weather_station_history_sample_years_mean",
    "weather_station_history_min_sample_years_required",
    "weather_station_history_live_ready",
    "weather_station_history_live_ready_reason",
)


def _is_orderable_price(price: float | None) -> bool:
    return price is not None and 0.0 < price < 1.0


def _clamp_probability(value: float | None) -> float | None:
    if value is None:
        return None
    return round(min(1.0, max(0.0, float(value))), 6)


def _normalize_probability_bounds(
    *,
    midpoint: float | None,
    low: float | None,
    high: float | None,
) -> tuple[float | None, float | None, float | None]:
    midpoint_clamped = _clamp_probability(midpoint)
    if midpoint_clamped is None:
        return (None, None, None)
    low_clamped = _clamp_probability(low if low is not None else midpoint_clamped)
    high_clamped = _clamp_probability(high if high is not None else midpoint_clamped)
    if low_clamped is None:
        low_clamped = midpoint_clamped
    if high_clamped is None:
        high_clamped = midpoint_clamped
    low_clamped = min(low_clamped, midpoint_clamped)
    high_clamped = max(high_clamped, midpoint_clamped)
    return (midpoint_clamped, low_clamped, high_clamped)


def load_prior_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _latest_market_rows(history_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)

    latest: dict[str, dict[str, str]] = {}
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest[ticker] = rows_sorted[-1]
    return latest


def build_prior_rows(
    *,
    prior_rows: list[dict[str, str]],
    latest_market_rows: dict[str, dict[str, str]],
    contracts_per_order: int = 1,
    maker_fee_multiplier_override: float | None = None,
    taker_fee_multiplier_override: float | None = None,
    conservative_fee_rounding: bool = True,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in prior_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if not ticker:
            continue
        latest = latest_market_rows.get(ticker)
        fair_yes_probability_raw = _parse_float(str(row.get("fair_yes_probability") or ""))
        fair_yes_probability_low_raw = _parse_float(str(row.get("fair_yes_probability_low") or ""))
        fair_yes_probability_high_raw = _parse_float(str(row.get("fair_yes_probability_high") or ""))
        fair_yes_probability, fair_yes_probability_low, fair_yes_probability_high = _normalize_probability_bounds(
            midpoint=fair_yes_probability_raw,
            low=fair_yes_probability_low_raw,
            high=fair_yes_probability_high_raw,
        )
        fair_yes_probability_conservative = fair_yes_probability_low
        confidence = _parse_float(str(row.get("confidence") or ""))
        evidence_count_raw = _parse_float(str(row.get("evidence_count") or ""))
        evidence_quality = _parse_float(str(row.get("evidence_quality") or ""))
        evidence_count = (
            max(0, int(evidence_count_raw)) if isinstance(evidence_count_raw, float) else ""
        )
        source_type = str(row.get("source_type") or "").strip()
        if not source_type and any(str(row.get(field) or "").strip() for field in ("thesis", "source_note", "updated_at")):
            source_type = "manual"
        last_evidence_at = str(row.get("last_evidence_at") or "").strip()
        latest_yes_bid = _parse_float(str(latest.get("yes_bid_dollars") or "")) if latest else None
        latest_yes_ask = _parse_float(str(latest.get("yes_ask_dollars") or "")) if latest else None
        fair_no_probability = round(1.0 - fair_yes_probability, 6) if fair_yes_probability is not None else None
        fair_no_probability_low = round(1.0 - fair_yes_probability_high, 6) if fair_yes_probability_high is not None else None
        fair_no_probability_high = round(1.0 - fair_yes_probability_low, 6) if fair_yes_probability_low is not None else None
        fair_no_probability_conservative = fair_no_probability_low
        latest_no_bid = round(1.0 - latest_yes_ask, 6) if latest_yes_ask is not None else None
        latest_no_ask = round(1.0 - latest_yes_bid, 6) if latest_yes_bid is not None else None
        market_mid = None
        no_mid = None
        if latest_yes_bid is not None and latest_yes_ask is not None:
            market_mid = round((latest_yes_bid + latest_yes_ask) / 2.0, 6)
            no_mid = round(1.0 - market_mid, 6)
        edge_to_yes_bid = (
            round(fair_yes_probability - latest_yes_bid, 6)
            if fair_yes_probability is not None and latest_yes_bid is not None
            else ""
        )
        edge_to_yes_ask = (
            round(fair_yes_probability - latest_yes_ask, 6)
            if fair_yes_probability is not None and latest_yes_ask is not None
            else ""
        )
        edge_to_yes_ask_net = (
            fee_adjusted_edge_per_contract(
                fair_probability=fair_yes_probability,
                entry_price_dollars=latest_yes_ask,
                contract_count=max(1, contracts_per_order),
                is_maker=False,
                market_ticker=ticker,
                fee_multiplier_override=taker_fee_multiplier_override,
                conservative_rounding=conservative_fee_rounding,
            )
            if fair_yes_probability is not None and latest_yes_ask is not None
            else ""
        )
        edge_to_mid = (
            round(fair_yes_probability - market_mid, 6)
            if fair_yes_probability is not None and market_mid is not None
            else ""
        )
        edge_to_no_bid = (
            round(fair_no_probability - latest_no_bid, 6)
            if fair_no_probability is not None and latest_no_bid is not None
            else ""
        )
        edge_to_no_ask = (
            round(fair_no_probability - latest_no_ask, 6)
            if fair_no_probability is not None and latest_no_ask is not None
            else ""
        )
        edge_to_no_ask_net = (
            fee_adjusted_edge_per_contract(
                fair_probability=fair_no_probability,
                entry_price_dollars=latest_no_ask,
                contract_count=max(1, contracts_per_order),
                is_maker=False,
                market_ticker=ticker,
                fee_multiplier_override=taker_fee_multiplier_override,
                conservative_rounding=conservative_fee_rounding,
            )
            if fair_no_probability is not None and latest_no_ask is not None
            else ""
        )
        edge_to_no_mid = (
            round(fair_no_probability - no_mid, 6)
            if fair_no_probability is not None and no_mid is not None
            else ""
        )
        edge_to_yes_bid_net = (
            fee_adjusted_edge_per_contract(
                fair_probability=fair_yes_probability,
                entry_price_dollars=latest_yes_bid,
                contract_count=max(1, contracts_per_order),
                is_maker=True,
                market_ticker=ticker,
                fee_multiplier_override=maker_fee_multiplier_override,
                conservative_rounding=conservative_fee_rounding,
            )
            if fair_yes_probability is not None and latest_yes_bid is not None
            else ""
        )
        edge_to_no_bid_net = (
            fee_adjusted_edge_per_contract(
                fair_probability=fair_no_probability,
                entry_price_dollars=latest_no_bid,
                contract_count=max(1, contracts_per_order),
                is_maker=True,
                market_ticker=ticker,
                fee_multiplier_override=maker_fee_multiplier_override,
                conservative_rounding=conservative_fee_rounding,
            )
            if fair_no_probability is not None and latest_no_bid is not None
            else ""
        )
        entry_candidates: list[tuple[str, float, float]] = []
        if isinstance(edge_to_yes_ask, float) and _is_orderable_price(latest_yes_ask):
            entry_candidates.append(("yes", edge_to_yes_ask, latest_yes_ask))
        if isinstance(edge_to_no_ask, float) and _is_orderable_price(latest_no_ask):
            entry_candidates.append(("no", edge_to_no_ask, latest_no_ask))
        best_entry_side = ""
        best_entry_edge: float | str = ""
        best_entry_edge_net_fees: float | str = ""
        best_entry_price: float | str = ""
        if entry_candidates:
            best_side, best_edge, best_price = max(entry_candidates, key=lambda item: item[1])
            best_entry_side = best_side
            best_entry_edge = round(best_edge, 6)
            best_entry_price = round(best_price, 6)
            best_entry_edge_net_fees = (
                edge_to_yes_ask_net if best_side == "yes" else edge_to_no_ask_net
            )
        maker_candidates: list[tuple[str, float, float]] = []
        if isinstance(edge_to_yes_bid, float) and _is_orderable_price(latest_yes_bid):
            maker_candidates.append(("yes", edge_to_yes_bid, latest_yes_bid))
        if isinstance(edge_to_no_bid, float) and _is_orderable_price(latest_no_bid):
            maker_candidates.append(("no", edge_to_no_bid, latest_no_bid))
        best_maker_entry_side = ""
        best_maker_entry_edge: float | str = ""
        best_maker_entry_edge_net_fees: float | str = ""
        best_maker_entry_price: float | str = ""
        if maker_candidates:
            best_side, best_edge, best_price = max(maker_candidates, key=lambda item: item[1])
            best_maker_entry_side = best_side
            best_maker_entry_edge = round(best_edge, 6)
            best_maker_entry_price = round(best_price, 6)
            best_maker_entry_edge_net_fees = (
                edge_to_yes_bid_net if best_side == "yes" else edge_to_no_bid_net
            )
        taker_fee_yes = (
            estimate_trade_fee(
                price_dollars=latest_yes_ask,
                contract_count=max(1, contracts_per_order),
                is_maker=False,
                market_ticker=ticker,
                fee_multiplier_override=taker_fee_multiplier_override,
                conservative_rounding=conservative_fee_rounding,
            ).fee_per_contract_dollars
            if latest_yes_ask is not None
            else ""
        )
        taker_fee_no = (
            estimate_trade_fee(
                price_dollars=latest_no_ask,
                contract_count=max(1, contracts_per_order),
                is_maker=False,
                market_ticker=ticker,
                fee_multiplier_override=taker_fee_multiplier_override,
                conservative_rounding=conservative_fee_rounding,
            ).fee_per_contract_dollars
            if latest_no_ask is not None
            else ""
        )
        maker_fee_yes = (
            estimate_trade_fee(
                price_dollars=latest_yes_bid,
                contract_count=max(1, contracts_per_order),
                is_maker=True,
                market_ticker=ticker,
                fee_multiplier_override=maker_fee_multiplier_override,
                conservative_rounding=conservative_fee_rounding,
            ).fee_per_contract_dollars
            if latest_yes_bid is not None
            else ""
        )
        maker_fee_no = (
            estimate_trade_fee(
                price_dollars=latest_no_bid,
                contract_count=max(1, contracts_per_order),
                is_maker=True,
                market_ticker=ticker,
                fee_multiplier_override=maker_fee_multiplier_override,
                conservative_rounding=conservative_fee_rounding,
            ).fee_per_contract_dollars
            if latest_no_bid is not None
            else ""
        )
        enriched_row = {
            "market_ticker": ticker,
            "category": str(latest.get("category") or "") if latest else "",
            "market_title": str(latest.get("market_title") or "") if latest else "",
            "close_time": str(latest.get("close_time") or "") if latest else "",
            "latest_history_captured_at": str(latest.get("captured_at") or "") if latest else "",
            "hours_to_close": _parse_float(str(latest.get("hours_to_close") or "")) if latest else "",
            "fair_yes_probability": fair_yes_probability if fair_yes_probability is not None else "",
            "fair_yes_probability_low": fair_yes_probability_low if fair_yes_probability_low is not None else "",
            "fair_yes_probability_high": fair_yes_probability_high if fair_yes_probability_high is not None else "",
            "fair_yes_probability_conservative": (
                fair_yes_probability_conservative if fair_yes_probability_conservative is not None else ""
            ),
            "fair_no_probability": fair_no_probability if fair_no_probability is not None else "",
            "fair_no_probability_low": fair_no_probability_low if fair_no_probability_low is not None else "",
            "fair_no_probability_high": fair_no_probability_high if fair_no_probability_high is not None else "",
            "fair_no_probability_conservative": (
                fair_no_probability_conservative if fair_no_probability_conservative is not None else ""
            ),
            "confidence": confidence if confidence is not None else "",
            "evidence_count": evidence_count,
            "evidence_quality": evidence_quality if evidence_quality is not None else "",
            "source_type": source_type,
            "last_evidence_at": last_evidence_at,
            "latest_yes_bid_dollars": latest_yes_bid if latest_yes_bid is not None else "",
            "latest_yes_ask_dollars": latest_yes_ask if latest_yes_ask is not None else "",
            "latest_yes_bid_size_contracts": _parse_float(str(latest.get("yes_bid_size_contracts") or "")) if latest else "",
            "latest_yes_ask_size_contracts": _parse_float(str(latest.get("yes_ask_size_contracts") or "")) if latest else "",
            "latest_no_bid_dollars": latest_no_bid if latest_no_bid is not None else "",
            "latest_no_ask_dollars": latest_no_ask if latest_no_ask is not None else "",
            "latest_spread_dollars": _parse_float(str(latest.get("spread_dollars") or "")) if latest else "",
            "latest_two_sided_book": str(latest.get("two_sided_book") or "") if latest else "",
            "latest_ten_dollar_fillable_at_best_ask": (
                str(latest.get("ten_dollar_fillable_at_best_ask") or "") if latest else ""
            ),
            "market_mid_probability": market_mid if market_mid is not None else "",
            "market_no_mid_probability": no_mid if no_mid is not None else "",
            "edge_to_yes_bid": edge_to_yes_bid,
            "edge_to_yes_ask": edge_to_yes_ask,
            "edge_to_yes_ask_net_fees": edge_to_yes_ask_net,
            "edge_to_mid": edge_to_mid,
            "edge_to_no_bid": edge_to_no_bid,
            "edge_to_no_bid_net_fees": edge_to_no_bid_net,
            "edge_to_no_ask": edge_to_no_ask,
            "edge_to_no_ask_net_fees": edge_to_no_ask_net,
            "edge_to_no_mid": edge_to_no_mid,
            "edge_to_yes_bid_net_fees": edge_to_yes_bid_net,
            "estimated_taker_fee_per_contract_yes": taker_fee_yes,
            "estimated_taker_fee_per_contract_no": taker_fee_no,
            "estimated_maker_fee_per_contract_yes": maker_fee_yes,
            "estimated_maker_fee_per_contract_no": maker_fee_no,
            "best_entry_side": best_entry_side,
            "best_entry_edge": best_entry_edge,
            "best_entry_edge_net_fees": best_entry_edge_net_fees,
            "best_entry_price_dollars": best_entry_price,
            "best_maker_entry_side": best_maker_entry_side,
            "best_maker_entry_edge": best_maker_entry_edge,
            "best_maker_entry_edge_net_fees": best_maker_entry_edge_net_fees,
            "best_maker_entry_price_dollars": best_maker_entry_price,
            "matched_live_market": latest is not None,
            "thesis": str(row.get("thesis") or ""),
            "source_note": str(row.get("source_note") or ""),
            "updated_at": str(row.get("updated_at") or ""),
        }
        for passthrough_key in _WEATHER_HISTORY_PASSTHROUGH_FIELDS:
            passthrough_value = row.get(passthrough_key)
            if passthrough_value not in (None, ""):
                enriched_row[passthrough_key] = passthrough_value
        enriched.append(enriched_row)

    enriched.sort(
        key=lambda row: (
            row["matched_live_market"],
            row["best_entry_edge_net_fees"] if isinstance(row.get("best_entry_edge_net_fees"), float) else -999.0,
            row["best_maker_entry_edge_net_fees"] if isinstance(row.get("best_maker_entry_edge_net_fees"), float) else -999.0,
            row["best_entry_edge"] if isinstance(row["best_entry_edge"], float) else -999.0,
            row["edge_to_yes_ask"] if isinstance(row["edge_to_yes_ask"], float) else -999.0,
        ),
        reverse=True,
    )
    return enriched


def _write_prior_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "market_ticker",
        "category",
        "market_title",
        "close_time",
        "hours_to_close",
        "fair_yes_probability",
        "fair_yes_probability_low",
        "fair_yes_probability_high",
        "fair_yes_probability_conservative",
        "fair_no_probability",
        "fair_no_probability_low",
        "fair_no_probability_high",
        "fair_no_probability_conservative",
        "confidence",
        "evidence_count",
        "evidence_quality",
        "source_type",
        "last_evidence_at",
        "latest_yes_bid_dollars",
        "latest_yes_ask_dollars",
        "latest_no_bid_dollars",
        "latest_no_ask_dollars",
        "market_mid_probability",
        "market_no_mid_probability",
        "edge_to_yes_bid",
        "edge_to_yes_ask",
        "edge_to_yes_ask_net_fees",
        "edge_to_mid",
        "edge_to_no_bid",
        "edge_to_no_bid_net_fees",
        "edge_to_no_ask",
        "edge_to_no_ask_net_fees",
        "edge_to_no_mid",
        "edge_to_yes_bid_net_fees",
        "estimated_taker_fee_per_contract_yes",
        "estimated_taker_fee_per_contract_no",
        "estimated_maker_fee_per_contract_yes",
        "estimated_maker_fee_per_contract_no",
        "best_entry_side",
        "best_entry_edge",
        "best_entry_edge_net_fees",
        "best_entry_price_dollars",
        "best_maker_entry_side",
        "best_maker_entry_edge",
        "best_maker_entry_edge_net_fees",
        "best_maker_entry_price_dollars",
        "matched_live_market",
        "thesis",
        "source_note",
        "updated_at",
    ]
    fieldnames.extend(_WEATHER_HISTORY_PASSTHROUGH_FIELDS)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_priors(
    *,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    top_n: int = 10,
    contracts_per_order: int = 1,
    maker_fee_multiplier_override: float | None = None,
    taker_fee_multiplier_override: float | None = None,
    conservative_fee_rounding: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    priors_path = Path(priors_csv)
    history_path = Path(history_csv)
    prior_rows = load_prior_rows(priors_path)
    history_rows = load_history_rows(history_path)
    enriched_rows = build_prior_rows(
        prior_rows=prior_rows,
        latest_market_rows=_latest_market_rows(history_rows),
        contracts_per_order=contracts_per_order,
        maker_fee_multiplier_override=maker_fee_multiplier_override,
        taker_fee_multiplier_override=taker_fee_multiplier_override,
        conservative_fee_rounding=conservative_fee_rounding,
    )

    matched_markets = sum(1 for row in enriched_rows if row["matched_live_market"])
    positive_edge_yes_bid = sum(
        1 for row in enriched_rows if isinstance(row["edge_to_yes_bid"], float) and row["edge_to_yes_bid"] > 0
    )
    positive_edge_yes_ask = sum(
        1 for row in enriched_rows if isinstance(row["edge_to_yes_ask"], float) and row["edge_to_yes_ask"] > 0
    )
    positive_edge_no_bid = sum(
        1 for row in enriched_rows if isinstance(row["edge_to_no_bid"], float) and row["edge_to_no_bid"] > 0
    )
    positive_edge_no_ask = sum(
        1 for row in enriched_rows if isinstance(row["edge_to_no_ask"], float) and row["edge_to_no_ask"] > 0
    )
    positive_best_entry = sum(
        1 for row in enriched_rows if isinstance(row["best_entry_edge"], float) and row["best_entry_edge"] > 0
    )
    positive_best_maker_entry = sum(
        1 for row in enriched_rows if isinstance(row["best_maker_entry_edge"], float) and row["best_maker_entry_edge"] > 0
    )
    positive_best_entry_net = sum(
        1
        for row in enriched_rows
        if isinstance(row.get("best_entry_edge_net_fees"), float) and float(row["best_entry_edge_net_fees"]) > 0
    )
    positive_best_maker_entry_net = sum(
        1
        for row in enriched_rows
        if isinstance(row.get("best_maker_entry_edge_net_fees"), float)
        and float(row["best_maker_entry_edge_net_fees"]) > 0
    )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_priors_{stamp}.csv"
    _write_prior_csv(csv_path, enriched_rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "priors_csv": str(priors_path),
        "prior_file_exists": priors_path.exists(),
        "history_csv": str(history_path),
        "prior_rows": len(prior_rows),
        "matched_live_markets": matched_markets,
        "positive_edge_yes_bid_markets": positive_edge_yes_bid,
        "positive_edge_yes_ask_markets": positive_edge_yes_ask,
        "positive_edge_no_bid_markets": positive_edge_no_bid,
        "positive_edge_no_ask_markets": positive_edge_no_ask,
        "positive_best_entry_markets": positive_best_entry,
        "positive_best_maker_entry_markets": positive_best_maker_entry,
        "positive_best_entry_markets_net_fees": positive_best_entry_net,
        "positive_best_maker_entry_markets_net_fees": positive_best_maker_entry_net,
        "top_market_ticker": enriched_rows[0]["market_ticker"] if enriched_rows else None,
        "top_market_hours_to_close": enriched_rows[0]["hours_to_close"] if enriched_rows else None,
        "top_market_edge_to_yes_ask": enriched_rows[0]["edge_to_yes_ask"] if enriched_rows else None,
        "top_market_best_entry_side": enriched_rows[0]["best_entry_side"] if enriched_rows else None,
        "top_market_best_entry_edge": enriched_rows[0]["best_entry_edge"] if enriched_rows else None,
        "top_market_best_entry_edge_net_fees": (
            enriched_rows[0]["best_entry_edge_net_fees"] if enriched_rows else None
        ),
        "top_market_best_maker_entry_side": (
            enriched_rows[0]["best_maker_entry_side"] if enriched_rows else None
        ),
        "top_market_best_maker_entry_edge": (
            enriched_rows[0]["best_maker_entry_edge"] if enriched_rows else None
        ),
        "top_market_best_maker_entry_edge_net_fees": (
            enriched_rows[0]["best_maker_entry_edge_net_fees"] if enriched_rows else None
        ),
        "top_markets": enriched_rows[:top_n],
        "status": "ready",
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_nonsports_priors_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
