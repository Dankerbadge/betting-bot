from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any

from betbot.kalshi_book import default_book_db_path, record_decisions
from betbot.kalshi_fees import estimate_trade_fee
from betbot.kalshi_incentives import fetch_incentive_map
from betbot.kalshi_micro_execute import _http_request_json
from betbot.kalshi_micro_plan import (
    default_balance_cache_path,
    _load_balance_cache,
    _write_balance_cache,
)
from betbot.kalshi_nonsports_priors import build_prior_rows, load_prior_rows
from betbot.kalshi_nonsports_quality import _parse_timestamp, load_history_rows
from betbot.live_smoke import HttpGetter, KalshiSigner, _http_get_json, _kalshi_sign_request
from betbot.live_snapshot import _kalshi_balance_snapshot
from betbot.onboarding import _is_placeholder, _parse_env_file


LIVE_ALLOWED_CANONICAL_NICHES = ("macro_release", "weather_energy_transmission")


def _latest_market_rows(history_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    latest_rows: dict[str, dict[str, str]] = {}
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest_rows[ticker] = rows_sorted[-1]
    return latest_rows


def _build_order_payload_preview(
    *,
    ticker: str,
    count: int,
    side: str,
    price_dollars: float,
) -> dict[str, Any]:
    payload = {
        "ticker": ticker,
        "side": side,
        "action": "buy",
        "count": count,
        "time_in_force": "good_till_canceled",
        "post_only": True,
        "cancel_order_on_pause": True,
        "self_trade_prevention_type": "maker",
    }
    price_key = "yes_price_dollars" if side == "yes" else "no_price_dollars"
    payload[price_key] = f"{price_dollars:.4f}"
    return payload


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _as_int(value: Any) -> int | None:
    numeric = _as_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _normalized_allowed_canonical_niches(
    allowed_canonical_niches: tuple[str, ...] | set[str] | None,
) -> set[str] | None:
    if not allowed_canonical_niches:
        return None
    normalized = {
        str(value).strip().lower()
        for value in allowed_canonical_niches
        if str(value).strip()
    }
    return normalized or None


def _canonical_policy_index(
    *,
    canonical_mapping_csv: str | None,
    canonical_threshold_csv: str | None,
    allowed_canonical_niches: set[str] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, Any]]:
    if not canonical_mapping_csv or not canonical_threshold_csv:
        return (
            {},
            {},
            {
                "canonical_policy_enabled": False,
                "canonical_policy_reason": "canonical_paths_not_provided",
            },
        )

    mapping_path = Path(canonical_mapping_csv)
    threshold_path = Path(canonical_threshold_csv)
    if not mapping_path.exists() or not threshold_path.exists():
        missing = []
        if not mapping_path.exists():
            missing.append(str(mapping_path))
        if not threshold_path.exists():
            missing.append(str(threshold_path))
        return (
            {},
            {},
            {
                "canonical_policy_enabled": False,
                "canonical_policy_reason": "canonical_files_missing",
                "canonical_policy_missing_files": missing,
            },
        )

    threshold_rows = _load_csv_rows(threshold_path)
    threshold_by_canonical: dict[str, dict[str, Any]] = {}
    threshold_rows_allowed_niche_count = 0
    threshold_rows_disallowed_niche_count = 0
    for row in threshold_rows:
        canonical_ticker = str(row.get("canonical_ticker") or "").strip()
        if not canonical_ticker:
            continue
        niche_value = str(row.get("niche") or "").strip()
        normalized_niche = niche_value.lower()
        if allowed_canonical_niches is not None:
            if normalized_niche in allowed_canonical_niches:
                threshold_rows_allowed_niche_count += 1
            else:
                threshold_rows_disallowed_niche_count += 1
        else:
            threshold_rows_allowed_niche_count += 1
        threshold_by_canonical[canonical_ticker] = {
            "canonical_ticker": canonical_ticker,
            "niche": niche_value,
            "execution_phase": str(row.get("execution_phase") or "").strip(),
            "release_cluster": str(row.get("release_cluster") or "").strip(),
            "entry_min_edge_net": _as_float(row.get("entry_min_edge_net")),
            "entry_min_confidence": _as_float(row.get("entry_min_confidence")),
            "entry_min_evidence_count": _as_int(row.get("entry_min_evidence_count")),
            "entry_max_price_dollars": _as_float(row.get("entry_max_price_dollars")),
            "entry_max_spread_dollars": _as_float(row.get("entry_max_spread_dollars")),
            "per_market_risk_cap_fraction_nav": _as_float(row.get("per_market_risk_cap_fraction_nav")),
            "release_cluster_risk_cap_fraction_nav": _as_float(row.get("release_cluster_risk_cap_fraction_nav")),
            "same_day_correlated_risk_cap_fraction_nav": _as_float(row.get("same_day_correlated_risk_cap_fraction_nav")),
            "notes": str(row.get("notes") or "").strip(),
        }

    mapping_rows = _load_csv_rows(mapping_path)
    index: dict[str, dict[str, Any]] = {}
    alias_candidates: dict[str, list[dict[str, Any]]] = {}
    mapped_rows_count = 0
    for row in mapping_rows:
        mapping_status = str(row.get("mapping_status") or "").strip().lower()
        canonical_ticker = str(row.get("canonical_ticker") or "").strip()
        live_market_ticker = str(row.get("live_market_ticker") or "").strip()
        if mapping_status != "mapped" or not canonical_ticker or not live_market_ticker:
            continue
        policy = threshold_by_canonical.get(canonical_ticker)
        if policy is None:
            continue
        merged = dict(policy)
        normalized_live_market_ticker = _normalize_market_ticker(live_market_ticker)
        merged.update(
            {
                "mapping_status": mapping_status,
                "live_market_ticker": live_market_ticker,
                "live_market_ticker_normalized": normalized_live_market_ticker,
                "live_event_ticker": str(row.get("live_event_ticker") or "").strip(),
                "mapping_confidence": _as_float(row.get("mapping_confidence")),
                "mapping_notes": str(row.get("mapping_notes") or "").strip(),
                "last_mapped_at": str(row.get("last_mapped_at") or "").strip(),
            }
        )
        index[normalized_live_market_ticker] = merged
        mapped_rows_count += 1
        for raw_lookup_value in (live_market_ticker, row.get("live_event_ticker")):
            for lookup_key in _canonical_lookup_keys(raw_lookup_value):
                alias_candidates.setdefault(lookup_key, []).append(merged)

    alias_index: dict[str, dict[str, Any]] = {}
    alias_collision_keys: list[str] = []
    for lookup_key, candidates in alias_candidates.items():
        canonical_choices = {
            str(candidate.get("canonical_ticker") or "").strip()
            for candidate in candidates
            if str(candidate.get("canonical_ticker") or "").strip()
        }
        if len(canonical_choices) != 1:
            alias_collision_keys.append(lookup_key)
            continue
        selected = max(candidates, key=lambda row: float(row.get("mapping_confidence") or 0.0))
        alias_index[lookup_key] = selected

    diagnostics = {
        "canonical_policy_enabled": True,
        "canonical_policy_reason": "loaded",
        "canonical_mapping_csv": str(mapping_path),
        "canonical_threshold_csv": str(threshold_path),
        "canonical_mapping_rows": len(mapping_rows),
        "canonical_threshold_rows": len(threshold_rows),
        "canonical_threshold_rows_allowed_niche": threshold_rows_allowed_niche_count,
        "canonical_threshold_rows_disallowed_niche": threshold_rows_disallowed_niche_count,
        "canonical_mapped_rows": mapped_rows_count,
        "canonical_policy_live_tickers": len(index),
        "canonical_policy_alias_lookup_keys": len(alias_index),
        "canonical_policy_alias_collision_keys": len(alias_collision_keys),
    }
    return index, alias_index, diagnostics


def _is_orderable_price(price: float | None) -> bool:
    return price is not None and 0.0 < price < 1.0


def _normalize_market_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def _canonical_lookup_keys(value: Any) -> list[str]:
    ticker = _normalize_market_ticker(value)
    if not ticker:
        return []
    keys: list[str] = []

    def _add_key(raw: str) -> None:
        candidate = _normalize_market_ticker(raw)
        if candidate and candidate not in keys:
            keys.append(candidate)

    _add_key(ticker)
    for marker in ("-T", "-B", "-P"):
        if marker in ticker:
            _add_key(ticker.split(marker, 1)[0])
    if ticker.endswith("-PART"):
        _add_key(ticker[: -len("-PART")])
    if "-" in ticker:
        prefix, suffix = ticker.rsplit("-", 1)
        if suffix in {"PART", "YES", "NO"}:
            _add_key(prefix)
        if suffix and suffix[0] in {"T", "B", "P"} and any(ch.isdigit() for ch in suffix):
            _add_key(prefix)
    if "-" in ticker:
        _add_key(ticker.split("-", 1)[0])
    return keys


def _resolve_canonical_policy_for_ticker(
    *,
    market_ticker: Any,
    canonical_policy_by_live_ticker: dict[str, dict[str, Any]] | None,
    canonical_policy_alias_by_lookup_key: dict[str, dict[str, Any]] | None,
) -> tuple[dict[str, Any], str | None, str | None]:
    ticker = _normalize_market_ticker(market_ticker)
    if not ticker:
        return {}, None, None
    if isinstance(canonical_policy_by_live_ticker, dict):
        exact = canonical_policy_by_live_ticker.get(ticker)
        if isinstance(exact, dict) and exact:
            return dict(exact), "exact_live_market_ticker", ticker
    if isinstance(canonical_policy_alias_by_lookup_key, dict):
        for lookup_key in _canonical_lookup_keys(ticker):
            alias = canonical_policy_alias_by_lookup_key.get(lookup_key)
            if isinstance(alias, dict) and alias:
                return dict(alias), "alias_lookup_key", lookup_key
    return {}, None, None


def _infer_canonical_niche_guess(row: dict[str, Any]) -> str:
    ticker = str(row.get("market_ticker") or "").strip().upper()
    category = str(row.get("category") or "").strip().lower()
    title = " ".join(
        (
            str(row.get("market_title") or ""),
            str(row.get("event_title") or ""),
            str(row.get("thesis") or ""),
        )
    ).lower()
    merged = f"{ticker} {title}"

    if ticker.startswith("KXIPO") or re.search(r"\bipo\b", merged):
        return "companies_ipo"
    if category in {"politics", "elections"} or any(
        token in merged
        for token in (
            "president",
            "senate",
            "house",
            "minister",
            "pardon",
            "trump",
            "cabinet",
            "gabbard",
            "bondi",
            "netanyahu",
        )
    ):
        return "politics"
    if category in {"companies"}:
        return "companies"
    if category in {"entertainment"}:
        return "entertainment"

    macro_tokens = (
        "cpi",
        "core pce",
        "payroll",
        "unemployment rate",
        "federal funds rate",
        "gdp",
        "ppi",
        "jolts",
        "retail sales",
        "durable goods",
        "housing starts",
        "building permits",
        "new home sales",
        "personal income",
        "fomc",
        "kxeconstat",
        "kxpcecore",
        "kxpayrolls",
        "kxgdp-",
        "kxfed-",
    )
    if any(
        token in merged
        for token in macro_tokens
    ):
        return "macro_release"

    energy_tokens = (
        "crude stocks",
        "cushing",
        "refinery utilization",
        "natural gas storage",
        "wngsr",
        "wpsr",
        "eia",
        "gasoline stocks",
        "distillate stocks",
        "heating oil",
        "propane",
        "diesel price",
        "kxng",
        "kxoil",
    )
    if any(
        token in merged
        for token in energy_tokens
    ):
        return "weather_energy_transmission"
    if any(
        token in merged
        for token in (
            "rain",
            "snow",
            "temperature",
            "weather",
            "climate",
            "kxrain",
            "kxsnow",
            "kxtemp",
            "kxhmonth",
        )
    ):
        return "weather_climate"
    if category in {"economics"} and any(token in merged for token in macro_tokens):
        return "macro_release"
    if category in {"climate and weather"}:
        return "weather_climate"
    return "unknown"


def _conservative_maker_candidate_for_side(
    *,
    row: dict[str, Any],
    side: str,
    contracts_per_order: int,
    maker_fee_multiplier_override: float | None,
    conservative_fee_rounding: bool,
) -> dict[str, float | str] | None:
    ticker = str(row.get("market_ticker") or "").strip()
    if side == "yes":
        price = _as_float(row.get("latest_yes_bid_dollars"))
        fair_probability_midpoint = _as_float(row.get("fair_yes_probability"))
        fair_probability_conservative = _as_float(row.get("fair_yes_probability_conservative"))
        if fair_probability_conservative is None:
            fair_probability_conservative = _as_float(row.get("fair_yes_probability_low"))
    else:
        price = _as_float(row.get("latest_no_bid_dollars"))
        fair_probability_midpoint = _as_float(row.get("fair_no_probability"))
        fair_probability_conservative = _as_float(row.get("fair_no_probability_conservative"))
        if fair_probability_conservative is None:
            fair_probability_conservative = _as_float(row.get("fair_no_probability_low"))

    if fair_probability_conservative is None:
        fair_probability_conservative = fair_probability_midpoint
    if (
        not _is_orderable_price(price)
        or fair_probability_midpoint is None
        or fair_probability_conservative is None
    ):
        return None

    fee_estimate = estimate_trade_fee(
        price_dollars=float(price),
        contract_count=max(1, contracts_per_order),
        is_maker=True,
        market_ticker=ticker,
        fee_multiplier_override=maker_fee_multiplier_override,
        conservative_rounding=conservative_fee_rounding,
    )
    maker_entry_edge_midpoint = round(float(fair_probability_midpoint) - float(price), 6)
    maker_entry_edge_conservative = round(float(fair_probability_conservative) - float(price), 6)
    maker_entry_edge_midpoint_net_fees = round(
        maker_entry_edge_midpoint - float(fee_estimate.fee_per_contract_dollars),
        6,
    )
    maker_entry_edge_conservative_net_fees = round(
        maker_entry_edge_conservative - float(fee_estimate.fee_per_contract_dollars),
        6,
    )
    return {
        "side": side,
        "maker_entry_price_dollars": round(float(price), 6),
        "fair_probability_midpoint": round(float(fair_probability_midpoint), 6),
        "fair_probability_conservative": round(float(fair_probability_conservative), 6),
        "maker_entry_edge_midpoint": maker_entry_edge_midpoint,
        "maker_entry_edge_midpoint_net_fees": maker_entry_edge_midpoint_net_fees,
        "maker_entry_edge_conservative": maker_entry_edge_conservative,
        "maker_entry_edge_conservative_net_fees": maker_entry_edge_conservative_net_fees,
    }


def _select_conservative_maker_candidate(
    *,
    row: dict[str, Any],
    contracts_per_order: int,
    maker_fee_multiplier_override: float | None,
    conservative_fee_rounding: bool,
) -> dict[str, float | str] | None:
    candidates: list[dict[str, float | str]] = []
    yes_candidate = _conservative_maker_candidate_for_side(
        row=row,
        side="yes",
        contracts_per_order=contracts_per_order,
        maker_fee_multiplier_override=maker_fee_multiplier_override,
        conservative_fee_rounding=conservative_fee_rounding,
    )
    if yes_candidate is not None:
        candidates.append(yes_candidate)
    no_candidate = _conservative_maker_candidate_for_side(
        row=row,
        side="no",
        contracts_per_order=contracts_per_order,
        maker_fee_multiplier_override=maker_fee_multiplier_override,
        conservative_fee_rounding=conservative_fee_rounding,
    )
    if no_candidate is not None:
        candidates.append(no_candidate)
    if not candidates:
        return None

    def _metric(item: dict[str, float | str], key: str) -> float:
        value = item.get(key)
        return float(value) if isinstance(value, (int, float)) else -999.0

    return max(
        candidates,
        key=lambda item: (
            _metric(item, "maker_entry_edge_conservative_net_fees"),
            _metric(item, "maker_entry_edge_conservative"),
            _metric(item, "maker_entry_edge_midpoint"),
        ),
    )


def _summarize_unmapped_canonical_markets(
    *,
    enriched_rows: list[dict[str, Any]],
    canonical_policy_by_live_ticker: dict[str, dict[str, Any]] | None,
    canonical_policy_alias_by_lookup_key: dict[str, dict[str, Any]] | None,
    allowed_canonical_niches: set[str] | None,
    max_rows: int = 25,
) -> dict[str, Any]:
    counts_by_guess: dict[str, int] = {}
    details: list[dict[str, Any]] = []
    total = 0
    in_allowed_guess = 0
    outside_allowed_guess = 0
    for row in enriched_rows:
        if not row.get("matched_live_market"):
            continue
        market_ticker = str(row.get("market_ticker") or "").strip()
        if not market_ticker:
            continue
        canonical_policy, _, _ = _resolve_canonical_policy_for_ticker(
            market_ticker=market_ticker,
            canonical_policy_by_live_ticker=canonical_policy_by_live_ticker,
            canonical_policy_alias_by_lookup_key=canonical_policy_alias_by_lookup_key,
        )
        if canonical_policy:
            continue
        total += 1
        niche_guess = _infer_canonical_niche_guess(row)
        counts_by_guess[niche_guess] = counts_by_guess.get(niche_guess, 0) + 1
        in_allowed = allowed_canonical_niches is None or niche_guess in allowed_canonical_niches
        if in_allowed:
            in_allowed_guess += 1
        else:
            outside_allowed_guess += 1
        details.append(
            {
                "market_ticker": market_ticker,
                "market_title": str(row.get("market_title") or ""),
                "category": str(row.get("category") or ""),
                "hours_to_close": _as_float(row.get("hours_to_close")),
                "best_maker_entry_edge_net_fees": _as_float(row.get("best_maker_entry_edge_net_fees")),
                "confidence": _as_float(row.get("confidence")),
                "niche_guess": niche_guess,
                "in_allowed_niche_guess": in_allowed,
                "canonical_lookup_keys": _canonical_lookup_keys(market_ticker)[:4],
                "why_unmapped": (
                    "no_canonical_policy_for_ticker_or_alias_key"
                    if in_allowed
                    else "outside_allowed_live_niche_guess"
                ),
            }
        )

    details.sort(
        key=lambda row: (
            1 if row.get("in_allowed_niche_guess") else 0,
            float(row.get("best_maker_entry_edge_net_fees") or -999.0),
            -float(row.get("hours_to_close") or 0.0),
            str(row.get("market_ticker") or ""),
        ),
        reverse=True,
    )
    return {
        "total": total,
        "in_allowed_niche_guess": in_allowed_guess,
        "outside_allowed_niche_guess": outside_allowed_guess,
        "counts_by_niche_guess": dict(sorted(counts_by_guess.items(), key=lambda item: (-item[1], item[0]))),
        "top_markets": details[: max(0, int(max_rows))],
    }


def build_micro_prior_plans(
    *,
    enriched_rows: list[dict[str, Any]],
    planning_bankroll_dollars: float,
    daily_risk_cap_dollars: float,
    contracts_per_order: int,
    max_orders: int,
    min_maker_edge: float,
    min_maker_edge_net_fees: float = 0.0,
    min_entry_price_dollars: float = 0.0,
    max_entry_price_dollars: float,
    routine_max_hours_to_close: float | None = None,
    max_hours_to_close_by_canonical_niche: dict[str, float] | None = None,
    routine_longdated_allowed_niches: set[str] | None = None,
    maker_fee_multiplier_override: float | None = None,
    conservative_fee_rounding: bool = True,
    incentive_bonus_per_contract_by_ticker: dict[str, float] | None = None,
    canonical_policy_by_live_ticker: dict[str, dict[str, Any]] | None = None,
    canonical_policy_alias_by_lookup_key: dict[str, dict[str, Any]] | None = None,
    require_canonical_mapping: bool = False,
    allowed_canonical_niches: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if planning_bankroll_dollars <= 0:
        raise ValueError("planning_bankroll_dollars must be positive")
    if daily_risk_cap_dollars <= 0:
        raise ValueError("daily_risk_cap_dollars must be positive")
    if contracts_per_order <= 0:
        raise ValueError("contracts_per_order must be positive")
    if max_orders <= 0:
        raise ValueError("max_orders must be positive")

    remaining_risk = round(daily_risk_cap_dollars, 4)
    plans: list[dict[str, Any]] = []
    skip_counts = {
        "not_live_matched": 0,
        "missing_maker_side": 0,
        "maker_edge_below_min": 0,
        "maker_edge_net_fees_below_min": 0,
        "entry_price_below_min": 0,
        "entry_price_above_max": 0,
        "routine_hours_to_close_above_max": 0,
        "budget_too_small": 0,
        "canonical_unmapped": 0,
        "canonical_unmapped_in_allowed_niche_guess": 0,
        "canonical_unmapped_outside_allowed_niche_guess": 0,
        "canonical_niche_disallowed": 0,
        "canonical_confidence_below_min": 0,
        "canonical_evidence_below_min": 0,
        "canonical_spread_above_max": 0,
        "canonical_per_market_risk_cap": 0,
        "canonical_release_cluster_risk_cap": 0,
        "canonical_correlated_risk_cap": 0,
    }
    release_cluster_spent_dollars: dict[str, float] = {}
    correlated_group_spent_dollars: dict[str, float] = {}
    longdated_allowed_niches = (
        {value.strip().lower() for value in routine_longdated_allowed_niches if value.strip()}
        if routine_longdated_allowed_niches
        else set()
    )
    max_hours_by_niche = {
        str(key).strip().lower(): float(value)
        for key, value in (max_hours_to_close_by_canonical_niche or {}).items()
        if str(key).strip() and isinstance(value, (int, float))
    }

    ranked_rows_with_candidates: list[tuple[dict[str, Any], dict[str, float | str] | None]] = []
    for row in enriched_rows:
        conservative_candidate = _select_conservative_maker_candidate(
            row=row,
            contracts_per_order=contracts_per_order,
            maker_fee_multiplier_override=maker_fee_multiplier_override,
            conservative_fee_rounding=conservative_fee_rounding,
        )
        ranked_rows_with_candidates.append((row, conservative_candidate))

    ranked_rows_with_candidates.sort(
        key=lambda item: (
            bool(item[0].get("matched_live_market")),
            (
                float(item[1]["maker_entry_edge_conservative_net_fees"])
                + (
                    float(incentive_bonus_per_contract_by_ticker.get(str(item[0].get("market_ticker") or ""), 0.0))
                    if isinstance(incentive_bonus_per_contract_by_ticker, dict)
                    else 0.0
                )
            )
            if isinstance(item[1], dict)
            else -999.0,
            float(item[1]["maker_entry_edge_conservative"]) if isinstance(item[1], dict) else -999.0,
            (
                float(item[1]["maker_entry_edge_conservative"]) / float(item[1]["maker_entry_price_dollars"])
                if isinstance(item[1], dict)
                and float(item[1]["maker_entry_price_dollars"]) > 0
                else -999.0
            ),
            (
                (
                    (float(item[1]["maker_entry_edge_conservative"]) / float(item[1]["maker_entry_price_dollars"]))
                    / (float(item[0]["hours_to_close"]) / 24.0)
                )
                if isinstance(item[1], dict)
                and float(item[1]["maker_entry_price_dollars"]) > 0
                and isinstance(item[0].get("hours_to_close"), (int, float))
                and float(item[0]["hours_to_close"]) > 0
                else -999.0
            ),
        ),
        reverse=True,
    )

    for row, conservative_candidate in ranked_rows_with_candidates:
        if len(plans) >= max_orders:
            break
        if not row.get("matched_live_market"):
            skip_counts["not_live_matched"] += 1
            continue
        if not isinstance(conservative_candidate, dict):
            skip_counts["missing_maker_side"] += 1
            continue
        side = str(conservative_candidate.get("side") or "").strip()
        price = conservative_candidate.get("maker_entry_price_dollars")
        maker_edge = conservative_candidate.get("maker_entry_edge_midpoint")
        maker_edge_net = conservative_candidate.get("maker_entry_edge_midpoint_net_fees")
        maker_edge_conservative = conservative_candidate.get("maker_entry_edge_conservative")
        maker_edge_conservative_net = conservative_candidate.get("maker_entry_edge_conservative_net_fees")
        if (
            side not in {"yes", "no"}
            or not isinstance(price, (int, float))
            or not isinstance(maker_edge, (int, float))
            or not isinstance(maker_edge_net, (int, float))
            or not isinstance(maker_edge_conservative, (int, float))
            or not isinstance(maker_edge_conservative_net, (int, float))
        ):
            skip_counts["missing_maker_side"] += 1
            continue
        ticker = str(row.get("market_ticker") or "")
        canonical_policy, canonical_mapping_match_type, canonical_mapping_match_key = _resolve_canonical_policy_for_ticker(
            market_ticker=ticker,
            canonical_policy_by_live_ticker=canonical_policy_by_live_ticker,
            canonical_policy_alias_by_lookup_key=canonical_policy_alias_by_lookup_key,
        )
        canonical_policy_applied = bool(canonical_policy)
        if require_canonical_mapping and not canonical_policy_applied:
            skip_counts["canonical_unmapped"] += 1
            niche_guess = _infer_canonical_niche_guess(row)
            if allowed_canonical_niches is None or niche_guess in allowed_canonical_niches:
                skip_counts["canonical_unmapped_in_allowed_niche_guess"] += 1
            else:
                skip_counts["canonical_unmapped_outside_allowed_niche_guess"] += 1
            continue

        effective_min_maker_edge = float(min_maker_edge)
        effective_min_maker_edge_net_fees = float(min_maker_edge_net_fees)
        effective_min_entry_price_dollars = max(0.0, float(min_entry_price_dollars))
        effective_max_entry_price_dollars = float(max_entry_price_dollars)
        effective_max_spread_dollars: float | None = None
        effective_min_confidence: float | None = None
        effective_min_evidence_count: int | None = None
        per_market_risk_cap_dollars: float | None = None
        release_cluster_risk_cap_dollars: float | None = None
        same_day_correlated_risk_cap_dollars: float | None = None
        canonical_ticker = str(canonical_policy.get("canonical_ticker") or "")
        canonical_niche = str(canonical_policy.get("niche") or "")
        canonical_niche_normalized = canonical_niche.strip().lower()
        if canonical_policy_applied and allowed_canonical_niches is not None:
            if canonical_niche_normalized not in allowed_canonical_niches:
                skip_counts["canonical_niche_disallowed"] += 1
                continue
        canonical_release_cluster = str(canonical_policy.get("release_cluster") or "")
        if canonical_policy_applied:
            entry_min_edge_net = _as_float(canonical_policy.get("entry_min_edge_net"))
            if entry_min_edge_net is not None:
                effective_min_maker_edge = max(effective_min_maker_edge, entry_min_edge_net)
                effective_min_maker_edge_net_fees = max(effective_min_maker_edge_net_fees, entry_min_edge_net)
            entry_max_price_dollars = _as_float(canonical_policy.get("entry_max_price_dollars"))
            if entry_max_price_dollars is not None:
                effective_max_entry_price_dollars = min(
                    effective_max_entry_price_dollars,
                    entry_max_price_dollars,
                )
            effective_max_spread_dollars = _as_float(canonical_policy.get("entry_max_spread_dollars"))
            effective_min_confidence = _as_float(canonical_policy.get("entry_min_confidence"))
            effective_min_evidence_count = _as_int(canonical_policy.get("entry_min_evidence_count"))

            per_market_risk_cap_fraction_nav = _as_float(canonical_policy.get("per_market_risk_cap_fraction_nav"))
            if per_market_risk_cap_fraction_nav is not None and per_market_risk_cap_fraction_nav > 0:
                per_market_risk_cap_dollars = round(planning_bankroll_dollars * per_market_risk_cap_fraction_nav, 4)
            release_cluster_risk_cap_fraction_nav = _as_float(
                canonical_policy.get("release_cluster_risk_cap_fraction_nav")
            )
            if release_cluster_risk_cap_fraction_nav is not None and release_cluster_risk_cap_fraction_nav > 0:
                release_cluster_risk_cap_dollars = round(
                    planning_bankroll_dollars * release_cluster_risk_cap_fraction_nav,
                    4,
                )
            same_day_correlated_risk_cap_fraction_nav = _as_float(
                canonical_policy.get("same_day_correlated_risk_cap_fraction_nav")
            )
            if same_day_correlated_risk_cap_fraction_nav is not None and same_day_correlated_risk_cap_fraction_nav > 0:
                same_day_correlated_risk_cap_dollars = round(
                    planning_bankroll_dollars * same_day_correlated_risk_cap_fraction_nav,
                    4,
                )

        if maker_edge_conservative < effective_min_maker_edge:
            skip_counts["maker_edge_below_min"] += 1
            continue
        incentive_bonus_per_contract = (
            float(incentive_bonus_per_contract_by_ticker.get(ticker, 0.0))
            if isinstance(incentive_bonus_per_contract_by_ticker, dict)
            else 0.0
        )
        maker_edge_net_with_incentive = float(maker_edge_net) + incentive_bonus_per_contract
        maker_edge_conservative_net_with_incentive = float(maker_edge_conservative_net) + incentive_bonus_per_contract
        if maker_edge_conservative_net_with_incentive < effective_min_maker_edge_net_fees:
            skip_counts["maker_edge_net_fees_below_min"] += 1
            continue
        if price < effective_min_entry_price_dollars:
            skip_counts["entry_price_below_min"] += 1
            continue
        if price > effective_max_entry_price_dollars:
            skip_counts["entry_price_above_max"] += 1
            continue

        if isinstance(routine_max_hours_to_close, (int, float)) and routine_max_hours_to_close > 0:
            hours_to_close_value = _as_float(row.get("hours_to_close"))
            effective_max_hours_to_close = float(routine_max_hours_to_close)
            niche_max_hours = max_hours_by_niche.get(canonical_niche_normalized)
            if isinstance(niche_max_hours, float) and niche_max_hours > 0:
                effective_max_hours_to_close = niche_max_hours
            if (
                isinstance(hours_to_close_value, float)
                and hours_to_close_value > effective_max_hours_to_close
                and canonical_niche_normalized not in longdated_allowed_niches
            ):
                skip_counts["routine_hours_to_close_above_max"] += 1
                continue

        if effective_min_confidence is not None:
            confidence_value = _as_float(row.get("confidence"))
            if confidence_value is None or confidence_value < effective_min_confidence:
                skip_counts["canonical_confidence_below_min"] += 1
                continue

        if effective_min_evidence_count is not None:
            evidence_count_value = _as_int(row.get("evidence_count"))
            if evidence_count_value is None or evidence_count_value < effective_min_evidence_count:
                skip_counts["canonical_evidence_below_min"] += 1
                continue

        if effective_max_spread_dollars is not None:
            latest_yes_bid = _as_float(row.get("latest_yes_bid_dollars"))
            latest_yes_ask = _as_float(row.get("latest_yes_ask_dollars"))
            spread_dollars = (
                round(max(0.0, latest_yes_ask - latest_yes_bid), 6)
                if latest_yes_bid is not None and latest_yes_ask is not None
                else None
            )
            if spread_dollars is None or spread_dollars > effective_max_spread_dollars:
                skip_counts["canonical_spread_above_max"] += 1
                continue

        estimated_entry_cost = round(float(price) * contracts_per_order, 4)
        if per_market_risk_cap_dollars is not None and estimated_entry_cost > per_market_risk_cap_dollars + 1e-9:
            skip_counts["canonical_per_market_risk_cap"] += 1
            continue
        if canonical_release_cluster and release_cluster_risk_cap_dollars is not None:
            cluster_spent = float(release_cluster_spent_dollars.get(canonical_release_cluster, 0.0))
            if cluster_spent + estimated_entry_cost > release_cluster_risk_cap_dollars + 1e-9:
                skip_counts["canonical_release_cluster_risk_cap"] += 1
                continue
        if canonical_niche and same_day_correlated_risk_cap_dollars is not None:
            group_spent = float(correlated_group_spent_dollars.get(canonical_niche, 0.0))
            if group_spent + estimated_entry_cost > same_day_correlated_risk_cap_dollars + 1e-9:
                skip_counts["canonical_correlated_risk_cap"] += 1
                continue
        if estimated_entry_cost > remaining_risk + 1e-9:
            skip_counts["budget_too_small"] += 1
            continue

        fair_probability = conservative_candidate.get("fair_probability_midpoint")
        fair_probability_conservative = conservative_candidate.get("fair_probability_conservative")
        fee_estimate = estimate_trade_fee(
            price_dollars=float(price),
            contract_count=contracts_per_order,
            is_maker=True,
            market_ticker=ticker,
            fee_multiplier_override=maker_fee_multiplier_override,
            conservative_rounding=conservative_fee_rounding,
        )
        estimated_max_profit = round((1.0 - float(price)) * contracts_per_order, 4)
        expected_value_dollars = round(float(maker_edge) * contracts_per_order, 4)
        expected_value_net_dollars = round(float(maker_edge_net_with_incentive) * contracts_per_order, 4)
        expected_value_conservative_dollars = round(float(maker_edge_conservative) * contracts_per_order, 4)
        expected_value_conservative_net_dollars = round(
            float(maker_edge_conservative_net_with_incentive) * contracts_per_order,
            4,
        )
        expected_incentive_value_dollars = round(incentive_bonus_per_contract * contracts_per_order, 6)
        expected_roi_on_cost = round(expected_value_dollars / estimated_entry_cost, 6) if estimated_entry_cost > 0 else ""
        expected_roi_on_cost_net = (
            round(expected_value_net_dollars / estimated_entry_cost, 6) if estimated_entry_cost > 0 else ""
        )
        expected_roi_on_cost_conservative = (
            round(expected_value_conservative_dollars / estimated_entry_cost, 6) if estimated_entry_cost > 0 else ""
        )
        expected_roi_on_cost_conservative_net = (
            round(expected_value_conservative_net_dollars / estimated_entry_cost, 6) if estimated_entry_cost > 0 else ""
        )
        max_profit_roi_on_cost = round(estimated_max_profit / estimated_entry_cost, 6) if estimated_entry_cost > 0 else ""
        hours_to_close = row.get("hours_to_close")
        expected_value_per_day_dollars: float | str = ""
        expected_value_per_day_net_dollars: float | str = ""
        expected_value_per_day_conservative_dollars: float | str = ""
        expected_value_per_day_conservative_net_dollars: float | str = ""
        expected_roi_per_day: float | str = ""
        expected_roi_per_day_net: float | str = ""
        expected_roi_per_day_conservative: float | str = ""
        expected_roi_per_day_conservative_net: float | str = ""
        if isinstance(hours_to_close, (int, float)) and hours_to_close > 0:
            days_to_close = hours_to_close / 24.0
            expected_value_per_day_dollars = round(expected_value_dollars / days_to_close, 6)
            expected_value_per_day_net_dollars = round(expected_value_net_dollars / days_to_close, 6)
            expected_value_per_day_conservative_dollars = round(
                expected_value_conservative_dollars / days_to_close,
                6,
            )
            expected_value_per_day_conservative_net_dollars = round(
                expected_value_conservative_net_dollars / days_to_close,
                6,
            )
            if isinstance(expected_roi_on_cost, float):
                expected_roi_per_day = round(expected_roi_on_cost / days_to_close, 6)
            if isinstance(expected_roi_on_cost_net, float):
                expected_roi_per_day_net = round(expected_roi_on_cost_net / days_to_close, 6)
            if isinstance(expected_roi_on_cost_conservative, float):
                expected_roi_per_day_conservative = round(expected_roi_on_cost_conservative / days_to_close, 6)
            if isinstance(expected_roi_on_cost_conservative_net, float):
                expected_roi_per_day_conservative_net = round(
                    expected_roi_on_cost_conservative_net / days_to_close,
                    6,
                )
        plans.append(
            {
                "plan_rank": len(plans) + 1,
                "category": row.get("category"),
                "market_ticker": row.get("market_ticker"),
                "market_title": row.get("market_title"),
                "close_time": row.get("close_time"),
                "hours_to_close": hours_to_close,
                "side": side,
                "canonical_ticker": canonical_ticker or "",
                "canonical_niche": canonical_niche or "",
                "canonical_release_cluster": canonical_release_cluster or "",
                "canonical_policy_applied": canonical_policy_applied,
                "canonical_mapping_match_type": canonical_mapping_match_type or "",
                "canonical_mapping_match_key": canonical_mapping_match_key or "",
                "maker_entry_price_dollars": round(float(price), 6),
                "maker_entry_edge": round(float(maker_edge), 6),
                "maker_entry_edge_net_fees": round(float(maker_edge_net), 6),
                "maker_entry_edge_net_total": round(float(maker_edge_net_with_incentive), 6),
                "maker_entry_edge_conservative": round(float(maker_edge_conservative), 6),
                "maker_entry_edge_conservative_net_fees": round(float(maker_edge_conservative_net), 6),
                "maker_entry_edge_conservative_net_total": round(float(maker_edge_conservative_net_with_incentive), 6),
                "effective_min_maker_edge": round(effective_min_maker_edge, 6),
                "effective_min_maker_edge_net_fees": round(effective_min_maker_edge_net_fees, 6),
                "effective_min_entry_price_dollars": round(effective_min_entry_price_dollars, 6),
                "effective_max_entry_price_dollars": round(effective_max_entry_price_dollars, 6),
                "effective_max_spread_dollars": (
                    round(effective_max_spread_dollars, 6)
                    if isinstance(effective_max_spread_dollars, (int, float))
                    else ""
                ),
                "effective_min_confidence": (
                    round(effective_min_confidence, 6) if isinstance(effective_min_confidence, (int, float)) else ""
                ),
                "effective_min_evidence_count": (
                    int(effective_min_evidence_count) if isinstance(effective_min_evidence_count, int) else ""
                ),
                "effective_per_market_risk_cap_dollars": (
                    round(per_market_risk_cap_dollars, 4) if isinstance(per_market_risk_cap_dollars, (int, float)) else ""
                ),
                "effective_release_cluster_risk_cap_dollars": (
                    round(release_cluster_risk_cap_dollars, 4)
                    if isinstance(release_cluster_risk_cap_dollars, (int, float))
                    else ""
                ),
                "effective_same_day_correlated_risk_cap_dollars": (
                    round(same_day_correlated_risk_cap_dollars, 4)
                    if isinstance(same_day_correlated_risk_cap_dollars, (int, float))
                    else ""
                ),
                "incentive_bonus_per_contract_dollars": round(incentive_bonus_per_contract, 6),
                "fair_probability": fair_probability,
                "fair_probability_conservative": fair_probability_conservative,
                "confidence": row.get("confidence"),
                "contracts_per_order": contracts_per_order,
                "estimated_entry_cost_dollars": estimated_entry_cost,
                "estimated_entry_fee_dollars": fee_estimate.rounded_fee_dollars,
                "estimated_entry_fee_per_contract_dollars": fee_estimate.fee_per_contract_dollars,
                "expected_incentive_value_dollars": expected_incentive_value_dollars,
                "expected_value_dollars": expected_value_dollars,
                "expected_value_net_dollars": expected_value_net_dollars,
                "expected_value_conservative_dollars": expected_value_conservative_dollars,
                "expected_value_conservative_net_dollars": expected_value_conservative_net_dollars,
                "expected_roi_on_cost": expected_roi_on_cost,
                "expected_roi_on_cost_net": expected_roi_on_cost_net,
                "expected_roi_on_cost_conservative": expected_roi_on_cost_conservative,
                "expected_roi_on_cost_conservative_net": expected_roi_on_cost_conservative_net,
                "expected_value_per_day_dollars": expected_value_per_day_dollars,
                "expected_value_per_day_net_dollars": expected_value_per_day_net_dollars,
                "expected_value_per_day_conservative_dollars": expected_value_per_day_conservative_dollars,
                "expected_value_per_day_conservative_net_dollars": expected_value_per_day_conservative_net_dollars,
                "expected_roi_per_day": expected_roi_per_day,
                "expected_roi_per_day_net": expected_roi_per_day_net,
                "expected_roi_per_day_conservative": expected_roi_per_day_conservative,
                "expected_roi_per_day_conservative_net": expected_roi_per_day_conservative_net,
                "estimated_max_loss_dollars": estimated_entry_cost,
                "estimated_max_profit_dollars": estimated_max_profit,
                "max_profit_roi_on_cost": max_profit_roi_on_cost,
                "planning_bankroll_fraction": round(estimated_entry_cost / planning_bankroll_dollars, 6),
                "thesis": row.get("thesis"),
                "order_payload_preview": _build_order_payload_preview(
                    ticker=str(row.get("market_ticker") or ""),
                    count=contracts_per_order,
                    side=side,
                    price_dollars=float(price),
                ),
            }
        )
        if canonical_release_cluster:
            release_cluster_spent_dollars[canonical_release_cluster] = round(
                float(release_cluster_spent_dollars.get(canonical_release_cluster, 0.0)) + estimated_entry_cost,
                4,
            )
        if canonical_niche:
            correlated_group_spent_dollars[canonical_niche] = round(
                float(correlated_group_spent_dollars.get(canonical_niche, 0.0)) + estimated_entry_cost,
                4,
            )
        remaining_risk = round(remaining_risk - estimated_entry_cost, 4)

    return plans, skip_counts


def _write_plan_csv(path: Path, plans: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "plan_rank",
        "category",
        "market_ticker",
        "market_title",
        "close_time",
        "hours_to_close",
        "side",
        "canonical_ticker",
        "canonical_niche",
        "canonical_release_cluster",
        "canonical_policy_applied",
        "canonical_mapping_match_type",
        "canonical_mapping_match_key",
        "maker_entry_price_dollars",
        "maker_entry_edge",
        "maker_entry_edge_net_fees",
        "maker_entry_edge_net_total",
        "maker_entry_edge_conservative",
        "maker_entry_edge_conservative_net_fees",
        "maker_entry_edge_conservative_net_total",
        "effective_min_maker_edge",
        "effective_min_maker_edge_net_fees",
        "effective_min_entry_price_dollars",
        "effective_max_entry_price_dollars",
        "effective_max_spread_dollars",
        "effective_min_confidence",
        "effective_min_evidence_count",
        "effective_per_market_risk_cap_dollars",
        "effective_release_cluster_risk_cap_dollars",
        "effective_same_day_correlated_risk_cap_dollars",
        "incentive_bonus_per_contract_dollars",
        "fair_probability",
        "fair_probability_conservative",
        "confidence",
        "contracts_per_order",
        "estimated_entry_cost_dollars",
        "estimated_entry_fee_dollars",
        "estimated_entry_fee_per_contract_dollars",
        "expected_incentive_value_dollars",
        "expected_value_dollars",
        "expected_value_net_dollars",
        "expected_value_conservative_dollars",
        "expected_value_conservative_net_dollars",
        "expected_roi_on_cost",
        "expected_roi_on_cost_net",
        "expected_roi_on_cost_conservative",
        "expected_roi_on_cost_conservative_net",
        "expected_value_per_day_dollars",
        "expected_value_per_day_net_dollars",
        "expected_value_per_day_conservative_dollars",
        "expected_value_per_day_conservative_net_dollars",
        "expected_roi_per_day",
        "expected_roi_per_day_net",
        "expected_roi_per_day_conservative",
        "expected_roi_per_day_conservative_net",
        "estimated_max_loss_dollars",
        "estimated_max_profit_dollars",
        "max_profit_roi_on_cost",
        "planning_bankroll_fraction",
        "thesis",
        "order_payload_preview",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for plan in plans:
            serializable = dict(plan)
            serializable["order_payload_preview"] = json.dumps(plan["order_payload_preview"], separators=(",", ":"))
            writer.writerow(serializable)


def run_kalshi_micro_prior_plan(
    *,
    env_file: str | None = None,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    contracts_per_order: int = 1,
    max_orders: int = 3,
    min_maker_edge: float = 0.005,
    min_maker_edge_net_fees: float = 0.0,
    min_entry_price_dollars: float = 0.0,
    max_entry_price_dollars: float = 0.99,
    routine_max_hours_to_close: float | None = None,
    max_hours_to_close_by_canonical_niche: dict[str, float] | None = None,
    routine_longdated_allowed_niches: tuple[str, ...] | set[str] | None = None,
    canonical_mapping_csv: str | None = "data/research/canonical_contract_mapping.csv",
    canonical_threshold_csv: str | None = "data/research/canonical_threshold_library.csv",
    prefer_canonical_thresholds: bool = True,
    require_canonical_mapping: bool = False,
    allowed_canonical_niches: tuple[str, ...] | set[str] | None = None,
    top_n: int = 10,
    timeout_seconds: float = 15.0,
    balance_cache_file: str | None = None,
    max_balance_cache_age_seconds: float = 86400.0,
    book_db_path: str | None = None,
    maker_fee_multiplier_override: float | None = None,
    taker_fee_multiplier_override: float | None = None,
    conservative_fee_rounding: bool = True,
    include_incentives: bool = False,
    http_request_json=_http_request_json,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    env_data = _parse_env_file(Path(env_file)) if env_file else {}
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
    incentive_bonus_per_contract_by_ticker: dict[str, float] = {}
    canonical_policy_by_live_ticker: dict[str, dict[str, Any]] = {}
    canonical_policy_alias_by_lookup_key: dict[str, dict[str, Any]] = {}
    normalized_allowed_niches = _normalized_allowed_canonical_niches(allowed_canonical_niches)
    canonical_policy_diagnostics: dict[str, Any] = {
        "canonical_policy_enabled": False,
        "canonical_policy_reason": "disabled_by_flag",
    }
    if prefer_canonical_thresholds:
        (
            canonical_policy_by_live_ticker,
            canonical_policy_alias_by_lookup_key,
            canonical_policy_diagnostics,
        ) = _canonical_policy_index(
            canonical_mapping_csv=canonical_mapping_csv,
            canonical_threshold_csv=canonical_threshold_csv,
            allowed_canonical_niches=normalized_allowed_niches,
        )
    plans, skip_counts = build_micro_prior_plans(
        enriched_rows=enriched_rows,
        planning_bankroll_dollars=planning_bankroll_dollars,
        daily_risk_cap_dollars=daily_risk_cap_dollars,
        contracts_per_order=contracts_per_order,
        max_orders=max_orders,
        min_maker_edge=min_maker_edge,
        min_maker_edge_net_fees=min_maker_edge_net_fees,
        min_entry_price_dollars=min_entry_price_dollars,
        max_entry_price_dollars=max_entry_price_dollars,
        routine_max_hours_to_close=routine_max_hours_to_close,
        max_hours_to_close_by_canonical_niche=max_hours_to_close_by_canonical_niche,
        routine_longdated_allowed_niches=(
            {value.strip().lower() for value in routine_longdated_allowed_niches if value.strip()}
            if routine_longdated_allowed_niches
            else None
        ),
        maker_fee_multiplier_override=maker_fee_multiplier_override,
        conservative_fee_rounding=conservative_fee_rounding,
        incentive_bonus_per_contract_by_ticker=incentive_bonus_per_contract_by_ticker,
        canonical_policy_by_live_ticker=canonical_policy_by_live_ticker,
        canonical_policy_alias_by_lookup_key=canonical_policy_alias_by_lookup_key,
        require_canonical_mapping=require_canonical_mapping,
        allowed_canonical_niches=normalized_allowed_niches,
    )
    live_balance_cents: int | None = None
    balance_error: str | None = None
    balance_source = "unknown"
    balance_cache_age_seconds: float | None = None
    if env_file:
        access_key_id = env_data.get("KALSHI_ACCESS_KEY_ID")
        private_key_path = env_data.get("KALSHI_PRIVATE_KEY_PATH")
        kalshi_env = (env_data.get("KALSHI_ENV") or "prod").strip().lower()
        balance_cache_path = Path(balance_cache_file) if balance_cache_file else default_balance_cache_path(output_dir)
        if include_incentives and not _is_placeholder(access_key_id) and not _is_placeholder(private_key_path):
            try:
                incentive_bonus_per_contract_by_ticker = fetch_incentive_map(
                    env_data=env_data,
                    timeout_seconds=timeout_seconds,
                    http_request_json=http_request_json,
                    sign_request=sign_request,
                )
            except Exception:
                incentive_bonus_per_contract_by_ticker = {}
            if incentive_bonus_per_contract_by_ticker:
                plans, skip_counts = build_micro_prior_plans(
                    enriched_rows=enriched_rows,
                    planning_bankroll_dollars=planning_bankroll_dollars,
                    daily_risk_cap_dollars=daily_risk_cap_dollars,
                    contracts_per_order=contracts_per_order,
                    max_orders=max_orders,
                    min_maker_edge=min_maker_edge,
                    min_maker_edge_net_fees=min_maker_edge_net_fees,
                    min_entry_price_dollars=min_entry_price_dollars,
                    max_entry_price_dollars=max_entry_price_dollars,
                    routine_max_hours_to_close=routine_max_hours_to_close,
                    max_hours_to_close_by_canonical_niche=max_hours_to_close_by_canonical_niche,
                    routine_longdated_allowed_niches=(
                        {value.strip().lower() for value in routine_longdated_allowed_niches if value.strip()}
                        if routine_longdated_allowed_niches
                        else None
                    ),
                    maker_fee_multiplier_override=maker_fee_multiplier_override,
                    conservative_fee_rounding=conservative_fee_rounding,
                    incentive_bonus_per_contract_by_ticker=incentive_bonus_per_contract_by_ticker,
                    canonical_policy_by_live_ticker=canonical_policy_by_live_ticker,
                    canonical_policy_alias_by_lookup_key=canonical_policy_alias_by_lookup_key,
                    require_canonical_mapping=require_canonical_mapping,
                    allowed_canonical_niches=normalized_allowed_niches,
                )
        if not _is_placeholder(access_key_id) and not _is_placeholder(private_key_path):
            try:
                balance_snapshot = _kalshi_balance_snapshot(
                    access_key_id=access_key_id or "",
                    private_key_path=private_key_path or "",
                    env_name=kalshi_env,
                    timeout_seconds=timeout_seconds,
                    http_get_json=http_get_json,
                    sign_request=sign_request,
                )
                if isinstance(balance_snapshot.get("balance_cents"), (int, float)):
                    live_balance_cents = int(balance_snapshot["balance_cents"])
                    balance_source = "live"
                    _write_balance_cache(
                        balance_cache_path,
                        balance_cents=live_balance_cents,
                        captured_at=captured_at,
                        kalshi_env=kalshi_env,
                    )
            except Exception as exc:  # pragma: no cover - defensive summary path
                balance_error = str(exc)
        if live_balance_cents is None:
            cached_balance = _load_balance_cache(
                balance_cache_path,
                captured_at=captured_at,
                kalshi_env=kalshi_env,
                max_age_seconds=max_balance_cache_age_seconds,
            )
            if cached_balance is not None:
                live_balance_cents = int(cached_balance["balance_cents"])
                balance_source = "cache"
                balance_cache_age_seconds = float(cached_balance["age_seconds"])
    else:
        balance_cache_path = Path(balance_cache_file) if balance_cache_file else default_balance_cache_path(output_dir)

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_micro_prior_plan_{stamp}.csv"
    _write_plan_csv(csv_path, plans)
    effective_book_db = Path(book_db_path) if book_db_path else default_book_db_path(output_dir)
    record_decisions(
        book_db_path=effective_book_db,
        source="kalshi_micro_prior_plan",
        captured_at=captured_at,
        plans=plans,
    )
    canonical_covered_tickers = {
        _normalize_market_ticker(plan.get("market_ticker"))
        for plan in plans
        if isinstance(plan, dict)
        and bool(plan.get("canonical_policy_applied"))
    }

    def _row_has_canonical_policy(row: dict[str, Any]) -> bool:
        canonical_policy, _, _ = _resolve_canonical_policy_for_ticker(
            market_ticker=row.get("market_ticker"),
            canonical_policy_by_live_ticker=canonical_policy_by_live_ticker,
            canonical_policy_alias_by_lookup_key=canonical_policy_alias_by_lookup_key,
        )
        return bool(canonical_policy)

    canonical_unmapped_analysis = _summarize_unmapped_canonical_markets(
        enriched_rows=enriched_rows,
        canonical_policy_by_live_ticker=canonical_policy_by_live_ticker,
        canonical_policy_alias_by_lookup_key=canonical_policy_alias_by_lookup_key,
        allowed_canonical_niches=normalized_allowed_niches,
        max_rows=25,
    )

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "priors_csv": str(priors_path),
        "history_csv": str(history_path),
        "prior_rows": len(prior_rows),
        "canonical_mapping_csv": canonical_policy_diagnostics.get("canonical_mapping_csv"),
        "canonical_threshold_csv": canonical_policy_diagnostics.get("canonical_threshold_csv"),
        "canonical_policy_enabled": bool(canonical_policy_diagnostics.get("canonical_policy_enabled")),
        "canonical_policy_reason": canonical_policy_diagnostics.get("canonical_policy_reason"),
        "prefer_canonical_thresholds": prefer_canonical_thresholds,
        "require_canonical_mapping": require_canonical_mapping,
        "min_entry_price_dollars": min_entry_price_dollars,
        "routine_max_hours_to_close": routine_max_hours_to_close,
        "max_hours_to_close_by_canonical_niche": max_hours_to_close_by_canonical_niche,
        "routine_longdated_allowed_niches": (
            sorted({value.strip().lower() for value in routine_longdated_allowed_niches if value.strip()})
            if routine_longdated_allowed_niches
            else None
        ),
        "allowed_canonical_niches": sorted(normalized_allowed_niches) if normalized_allowed_niches else None,
        "canonical_policy_live_tickers": canonical_policy_diagnostics.get("canonical_policy_live_tickers"),
        "canonical_policy_alias_lookup_keys": canonical_policy_diagnostics.get("canonical_policy_alias_lookup_keys"),
        "canonical_policy_alias_collision_keys": canonical_policy_diagnostics.get("canonical_policy_alias_collision_keys"),
        "canonical_mapping_rows": canonical_policy_diagnostics.get("canonical_mapping_rows"),
        "canonical_threshold_rows": canonical_policy_diagnostics.get("canonical_threshold_rows"),
        "canonical_threshold_rows_allowed_niche": canonical_policy_diagnostics.get(
            "canonical_threshold_rows_allowed_niche"
        ),
        "canonical_threshold_rows_disallowed_niche": canonical_policy_diagnostics.get(
            "canonical_threshold_rows_disallowed_niche"
        ),
        "canonical_mapped_rows": canonical_policy_diagnostics.get("canonical_mapped_rows"),
        "canonical_policy_missing_files": canonical_policy_diagnostics.get("canonical_policy_missing_files"),
        "matched_live_markets": sum(1 for row in enriched_rows if row.get("matched_live_market")),
        "matched_live_markets_with_canonical_policy": sum(
            1
            for row in enriched_rows
            if _row_has_canonical_policy(row)
        ),
        "positive_maker_entry_markets": sum(
            1 for row in enriched_rows
            if isinstance(row.get("best_maker_entry_edge"), (int, float)) and row["best_maker_entry_edge"] > 0
        ),
        "positive_maker_entry_markets_net_fees": sum(
            1
            for row in enriched_rows
            if isinstance(row.get("best_maker_entry_edge_net_fees"), (int, float))
            and row["best_maker_entry_edge_net_fees"] > 0
        ),
        "positive_maker_entry_markets_with_canonical_policy": sum(
            1
            for row in enriched_rows
            if _row_has_canonical_policy(row)
            and isinstance(row.get("best_maker_entry_edge"), (int, float))
            and row["best_maker_entry_edge"] > 0
        ),
        "positive_maker_entry_markets_net_fees_with_canonical_policy": sum(
            1
            for row in enriched_rows
            if _row_has_canonical_policy(row)
            and isinstance(row.get("best_maker_entry_edge_net_fees"), (int, float))
            and row["best_maker_entry_edge_net_fees"] > 0
        ),
        "canonical_unmapped_total": canonical_unmapped_analysis["total"],
        "canonical_unmapped_in_allowed_niche_guess": canonical_unmapped_analysis["in_allowed_niche_guess"],
        "canonical_unmapped_outside_allowed_niche_guess": canonical_unmapped_analysis["outside_allowed_niche_guess"],
        "canonical_unmapped_counts_by_niche_guess": canonical_unmapped_analysis["counts_by_niche_guess"],
        "canonical_unmapped_top_markets": canonical_unmapped_analysis["top_markets"],
        "planned_orders": len(plans),
        "top_market_ticker": plans[0]["market_ticker"] if plans else None,
        "top_market_title": plans[0]["market_title"] if plans else None,
        "top_market_close_time": plans[0]["close_time"] if plans else None,
        "top_market_hours_to_close": plans[0]["hours_to_close"] if plans else None,
        "top_market_side": plans[0]["side"] if plans else None,
        "top_market_canonical_ticker": plans[0]["canonical_ticker"] if plans else None,
        "top_market_canonical_niche": plans[0]["canonical_niche"] if plans else None,
        "top_market_canonical_release_cluster": plans[0]["canonical_release_cluster"] if plans else None,
        "top_market_canonical_policy_applied": plans[0]["canonical_policy_applied"] if plans else None,
        "top_market_canonical_mapping_match_type": plans[0]["canonical_mapping_match_type"] if plans else None,
        "top_market_canonical_mapping_match_key": plans[0]["canonical_mapping_match_key"] if plans else None,
        "top_market_maker_entry_price_dollars": plans[0]["maker_entry_price_dollars"] if plans else None,
        "top_market_maker_entry_edge": plans[0]["maker_entry_edge"] if plans else None,
        "top_market_maker_entry_edge_net_fees": plans[0]["maker_entry_edge_net_fees"] if plans else None,
        "top_market_maker_entry_edge_net_total": plans[0]["maker_entry_edge_net_total"] if plans else None,
        "top_market_maker_entry_edge_conservative": (
            plans[0]["maker_entry_edge_conservative"] if plans else None
        ),
        "top_market_maker_entry_edge_conservative_net_fees": (
            plans[0]["maker_entry_edge_conservative_net_fees"] if plans else None
        ),
        "top_market_maker_entry_edge_conservative_net_total": (
            plans[0]["maker_entry_edge_conservative_net_total"] if plans else None
        ),
        "top_market_incentive_bonus_per_contract_dollars": (
            plans[0]["incentive_bonus_per_contract_dollars"] if plans else None
        ),
        "top_market_estimated_entry_cost_dollars": plans[0]["estimated_entry_cost_dollars"] if plans else None,
        "top_market_estimated_entry_fee_dollars": plans[0]["estimated_entry_fee_dollars"] if plans else None,
        "top_market_expected_incentive_value_dollars": plans[0]["expected_incentive_value_dollars"] if plans else None,
        "top_market_expected_value_dollars": plans[0]["expected_value_dollars"] if plans else None,
        "top_market_expected_value_net_dollars": plans[0]["expected_value_net_dollars"] if plans else None,
        "top_market_expected_value_conservative_dollars": (
            plans[0]["expected_value_conservative_dollars"] if plans else None
        ),
        "top_market_expected_value_conservative_net_dollars": (
            plans[0]["expected_value_conservative_net_dollars"] if plans else None
        ),
        "top_market_expected_roi_on_cost": plans[0]["expected_roi_on_cost"] if plans else None,
        "top_market_expected_roi_on_cost_net": plans[0]["expected_roi_on_cost_net"] if plans else None,
        "top_market_expected_roi_on_cost_conservative": (
            plans[0]["expected_roi_on_cost_conservative"] if plans else None
        ),
        "top_market_expected_roi_on_cost_conservative_net": (
            plans[0]["expected_roi_on_cost_conservative_net"] if plans else None
        ),
        "top_market_expected_value_per_day_dollars": plans[0]["expected_value_per_day_dollars"] if plans else None,
        "top_market_expected_value_per_day_net_dollars": (
            plans[0]["expected_value_per_day_net_dollars"] if plans else None
        ),
        "top_market_expected_value_per_day_conservative_dollars": (
            plans[0]["expected_value_per_day_conservative_dollars"] if plans else None
        ),
        "top_market_expected_value_per_day_conservative_net_dollars": (
            plans[0]["expected_value_per_day_conservative_net_dollars"] if plans else None
        ),
        "top_market_expected_roi_per_day": plans[0]["expected_roi_per_day"] if plans else None,
        "top_market_expected_roi_per_day_net": plans[0]["expected_roi_per_day_net"] if plans else None,
        "top_market_expected_roi_per_day_conservative": (
            plans[0]["expected_roi_per_day_conservative"] if plans else None
        ),
        "top_market_expected_roi_per_day_conservative_net": (
            plans[0]["expected_roi_per_day_conservative_net"] if plans else None
        ),
        "top_market_estimated_max_profit_dollars": plans[0]["estimated_max_profit_dollars"] if plans else None,
        "top_market_estimated_max_loss_dollars": plans[0]["estimated_max_loss_dollars"] if plans else None,
        "top_market_max_profit_roi_on_cost": plans[0]["max_profit_roi_on_cost"] if plans else None,
        "top_market_fair_probability": plans[0]["fair_probability"] if plans else None,
        "top_market_fair_probability_conservative": (
            plans[0]["fair_probability_conservative"] if plans else None
        ),
        "top_market_confidence": plans[0]["confidence"] if plans else None,
        "top_market_thesis": plans[0]["thesis"] if plans else None,
        "total_planned_cost_dollars": round(sum(float(plan["estimated_entry_cost_dollars"]) for plan in plans), 4),
        "actual_live_balance_dollars": round(live_balance_cents / 100.0, 2) if live_balance_cents is not None else None,
        "actual_live_balance_source": balance_source,
        "balance_live_verified": balance_source == "live",
        "funding_gap_dollars": (
            round(max(0.0, sum(float(plan["estimated_entry_cost_dollars"]) for plan in plans) - (live_balance_cents / 100.0)), 4)
            if live_balance_cents is not None
            else None
        ),
        "balance_check_error": balance_error,
        "balance_cache_file": str(balance_cache_path),
        "balance_cache_age_seconds": balance_cache_age_seconds,
        "events_error": None,
        "board_warning": None,
        "include_incentives": include_incentives,
        "incentive_markets_loaded": len(incentive_bonus_per_contract_by_ticker),
        "skip_counts": skip_counts,
        "canonical_covered_planned_tickers": sorted(ticker for ticker in canonical_covered_tickers if ticker),
        "top_plans": plans[:top_n],
        "status": "no_candidates" if not plans else "ready",
        "orders": plans,
        "book_db_path": str(effective_book_db),
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_micro_prior_plan_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
