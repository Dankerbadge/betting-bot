from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_book import count_open_positions, default_book_db_path
from betbot.kelly_unified import DEFAULT_MIN_KELLY_USED as KELLY_UNIFIED_MIN_LIVE_USED_DEFAULT
from betbot.kalshi_micro_execute import (
    AuthenticatedRequester,
    TimeSleeper,
    run_kalshi_micro_execute,
    _http_request_json,
)
from betbot.kalshi_micro_ledger import default_ledger_path, summarize_trade_ledger, trading_day_for_timestamp
from betbot.kalshi_micro_prior_plan import LIVE_ALLOWED_CANONICAL_NICHES, run_kalshi_micro_prior_plan
from betbot.kalshi_nonsports_quality import _parse_timestamp, load_history_rows
from betbot.runtime_version import (
    build_runtime_version_block,
    file_mtime_utc,
    infer_fill_model_mode,
)
from betbot.kalshi_weather_settlement import build_weather_settlement_spec
from betbot.live_smoke import HttpGetter, KalshiSigner, _http_get_json, _kalshi_sign_request
from betbot.onboarding import _parse_env_file


_DAILY_WEATHER_CONTRACT_FAMILIES = {"daily_rain", "daily_temperature", "daily_snow"}
_CLIMATE_ROUTER_PILOT_DEFAULT_ALLOWED_CLASSES = ("tradable_positive", "hot_positive")


def _normalize_contract_family_filters(values: tuple[str, ...] | None) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    if values:
        for raw_value in values:
            token = str(raw_value or "").strip().lower()
            if not token or token in seen:
                continue
            normalized.append(token)
            seen.add(token)
    return tuple(normalized)


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "t"}:
        return True
    if text in {"0", "false", "no", "n", "f"}:
        return False
    return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _is_orderable_price(value: Any) -> bool:
    parsed = _as_float(value)
    return isinstance(parsed, float) and 0.0 < parsed < 1.0


def _build_order_payload_preview(*, ticker: str, count: int, side: str, price_dollars: float) -> dict[str, Any]:
    normalized_side = side.strip().lower()
    payload: dict[str, Any] = {
        "ticker": ticker,
        "count": max(1, int(count)),
        "side": "no" if normalized_side == "no" else "yes",
        "type": "limit",
    }
    if payload["side"] == "no":
        payload["no_price_dollars"] = round(float(price_dollars), 6)
    else:
        payload["yes_price_dollars"] = round(float(price_dollars), 6)
    return payload


def _latest_climate_router_summary_path(output_dir: str) -> Path | None:
    directory = Path(output_dir)
    candidates = sorted(
        directory.glob("kalshi_climate_router_summary_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        if path.is_file():
            return path
    return None


def _load_climate_router_summary(
    *,
    output_dir: str,
    explicit_path: str | None = None,
) -> tuple[dict[str, Any] | None, str | None, str]:
    summary_path: Path | None = None
    selection_mode = "missing"
    if explicit_path:
        summary_path = Path(explicit_path)
        selection_mode = "explicit_path"
    else:
        summary_path = _latest_climate_router_summary_path(output_dir)
        selection_mode = "latest_mtime"
    if summary_path is None:
        return None, None, selection_mode
    if not summary_path.exists() or not summary_path.is_file():
        return None, str(summary_path), selection_mode
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return None, str(summary_path), selection_mode
    if not isinstance(payload, dict):
        return None, str(summary_path), selection_mode
    return payload, str(summary_path), selection_mode


def _normalize_climate_router_allowed_classes(values: tuple[str, ...] | None) -> tuple[str, ...]:
    normalized: set[str] = set()
    if values:
        for raw_value in values:
            token = str(raw_value or "").strip().lower()
            if not token:
                continue
            if token == "tradable":
                normalized.update({"tradable_positive", "hot_positive"})
            elif token == "hot":
                normalized.add("hot_positive")
            else:
                normalized.add(token)
    if not normalized:
        normalized = set(_CLIMATE_ROUTER_PILOT_DEFAULT_ALLOWED_CLASSES)
    return tuple(sorted(normalized))


def _build_climate_router_pilot_plan_row(
    *,
    candidate: dict[str, Any],
    contracts: int,
    planning_bankroll_dollars: float,
    shadow_rank: int,
) -> dict[str, Any] | None:
    ticker = str(candidate.get("market_ticker") or "").strip()
    side = str(candidate.get("theoretical_side") or "").strip().lower()
    reference_price = _as_float(candidate.get("theoretical_reference_price"))
    theoretical_edge = _as_float(candidate.get("theoretical_edge_net"))
    fair_yes = _as_float(candidate.get("fair_yes_probability"))
    fair_no = _as_float(candidate.get("fair_no_probability"))
    if not ticker or side not in {"yes", "no"}:
        return None
    if not isinstance(reference_price, float) or not _is_orderable_price(reference_price):
        return None
    if not isinstance(theoretical_edge, float):
        return None

    fair_probability = fair_yes if side == "yes" else fair_no
    if not isinstance(fair_probability, float):
        fair_probability = fair_yes if isinstance(fair_yes, float) else fair_no
    estimated_entry_cost = round(float(reference_price) * max(1, int(contracts)), 4)
    expected_value_dollars = round(float(theoretical_edge) * max(1, int(contracts)), 4)
    expected_roi_on_cost: float | str = ""
    if estimated_entry_cost > 0:
        expected_roi_on_cost = round(expected_value_dollars / estimated_entry_cost, 6)
    hours_to_close = _as_float(candidate.get("hours_to_close"))
    expected_value_per_day_dollars: float | str = ""
    expected_roi_per_day: float | str = ""
    if isinstance(hours_to_close, float) and hours_to_close > 0:
        days_to_close = hours_to_close / 24.0
        if days_to_close > 0:
            expected_value_per_day_dollars = round(expected_value_dollars / days_to_close, 6)
            if isinstance(expected_roi_on_cost, float):
                expected_roi_per_day = round(expected_roi_on_cost / days_to_close, 6)
    estimated_max_profit_dollars = round((1.0 - float(reference_price)) * max(1, int(contracts)), 4)
    max_profit_roi_on_cost: float | str = ""
    if estimated_entry_cost > 0:
        max_profit_roi_on_cost = round(estimated_max_profit_dollars / estimated_entry_cost, 6)
    confidence_value = _as_float(candidate.get("confidence"))
    contract_family = str(candidate.get("contract_family") or "").strip().lower()
    canonical_niche = "weather_climate"
    reference_source = str(candidate.get("theoretical_reference_source") or "").strip()
    strip_key = str(candidate.get("strip_key") or "").strip()
    opportunity_class = str(candidate.get("opportunity_class") or "").strip().lower()
    suggested_risk_dollars = _as_float(candidate.get("risk_dollars"))
    if suggested_risk_dollars is None:
        suggested_risk_dollars = _as_float(candidate.get("allocator_suggested_risk_dollars"))
    if suggested_risk_dollars is None:
        suggested_risk_dollars = estimated_entry_cost
    router_true_probability = fair_probability if isinstance(fair_probability, float) else None
    router_break_even_probability: float | str = ""
    if isinstance(reference_price, float):
        if side == "no":
            router_break_even_probability = round(max(0.0, min(1.0, 1.0 - reference_price)), 6)
        else:
            router_break_even_probability = round(max(0.0, min(1.0, reference_price)), 6)
    thesis = (
        str(candidate.get("thesis") or "").strip()
        or f"Climate router pilot candidate ({str(candidate.get('opportunity_class') or '').strip() or 'tradable'})."
    )

    return {
        "plan_rank": "",
        "category": "Climate Router Pilot",
        "market_ticker": ticker,
        "market_title": candidate.get("market_title"),
        "close_time": candidate.get("close_time"),
        "hours_to_close": hours_to_close if isinstance(hours_to_close, float) else "",
        "contract_family": contract_family,
        "weather_station_history_status": "",
        "weather_station_history_sample_years": "",
        "weather_station_history_min_sample_years_required": "",
        "weather_station_history_live_ready": "",
        "weather_station_history_live_ready_reason": "",
        "side": side,
        "canonical_ticker": str(candidate.get("canonical_ticker") or ticker),
        "canonical_niche": canonical_niche,
        "canonical_release_cluster": "",
        "canonical_policy_applied": True,
        "canonical_mapping_match_type": "climate_router_pilot",
        "canonical_mapping_match_key": str(candidate.get("strip_key") or ticker),
        "maker_entry_price_dollars": round(float(reference_price), 6),
        "maker_entry_edge": round(float(theoretical_edge), 6),
        "maker_entry_edge_net_fees": round(float(theoretical_edge), 6),
        "maker_entry_edge_net_total": round(float(theoretical_edge), 6),
        "maker_entry_edge_conservative": round(float(theoretical_edge), 6),
        "maker_entry_edge_conservative_net_fees": round(float(theoretical_edge), 6),
        "maker_entry_edge_conservative_net_total": round(float(theoretical_edge), 6),
        "effective_min_maker_edge": "",
        "effective_min_maker_edge_net_fees": "",
        "effective_min_entry_price_dollars": "",
        "effective_max_entry_price_dollars": "",
        "effective_max_spread_dollars": "",
        "effective_min_confidence": "",
        "effective_min_evidence_count": "",
        "effective_per_market_risk_cap_dollars": "",
        "effective_release_cluster_risk_cap_dollars": "",
        "effective_same_day_correlated_risk_cap_dollars": "",
        "incentive_bonus_per_contract_dollars": 0.0,
        "fair_probability": fair_probability if isinstance(fair_probability, float) else "",
        "fair_probability_conservative": fair_probability if isinstance(fair_probability, float) else "",
        "confidence": confidence_value if isinstance(confidence_value, float) else "",
        "contracts_per_order": max(1, int(contracts)),
        "estimated_entry_cost_dollars": estimated_entry_cost,
        "estimated_entry_fee_dollars": 0.0,
        "estimated_entry_fee_per_contract_dollars": 0.0,
        "expected_incentive_value_dollars": 0.0,
        "expected_value_dollars": expected_value_dollars,
        "expected_value_net_dollars": expected_value_dollars,
        "expected_value_conservative_dollars": expected_value_dollars,
        "expected_value_conservative_net_dollars": expected_value_dollars,
        "expected_roi_on_cost": expected_roi_on_cost,
        "expected_roi_on_cost_net": expected_roi_on_cost,
        "expected_roi_on_cost_conservative": expected_roi_on_cost,
        "expected_roi_on_cost_conservative_net": expected_roi_on_cost,
        "expected_value_per_day_dollars": expected_value_per_day_dollars,
        "expected_value_per_day_net_dollars": expected_value_per_day_dollars,
        "expected_value_per_day_conservative_dollars": expected_value_per_day_dollars,
        "expected_value_per_day_conservative_net_dollars": expected_value_per_day_dollars,
        "expected_roi_per_day": expected_roi_per_day,
        "expected_roi_per_day_net": expected_roi_per_day,
        "expected_roi_per_day_conservative": expected_roi_per_day,
        "expected_roi_per_day_conservative_net": expected_roi_per_day,
        "estimated_max_loss_dollars": estimated_entry_cost,
        "estimated_max_profit_dollars": estimated_max_profit_dollars,
        "max_profit_roi_on_cost": max_profit_roi_on_cost,
        "planning_bankroll_fraction": (
            round(estimated_entry_cost / planning_bankroll_dollars, 6)
            if planning_bankroll_dollars > 0
            else ""
        ),
        "thesis": thesis,
        "order_payload_preview": _build_order_payload_preview(
            ticker=ticker,
            count=max(1, int(contracts)),
            side=side,
            price_dollars=float(reference_price),
        ),
        "climate_router_pilot_candidate": True,
        "source_strategy": "climate_router_pilot",
        "router_opportunity_class": opportunity_class,
        "router_expected_value_dollars": expected_value_dollars,
        "router_reference_price": round(float(reference_price), 6),
        "router_reference_price_source": reference_source,
        "router_true_probability": router_true_probability if isinstance(router_true_probability, float) else "",
        "router_break_even_probability": router_break_even_probability,
        "router_suggested_risk_dollars": round(float(suggested_risk_dollars), 6),
        "router_shadow_rank": max(1, int(shadow_rank)),
        "router_family": contract_family,
        "router_strip_id": strip_key,
        "climate_router_opportunity_class": opportunity_class,
        "climate_router_availability_state": str(candidate.get("availability_state") or "").strip().lower(),
        "climate_router_reference_source": reference_source,
        "climate_router_required_ev_dollars": "",
    }


def _prepare_plan_with_climate_router_pilot(
    *,
    plan_summary: dict[str, Any],
    output_dir: str,
    planning_bankroll_dollars: float,
    max_orders: int,
    contracts_per_order: int,
    climate_router_pilot_enabled: bool,
    climate_router_summary_json: str | None,
    climate_router_pilot_max_orders_per_run: int,
    climate_router_pilot_contracts_cap: int,
    climate_router_pilot_required_ev_dollars: float,
    climate_router_pilot_allowed_classes: tuple[str, ...] | None,
    climate_router_pilot_allowed_families: tuple[str, ...] | None,
    climate_router_pilot_excluded_families: tuple[str, ...] | None,
    climate_router_pilot_policy_scope_override_enabled: bool,
    daily_weather_live_only_effective: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    telemetry: dict[str, Any] = {
        "climate_router_pilot_enabled": bool(climate_router_pilot_enabled),
        "climate_router_pilot_status": "disabled",
        "climate_router_pilot_reason": "disabled",
        "climate_router_pilot_summary_file": None,
        "climate_router_pilot_selection_mode": "missing",
        "climate_router_pilot_summary_status": None,
        "climate_router_pilot_allowed_classes": list(
            _normalize_climate_router_allowed_classes(climate_router_pilot_allowed_classes)
        ),
        "climate_router_pilot_allowed_families": list(
            _normalize_contract_family_filters(climate_router_pilot_allowed_families)
        ),
        "climate_router_pilot_excluded_families": list(
            _normalize_contract_family_filters(climate_router_pilot_excluded_families)
        ),
        "climate_router_pilot_max_orders_per_run": max(0, int(climate_router_pilot_max_orders_per_run)),
        "climate_router_pilot_contracts_cap": max(1, int(climate_router_pilot_contracts_cap)),
        "climate_router_pilot_required_ev_dollars": round(max(0.0, float(climate_router_pilot_required_ev_dollars)), 6),
        "climate_router_pilot_policy_scope_override_enabled": bool(
            climate_router_pilot_policy_scope_override_enabled
        ),
        "climate_router_pilot_policy_scope_override_attempts": 0,
        "climate_router_pilot_policy_scope_override_submissions": 0,
        "climate_router_pilot_policy_scope_override_blocked_reason_counts": {},
        "climate_router_pilot_considered_rows": 0,
        "climate_router_pilot_submitted_rows": 0,
        "climate_router_pilot_expected_value_dollars": 0.0,
        "climate_router_pilot_blocked_reason_counts": {},
        "climate_router_pilot_selected_tickers": [],
    }
    effective_summary = dict(plan_summary)
    existing_orders = [dict(row) for row in plan_summary.get("orders", []) if isinstance(row, dict)]
    effective_summary["orders"] = existing_orders
    if not bool(climate_router_pilot_enabled):
        return effective_summary, telemetry

    router_summary, summary_path, selection_mode = _load_climate_router_summary(
        output_dir=output_dir,
        explicit_path=climate_router_summary_json,
    )
    telemetry["climate_router_pilot_selection_mode"] = selection_mode
    telemetry["climate_router_pilot_summary_file"] = summary_path
    if not isinstance(router_summary, dict):
        telemetry["climate_router_pilot_status"] = "missing_router_summary"
        telemetry["climate_router_pilot_reason"] = "missing_router_summary"
        return effective_summary, telemetry

    telemetry["climate_router_pilot_summary_status"] = str(router_summary.get("status") or "").strip().lower() or None
    top_tradable = [row for row in router_summary.get("top_tradable_candidates", []) if isinstance(row, dict)]
    telemetry["climate_router_pilot_considered_rows"] = len(top_tradable)
    if not top_tradable:
        telemetry["climate_router_pilot_status"] = "no_router_tradable_rows"
        telemetry["climate_router_pilot_reason"] = "no_router_tradable_rows"
        return effective_summary, telemetry

    allowed_classes = set(_normalize_climate_router_allowed_classes(climate_router_pilot_allowed_classes))
    allowed_families = set(_normalize_contract_family_filters(climate_router_pilot_allowed_families))
    excluded_families = set(_normalize_contract_family_filters(climate_router_pilot_excluded_families))
    existing_tickers = {str(row.get("market_ticker") or "").strip().upper() for row in existing_orders}
    blocked_reason_counts: dict[str, int] = {}

    max_orders_total = max(1, int(max_orders))
    max_pilot_orders = max(0, int(climate_router_pilot_max_orders_per_run))
    if max_pilot_orders <= 0:
        telemetry["climate_router_pilot_status"] = "disabled_by_max_orders"
        telemetry["climate_router_pilot_reason"] = "disabled_by_max_orders"
        return effective_summary, telemetry
    available_slots = max(0, max_orders_total - len(existing_orders))
    if available_slots <= 0:
        telemetry["climate_router_pilot_status"] = "max_orders_already_allocated"
        telemetry["climate_router_pilot_reason"] = "max_orders_already_allocated"
        return effective_summary, telemetry

    effective_pilot_limit = min(max_pilot_orders, available_slots)
    contracts_cap = max(1, int(climate_router_pilot_contracts_cap))
    required_ev = max(0.0, float(climate_router_pilot_required_ev_dollars))
    selected_rows: list[dict[str, Any]] = []
    total_expected_value = 0.0
    policy_scope_override_attempts = 0
    policy_scope_override_submissions = 0
    policy_scope_override_blocked_reason_counts: dict[str, int] = {}

    for candidate_index, candidate in enumerate(top_tradable, start=1):
        if len(selected_rows) >= effective_pilot_limit:
            blocked_reason_counts["pilot_limit_reached"] = blocked_reason_counts.get("pilot_limit_reached", 0) + 1
            continue
        ticker = str(candidate.get("market_ticker") or "").strip().upper()
        if not ticker:
            blocked_reason_counts["missing_market_ticker"] = blocked_reason_counts.get("missing_market_ticker", 0) + 1
            continue
        if ticker in existing_tickers:
            blocked_reason_counts["duplicate_ticker_existing_plan"] = (
                blocked_reason_counts.get("duplicate_ticker_existing_plan", 0) + 1
            )
            continue
        opportunity_class = str(candidate.get("opportunity_class") or "").strip().lower()
        if opportunity_class and opportunity_class not in allowed_classes:
            blocked_reason_counts["opportunity_class_not_allowed"] = (
                blocked_reason_counts.get("opportunity_class_not_allowed", 0) + 1
            )
            continue
        contract_family = str(candidate.get("contract_family") or "").strip().lower()
        if allowed_families and contract_family not in allowed_families:
            blocked_reason_counts["contract_family_not_allowed"] = (
                blocked_reason_counts.get("contract_family_not_allowed", 0) + 1
            )
            continue
        if excluded_families and contract_family in excluded_families:
            blocked_reason_counts["contract_family_excluded"] = (
                blocked_reason_counts.get("contract_family_excluded", 0) + 1
            )
            continue
        policy_scope_override_used = False
        policy_scope_override_reason = ""
        if daily_weather_live_only_effective and contract_family not in _DAILY_WEATHER_CONTRACT_FAMILIES:
            policy_scope_override_attempts += 1
            if not bool(climate_router_pilot_policy_scope_override_enabled):
                blocked_reason_counts["daily_weather_only_mode"] = blocked_reason_counts.get("daily_weather_only_mode", 0) + 1
                policy_scope_override_blocked_reason_counts["override_disabled"] = (
                    policy_scope_override_blocked_reason_counts.get("override_disabled", 0) + 1
                )
                continue
            if max_pilot_orders > 1:
                blocked_reason_counts["policy_scope_override_requires_max_orders_per_run_le_1"] = (
                    blocked_reason_counts.get("policy_scope_override_requires_max_orders_per_run_le_1", 0) + 1
                )
                policy_scope_override_blocked_reason_counts["max_orders_per_run_gt_1"] = (
                    policy_scope_override_blocked_reason_counts.get("max_orders_per_run_gt_1", 0) + 1
                )
                continue
            if contracts_cap > 1:
                blocked_reason_counts["policy_scope_override_requires_contracts_cap_le_1"] = (
                    blocked_reason_counts.get("policy_scope_override_requires_contracts_cap_le_1", 0) + 1
                )
                policy_scope_override_blocked_reason_counts["contracts_cap_gt_1"] = (
                    policy_scope_override_blocked_reason_counts.get("contracts_cap_gt_1", 0) + 1
                )
                continue
            policy_scope_override_used = True
            policy_scope_override_reason = "daily_weather_live_only_override_for_climate_router_pilot"
        contracts = min(max(1, int(contracts_per_order)), contracts_cap)
        plan_row = _build_climate_router_pilot_plan_row(
            candidate=candidate,
            contracts=contracts,
            planning_bankroll_dollars=float(planning_bankroll_dollars),
            shadow_rank=candidate_index,
        )
        if not isinstance(plan_row, dict):
            blocked_reason_counts["candidate_shape_invalid"] = blocked_reason_counts.get("candidate_shape_invalid", 0) + 1
            continue
        expected_value_dollars = _as_float(plan_row.get("expected_value_dollars"))
        if not isinstance(expected_value_dollars, float):
            expected_value_dollars = 0.0
        if expected_value_dollars + 1e-9 < required_ev:
            blocked_reason_counts["expected_value_below_required"] = (
                blocked_reason_counts.get("expected_value_below_required", 0) + 1
            )
            continue
        plan_row["pilot_policy_scope_override_used"] = bool(policy_scope_override_used)
        plan_row["pilot_policy_scope_override_enabled"] = bool(climate_router_pilot_policy_scope_override_enabled)
        plan_row["pilot_policy_scope_override_applicable"] = bool(
            daily_weather_live_only_effective
            and contract_family not in _DAILY_WEATHER_CONTRACT_FAMILIES
        )
        plan_row["pilot_policy_scope_override_reason"] = (
            policy_scope_override_reason if policy_scope_override_used else ""
        )
        plan_row["pilot_policy_scope_override_family"] = (
            contract_family if policy_scope_override_used else ""
        )
        plan_row["pilot_policy_scope_override_ticker"] = (
            ticker if policy_scope_override_used else ""
        )
        plan_row["climate_router_required_ev_dollars"] = required_ev
        selected_rows.append(plan_row)
        existing_tickers.add(ticker)
        total_expected_value += expected_value_dollars
        if policy_scope_override_used:
            policy_scope_override_submissions += 1

    telemetry["climate_router_pilot_policy_scope_override_attempts"] = int(policy_scope_override_attempts)
    telemetry["climate_router_pilot_policy_scope_override_submissions"] = int(policy_scope_override_submissions)
    telemetry["climate_router_pilot_policy_scope_override_blocked_reason_counts"] = dict(
        sorted(policy_scope_override_blocked_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    telemetry["climate_router_pilot_submitted_rows"] = len(selected_rows)
    telemetry["climate_router_pilot_expected_value_dollars"] = round(total_expected_value, 6)
    telemetry["climate_router_pilot_blocked_reason_counts"] = dict(
        sorted(blocked_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    telemetry["climate_router_pilot_selected_tickers"] = [
        str(row.get("market_ticker") or "").strip() for row in selected_rows
    ]

    if not selected_rows:
        telemetry["climate_router_pilot_status"] = "no_router_pilot_candidates"
        telemetry["climate_router_pilot_reason"] = "no_router_pilot_candidates"
        return effective_summary, telemetry

    merged_orders = list(existing_orders) + selected_rows
    for plan_rank, row in enumerate(merged_orders, start=1):
        row["plan_rank"] = plan_rank
    effective_summary["orders"] = merged_orders
    effective_summary["planned_orders"] = len(merged_orders)
    if str(effective_summary.get("status") or "").strip().lower() in {"", "no_candidates"}:
        effective_summary["status"] = "ready"

    positive_orders = [
        row
        for row in merged_orders
        if isinstance(_as_float(row.get("maker_entry_edge")), float) and _as_float(row.get("maker_entry_edge")) > 0
    ]
    positive_policy_orders = [
        row
        for row in positive_orders
        if _as_bool(row.get("canonical_policy_applied")) is True or bool(row.get("canonical_policy_applied"))
    ]
    effective_summary["positive_maker_entry_markets"] = max(
        int(effective_summary.get("positive_maker_entry_markets") or 0),
        len(positive_orders),
    )
    effective_summary["positive_maker_entry_markets_with_canonical_policy"] = max(
        int(effective_summary.get("positive_maker_entry_markets_with_canonical_policy") or 0),
        len(positive_policy_orders),
    )

    if not existing_orders and merged_orders:
        top = merged_orders[0]
        effective_summary["top_market_ticker"] = top.get("market_ticker")
        effective_summary["top_market_title"] = top.get("market_title")
        effective_summary["top_market_close_time"] = top.get("close_time")
        effective_summary["top_market_hours_to_close"] = top.get("hours_to_close")
        effective_summary["top_market_side"] = top.get("side")
        effective_summary["top_market_canonical_ticker"] = top.get("canonical_ticker")
        effective_summary["top_market_canonical_niche"] = top.get("canonical_niche")
        effective_summary["top_market_canonical_policy_applied"] = top.get("canonical_policy_applied")
        effective_summary["top_market_contract_family"] = top.get("contract_family")
        effective_summary["top_market_maker_entry_price_dollars"] = top.get("maker_entry_price_dollars")
        effective_summary["top_market_maker_entry_edge"] = top.get("maker_entry_edge")
        effective_summary["top_market_maker_entry_edge_net_fees"] = top.get("maker_entry_edge_net_fees")
        effective_summary["top_market_estimated_entry_cost_dollars"] = top.get("estimated_entry_cost_dollars")
        effective_summary["top_market_estimated_entry_fee_dollars"] = top.get("estimated_entry_fee_dollars")
        effective_summary["top_market_expected_value_dollars"] = top.get("expected_value_dollars")
        effective_summary["top_market_expected_value_net_dollars"] = top.get("expected_value_net_dollars")
        effective_summary["top_market_expected_roi_on_cost"] = top.get("expected_roi_on_cost")
        effective_summary["top_market_expected_roi_on_cost_net"] = top.get("expected_roi_on_cost_net")
        effective_summary["top_market_expected_value_per_day_dollars"] = top.get("expected_value_per_day_dollars")
        effective_summary["top_market_expected_value_per_day_net_dollars"] = top.get(
            "expected_value_per_day_net_dollars"
        )
        effective_summary["top_market_expected_roi_per_day"] = top.get("expected_roi_per_day")
        effective_summary["top_market_expected_roi_per_day_net"] = top.get("expected_roi_per_day_net")
        effective_summary["top_market_estimated_max_profit_dollars"] = top.get("estimated_max_profit_dollars")
        effective_summary["top_market_estimated_max_loss_dollars"] = top.get("estimated_max_loss_dollars")
        effective_summary["top_market_max_profit_roi_on_cost"] = top.get("max_profit_roi_on_cost")
        effective_summary["top_market_fair_probability"] = top.get("fair_probability")
        effective_summary["top_market_fair_probability_conservative"] = top.get("fair_probability_conservative")
        effective_summary["top_market_confidence"] = top.get("confidence")
        effective_summary["top_market_thesis"] = top.get("thesis")

    telemetry["climate_router_pilot_status"] = "ready"
    telemetry["climate_router_pilot_reason"] = "router_candidates_promoted"
    return effective_summary, telemetry


def _latest_history_rows(history_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
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


def _daily_weather_board_summary(
    history_csv: str,
    *,
    now: datetime | None = None,
    max_capture_age_seconds: float | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    capture_age_limit_seconds = (
        max(0.0, float(max_capture_age_seconds))
        if isinstance(max_capture_age_seconds, (int, float))
        else None
    )
    path = Path(history_csv)
    if not path.exists():
        return {
            "status": "missing_history",
            "history_csv": str(path),
            "latest_markets": 0,
            "weather_markets_total": 0,
            "daily_weather_markets_total": 0,
            "weather_family_counts": {},
            "daily_weather_family_counts": {},
            "daily_weather_tickers": [],
            "contract_family_by_ticker": {},
            "latest_captured_at": None,
            "latest_capture_age_seconds": None,
            "max_capture_age_seconds": capture_age_limit_seconds,
            "capture_fresh": None,
        }

    history_rows = load_history_rows(path)
    latest_rows = _latest_history_rows(history_rows)
    weather_family_counts: dict[str, int] = {}
    daily_weather_family_counts: dict[str, int] = {}
    daily_weather_tickers: list[str] = []
    contract_family_by_ticker: dict[str, str] = {}
    latest_captured_at_dt: datetime | None = None
    latest_daily_weather_captured_at_dt: datetime | None = None

    for ticker, row in latest_rows.items():
        captured_value = _parse_timestamp(str(row.get("captured_at") or ""))
        if isinstance(captured_value, datetime):
            if latest_captured_at_dt is None or captured_value > latest_captured_at_dt:
                latest_captured_at_dt = captured_value
        settlement = build_weather_settlement_spec(row)
        contract_family = str(settlement.get("contract_family") or "").strip().lower()
        if contract_family:
            contract_family_by_ticker[ticker] = contract_family
        if contract_family in {"non_weather", "weather_other", ""}:
            continue
        weather_family_counts[contract_family] = weather_family_counts.get(contract_family, 0) + 1
        if contract_family in _DAILY_WEATHER_CONTRACT_FAMILIES:
            daily_weather_family_counts[contract_family] = daily_weather_family_counts.get(contract_family, 0) + 1
            daily_weather_tickers.append(ticker)
            if isinstance(captured_value, datetime):
                if latest_daily_weather_captured_at_dt is None or captured_value > latest_daily_weather_captured_at_dt:
                    latest_daily_weather_captured_at_dt = captured_value

    latest_overall_capture_age_seconds = (
        round(max(0.0, (captured_at - latest_captured_at_dt).total_seconds()), 3)
        if isinstance(latest_captured_at_dt, datetime)
        else None
    )
    latest_capture_age_seconds = (
        round(max(0.0, (captured_at - latest_daily_weather_captured_at_dt).total_seconds()), 3)
        if isinstance(latest_daily_weather_captured_at_dt, datetime)
        else None
    )
    capture_fresh = (
        latest_capture_age_seconds <= capture_age_limit_seconds
        if isinstance(latest_capture_age_seconds, float) and isinstance(capture_age_limit_seconds, float)
        else None
    )
    board_status = "ready"
    if sum(daily_weather_family_counts.values()) <= 0:
        board_status = "daily_weather_missing"
    elif latest_daily_weather_captured_at_dt is None:
        board_status = "missing_capture_timestamp"
    elif capture_fresh is False:
        board_status = "stale"

    return {
        "status": board_status,
        "history_csv": str(path),
        "latest_markets": len(latest_rows),
        "weather_markets_total": sum(weather_family_counts.values()),
        "daily_weather_markets_total": sum(daily_weather_family_counts.values()),
        "weather_family_counts": dict(sorted(weather_family_counts.items(), key=lambda item: (-item[1], item[0]))),
        "daily_weather_family_counts": dict(
            sorted(daily_weather_family_counts.items(), key=lambda item: (-item[1], item[0]))
        ),
        "daily_weather_tickers": sorted(daily_weather_tickers)[:25],
        "contract_family_by_ticker": contract_family_by_ticker,
        "latest_captured_at": (
            latest_daily_weather_captured_at_dt.isoformat()
            if isinstance(latest_daily_weather_captured_at_dt, datetime)
            else None
        ),
        "latest_capture_age_seconds": latest_capture_age_seconds,
        "max_capture_age_seconds": capture_age_limit_seconds,
        "capture_fresh": capture_fresh,
        "latest_overall_captured_at": latest_captured_at_dt.isoformat() if isinstance(latest_captured_at_dt, datetime) else None,
        "latest_overall_capture_age_seconds": latest_overall_capture_age_seconds,
    }


def build_prior_trade_gate_decision(
    *,
    plan_summary: dict[str, Any],
    ledger_summary: dict[str, Any],
    max_live_submissions_per_day: int,
    max_live_cost_per_day_dollars: float,
    open_positions_count: int,
    min_live_maker_edge: float,
    min_live_maker_edge_net_fees: float,
    min_live_selected_fair_probability: float | None = None,
    allowed_canonical_niches: tuple[str, ...] | None = None,
    top_market_contract_family: str | None = None,
    top_market_weather_history_status: str | None = None,
    top_market_weather_history_live_ready: bool | None = None,
    top_market_weather_history_live_ready_reason: str | None = None,
    weather_history_unhealthy_filtered: int = 0,
    enforce_weather_history_live_ready: bool = False,
    daily_weather_board_summary: dict[str, Any] | None = None,
    enforce_daily_weather_live_only: bool = False,
    climate_router_pilot_policy_scope_override_active: bool = False,
    require_daily_weather_board_coverage: bool = False,
    require_daily_weather_board_freshness: bool = False,
) -> dict[str, Any]:
    min_live_selected_fair_probability_effective = _as_float(min_live_selected_fair_probability)
    if isinstance(min_live_selected_fair_probability_effective, float) and (
        min_live_selected_fair_probability_effective < 0.0
        or min_live_selected_fair_probability_effective > 1.0
    ):
        raise ValueError("min_live_selected_fair_probability must be between 0 and 1")
    live_submissions_today = int(ledger_summary.get("live_submissions_today") or 0)
    live_submitted_cost_today = float(ledger_summary.get("live_submitted_cost_today") or 0.0)
    live_submissions_remaining_today = int(
        ledger_summary.get("live_submissions_remaining_today") or max(0, max_live_submissions_per_day - live_submissions_today)
    )
    live_submissions_to_date = int(ledger_summary.get("live_submissions_to_date") or live_submissions_today)
    live_submission_days_elapsed = int(ledger_summary.get("live_submission_days_elapsed") or 1)
    live_submission_budget_total = int(
        ledger_summary.get("live_submission_budget_total") or (live_submission_days_elapsed * max_live_submissions_per_day)
    )
    live_submission_budget_remaining = int(
        ledger_summary.get("live_submission_budget_remaining") or live_submissions_remaining_today
    )
    live_cost_budget_total = float(
        ledger_summary.get("live_cost_budget_total") or (live_submission_days_elapsed * max_live_cost_per_day_dollars)
    )
    live_cost_budget_remaining = float(
        ledger_summary.get("live_cost_budget_remaining") or max(0.0, live_cost_budget_total - live_submitted_cost_today)
    )
    live_cost_remaining_today = float(
        ledger_summary.get("live_cost_remaining_today") or max(0.0, max_live_cost_per_day_dollars - live_submitted_cost_today)
    )

    planned_orders = int(plan_summary.get("planned_orders") or 0)
    selection_lane = str(plan_summary.get("selection_lane") or "maker_edge").strip().lower() or "maker_edge"
    positive_maker_entry_markets_total = int(plan_summary.get("positive_maker_entry_markets") or 0)
    positive_maker_entry_markets = int(
        plan_summary.get("positive_maker_entry_markets_with_canonical_policy") or positive_maker_entry_markets_total
    )
    top_market_side = str(plan_summary.get("top_market_side") or "")
    top_market_canonical_ticker = plan_summary.get("top_market_canonical_ticker")
    top_market_canonical_niche = str(plan_summary.get("top_market_canonical_niche") or "").strip().lower()
    top_market_contract_family_normalized = str(top_market_contract_family or "").strip().lower()
    top_market_weather_history_status_normalized = str(top_market_weather_history_status or "").strip().lower()
    top_market_weather_history_live_ready_effective = (
        bool(top_market_weather_history_live_ready)
        if isinstance(top_market_weather_history_live_ready, bool)
        else None
    )
    top_market_weather_history_live_ready_reason_text = str(top_market_weather_history_live_ready_reason or "").strip()
    weather_history_unhealthy_filtered_count = max(0, int(weather_history_unhealthy_filtered or 0))
    top_market_canonical_policy_applied = plan_summary.get("top_market_canonical_policy_applied")
    top_market_maker_entry_edge = plan_summary.get("top_market_maker_entry_edge")
    top_market_maker_entry_edge_net_fees = plan_summary.get("top_market_maker_entry_edge_net_fees")
    top_market_kelly_used = _as_float(plan_summary.get("top_market_kelly_used"))
    top_market_kelly_reject_reason = str(plan_summary.get("top_market_kelly_reject_reason") or "").strip() or None
    top_market_fair_probability = _as_float(plan_summary.get("top_market_fair_probability"))
    if top_market_fair_probability is None:
        top_market_fair_probability = _as_float(plan_summary.get("top_market_fair_probability_conservative"))
    funding_gap_dollars = plan_summary.get("funding_gap_dollars")
    actual_live_balance_dollars = plan_summary.get("actual_live_balance_dollars")
    daily_weather_markets_total = int((daily_weather_board_summary or {}).get("daily_weather_markets_total") or 0)
    daily_weather_family_counts = dict((daily_weather_board_summary or {}).get("daily_weather_family_counts") or {})
    daily_weather_board_capture_fresh = _as_bool((daily_weather_board_summary or {}).get("capture_fresh"))
    daily_weather_board_latest_captured_at = str((daily_weather_board_summary or {}).get("latest_captured_at") or "").strip()
    daily_weather_board_latest_capture_age_seconds_raw = (daily_weather_board_summary or {}).get("latest_capture_age_seconds")
    daily_weather_board_latest_capture_age_seconds = (
        round(float(daily_weather_board_latest_capture_age_seconds_raw), 3)
        if isinstance(daily_weather_board_latest_capture_age_seconds_raw, (int, float))
        else None
    )
    daily_weather_board_max_capture_age_seconds_raw = (daily_weather_board_summary or {}).get("max_capture_age_seconds")
    daily_weather_board_max_capture_age_seconds = (
        round(float(daily_weather_board_max_capture_age_seconds_raw), 3)
        if isinstance(daily_weather_board_max_capture_age_seconds_raw, (int, float))
        else None
    )
    daily_weather_candidates_total = int(plan_summary.get("weather_history_daily_candidates_total") or 0)
    daily_weather_live_only_enforced_effective = bool(
        enforce_daily_weather_live_only and not climate_router_pilot_policy_scope_override_active
    )
    stale_daily_weather_board = (
        require_daily_weather_board_freshness
        and daily_weather_markets_total > 0
        and daily_weather_board_capture_fresh is not True
        and (
            daily_weather_live_only_enforced_effective
            or top_market_contract_family_normalized in _DAILY_WEATHER_CONTRACT_FAMILIES
            or daily_weather_candidates_total > 0
        )
    )

    blockers: list[str] = []
    if actual_live_balance_dollars is None:
        blockers.append("Live balance could not be verified.")
    elif actual_live_balance_dollars in (0, 0.0):
        blockers.append("Live balance is not funded.")
    elif isinstance(funding_gap_dollars, (int, float)) and funding_gap_dollars > 0:
        blockers.append("Planned prior-backed workflow still shows a funding gap.")
    if live_submission_budget_remaining <= 0:
        blockers.append("Accumulated live submission budget is exhausted.")
    if live_cost_budget_remaining <= 0:
        blockers.append("Accumulated live cost budget is exhausted.")
    if planned_orders <= 0:
        blockers.append("No prior-backed maker plans are available.")
    if positive_maker_entry_markets <= 0:
        blockers.append("No prior-backed maker edge is currently positive.")
    if planned_orders > 0:
        if top_market_side not in {"yes", "no"}:
            blockers.append("Top prior-backed plan does not specify a tradable side.")
        if selection_lane == "kelly_unified":
            if (
                not isinstance(top_market_maker_entry_edge_net_fees, (int, float))
                or top_market_maker_entry_edge_net_fees < min_live_maker_edge_net_fees
            ):
                blockers.append(
                    f"Top prior-backed maker edge net fees is below the live minimum of {min_live_maker_edge_net_fees:.3f}."
                )
            if top_market_kelly_reject_reason:
                blockers.append(
                    "Top prior-backed Kelly gate rejected candidate "
                    f"({top_market_kelly_reject_reason})."
                )
            if (
                not isinstance(top_market_kelly_used, float)
                or top_market_kelly_used < KELLY_UNIFIED_MIN_LIVE_USED_DEFAULT
            ):
                blockers.append(
                    "Top prior-backed Kelly used fraction is below the live minimum of "
                    f"{KELLY_UNIFIED_MIN_LIVE_USED_DEFAULT:.3f}."
                )
        else:
            if not isinstance(top_market_maker_entry_edge, (int, float)) or top_market_maker_entry_edge < min_live_maker_edge:
                blockers.append(f"Top prior-backed maker edge is below the live minimum of {min_live_maker_edge:.3f}.")
            if (
                not isinstance(top_market_maker_entry_edge_net_fees, (int, float))
                or top_market_maker_entry_edge_net_fees < min_live_maker_edge_net_fees
            ):
                blockers.append(
                    f"Top prior-backed maker edge net fees is below the live minimum of {min_live_maker_edge_net_fees:.3f}."
                )
        if (
            isinstance(min_live_selected_fair_probability_effective, float)
            and (
                not isinstance(top_market_fair_probability, float)
                or top_market_fair_probability < min_live_selected_fair_probability_effective
            )
        ):
            blockers.append(
                "Top prior-backed selected fair probability is below the live minimum of "
                f"{min_live_selected_fair_probability_effective:.3f}."
            )
    normalized_allowed_niches = (
        {value.strip().lower() for value in allowed_canonical_niches if value.strip()}
        if allowed_canonical_niches
        else set()
    )
    if normalized_allowed_niches and planned_orders > 0:
        if not top_market_canonical_policy_applied:
            blockers.append("Top prior-backed plan is missing canonical policy coverage.")
        elif top_market_canonical_niche not in normalized_allowed_niches:
            blockers.append("Top prior-backed plan is outside allowed live niches.")
    if require_daily_weather_board_coverage and daily_weather_markets_total <= 0:
        blockers.append(
            "No daily weather markets are present in the captured board snapshot; refusing live mode until board coverage is restored."
        )
    if stale_daily_weather_board:
        freshness_label = (
            f"age={daily_weather_board_latest_capture_age_seconds:.1f}s"
            if isinstance(daily_weather_board_latest_capture_age_seconds, float)
            else "age=unknown"
        )
        max_age_label = (
            f"max={daily_weather_board_max_capture_age_seconds:.1f}s"
            if isinstance(daily_weather_board_max_capture_age_seconds, float)
            else "max=unknown"
        )
        blockers.append(
            "Daily weather board snapshot is stale for live gating "
            f"({freshness_label}, {max_age_label})."
        )
    if (
        daily_weather_live_only_enforced_effective
        and planned_orders > 0
        and top_market_contract_family_normalized not in _DAILY_WEATHER_CONTRACT_FAMILIES
    ):
        blockers.append(
            "Daily-weather-only live mode is enabled, but the top prior-backed plan is not a daily weather contract."
        )
    if (
        enforce_weather_history_live_ready
        and planned_orders > 0
        and top_market_contract_family_normalized in _DAILY_WEATHER_CONTRACT_FAMILIES
        and top_market_weather_history_live_ready_effective is not True
    ):
        health_reason = (
            top_market_weather_history_live_ready_reason_text
            or (
                f"status_{top_market_weather_history_status_normalized}"
                if top_market_weather_history_status_normalized
                else "unknown"
            )
        )
        blockers.append(
            "Daily weather live mode requires fresh station-history readiness, "
            f"but the top plan is not live-ready ({health_reason})."
        )
    if (
        enforce_weather_history_live_ready
        and planned_orders <= 0
        and weather_history_unhealthy_filtered_count > 0
    ):
        blockers.append(
            "Daily weather live mode filtered all candidate plans due to unhealthy station-history readiness."
        )

    gate_pass = len(blockers) == 0
    gate_status = "pass" if gate_pass else "hold"
    if not gate_pass:
        if actual_live_balance_dollars is None:
            gate_status = "balance_unavailable"
        elif actual_live_balance_dollars in (0, 0.0) or (
            isinstance(funding_gap_dollars, (int, float)) and funding_gap_dollars > 0
        ):
            gate_status = "needs_funding"
        elif live_submission_budget_remaining <= 0 or live_cost_budget_remaining <= 0:
            gate_status = "cap_reached"
        elif require_daily_weather_board_coverage and daily_weather_markets_total <= 0:
            gate_status = "daily_weather_board_missing"
        elif stale_daily_weather_board:
            gate_status = "daily_weather_board_stale"
        elif (
            daily_weather_live_only_enforced_effective
            and planned_orders > 0
            and top_market_contract_family_normalized not in _DAILY_WEATHER_CONTRACT_FAMILIES
        ):
            gate_status = "daily_weather_only"
        elif (
            enforce_weather_history_live_ready
            and (
                (
                    planned_orders > 0
                    and top_market_contract_family_normalized in _DAILY_WEATHER_CONTRACT_FAMILIES
                    and top_market_weather_history_live_ready_effective is not True
                )
                or (planned_orders <= 0 and weather_history_unhealthy_filtered_count > 0)
            )
        ):
            gate_status = "weather_history_unhealthy"
        elif planned_orders <= 0:
            gate_status = "no_candidates"
        elif positive_maker_entry_markets <= 0:
            gate_status = "no_edge"
        elif (
            isinstance(min_live_selected_fair_probability_effective, float)
            and (
                not isinstance(top_market_fair_probability, float)
                or top_market_fair_probability < min_live_selected_fair_probability_effective
            )
        ):
            gate_status = "probability_too_low"
        elif selection_lane == "kelly_unified" and (
            top_market_kelly_reject_reason is not None
            or not isinstance(top_market_kelly_used, float)
            or top_market_kelly_used < KELLY_UNIFIED_MIN_LIVE_USED_DEFAULT
        ):
            gate_status = "kelly_too_small"
        else:
            gate_status = "edge_too_small"

    gate_score = round(
        min(
            100.0,
            positive_maker_entry_markets * 30.0
            + min(float(top_market_maker_entry_edge or 0.0), 0.05) * 1000.0
            + min(live_submission_budget_remaining, max_live_submissions_per_day * 2) * 3.0,
        ),
        2,
    )
    return {
        "gate_pass": gate_pass,
        "gate_status": gate_status,
        "gate_score": gate_score,
        "gate_blockers": blockers,
        "open_positions_count": int(open_positions_count),
        "live_submissions_to_date": live_submissions_to_date,
        "live_submissions_remaining_today": live_submissions_remaining_today,
        "live_submission_days_elapsed": live_submission_days_elapsed,
        "live_submission_budget_total": live_submission_budget_total,
        "live_submission_budget_remaining": live_submission_budget_remaining,
        "live_cost_budget_total": round(live_cost_budget_total, 4),
        "live_cost_budget_remaining": round(live_cost_budget_remaining, 4),
        "live_cost_remaining_today": round(live_cost_remaining_today, 4),
        "live_cost_remaining_dollars": round(live_cost_budget_remaining, 4),
        "planned_orders": planned_orders,
        "selection_lane": selection_lane,
        "positive_maker_entry_markets_total": positive_maker_entry_markets_total,
        "positive_maker_entry_markets": positive_maker_entry_markets,
        "top_market_ticker": plan_summary.get("top_market_ticker"),
        "top_market_title": plan_summary.get("top_market_title"),
        "top_market_close_time": plan_summary.get("top_market_close_time"),
        "top_market_hours_to_close": plan_summary.get("top_market_hours_to_close"),
        "top_market_side": top_market_side or None,
        "top_market_canonical_ticker": top_market_canonical_ticker,
        "top_market_canonical_niche": top_market_canonical_niche or None,
        "top_market_contract_family": top_market_contract_family_normalized or None,
        "top_market_canonical_policy_applied": top_market_canonical_policy_applied,
        "allowed_canonical_niches": sorted(normalized_allowed_niches) if normalized_allowed_niches else None,
        "daily_weather_live_only_enforced": bool(daily_weather_live_only_enforced_effective),
        "daily_weather_live_only_requested": bool(enforce_daily_weather_live_only),
        "climate_router_pilot_policy_scope_override_active": bool(
            climate_router_pilot_policy_scope_override_active
        ),
        "weather_history_live_gate_enforced": bool(enforce_weather_history_live_ready),
        "weather_history_unhealthy_filtered": weather_history_unhealthy_filtered_count,
        "daily_weather_board_coverage_required": bool(require_daily_weather_board_coverage),
        "daily_weather_board_freshness_required": bool(require_daily_weather_board_freshness),
        "daily_weather_markets_total": daily_weather_markets_total,
        "daily_weather_family_counts": daily_weather_family_counts,
        "daily_weather_board_latest_captured_at": daily_weather_board_latest_captured_at or None,
        "daily_weather_board_latest_capture_age_seconds": daily_weather_board_latest_capture_age_seconds,
        "daily_weather_board_max_capture_age_seconds": daily_weather_board_max_capture_age_seconds,
        "daily_weather_board_capture_fresh": daily_weather_board_capture_fresh,
        "top_market_weather_history_status": top_market_weather_history_status_normalized or None,
        "top_market_weather_history_live_ready": top_market_weather_history_live_ready_effective,
        "top_market_weather_history_live_ready_reason": top_market_weather_history_live_ready_reason_text or None,
        "top_market_maker_entry_price_dollars": plan_summary.get("top_market_maker_entry_price_dollars"),
        "top_market_maker_entry_edge": top_market_maker_entry_edge,
        "top_market_maker_entry_edge_net_fees": top_market_maker_entry_edge_net_fees,
        "top_market_kelly_used": top_market_kelly_used,
        "top_market_kelly_reject_reason": top_market_kelly_reject_reason,
        "min_live_selected_fair_probability": min_live_selected_fair_probability_effective,
        "top_market_estimated_entry_cost_dollars": plan_summary.get("top_market_estimated_entry_cost_dollars"),
        "top_market_estimated_entry_fee_dollars": plan_summary.get("top_market_estimated_entry_fee_dollars"),
        "top_market_expected_value_dollars": plan_summary.get("top_market_expected_value_dollars"),
        "top_market_expected_value_net_dollars": plan_summary.get("top_market_expected_value_net_dollars"),
        "top_market_expected_roi_on_cost": plan_summary.get("top_market_expected_roi_on_cost"),
        "top_market_expected_roi_on_cost_net": plan_summary.get("top_market_expected_roi_on_cost_net"),
        "top_market_expected_value_per_day_dollars": plan_summary.get("top_market_expected_value_per_day_dollars"),
        "top_market_expected_value_per_day_net_dollars": plan_summary.get("top_market_expected_value_per_day_net_dollars"),
        "top_market_expected_roi_per_day": plan_summary.get("top_market_expected_roi_per_day"),
        "top_market_expected_roi_per_day_net": plan_summary.get("top_market_expected_roi_per_day_net"),
        "top_market_estimated_max_profit_dollars": plan_summary.get("top_market_estimated_max_profit_dollars"),
        "top_market_estimated_max_loss_dollars": plan_summary.get("top_market_estimated_max_loss_dollars"),
        "top_market_max_profit_roi_on_cost": plan_summary.get("top_market_max_profit_roi_on_cost"),
        "top_market_fair_probability": top_market_fair_probability,
        "top_market_fair_probability_conservative": plan_summary.get("top_market_fair_probability_conservative"),
        "top_market_confidence": plan_summary.get("top_market_confidence"),
        "top_market_thesis": plan_summary.get("top_market_thesis"),
    }


def run_kalshi_micro_prior_execute(
    *,
    env_file: str,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    contracts_per_order: int = 1,
    max_orders: int = 3,
    min_maker_edge: float = 0.005,
    min_maker_edge_net_fees: float = 0.0,
    selection_lane: str = "maker_edge",
    min_selected_fair_probability: float | None = None,
    max_entry_price_dollars: float = 0.99,
    min_live_entry_price_dollars: float = 0.03,
    max_live_entry_price_dollars: float = 0.95,
    routine_live_max_hours_to_close: float = 48.0,
    max_live_hours_to_close_by_niche: dict[str, float] | None = None,
    routine_live_longdated_allowed_niches: tuple[str, ...] = (),
    canonical_mapping_csv: str | None = "data/research/canonical_contract_mapping.csv",
    canonical_threshold_csv: str | None = "data/research/canonical_threshold_library.csv",
    prefer_canonical_thresholds: bool = True,
    require_canonical_mapping_for_live: bool = True,
    enforce_canonical_dataset: bool = True,
    timeout_seconds: float = 15.0,
    allow_live_orders: bool = False,
    cancel_resting_immediately: bool = False,
    resting_hold_seconds: float = 0.0,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    auto_cancel_duplicate_open_orders: bool = True,
    min_live_maker_edge: float = 0.01,
    min_live_maker_edge_net_fees: float = 0.0,
    min_live_selected_fair_probability: float | None = None,
    include_incentives: bool = False,
    ledger_csv: str | None = None,
    book_db_path: str | None = None,
    execution_event_log_csv: str | None = None,
    execution_journal_db_path: str | None = None,
    execution_frontier_recent_rows: int = 5000,
    execution_frontier_report_json: str | None = None,
    execution_frontier_max_report_age_seconds: float | None = 10800.0,
    execution_empirical_fill_model_enabled: bool = True,
    execution_empirical_fill_model_lookback_days: float = 21.0,
    execution_empirical_fill_model_recent_events: int = 20000,
    execution_empirical_fill_model_min_effective_samples: float = 6.0,
    execution_empirical_fill_model_prefer_empirical: bool = True,
    enable_untrusted_bucket_probe_exploration: bool = True,
    untrusted_bucket_probe_max_orders_per_run: int = 1,
    untrusted_bucket_probe_required_edge_buffer_dollars: float = 0.01,
    untrusted_bucket_probe_contracts_cap: int = 1,
    enforce_ws_state_authority: bool = True,
    ws_state_json: str | None = None,
    ws_state_max_age_seconds: float = 30.0,
    enforce_daily_weather_live_only: bool = False,
    require_daily_weather_board_coverage_for_live: bool = False,
    daily_weather_board_max_age_seconds: float = 900.0,
    climate_router_pilot_enabled: bool = False,
    climate_router_summary_json: str | None = None,
    climate_router_pilot_max_orders_per_run: int = 1,
    climate_router_pilot_contracts_cap: int = 1,
    climate_router_pilot_required_ev_dollars: float = 0.01,
    climate_router_pilot_allowed_classes: tuple[str, ...] = ("tradable",),
    climate_router_pilot_allowed_families: tuple[str, ...] = (),
    climate_router_pilot_excluded_families: tuple[str, ...] = (),
    climate_router_pilot_policy_scope_override_enabled: bool = False,
    http_request_json: AuthenticatedRequester = _http_request_json,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
    sleep_fn: TimeSleeper | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    run_id = f"kalshi_micro_prior_execute::{captured_at.strftime('%Y%m%d_%H%M%S_%f')[:-3]}"
    env_data = _parse_env_file(Path(env_file))
    timezone_name = str(env_data.get("BETBOT_TIMEZONE") or "America/New_York").strip()
    trading_day = trading_day_for_timestamp(captured_at, timezone_name)
    ledger_path = Path(ledger_csv) if ledger_csv else default_ledger_path(output_dir)
    effective_book_db_path = Path(book_db_path) if book_db_path else default_book_db_path(output_dir)
    enforce_canonical_dataset_effective = bool(enforce_canonical_dataset)
    daily_weather_live_only_effective = bool(allow_live_orders and enforce_daily_weather_live_only)
    require_daily_weather_board_coverage_effective = bool(
        allow_live_orders and require_daily_weather_board_coverage_for_live
    )
    if daily_weather_live_only_effective:
        allowed_live_canonical_niches = ("weather_climate",)
    else:
        allowed_live_canonical_niches = (
            LIVE_ALLOWED_CANONICAL_NICHES if (allow_live_orders or enforce_canonical_dataset_effective) else None
        )
    effective_min_live_selected_fair_probability = _as_float(min_live_selected_fair_probability)
    if effective_min_live_selected_fair_probability is None:
        effective_min_live_selected_fair_probability = _as_float(min_selected_fair_probability)
    if isinstance(effective_min_live_selected_fair_probability, float) and (
        effective_min_live_selected_fair_probability < 0.0
        or effective_min_live_selected_fair_probability > 1.0
    ):
        raise ValueError("min_live_selected_fair_probability must be between 0 and 1")
    enforce_live_quality_filters = bool(allow_live_orders or enforce_canonical_dataset_effective)
    effective_min_entry_price_dollars = (
        max(0.0, float(min_live_entry_price_dollars)) if enforce_live_quality_filters else 0.0
    )
    effective_max_entry_price_dollars = (
        min(float(max_entry_price_dollars), float(max_live_entry_price_dollars))
        if enforce_live_quality_filters
        else float(max_entry_price_dollars)
    )
    effective_routine_max_hours_to_close = (
        float(routine_live_max_hours_to_close)
        if enforce_live_quality_filters and float(routine_live_max_hours_to_close) > 0
        else None
    )
    default_max_hours_by_niche = {
        "macro_release": 168.0,
        "weather_energy_transmission": 72.0,
        "weather_climate": 36.0,
    }
    effective_max_hours_to_close_by_niche = dict(default_max_hours_by_niche)
    if isinstance(max_live_hours_to_close_by_niche, dict):
        for key, value in max_live_hours_to_close_by_niche.items():
            if not isinstance(key, str):
                continue
            if not isinstance(value, (int, float)):
                continue
            effective_max_hours_to_close_by_niche[key.strip().lower()] = float(value)
    if not enforce_live_quality_filters:
        effective_max_hours_to_close_by_niche = {}
    effective_routine_longdated_allowed_niches = (
        tuple(routine_live_longdated_allowed_niches) if enforce_live_quality_filters else ()
    )
    prefer_canonical_thresholds_effective = (
        True if (allow_live_orders or enforce_canonical_dataset_effective) else prefer_canonical_thresholds
    )
    require_canonical_mapping_for_live_effective = (
        True if (allow_live_orders or enforce_canonical_dataset_effective) else False
    )
    weather_board_summary_raw = _daily_weather_board_summary(
        history_csv,
        now=captured_at,
        max_capture_age_seconds=(
            daily_weather_board_max_age_seconds
            if enforce_live_quality_filters
            else None
        ),
    )

    plan_summary = run_kalshi_micro_prior_plan(
        env_file=env_file,
        priors_csv=priors_csv,
        history_csv=history_csv,
        output_dir=output_dir,
        planning_bankroll_dollars=planning_bankroll_dollars,
        daily_risk_cap_dollars=daily_risk_cap_dollars,
        contracts_per_order=contracts_per_order,
        max_orders=max_orders,
        min_maker_edge=min_maker_edge,
        min_maker_edge_net_fees=min_maker_edge_net_fees,
        selection_lane=selection_lane,
        min_selected_fair_probability=min_selected_fair_probability,
        min_entry_price_dollars=effective_min_entry_price_dollars,
        max_entry_price_dollars=effective_max_entry_price_dollars,
        routine_max_hours_to_close=effective_routine_max_hours_to_close,
        max_hours_to_close_by_canonical_niche=effective_max_hours_to_close_by_niche,
        routine_longdated_allowed_niches=effective_routine_longdated_allowed_niches,
        canonical_mapping_csv=canonical_mapping_csv,
        canonical_threshold_csv=canonical_threshold_csv,
        prefer_canonical_thresholds=prefer_canonical_thresholds_effective,
        require_canonical_mapping=require_canonical_mapping_for_live_effective,
        allowed_canonical_niches=allowed_live_canonical_niches,
        book_db_path=book_db_path,
        include_incentives=include_incentives,
        require_weather_history_live_ready_for_daily_weather=enforce_live_quality_filters,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        http_get_json=http_get_json,
        sign_request=sign_request,
        now=captured_at,
    )
    plan_summary, climate_router_pilot_summary = _prepare_plan_with_climate_router_pilot(
        plan_summary=plan_summary,
        output_dir=output_dir,
        planning_bankroll_dollars=float(planning_bankroll_dollars),
        max_orders=max_orders,
        contracts_per_order=contracts_per_order,
        climate_router_pilot_enabled=bool(climate_router_pilot_enabled),
        climate_router_summary_json=climate_router_summary_json,
        climate_router_pilot_max_orders_per_run=max(0, int(climate_router_pilot_max_orders_per_run)),
        climate_router_pilot_contracts_cap=max(1, int(climate_router_pilot_contracts_cap)),
        climate_router_pilot_required_ev_dollars=max(0.0, float(climate_router_pilot_required_ev_dollars)),
        climate_router_pilot_allowed_classes=tuple(climate_router_pilot_allowed_classes or ()),
        climate_router_pilot_allowed_families=tuple(climate_router_pilot_allowed_families or ()),
        climate_router_pilot_excluded_families=tuple(climate_router_pilot_excluded_families or ()),
        climate_router_pilot_policy_scope_override_enabled=bool(
            climate_router_pilot_policy_scope_override_enabled
        ),
        daily_weather_live_only_effective=daily_weather_live_only_effective,
    )
    climate_router_pilot_policy_scope_override_attempts = max(
        0,
        int(climate_router_pilot_summary.get("climate_router_pilot_policy_scope_override_attempts") or 0),
    )
    climate_router_pilot_policy_scope_override_submissions = max(
        0,
        int(climate_router_pilot_summary.get("climate_router_pilot_policy_scope_override_submissions") or 0),
    )
    climate_router_pilot_policy_scope_override_gate_active = bool(
        daily_weather_live_only_effective
        and bool(climate_router_pilot_policy_scope_override_enabled)
        and climate_router_pilot_policy_scope_override_submissions > 0
    )
    climate_router_pilot_policy_scope_override_applicable = bool(
        daily_weather_live_only_effective
        and climate_router_pilot_policy_scope_override_attempts > 0
    )
    top_market_ticker = str(plan_summary.get("top_market_ticker") or "").strip()
    contract_family_by_ticker = dict(weather_board_summary_raw.get("contract_family_by_ticker") or {})
    top_market_contract_family = str(contract_family_by_ticker.get(top_market_ticker) or "").strip().lower()
    if not top_market_contract_family:
        top_market_contract_family = str(plan_summary.get("top_market_contract_family") or "").strip().lower()
    top_market_weather_history_status = (
        str(plan_summary.get("top_market_weather_station_history_status") or "").strip().lower() or None
    )
    top_market_weather_history_live_ready = _as_bool(plan_summary.get("top_market_weather_station_history_live_ready"))
    top_market_weather_history_live_ready_reason = (
        str(plan_summary.get("top_market_weather_station_history_live_ready_reason") or "").strip() or None
    )
    weather_board_summary = {
        key: value
        for key, value in weather_board_summary_raw.items()
        if key != "contract_family_by_ticker"
    }
    ledger_summary_before = summarize_trade_ledger(
        path=ledger_path,
        timezone_name=timezone_name,
        trading_day=trading_day,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        book_db_path=effective_book_db_path,
    )
    open_positions_count = count_open_positions(book_db_path=effective_book_db_path)
    weather_history_live_gate_effective = bool(enforce_live_quality_filters)
    prior_trade_gate_summary = build_prior_trade_gate_decision(
        plan_summary=plan_summary,
        ledger_summary=ledger_summary_before,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        open_positions_count=open_positions_count,
        min_live_maker_edge=min_live_maker_edge,
        min_live_maker_edge_net_fees=min_live_maker_edge_net_fees,
        min_live_selected_fair_probability=effective_min_live_selected_fair_probability,
        allowed_canonical_niches=allowed_live_canonical_niches,
        top_market_contract_family=top_market_contract_family or None,
        top_market_weather_history_status=top_market_weather_history_status,
        top_market_weather_history_live_ready=top_market_weather_history_live_ready,
        top_market_weather_history_live_ready_reason=top_market_weather_history_live_ready_reason,
        weather_history_unhealthy_filtered=int(plan_summary.get("weather_history_unhealthy_filtered") or 0),
        enforce_weather_history_live_ready=weather_history_live_gate_effective,
        daily_weather_board_summary=weather_board_summary,
        enforce_daily_weather_live_only=daily_weather_live_only_effective,
        climate_router_pilot_policy_scope_override_active=climate_router_pilot_policy_scope_override_gate_active,
        require_daily_weather_board_coverage=require_daily_weather_board_coverage_effective,
        require_daily_weather_board_freshness=enforce_live_quality_filters,
    )

    def prior_plan_adapter(**kwargs: Any) -> dict[str, Any]:
        return dict(plan_summary)

    effective_allow_live_orders = allow_live_orders and bool(prior_trade_gate_summary.get("gate_pass"))
    if not bool(climate_router_pilot_policy_scope_override_enabled):
        climate_router_pilot_policy_scope_override_status = "inactive_disabled"
    elif climate_router_pilot_policy_scope_override_submissions > 0 and effective_allow_live_orders:
        climate_router_pilot_policy_scope_override_status = "active"
    elif climate_router_pilot_policy_scope_override_applicable and not effective_allow_live_orders:
        climate_router_pilot_policy_scope_override_status = "enabled_pending_live_mode"
    else:
        climate_router_pilot_policy_scope_override_status = "inactive_not_applicable"
    climate_router_pilot_policy_scope_override_active = (
        climate_router_pilot_policy_scope_override_status == "active"
    )

    execute_kwargs: dict[str, Any] = {
        "env_file": env_file,
        "output_dir": output_dir,
        "planning_bankroll_dollars": planning_bankroll_dollars,
        "daily_risk_cap_dollars": daily_risk_cap_dollars,
        "contracts_per_order": contracts_per_order,
        "max_orders": max_orders,
        "allow_live_orders": effective_allow_live_orders,
        "cancel_resting_immediately": cancel_resting_immediately,
        "resting_hold_seconds": resting_hold_seconds,
        "max_live_submissions_per_day": max_live_submissions_per_day,
        "max_live_cost_per_day_dollars": max_live_cost_per_day_dollars,
        "auto_cancel_duplicate_open_orders": auto_cancel_duplicate_open_orders,
        "ledger_csv": str(ledger_path),
        "book_db_path": str(effective_book_db_path),
        "execution_event_log_csv": execution_event_log_csv,
        "execution_journal_db_path": execution_journal_db_path,
        "execution_frontier_recent_rows": execution_frontier_recent_rows,
        "execution_frontier_report_json": execution_frontier_report_json,
        "execution_frontier_max_report_age_seconds": execution_frontier_max_report_age_seconds,
        "execution_empirical_fill_model_enabled": execution_empirical_fill_model_enabled,
        "execution_empirical_fill_model_lookback_days": execution_empirical_fill_model_lookback_days,
        "execution_empirical_fill_model_recent_events": execution_empirical_fill_model_recent_events,
        "execution_empirical_fill_model_min_effective_samples": execution_empirical_fill_model_min_effective_samples,
        "execution_empirical_fill_model_prefer_empirical": execution_empirical_fill_model_prefer_empirical,
        "enable_untrusted_bucket_probe_exploration": enable_untrusted_bucket_probe_exploration,
        "untrusted_bucket_probe_max_orders_per_run": untrusted_bucket_probe_max_orders_per_run,
        "untrusted_bucket_probe_required_edge_buffer_dollars": untrusted_bucket_probe_required_edge_buffer_dollars,
        "untrusted_bucket_probe_contracts_cap": untrusted_bucket_probe_contracts_cap,
        "enforce_ws_state_authority": enforce_ws_state_authority,
        "ws_state_json": ws_state_json,
        "ws_state_max_age_seconds": ws_state_max_age_seconds,
        "history_csv": history_csv,
        "timeout_seconds": timeout_seconds,
        "http_request_json": http_request_json,
        "http_get_json": http_get_json,
        "sign_request": sign_request,
        "plan_runner": prior_plan_adapter,
        "now": captured_at,
    }
    if sleep_fn is not None:
        execute_kwargs["sleep_fn"] = sleep_fn
    execute_summary = run_kalshi_micro_execute(**execute_kwargs)

    summary_status = execute_summary.get("status")
    if allow_live_orders and not prior_trade_gate_summary.get("gate_pass", False):
        summary_status = "blocked_prior_trade_gate"
    execute_attempts = execute_summary.get("attempts")
    fill_model_mode = infer_fill_model_mode(
        attempts=execute_attempts if isinstance(execute_attempts, list) else None,
        prefer_empirical_fill_model=execute_summary.get("execution_empirical_fill_model_prefer_empirical"),
        empirical_fill_enabled=execute_summary.get("execution_empirical_fill_model_enabled"),
    )
    top_market_fair_probability_raw = prior_trade_gate_summary.get("top_market_fair_probability")
    top_market_execution_probability_guarded = (
        prior_trade_gate_summary.get("top_market_fair_probability_conservative")
        if isinstance(prior_trade_gate_summary, dict)
        else None
    )
    if top_market_execution_probability_guarded in (None, ""):
        top_market_execution_probability_guarded = top_market_fair_probability_raw
    execution_frontier_reference_file = execute_summary.get("execution_frontier_break_even_reference_file")
    runtime_version = build_runtime_version_block(
        run_started_at=captured_at,
        run_id=run_id,
        git_cwd=Path.cwd(),
        fill_model_mode=fill_model_mode,
        prefer_empirical_fill_model=execute_summary.get("execution_empirical_fill_model_prefer_empirical"),
        frontier_artifact_path=execution_frontier_reference_file,
        frontier_selection_mode=execute_summary.get("execution_frontier_selection_mode"),
        as_of=captured_at,
    )

    summary = {
        "run_id": run_id,
        "captured_at": captured_at.isoformat(),
        "run_started_at_utc": captured_at.isoformat(),
        "env_file": env_file,
        "allow_live_orders_requested": allow_live_orders,
        "allow_live_orders_effective": effective_allow_live_orders,
        "selection_lane": str(selection_lane or "").strip().lower() or "maker_edge",
        "min_selected_fair_probability": _as_float(min_selected_fair_probability),
        "min_live_selected_fair_probability": effective_min_live_selected_fair_probability,
        "enforce_live_quality_filters": enforce_live_quality_filters,
        "effective_min_entry_price_dollars": effective_min_entry_price_dollars,
        "effective_max_entry_price_dollars": effective_max_entry_price_dollars,
        "effective_routine_max_hours_to_close": effective_routine_max_hours_to_close,
        "effective_max_hours_to_close_by_niche": (
            dict(effective_max_hours_to_close_by_niche) if effective_max_hours_to_close_by_niche else None
        ),
        "effective_routine_longdated_allowed_niches": (
            list(effective_routine_longdated_allowed_niches)
            if effective_routine_longdated_allowed_niches
            else None
        ),
        "canonical_mapping_csv": canonical_mapping_csv,
        "canonical_threshold_csv": canonical_threshold_csv,
        "prefer_canonical_thresholds": prefer_canonical_thresholds,
        "prefer_canonical_thresholds_effective": prefer_canonical_thresholds_effective,
        "require_canonical_mapping_for_live": require_canonical_mapping_for_live,
        "require_canonical_mapping_for_live_effective": require_canonical_mapping_for_live_effective,
        "enforce_canonical_dataset": enforce_canonical_dataset,
        "enforce_canonical_dataset_effective": enforce_canonical_dataset_effective,
        "enforce_daily_weather_live_only": bool(enforce_daily_weather_live_only),
        "daily_weather_live_only_effective": daily_weather_live_only_effective,
        "require_daily_weather_board_coverage_for_live": bool(require_daily_weather_board_coverage_for_live),
        "require_daily_weather_board_coverage_effective": require_daily_weather_board_coverage_effective,
        "daily_weather_board_max_age_seconds": max(0.0, float(daily_weather_board_max_age_seconds)),
        "climate_router_pilot_enabled": bool(climate_router_pilot_enabled),
        "climate_router_pilot_summary_json": climate_router_summary_json,
        "climate_router_pilot_policy_scope_override_enabled": bool(
            climate_router_pilot_policy_scope_override_enabled
        ),
        "climate_router_pilot_policy_scope_override_active": climate_router_pilot_policy_scope_override_active,
        "climate_router_pilot_policy_scope_override_status": climate_router_pilot_policy_scope_override_status,
        "climate_router_pilot_policy_scope_override_gate_active": (
            climate_router_pilot_policy_scope_override_gate_active
        ),
        "climate_router_pilot_policy_scope_override_applicable": (
            climate_router_pilot_policy_scope_override_applicable
        ),
        "climate_router_pilot_max_orders_per_run": max(0, int(climate_router_pilot_max_orders_per_run)),
        "climate_router_pilot_contracts_cap": max(1, int(climate_router_pilot_contracts_cap)),
        "climate_router_pilot_required_ev_dollars": round(max(0.0, float(climate_router_pilot_required_ev_dollars)), 6),
        "climate_router_pilot_allowed_classes": list(
            _normalize_climate_router_allowed_classes(tuple(climate_router_pilot_allowed_classes or ()))
        ),
        "climate_router_pilot_allowed_families": list(
            _normalize_contract_family_filters(tuple(climate_router_pilot_allowed_families or ()))
        ),
        "climate_router_pilot_excluded_families": list(
            _normalize_contract_family_filters(tuple(climate_router_pilot_excluded_families or ()))
        ),
        "climate_router_pilot_status": climate_router_pilot_summary.get("climate_router_pilot_status"),
        "climate_router_pilot_reason": climate_router_pilot_summary.get("climate_router_pilot_reason"),
        "climate_router_pilot_summary_file": climate_router_pilot_summary.get("climate_router_pilot_summary_file"),
        "climate_router_pilot_selection_mode": climate_router_pilot_summary.get("climate_router_pilot_selection_mode"),
        "climate_router_pilot_summary_status": climate_router_pilot_summary.get("climate_router_pilot_summary_status"),
        "climate_router_pilot_considered_rows": climate_router_pilot_summary.get("climate_router_pilot_considered_rows"),
        "climate_router_pilot_promoted_rows": climate_router_pilot_summary.get("climate_router_pilot_submitted_rows"),
        "climate_router_pilot_submitted_rows": climate_router_pilot_summary.get("climate_router_pilot_submitted_rows"),
        "climate_router_pilot_expected_value_dollars": climate_router_pilot_summary.get(
            "climate_router_pilot_expected_value_dollars"
        ),
        "climate_router_pilot_blocked_reason_counts": climate_router_pilot_summary.get(
            "climate_router_pilot_blocked_reason_counts"
        ),
        "climate_router_pilot_selected_tickers": climate_router_pilot_summary.get(
            "climate_router_pilot_selected_tickers"
        ),
        "climate_router_pilot_policy_scope_override_attempts": climate_router_pilot_policy_scope_override_attempts,
        "climate_router_pilot_policy_scope_override_submissions": climate_router_pilot_policy_scope_override_submissions,
        "climate_router_pilot_policy_scope_override_blocked_reason_counts": climate_router_pilot_summary.get(
            "climate_router_pilot_policy_scope_override_blocked_reason_counts"
        ),
        "climate_router_pilot_allowed_families_effective": climate_router_pilot_summary.get(
            "climate_router_pilot_allowed_families"
        ),
        "climate_router_pilot_excluded_families_effective": climate_router_pilot_summary.get(
            "climate_router_pilot_excluded_families"
        ),
        "climate_router_pilot_execute_considered_rows": execute_summary.get("climate_router_pilot_execute_considered_rows"),
        "climate_router_pilot_live_mode_enabled": execute_summary.get("climate_router_pilot_live_mode_enabled"),
        "climate_router_pilot_live_eligible_rows": execute_summary.get("climate_router_pilot_live_eligible_rows"),
        "climate_router_pilot_would_attempt_live_if_enabled": execute_summary.get(
            "climate_router_pilot_would_attempt_live_if_enabled"
        ),
        "climate_router_pilot_blocked_dry_run_only_rows": execute_summary.get(
            "climate_router_pilot_blocked_dry_run_only_rows"
        ),
        "climate_router_pilot_blocked_research_dry_run_only_reason_counts": execute_summary.get(
            "climate_router_pilot_blocked_research_dry_run_only_reason_counts"
        ),
        "climate_router_pilot_non_policy_gates_passed_rows": execute_summary.get(
            "climate_router_pilot_non_policy_gates_passed_rows"
        ),
        "climate_router_pilot_attempted_orders": execute_summary.get("climate_router_pilot_attempted_orders"),
        "climate_router_pilot_acked_orders": execute_summary.get("climate_router_pilot_acked_orders"),
        "climate_router_pilot_resting_orders": execute_summary.get("climate_router_pilot_resting_orders"),
        "climate_router_pilot_filled_orders": execute_summary.get("climate_router_pilot_filled_orders"),
        "climate_router_pilot_blocked_post_promotion_reason_counts": execute_summary.get(
            "climate_router_pilot_blocked_post_promotion_reason_counts"
        ),
        "climate_router_pilot_blocked_frontier_insufficient_data": execute_summary.get(
            "climate_router_pilot_blocked_frontier_insufficient_data"
        ),
        "climate_router_pilot_blocked_balance": execute_summary.get("climate_router_pilot_blocked_balance"),
        "climate_router_pilot_blocked_board_stale": execute_summary.get("climate_router_pilot_blocked_board_stale"),
        "climate_router_pilot_blocked_weather_history": execute_summary.get(
            "climate_router_pilot_blocked_weather_history"
        ),
        "climate_router_pilot_blocked_duplicate_ticker": execute_summary.get(
            "climate_router_pilot_blocked_duplicate_ticker"
        ),
        "climate_router_pilot_blocked_no_orderable_side_on_recheck": execute_summary.get(
            "climate_router_pilot_blocked_no_orderable_side_on_recheck"
        ),
        "climate_router_pilot_blocked_ev_below_threshold": execute_summary.get(
            "climate_router_pilot_blocked_ev_below_threshold"
        ),
        "climate_router_pilot_blocked_research_dry_run_only": execute_summary.get(
            "climate_router_pilot_blocked_research_dry_run_only"
        ),
        "climate_router_pilot_blocked_live_disabled": execute_summary.get(
            "climate_router_pilot_blocked_live_disabled"
        ),
        "climate_router_pilot_blocked_policy_scope": execute_summary.get(
            "climate_router_pilot_blocked_policy_scope"
        ),
        "climate_router_pilot_blocked_family_filter": execute_summary.get(
            "climate_router_pilot_blocked_family_filter"
        ),
        "climate_router_pilot_blocked_contract_cap": execute_summary.get(
            "climate_router_pilot_blocked_contract_cap"
        ),
        "climate_router_pilot_frontier_bootstrap_submitted_attempts": execute_summary.get(
            "climate_router_pilot_frontier_bootstrap_submitted_attempts"
        ),
        "climate_router_pilot_frontier_bootstrap_blocked_attempts": execute_summary.get(
            "climate_router_pilot_frontier_bootstrap_blocked_attempts"
        ),
        "climate_router_pilot_policy_scope_override_status_execute": execute_summary.get(
            "climate_router_pilot_policy_scope_override_status"
        ),
        "allowed_live_canonical_niches": list(allowed_live_canonical_niches) if allowed_live_canonical_niches else None,
        "weather_board_summary": weather_board_summary,
        "history_csv_path": str(history_csv),
        "history_csv_mtime_utc": file_mtime_utc(history_csv),
        "daily_weather_board_age_seconds": weather_board_summary.get("latest_capture_age_seconds"),
        "weather_station_history_cache_age_seconds": plan_summary.get("top_market_weather_station_history_cache_age_seconds"),
        "balance_heartbeat_age_seconds": execute_summary.get("balance_cache_age_seconds"),
        "fill_model_mode": fill_model_mode,
        "weather_history_live_gate_effective": weather_history_live_gate_effective,
        "plan_weather_history_live_filter_enabled": plan_summary.get("weather_history_live_filter_enabled"),
        "plan_weather_history_daily_candidates_total": plan_summary.get("weather_history_daily_candidates_total"),
        "plan_weather_history_daily_candidates_live_ready": plan_summary.get(
            "weather_history_daily_candidates_live_ready"
        ),
        "plan_weather_history_daily_candidates_unhealthy": plan_summary.get(
            "weather_history_daily_candidates_unhealthy"
        ),
        "plan_weather_history_unhealthy_filtered": plan_summary.get("weather_history_unhealthy_filtered"),
        "plan_weather_history_next_live_ready_candidate_ticker": plan_summary.get(
            "weather_history_next_live_ready_candidate_ticker"
        ),
        "plan_weather_history_next_live_ready_candidate_edge_net_fees": plan_summary.get(
            "weather_history_next_live_ready_candidate_edge_net_fees"
        ),
        "plan_canonical_policy_enabled": plan_summary.get("canonical_policy_enabled"),
        "plan_canonical_policy_reason": plan_summary.get("canonical_policy_reason"),
        "plan_matched_live_markets_with_canonical_policy": plan_summary.get(
            "matched_live_markets_with_canonical_policy"
        ),
        "plan_top_market_weather_station_history_status": top_market_weather_history_status,
        "plan_top_market_weather_station_history_live_ready": top_market_weather_history_live_ready,
        "plan_top_market_weather_station_history_live_ready_reason": top_market_weather_history_live_ready_reason,
        "plan_top_market_canonical_ticker": plan_summary.get("top_market_canonical_ticker"),
        "plan_top_market_canonical_policy_applied": plan_summary.get("top_market_canonical_policy_applied"),
        "include_incentives": include_incentives,
        "auto_cancel_duplicate_open_orders": auto_cancel_duplicate_open_orders,
        "prior_trade_gate_summary": prior_trade_gate_summary,
        "plan_summary_file": plan_summary.get("output_file"),
        "plan_output_csv": plan_summary.get("output_csv"),
        "execute_summary_file": execute_summary.get("output_file"),
        "execute_output_csv": execute_summary.get("output_csv"),
        "planned_orders": execute_summary.get("planned_orders"),
        "total_planned_cost_dollars": execute_summary.get("total_planned_cost_dollars"),
        "live_execution_lock_path": execute_summary.get("live_execution_lock_path"),
        "live_execution_lock_acquired": execute_summary.get("live_execution_lock_acquired"),
        "live_execution_lock_error": execute_summary.get("live_execution_lock_error"),
        "actual_live_balance_dollars": execute_summary.get("actual_live_balance_dollars"),
        "actual_live_balance_source": execute_summary.get("actual_live_balance_source"),
        "balance_live_verified": execute_summary.get("balance_live_verified"),
        "funding_gap_dollars": execute_summary.get("funding_gap_dollars"),
        "open_positions_count": open_positions_count,
        "live_submissions_to_date": prior_trade_gate_summary.get("live_submissions_to_date"),
        "live_submissions_remaining_today": prior_trade_gate_summary.get("live_submissions_remaining_today"),
        "live_submission_days_elapsed": prior_trade_gate_summary.get("live_submission_days_elapsed"),
        "live_submission_budget_total": prior_trade_gate_summary.get("live_submission_budget_total"),
        "live_submission_budget_remaining": prior_trade_gate_summary.get("live_submission_budget_remaining"),
        "top_market_ticker": prior_trade_gate_summary.get("top_market_ticker"),
        "top_market_title": prior_trade_gate_summary.get("top_market_title"),
        "top_market_close_time": prior_trade_gate_summary.get("top_market_close_time"),
        "top_market_hours_to_close": prior_trade_gate_summary.get("top_market_hours_to_close"),
        "top_market_side": prior_trade_gate_summary.get("top_market_side"),
        "top_market_canonical_ticker": prior_trade_gate_summary.get("top_market_canonical_ticker"),
        "top_market_canonical_niche": prior_trade_gate_summary.get("top_market_canonical_niche"),
        "top_market_canonical_policy_applied": prior_trade_gate_summary.get("top_market_canonical_policy_applied"),
        "top_market_maker_entry_price_dollars": prior_trade_gate_summary.get("top_market_maker_entry_price_dollars"),
        "top_market_maker_entry_edge": prior_trade_gate_summary.get("top_market_maker_entry_edge"),
        "top_market_maker_entry_edge_net_fees": prior_trade_gate_summary.get("top_market_maker_entry_edge_net_fees"),
        "top_market_estimated_entry_cost_dollars": prior_trade_gate_summary.get("top_market_estimated_entry_cost_dollars"),
        "top_market_estimated_entry_fee_dollars": prior_trade_gate_summary.get("top_market_estimated_entry_fee_dollars"),
        "top_market_expected_value_dollars": prior_trade_gate_summary.get("top_market_expected_value_dollars"),
        "top_market_expected_value_net_dollars": prior_trade_gate_summary.get("top_market_expected_value_net_dollars"),
        "top_market_expected_roi_on_cost": prior_trade_gate_summary.get("top_market_expected_roi_on_cost"),
        "top_market_expected_roi_on_cost_net": prior_trade_gate_summary.get("top_market_expected_roi_on_cost_net"),
        "top_market_expected_value_per_day_dollars": prior_trade_gate_summary.get("top_market_expected_value_per_day_dollars"),
        "top_market_expected_value_per_day_net_dollars": prior_trade_gate_summary.get("top_market_expected_value_per_day_net_dollars"),
        "top_market_expected_roi_per_day": prior_trade_gate_summary.get("top_market_expected_roi_per_day"),
        "top_market_expected_roi_per_day_net": prior_trade_gate_summary.get("top_market_expected_roi_per_day_net"),
        "top_market_estimated_max_profit_dollars": prior_trade_gate_summary.get("top_market_estimated_max_profit_dollars"),
        "top_market_estimated_max_loss_dollars": prior_trade_gate_summary.get("top_market_estimated_max_loss_dollars"),
        "top_market_max_profit_roi_on_cost": prior_trade_gate_summary.get("top_market_max_profit_roi_on_cost"),
        "top_market_fair_probability": prior_trade_gate_summary.get("top_market_fair_probability"),
        "top_market_fair_probability_raw": top_market_fair_probability_raw,
        "top_market_execution_probability_guarded": top_market_execution_probability_guarded,
        "top_market_confidence": prior_trade_gate_summary.get("top_market_confidence"),
        "top_market_thesis": prior_trade_gate_summary.get("top_market_thesis"),
        "fill_probability_source": (
            execute_summary.get("attempts")[0].get("execution_fill_probability_source")
            if isinstance(execute_summary.get("attempts"), list) and execute_summary.get("attempts")
            and isinstance(execute_summary.get("attempts")[0], dict)
            else None
        ),
        "empirical_fill_weight": (
            execute_summary.get("attempts")[0].get("execution_fill_probability_model_weight_empirical")
            if isinstance(execute_summary.get("attempts"), list) and execute_summary.get("attempts")
            and isinstance(execute_summary.get("attempts")[0], dict)
            else None
        ),
        "heuristic_fill_weight": (
            execute_summary.get("attempts")[0].get("execution_fill_probability_model_weight_heuristic")
            if isinstance(execute_summary.get("attempts"), list) and execute_summary.get("attempts")
            and isinstance(execute_summary.get("attempts")[0], dict)
            else None
        ),
        "probe_lane_used": (
            execute_summary.get("attempts")[0].get("probe_lane_used")
            if isinstance(execute_summary.get("attempts"), list) and execute_summary.get("attempts")
            and isinstance(execute_summary.get("attempts")[0], dict)
            else None
        ),
        "probe_reason": (
            execute_summary.get("attempts")[0].get("probe_reason")
            if isinstance(execute_summary.get("attempts"), list) and execute_summary.get("attempts")
            and isinstance(execute_summary.get("attempts")[0], dict)
            else None
        ),
        "status": summary_status,
        "blocked_duplicate_open_order_attempts": execute_summary.get("blocked_duplicate_open_order_attempts"),
        "blocked_submission_budget_attempts": execute_summary.get("blocked_submission_budget_attempts"),
        "blocked_live_cost_cap_attempts": execute_summary.get("blocked_live_cost_cap_attempts"),
        "janitor_attempts": execute_summary.get("janitor_attempts"),
        "janitor_canceled_open_orders_count": execute_summary.get("janitor_canceled_open_orders_count"),
        "janitor_cancel_failed_attempts": execute_summary.get("janitor_cancel_failed_attempts"),
        "blocked_execution_policy_attempts": execute_summary.get("blocked_execution_policy_attempts"),
        "execution_policy_active_attempts": execute_summary.get("execution_policy_active_attempts"),
        "execution_policy_submit_attempts": execute_summary.get("execution_policy_submit_attempts"),
        "execution_policy_skip_attempts": execute_summary.get("execution_policy_skip_attempts"),
        "execution_event_log_csv": execute_summary.get("execution_event_log_csv"),
        "execution_event_rows_written": execute_summary.get("execution_event_rows_written"),
        "execution_journal_db_path": execute_summary.get("execution_journal_db_path"),
        "execution_journal_run_id": execute_summary.get("execution_journal_run_id"),
        "execution_journal_rows_written": execute_summary.get("execution_journal_rows_written"),
        "enforce_ws_state_authority": execute_summary.get("enforce_ws_state_authority"),
        "ws_state_authority": execute_summary.get("ws_state_authority"),
        "execution_frontier_status": execute_summary.get("execution_frontier_status"),
        "execution_frontier_summary_file": execute_summary.get("execution_frontier_summary_file"),
        "execution_frontier_bucket_csv": execute_summary.get("execution_frontier_bucket_csv"),
        "execution_frontier_break_even_reference_file": execute_summary.get(
            "execution_frontier_break_even_reference_file"
        ),
        "frontier_artifact_path": runtime_version.get("frontier_artifact_path"),
        "frontier_artifact_sha256": runtime_version.get("frontier_artifact_sha256"),
        "frontier_artifact_file_sha256": runtime_version.get("frontier_artifact_file_sha256"),
        "frontier_artifact_payload_sha256": runtime_version.get("frontier_artifact_payload_sha256"),
        "frontier_artifact_as_of_utc": runtime_version.get("frontier_artifact_as_of_utc"),
        "frontier_artifact_age_seconds": runtime_version.get("frontier_artifact_age_seconds"),
        "execution_frontier_selection_mode": execute_summary.get("execution_frontier_selection_mode"),
        "execution_frontier_report_age_seconds": execute_summary.get("execution_frontier_report_age_seconds"),
        "execution_frontier_report_stale": execute_summary.get("execution_frontier_report_stale"),
        "execution_frontier_report_stale_reason": execute_summary.get(
            "execution_frontier_report_stale_reason"
        ),
        "execution_frontier_recommendations": execute_summary.get("execution_frontier_recommendations"),
        "execution_empirical_fill_model_enabled": execute_summary.get(
            "execution_empirical_fill_model_enabled"
        ),
        "execution_empirical_fill_model_lookback_days": execute_summary.get(
            "execution_empirical_fill_model_lookback_days"
        ),
        "execution_empirical_fill_model_recent_events": execute_summary.get(
            "execution_empirical_fill_model_recent_events"
        ),
        "execution_empirical_fill_model_min_effective_samples": execute_summary.get(
            "execution_empirical_fill_model_min_effective_samples"
        ),
        "execution_empirical_fill_model_prefer_empirical": execute_summary.get(
            "execution_empirical_fill_model_prefer_empirical"
        ),
        "execution_empirical_fill_training_rows": execute_summary.get(
            "execution_empirical_fill_training_rows"
        ),
        "untrusted_bucket_probe_exploration_enabled": execute_summary.get(
            "untrusted_bucket_probe_exploration_enabled"
        ),
        "untrusted_bucket_probe_submitted_attempts": execute_summary.get(
            "untrusted_bucket_probe_submitted_attempts"
        ),
        "untrusted_bucket_probe_blocked_attempts": execute_summary.get(
            "untrusted_bucket_probe_blocked_attempts"
        ),
        "untrusted_bucket_probe_reason_counts": execute_summary.get(
            "untrusted_bucket_probe_reason_counts"
        ),
        "orderbook_outage_short_circuit_triggered": execute_summary.get("orderbook_outage_short_circuit_triggered"),
        "orderbook_outage_short_circuit_trigger_market_ticker": execute_summary.get(
            "orderbook_outage_short_circuit_trigger_market_ticker"
        ),
        "orderbook_outage_short_circuit_skipped_orders": execute_summary.get(
            "orderbook_outage_short_circuit_skipped_orders"
        ),
        "duplicate_open_order_markets": execute_summary.get("duplicate_open_order_markets"),
        "attempts": execute_summary.get("attempts"),
        "runtime_version": runtime_version,
    }

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_micro_prior_execute_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
