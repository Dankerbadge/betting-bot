from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_book import count_open_positions, default_book_db_path
from betbot.kalshi_micro_execute import (
    AuthenticatedRequester,
    TimeSleeper,
    run_kalshi_micro_execute,
    _http_request_json,
)
from betbot.kalshi_micro_ledger import default_ledger_path, summarize_trade_ledger, trading_day_for_timestamp
from betbot.kalshi_micro_prior_plan import LIVE_ALLOWED_CANONICAL_NICHES, run_kalshi_micro_prior_plan
from betbot.kalshi_nonsports_quality import _parse_timestamp, load_history_rows
from betbot.kalshi_weather_settlement import build_weather_settlement_spec
from betbot.live_smoke import HttpGetter, KalshiSigner, _http_get_json, _kalshi_sign_request
from betbot.onboarding import _parse_env_file


_DAILY_WEATHER_CONTRACT_FAMILIES = {"daily_rain", "daily_temperature", "daily_snow"}


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
    allowed_canonical_niches: tuple[str, ...] | None = None,
    top_market_contract_family: str | None = None,
    top_market_weather_history_status: str | None = None,
    top_market_weather_history_live_ready: bool | None = None,
    top_market_weather_history_live_ready_reason: str | None = None,
    weather_history_unhealthy_filtered: int = 0,
    enforce_weather_history_live_ready: bool = False,
    daily_weather_board_summary: dict[str, Any] | None = None,
    enforce_daily_weather_live_only: bool = False,
    require_daily_weather_board_coverage: bool = False,
    require_daily_weather_board_freshness: bool = False,
) -> dict[str, Any]:
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
    stale_daily_weather_board = (
        require_daily_weather_board_freshness
        and daily_weather_markets_total > 0
        and daily_weather_board_capture_fresh is not True
        and (
            enforce_daily_weather_live_only
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
        if not isinstance(top_market_maker_entry_edge, (int, float)) or top_market_maker_entry_edge < min_live_maker_edge:
            blockers.append(f"Top prior-backed maker edge is below the live minimum of {min_live_maker_edge:.3f}.")
        if (
            not isinstance(top_market_maker_entry_edge_net_fees, (int, float))
            or top_market_maker_entry_edge_net_fees < min_live_maker_edge_net_fees
        ):
            blockers.append(
                f"Top prior-backed maker edge net fees is below the live minimum of {min_live_maker_edge_net_fees:.3f}."
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
        enforce_daily_weather_live_only
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
            enforce_daily_weather_live_only
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
        "daily_weather_live_only_enforced": bool(enforce_daily_weather_live_only),
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
        "top_market_fair_probability": plan_summary.get("top_market_fair_probability"),
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
    http_request_json: AuthenticatedRequester = _http_request_json,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
    sleep_fn: TimeSleeper | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
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
        allowed_canonical_niches=allowed_live_canonical_niches,
        top_market_contract_family=top_market_contract_family or None,
        top_market_weather_history_status=top_market_weather_history_status,
        top_market_weather_history_live_ready=top_market_weather_history_live_ready,
        top_market_weather_history_live_ready_reason=top_market_weather_history_live_ready_reason,
        weather_history_unhealthy_filtered=int(plan_summary.get("weather_history_unhealthy_filtered") or 0),
        enforce_weather_history_live_ready=weather_history_live_gate_effective,
        daily_weather_board_summary=weather_board_summary,
        enforce_daily_weather_live_only=daily_weather_live_only_effective,
        require_daily_weather_board_coverage=require_daily_weather_board_coverage_effective,
        require_daily_weather_board_freshness=enforce_live_quality_filters,
    )

    def prior_plan_adapter(**kwargs: Any) -> dict[str, Any]:
        return dict(plan_summary)

    effective_allow_live_orders = allow_live_orders and bool(prior_trade_gate_summary.get("gate_pass"))
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

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "allow_live_orders_requested": allow_live_orders,
        "allow_live_orders_effective": effective_allow_live_orders,
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
        "allowed_live_canonical_niches": list(allowed_live_canonical_niches) if allowed_live_canonical_niches else None,
        "weather_board_summary": weather_board_summary,
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
        "top_market_confidence": prior_trade_gate_summary.get("top_market_confidence"),
        "top_market_thesis": prior_trade_gate_summary.get("top_market_thesis"),
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
    }

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_micro_prior_execute_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
