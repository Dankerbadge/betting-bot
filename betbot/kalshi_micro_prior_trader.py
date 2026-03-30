from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from betbot.kalshi_micro_prior_execute import run_kalshi_micro_prior_execute
from betbot.kalshi_micro_prior_plan import LIVE_ALLOWED_CANONICAL_NICHES
from betbot.kalshi_micro_reconcile import run_kalshi_micro_reconcile
from betbot.kalshi_micro_watch_history import default_watch_history_path, summarize_watch_history
from betbot.kalshi_nonsports_auto_priors import run_kalshi_nonsports_auto_priors
from betbot.kalshi_nonsports_capture import run_kalshi_nonsports_capture
from betbot.kalshi_weather_priors import run_kalshi_weather_priors
from betbot.live_smoke import HttpGetter, KalshiSigner, _http_get_json, _kalshi_sign_request
from betbot.kalshi_micro_execute import _http_request_json
from betbot.temporary_live_env import temporary_live_env_file


CaptureRunner = Callable[..., dict[str, Any]]
PriorExecuteRunner = Callable[..., dict[str, Any]]
ReconcileRunner = Callable[..., dict[str, Any]]
AutoPriorRunner = Callable[..., dict[str, Any]]
WeatherPriorRunner = Callable[..., dict[str, Any]]

_FAILURE_ATTEMPT_RESULTS = {
    "orderbook_unavailable",
    "blocked_sports_guardrail",
    "blocked_by_safety_flag",
    "blocked_concurrent_live_execution",
    "blocked_submission_budget",
    "blocked_live_cost_cap",
    "rate_limited",
    "upstream_error",
    "blocked_trade_gate",
    "janitor_cancel_failed",
    "blocked_duplicate_open_order",
    "submit_failed",
    "submission_failed",
    "queue_position_unavailable",
    "cancel_failed",
}


def _build_attempt_failure_summary(summary: dict[str, Any]) -> dict[str, Any]:
    attempts = summary.get("prior_execute_attempts")
    if not isinstance(attempts, list):
        attempts = summary.get("attempts")

    failure_count = 0
    retryable_count = 0
    market_tickers: list[str] = []
    result_counts: dict[str, int] = {}
    http_status_counts: dict[str, int] = {}
    error_type_counts: dict[str, int] = {}

    if not isinstance(attempts, list):
        return {
            "prior_execute_failure_attempts_count": 0,
            "prior_execute_failure_retryable_attempts_count": 0,
            "prior_execute_failure_market_tickers": [],
            "prior_execute_failure_result_counts": {},
            "prior_execute_failure_http_status_counts": {},
            "prior_execute_failure_error_type_counts": {},
        }

    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        result = str(attempt.get("result") or "").strip().lower()
        if result not in _FAILURE_ATTEMPT_RESULTS:
            continue
        failure_count += 1
        result_counts[result] = result_counts.get(result, 0) + 1

        market_ticker = str(attempt.get("market_ticker") or "").strip()
        if market_ticker and market_ticker not in market_tickers:
            market_tickers.append(market_ticker)

        retryable = False
        for status_field_name, label, error_type_field_name in (
            ("orderbook_http_status", "orderbook", "orderbook_error_type"),
            ("submission_http_status", "submission", "submission_error_type"),
            ("queue_position_http_status", "queue_position", "queue_position_error_type"),
            ("cancel_http_status", "cancel", "cancel_error_type"),
        ):
            normalized_status = str(attempt.get(status_field_name) or "").strip()
            if not normalized_status:
                continue
            counts_key = f"{label}:{normalized_status}"
            http_status_counts[counts_key] = http_status_counts.get(counts_key, 0) + 1
            if normalized_status in {"429", "599"}:
                retryable = True
            error_type = str(attempt.get(error_type_field_name) or "").strip().lower()
            if error_type:
                type_counts_key = f"{label}:{error_type}"
                error_type_counts[type_counts_key] = error_type_counts.get(type_counts_key, 0) + 1
        if result in {"rate_limited", "upstream_error"}:
            retryable = True
        if retryable:
            retryable_count += 1

    return {
        "prior_execute_failure_attempts_count": failure_count,
        "prior_execute_failure_retryable_attempts_count": retryable_count,
        "prior_execute_failure_market_tickers": market_tickers,
        "prior_execute_failure_result_counts": result_counts,
        "prior_execute_failure_http_status_counts": http_status_counts,
        "prior_execute_failure_error_type_counts": error_type_counts,
    }


def _classify_capture_error(error: Any) -> str | None:
    text = str(error or "").strip().lower()
    if not text:
        return None
    if "nodename nor servname" in text or "name or service not known" in text or "temporary failure in name resolution" in text:
        return "dns_resolution_error"
    if "timed out" in text or "timeout" in text:
        return "timeout"
    if "status 429" in text or "rate limit" in text:
        return "rate_limited"
    if "payload did not contain events[]" in text:
        return "payload_shape_error"
    if "network_error" in text or "urlopen error" in text or "network" in text:
        return "network_error"
    if "status " in text:
        return "http_error"
    return "upstream_error"


def _build_capture_failure_reason(summary: dict[str, Any]) -> str:
    capture_status = str(summary.get("capture_status") or "error")
    capture_error = summary.get("capture_error")
    capture_error_kind = summary.get("capture_error_kind")
    retries_used = summary.get("capture_scan_search_retries_total")
    if not isinstance(retries_used, int):
        retries_used = 0

    reason = f"Fresh capture returned {capture_status}; prior execution was skipped."
    if capture_error_kind:
        reason = f"{reason} Capture error kind: {capture_error_kind}."
    if capture_error:
        capture_error_text = str(capture_error).strip()
        if capture_error_text and capture_error_text[-1] not in ".!?":
            capture_error_text = f"{capture_error_text}."
        reason = f"{reason} Capture error: {capture_error_text}"
    if retries_used > 0:
        reason = f"{reason} Scan retries used: {retries_used}."
    return reason


def _classify_prior_execute_error(summary: dict[str, Any]) -> str | None:
    status = str(summary.get("status") or "").strip().lower()
    if status in {
        "",
        "dry_run",
        "dry_run_orderbook_degraded",
        "live_submitted",
        "live_submitted_and_canceled",
        "blocked_prior_trade_gate",
    }:
        return None

    attempts = summary.get("attempts")
    if isinstance(attempts, list):
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            result = str(attempt.get("result") or "").strip().lower()
            orderbook_status = attempt.get("orderbook_http_status")
            submission_status = attempt.get("submission_http_status")
            queue_status = attempt.get("queue_position_http_status")
            cancel_status = attempt.get("cancel_http_status")
            orderbook_error_type = str(attempt.get("orderbook_error_type") or "").strip().lower()
            if result == "orderbook_unavailable":
                if orderbook_error_type == "config_error":
                    return "config_error"
                if orderbook_error_type == "signing_error":
                    return "signing_error"
                if orderbook_error_type == "url_error":
                    return _classify_capture_error(attempt.get("orderbook_error")) or "network_error"
                if orderbook_error_type == "payload_shape_error":
                    return "payload_shape_error"
                if str(orderbook_status).strip() == "599":
                    return "network_error"
                return "orderbook_unavailable"
            if result in {"submit_failed", "submission_failed", "queue_position_unavailable"}:
                normalized_status = str(submission_status or queue_status).strip()
                if normalized_status == "599":
                    return "network_error"
                if normalized_status == "429":
                    return "rate_limited"
                if normalized_status:
                    return "http_error"
                return result
            if result in {"cancel_failed", "janitor_cancel_failed"}:
                normalized_status = str(cancel_status).strip()
                if normalized_status == "599":
                    return "network_error"
                if normalized_status == "429":
                    return "rate_limited"
                if normalized_status:
                    return "http_error"
                return result

    return _classify_capture_error(summary.get("error") or status)


def _build_prior_execute_failure_reason(summary: dict[str, Any]) -> str:
    execute_status = str(summary.get("prior_execute_status") or summary.get("status") or "error")
    error_kind = summary.get("prior_execute_error_kind")
    reason = f"Prior execution finished with {execute_status}."
    if error_kind:
        reason = f"{reason} Failure kind: {error_kind}."

    attempts = summary.get("prior_execute_attempts")
    if not isinstance(attempts, list):
        attempts = summary.get("attempts")
    if isinstance(attempts, list):
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            result = str(attempt.get("result") or "").strip()
            if result.lower() not in _FAILURE_ATTEMPT_RESULTS:
                continue
            market_ticker = str(attempt.get("market_ticker") or "").strip()
            orderbook_status = str(attempt.get("orderbook_http_status") or "").strip()
            submission_status = str(attempt.get("submission_http_status") or "").strip()
            queue_status = str(attempt.get("queue_position_http_status") or "").strip()
            cancel_status = str(attempt.get("cancel_http_status") or "").strip()
            orderbook_error_type = str(attempt.get("orderbook_error_type") or "").strip()
            orderbook_error = str(attempt.get("orderbook_error") or "").strip()
            if result:
                reason = f"{reason} First failing attempt result: {result}"
                if market_ticker:
                    reason = f"{reason} on {market_ticker}"
                reason = f"{reason}."
                if orderbook_status:
                    reason = f"{reason} Orderbook HTTP status: {orderbook_status}."
                if orderbook_error_type:
                    reason = f"{reason} Orderbook error type: {orderbook_error_type}."
                if orderbook_error:
                    reason = f"{reason} Orderbook error: {orderbook_error}."
                if submission_status:
                    reason = f"{reason} Submission HTTP status: {submission_status}."
                if queue_status:
                    reason = f"{reason} Queue-position HTTP status: {queue_status}."
                if cancel_status:
                    reason = f"{reason} Cancel HTTP status: {cancel_status}."
                break

    failure_count = summary.get("prior_execute_failure_attempts_count")
    if isinstance(failure_count, int) and failure_count > 1:
        reason = f"{reason} Total failing attempts: {failure_count}."

    failure_markets = summary.get("prior_execute_failure_market_tickers")
    if isinstance(failure_markets, list) and failure_markets:
        reason = f"{reason} Affected markets: {', '.join(str(market) for market in failure_markets)}."

    http_status_counts = summary.get("prior_execute_failure_http_status_counts")
    if isinstance(http_status_counts, dict) and http_status_counts:
        status_parts = []
        for key in sorted(http_status_counts):
            count = http_status_counts[key]
            if isinstance(count, int) and count > 0:
                status_parts.append(f"{key}x{count}")
        if status_parts:
            reason = f"{reason} Failure HTTP statuses: {', '.join(status_parts)}."
    error_type_counts = summary.get("prior_execute_failure_error_type_counts")
    if isinstance(error_type_counts, dict) and error_type_counts:
        type_parts = []
        for key in sorted(error_type_counts):
            count = error_type_counts[key]
            if isinstance(count, int) and count > 0:
                type_parts.append(f"{key}x{count}")
        if type_parts:
            reason = f"{reason} Failure error types: {', '.join(type_parts)}."
    return reason


def _ready_for_live_order(summary: dict[str, Any]) -> tuple[bool, str]:
    downgraded_reason = str(summary.get("live_orders_downgraded_reason") or "").strip()
    if downgraded_reason:
        return False, downgraded_reason
    if summary.get("capture_status") not in {None, "ready"}:
        return False, "Fresh capture is not ready."
    ws_state_authority = summary.get("ws_state_authority")
    if summary.get("enforce_ws_state_authority") and isinstance(ws_state_authority, dict):
        ws_status = str(ws_state_authority.get("status") or "").strip().lower()
        if ws_status and ws_status != "ready":
            return False, f"Websocket-state authority is not healthy yet ({ws_status})."
    if summary.get("prior_trade_gate_pass") is not True:
        gate_status = str(summary.get("prior_trade_gate_status") or "hold")
        return False, f"Prior trade gate is not passing ({gate_status})."
    if summary.get("actual_live_balance_source") != "live":
        return False, "Live balance is not currently live-verified; the dry-run path can use cached balance, but real orders should reverify first."
    if summary.get("prior_execute_status") != "dry_run":
        return False, "This run did not finish in dry-run mode."
    reconcile_status = str(summary.get("reconcile_status") or "")
    if reconcile_status not in {"", "no_order_ids", "ready"}:
        return False, f"Reconcile status is not clear enough yet ({reconcile_status})."
    return True, "Prior trade gate passed and the dry-run path is clear for an explicit 1-contract live order."


def _ready_for_auto_live_order(
    summary: dict[str, Any],
    *,
    min_expected_roi_on_cost: float = 0.03,
    min_expected_roi_per_day: float = 0.0075,
    default_max_hours_to_close: float = 48.0,
    max_hours_to_close_by_canonical_niche: dict[str, float] | None = None,
    macro_probe_max_hours_to_close: float = 168.0,
    macro_probe_min_expected_roi_per_day: float = 0.01,
) -> tuple[bool, str]:
    niche_windows = {
        "macro_release": 168.0,
        "weather_energy_transmission": 72.0,
        "weather_climate": 36.0,
    }
    if isinstance(max_hours_to_close_by_canonical_niche, dict):
        for key, value in max_hours_to_close_by_canonical_niche.items():
            if not isinstance(key, str):
                continue
            try:
                niche_windows[key.strip().lower()] = float(value)
            except (TypeError, ValueError):
                continue

    ready_for_live = bool(summary.get("ready_for_live_order"))
    if not ready_for_live:
        return False, "Manual live readiness is not clear yet."

    expected_roi_on_cost = summary.get("top_market_expected_roi_on_cost_net")
    if not isinstance(expected_roi_on_cost, (int, float)):
        expected_roi_on_cost = summary.get("top_market_expected_roi_on_cost")
    if not isinstance(expected_roi_on_cost, (int, float)) or expected_roi_on_cost < min_expected_roi_on_cost:
        return (
            False,
            f"Expected ROI on cost is below the auto-live minimum of {min_expected_roi_on_cost:.4f}.",
        )

    expected_roi_per_day = summary.get("top_market_expected_roi_per_day_net")
    if not isinstance(expected_roi_per_day, (int, float)):
        expected_roi_per_day = summary.get("top_market_expected_roi_per_day")
    if not isinstance(expected_roi_per_day, (int, float)) or expected_roi_per_day < min_expected_roi_per_day:
        return (
            False,
            f"Expected ROI per day is below the auto-live minimum of {min_expected_roi_per_day:.4f}.",
        )

    canonical_niche = str(summary.get("top_market_canonical_niche") or "").strip().lower()
    max_hours_to_close = niche_windows.get(canonical_niche, default_max_hours_to_close)
    hours_to_close = summary.get("top_market_hours_to_close")
    if not isinstance(hours_to_close, (int, float)):
        return False, "Time to close is unavailable for the top market."
    if hours_to_close > max_hours_to_close:
        if (
            canonical_niche == "macro_release"
            and hours_to_close <= macro_probe_max_hours_to_close
            and isinstance(expected_roi_per_day, (int, float))
            and expected_roi_per_day >= macro_probe_min_expected_roi_per_day
        ):
            return True, (
                "Macro release is outside the full-size auto-live window but qualifies for probe-size live mode "
                f"({hours_to_close:.1f}h to close, ROI/day {expected_roi_per_day:.4f})."
            )
        niche_label = canonical_niche or "default"
        return False, (
            f"Time to close is above the auto-live maximum for {niche_label} "
            f"({hours_to_close:.1f}h > {max_hours_to_close:.1f}h)."
        )

    return True, "Manual live readiness is clear and the capital-efficiency thresholds pass for unattended auto-live."


def run_kalshi_micro_prior_trader(
    *,
    env_file: str,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    output_dir: str = "outputs",
    history_csv: str | None = None,
    watch_history_csv: str | None = None,
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    contracts_per_order: int = 1,
    max_orders: int = 3,
    min_maker_edge: float = 0.005,
    min_maker_edge_net_fees: float = 0.0,
    max_entry_price_dollars: float = 0.99,
    canonical_mapping_csv: str | None = "data/research/canonical_contract_mapping.csv",
    canonical_threshold_csv: str | None = "data/research/canonical_threshold_library.csv",
    prefer_canonical_thresholds: bool = True,
    require_canonical_mapping_for_live: bool = True,
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
    enforce_ws_state_authority: bool = False,
    ws_state_json: str | None = None,
    ws_state_max_age_seconds: float = 30.0,
    enforce_daily_weather_live_only: bool = True,
    require_daily_weather_board_coverage_for_live: bool = True,
    auto_refresh_weather_priors: bool = True,
    auto_weather_prior_max_markets: int = 30,
    auto_weather_allowed_contract_families: tuple[str, ...] = (
        "daily_rain",
        "daily_temperature",
        "monthly_climate_anomaly",
    ),
    auto_refresh_priors: bool = True,
    auto_prior_max_markets: int = 15,
    auto_prior_min_evidence_count: int = 2,
    auto_prior_min_evidence_quality: float = 0.55,
    auto_prior_min_high_trust_sources: int = 1,
    auto_prior_restrict_to_mapped_live_tickers: bool = True,
    auto_prior_allowed_canonical_niches: tuple[str, ...] = LIVE_ALLOWED_CANONICAL_NICHES,
    auto_prior_allowed_categories: tuple[str, ...] | None = None,
    auto_prior_disallowed_categories: tuple[str, ...] | None = ("Sports",),
    capture_before_execute: bool = True,
    capture_max_hours_to_close: float | None = 4000.0,
    capture_page_limit: int = 200,
    capture_max_pages: int = 12,
    use_temporary_live_env: bool = False,
    http_request_json=_http_request_json,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
    capture_runner: CaptureRunner = run_kalshi_nonsports_capture,
    weather_prior_runner: WeatherPriorRunner = run_kalshi_weather_priors,
    auto_prior_runner: AutoPriorRunner = run_kalshi_nonsports_auto_priors,
    prior_execute_runner: PriorExecuteRunner = run_kalshi_micro_prior_execute,
    reconcile_runner: ReconcileRunner = run_kalshi_micro_reconcile,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    allow_live_orders_requested = bool(allow_live_orders)
    live_orders_downgraded_reason: str | None = None
    if allow_live_orders_requested and not capture_before_execute:
        # Live orders must never run from stale history snapshots.
        allow_live_orders = False
        live_orders_downgraded_reason = (
            "Live orders were requested without a fresh capture; downgraded to dry-run for stale-data safety."
        )

    effective_history_csv = history_csv or str(Path(output_dir) / "kalshi_nonsports_history.csv")
    watch_history_path = Path(watch_history_csv) if watch_history_csv else default_watch_history_path(output_dir)
    watch_history_summary = summarize_watch_history(watch_history_path)

    capture_summary: dict[str, Any] | None = None
    if capture_before_execute:
        capture_summary = capture_runner(
            env_file=env_file,
            output_dir=output_dir,
            history_csv=effective_history_csv,
            timeout_seconds=timeout_seconds,
            excluded_categories=("Sports",),
            max_hours_to_close=capture_max_hours_to_close,
            page_limit=max(1, capture_page_limit),
            max_pages=max(1, capture_max_pages),
            now=captured_at,
        )

    weather_prior_summary: dict[str, Any] | None = None
    if auto_refresh_weather_priors:
        history_path = Path(effective_history_csv)
        if history_path.exists():
            try:
                weather_prior_summary = weather_prior_runner(
                    priors_csv=priors_csv,
                    history_csv=effective_history_csv,
                    output_dir=output_dir,
                    allowed_contract_families=auto_weather_allowed_contract_families,
                    max_markets=max(1, auto_weather_prior_max_markets),
                    timeout_seconds=timeout_seconds,
                    protect_manual=True,
                    write_back_to_priors=True,
                    now=captured_at,
                )
            except Exception as exc:
                weather_prior_summary = {
                    "status": "error",
                    "error": str(exc),
                    "history_csv": effective_history_csv,
                    "priors_csv": priors_csv,
                }
        else:
            weather_prior_summary = {
                "status": "skipped_missing_history",
                "history_csv": effective_history_csv,
                "priors_csv": priors_csv,
            }

    auto_prior_summary: dict[str, Any] | None = None
    if auto_refresh_priors:
        history_path = Path(effective_history_csv)
        if history_path.exists():
            try:
                auto_prior_summary = auto_prior_runner(
                    priors_csv=priors_csv,
                    history_csv=effective_history_csv,
                    output_dir=output_dir,
                    canonical_mapping_csv=canonical_mapping_csv,
                    allowed_canonical_niches=auto_prior_allowed_canonical_niches,
                    restrict_to_mapped_live_tickers=auto_prior_restrict_to_mapped_live_tickers,
                    allowed_categories=auto_prior_allowed_categories,
                    disallowed_categories=auto_prior_disallowed_categories,
                    max_markets=max(1, auto_prior_max_markets),
                    timeout_seconds=timeout_seconds,
                    min_evidence_count=max(1, auto_prior_min_evidence_count),
                    min_evidence_quality=max(0.0, min(1.0, auto_prior_min_evidence_quality)),
                    min_high_trust_sources=max(0, auto_prior_min_high_trust_sources),
                    protect_manual=True,
                    write_back_to_priors=True,
                    now=captured_at,
                )
            except Exception as exc:
                auto_prior_summary = {
                    "status": "error",
                    "error": str(exc),
                    "history_csv": effective_history_csv,
                    "priors_csv": priors_csv,
                }
        else:
            auto_prior_summary = {
                "status": "skipped_missing_history",
                "history_csv": effective_history_csv,
                "priors_csv": priors_csv,
            }

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "priors_csv": priors_csv,
        "allow_live_orders_requested": allow_live_orders_requested,
        "allow_live_orders_effective": bool(allow_live_orders),
        "live_orders_downgraded_reason": live_orders_downgraded_reason,
        "capture_required_for_live_orders": True,
        "canonical_mapping_csv": canonical_mapping_csv,
        "canonical_threshold_csv": canonical_threshold_csv,
        "prefer_canonical_thresholds": prefer_canonical_thresholds,
        "require_canonical_mapping_for_live": require_canonical_mapping_for_live,
        "include_incentives": include_incentives,
        "auto_refresh_weather_priors": auto_refresh_weather_priors,
        "auto_weather_prior_max_markets": max(1, auto_weather_prior_max_markets),
        "auto_weather_allowed_contract_families": sorted(
            {value.strip().lower() for value in auto_weather_allowed_contract_families if value.strip()}
        )
        if auto_weather_allowed_contract_families
        else None,
        "weather_priors_status": (
            weather_prior_summary.get("status") if isinstance(weather_prior_summary, dict) else "skipped"
        ),
        "weather_priors_generated": (
            weather_prior_summary.get("generated_priors") if isinstance(weather_prior_summary, dict) else 0
        ),
        "weather_priors_inserted_rows": (
            weather_prior_summary.get("inserted_rows") if isinstance(weather_prior_summary, dict) else 0
        ),
        "weather_priors_updated_rows": (
            weather_prior_summary.get("updated_rows") if isinstance(weather_prior_summary, dict) else 0
        ),
        "weather_priors_manual_rows_protected": (
            weather_prior_summary.get("manual_rows_protected") if isinstance(weather_prior_summary, dict) else 0
        ),
        "weather_priors_error": weather_prior_summary.get("error") if isinstance(weather_prior_summary, dict) else None,
        "weather_priors_error_kind": (
            weather_prior_summary.get("error_kind")
            if isinstance(weather_prior_summary, dict)
            else None
        ),
        "weather_priors_fetch_errors_count": (
            weather_prior_summary.get("fetch_errors_count")
            if isinstance(weather_prior_summary, dict)
            else 0
        ),
        "weather_priors_fetch_error_kind_counts": (
            weather_prior_summary.get("fetch_error_kind_counts")
            if isinstance(weather_prior_summary, dict)
            else {}
        ),
        "weather_priors_summary_file": (
            weather_prior_summary.get("output_file") if isinstance(weather_prior_summary, dict) else None
        ),
        "weather_priors_output_csv": (
            weather_prior_summary.get("output_csv") if isinstance(weather_prior_summary, dict) else None
        ),
        "weather_priors_skipped_output_csv": (
            weather_prior_summary.get("skipped_output_csv") if isinstance(weather_prior_summary, dict) else None
        ),
        "auto_refresh_priors": auto_refresh_priors,
        "auto_prior_restrict_to_mapped_live_tickers": auto_prior_restrict_to_mapped_live_tickers,
        "auto_prior_allowed_canonical_niches": sorted(
            {value.strip().lower() for value in auto_prior_allowed_canonical_niches if value.strip()}
        )
        if auto_prior_allowed_canonical_niches
        else None,
        "auto_prior_allowed_categories": list(auto_prior_allowed_categories) if auto_prior_allowed_categories else None,
        "auto_prior_disallowed_categories": (
            list(auto_prior_disallowed_categories) if auto_prior_disallowed_categories else None
        ),
        "auto_priors_status": auto_prior_summary.get("status") if isinstance(auto_prior_summary, dict) else "skipped",
        "auto_priors_generated": auto_prior_summary.get("generated_priors") if isinstance(auto_prior_summary, dict) else 0,
        "auto_priors_candidate_markets_filtered_out": (
            auto_prior_summary.get("candidate_markets_filtered_out") if isinstance(auto_prior_summary, dict) else 0
        ),
        "auto_priors_inserted_rows": auto_prior_summary.get("inserted_rows") if isinstance(auto_prior_summary, dict) else 0,
        "auto_priors_updated_rows": auto_prior_summary.get("updated_rows") if isinstance(auto_prior_summary, dict) else 0,
        "auto_priors_manual_rows_protected": (
            auto_prior_summary.get("manual_rows_protected") if isinstance(auto_prior_summary, dict) else 0
        ),
        "auto_priors_error": auto_prior_summary.get("error") if isinstance(auto_prior_summary, dict) else None,
        "auto_priors_summary_file": auto_prior_summary.get("output_file") if isinstance(auto_prior_summary, dict) else None,
        "auto_priors_output_csv": auto_prior_summary.get("output_csv") if isinstance(auto_prior_summary, dict) else None,
        "auto_priors_skipped_output_csv": (
            auto_prior_summary.get("skipped_output_csv") if isinstance(auto_prior_summary, dict) else None
        ),
        "capture_before_execute": capture_before_execute,
        "capture_max_hours_to_close": capture_max_hours_to_close,
        "capture_page_limit": max(1, capture_page_limit),
        "capture_max_pages": max(1, capture_max_pages),
        "enforce_daily_weather_live_only": bool(enforce_daily_weather_live_only),
        "require_daily_weather_board_coverage_for_live": bool(require_daily_weather_board_coverage_for_live),
        "live_env_mode": "temporary_copy" if allow_live_orders and use_temporary_live_env else "source_env",
        "auto_cancel_duplicate_open_orders": auto_cancel_duplicate_open_orders,
        "capture_status": capture_summary.get("status") if isinstance(capture_summary, dict) else None,
        "capture_summary_file": capture_summary.get("scan_summary_file") if isinstance(capture_summary, dict) else None,
        "capture_scan_page_requests": (
            capture_summary.get("scan_page_requests") if isinstance(capture_summary, dict) else None
        ),
        "capture_scan_rate_limit_retries_used": (
            capture_summary.get("scan_rate_limit_retries_used") if isinstance(capture_summary, dict) else None
        ),
        "capture_scan_network_retries_used": (
            capture_summary.get("scan_network_retries_used") if isinstance(capture_summary, dict) else None
        ),
        "capture_scan_transient_http_retries_used": (
            capture_summary.get("scan_transient_http_retries_used") if isinstance(capture_summary, dict) else None
        ),
        "capture_scan_search_retries_total": (
            capture_summary.get("scan_search_retries_total") if isinstance(capture_summary, dict) else None
        ),
        "capture_scan_search_health_status": (
            capture_summary.get("scan_search_health_status") if isinstance(capture_summary, dict) else None
        ),
        "capture_scan_events_fetched": (
            capture_summary.get("scan_events_fetched") if isinstance(capture_summary, dict) else None
        ),
        "capture_scan_markets_ranked": (
            capture_summary.get("scan_markets_ranked") if isinstance(capture_summary, dict) else None
        ),
        "capture_history_csv": capture_summary.get("history_csv") if isinstance(capture_summary, dict) else effective_history_csv,
        "book_db_path": book_db_path,
        "execution_event_log_csv": execution_event_log_csv,
        "execution_journal_db_path": execution_journal_db_path,
        "execution_frontier_recent_rows": max(1, int(execution_frontier_recent_rows)),
        "enforce_ws_state_authority": enforce_ws_state_authority,
        "ws_state_json": ws_state_json,
        "ws_state_max_age_seconds": ws_state_max_age_seconds,
        "watch_history_csv": str(watch_history_path),
        "watch_runs_total": watch_history_summary.get("watch_runs_total"),
        "watch_latest_recorded_at": watch_history_summary.get("latest_recorded_at"),
        "watch_board_regime": watch_history_summary.get("board_regime"),
        "watch_board_regime_reason": watch_history_summary.get("board_regime_reason"),
        "watch_focus_market_mode": watch_history_summary.get("latest_focus_market_mode"),
        "watch_focus_market_ticker": watch_history_summary.get("latest_focus_market_ticker"),
        "watch_focus_market_streak": watch_history_summary.get("focus_market_streak"),
        "watch_focus_market_state": watch_history_summary.get("focus_market_state"),
        "watch_focus_market_state_reason": watch_history_summary.get("focus_market_state_reason"),
        "watch_recent_focus_market_changes": watch_history_summary.get("recent_focus_market_changes"),
        "watch_recommendation_streak": watch_history_summary.get("recommendation_streak"),
        "watch_trade_gate_status_streak": watch_history_summary.get("trade_gate_status_streak"),
        "status": "hold",
        "action_taken": "hold",
    }

    capture_degraded = isinstance(capture_summary, dict) and capture_summary.get("status") not in {None, "ready"}
    capture_history_exists = Path(effective_history_csv).exists()
    capture_fallback_to_history = bool(capture_degraded and not allow_live_orders and capture_history_exists)
    summary["capture_fallback_to_history"] = capture_fallback_to_history
    summary["capture_history_exists"] = capture_history_exists

    if capture_degraded and not capture_fallback_to_history:
        summary["status"] = str(capture_summary.get("status"))
        summary["capture_error"] = capture_summary.get("scan_error")
        summary["capture_error_kind"] = _classify_capture_error(capture_summary.get("scan_error"))
        if not capture_history_exists and not allow_live_orders:
            summary["status_reason"] = (
                f"{_build_capture_failure_reason(summary)} Existing history CSV is missing, so dry-run fallback is unavailable."
            )
        else:
            summary["status_reason"] = _build_capture_failure_reason(summary)
    else:
        if capture_degraded and capture_fallback_to_history and isinstance(capture_summary, dict):
            summary["capture_error"] = capture_summary.get("scan_error")
            summary["capture_error_kind"] = _classify_capture_error(capture_summary.get("scan_error"))
            summary["status_reason"] = (
                f"{_build_capture_failure_reason(summary)} "
                "Continuing with existing history in dry-run fallback mode."
            )
        env_context = (
            temporary_live_env_file(env_file)
            if allow_live_orders and use_temporary_live_env
            else nullcontext(env_file)
        )
        with env_context as execute_env_file:
            execute_summary = prior_execute_runner(
                env_file=execute_env_file,
                priors_csv=priors_csv,
                history_csv=effective_history_csv,
                output_dir=output_dir,
                planning_bankroll_dollars=planning_bankroll_dollars,
                daily_risk_cap_dollars=daily_risk_cap_dollars,
                contracts_per_order=contracts_per_order,
                max_orders=max_orders,
                min_maker_edge=min_maker_edge,
                min_maker_edge_net_fees=min_maker_edge_net_fees,
                max_entry_price_dollars=max_entry_price_dollars,
                canonical_mapping_csv=canonical_mapping_csv,
                canonical_threshold_csv=canonical_threshold_csv,
                prefer_canonical_thresholds=prefer_canonical_thresholds,
                require_canonical_mapping_for_live=require_canonical_mapping_for_live,
                timeout_seconds=timeout_seconds,
                allow_live_orders=allow_live_orders,
                cancel_resting_immediately=cancel_resting_immediately,
                resting_hold_seconds=resting_hold_seconds,
                max_live_submissions_per_day=max_live_submissions_per_day,
                max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
                auto_cancel_duplicate_open_orders=auto_cancel_duplicate_open_orders,
                min_live_maker_edge=min_live_maker_edge,
                min_live_maker_edge_net_fees=min_live_maker_edge_net_fees,
                include_incentives=include_incentives,
                ledger_csv=ledger_csv,
                book_db_path=book_db_path,
                execution_event_log_csv=execution_event_log_csv,
                execution_journal_db_path=execution_journal_db_path,
                execution_frontier_recent_rows=max(1, int(execution_frontier_recent_rows)),
                enforce_ws_state_authority=enforce_ws_state_authority,
                ws_state_json=ws_state_json,
                ws_state_max_age_seconds=ws_state_max_age_seconds,
                enforce_daily_weather_live_only=enforce_daily_weather_live_only,
                require_daily_weather_board_coverage_for_live=require_daily_weather_board_coverage_for_live,
                http_request_json=http_request_json,
                http_get_json=http_get_json,
                sign_request=sign_request,
                now=captured_at,
            )
            prior_gate = execute_summary.get("prior_trade_gate_summary", {})
            summary.update(
                {
                    "prior_execute_status": execute_summary.get("status"),
                    "prior_execute_error_kind": _classify_prior_execute_error(execute_summary),
                    "blocked_duplicate_open_order_attempts": execute_summary.get(
                        "blocked_duplicate_open_order_attempts"
                    ),
                    "blocked_submission_budget_attempts": execute_summary.get(
                        "blocked_submission_budget_attempts"
                    ),
                    "blocked_live_cost_cap_attempts": execute_summary.get("blocked_live_cost_cap_attempts"),
                    "blocked_execution_policy_attempts": execute_summary.get("blocked_execution_policy_attempts"),
                    "execution_policy_active_attempts": execute_summary.get("execution_policy_active_attempts"),
                    "execution_policy_submit_attempts": execute_summary.get("execution_policy_submit_attempts"),
                    "execution_policy_skip_attempts": execute_summary.get("execution_policy_skip_attempts"),
                    "janitor_attempts": execute_summary.get("janitor_attempts"),
                    "janitor_canceled_open_orders_count": execute_summary.get(
                        "janitor_canceled_open_orders_count"
                    ),
                    "janitor_cancel_failed_attempts": execute_summary.get("janitor_cancel_failed_attempts"),
                    "orderbook_outage_short_circuit_triggered": execute_summary.get(
                        "orderbook_outage_short_circuit_triggered"
                    ),
                    "orderbook_outage_short_circuit_trigger_market_ticker": execute_summary.get(
                        "orderbook_outage_short_circuit_trigger_market_ticker"
                    ),
                    "orderbook_outage_short_circuit_skipped_orders": execute_summary.get(
                        "orderbook_outage_short_circuit_skipped_orders"
                    ),
                    "execution_event_rows_written": execute_summary.get("execution_event_rows_written"),
                    "execution_journal_db_path": execute_summary.get("execution_journal_db_path"),
                    "execution_journal_run_id": execute_summary.get("execution_journal_run_id"),
                    "execution_journal_rows_written": execute_summary.get("execution_journal_rows_written"),
                    "enforce_ws_state_authority": execute_summary.get("enforce_ws_state_authority"),
                    "ws_state_authority": execute_summary.get("ws_state_authority"),
                    "execution_frontier_status": execute_summary.get("execution_frontier_status"),
                    "execution_frontier_summary_file": execute_summary.get("execution_frontier_summary_file"),
                    "execution_frontier_bucket_csv": execute_summary.get("execution_frontier_bucket_csv"),
                    "execution_frontier_recommendations": execute_summary.get("execution_frontier_recommendations"),
                    "duplicate_open_order_markets": execute_summary.get("duplicate_open_order_markets"),
                    "prior_execute_summary_file": execute_summary.get("output_file"),
                    "prior_execute_csv": execute_summary.get("execute_output_csv"),
                    "prior_execute_attempts": execute_summary.get("attempts"),
                    "prior_plan_summary_file": execute_summary.get("plan_summary_file"),
                    "live_execution_lock_path": execute_summary.get("live_execution_lock_path"),
                    "live_execution_lock_acquired": execute_summary.get("live_execution_lock_acquired"),
                    "live_execution_lock_error": execute_summary.get("live_execution_lock_error"),
                    "actual_live_balance_dollars": execute_summary.get("actual_live_balance_dollars"),
                    "actual_live_balance_source": execute_summary.get("actual_live_balance_source"),
                    "balance_live_verified": execute_summary.get("balance_live_verified"),
                    "prior_trade_gate_pass": prior_gate.get("gate_pass") if isinstance(prior_gate, dict) else None,
                    "prior_trade_gate_status": prior_gate.get("gate_status") if isinstance(prior_gate, dict) else None,
                    "prior_trade_gate_score": prior_gate.get("gate_score") if isinstance(prior_gate, dict) else None,
                    "prior_trade_gate_blockers": prior_gate.get("gate_blockers") if isinstance(prior_gate, dict) else None,
                    "top_market_ticker": prior_gate.get("top_market_ticker") if isinstance(prior_gate, dict) else None,
                    "top_market_title": prior_gate.get("top_market_title") if isinstance(prior_gate, dict) else None,
                    "top_market_close_time": (
                        prior_gate.get("top_market_close_time") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_hours_to_close": (
                        prior_gate.get("top_market_hours_to_close") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_side": prior_gate.get("top_market_side") if isinstance(prior_gate, dict) else None,
                    "top_market_canonical_ticker": (
                        prior_gate.get("top_market_canonical_ticker") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_canonical_niche": (
                        prior_gate.get("top_market_canonical_niche") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_canonical_policy_applied": (
                        prior_gate.get("top_market_canonical_policy_applied") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_maker_entry_price_dollars": (
                        prior_gate.get("top_market_maker_entry_price_dollars") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_maker_entry_edge": (
                        prior_gate.get("top_market_maker_entry_edge") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_maker_entry_edge_net_fees": (
                        prior_gate.get("top_market_maker_entry_edge_net_fees") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_estimated_entry_cost_dollars": (
                        prior_gate.get("top_market_estimated_entry_cost_dollars") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_estimated_entry_fee_dollars": (
                        prior_gate.get("top_market_estimated_entry_fee_dollars") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_expected_value_dollars": (
                        prior_gate.get("top_market_expected_value_dollars") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_expected_value_net_dollars": (
                        prior_gate.get("top_market_expected_value_net_dollars") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_expected_roi_on_cost": (
                        prior_gate.get("top_market_expected_roi_on_cost") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_expected_roi_on_cost_net": (
                        prior_gate.get("top_market_expected_roi_on_cost_net") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_expected_value_per_day_dollars": (
                        prior_gate.get("top_market_expected_value_per_day_dollars")
                        if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_expected_value_per_day_net_dollars": (
                        prior_gate.get("top_market_expected_value_per_day_net_dollars")
                        if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_expected_roi_per_day": (
                        prior_gate.get("top_market_expected_roi_per_day") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_expected_roi_per_day_net": (
                        prior_gate.get("top_market_expected_roi_per_day_net") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_estimated_max_profit_dollars": (
                        prior_gate.get("top_market_estimated_max_profit_dollars") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_estimated_max_loss_dollars": (
                        prior_gate.get("top_market_estimated_max_loss_dollars") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_max_profit_roi_on_cost": (
                        prior_gate.get("top_market_max_profit_roi_on_cost") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_fair_probability": (
                        prior_gate.get("top_market_fair_probability") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_confidence": (
                        prior_gate.get("top_market_confidence") if isinstance(prior_gate, dict) else None
                    ),
                    "top_market_thesis": prior_gate.get("top_market_thesis") if isinstance(prior_gate, dict) else None,
                }
            )
            summary.update(_build_attempt_failure_summary(summary))
            if allow_live_orders and execute_summary.get("status") == "blocked_prior_trade_gate":
                summary["status"] = "hold"
                summary["action_taken"] = "hold"
            else:
                reconcile_summary = reconcile_runner(
                    env_file=execute_env_file,
                    execute_summary_file=execute_summary.get("execute_summary_file"),
                    output_dir=output_dir,
                    book_db_path=book_db_path,
                    timeout_seconds=timeout_seconds,
                    now=captured_at,
                )
                summary.update(
                    {
                        "reconcile_status": reconcile_summary.get("status"),
                        "reconcile_summary_file": reconcile_summary.get("output_file"),
                    }
                )
                execute_status = str(execute_summary.get("status") or "")
                if execute_status == "dry_run":
                    summary["status"] = "dry_run"
                    summary["action_taken"] = "dry_run_execute_reconcile"
                elif execute_status == "dry_run_orderbook_degraded":
                    summary["status"] = "dry_run_degraded"
                    summary["action_taken"] = "dry_run_execute_reconcile"
                    if "status_reason" not in summary:
                        summary["status_reason"] = (
                            "Dry run completed, but one or more orderbook probes were unavailable due transient network conditions."
                        )
                else:
                    summary["status"] = execute_status
                    summary["action_taken"] = (
                        "live_execute_reconcile" if allow_live_orders else "dry_run_execute_reconcile"
                    )
                    summary["status_reason"] = _build_prior_execute_failure_reason(summary)

    ready_for_live_order, ready_reason = _ready_for_live_order(summary)
    summary["ready_for_live_order"] = ready_for_live_order
    summary["ready_for_live_order_reason"] = ready_reason
    ready_for_auto_live_order, auto_live_reason = _ready_for_auto_live_order(summary)
    summary["ready_for_auto_live_order"] = ready_for_auto_live_order
    summary["ready_for_auto_live_order_reason"] = auto_live_reason

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_micro_prior_trader_summary_{stamp}.json"
    summary["output_file"] = str(output_path)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
