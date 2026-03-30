from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time
from typing import Any, Callable
from urllib.error import URLError

from betbot.dns_guard import is_dns_resolution_error, run_dns_doctor
from betbot.kalshi_arb_scan import run_kalshi_arb_scan
from betbot.kalshi_micro_execute import _http_request_json
from betbot.kalshi_micro_prior_trader import run_kalshi_micro_prior_trader
from betbot.live_smoke import (
    HttpGetter,
    KALSHI_API_ROOTS,
    KalshiSigner,
    _http_get_json,
    _kalshi_sign_request,
    kalshi_api_root_candidates,
)
from betbot.onboarding import _parse_env_file


_TRADER_FAILURE_STATUSES = {
    "upstream_error",
    "rate_limited",
    "error",
    "live_submit_failed",
    "live_janitor_cancel_failed",
}
_NO_REAL_CANDIDATE_STATUSES = {
    "no_candidates",
    "no_edge",
    "edge_too_small",
}
_ARB_FAILURE_STATUSES = {
    "upstream_error",
    "rate_limited",
    "error",
}

DnsDoctorRunner = Callable[..., dict[str, Any]]


def _resolve_output_dir(output_dir: str) -> tuple[Path, str | None]:
    path = Path(output_dir)
    if path.suffix:
        parent = path.parent if str(path.parent) not in {"", "."} else Path(".")
        return parent, f"normalized_file_like_output_dir:{path}"
    return path, None


@dataclass
class _TokenBucket:
    capacity: float
    refill_per_second: float
    tokens: float
    last_refill_monotonic: float

    def refill(self) -> None:
        now = time.monotonic()
        elapsed = max(0.0, now - self.last_refill_monotonic)
        self.last_refill_monotonic = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_second)

    def consume(self, amount: float = 1.0) -> float:
        self.refill()
        if self.tokens >= amount:
            self.tokens -= amount
            return 0.0
        missing = amount - self.tokens
        self.tokens = 0.0
        if self.refill_per_second <= 0:
            return 0.0
        return missing / self.refill_per_second


class ApiRateLimiter:
    def __init__(self, *, read_per_minute: float, write_per_minute: float) -> None:
        now = time.monotonic()
        self._read = _TokenBucket(
            capacity=max(1.0, read_per_minute),
            refill_per_second=max(0.01, read_per_minute / 60.0),
            tokens=max(1.0, read_per_minute),
            last_refill_monotonic=now,
        )
        self._write = _TokenBucket(
            capacity=max(1.0, write_per_minute),
            refill_per_second=max(0.01, write_per_minute / 60.0),
            tokens=max(1.0, write_per_minute),
            last_refill_monotonic=now,
        )

    def throttle(self, is_write: bool) -> None:
        bucket = self._write if is_write else self._read
        wait_seconds = bucket.consume(1.0)
        if wait_seconds > 0:
            time.sleep(wait_seconds)


def _make_throttled_http_get_json(
    *,
    rate_limiter: ApiRateLimiter,
    http_get_json: HttpGetter,
) -> HttpGetter:
    def wrapped(url: str, headers: dict[str, str], timeout_seconds: float) -> tuple[int, Any]:
        rate_limiter.throttle(is_write=False)
        return http_get_json(url, headers, timeout_seconds)

    return wrapped


def _make_throttled_http_request_json(
    *,
    rate_limiter: ApiRateLimiter,
):
    def wrapped(
        url: str,
        method: str,
        headers: dict[str, str],
        body: Any | None,
        timeout_seconds: float,
    ) -> tuple[int, Any]:
        rate_limiter.throttle(is_write=method.upper() in {"POST", "PUT", "PATCH", "DELETE"})
        return _http_request_json(url, method, headers, body, timeout_seconds)

    return wrapped


def _read_exchange_status(
    *,
    env_file: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
) -> dict[str, Any]:
    env = _parse_env_file(Path(env_file))
    kalshi_env = (env.get("KALSHI_ENV") or "prod").strip().lower()
    if kalshi_env not in KALSHI_API_ROOTS:
        kalshi_env = "prod"
    api_roots = kalshi_api_root_candidates(kalshi_env)
    headers = {
        "Accept": "application/json",
        "User-Agent": "betbot-kalshi-supervisor/1.0",
    }
    status_code: int | None = None
    payload: Any = {}
    network_error: str | None = None
    dns_error = False
    api_root_used: str | None = None
    api_roots_attempted: list[str] = []
    max_retries = 2
    for root_index, api_root in enumerate(api_roots):
        request_url = f"{api_root}/exchange/status"
        api_roots_attempted.append(api_root)
        for attempt in range(max_retries + 1):
            try:
                status_code, payload = http_get_json(
                    request_url,
                    headers,
                    timeout_seconds,
                )
                network_error = None
                dns_error = False
                api_root_used = api_root
                break
            except URLError as exc:
                network_error = str(exc.reason)
                dns_error = is_dns_resolution_error(exc)
                if attempt >= max_retries:
                    status_code = None
                    payload = {"error": f"network_error: {network_error}"}
                    break
                time.sleep(0.75 * (2**attempt))
        if status_code is not None:
            break
        if root_index < len(api_roots) - 1:
            continue

    trading_active = False
    if status_code == 200 and isinstance(payload, dict):
        if isinstance(payload.get("trading_active"), bool):
            trading_active = bool(payload.get("trading_active"))
        elif isinstance(payload.get("exchange_active"), bool):
            trading_active = bool(payload.get("exchange_active"))
    return {
        "http_status": status_code,
        "payload": payload,
        "trading_active": trading_active,
        "network_error": network_error,
        "dns_error": dns_error,
        "api_root_used": api_root_used,
        "api_roots_attempted": api_roots_attempted,
    }


def _as_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _exchange_status_has_upstream_issue(exchange_status: dict[str, Any]) -> bool:
    if bool(exchange_status.get("dns_error")):
        return True
    network_error = str(exchange_status.get("network_error") or "").strip().lower()
    if network_error:
        return True
    return exchange_status.get("http_status") is None


def _is_no_real_candidates_state(trader_summary: dict[str, Any]) -> bool:
    status = _as_status(trader_summary.get("status"))
    prior_execute_status = _as_status(trader_summary.get("prior_execute_status"))
    prior_gate_status = _as_status(trader_summary.get("prior_trade_gate_status"))
    return (
        status in _NO_REAL_CANDIDATE_STATUSES
        or prior_execute_status in _NO_REAL_CANDIDATE_STATUSES
        or prior_gate_status in _NO_REAL_CANDIDATE_STATUSES
    )


def _collect_trader_failure_reasons(trader_summary: dict[str, Any]) -> list[str]:
    reasons: list[str] = []

    status = _as_status(trader_summary.get("status"))
    capture_status = _as_status(trader_summary.get("capture_status"))
    prior_execute_status = _as_status(trader_summary.get("prior_execute_status"))
    reconcile_status = _as_status(trader_summary.get("reconcile_status"))
    auto_priors_status = _as_status(trader_summary.get("auto_priors_status"))

    if _is_no_real_candidates_state(trader_summary):
        return reasons

    if status in _TRADER_FAILURE_STATUSES:
        reasons.append(f"prior_trader_status:{status}")
    if capture_status in _TRADER_FAILURE_STATUSES:
        reasons.append(f"capture_status:{capture_status}")
    if prior_execute_status in _TRADER_FAILURE_STATUSES:
        reasons.append(f"prior_execute_status:{prior_execute_status}")
    if reconcile_status in {"upstream_error", "rate_limited", "error"}:
        reasons.append(f"reconcile_status:{reconcile_status}")
    if auto_priors_status == "error":
        reasons.append("auto_priors_status:error")
    if trader_summary.get("auto_priors_error"):
        reasons.append("auto_priors_error_present")
    if not status:
        reasons.append("prior_trader_status:missing")
    return list(dict.fromkeys(reasons))


def _collect_arb_failure_reasons(arb_summary: dict[str, Any] | None) -> list[str]:
    if not isinstance(arb_summary, dict):
        return []
    arb_status = _as_status(arb_summary.get("status"))
    if arb_status in _ARB_FAILURE_STATUSES:
        return [f"arb_scan_status:{arb_status}"]
    return []


def run_kalshi_supervisor(
    *,
    env_file: str,
    output_dir: str = "outputs",
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str | None = None,
    ledger_csv: str | None = None,
    book_db_path: str | None = None,
    cycles: int = 1,
    sleep_between_cycles_seconds: float = 20.0,
    timeout_seconds: float = 15.0,
    allow_live_orders: bool = False,
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    contracts_per_order: int = 1,
    max_orders: int = 3,
    min_maker_edge: float = 0.005,
    min_maker_edge_net_fees: float = 0.0,
    max_entry_price_dollars: float = 0.99,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    auto_cancel_duplicate_open_orders: bool = True,
    min_live_maker_edge: float = 0.01,
    min_live_maker_edge_net_fees: float = 0.0,
    cancel_resting_immediately: bool = False,
    resting_hold_seconds: float = 0.0,
    include_incentives: bool = True,
    auto_refresh_priors: bool = True,
    auto_prior_max_markets: int = 15,
    auto_prior_min_evidence_count: int = 2,
    auto_prior_min_evidence_quality: float = 0.55,
    auto_prior_min_high_trust_sources: int = 1,
    enforce_ws_state_authority: bool = True,
    ws_state_json: str | None = None,
    ws_state_max_age_seconds: float = 30.0,
    read_requests_per_minute: float = 120.0,
    write_requests_per_minute: float = 30.0,
    failure_remediation_enabled: bool = True,
    failure_remediation_max_retries: int = 2,
    failure_remediation_backoff_seconds: float = 5.0,
    failure_remediation_timeout_multiplier: float = 1.5,
    failure_remediation_timeout_cap_seconds: float = 45.0,
    exchange_status_self_heal_attempts: int = 2,
    exchange_status_self_heal_pause_seconds: float = 10.0,
    exchange_status_run_dns_doctor: bool = True,
    exchange_status_self_heal_timeout_multiplier: float = 1.5,
    exchange_status_self_heal_timeout_cap_seconds: float = 45.0,
    run_arb_scan_each_cycle: bool = True,
    dns_doctor_runner: DnsDoctorRunner = run_dns_doctor,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    out_dir, output_dir_warning = _resolve_output_dir(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    effective_output_dir = str(out_dir)
    effective_history_csv = history_csv or str(out_dir / "kalshi_nonsports_history.csv")
    safe_cycles = max(1, cycles)

    rate_limiter = ApiRateLimiter(
        read_per_minute=read_requests_per_minute,
        write_per_minute=write_requests_per_minute,
    )
    throttled_get = _make_throttled_http_get_json(rate_limiter=rate_limiter, http_get_json=http_get_json)
    throttled_request = _make_throttled_http_request_json(rate_limiter=rate_limiter)

    max_retries = max(0, failure_remediation_max_retries)
    base_backoff_seconds = max(0.0, failure_remediation_backoff_seconds)
    failure_remediation_timeout_multiplier_safe = max(1.0, float(failure_remediation_timeout_multiplier))
    failure_remediation_timeout_cap_seconds_safe = max(
        max(0.25, float(timeout_seconds)),
        float(failure_remediation_timeout_cap_seconds),
    )
    exchange_status_heal_attempts = max(0, int(exchange_status_self_heal_attempts))
    exchange_status_heal_pause_seconds = max(0.0, float(exchange_status_self_heal_pause_seconds))
    exchange_status_heal_timeout_multiplier = max(1.0, float(exchange_status_self_heal_timeout_multiplier))
    exchange_status_heal_timeout_cap_seconds = max(
        max(0.25, float(timeout_seconds)),
        float(exchange_status_self_heal_timeout_cap_seconds),
    )
    cycle_summaries: list[dict[str, Any]] = []
    cycles_with_failures = 0
    cycles_with_unremediated_failures = 0
    cycles_with_remediation = 0
    for index in range(safe_cycles):
        cycle_started_at = datetime.now(timezone.utc)
        exchange_status = _read_exchange_status(
            env_file=env_file,
            timeout_seconds=timeout_seconds,
            http_get_json=throttled_get,
        )
        exchange_status_history: list[dict[str, Any]] = [exchange_status]
        exchange_status_remediation_actions: list[dict[str, Any]] = []
        exchange_status_remediation_applied = False
        exchange_status_remediation_recovered = False
        exchange_status_remediation_attempts_used = 0
        if allow_live_orders and _exchange_status_has_upstream_issue(exchange_status):
            for remediation_index in range(exchange_status_heal_attempts):
                remediation_timeout_seconds = min(
                    max(0.25, float(timeout_seconds))
                    * (exchange_status_heal_timeout_multiplier ** (remediation_index + 1)),
                    exchange_status_heal_timeout_cap_seconds,
                )
                exchange_status_remediation_applied = True
                remediation_dns_summary: dict[str, Any] | None = None
                remediation_dns_error: str | None = None
                if exchange_status_run_dns_doctor:
                    try:
                        remediation_dns_summary = dns_doctor_runner(
                            env_file=env_file,
                            output_dir=effective_output_dir,
                            timeout_seconds=max(0.25, min(3.0, remediation_timeout_seconds / 6.0)),
                        )
                    except Exception as exc:  # pragma: no cover - defensive runtime path
                        remediation_dns_error = str(exc)
                if exchange_status_heal_pause_seconds > 0:
                    time.sleep(exchange_status_heal_pause_seconds)
                exchange_status = _read_exchange_status(
                    env_file=env_file,
                    timeout_seconds=remediation_timeout_seconds,
                    http_get_json=throttled_get,
                )
                exchange_status_history.append(exchange_status)
                exchange_status_remediation_attempts_used += 1
                exchange_status_remediation_actions.append(
                    {
                        "attempt": remediation_index + 1,
                        "dns_doctor_status": (remediation_dns_summary or {}).get("status"),
                        "dns_doctor_output_file": (remediation_dns_summary or {}).get("output_file"),
                        "dns_doctor_error": remediation_dns_error,
                        "dns_doctor_timeout_seconds": max(0.25, min(3.0, remediation_timeout_seconds / 6.0)),
                        "retry_timeout_seconds": remediation_timeout_seconds,
                        "sleep_seconds": exchange_status_heal_pause_seconds,
                        "exchange_status_http": exchange_status.get("http_status"),
                        "exchange_dns_error": exchange_status.get("dns_error"),
                        "exchange_network_error": exchange_status.get("network_error"),
                    }
                )
                if not _exchange_status_has_upstream_issue(exchange_status):
                    exchange_status_remediation_recovered = True
                    break
        cycle_live_enabled = allow_live_orders and bool(exchange_status.get("trading_active"))

        trader_attempts: list[dict[str, Any]] = []
        remediation_actions: list[dict[str, Any]] = []
        capture_before_execute = True
        force_dry_run = False
        trader_summary: dict[str, Any] = {}
        initial_failure_reasons: list[str] = []
        final_failure_reasons: list[str] = []
        no_real_candidates = False
        remediation_applied = False
        remediation_recovered = False
        remediation_attempts_used = 0

        for attempt_index in range(max_retries + 1):
            attempt_now = cycle_started_at + timedelta(milliseconds=attempt_index)
            live_enabled_for_attempt = cycle_live_enabled and not force_dry_run
            attempt_timeout_seconds = min(
                max(0.25, float(timeout_seconds))
                * (failure_remediation_timeout_multiplier_safe**attempt_index),
                failure_remediation_timeout_cap_seconds_safe,
            )
            try:
                trader_summary = run_kalshi_micro_prior_trader(
                    env_file=env_file,
                    priors_csv=priors_csv,
                    output_dir=effective_output_dir,
                    history_csv=effective_history_csv,
                    planning_bankroll_dollars=planning_bankroll_dollars,
                    daily_risk_cap_dollars=daily_risk_cap_dollars,
                    contracts_per_order=contracts_per_order,
                    max_orders=max_orders,
                    min_maker_edge=min_maker_edge,
                    min_maker_edge_net_fees=min_maker_edge_net_fees,
                    max_entry_price_dollars=max_entry_price_dollars,
                    timeout_seconds=attempt_timeout_seconds,
                    allow_live_orders=live_enabled_for_attempt,
                    max_live_submissions_per_day=max_live_submissions_per_day,
                    max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
                    auto_cancel_duplicate_open_orders=auto_cancel_duplicate_open_orders,
                    min_live_maker_edge=min_live_maker_edge,
                    min_live_maker_edge_net_fees=min_live_maker_edge_net_fees,
                    cancel_resting_immediately=cancel_resting_immediately,
                    resting_hold_seconds=resting_hold_seconds,
                    include_incentives=include_incentives,
                    auto_refresh_priors=auto_refresh_priors,
                    auto_prior_max_markets=auto_prior_max_markets,
                    auto_prior_min_evidence_count=auto_prior_min_evidence_count,
                    auto_prior_min_evidence_quality=auto_prior_min_evidence_quality,
                    auto_prior_min_high_trust_sources=auto_prior_min_high_trust_sources,
                    enforce_ws_state_authority=enforce_ws_state_authority,
                    ws_state_json=ws_state_json,
                    ws_state_max_age_seconds=ws_state_max_age_seconds,
                    ledger_csv=ledger_csv,
                    book_db_path=book_db_path,
                    capture_before_execute=capture_before_execute,
                    use_temporary_live_env=live_enabled_for_attempt,
                    http_request_json=throttled_request,
                    http_get_json=throttled_get,
                    sign_request=sign_request,
                    now=attempt_now,
                )
            except Exception as exc:  # pragma: no cover - defensive runtime path
                trader_summary = {
                    "status": "error",
                    "error": str(exc),
                }

            attempt_failure_reasons = _collect_trader_failure_reasons(trader_summary)
            no_real_candidates = _is_no_real_candidates_state(trader_summary)
            trader_attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "capture_before_execute": capture_before_execute,
                    "allow_live_orders": live_enabled_for_attempt,
                    "timeout_seconds": attempt_timeout_seconds,
                    "status": trader_summary.get("status"),
                    "capture_status": trader_summary.get("capture_status"),
                    "blocked_submission_budget_attempts": trader_summary.get("blocked_submission_budget_attempts"),
                    "blocked_live_cost_cap_attempts": trader_summary.get("blocked_live_cost_cap_attempts"),
                    "janitor_attempts": trader_summary.get("janitor_attempts"),
                    "janitor_canceled_open_orders_count": trader_summary.get("janitor_canceled_open_orders_count"),
                    "janitor_cancel_failed_attempts": trader_summary.get("janitor_cancel_failed_attempts"),
                    "live_execution_lock_acquired": trader_summary.get("live_execution_lock_acquired"),
                    "live_execution_lock_error": trader_summary.get("live_execution_lock_error"),
                    "capture_scan_search_health_status": trader_summary.get("capture_scan_search_health_status"),
                    "capture_scan_search_retries_total": trader_summary.get("capture_scan_search_retries_total"),
                    "capture_scan_page_requests": trader_summary.get("capture_scan_page_requests"),
                    "capture_scan_rate_limit_retries_used": trader_summary.get("capture_scan_rate_limit_retries_used"),
                    "capture_scan_network_retries_used": trader_summary.get("capture_scan_network_retries_used"),
                    "capture_scan_transient_http_retries_used": trader_summary.get(
                        "capture_scan_transient_http_retries_used"
                    ),
                    "prior_execute_status": trader_summary.get("prior_execute_status"),
                    "duplicate_open_order_markets": trader_summary.get("duplicate_open_order_markets"),
                    "prior_trade_gate_status": trader_summary.get("prior_trade_gate_status"),
                    "auto_priors_status": trader_summary.get("auto_priors_status"),
                    "enforce_ws_state_authority": trader_summary.get("enforce_ws_state_authority"),
                    "ws_state_authority_status": (
                        (trader_summary.get("ws_state_authority") or {}).get("status")
                        if isinstance(trader_summary.get("ws_state_authority"), dict)
                        else None
                    ),
                    "prior_trader_summary_file": trader_summary.get("output_file"),
                    "failure_reasons": attempt_failure_reasons,
                    "no_real_candidates": no_real_candidates,
                }
            )
            if attempt_index == 0:
                initial_failure_reasons = attempt_failure_reasons

            final_failure_reasons = attempt_failure_reasons
            if no_real_candidates or not final_failure_reasons:
                remediation_recovered = remediation_applied and not final_failure_reasons
                break
            if not failure_remediation_enabled or attempt_index >= max_retries:
                break

            remediation_applied = True
            remediation_attempts_used += 1
            status_now = _as_status(trader_summary.get("status"))
            capture_status_now = _as_status(trader_summary.get("capture_status"))
            action_flags: list[str] = []

            if capture_status_now in {"upstream_error", "rate_limited"}:
                capture_before_execute = False
                action_flags.append("retry_without_fresh_capture")
                # Never allow live orders after a failed fresh capture; retry in dry run only.
                force_dry_run = True
                action_flags.append("force_dry_run_stale_capture")
            if status_now in {"live_submit_failed", "error"}:
                force_dry_run = True
                action_flags.append("force_dry_run_retry")
            if not action_flags:
                capture_before_execute = False
                action_flags.append("retry_transient")

            wait_seconds = base_backoff_seconds * (2**attempt_index)
            if wait_seconds > 0:
                time.sleep(wait_seconds)

            exchange_status = _read_exchange_status(
                env_file=env_file,
                timeout_seconds=attempt_timeout_seconds,
                http_get_json=throttled_get,
            )
            exchange_status_history.append(exchange_status)
            cycle_live_enabled = allow_live_orders and bool(exchange_status.get("trading_active"))
            remediation_actions.append(
                {
                    "attempt": attempt_index + 1,
                    "actions": action_flags,
                    "wait_seconds": wait_seconds,
                    "retry_timeout_seconds": attempt_timeout_seconds,
                    "exchange_status_http": exchange_status.get("http_status"),
                    "exchange_dns_error": exchange_status.get("dns_error"),
                    "exchange_network_error": exchange_status.get("network_error"),
                }
            )

        arb_summary: dict[str, Any] | None = None
        arb_attempts: list[dict[str, Any]] = []
        arb_initial_failure_reasons: list[str] = []
        arb_failure_reasons: list[str] = []
        if run_arb_scan_each_cycle:
            for attempt_index in range(max_retries + 1):
                arb_attempt_now = cycle_started_at + timedelta(milliseconds=100 + attempt_index)
                arb_attempt_timeout_seconds = min(
                    max(0.25, float(timeout_seconds))
                    * (failure_remediation_timeout_multiplier_safe**attempt_index),
                    failure_remediation_timeout_cap_seconds_safe,
                )
                try:
                    arb_summary = run_kalshi_arb_scan(
                        env_file=env_file,
                        output_dir=effective_output_dir,
                        timeout_seconds=arb_attempt_timeout_seconds,
                        http_get_json=throttled_get,
                        now=arb_attempt_now,
                    )
                except Exception as exc:  # pragma: no cover - defensive runtime path
                    arb_summary = {
                        "status": "error",
                        "events_error": str(exc),
                    }
                arb_failure_reasons = _collect_arb_failure_reasons(arb_summary)
                if attempt_index == 0:
                    arb_initial_failure_reasons = list(arb_failure_reasons)
                arb_attempts.append(
                    {
                        "attempt": attempt_index + 1,
                        "timeout_seconds": arb_attempt_timeout_seconds,
                        "status": arb_summary.get("status"),
                        "arb_scan_summary_file": arb_summary.get("output_file") if isinstance(arb_summary, dict) else None,
                        "failure_reasons": arb_failure_reasons,
                    }
                )
                if not arb_failure_reasons or not failure_remediation_enabled or attempt_index >= max_retries:
                    break
                remediation_applied = True
                remediation_attempts_used += 1
                wait_seconds = base_backoff_seconds * (2**attempt_index)
                if wait_seconds > 0:
                    time.sleep(wait_seconds)

        combined_final_failure_reasons = list(dict.fromkeys(final_failure_reasons + arb_failure_reasons))
        combined_initial_failure_reasons = list(dict.fromkeys(initial_failure_reasons + arb_initial_failure_reasons))
        cycle_has_failure = bool(combined_initial_failure_reasons)
        cycle_has_unremediated_failure = bool(combined_final_failure_reasons)
        if cycle_has_failure:
            cycles_with_failures += 1
        if remediation_applied:
            cycles_with_remediation += 1
        if cycle_has_unremediated_failure:
            cycles_with_unremediated_failures += 1

        cycle_summaries.append(
            {
                "cycle": index + 1,
                "started_at": cycle_started_at.isoformat(),
                "exchange_status": exchange_status,
                "exchange_status_history": exchange_status_history,
                "exchange_status_remediation_applied": exchange_status_remediation_applied,
                "exchange_status_remediation_recovered": exchange_status_remediation_recovered,
                "exchange_status_remediation_attempts_used": exchange_status_remediation_attempts_used,
                "exchange_status_remediation_attempts_max": exchange_status_heal_attempts,
                "exchange_status_remediation_actions": exchange_status_remediation_actions,
                "live_orders_requested": allow_live_orders,
                "live_orders_enabled_for_cycle": cycle_live_enabled,
                "prior_trader_status": trader_summary.get("status"),
                "blocked_duplicate_open_order_attempts": trader_summary.get("blocked_duplicate_open_order_attempts"),
                "blocked_submission_budget_attempts": trader_summary.get("blocked_submission_budget_attempts"),
                "blocked_live_cost_cap_attempts": trader_summary.get("blocked_live_cost_cap_attempts"),
                "janitor_attempts": trader_summary.get("janitor_attempts"),
                "janitor_canceled_open_orders_count": trader_summary.get("janitor_canceled_open_orders_count"),
                "janitor_cancel_failed_attempts": trader_summary.get("janitor_cancel_failed_attempts"),
                "live_execution_lock_acquired": trader_summary.get("live_execution_lock_acquired"),
                "live_execution_lock_error": trader_summary.get("live_execution_lock_error"),
                "duplicate_open_order_markets": trader_summary.get("duplicate_open_order_markets"),
                "prior_trader_summary_file": trader_summary.get("output_file"),
                "capture_scan_search_health_status": trader_summary.get("capture_scan_search_health_status"),
                "capture_scan_search_retries_total": trader_summary.get("capture_scan_search_retries_total"),
                "capture_scan_page_requests": trader_summary.get("capture_scan_page_requests"),
                "capture_scan_rate_limit_retries_used": trader_summary.get("capture_scan_rate_limit_retries_used"),
                "capture_scan_network_retries_used": trader_summary.get("capture_scan_network_retries_used"),
                "capture_scan_transient_http_retries_used": trader_summary.get(
                    "capture_scan_transient_http_retries_used"
                ),
                "auto_priors_status": trader_summary.get("auto_priors_status"),
                "auto_priors_generated": trader_summary.get("auto_priors_generated"),
                "auto_priors_inserted_rows": trader_summary.get("auto_priors_inserted_rows"),
                "auto_priors_updated_rows": trader_summary.get("auto_priors_updated_rows"),
                "enforce_ws_state_authority": trader_summary.get("enforce_ws_state_authority"),
                "ws_state_authority_status": (
                    (trader_summary.get("ws_state_authority") or {}).get("status")
                    if isinstance(trader_summary.get("ws_state_authority"), dict)
                    else None
                ),
                "arb_scan_status": arb_summary.get("status") if isinstance(arb_summary, dict) else None,
                "arb_scan_summary_file": arb_summary.get("output_file") if isinstance(arb_summary, dict) else None,
                "no_real_candidates": no_real_candidates,
                "failure_detected": cycle_has_failure,
                "unremediated_failure": cycle_has_unremediated_failure,
                "initial_failure_reasons": combined_initial_failure_reasons,
                "final_failure_reasons": combined_final_failure_reasons,
                "remediation_applied": remediation_applied,
                "remediation_recovered": remediation_recovered and not cycle_has_unremediated_failure,
                "remediation_attempts_used": remediation_attempts_used,
                "remediation_attempts_max": max_retries,
                "remediation_actions": remediation_actions,
                "trader_attempts": trader_attempts,
                "arb_attempts": arb_attempts,
            }
        )
        if index < safe_cycles - 1 and sleep_between_cycles_seconds > 0:
            time.sleep(sleep_between_cycles_seconds)

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "requested_output_dir": output_dir,
        "resolved_output_dir": effective_output_dir,
        "output_dir_warning": output_dir_warning,
        "priors_csv": priors_csv,
        "history_csv": effective_history_csv,
        "cycles_requested": cycles,
        "cycles_run": safe_cycles,
        "allow_live_orders_requested": allow_live_orders,
        "read_requests_per_minute": read_requests_per_minute,
        "write_requests_per_minute": write_requests_per_minute,
        "failure_remediation_enabled": failure_remediation_enabled,
        "failure_remediation_max_retries": max_retries,
        "failure_remediation_backoff_seconds": base_backoff_seconds,
        "failure_remediation_timeout_multiplier": failure_remediation_timeout_multiplier_safe,
        "failure_remediation_timeout_cap_seconds": failure_remediation_timeout_cap_seconds_safe,
        "exchange_status_self_heal_attempts": exchange_status_heal_attempts,
        "exchange_status_self_heal_pause_seconds": exchange_status_heal_pause_seconds,
        "exchange_status_run_dns_doctor": exchange_status_run_dns_doctor,
        "exchange_status_self_heal_timeout_multiplier": exchange_status_heal_timeout_multiplier,
        "exchange_status_self_heal_timeout_cap_seconds": exchange_status_heal_timeout_cap_seconds,
        "cycles_with_failures": cycles_with_failures,
        "cycles_with_remediation": cycles_with_remediation,
        "cycles_with_unremediated_failures": cycles_with_unremediated_failures,
        "run_arb_scan_each_cycle": run_arb_scan_each_cycle,
        "include_incentives": include_incentives,
        "auto_cancel_duplicate_open_orders": auto_cancel_duplicate_open_orders,
        "auto_refresh_priors": auto_refresh_priors,
        "auto_prior_max_markets": auto_prior_max_markets,
        "auto_prior_min_evidence_count": auto_prior_min_evidence_count,
        "auto_prior_min_evidence_quality": auto_prior_min_evidence_quality,
        "auto_prior_min_high_trust_sources": auto_prior_min_high_trust_sources,
        "enforce_ws_state_authority": enforce_ws_state_authority,
        "ws_state_json": ws_state_json,
        "ws_state_max_age_seconds": ws_state_max_age_seconds,
        "cycle_summaries": cycle_summaries,
        "status": "degraded_ready" if cycles_with_unremediated_failures > 0 else "ready",
    }
    output_path = out_dir / f"kalshi_supervisor_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
