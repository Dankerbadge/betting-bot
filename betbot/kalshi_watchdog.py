from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time
from typing import Any, Callable

from betbot.dns_guard import run_dns_doctor
from betbot.kalshi_autopilot import run_kalshi_autopilot


AutopilotRunner = Callable[..., dict[str, Any]]
DnsDoctorRunner = Callable[..., dict[str, Any]]
SleepFn = Callable[[float], None]

_UPSTREAM_TOKENS = (
    "dns",
    "upstream",
    "rate_limited",
    "network_error",
    "name or service not known",
    "nodename nor servname",
    "temporary failure in name resolution",
    "timeout",
)


def _as_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _matches_upstream_pattern(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return False
    return any(token in lowered for token in _UPSTREAM_TOKENS)


def _detect_upstream_incident(autopilot_summary: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    blockers = autopilot_summary.get("preflight_blockers")
    if isinstance(blockers, list):
        for blocker in blockers:
            blocker_text = str(blocker or "").strip()
            if blocker_text and _matches_upstream_pattern(blocker_text):
                reasons.append(f"preflight_blocker:{blocker_text}")

    preflight = autopilot_summary.get("preflight")
    if isinstance(preflight, dict):
        for check_name in ("dns_doctor", "live_smoke", "ws_state_collect"):
            check_payload = preflight.get(check_name)
            if not isinstance(check_payload, dict):
                continue
            status = _as_status(check_payload.get("status"))
            if _matches_upstream_pattern(status):
                reasons.append(f"{check_name}_status:{status}")

    supervisor_status = _as_status(autopilot_summary.get("supervisor_status"))
    if _matches_upstream_pattern(supervisor_status):
        reasons.append(f"supervisor_status:{supervisor_status}")

    supervisor_summary = autopilot_summary.get("supervisor_summary")
    if isinstance(supervisor_summary, dict):
        cycle_summaries = supervisor_summary.get("cycle_summaries")
        if isinstance(cycle_summaries, list):
            for cycle in cycle_summaries:
                if not isinstance(cycle, dict):
                    continue
                exchange_status = cycle.get("exchange_status")
                if isinstance(exchange_status, dict):
                    if bool(exchange_status.get("dns_error")):
                        reasons.append("exchange_status:dns_error")
                    network_error = str(exchange_status.get("network_error") or "").strip()
                    if _matches_upstream_pattern(network_error):
                        reasons.append(f"exchange_status:network_error:{network_error}")

                final_failure_reasons = cycle.get("final_failure_reasons")
                if isinstance(final_failure_reasons, list):
                    for reason in final_failure_reasons:
                        reason_text = str(reason or "").strip()
                        if reason_text and _matches_upstream_pattern(reason_text):
                            reasons.append(f"supervisor_failure_reason:{reason_text}")

    deduped_reasons = list(dict.fromkeys(reasons))
    return bool(deduped_reasons), deduped_reasons


def _is_healthy_run(autopilot_summary: dict[str, Any]) -> bool:
    return _as_status(autopilot_summary.get("status")) == "ready" and bool(
        autopilot_summary.get("preflight_gate_pass", True)
    )


def _autopilot_has_dns_activity(autopilot_summary: dict[str, Any]) -> bool:
    summary_file = str(autopilot_summary.get("dns_doctor_summary_file") or "").strip()
    if summary_file:
        return True

    preflight = autopilot_summary.get("preflight")
    if isinstance(preflight, dict):
        dns_payload = preflight.get("dns_doctor")
        if isinstance(dns_payload, dict):
            if str(dns_payload.get("status") or "").strip():
                return True
            if str(dns_payload.get("output_file") or "").strip():
                return True

    remediation_runs = autopilot_summary.get("preflight_dns_remediation_runs")
    if isinstance(remediation_runs, list) and remediation_runs:
        return True

    return False


def _load_kill_switch_state(path: Path, *, now: datetime) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "updated_at": now.isoformat(),
        "kill_switch_active": False,
        "kill_switch_reason": None,
        "kill_switch_engaged_at": None,
        "kill_switch_until": None,
        "consecutive_upstream_failures": 0,
        "consecutive_healthy_runs": 0,
        "total_upstream_failures": 0,
        "total_runs": 0,
        "last_upstream_incident_at": None,
        "last_autopilot_status": None,
        "last_autopilot_summary_file": None,
    }
    payload = _load_json(path)
    if payload is None:
        return defaults

    kill_switch_until = _parse_datetime(payload.get("kill_switch_until"))
    kill_switch_engaged_at = _parse_datetime(payload.get("kill_switch_engaged_at"))
    last_upstream_incident_at = _parse_datetime(payload.get("last_upstream_incident_at"))

    defaults.update(
        {
            "kill_switch_active": bool(payload.get("kill_switch_active")),
            "kill_switch_reason": payload.get("kill_switch_reason"),
            "kill_switch_engaged_at": _to_iso(kill_switch_engaged_at),
            "kill_switch_until": _to_iso(kill_switch_until),
            "consecutive_upstream_failures": max(0, _coerce_int(payload.get("consecutive_upstream_failures"), 0)),
            "consecutive_healthy_runs": max(0, _coerce_int(payload.get("consecutive_healthy_runs"), 0)),
            "total_upstream_failures": max(0, _coerce_int(payload.get("total_upstream_failures"), 0)),
            "total_runs": max(0, _coerce_int(payload.get("total_runs"), 0)),
            "last_upstream_incident_at": _to_iso(last_upstream_incident_at),
            "last_autopilot_status": payload.get("last_autopilot_status"),
            "last_autopilot_summary_file": payload.get("last_autopilot_summary_file"),
        }
    )
    return defaults


def run_kalshi_watchdog(
    *,
    env_file: str,
    output_dir: str = "outputs",
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str | None = None,
    ledger_csv: str | None = None,
    book_db_path: str | None = None,
    allow_live_orders: bool = False,
    loops: int = 0,
    sleep_between_loops_seconds: float = 60.0,
    autopilot_cycles: int = 1,
    autopilot_sleep_between_cycles_seconds: float = 20.0,
    timeout_seconds: float = 15.0,
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    contracts_per_order: int = 1,
    max_orders: int = 3,
    min_maker_edge: float = 0.005,
    min_maker_edge_net_fees: float = 0.0,
    max_entry_price_dollars: float = 0.99,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    preflight_run_dns_doctor: bool = True,
    preflight_run_live_smoke: bool = True,
    preflight_run_ws_state_collect: bool = True,
    ws_collect_run_seconds: float = 45.0,
    ws_collect_max_events: int = 250,
    ws_state_json: str | None = None,
    ws_state_max_age_seconds: float = 30.0,
    preflight_self_heal_attempts: int = 2,
    preflight_self_heal_pause_seconds: float = 10.0,
    preflight_self_heal_upstream_only: bool = True,
    preflight_self_heal_run_dns_doctor: bool = True,
    enable_progressive_scaling: bool = True,
    scaling_lookback_runs: int = 20,
    scaling_green_runs_per_step: int = 3,
    scaling_step_live_submissions: int = 1,
    scaling_step_live_cost_dollars: float = 1.0,
    scaling_step_daily_risk_cap_dollars: float = 1.0,
    scaling_hard_max_live_submissions_per_day: int = 12,
    scaling_hard_max_live_cost_per_day_dollars: float = 12.0,
    scaling_hard_max_daily_risk_cap_dollars: float = 12.0,
    upstream_incident_threshold: int = 3,
    kill_switch_cooldown_seconds: float = 1800.0,
    healthy_runs_to_clear_kill_switch: int = 1,
    upstream_retry_backoff_base_seconds: float = 15.0,
    upstream_retry_backoff_max_seconds: float = 300.0,
    self_heal_attempts_per_run: int = 2,
    self_heal_pause_seconds: float = 10.0,
    run_dns_doctor_on_upstream: bool = True,
    kill_switch_state_json: str | None = None,
    autopilot_runner: AutopilotRunner = run_kalshi_autopilot,
    dns_doctor_runner: DnsDoctorRunner = run_dns_doctor,
    sleep_fn: SleepFn = time.sleep,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    effective_history_csv = history_csv or str(out_dir / "kalshi_nonsports_history.csv")
    state_path = Path(kill_switch_state_json) if kill_switch_state_json else out_dir / "kalshi_live_kill_switch_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)

    state = _load_kill_switch_state(state_path, now=captured_at)
    kill_switch_until = _parse_datetime(state.get("kill_switch_until"))
    kill_switch_engaged_at = _parse_datetime(state.get("kill_switch_engaged_at"))
    kill_switch_reason = state.get("kill_switch_reason")
    consecutive_upstream_failures = max(0, _coerce_int(state.get("consecutive_upstream_failures"), 0))
    consecutive_healthy_runs = max(0, _coerce_int(state.get("consecutive_healthy_runs"), 0))
    total_upstream_failures = max(0, _coerce_int(state.get("total_upstream_failures"), 0))
    total_runs = max(0, _coerce_int(state.get("total_runs"), 0))
    last_upstream_incident_at = _parse_datetime(state.get("last_upstream_incident_at"))

    safe_threshold = max(1, int(upstream_incident_threshold))
    safe_cooldown_seconds = max(0.0, float(kill_switch_cooldown_seconds))
    safe_clear_runs = max(1, int(healthy_runs_to_clear_kill_switch))
    safe_sleep_between_loops = max(0.0, float(sleep_between_loops_seconds))
    safe_upstream_backoff_base = max(0.0, float(upstream_retry_backoff_base_seconds))
    safe_upstream_backoff_max = max(safe_upstream_backoff_base, float(upstream_retry_backoff_max_seconds))
    safe_self_heal_attempts = max(0, int(self_heal_attempts_per_run))
    safe_self_heal_pause = max(0.0, float(self_heal_pause_seconds))

    target_loops = int(loops)
    run_forever = target_loops == 0
    if target_loops < 0:
        target_loops = 1
    if target_loops == 0 and not run_forever:
        target_loops = 1
    if target_loops > 0:
        target_loops = max(1, target_loops)

    run_summaries: list[dict[str, Any]] = []
    dns_remediations_attempted = 0
    dns_remediations_skipped_due_autopilot = 0
    kill_switch_engagements = 0
    kill_switch_releases = 0
    elapsed_seconds = 0.0
    interrupted = False

    def _run_autopilot_attempt(*, attempt_started_at: datetime, live_enabled: bool) -> dict[str, Any]:
        return autopilot_runner(
            env_file=env_file,
            output_dir=output_dir,
            priors_csv=priors_csv,
            history_csv=effective_history_csv,
            ledger_csv=ledger_csv,
            book_db_path=book_db_path,
            allow_live_orders=live_enabled,
            cycles=autopilot_cycles,
            sleep_between_cycles_seconds=autopilot_sleep_between_cycles_seconds,
            timeout_seconds=timeout_seconds,
            planning_bankroll_dollars=planning_bankroll_dollars,
            daily_risk_cap_dollars=daily_risk_cap_dollars,
            contracts_per_order=contracts_per_order,
            max_orders=max_orders,
            min_maker_edge=min_maker_edge,
            min_maker_edge_net_fees=min_maker_edge_net_fees,
            max_entry_price_dollars=max_entry_price_dollars,
            max_live_submissions_per_day=max_live_submissions_per_day,
            max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
            preflight_run_dns_doctor=preflight_run_dns_doctor,
            preflight_run_live_smoke=preflight_run_live_smoke,
            preflight_run_ws_state_collect=preflight_run_ws_state_collect,
            ws_collect_run_seconds=ws_collect_run_seconds,
            ws_collect_max_events=ws_collect_max_events,
            ws_state_json=ws_state_json,
            ws_state_max_age_seconds=ws_state_max_age_seconds,
            preflight_self_heal_attempts=preflight_self_heal_attempts,
            preflight_self_heal_pause_seconds=preflight_self_heal_pause_seconds,
            preflight_self_heal_upstream_only=preflight_self_heal_upstream_only,
            preflight_self_heal_run_dns_doctor=preflight_self_heal_run_dns_doctor,
            enable_progressive_scaling=enable_progressive_scaling,
            scaling_lookback_runs=scaling_lookback_runs,
            scaling_green_runs_per_step=scaling_green_runs_per_step,
            scaling_step_live_submissions=scaling_step_live_submissions,
            scaling_step_live_cost_dollars=scaling_step_live_cost_dollars,
            scaling_step_daily_risk_cap_dollars=scaling_step_daily_risk_cap_dollars,
            scaling_hard_max_live_submissions_per_day=scaling_hard_max_live_submissions_per_day,
            scaling_hard_max_live_cost_per_day_dollars=scaling_hard_max_live_cost_per_day_dollars,
            scaling_hard_max_daily_risk_cap_dollars=scaling_hard_max_daily_risk_cap_dollars,
            now=attempt_started_at,
        )

    run_index = 0
    while run_forever or run_index < target_loops:
        run_started_at = captured_at + timedelta(seconds=elapsed_seconds)
        if kill_switch_until is not None and kill_switch_until <= run_started_at:
            kill_switch_until = None
            kill_switch_reason = None
            kill_switch_engaged_at = None

        kill_switch_active_before_run = kill_switch_until is not None and kill_switch_until > run_started_at
        kill_switch_until_before_run = kill_switch_until
        allow_live_orders_effective = bool(allow_live_orders and not kill_switch_active_before_run)
        remediation_dns_summary: dict[str, Any] | None = None
        remediation_dns_runs: list[dict[str, Any]] = []
        remediation_dns_skipped_due_autopilot = 0
        kill_switch_engaged_this_run = False
        kill_switch_released_this_run = False
        autopilot_attempts: list[dict[str, Any]] = []
        autopilot_summary: dict[str, Any] = {}
        upstream_incident = False
        upstream_reasons: list[str] = []
        healthy_run = False
        self_heal_attempts_used = 0
        self_healed = False
        run_inner_elapsed_seconds = 0.0

        while True:
            attempt_started_at = run_started_at + timedelta(seconds=run_inner_elapsed_seconds)
            try:
                autopilot_summary = _run_autopilot_attempt(
                    attempt_started_at=attempt_started_at,
                    live_enabled=allow_live_orders_effective,
                )
            except KeyboardInterrupt:
                interrupted = True
                break
            except Exception as exc:  # pragma: no cover - defensive runtime path
                autopilot_summary = {
                    "status": "error",
                    "error": str(exc),
                    "output_file": None,
                }

            upstream_incident, upstream_reasons = _detect_upstream_incident(autopilot_summary)
            healthy_run = _is_healthy_run(autopilot_summary) and not upstream_incident
            autopilot_attempts.append(
                {
                    "attempt": len(autopilot_attempts) + 1,
                    "started_at": attempt_started_at.isoformat(),
                    "allow_live_orders_effective": allow_live_orders_effective,
                    "autopilot_status": autopilot_summary.get("status"),
                    "autopilot_output_file": autopilot_summary.get("output_file"),
                    "upstream_incident_detected": upstream_incident,
                    "upstream_incident_reasons": upstream_reasons,
                    "healthy_run": healthy_run,
                }
            )
            if not upstream_incident:
                self_healed = self_heal_attempts_used > 0
                break

            if run_dns_doctor_on_upstream:
                if _autopilot_has_dns_activity(autopilot_summary):
                    remediation_dns_skipped_due_autopilot += 1
                    dns_remediations_skipped_due_autopilot += 1
                    remediation_dns_runs.append(
                        {
                            "attempt": len(remediation_dns_runs) + 1,
                            "status": "skipped_already_covered_by_autopilot",
                            "output_file": None,
                        }
                    )
                else:
                    remediation_dns_summary = dns_doctor_runner(
                        env_file=env_file,
                        output_dir=output_dir,
                        timeout_seconds=max(0.25, min(3.0, timeout_seconds / 6.0)),
                    )
                    dns_remediations_attempted += 1
                    remediation_dns_runs.append(
                        {
                            "attempt": len(remediation_dns_runs) + 1,
                            "status": remediation_dns_summary.get("status"),
                            "output_file": remediation_dns_summary.get("output_file"),
                        }
                    )

            if kill_switch_active_before_run or self_heal_attempts_used >= safe_self_heal_attempts:
                break
            self_heal_attempts_used += 1
            if safe_self_heal_pause > 0:
                try:
                    sleep_fn(safe_self_heal_pause)
                except KeyboardInterrupt:
                    interrupted = True
                    break
                run_inner_elapsed_seconds += safe_self_heal_pause

        if interrupted and not autopilot_attempts:
            break

        run_index += 1
        total_runs += 1
        elapsed_seconds += run_inner_elapsed_seconds

        if upstream_incident:
            consecutive_upstream_failures += 1
            consecutive_healthy_runs = 0
            total_upstream_failures += 1
            last_upstream_incident_at = run_started_at

            if (
                safe_cooldown_seconds > 0
                and consecutive_upstream_failures >= safe_threshold
                and not kill_switch_active_before_run
            ):
                kill_switch_until = run_started_at + timedelta(seconds=safe_cooldown_seconds)
                kill_switch_reason = "upstream_incident_threshold"
                kill_switch_engaged_at = run_started_at
                kill_switch_engaged_this_run = True
                kill_switch_engagements += 1
        else:
            consecutive_upstream_failures = 0
            if healthy_run:
                consecutive_healthy_runs += 1
            else:
                consecutive_healthy_runs = 0
            if (
                kill_switch_until is not None
                and kill_switch_until > run_started_at
                and healthy_run
                and consecutive_healthy_runs >= safe_clear_runs
            ):
                kill_switch_until = None
                kill_switch_reason = None
                kill_switch_engaged_at = None
                kill_switch_released_this_run = True
                kill_switch_releases += 1

        if upstream_incident:
            exponent = max(0, consecutive_upstream_failures - 1)
            next_sleep_seconds = min(safe_upstream_backoff_max, safe_upstream_backoff_base * (2**exponent))
        else:
            next_sleep_seconds = safe_sleep_between_loops

        kill_switch_active_after_run = kill_switch_until is not None and kill_switch_until > run_started_at
        run_summaries.append(
            {
                "run": run_index,
                "started_at": run_started_at.isoformat(),
                "allow_live_orders_requested": bool(allow_live_orders),
                "allow_live_orders_effective": allow_live_orders_effective,
                "kill_switch_active_before_run": kill_switch_active_before_run,
                "kill_switch_until_before_run": _to_iso(kill_switch_until_before_run),
                "autopilot_status": autopilot_summary.get("status"),
                "autopilot_output_file": autopilot_summary.get("output_file"),
                "autopilot_attempts_total": len(autopilot_attempts),
                "autopilot_attempts": autopilot_attempts,
                "upstream_incident_detected": upstream_incident,
                "upstream_incident_reasons": upstream_reasons,
                "healthy_run": healthy_run,
                "self_heal_attempts_used": self_heal_attempts_used,
                "self_healed": self_healed,
                "self_heal_pause_seconds": safe_self_heal_pause,
                "consecutive_upstream_failures": consecutive_upstream_failures,
                "consecutive_healthy_runs": consecutive_healthy_runs,
                "kill_switch_engaged": kill_switch_engaged_this_run,
                "kill_switch_released": kill_switch_released_this_run,
                "kill_switch_active_after_run": kill_switch_active_after_run,
                "kill_switch_until_after_run": _to_iso(kill_switch_until),
                "remediation_dns_status": (remediation_dns_summary or {}).get("status"),
                "remediation_dns_output_file": (remediation_dns_summary or {}).get("output_file"),
                "remediation_dns_runs": remediation_dns_runs,
                "remediation_dns_skipped_due_autopilot": remediation_dns_skipped_due_autopilot,
                "sleep_seconds_before_next_run": next_sleep_seconds,
            }
        )

        state_payload = {
            "updated_at": run_started_at.isoformat(),
            "kill_switch_active": kill_switch_active_after_run,
            "kill_switch_reason": kill_switch_reason,
            "kill_switch_engaged_at": _to_iso(kill_switch_engaged_at),
            "kill_switch_until": _to_iso(kill_switch_until),
            "consecutive_upstream_failures": consecutive_upstream_failures,
            "consecutive_healthy_runs": consecutive_healthy_runs,
            "total_upstream_failures": total_upstream_failures,
            "total_runs": total_runs,
            "last_upstream_incident_at": _to_iso(last_upstream_incident_at),
            "last_autopilot_status": autopilot_summary.get("status"),
            "last_autopilot_summary_file": autopilot_summary.get("output_file"),
        }
        state_path.write_text(json.dumps(state_payload, indent=2), encoding="utf-8")

        if interrupted:
            break
        if not run_forever and run_index >= target_loops:
            break
        if next_sleep_seconds > 0:
            try:
                sleep_fn(next_sleep_seconds)
            except KeyboardInterrupt:
                interrupted = True
                break
        elapsed_seconds += max(0.0, next_sleep_seconds)

    final_kill_switch_active = kill_switch_until is not None and kill_switch_until > (
        captured_at + timedelta(seconds=elapsed_seconds)
    )
    if interrupted:
        status = "interrupted"
    elif final_kill_switch_active:
        status = "kill_switch_active"
    elif run_summaries and any(_as_status(item.get("autopilot_status")) == "error" for item in run_summaries):
        status = "degraded"
    else:
        status = "ready"

    summary = {
        "captured_at": captured_at.isoformat(),
        "status": status,
        "env_file": env_file,
        "output_dir": output_dir,
        "loops_requested": loops,
        "loops_run": run_index,
        "run_forever_mode": run_forever,
        "allow_live_orders_requested": bool(allow_live_orders),
        "kill_switch_state_json": str(state_path),
        "kill_switch_active": final_kill_switch_active,
        "kill_switch_reason": kill_switch_reason,
        "kill_switch_until": _to_iso(kill_switch_until),
        "upstream_incident_threshold": safe_threshold,
        "kill_switch_cooldown_seconds": safe_cooldown_seconds,
        "healthy_runs_to_clear_kill_switch": safe_clear_runs,
        "upstream_retry_backoff_base_seconds": safe_upstream_backoff_base,
        "upstream_retry_backoff_max_seconds": safe_upstream_backoff_max,
        "preflight_self_heal_attempts": preflight_self_heal_attempts,
        "preflight_self_heal_pause_seconds": preflight_self_heal_pause_seconds,
        "preflight_self_heal_upstream_only": preflight_self_heal_upstream_only,
        "preflight_self_heal_run_dns_doctor": preflight_self_heal_run_dns_doctor,
        "self_heal_attempts_per_run": safe_self_heal_attempts,
        "self_heal_pause_seconds": safe_self_heal_pause,
        "dns_remediations_attempted": dns_remediations_attempted,
        "dns_remediations_skipped_due_autopilot": dns_remediations_skipped_due_autopilot,
        "kill_switch_engagements": kill_switch_engagements,
        "kill_switch_releases": kill_switch_releases,
        "total_runs_lifetime": total_runs,
        "total_upstream_failures_lifetime": total_upstream_failures,
        "consecutive_upstream_failures": consecutive_upstream_failures,
        "consecutive_healthy_runs": consecutive_healthy_runs,
        "run_summaries": run_summaries,
    }
    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    output_path = out_dir / f"kalshi_watchdog_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
