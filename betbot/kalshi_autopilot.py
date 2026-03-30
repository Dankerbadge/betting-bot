from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from betbot.dns_guard import run_dns_doctor
from betbot.kalshi_supervisor import run_kalshi_supervisor
from betbot.kalshi_ws_state import DEFAULT_WS_CHANNELS, run_kalshi_ws_state_collect
from betbot.live_smoke import run_live_smoke


DnsDoctorRunner = Callable[..., dict[str, Any]]
LiveSmokeRunner = Callable[..., dict[str, Any]]
WsCollectRunner = Callable[..., dict[str, Any]]
SupervisorRunner = Callable[..., dict[str, Any]]


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


def _is_green_autopilot_run(payload: dict[str, Any]) -> bool:
    if _as_status(payload.get("status")) != "ready":
        return False
    if not bool(payload.get("effective_allow_live_orders")):
        return False
    if not bool(payload.get("preflight_gate_pass")):
        return False
    if _as_status(payload.get("supervisor_status")) != "ready":
        return False
    if int(payload.get("cycles_with_failures") or 0) > 0:
        return False
    if int(payload.get("cycles_with_unremediated_failures") or 0) > 0:
        return False
    return True


def _recent_autopilot_runs(
    *,
    output_dir: Path,
    lookback: int,
) -> list[dict[str, Any]]:
    paths = sorted(
        output_dir.glob("kalshi_autopilot_summary_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    runs: list[dict[str, Any]] = []
    for path in paths:
        payload = _load_json(path)
        if payload is None:
            continue
        runs.append(payload)
        if len(runs) >= max(0, lookback):
            break
    return runs


def _count_consecutive_green_runs(runs: list[dict[str, Any]]) -> int:
    streak = 0
    for run in runs:
        if _is_green_autopilot_run(run):
            streak += 1
            continue
        break
    return streak


def run_kalshi_autopilot(
    *,
    env_file: str,
    output_dir: str = "outputs",
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str | None = None,
    ledger_csv: str | None = None,
    book_db_path: str | None = None,
    allow_live_orders: bool = False,
    cycles: int = 1,
    sleep_between_cycles_seconds: float = 20.0,
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
    enable_progressive_scaling: bool = True,
    scaling_lookback_runs: int = 20,
    scaling_green_runs_per_step: int = 3,
    scaling_step_live_submissions: int = 1,
    scaling_step_live_cost_dollars: float = 1.0,
    scaling_step_daily_risk_cap_dollars: float = 1.0,
    scaling_hard_max_live_submissions_per_day: int = 12,
    scaling_hard_max_live_cost_per_day_dollars: float = 12.0,
    scaling_hard_max_daily_risk_cap_dollars: float = 12.0,
    dns_doctor_runner: DnsDoctorRunner = run_dns_doctor,
    live_smoke_runner: LiveSmokeRunner = run_live_smoke,
    ws_collect_runner: WsCollectRunner = run_kalshi_ws_state_collect,
    supervisor_runner: SupervisorRunner = run_kalshi_supervisor,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    effective_history_csv = history_csv or str(out_dir / "kalshi_nonsports_history.csv")

    preflight: dict[str, Any] = {}
    preflight_gate_pass = True
    preflight_blockers: list[str] = []

    dns_summary: dict[str, Any] | None = None
    if preflight_run_dns_doctor:
        dns_summary = dns_doctor_runner(
            env_file=env_file,
            output_dir=output_dir,
            timeout_seconds=max(0.25, min(3.0, timeout_seconds / 6.0)),
        )
        dns_status = _as_status(dns_summary.get("status"))
        if dns_status == "failed":
            preflight_gate_pass = False
            preflight_blockers.append("dns_doctor_failed")
        preflight["dns_doctor"] = {
            "status": dns_summary.get("status"),
            "output_file": dns_summary.get("output_file"),
            "hosts_checked": dns_summary.get("hosts_checked"),
            "hosts_healthy": dns_summary.get("hosts_healthy"),
        }

    smoke_summary: dict[str, Any] | None = None
    if preflight_run_live_smoke:
        smoke_summary = live_smoke_runner(
            env_file=env_file,
            output_dir=output_dir,
            timeout_seconds=timeout_seconds,
            include_odds_provider_check=True,
        )
        smoke_status = _as_status(smoke_summary.get("status"))
        if smoke_status != "passed":
            preflight_gate_pass = False
            preflight_blockers.append(f"live_smoke_{smoke_status or 'failed'}")
        preflight["live_smoke"] = {
            "status": smoke_summary.get("status"),
            "checks_failed": smoke_summary.get("checks_failed"),
            "output_file": smoke_summary.get("output_file"),
        }

    ws_collect_summary: dict[str, Any] | None = None
    effective_ws_state_json = ws_state_json
    if preflight_run_ws_state_collect:
        ws_collect_summary = ws_collect_runner(
            env_file=env_file,
            channels=DEFAULT_WS_CHANNELS,
            output_dir=output_dir,
            ws_state_json=ws_state_json,
            max_staleness_seconds=ws_state_max_age_seconds,
            run_seconds=ws_collect_run_seconds,
            max_events=ws_collect_max_events,
        )
        ws_status = _as_status(ws_collect_summary.get("status"))
        if ws_status != "ready":
            preflight_gate_pass = False
            preflight_blockers.append(f"ws_state_{ws_status or 'failed'}")
        if isinstance(ws_collect_summary.get("ws_state_json"), str) and ws_collect_summary.get("ws_state_json"):
            effective_ws_state_json = str(ws_collect_summary.get("ws_state_json"))
        preflight["ws_state_collect"] = {
            "status": ws_collect_summary.get("status"),
            "gate_pass": ws_collect_summary.get("gate_pass"),
            "events_logged": ws_collect_summary.get("events_logged"),
            "ws_url_used": ws_collect_summary.get("ws_url_used"),
            "output_file": ws_collect_summary.get("output_file"),
            "ws_state_json": ws_collect_summary.get("ws_state_json"),
        }

    recent_runs = _recent_autopilot_runs(output_dir=out_dir, lookback=scaling_lookback_runs)
    consecutive_green_runs = _count_consecutive_green_runs(recent_runs)
    green_runs_per_step = max(1, int(scaling_green_runs_per_step))
    scaling_steps = 0
    if enable_progressive_scaling:
        scaling_steps = consecutive_green_runs // green_runs_per_step

    derived_max_live_submissions = max_live_submissions_per_day
    derived_max_live_cost = max_live_cost_per_day_dollars
    derived_daily_risk_cap = daily_risk_cap_dollars
    if scaling_steps > 0:
        derived_max_live_submissions = min(
            max(0, int(max_live_submissions_per_day))
            + scaling_steps * max(0, int(scaling_step_live_submissions)),
            max(0, int(scaling_hard_max_live_submissions_per_day)),
        )
        derived_max_live_cost = min(
            max(0.0, float(max_live_cost_per_day_dollars))
            + scaling_steps * max(0.0, float(scaling_step_live_cost_dollars)),
            max(0.0, float(scaling_hard_max_live_cost_per_day_dollars)),
        )
        derived_daily_risk_cap = min(
            max(0.0, float(daily_risk_cap_dollars))
            + scaling_steps * max(0.0, float(scaling_step_daily_risk_cap_dollars)),
            max(0.0, float(scaling_hard_max_daily_risk_cap_dollars)),
        )

    effective_allow_live_orders = bool(allow_live_orders and preflight_gate_pass)
    supervisor_summary = supervisor_runner(
        env_file=env_file,
        output_dir=output_dir,
        priors_csv=priors_csv,
        history_csv=effective_history_csv,
        ledger_csv=ledger_csv,
        book_db_path=book_db_path,
        cycles=cycles,
        sleep_between_cycles_seconds=sleep_between_cycles_seconds,
        timeout_seconds=timeout_seconds,
        allow_live_orders=effective_allow_live_orders,
        planning_bankroll_dollars=planning_bankroll_dollars,
        daily_risk_cap_dollars=derived_daily_risk_cap,
        contracts_per_order=contracts_per_order,
        max_orders=max_orders,
        min_maker_edge=min_maker_edge,
        min_maker_edge_net_fees=min_maker_edge_net_fees,
        max_entry_price_dollars=max_entry_price_dollars,
        max_live_submissions_per_day=derived_max_live_submissions,
        max_live_cost_per_day_dollars=derived_max_live_cost,
        enforce_ws_state_authority=True,
        ws_state_json=effective_ws_state_json,
        ws_state_max_age_seconds=ws_state_max_age_seconds,
    )

    supervisor_status = _as_status(supervisor_summary.get("status"))
    if supervisor_status == "ready" and preflight_gate_pass:
        status = "ready"
    elif supervisor_status == "ready" and not preflight_gate_pass:
        status = "guarded_dry_run"
    else:
        status = "degraded"

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": env_file,
        "output_dir": output_dir,
        "status": status,
        "allow_live_orders_requested": bool(allow_live_orders),
        "effective_allow_live_orders": effective_allow_live_orders,
        "preflight_gate_pass": preflight_gate_pass,
        "preflight_blockers": preflight_blockers,
        "preflight": preflight,
        "effective_ws_state_json": effective_ws_state_json,
        "enable_progressive_scaling": enable_progressive_scaling,
        "scaling_lookback_runs": scaling_lookback_runs,
        "scaling_green_runs_per_step": green_runs_per_step,
        "consecutive_green_runs": consecutive_green_runs,
        "scaling_steps_applied": scaling_steps,
        "base_max_live_submissions_per_day": max_live_submissions_per_day,
        "base_max_live_cost_per_day_dollars": max_live_cost_per_day_dollars,
        "base_daily_risk_cap_dollars": daily_risk_cap_dollars,
        "effective_max_live_submissions_per_day": derived_max_live_submissions,
        "effective_max_live_cost_per_day_dollars": round(float(derived_max_live_cost), 6),
        "effective_daily_risk_cap_dollars": round(float(derived_daily_risk_cap), 6),
        "supervisor_status": supervisor_summary.get("status"),
        "cycles_with_failures": supervisor_summary.get("cycles_with_failures"),
        "cycles_with_unremediated_failures": supervisor_summary.get("cycles_with_unremediated_failures"),
        "supervisor_summary_file": supervisor_summary.get("output_file"),
        "supervisor_summary": supervisor_summary,
        "dns_doctor_summary_file": (dns_summary or {}).get("output_file"),
        "live_smoke_summary_file": (smoke_summary or {}).get("output_file"),
        "ws_state_collect_summary_file": (ws_collect_summary or {}).get("output_file"),
    }
    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    output_path = out_dir / f"kalshi_autopilot_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
