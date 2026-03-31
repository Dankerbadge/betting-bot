#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export BETBOT_REPO_ROOT="$REPO_ROOT"
export BETBOT_OUTPUT_DIR="${BETBOT_OUTPUT_DIR:-$REPO_ROOT/outputs}"
export BETBOT_ENV_FILE="${BETBOT_ENV_FILE:-$REPO_ROOT/data/research/account_onboarding.env.template}"
export BETBOT_HISTORY_CSV="${BETBOT_HISTORY_CSV:-$BETBOT_OUTPUT_DIR/kalshi_nonsports_history.csv}"
export BETBOT_PRIORS_CSV="${BETBOT_PRIORS_CSV:-$REPO_ROOT/data/research/kalshi_nonsports_priors.csv}"
export BETBOT_WEATHER_LOOKBACK_YEARS="${BETBOT_WEATHER_LOOKBACK_YEARS:-15}"
export BETBOT_WEATHER_PREWARM_MAX_KEYS="${BETBOT_WEATHER_PREWARM_MAX_KEYS:-500}"
export BETBOT_WEATHER_CACHE_MAX_AGE_HOURS="${BETBOT_WEATHER_CACHE_MAX_AGE_HOURS:-24}"
export BETBOT_TIMEOUT_SECONDS="${BETBOT_TIMEOUT_SECONDS:-15}"

cd "$REPO_ROOT"

python3 - <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _file_meta(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "mtime_utc": None,
            "age_seconds": None,
            "size_bytes": None,
        }
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    age = max(0.0, (datetime.now(timezone.utc) - mtime).total_seconds())
    return {
        "path": str(path),
        "exists": True,
        "mtime_utc": mtime.isoformat(),
        "age_seconds": round(age, 3),
        "size_bytes": int(path.stat().st_size),
    }


def _choose_betbot_launcher(repo_root: Path) -> list[str]:
    venv_python = repo_root / ".venv" / "bin" / "python"
    if venv_python.exists() and os.access(venv_python, os.X_OK):
        return [str(venv_python), "-m", "betbot.cli"]
    return [sys.executable, "-m", "betbot.cli"]


def _run_step(
    *,
    name: str,
    launcher: list[str],
    args: list[str],
    cwd: Path,
    run_dir: Path,
) -> dict[str, Any]:
    started_at = _now_iso()
    started_monotonic = time.monotonic()
    stdout_file = run_dir / f"{name}.stdout.json"
    stderr_file = run_dir / f"{name}.stderr.log"

    cmd = launcher + args
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )

    stdout_file.write_text(proc.stdout or "", encoding="utf-8")
    stderr_file.write_text(proc.stderr or "", encoding="utf-8")

    parsed: dict[str, Any] | None = None
    parse_error = None
    if (proc.stdout or "").strip():
        try:
            payload = json.loads(proc.stdout)
            if isinstance(payload, dict):
                parsed = payload
            else:
                parse_error = "stdout_json_not_object"
        except json.JSONDecodeError as exc:
            parse_error = f"stdout_json_decode_error:{exc}"

    finished_at = _now_iso()
    duration_seconds = round(time.monotonic() - started_monotonic, 3)

    step = {
        "name": name,
        "command": cmd,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "duration_seconds": duration_seconds,
        "exit_code": int(proc.returncode),
        "stdout_json_file": str(stdout_file),
        "stderr_log_file": str(stderr_file),
        "stdout_json_parse_error": parse_error,
        "status": (parsed or {}).get("status") if isinstance(parsed, dict) else None,
        "output_file": (parsed or {}).get("output_file") if isinstance(parsed, dict) else None,
        "ok": proc.returncode == 0,
    }

    if isinstance(parsed, dict):
        if name == "micro_status":
            step["top_market_ticker"] = parsed.get("top_market_ticker")
            step["top_market_edge_net_fees"] = parsed.get("top_market_edge_net_fees")
            step["status_reason"] = parsed.get("status_reason")
        elif name == "capture":
            step["scan_status"] = parsed.get("scan_status")
            step["rows_appended"] = parsed.get("rows_appended")
            step["scan_summary_file"] = parsed.get("scan_summary_file")
        elif name == "weather_prewarm":
            step["ready_station_day_keys"] = parsed.get("ready_station_day_keys")
            step["refreshed_station_day_keys"] = parsed.get("refreshed_station_day_keys")
            step["failed_station_day_keys"] = parsed.get("failed_station_day_keys")
        elif name == "prior_trader_dry_run":
            step["allow_live_orders_effective"] = parsed.get("allow_live_orders_effective")
            step["prior_execute_status"] = parsed.get("prior_execute_status")
            step["execution_frontier_status"] = parsed.get("execution_frontier_status")
            step["capture_status"] = parsed.get("capture_status")
            step["prior_trade_gate_status"] = parsed.get("prior_trade_gate_status")

    return step


def main() -> int:
    repo_root = Path(os.environ["BETBOT_REPO_ROOT"])
    output_dir = Path(os.environ["BETBOT_OUTPUT_DIR"])
    env_file = Path(os.environ["BETBOT_ENV_FILE"])
    history_csv = Path(os.environ["BETBOT_HISTORY_CSV"])
    priors_csv = Path(os.environ["BETBOT_PRIORS_CSV"])

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_root = output_dir / "overnight_alpha"
    run_logs = run_root / "logs" / run_stamp
    run_reports = run_root / "reports"
    run_logs.mkdir(parents=True, exist_ok=True)
    run_reports.mkdir(parents=True, exist_ok=True)

    launcher = _choose_betbot_launcher(repo_root)

    started_at = _now_iso()

    steps: list[dict[str, Any]] = []
    steps.append(
        _run_step(
            name="micro_status",
            launcher=launcher,
            args=[
                "kalshi-micro-status",
                "--env-file",
                str(env_file),
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            ],
            cwd=repo_root,
            run_dir=run_logs,
        )
    )
    steps.append(
        _run_step(
            name="capture",
            launcher=launcher,
            args=[
                "kalshi-nonsports-capture",
                "--env-file",
                str(env_file),
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
                "--max-hours-to-close",
                "4000",
                "--page-limit",
                "200",
                "--max-pages",
                "12",
            ],
            cwd=repo_root,
            run_dir=run_logs,
        )
    )
    steps.append(
        _run_step(
            name="weather_prewarm",
            launcher=launcher,
            args=[
                "kalshi-weather-prewarm",
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
                "--historical-lookback-years",
                str(int(os.environ.get("BETBOT_WEATHER_LOOKBACK_YEARS", "15"))),
                "--max-station-day-keys",
                str(int(os.environ.get("BETBOT_WEATHER_PREWARM_MAX_KEYS", "500"))),
                "--station-history-cache-max-age-hours",
                str(float(os.environ.get("BETBOT_WEATHER_CACHE_MAX_AGE_HOURS", "24"))),
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            ],
            cwd=repo_root,
            run_dir=run_logs,
        )
    )
    steps.append(
        _run_step(
            name="prior_trader_dry_run",
            launcher=launcher,
            args=[
                "kalshi-micro-prior-trader",
                "--env-file",
                str(env_file),
                "--priors-csv",
                str(priors_csv),
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
                "--enforce-ws-state-authority",
                "--disable-auto-refresh-weather-priors",
                "--disable-auto-refresh-priors",
            ],
            cwd=repo_root,
            run_dir=run_logs,
        )
    )

    failed_steps = [step["name"] for step in steps if not bool(step.get("ok"))]
    degraded_reasons: list[str] = []

    prior_trader_step = next((step for step in steps if step.get("name") == "prior_trader_dry_run"), None)
    if isinstance(prior_trader_step, dict) and bool(prior_trader_step.get("allow_live_orders_effective")):
        degraded_reasons.append("prior_trader_reported_allow_live_orders_effective=true_in_dry_run_setup")

    overall_status = "ok"
    if failed_steps:
        overall_status = "failed"
    elif degraded_reasons:
        overall_status = "degraded"

    latest_prior_summary_file = ""
    if isinstance(prior_trader_step, dict):
        latest_prior_summary_file = str(prior_trader_step.get("output_file") or "").strip()

    report = {
        "run_started_at_utc": started_at,
        "run_finished_at_utc": _now_iso(),
        "run_stamp_utc": run_stamp,
        "repo_root": str(repo_root),
        "mode": "research_dry_run_only",
        "live_orders_allowed": False,
        "betbot_launcher": launcher,
        "steps": steps,
        "overall_status": overall_status,
        "failed_steps": failed_steps,
        "degraded_reasons": degraded_reasons,
        "freshness": {
            "history_csv": _file_meta(history_csv),
            "priors_csv": _file_meta(priors_csv),
            "ws_state_latest_json": _file_meta(output_dir / "kalshi_ws_state_latest.json"),
            "execution_journal_db": _file_meta(output_dir / "kalshi_execution_journal.sqlite3"),
            "latest_prior_trader_summary": _file_meta(Path(latest_prior_summary_file)) if latest_prior_summary_file else {
                "path": None,
                "exists": False,
                "mtime_utc": None,
                "age_seconds": None,
                "size_bytes": None,
            },
        },
    }

    run_report_path = run_reports / f"overnight_alpha_{run_stamp}.json"
    latest_report_path = output_dir / "overnight_alpha_latest.json"
    run_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    latest_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps({
        "status": overall_status,
        "run_report": str(run_report_path),
        "latest_report": str(latest_report_path),
        "failed_steps": failed_steps,
        "degraded_reasons": degraded_reasons,
    }, indent=2))

    return 0 if overall_status == "ok" else 1


raise SystemExit(main())
PY
