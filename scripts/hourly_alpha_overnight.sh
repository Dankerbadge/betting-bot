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
export BETBOT_FRONTIER_RECENT_ROWS="${BETBOT_FRONTIER_RECENT_ROWS:-20000}"
export BETBOT_FRONTIER_MAX_AGE_SECONDS="${BETBOT_FRONTIER_MAX_AGE_SECONDS:-10800}"
export BETBOT_BALANCE_MAX_AGE_SECONDS="${BETBOT_BALANCE_MAX_AGE_SECONDS:-900}"
export BETBOT_MIN_SECONDS_BETWEEN_RUNS="${BETBOT_MIN_SECONDS_BETWEEN_RUNS:-2700}"

RUN_ROOT="$BETBOT_OUTPUT_DIR/overnight_alpha"
LOCK_DIR="$RUN_ROOT/.hourly_lock"
mkdir -p "$RUN_ROOT"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  python3 - <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path

captured_at = datetime.now(timezone.utc).isoformat()
output_dir = Path(os.environ["BETBOT_OUTPUT_DIR"])
run_root = output_dir / "overnight_alpha"
payload = {
    "run_started_at_utc": captured_at,
    "run_finished_at_utc": captured_at,
    "run_stamp_utc": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
    "mode": "research_dry_run_only",
    "overall_status": "skipped_locked",
    "pipeline_ready": False,
    "live_ready": False,
    "live_blockers": ["scheduler_lock_held"],
    "balance_heartbeat": {
        "status": "not_run",
        "live_ready": False,
        "source": "unknown",
        "balance_dollars": None,
        "cache_age_seconds": None,
        "freshness_threshold_seconds": None,
        "blockers": [],
        "check_error": None,
        "cache_file": None,
    },
    "execution_frontier": {
        "status": "not_run",
        "trusted_bucket_count": 0,
        "untrusted_bucket_count": 0,
        "submitted_orders": 0,
        "filled_orders": 0,
        "fill_samples_with_markout": 0,
        "bucket_markout_sample_counts_by_horizon": {},
        "output_file": None,
    },
    "skip_reason": "skipped_locked",
    "steps": [],
    "failed_steps": [],
    "degraded_reasons": [],
}
(run_root / "last_lock_skip.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
(output_dir / "overnight_alpha_latest.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(
    json.dumps(
        {
            "status": "skipped_locked",
            "latest_report": str(output_dir / "overnight_alpha_latest.json"),
        },
        indent=2,
    )
)
PY
  exit 0
fi

cleanup_lock() {
  rm -f "$LOCK_DIR/owner.json" 2>/dev/null || true
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup_lock EXIT INT TERM
printf '{"pid":%d,"started_at_utc":"%s"}\n' "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$LOCK_DIR/owner.json"

cd "$REPO_ROOT"

python3 - <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


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


def _synthetic_step(
    *,
    name: str,
    status: str,
    ok: bool,
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now_iso = _now_iso()
    step = {
        "name": name,
        "command": [],
        "started_at_utc": now_iso,
        "finished_at_utc": now_iso,
        "duration_seconds": 0.0,
        "exit_code": 0 if ok else 1,
        "stdout_json_file": None,
        "stderr_log_file": None,
        "stdout_json_parse_error": None,
        "status": status,
        "output_file": None,
        "ok": ok,
    }
    if reason:
        step["reason"] = reason
    if payload:
        step.update(payload)
    return step


def _weather_cache_state(*, output_dir: Path, max_age_hours: float) -> dict[str, Any]:
    cache_dir = output_dir / "weather_station_history_cache"
    threshold_seconds = max(0.0, float(max_age_hours)) * 3600.0
    state: dict[str, Any] = {
        "cache_dir": str(cache_dir),
        "cache_exists": cache_dir.exists(),
        "cache_file_count": 0,
        "cache_newest_age_seconds": None,
        "cache_freshness_threshold_seconds": round(threshold_seconds, 3),
        "cache_stale": True,
        "cache_reason": "cache_missing_or_empty",
    }
    if not cache_dir.exists():
        return state
    entries = [path for path in cache_dir.glob("*.json") if path.is_file()]
    state["cache_file_count"] = len(entries)
    if not entries:
        return state
    newest_mtime = max(path.stat().st_mtime for path in entries)
    newest_dt = datetime.fromtimestamp(newest_mtime, tz=timezone.utc)
    newest_age_seconds = max(0.0, (datetime.now(timezone.utc) - newest_dt).total_seconds())
    fresh = newest_age_seconds <= threshold_seconds
    state.update(
        {
            "cache_newest_mtime_utc": newest_dt.isoformat(),
            "cache_newest_age_seconds": round(newest_age_seconds, 3),
            "cache_stale": not fresh,
            "cache_reason": "cache_fresh" if fresh else "cache_stale",
        }
    )
    return state


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
            step["actual_live_balance_dollars"] = parsed.get("actual_live_balance_dollars")
            step["actual_live_balance_source"] = parsed.get("actual_live_balance_source")
            step["balance_live_verified"] = parsed.get("balance_live_verified")
            step["balance_cache_age_seconds"] = parsed.get("balance_cache_age_seconds")
            step["balance_cache_file"] = parsed.get("balance_cache_file")
            step["balance_check_error"] = parsed.get("balance_check_error")
        elif name == "capture":
            step["scan_status"] = parsed.get("scan_status")
            step["rows_appended"] = parsed.get("rows_appended")
            step["scan_summary_file"] = parsed.get("scan_summary_file")
        elif name == "weather_prewarm":
            step["ready_station_day_keys"] = parsed.get("ready_station_day_keys")
            step["refreshed_station_day_keys"] = parsed.get("refreshed_station_day_keys")
            step["failed_station_day_keys"] = parsed.get("failed_station_day_keys")
            step["status_counts"] = parsed.get("status_counts")
            step["station_history_cache_dir"] = parsed.get("station_history_cache_dir")
        elif name == "execution_frontier_refresh":
            trusted = parsed.get("trusted_break_even_edge_by_bucket")
            trust_map = parsed.get("bucket_markout_trust_by_bucket")
            trusted_bucket_count = len(trusted) if isinstance(trusted, dict) else 0
            untrusted_bucket_count = 0
            if isinstance(trust_map, dict):
                untrusted_bucket_count = sum(1 for item in trust_map.values() if not bool((item or {}).get("trusted")))
            step["submitted_orders"] = parsed.get("submitted_orders")
            step["filled_orders"] = parsed.get("filled_orders")
            step["fill_samples_with_markout"] = parsed.get("fill_samples_with_markout")
            step["trusted_bucket_count"] = trusted_bucket_count
            step["untrusted_bucket_count"] = untrusted_bucket_count
            step["bucket_markout_sample_counts_by_horizon"] = parsed.get("bucket_markout_sample_counts_by_horizon")
            step["recommendations"] = parsed.get("recommendations")
        elif name == "prior_trader_dry_run":
            step["allow_live_orders_effective"] = parsed.get("allow_live_orders_effective")
            step["prior_execute_status"] = parsed.get("prior_execute_status")
            step["execution_frontier_status"] = parsed.get("execution_frontier_status")
            step["execution_frontier_report_reference_file"] = (
                parsed.get("execution_frontier_report_reference_file")
                or parsed.get("execution_frontier_break_even_reference_file")
            )
            step["execution_frontier_report_selection_mode"] = (
                parsed.get("execution_frontier_report_selection_mode")
                or parsed.get("execution_frontier_selection_mode")
            )
            step["execution_frontier_report_stale"] = parsed.get("execution_frontier_report_stale")
            step["execution_frontier_report_stale_reason"] = parsed.get("execution_frontier_report_stale_reason")
            step["capture_status"] = parsed.get("capture_status")
            step["prior_trade_gate_status"] = parsed.get("prior_trade_gate_status")
            step["top_market_ticker"] = parsed.get("top_market_ticker")
            step["top_market_contract_family"] = parsed.get("top_market_contract_family")
            step["top_market_weather_history_status"] = parsed.get("top_market_weather_history_status")
            step["top_market_weather_history_live_ready"] = parsed.get("top_market_weather_history_live_ready")
            step["top_market_weather_history_live_ready_reason"] = parsed.get("top_market_weather_history_live_ready_reason")
            step["daily_weather_board_fresh"] = parsed.get("daily_weather_board_fresh")
            step["daily_weather_board_age_seconds"] = parsed.get("daily_weather_board_age_seconds")
            step["daily_weather_markets"] = parsed.get("daily_weather_markets")
            step["daily_weather_markets_with_fresh_snapshot"] = parsed.get("daily_weather_markets_with_fresh_snapshot")
            step["daily_weather_board_freshness_threshold_seconds"] = parsed.get(
                "daily_weather_board_freshness_threshold_seconds"
            )

    return step


def _run_preflight(
    *,
    launcher: list[str],
    cwd: Path,
    run_dir: Path,
) -> dict[str, Any]:
    started_at = _now_iso()
    started_monotonic = time.monotonic()
    stdout_file = run_dir / "preflight.stdout.json"
    stderr_file = run_dir / "preflight.stderr.log"
    stderr_lines: list[str] = []
    checks: list[dict[str, Any]] = []

    required_commands = [
        "kalshi-micro-status",
        "kalshi-nonsports-capture",
        "kalshi-weather-prewarm",
        "kalshi-execution-frontier",
        "kalshi-micro-prior-trader",
    ]
    help_proc = subprocess.run(
        launcher + ["--help"],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    help_text = f"{help_proc.stdout}\n{help_proc.stderr}".lower()
    missing_commands = [command for command in required_commands if command.lower() not in help_text]
    command_ok = help_proc.returncode == 0 and not missing_commands
    checks.append(
        {
            "name": "cli_commands",
            "ok": command_ok,
            "missing_commands": missing_commands,
            "return_code": int(help_proc.returncode),
        }
    )
    if not command_ok:
        stderr_lines.append(
            "cli_commands failed: "
            + (
                f"missing={','.join(missing_commands)}"
                if missing_commands
                else f"help_return_code={help_proc.returncode}"
            )
        )

    python_exe = launcher[0]
    import_proc = subprocess.run(
        [
            python_exe,
            "-c",
            (
                "import betbot.kalshi_micro_execute; "
                "import betbot.kalshi_nonsports_priors; "
                "import betbot.kalshi_weather_priors"
            ),
        ],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    import_ok = import_proc.returncode == 0
    checks.append({"name": "module_imports", "ok": import_ok, "return_code": int(import_proc.returncode)})
    if not import_ok:
        stderr_lines.append(f"module_imports failed: {import_proc.stderr.strip() or import_proc.stdout.strip()}")

    schema_proc = subprocess.run(
        [
            python_exe,
            "-c",
            (
                "from pathlib import Path\n"
                "import tempfile\n"
                "from betbot.kalshi_nonsports_priors import _write_prior_csv\n"
                "tmp = Path(tempfile.mkdtemp()) / 'preflight_priors.csv'\n"
                "_write_prior_csv(tmp, [{'market_ticker': 'PREFLIGHT-TEST', 'contract_family': 'daily_rain'}])\n"
                "assert tmp.exists()\n"
            ),
        ],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    schema_ok = schema_proc.returncode == 0
    checks.append({"name": "priors_csv_schema", "ok": schema_ok, "return_code": int(schema_proc.returncode)})
    if not schema_ok:
        stderr_lines.append(f"priors_csv_schema failed: {schema_proc.stderr.strip() or schema_proc.stdout.strip()}")

    ok = all(bool(check.get("ok")) for check in checks)
    status = "ready" if ok else "failed"

    stdout_file.write_text(json.dumps({"status": status, "checks": checks}, indent=2), encoding="utf-8")
    stderr_file.write_text("\n".join(stderr_lines), encoding="utf-8")

    finished_at = _now_iso()
    duration_seconds = round(time.monotonic() - started_monotonic, 3)
    return {
        "name": "preflight",
        "command": ["preflight:self_test"],
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "duration_seconds": duration_seconds,
        "exit_code": 0 if ok else 1,
        "stdout_json_file": str(stdout_file),
        "stderr_log_file": str(stderr_file),
        "stdout_json_parse_error": None,
        "status": status,
        "output_file": None,
        "ok": ok,
        "checks": checks,
    }


def _build_balance_heartbeat(
    *,
    micro_status_step: dict[str, Any] | None,
    output_dir: Path,
    max_age_seconds: float,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if isinstance(micro_status_step, dict):
        payload = dict(micro_status_step)
        output_file = Path(str(micro_status_step.get("output_file") or ""))
        if output_file.exists():
            summary_payload = _load_json(output_file)
            if isinstance(summary_payload, dict):
                payload = {**summary_payload, **payload}

    balance_dollars = _parse_float(payload.get("actual_live_balance_dollars"))
    source = str(payload.get("actual_live_balance_source") or "unknown").strip().lower() or "unknown"
    live_verified = bool(payload.get("balance_live_verified"))
    cache_age_seconds = _parse_float(payload.get("balance_cache_age_seconds"))
    check_error = str(payload.get("balance_check_error") or "").strip()
    cache_file = Path(str(payload.get("balance_cache_file") or output_dir / "kalshi_live_balance_cache.json"))
    cache_meta = _file_meta(cache_file)
    if cache_age_seconds is None:
        cache_age_seconds = _parse_float(cache_meta.get("age_seconds"))

    freshness_ok = False
    if source == "live":
        freshness_ok = live_verified and not check_error
    elif source in {"cache", "cached", "fallback"}:
        freshness_ok = (cache_age_seconds is not None and cache_age_seconds <= max_age_seconds) and not check_error

    live_ready = bool(
        isinstance(balance_dollars, float)
        and balance_dollars > 0.0
        and freshness_ok
    )

    blockers: list[str] = []
    if check_error:
        blockers.append(f"balance_error:{check_error}")
    if not isinstance(balance_dollars, float):
        blockers.append("balance_unavailable")
    elif balance_dollars <= 0.0:
        blockers.append("balance_nonpositive")
    if not freshness_ok:
        blockers.append("balance_stale_or_unverified")

    status = "ready" if live_ready else "unavailable"
    return _synthetic_step(
        name="balance_heartbeat",
        status=status,
        ok=True,
        payload={
            "balance_dollars": balance_dollars,
            "balance_source": source,
            "balance_live_verified": live_verified,
            "balance_cache_age_seconds": cache_age_seconds,
            "balance_freshness_threshold_seconds": max_age_seconds,
            "balance_cache_file": str(cache_file),
            "balance_cache_file_meta": cache_meta,
            "balance_check_error": check_error or None,
            "balance_live_ready": live_ready,
            "balance_blockers": blockers,
        },
    )


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _top_level_balance_heartbeat(step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(step, dict):
        return {
            "status": "not_run",
            "live_ready": False,
            "source": "unknown",
            "balance_dollars": None,
            "cache_age_seconds": None,
            "freshness_threshold_seconds": None,
            "blockers": [],
            "check_error": None,
            "cache_file": None,
        }
    return {
        "status": str(step.get("status") or "").strip() or "unknown",
        "live_ready": bool(step.get("balance_live_ready")),
        "source": str(step.get("balance_source") or "unknown"),
        "balance_dollars": _parse_float(step.get("balance_dollars")),
        "cache_age_seconds": _parse_float(step.get("balance_cache_age_seconds")),
        "freshness_threshold_seconds": _parse_float(step.get("balance_freshness_threshold_seconds")),
        "blockers": list(step.get("balance_blockers") or []),
        "check_error": step.get("balance_check_error"),
        "cache_file": step.get("balance_cache_file"),
    }


def _top_level_execution_frontier(step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(step, dict):
        return {
            "status": "not_run",
            "trusted_bucket_count": 0,
            "untrusted_bucket_count": 0,
            "submitted_orders": 0,
            "filled_orders": 0,
            "fill_samples_with_markout": 0,
            "bucket_markout_sample_counts_by_horizon": {},
            "output_file": None,
        }
    return {
        "status": str(step.get("status") or "").strip() or "unknown",
        "trusted_bucket_count": int(step.get("trusted_bucket_count") or 0),
        "untrusted_bucket_count": int(step.get("untrusted_bucket_count") or 0),
        "submitted_orders": int(step.get("submitted_orders") or 0),
        "filled_orders": int(step.get("filled_orders") or 0),
        "fill_samples_with_markout": int(step.get("fill_samples_with_markout") or 0),
        "bucket_markout_sample_counts_by_horizon": dict(step.get("bucket_markout_sample_counts_by_horizon") or {}),
        "output_file": step.get("output_file"),
    }


def _write_report(
    *,
    run_report_path: Path,
    latest_report_path: Path,
    report: dict[str, Any],
) -> None:
    run_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    latest_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


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

    run_report_path = run_reports / f"overnight_alpha_{run_stamp}.json"
    latest_report_path = output_dir / "overnight_alpha_latest.json"
    launcher = _choose_betbot_launcher(repo_root)
    started_at = _now_iso()

    min_seconds_between_runs = max(0.0, float(os.environ.get("BETBOT_MIN_SECONDS_BETWEEN_RUNS", "2700")))
    latest_existing = _load_json(latest_report_path)
    if isinstance(latest_existing, dict):
        previous_started = _parse_iso(latest_existing.get("run_started_at_utc"))
        if previous_started is not None:
            age_seconds = max(0.0, (datetime.now(timezone.utc) - previous_started).total_seconds())
            if age_seconds < min_seconds_between_runs:
                report = {
                    "run_started_at_utc": started_at,
                    "run_finished_at_utc": _now_iso(),
                    "run_stamp_utc": run_stamp,
                    "repo_root": str(repo_root),
                    "mode": "research_dry_run_only",
                    "live_orders_allowed": False,
                    "betbot_launcher": launcher,
                    "steps": [
                        _synthetic_step(
                            name="run_interval_guard",
                            status="skipped_recent_run",
                            ok=True,
                            reason="previous_run_too_recent",
                            payload={
                                "previous_run_started_at_utc": previous_started.isoformat(),
                                "previous_run_age_seconds": round(age_seconds, 3),
                                "min_seconds_between_runs": round(min_seconds_between_runs, 3),
                            },
                        )
                    ],
                    "overall_status": "skipped_recent_run",
                    "failed_steps": [],
                    "degraded_reasons": [],
                    "pipeline_ready": False,
                    "live_ready": False,
                    "live_blockers": ["run_skipped_recent_run"],
                    "balance_heartbeat": _top_level_balance_heartbeat(None),
                    "execution_frontier": _top_level_execution_frontier(None),
                    "skip_reason": "skipped_recent_run",
                    "freshness": {
                        "history_csv": _file_meta(history_csv),
                        "priors_csv": _file_meta(priors_csv),
                        "ws_state_latest_json": _file_meta(output_dir / "kalshi_ws_state_latest.json"),
                        "execution_journal_db": _file_meta(output_dir / "kalshi_execution_journal.sqlite3"),
                        "latest_prior_trader_summary": {
                            "path": None,
                            "exists": False,
                            "mtime_utc": None,
                            "age_seconds": None,
                            "size_bytes": None,
                        },
                    },
                }
                _write_report(run_report_path=run_report_path, latest_report_path=latest_report_path, report=report)
                print(
                    json.dumps(
                        {
                            "status": report["overall_status"],
                            "run_report": str(run_report_path),
                            "latest_report": str(latest_report_path),
                            "failed_steps": [],
                            "degraded_reasons": [],
                        },
                        indent=2,
                    )
                )
                return 0

    steps: list[dict[str, Any]] = []
    preflight_step = _run_preflight(launcher=launcher, cwd=repo_root, run_dir=run_logs)
    steps.append(preflight_step)
    if not bool(preflight_step.get("ok")):
        failed_steps = [step["name"] for step in steps if not bool(step.get("ok"))]
        report = {
            "run_started_at_utc": started_at,
            "run_finished_at_utc": _now_iso(),
            "run_stamp_utc": run_stamp,
            "repo_root": str(repo_root),
            "mode": "research_dry_run_only",
            "live_orders_allowed": False,
            "betbot_launcher": launcher,
            "steps": steps,
            "overall_status": "failed",
            "failed_steps": failed_steps,
            "degraded_reasons": [],
            "pipeline_ready": False,
            "live_ready": False,
            "live_blockers": ["preflight_failed"],
            "balance_heartbeat": _top_level_balance_heartbeat(None),
            "execution_frontier": _top_level_execution_frontier(None),
            "freshness": {
                "history_csv": _file_meta(history_csv),
                "priors_csv": _file_meta(priors_csv),
                "ws_state_latest_json": _file_meta(output_dir / "kalshi_ws_state_latest.json"),
                "execution_journal_db": _file_meta(output_dir / "kalshi_execution_journal.sqlite3"),
                "latest_prior_trader_summary": {
                    "path": None,
                    "exists": False,
                    "mtime_utc": None,
                    "age_seconds": None,
                    "size_bytes": None,
                },
            },
        }
        _write_report(run_report_path=run_report_path, latest_report_path=latest_report_path, report=report)
        print(
            json.dumps(
                {
                    "status": report["overall_status"],
                    "run_report": str(run_report_path),
                    "latest_report": str(latest_report_path),
                    "failed_steps": failed_steps,
                    "degraded_reasons": [],
                },
                indent=2,
            )
        )
        return 1

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

    weather_cache = _weather_cache_state(
        output_dir=output_dir,
        max_age_hours=float(os.environ.get("BETBOT_WEATHER_CACHE_MAX_AGE_HOURS", "24")),
    )
    if bool(weather_cache.get("cache_stale")):
        weather_step = _run_step(
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
        weather_step["cache_state_before"] = weather_cache
        steps.append(weather_step)
    else:
        steps.append(
            _synthetic_step(
                name="weather_prewarm",
                status="skipped_fresh_cache",
                ok=True,
                reason="weather_station_history_cache_fresh",
                payload={"cache_state_before": weather_cache},
            )
        )

    frontier_refresh_step = _run_step(
        name="execution_frontier_refresh",
        launcher=launcher,
        args=[
            "kalshi-execution-frontier",
            "--output-dir",
            str(output_dir),
            "--journal-db-path",
            str(output_dir / "kalshi_execution_journal.sqlite3"),
            "--recent-rows",
            str(max(1, int(os.environ.get("BETBOT_FRONTIER_RECENT_ROWS", "20000")))),
        ],
        cwd=repo_root,
        run_dir=run_logs,
    )
    steps.append(frontier_refresh_step)

    micro_status_step = next((step for step in steps if step.get("name") == "micro_status"), None)
    steps.append(
        _build_balance_heartbeat(
            micro_status_step=micro_status_step if isinstance(micro_status_step, dict) else None,
            output_dir=output_dir,
            max_age_seconds=max(0.0, float(os.environ.get("BETBOT_BALANCE_MAX_AGE_SECONDS", "900"))),
        )
    )

    prior_trader_args = [
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
    ]
    frontier_report_path = str(frontier_refresh_step.get("output_file") or "").strip()
    if bool(frontier_refresh_step.get("ok")) and frontier_report_path and Path(frontier_report_path).exists():
        prior_trader_args.extend(
            [
                "--execution-frontier-report-json",
                frontier_report_path,
                "--execution-frontier-max-report-age-seconds",
                str(max(0.0, float(os.environ.get("BETBOT_FRONTIER_MAX_AGE_SECONDS", "10800")))),
            ]
        )

    steps.append(
        _run_step(
            name="prior_trader_dry_run",
            launcher=launcher,
            args=prior_trader_args,
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

    balance_step = next((step for step in steps if step.get("name") == "balance_heartbeat"), None)
    frontier_step = next((step for step in steps if step.get("name") == "execution_frontier_refresh"), None)

    live_blockers: list[str] = []
    if isinstance(balance_step, dict) and not bool(balance_step.get("balance_live_ready")):
        live_blockers.extend(list(balance_step.get("balance_blockers") or []))
    if isinstance(frontier_step, dict):
        frontier_status = str(frontier_step.get("status") or "").strip().lower()
        if frontier_status and frontier_status != "ready":
            live_blockers.append(f"execution_frontier_{frontier_status}")
    if isinstance(prior_trader_step, dict):
        gate_status = str(prior_trader_step.get("prior_trade_gate_status") or "").strip().lower()
        if gate_status and gate_status not in {"gate_pass", "pass", "ok"}:
            live_blockers.append(f"prior_trade_gate_{gate_status}")
        capture_status = str(prior_trader_step.get("capture_status") or "").strip().lower()
        if capture_status and capture_status not in {"ready", "ok"}:
            live_blockers.append(f"capture_{capture_status}")
    live_blockers = _dedupe(live_blockers)
    pipeline_ready = overall_status == "ok"
    live_ready = pipeline_ready and not live_blockers
    top_level_balance = _top_level_balance_heartbeat(balance_step if isinstance(balance_step, dict) else None)
    top_level_frontier = _top_level_execution_frontier(frontier_step if isinstance(frontier_step, dict) else None)

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
        "pipeline_ready": pipeline_ready,
        "live_ready": live_ready,
        "live_blockers": live_blockers,
        "balance_heartbeat": top_level_balance,
        "execution_frontier": top_level_frontier,
        "freshness": {
            "history_csv": _file_meta(history_csv),
            "priors_csv": _file_meta(priors_csv),
            "ws_state_latest_json": _file_meta(output_dir / "kalshi_ws_state_latest.json"),
            "execution_journal_db": _file_meta(output_dir / "kalshi_execution_journal.sqlite3"),
            "latest_prior_trader_summary": _file_meta(Path(latest_prior_summary_file))
            if latest_prior_summary_file
            else {
                "path": None,
                "exists": False,
                "mtime_utc": None,
                "age_seconds": None,
                "size_bytes": None,
            },
        },
    }

    _write_report(run_report_path=run_report_path, latest_report_path=latest_report_path, report=report)

    print(
        json.dumps(
            {
                "status": overall_status,
                "pipeline_ready": pipeline_ready,
                "live_ready": live_ready,
                "live_blockers": live_blockers,
                "run_report": str(run_report_path),
                "latest_report": str(latest_report_path),
                "failed_steps": failed_steps,
                "degraded_reasons": degraded_reasons,
            },
            indent=2,
        )
    )

    return 0 if overall_status == "ok" else 1


raise SystemExit(main())
PY
