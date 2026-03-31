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
export BETBOT_WEATHER_PRIOR_MAX_AGE_HOURS="${BETBOT_WEATHER_PRIOR_MAX_AGE_HOURS:-6}"
export BETBOT_WEATHER_PRIOR_MAX_MARKETS="${BETBOT_WEATHER_PRIOR_MAX_MARKETS:-30}"
export BETBOT_WEATHER_ALLOWED_CONTRACT_FAMILIES="${BETBOT_WEATHER_ALLOWED_CONTRACT_FAMILIES:-daily_rain,daily_temperature}"
export BETBOT_TIMEOUT_SECONDS="${BETBOT_TIMEOUT_SECONDS:-15}"
export BETBOT_FRONTIER_RECENT_ROWS="${BETBOT_FRONTIER_RECENT_ROWS:-20000}"
export BETBOT_FRONTIER_MAX_AGE_SECONDS="${BETBOT_FRONTIER_MAX_AGE_SECONDS:-10800}"
export BETBOT_BALANCE_MAX_AGE_SECONDS="${BETBOT_BALANCE_MAX_AGE_SECONDS:-900}"
export BETBOT_BALANCE_SMOKE_ON_FAILURE="${BETBOT_BALANCE_SMOKE_ON_FAILURE:-1}"
export BETBOT_MIN_SECONDS_BETWEEN_RUNS="${BETBOT_MIN_SECONDS_BETWEEN_RUNS:-2700}"

RUN_ROOT="$BETBOT_OUTPUT_DIR/overnight_alpha"
LOCK_DIR="$RUN_ROOT/.hourly_lock"
mkdir -p "$RUN_ROOT"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  python3 - <<'PY'
from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path

try:
    from betbot.runtime_version import build_runtime_version_block
except Exception:  # pragma: no cover - best effort metadata only
    build_runtime_version_block = None  # type: ignore[assignment]

captured_at = datetime.now(timezone.utc).isoformat()
run_started_dt = datetime.now(timezone.utc)
output_dir = Path(os.environ["BETBOT_OUTPUT_DIR"])
run_root = output_dir / "overnight_alpha"
payload = {
    "run_id": f"hourly_alpha_overnight::{run_started_dt.strftime('%Y%m%d_%H%M%S_%f')[:-3]}",
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
    "top_market_ticker": None,
    "top_market_contract_family": None,
    "fair_yes_probability_raw": None,
    "execution_probability_guarded": None,
    "fill_probability_source": None,
    "empirical_fill_weight": None,
    "heuristic_fill_weight": None,
    "probe_lane_used": None,
    "probe_reason": None,
    "skip_reason": "skipped_locked",
    "steps": [],
    "failed_steps": [],
    "degraded_reasons": [],
}
if callable(build_runtime_version_block):
    payload["runtime_version"] = build_runtime_version_block(
        run_started_at=run_started_dt,
        run_id=f"hourly_alpha_overnight::{run_started_dt.strftime('%Y%m%d_%H%M%S_%f')[:-3]}",
        git_cwd=Path(os.environ.get("BETBOT_REPO_ROOT") or "."),
        as_of=run_started_dt,
    )
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

import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import Any

try:
    from betbot.runtime_version import build_runtime_version_block
except Exception:  # pragma: no cover - best effort metadata only
    build_runtime_version_block = None  # type: ignore[assignment]


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


def _parse_csv_list(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    items = [part.strip() for part in text.split(",")]
    return [item for item in items if item]


def _weather_prior_state(
    *,
    priors_csv: Path,
    max_age_hours: float,
    allowed_contract_families: list[str],
) -> dict[str, Any]:
    threshold_seconds = max(0.0, float(max_age_hours)) * 3600.0
    meta = _file_meta(priors_csv)
    state: dict[str, Any] = {
        "priors_csv": str(priors_csv),
        "priors_exists": bool(meta.get("exists")),
        "priors_mtime_utc": meta.get("mtime_utc"),
        "priors_age_seconds": meta.get("age_seconds"),
        "priors_freshness_threshold_seconds": round(threshold_seconds, 3),
        "allowed_contract_families": list(allowed_contract_families),
        "contract_family_counts": {},
        "allowed_family_counts": {},
        "allowed_rows_total": 0,
        "stale": True,
        "reason": "priors_missing",
        "parse_error": None,
    }
    if not bool(meta.get("exists")):
        return state
    age_seconds = _parse_float(meta.get("age_seconds"))
    if age_seconds is not None and age_seconds > threshold_seconds:
        state["reason"] = "priors_stale"
    else:
        state["reason"] = "no_allowed_weather_priors"

    counts: dict[str, int] = {}
    try:
        with priors_csv.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                family = str((row or {}).get("contract_family") or "").strip()
                if not family:
                    continue
                counts[family] = counts.get(family, 0) + 1
    except Exception as exc:  # pragma: no cover - best effort diagnostics
        state["parse_error"] = str(exc)
        state["reason"] = "priors_parse_error"
        return state

    state["contract_family_counts"] = counts
    allowed_counts = {family: int(counts.get(family, 0)) for family in allowed_contract_families}
    state["allowed_family_counts"] = allowed_counts
    allowed_rows_total = sum(allowed_counts.values())
    state["allowed_rows_total"] = int(allowed_rows_total)

    if state["reason"] == "priors_stale":
        state["stale"] = True
    elif allowed_rows_total <= 0:
        state["stale"] = True
        state["reason"] = "no_allowed_weather_priors"
    else:
        state["stale"] = False
        state["reason"] = "weather_priors_fresh"
    return state


def _classify_balance_smoke_failure(*, message: Any, http_status: Any) -> str:
    text = str(message or "").strip().lower()
    status_text = str(http_status or "").strip()
    if "missing" in text and ("credential" in text or "environment" in text):
        return "missing_credentials"
    if "dns" in text:
        return "dns_failure"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if status_text in {"401", "403"} or "unauthorized" in text or "forbidden" in text:
        return "auth_failed"
    if "connection" in text or "network" in text:
        return "network_failure"
    if status_text:
        return f"http_{status_text}"
    return "unknown"


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
        elif name == "weather_prior_refresh":
            step["generated_priors"] = parsed.get("generated_priors")
            step["candidate_markets"] = parsed.get("candidate_markets")
            step["inserted_rows"] = parsed.get("inserted_rows")
            step["updated_rows"] = parsed.get("updated_rows")
            step["manual_rows_protected"] = parsed.get("manual_rows_protected")
            step["contract_family_generated_counts"] = parsed.get("contract_family_generated_counts")
            step["station_history_status_counts"] = parsed.get("station_history_status_counts")
            step["top_market_ticker"] = parsed.get("top_market_ticker")
            step["top_market_confidence"] = parsed.get("top_market_confidence")
        elif name == "balance_smoke":
            checks = parsed.get("checks")
            step["checks_total"] = parsed.get("checks_total")
            step["checks_failed"] = parsed.get("checks_failed")
            step["smoke_status"] = parsed.get("status")
            step["kalshi_ok"] = None
            step["kalshi_message"] = None
            step["kalshi_http_status"] = None
            step["kalshi_failure_kind"] = None
            if isinstance(checks, list):
                kalshi_check = next(
                    (
                        item
                        for item in checks
                        if isinstance(item, dict) and str(item.get("component") or "").strip().lower() == "kalshi"
                    ),
                    None,
                )
                if isinstance(kalshi_check, dict):
                    step["kalshi_ok"] = bool(kalshi_check.get("ok"))
                    step["kalshi_message"] = kalshi_check.get("message")
                    step["kalshi_http_status"] = kalshi_check.get("http_status")
                    step["kalshi_failure_kind"] = _classify_balance_smoke_failure(
                        message=kalshi_check.get("message"),
                        http_status=kalshi_check.get("http_status"),
                    )
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
            step["frontier_artifact_path"] = parsed.get("frontier_artifact_path")
            step["frontier_artifact_sha256"] = parsed.get("frontier_artifact_sha256")
            step["frontier_artifact_file_sha256"] = parsed.get("frontier_artifact_file_sha256")
            step["frontier_artifact_payload_sha256"] = parsed.get("frontier_artifact_payload_sha256")
            step["frontier_artifact_as_of_utc"] = parsed.get("frontier_artifact_as_of_utc")
            step["frontier_artifact_age_seconds"] = parsed.get("frontier_artifact_age_seconds")
            step["frontier_selection_mode"] = parsed.get("frontier_selection_mode")
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
            step["rain_model_tag"] = parsed.get("rain_model_tag")
            step["temperature_model_tag"] = parsed.get("temperature_model_tag")
            step["weather_priors_version"] = parsed.get("weather_priors_version")
            step["fill_model_mode"] = parsed.get("fill_model_mode")
            step["execution_empirical_fill_model_prefer_empirical"] = parsed.get(
                "execution_empirical_fill_model_prefer_empirical"
            )
            step["history_csv_path"] = parsed.get("history_csv_path")
            step["history_csv_mtime_utc"] = parsed.get("history_csv_mtime_utc")
            step["weather_station_history_cache_age_seconds"] = parsed.get("weather_station_history_cache_age_seconds")
            step["balance_heartbeat_age_seconds"] = parsed.get("balance_heartbeat_age_seconds")
            step["fair_yes_probability_raw"] = (
                parsed.get("fair_yes_probability_raw")
                or parsed.get("top_market_fair_probability_raw")
            )
            step["execution_probability_guarded"] = (
                parsed.get("execution_probability_guarded")
                or parsed.get("top_market_execution_probability_guarded")
            )
            step["fill_probability_source"] = parsed.get("fill_probability_source")
            step["empirical_fill_weight"] = parsed.get("empirical_fill_weight")
            step["heuristic_fill_weight"] = parsed.get("heuristic_fill_weight")
            step["probe_lane_used"] = parsed.get("probe_lane_used")
            step["probe_reason"] = parsed.get("probe_reason")
            step["frontier_artifact_path"] = parsed.get("frontier_artifact_path")
            step["frontier_artifact_sha256"] = parsed.get("frontier_artifact_sha256")
            step["frontier_artifact_file_sha256"] = parsed.get("frontier_artifact_file_sha256")
            step["frontier_artifact_payload_sha256"] = parsed.get("frontier_artifact_payload_sha256")
            step["frontier_artifact_as_of_utc"] = parsed.get("frontier_artifact_as_of_utc")
            step["frontier_artifact_age_seconds"] = parsed.get("frontier_artifact_age_seconds")

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
        "live-smoke",
        "kalshi-weather-priors",
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


def _runtime_version_for_report(
    *,
    run_started_at: str,
    run_id: str,
    repo_root: Path,
    prior_trader_step: dict[str, Any] | None,
    frontier_step: dict[str, Any] | None,
    as_of: datetime,
) -> dict[str, Any]:
    if not callable(build_runtime_version_block):
        return {
            "git_sha": "unknown",
            "git_branch": "unknown",
            "git_dirty": None,
            "run_id": run_id,
            "run_started_at_utc": run_started_at,
            "rain_model_tag": None,
            "temperature_model_tag": None,
            "fill_model_mode": None,
            "prefer_empirical_fill_model": None,
            "weather_priors_version": None,
            "frontier_artifact_path": None,
            "frontier_artifact_sha256": None,
            "frontier_artifact_file_sha256": None,
            "frontier_artifact_payload_sha256": None,
            "frontier_artifact_as_of_utc": None,
            "frontier_artifact_age_seconds": None,
            "frontier_selection_mode": None,
            "frontier_trusted_bucket_count": 0,
            "frontier_untrusted_bucket_count": 0,
        }
    frontier_path = None
    frontier_selection_mode = None
    if isinstance(prior_trader_step, dict):
        frontier_path = (
            prior_trader_step.get("execution_frontier_report_reference_file")
            or prior_trader_step.get("execution_frontier_break_even_reference_file")
            or prior_trader_step.get("frontier_artifact_path")
        )
        frontier_selection_mode = (
            prior_trader_step.get("execution_frontier_report_selection_mode")
            or prior_trader_step.get("execution_frontier_selection_mode")
        )
    if not frontier_path and isinstance(frontier_step, dict):
        frontier_path = frontier_step.get("output_file") or frontier_step.get("frontier_artifact_path")
    if not frontier_selection_mode and isinstance(frontier_step, dict):
        frontier_selection_mode = frontier_step.get("frontier_selection_mode") or "self_generated"
    return build_runtime_version_block(
        run_started_at=run_started_at,
        run_id=run_id,
        git_cwd=repo_root,
        rain_model_tag=(
            prior_trader_step.get("rain_model_tag")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        temperature_model_tag=(
            prior_trader_step.get("temperature_model_tag")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        fill_model_mode=(
            prior_trader_step.get("fill_model_mode")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        prefer_empirical_fill_model=(
            prior_trader_step.get("execution_empirical_fill_model_prefer_empirical")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        weather_priors_version_name=(
            prior_trader_step.get("weather_priors_version")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        frontier_artifact_path=frontier_path,
        frontier_selection_mode=frontier_selection_mode,
        as_of=as_of,
    )


def _top_level_decision_identity(step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(step, dict):
        return {
            "top_market_ticker": None,
            "top_market_contract_family": None,
            "fair_yes_probability_raw": None,
            "execution_probability_guarded": None,
            "fill_probability_source": None,
            "empirical_fill_weight": None,
            "heuristic_fill_weight": None,
            "probe_lane_used": None,
            "probe_reason": None,
        }
    return {
        "top_market_ticker": step.get("top_market_ticker"),
        "top_market_contract_family": step.get("top_market_contract_family"),
        "fair_yes_probability_raw": _parse_float(step.get("fair_yes_probability_raw")),
        "execution_probability_guarded": _parse_float(step.get("execution_probability_guarded")),
        "fill_probability_source": step.get("fill_probability_source"),
        "empirical_fill_weight": _parse_float(step.get("empirical_fill_weight")),
        "heuristic_fill_weight": _parse_float(step.get("heuristic_fill_weight")),
        "probe_lane_used": step.get("probe_lane_used"),
        "probe_reason": step.get("probe_reason"),
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
    run_id = f"hourly_alpha_overnight::{run_stamp}"
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
                    "run_id": run_id,
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
                    "decision_identity": _top_level_decision_identity(None),
                    "top_market_ticker": None,
                    "top_market_contract_family": None,
                    "fair_yes_probability_raw": None,
                    "execution_probability_guarded": None,
                    "fill_probability_source": None,
                    "empirical_fill_weight": None,
                    "heuristic_fill_weight": None,
                    "probe_lane_used": None,
                    "probe_reason": None,
                    "history_csv_path": str(history_csv),
                    "history_csv_mtime_utc": _file_meta(history_csv).get("mtime_utc"),
                    "daily_weather_board_age_seconds": None,
                    "weather_station_history_cache_age_seconds": None,
                    "balance_heartbeat_age_seconds": None,
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
                report["runtime_version"] = _runtime_version_for_report(
                    run_started_at=started_at,
                    run_id=run_id,
                    repo_root=repo_root,
                    prior_trader_step=None,
                    frontier_step=None,
                    as_of=datetime.now(timezone.utc),
                )
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
            "run_id": run_id,
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
            "decision_identity": _top_level_decision_identity(None),
            "top_market_ticker": None,
            "top_market_contract_family": None,
            "fair_yes_probability_raw": None,
            "execution_probability_guarded": None,
            "fill_probability_source": None,
            "empirical_fill_weight": None,
            "heuristic_fill_weight": None,
            "probe_lane_used": None,
            "probe_reason": None,
            "history_csv_path": str(history_csv),
            "history_csv_mtime_utc": _file_meta(history_csv).get("mtime_utc"),
            "daily_weather_board_age_seconds": None,
            "weather_station_history_cache_age_seconds": None,
            "balance_heartbeat_age_seconds": None,
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
        report["runtime_version"] = _runtime_version_for_report(
            run_started_at=started_at,
            run_id=run_id,
            repo_root=repo_root,
            prior_trader_step=None,
            frontier_step=None,
            as_of=datetime.now(timezone.utc),
        )
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

    allowed_weather_contract_families = _parse_csv_list(
        os.environ.get("BETBOT_WEATHER_ALLOWED_CONTRACT_FAMILIES")
    )
    if not allowed_weather_contract_families:
        allowed_weather_contract_families = ["daily_rain", "daily_temperature"]
    weather_prior_state_before = _weather_prior_state(
        priors_csv=priors_csv,
        max_age_hours=float(os.environ.get("BETBOT_WEATHER_PRIOR_MAX_AGE_HOURS", "6")),
        allowed_contract_families=allowed_weather_contract_families,
    )
    if bool(weather_prior_state_before.get("stale")):
        weather_prior_step = _run_step(
            name="weather_prior_refresh",
            launcher=launcher,
            args=[
                "kalshi-weather-priors",
                "--priors-csv",
                str(priors_csv),
                "--history-csv",
                str(history_csv),
                "--allowed-contract-families",
                ",".join(allowed_weather_contract_families),
                "--max-markets",
                str(max(1, int(os.environ.get("BETBOT_WEATHER_PRIOR_MAX_MARKETS", "30")))),
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
                "--historical-lookback-years",
                str(int(os.environ.get("BETBOT_WEATHER_LOOKBACK_YEARS", "15"))),
                "--station-history-cache-max-age-hours",
                str(float(os.environ.get("BETBOT_WEATHER_CACHE_MAX_AGE_HOURS", "24"))),
                "--output-dir",
                str(output_dir),
            ],
            cwd=repo_root,
            run_dir=run_logs,
        )
        weather_prior_step["prior_state_before"] = weather_prior_state_before
        steps.append(weather_prior_step)
    else:
        steps.append(
            _synthetic_step(
                name="weather_prior_refresh",
                status="skipped_fresh_weather_priors",
                ok=True,
                reason=str(weather_prior_state_before.get("reason") or "weather_priors_fresh"),
                payload={"prior_state_before": weather_prior_state_before},
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
    balance_step_initial = _build_balance_heartbeat(
        micro_status_step=micro_status_step if isinstance(micro_status_step, dict) else None,
        output_dir=output_dir,
        max_age_seconds=max(0.0, float(os.environ.get("BETBOT_BALANCE_MAX_AGE_SECONDS", "900"))),
    )
    steps.append(balance_step_initial)

    balance_smoke_enabled = str(os.environ.get("BETBOT_BALANCE_SMOKE_ON_FAILURE", "1")).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    if balance_smoke_enabled and not bool(balance_step_initial.get("balance_live_ready")):
        balance_smoke_step = _run_step(
            name="balance_smoke",
            launcher=launcher,
            args=[
                "live-smoke",
                "--env-file",
                str(env_file),
                "--skip-odds-provider-check",
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
                "--output-dir",
                str(output_dir),
            ],
            cwd=repo_root,
            run_dir=run_logs,
        )
        # Balance smoke is diagnostic: a failing smoke should enrich blockers,
        # not mark the whole orchestration step as failed.
        balance_smoke_step["ok"] = True
        balance_smoke_step["diagnostic_only"] = True
        steps.append(balance_smoke_step)
    elif balance_smoke_enabled:
        steps.append(
            _synthetic_step(
                name="balance_smoke",
                status="skipped_balance_ready",
                ok=True,
                reason="balance_live_ready",
            )
        )
    else:
        steps.append(
            _synthetic_step(
                name="balance_smoke",
                status="skipped_disabled",
                ok=True,
                reason="balance_smoke_disabled",
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
    balance_smoke_step = next((step for step in steps if step.get("name") == "balance_smoke"), None)
    frontier_step = next((step for step in steps if step.get("name") == "execution_frontier_refresh"), None)
    weather_prior_step = next((step for step in steps if step.get("name") == "weather_prior_refresh"), None)
    weather_prior_state_after = _weather_prior_state(
        priors_csv=priors_csv,
        max_age_hours=float(os.environ.get("BETBOT_WEATHER_PRIOR_MAX_AGE_HOURS", "6")),
        allowed_contract_families=allowed_weather_contract_families,
    )

    live_blockers: list[str] = []
    if isinstance(balance_step, dict) and not bool(balance_step.get("balance_live_ready")):
        live_blockers.extend(list(balance_step.get("balance_blockers") or []))
    if isinstance(balance_smoke_step, dict) and not bool(balance_smoke_step.get("kalshi_ok")):
        failure_kind = str(balance_smoke_step.get("kalshi_failure_kind") or "").strip()
        if failure_kind:
            live_blockers.append(f"balance_smoke_{failure_kind}")
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
    if bool(weather_prior_state_after.get("stale")):
        live_blockers.append("weather_priors_stale_or_empty")
    live_blockers = _dedupe(live_blockers)
    pipeline_ready = overall_status == "ok"
    live_ready = pipeline_ready and not live_blockers
    top_level_balance = _top_level_balance_heartbeat(balance_step if isinstance(balance_step, dict) else None)
    top_level_frontier = _top_level_execution_frontier(frontier_step if isinstance(frontier_step, dict) else None)
    decision_identity = _top_level_decision_identity(prior_trader_step if isinstance(prior_trader_step, dict) else None)
    runtime_version = _runtime_version_for_report(
        run_started_at=started_at,
        run_id=run_id,
        repo_root=repo_root,
        prior_trader_step=prior_trader_step if isinstance(prior_trader_step, dict) else None,
        frontier_step=frontier_step if isinstance(frontier_step, dict) else None,
        as_of=datetime.now(timezone.utc),
    )

    latest_prior_summary_file = ""
    if isinstance(prior_trader_step, dict):
        latest_prior_summary_file = str(prior_trader_step.get("output_file") or "").strip()

    report = {
        "run_id": run_id,
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
        "balance_smoke_status": (
            balance_smoke_step.get("smoke_status")
            if isinstance(balance_smoke_step, dict)
            else None
        ),
        "balance_smoke_failure_kind": (
            balance_smoke_step.get("kalshi_failure_kind")
            if isinstance(balance_smoke_step, dict)
            else None
        ),
        "balance_smoke_http_status": (
            balance_smoke_step.get("kalshi_http_status")
            if isinstance(balance_smoke_step, dict)
            else None
        ),
        "balance_smoke_message": (
            balance_smoke_step.get("kalshi_message")
            if isinstance(balance_smoke_step, dict)
            else None
        ),
        "execution_frontier": top_level_frontier,
        "decision_identity": decision_identity,
        "top_market_ticker": decision_identity.get("top_market_ticker"),
        "top_market_contract_family": decision_identity.get("top_market_contract_family"),
        "fair_yes_probability_raw": decision_identity.get("fair_yes_probability_raw"),
        "execution_probability_guarded": decision_identity.get("execution_probability_guarded"),
        "fill_probability_source": decision_identity.get("fill_probability_source"),
        "empirical_fill_weight": decision_identity.get("empirical_fill_weight"),
        "heuristic_fill_weight": decision_identity.get("heuristic_fill_weight"),
        "probe_lane_used": decision_identity.get("probe_lane_used"),
        "probe_reason": decision_identity.get("probe_reason"),
        "rain_model_tag": (
            prior_trader_step.get("rain_model_tag")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "temperature_model_tag": (
            prior_trader_step.get("temperature_model_tag")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "fill_model_mode": (
            prior_trader_step.get("fill_model_mode")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "weather_prior_refresh_status": (
            weather_prior_step.get("status")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_prior_refresh_reason": (
            weather_prior_step.get("reason")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_prior_allowed_contract_families": list(allowed_weather_contract_families),
        "weather_prior_allowed_family_counts": weather_prior_state_after.get("allowed_family_counts"),
        "weather_prior_allowed_rows_total": weather_prior_state_after.get("allowed_rows_total"),
        "weather_prior_state_reason": weather_prior_state_after.get("reason"),
        "weather_prior_stale": bool(weather_prior_state_after.get("stale")),
        "weather_prior_age_seconds": weather_prior_state_after.get("priors_age_seconds"),
        "prefer_empirical_fill_model": (
            prior_trader_step.get("execution_empirical_fill_model_prefer_empirical")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "weather_priors_version": (
            prior_trader_step.get("weather_priors_version")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "frontier_artifact_path": runtime_version.get("frontier_artifact_path"),
        "frontier_artifact_sha256": runtime_version.get("frontier_artifact_sha256"),
        "frontier_artifact_file_sha256": runtime_version.get("frontier_artifact_file_sha256"),
        "frontier_artifact_payload_sha256": runtime_version.get("frontier_artifact_payload_sha256"),
        "frontier_artifact_as_of_utc": runtime_version.get("frontier_artifact_as_of_utc"),
        "frontier_artifact_age_seconds": runtime_version.get("frontier_artifact_age_seconds"),
        "frontier_selection_mode": runtime_version.get("frontier_selection_mode"),
        "frontier_trusted_bucket_count": runtime_version.get("frontier_trusted_bucket_count"),
        "frontier_untrusted_bucket_count": runtime_version.get("frontier_untrusted_bucket_count"),
        "history_csv_path": str(history_csv),
        "history_csv_mtime_utc": _file_meta(history_csv).get("mtime_utc"),
        "daily_weather_board_age_seconds": (
            prior_trader_step.get("daily_weather_board_age_seconds")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "weather_station_history_cache_age_seconds": (
            prior_trader_step.get("weather_station_history_cache_age_seconds")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "balance_heartbeat_age_seconds": (
            prior_trader_step.get("balance_heartbeat_age_seconds")
            if isinstance(prior_trader_step, dict)
            else None
        ),
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
        "runtime_version": runtime_version,
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
