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
export BETBOT_WEATHER_CDO_TOKEN_FILE="${BETBOT_WEATHER_CDO_TOKEN_FILE:-$REPO_ROOT/.secrets/noaa_cdo_token.txt}"
export BETBOT_TIMEOUT_SECONDS="${BETBOT_TIMEOUT_SECONDS:-15}"
export BETBOT_CAPTURE_MAX_HOURS_TO_CLOSE="${BETBOT_CAPTURE_MAX_HOURS_TO_CLOSE:-4000}"
export BETBOT_CAPTURE_PAGE_LIMIT="${BETBOT_CAPTURE_PAGE_LIMIT:-200}"
export BETBOT_CAPTURE_MAX_PAGES="${BETBOT_CAPTURE_MAX_PAGES:-12}"
export BETBOT_DAILY_WEATHER_RECOVERY_MAX_HOURS_TO_CLOSE="${BETBOT_DAILY_WEATHER_RECOVERY_MAX_HOURS_TO_CLOSE:-6000}"
export BETBOT_DAILY_WEATHER_RECOVERY_PAGE_LIMIT="${BETBOT_DAILY_WEATHER_RECOVERY_PAGE_LIMIT:-300}"
export BETBOT_DAILY_WEATHER_RECOVERY_MAX_PAGES="${BETBOT_DAILY_WEATHER_RECOVERY_MAX_PAGES:-24}"
export BETBOT_FRONTIER_RECENT_ROWS="${BETBOT_FRONTIER_RECENT_ROWS:-20000}"
export BETBOT_FRONTIER_MAX_AGE_SECONDS="${BETBOT_FRONTIER_MAX_AGE_SECONDS:-10800}"
export BETBOT_BALANCE_MAX_AGE_SECONDS="${BETBOT_BALANCE_MAX_AGE_SECONDS:-900}"
export BETBOT_BALANCE_SMOKE_ON_FAILURE="${BETBOT_BALANCE_SMOKE_ON_FAILURE:-1}"
export BETBOT_DAILY_WEATHER_STALE_RECOVERY_ENABLED="${BETBOT_DAILY_WEATHER_STALE_RECOVERY_ENABLED:-1}"
export BETBOT_DAILY_WEATHER_STALE_RECOVERY_MAX_RETRIES="${BETBOT_DAILY_WEATHER_STALE_RECOVERY_MAX_RETRIES:-1}"
export BETBOT_DAILY_WEATHER_STALE_RECOVERY_SLEEP_SECONDS="${BETBOT_DAILY_WEATHER_STALE_RECOVERY_SLEEP_SECONDS:-2}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_ENABLED="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_ENABLED:-1}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_MAX_MARKETS="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_MAX_MARKETS:-20}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_ON_BASE_CAPTURE="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_ON_BASE_CAPTURE:-1}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_MAX_MARKETS="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_MAX_MARKETS:-4}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_INTERVAL_RUNS="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_INTERVAL_RUNS:-3}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_STATE_FILE="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_STATE_FILE:-$BETBOT_OUTPUT_DIR/overnight_alpha/daily_weather_ticker_refresh_state.json}"
export BETBOT_DAILY_WEATHER_WAKEUP_BURST_ENABLED="${BETBOT_DAILY_WEATHER_WAKEUP_BURST_ENABLED:-1}"
export BETBOT_DAILY_WEATHER_WAKEUP_BURST_MAX_MARKETS="${BETBOT_DAILY_WEATHER_WAKEUP_BURST_MAX_MARKETS:-4}"
export BETBOT_DAILY_WEATHER_WAKEUP_BURST_SLEEP_SECONDS="${BETBOT_DAILY_WEATHER_WAKEUP_BURST_SLEEP_SECONDS:-1}"
export BETBOT_DAILY_WEATHER_WAKEUP_REPRIORITIZE_ENABLED="${BETBOT_DAILY_WEATHER_WAKEUP_REPRIORITIZE_ENABLED:-1}"
export BETBOT_DAILY_WEATHER_RECOVERY_ALERT_WINDOW_HOURS="${BETBOT_DAILY_WEATHER_RECOVERY_ALERT_WINDOW_HOURS:-6}"
export BETBOT_DAILY_WEATHER_RECOVERY_ALERT_THRESHOLD="${BETBOT_DAILY_WEATHER_RECOVERY_ALERT_THRESHOLD:-3}"
export BETBOT_DAILY_WEATHER_RECOVERY_ALERT_MAX_EVENTS="${BETBOT_DAILY_WEATHER_RECOVERY_ALERT_MAX_EVENTS:-500}"
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
    "prior_trade_gate_status": None,
    "prior_trade_gate_blockers": None,
    "no_candidates_diagnostics": None,
    "probe_policy": {
        "enable_untrusted_bucket_probe_exploration": None,
        "untrusted_bucket_probe_exploration_enabled": None,
        "untrusted_bucket_probe_max_orders_per_run": None,
        "untrusted_bucket_probe_required_edge_buffer_dollars": None,
        "untrusted_bucket_probe_contracts_cap": None,
        "untrusted_bucket_probe_submitted_attempts": None,
        "untrusted_bucket_probe_blocked_attempts": None,
        "untrusted_bucket_probe_reason_counts": None,
    },
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
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

try:
    from betbot.runtime_version import build_runtime_version_block
except Exception:  # pragma: no cover - best effort metadata only
    build_runtime_version_block = None  # type: ignore[assignment]
try:
    from betbot.onboarding import _is_placeholder as _onboarding_is_placeholder
    from betbot.onboarding import _parse_env_file as _onboarding_parse_env_file
except Exception:  # pragma: no cover - fallback parser for control-plane only
    _onboarding_is_placeholder = None  # type: ignore[assignment]
    _onboarding_parse_env_file = None  # type: ignore[assignment]
try:
    from betbot.dns_guard import urlopen_with_dns_recovery as _urlopen_with_dns_recovery
except Exception:  # pragma: no cover - fallback for constrained runtime
    _urlopen_with_dns_recovery = None  # type: ignore[assignment]
try:
    from betbot.kalshi_nonsports_capture import HISTORY_FIELDNAMES as _capture_history_fieldnames
    from betbot.kalshi_nonsports_capture import _append_history as _capture_append_history
except Exception:  # pragma: no cover - fallback when module unavailable
    _capture_history_fieldnames = []
    _capture_append_history = None  # type: ignore[assignment]


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


def _coerce_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return int(default)
    try:
        return int(float(text))
    except ValueError:
        return int(default)


def _minutes_since_iso(value: Any, *, now_utc: datetime) -> float | None:
    timestamp = _parse_iso(value)
    if not isinstance(timestamp, datetime):
        return None
    return max(0.0, (now_utc - timestamp).total_seconds() / 60.0)


def _daily_weather_watch_priority_score(
    *,
    now_utc: datetime,
    run_counter: int,
    ticker_stats: dict[str, Any] | None,
    prior_hours_to_close: float | None,
) -> tuple[float, dict[str, Any]]:
    stats = dict(ticker_stats or {})
    wakeup_count = max(0, _coerce_int(stats.get("wakeup_count"), 0))
    endpoint_only_streak_count = max(0, _coerce_int(stats.get("endpoint_only_streak_count"), 0))
    minutes_since_last_wakeup = _minutes_since_iso(stats.get("last_wakeup_at_utc"), now_utc=now_utc)
    minutes_since_last_non_endpoint = _minutes_since_iso(stats.get("last_non_endpoint_at_utc"), now_utc=now_utc)
    had_orderable_side_ever = bool(stats.get("had_orderable_side_ever"))
    last_watch_selected_run_counter = max(0, _coerce_int(stats.get("last_watch_selected_run_counter"), 0))
    runs_since_last_watch_selected = (
        max(0, int(run_counter) - int(last_watch_selected_run_counter))
        if last_watch_selected_run_counter > 0
        else int(run_counter)
    )
    hours_to_close = (
        prior_hours_to_close
        if isinstance(prior_hours_to_close, float)
        else _parse_float(stats.get("last_hours_to_close"))
    )

    score = 0.0
    score += min(8.0, float(wakeup_count) * 1.5)
    if isinstance(minutes_since_last_non_endpoint, float):
        # Recent non-endpoint activity is the strongest wake-up predictor.
        score += max(0.0, 10.0 - min(minutes_since_last_non_endpoint, 600.0) / 60.0)
    if had_orderable_side_ever:
        score += 4.0
    if isinstance(hours_to_close, float) and hours_to_close > 0.0:
        score += max(0.0, 4.0 - min(hours_to_close, 96.0) / 24.0)
    score += min(3.0, max(0.0, float(runs_since_last_watch_selected)) / 4.0)
    score -= min(5.0, float(endpoint_only_streak_count) * 0.35)

    components = {
        "wakeup_count": wakeup_count,
        "minutes_since_last_wakeup": (
            round(float(minutes_since_last_wakeup), 3)
            if isinstance(minutes_since_last_wakeup, float)
            else None
        ),
        "minutes_since_last_non_endpoint": (
            round(float(minutes_since_last_non_endpoint), 3)
            if isinstance(minutes_since_last_non_endpoint, float)
            else None
        ),
        "hours_to_close": round(float(hours_to_close), 6) if isinstance(hours_to_close, float) else None,
        "had_orderable_side_ever": had_orderable_side_ever,
        "endpoint_only_streak_count": endpoint_only_streak_count,
        "runs_since_last_watch_selected": runs_since_last_watch_selected,
    }
    return round(score, 6), components


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _parse_env_values(path: Path) -> dict[str, str]:
    if callable(_onboarding_parse_env_file):
        return dict(_onboarding_parse_env_file(path))
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _is_placeholder_value(value: Any) -> bool:
    if callable(_onboarding_is_placeholder):
        return bool(_onboarding_is_placeholder(None if value is None else str(value)))
    raw = str(value or "").strip()
    if not raw:
        return True
    return raw.upper().startswith("TODO")


def _env_has_kalshi_credentials(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, "env_file_missing"
    try:
        data = _parse_env_values(path)
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"env_parse_error:{exc}"
    env_name = str(data.get("KALSHI_ENV") or "").strip().lower()
    if env_name not in {"demo", "prod", "production"}:
        return False, "kalshi_env_invalid"
    access_key = data.get("KALSHI_ACCESS_KEY_ID")
    if _is_placeholder_value(access_key):
        return False, "kalshi_access_key_missing"
    private_key_path_raw = str(data.get("KALSHI_PRIVATE_KEY_PATH") or "").strip()
    if _is_placeholder_value(private_key_path_raw):
        return False, "kalshi_private_key_path_missing"
    private_key_path = Path(private_key_path_raw).expanduser()
    if not private_key_path.is_absolute():
        private_key_path = (path.parent / private_key_path).resolve()
    if not private_key_path.exists():
        return False, "kalshi_private_key_file_missing"
    return True, "kalshi_credentials_ready"


def _resolve_env_file(*, requested_path: Path, repo_root: Path) -> dict[str, Any]:
    requested = requested_path.expanduser()
    local_candidate = repo_root / "data" / "research" / "account_onboarding.local.env"
    template_candidate = repo_root / "data" / "research" / "account_onboarding.env.template"
    requested_ready, requested_reason = _env_has_kalshi_credentials(requested)
    effective = requested
    source = "requested"
    resolution_reason = requested_reason
    if (not requested_ready) and local_candidate != requested:
        local_ready, local_reason = _env_has_kalshi_credentials(local_candidate)
        if local_ready:
            effective = local_candidate
            source = "auto_local_override"
            resolution_reason = f"requested_{requested_reason}; using_local_{local_reason}"
    if not effective.exists() and template_candidate.exists():
        effective = template_candidate
        if source == "requested":
            source = "fallback_template"
        resolution_reason = "effective_env_missing_fallback_template"
    effective_ready, effective_reason = _env_has_kalshi_credentials(effective)
    return {
        "env_file_requested": str(requested),
        "env_file_effective": str(effective),
        "env_file_source": source,
        "env_file_resolution_reason": resolution_reason,
        "env_file_requested_kalshi_ready": bool(requested_ready),
        "env_file_requested_kalshi_ready_reason": requested_reason,
        "env_file_kalshi_ready": bool(effective_ready),
        "env_file_kalshi_ready_reason": effective_reason,
    }


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


def _is_enabled(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    return text not in {"0", "false", "no", "off"}


def _latest_step_with_prefix(steps: list[dict[str, Any]], prefix: str) -> dict[str, Any] | None:
    for step in reversed(steps):
        if str(step.get("name") or "").startswith(prefix):
            return step
    return None


def _is_daily_weather_board_stale_gate(step: dict[str, Any] | None) -> bool:
    if not isinstance(step, dict):
        return False
    gate_status = str(step.get("prior_trade_gate_status") or "").strip().lower()
    return gate_status == "daily_weather_board_stale"


def _kalshi_api_roots_for_env(env_values: dict[str, str]) -> tuple[str, ...]:
    env_name = str(env_values.get("KALSHI_ENV") or "prod").strip().lower()
    if env_name == "demo":
        return ("https://demo-api.kalshi.co/trade-api/v2",)
    if env_name in {"prod", "production"}:
        return (
            "https://api.elections.kalshi.com/trade-api/v2",
            "https://trading-api.kalshi.com/trade-api/v2",
        )
    return ("https://api.elections.kalshi.com/trade-api/v2",)


def _http_get_json_url(url: str, timeout_seconds: float) -> tuple[int, Any]:
    request = Request(
        url=url,
        headers={
            "Accept": "application/json",
            "User-Agent": "betbot-hourly-overnight/1.0",
        },
        method="GET",
    )
    try:
        if callable(_urlopen_with_dns_recovery):
            with _urlopen_with_dns_recovery(
                request,
                timeout_seconds=max(1.0, float(timeout_seconds)),
                urlopen_fn=urlopen,
            ) as response:
                status = int(getattr(response, "status", 200) or 200)
                payload_text = response.read().decode("utf-8", errors="replace")
        else:
            with urlopen(request, timeout=max(1.0, float(timeout_seconds))) as response:
                status = int(getattr(response, "status", 200) or 200)
                payload_text = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        status = int(exc.code or 0)
        payload_text = exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        return 599, {"error": str(exc), "error_type": type(exc).__name__}
    try:
        return status, json.loads(payload_text)
    except json.JSONDecodeError:
        return status, {"raw_text": payload_text[:400]}


def _is_orderable_price(price: float | None) -> bool:
    return isinstance(price, float) and 0.0 < price < 1.0


def _is_endpoint_quote(price: float | None) -> bool:
    return isinstance(price, float) and (price <= 0.0 or price >= 1.0)


def _classify_daily_weather_lane_from_quotes(
    *,
    yes_bid: float | None,
    no_bid: float | None,
    yes_ask: float | None,
    no_ask: float | None,
) -> dict[str, Any]:
    quotes_present = [value for value in (yes_bid, no_bid, yes_ask, no_ask) if isinstance(value, float)]
    has_orderable_bid = _is_orderable_price(yes_bid) or _is_orderable_price(no_bid)
    has_orderable_ask = _is_orderable_price(yes_ask) or _is_orderable_price(no_ask)
    has_orderable_side = has_orderable_bid or has_orderable_ask
    endpoint_only = (
        bool(quotes_present)
        and not has_orderable_side
        and all(_is_endpoint_quote(value) for value in quotes_present)
    )
    lane = "watch_endpoint_only" if endpoint_only else "tradable"
    return {
        "lane": lane,
        "quotes_present_count": len(quotes_present),
        "has_orderable_bid": has_orderable_bid,
        "has_orderable_ask": has_orderable_ask,
        "has_orderable_side": has_orderable_side,
        "endpoint_only": endpoint_only,
    }


def _load_daily_weather_refresh_state(path: Path) -> dict[str, Any]:
    payload = _load_json(path)
    if not isinstance(payload, dict):
        return {
            "run_counter": 0,
            "watch_cursor": 0,
            "lane_by_ticker": {},
            "has_orderable_side_by_ticker": {},
            "ticker_stats_by_ticker": {},
        }
    lane_by_ticker = payload.get("lane_by_ticker")
    if not isinstance(lane_by_ticker, dict):
        lane_by_ticker = payload.get("ticker_lane_map")
    if not isinstance(lane_by_ticker, dict):
        lane_by_ticker = {}
    has_orderable_side_by_ticker = payload.get("has_orderable_side_by_ticker")
    if not isinstance(has_orderable_side_by_ticker, dict):
        has_orderable_side_by_ticker = payload.get("ticker_orderable_map")
    if not isinstance(has_orderable_side_by_ticker, dict):
        has_orderable_side_by_ticker = {}
    ticker_stats_by_ticker = payload.get("ticker_stats_by_ticker")
    if not isinstance(ticker_stats_by_ticker, dict):
        ticker_stats_by_ticker = {}
    run_counter_raw = payload.get("run_counter")
    watch_cursor_raw = payload.get("watch_cursor")
    run_counter = int(run_counter_raw) if isinstance(run_counter_raw, (int, float)) else 0
    watch_cursor = int(watch_cursor_raw) if isinstance(watch_cursor_raw, (int, float)) else 0
    return {
        "run_counter": max(0, run_counter),
        "watch_cursor": max(0, watch_cursor),
        "lane_by_ticker": lane_by_ticker,
        "has_orderable_side_by_ticker": has_orderable_side_by_ticker,
        "ticker_stats_by_ticker": ticker_stats_by_ticker,
    }


def _write_daily_weather_refresh_state(path: Path, payload: dict[str, Any]) -> str | None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return None
    except Exception as exc:  # pragma: no cover - best effort persistence
        return str(exc)


def _classify_daily_weather_lane_from_prior_row(row: dict[str, Any]) -> dict[str, Any]:
    yes_bid = _parse_float(row.get("latest_yes_bid_dollars"))
    no_bid = _parse_float(row.get("latest_no_bid_dollars"))
    yes_ask = _parse_float(row.get("latest_yes_ask_dollars"))
    no_ask = _parse_float(row.get("latest_no_ask_dollars"))
    if yes_bid is None:
        yes_bid = _parse_float(row.get("yes_bid_dollars"))
    if no_bid is None:
        no_bid = _parse_float(row.get("no_bid_dollars"))
    if yes_ask is None:
        yes_ask = _parse_float(row.get("yes_ask_dollars"))
    if no_ask is None:
        no_ask = _parse_float(row.get("no_ask_dollars"))
    return _classify_daily_weather_lane_from_quotes(
        yes_bid=yes_bid,
        no_bid=no_bid,
        yes_ask=yes_ask,
        no_ask=no_ask,
    )


def _load_daily_weather_tickers_from_priors(
    *,
    priors_csv: Path,
    allowed_contract_families: list[str],
    max_markets: int,
) -> dict[str, Any]:
    if not priors_csv.exists():
        return {
            "selected_tickers": [],
            "tradable_tickers": [],
            "watch_tickers": [],
            "ranked_watch_tickers": [],
            "watch_hours_to_close_by_ticker": {},
            "watch_tickers_selected": [],
            "lane_by_ticker": {},
            "has_orderable_side_by_ticker": {},
            "ticker_stats_by_ticker": {},
            "lane_counts": {},
            "watch_refresh_due": False,
            "watch_refresh_reason": "priors_missing",
            "watch_refresh_interval_runs": 1,
            "watch_max_markets": 0,
            "watch_cursor_before": 0,
            "watch_cursor_after": 0,
            "state_run_counter": 0,
            "state_write_error": None,
            "endpoint_to_orderable_transition_count": 0,
            "endpoint_to_orderable_transition_tickers": [],
            "watch_priority_candidates": [],
        }
    allowed_set = {str(value or "").strip().lower() for value in allowed_contract_families if str(value or "").strip()}
    max_markets_effective = max(1, int(max_markets))
    max_watch_markets = max(0, int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_MAX_MARKETS", "4")))
    watch_interval_runs = max(1, int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_INTERVAL_RUNS", "3")))
    state_path = Path(
        str(
            os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_STATE_FILE")
            or (Path(os.environ["BETBOT_OUTPUT_DIR"]) / "overnight_alpha" / "daily_weather_ticker_refresh_state.json")
        )
    )
    state_before = _load_daily_weather_refresh_state(state_path)
    state_run_counter = int(state_before.get("run_counter") or 0) + 1
    watch_cursor_before = int(state_before.get("watch_cursor") or 0)
    prev_lane_by_ticker = dict(state_before.get("lane_by_ticker") or {})
    prev_has_orderable_side_by_ticker = dict(state_before.get("has_orderable_side_by_ticker") or {})
    ticker_stats_by_ticker = dict(state_before.get("ticker_stats_by_ticker") or {})
    now_utc = datetime.now(timezone.utc)

    tradable_tickers: list[str] = []
    watch_tickers: list[str] = []
    prior_hours_to_close_by_ticker: dict[str, float | None] = {}
    lane_by_ticker: dict[str, str] = {}
    has_orderable_side_by_ticker: dict[str, bool] = {}
    lane_counts: dict[str, int] = {}
    endpoint_to_orderable_tickers: list[str] = []
    seen: set[str] = set()
    with priors_csv.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            contract_family = str(row.get("contract_family") or "").strip().lower()
            if contract_family not in allowed_set:
                continue
            ticker = str(row.get("market_ticker") or "").strip().upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            lane_info = _classify_daily_weather_lane_from_prior_row(row)
            lane = str(lane_info.get("lane") or "tradable")
            has_orderable_side = bool(lane_info.get("has_orderable_side"))
            quotes_present_count = int(lane_info.get("quotes_present_count") or 0)
            if (
                quotes_present_count == 0
                and not has_orderable_side
                and str(prev_lane_by_ticker.get(ticker) or "").strip()
            ):
                lane = str(prev_lane_by_ticker.get(ticker) or lane)
                has_orderable_side = bool(prev_has_orderable_side_by_ticker.get(ticker))
            lane_by_ticker[ticker] = lane
            has_orderable_side_by_ticker[ticker] = has_orderable_side
            lane_counts[lane] = int(lane_counts.get(lane, 0)) + 1
            prior_hours_to_close_by_ticker[ticker] = _parse_float(row.get("hours_to_close"))
            if lane == "watch_endpoint_only":
                watch_tickers.append(ticker)
            else:
                tradable_tickers.append(ticker)
            if (
                str(prev_lane_by_ticker.get(ticker) or "") == "watch_endpoint_only"
                and bool(prev_has_orderable_side_by_ticker.get(ticker))
                is False
                and has_orderable_side
            ):
                endpoint_to_orderable_tickers.append(ticker)

    watch_refresh_due = (state_run_counter % watch_interval_runs == 0) or not tradable_tickers
    watch_refresh_reason = "interval_due" if (state_run_counter % watch_interval_runs == 0) else "interval_not_due"
    if not tradable_tickers:
        watch_refresh_reason = "no_tradable_tickers"
    watch_priority_candidates: list[dict[str, Any]] = []
    ranked_watch_tickers = list(watch_tickers)
    if ranked_watch_tickers:
        scored: list[tuple[float, str, dict[str, Any]]] = []
        for ticker in ranked_watch_tickers:
            score, components = _daily_weather_watch_priority_score(
                now_utc=now_utc,
                run_counter=state_run_counter,
                ticker_stats=dict(ticker_stats_by_ticker.get(ticker) or {}),
                prior_hours_to_close=prior_hours_to_close_by_ticker.get(ticker),
            )
            scored.append((score, ticker, components))
        scored.sort(
            key=lambda item: (
                -float(item[0]),
                float(item[2].get("minutes_since_last_non_endpoint") or 999999.0),
                float(item[2].get("hours_to_close") or 999999.0),
                item[1],
            )
        )
        ranked_watch_tickers = [ticker for _, ticker, _ in scored]
        watch_priority_candidates = [
            {
                "market_ticker": ticker,
                "priority_score": score,
                **components,
            }
            for score, ticker, components in scored[: min(20, len(scored))]
        ]
    selected_watch_tickers: list[str] = []
    watch_cursor_after = watch_cursor_before
    if watch_refresh_due and ranked_watch_tickers and max_watch_markets > 0:
        watch_take = min(max_watch_markets, max_markets_effective)
        selected_watch_tickers = list(ranked_watch_tickers[:watch_take])
        watch_cursor_after = (watch_cursor_before + len(selected_watch_tickers)) % max(
            1,
            len(ranked_watch_tickers),
        )

    selected_tickers = list(tradable_tickers[:max_markets_effective])
    remaining_slots = max(0, max_markets_effective - len(selected_tickers))
    if remaining_slots > 0 and selected_watch_tickers:
        selected_tickers.extend(selected_watch_tickers[:remaining_slots])
    selected_tickers = selected_tickers[:max_markets_effective]

    state_write_error = None
    return {
        "selected_tickers": selected_tickers,
        "tradable_tickers": tradable_tickers,
        "watch_tickers": watch_tickers,
        "ranked_watch_tickers": ranked_watch_tickers,
        "watch_hours_to_close_by_ticker": {
            ticker: prior_hours_to_close_by_ticker.get(ticker)
            for ticker in ranked_watch_tickers
            if ticker in prior_hours_to_close_by_ticker
        },
        "watch_tickers_selected": selected_watch_tickers,
        "lane_by_ticker": lane_by_ticker,
        "has_orderable_side_by_ticker": has_orderable_side_by_ticker,
        "ticker_stats_by_ticker": ticker_stats_by_ticker,
        "lane_counts": lane_counts,
        "watch_refresh_due": watch_refresh_due,
        "watch_refresh_reason": watch_refresh_reason,
        "watch_refresh_interval_runs": watch_interval_runs,
        "watch_max_markets": max_watch_markets,
        "watch_cursor_before": watch_cursor_before,
        "watch_cursor_after": watch_cursor_after,
        "state_run_counter": state_run_counter,
        "state_file": str(state_path),
        "state_write_error": state_write_error,
        "previous_lane_by_ticker": prev_lane_by_ticker,
        "previous_has_orderable_side_by_ticker": prev_has_orderable_side_by_ticker,
        "endpoint_to_orderable_transition_count": len(endpoint_to_orderable_tickers),
        "endpoint_to_orderable_transition_tickers": endpoint_to_orderable_tickers[:25],
        "watch_priority_candidates": watch_priority_candidates,
        "watch_selection_mode": "priority_score",
    }


def _append_history_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if callable(_capture_append_history):
        _capture_append_history(path, rows)
        return
    fieldnames = list(_capture_history_fieldnames or [])
    if not fieldnames:
        raise RuntimeError("history fieldnames unavailable for ticker refresh append")
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _history_row_from_market_payload(
    *,
    market_ticker: str,
    market_payload: dict[str, Any],
    captured_at: datetime,
) -> dict[str, Any]:
    yes_bid = _parse_float(market_payload.get("yes_bid_dollars"))
    yes_ask = _parse_float(market_payload.get("yes_ask_dollars"))
    no_bid = _parse_float(market_payload.get("no_bid_dollars"))
    no_ask = _parse_float(market_payload.get("no_ask_dollars"))
    last_price = _parse_float(market_payload.get("last_price_dollars"))
    liquidity = _parse_float(market_payload.get("liquidity_dollars"))
    volume_24h = _parse_float(market_payload.get("volume_24h_fp"))
    open_interest = _parse_float(market_payload.get("open_interest_fp"))
    yes_bid_size = _parse_float(market_payload.get("yes_bid_size_fp"))
    yes_ask_size = _parse_float(market_payload.get("yes_ask_size_fp"))
    spread = ""
    if isinstance(yes_ask, float) and isinstance(yes_bid, float) and yes_ask >= yes_bid:
        spread = round(yes_ask - yes_bid, 6)
    contracts_for_ten_dollars = None
    ten_dollar_fillable = False
    if isinstance(yes_ask, float) and yes_ask > 0:
        contracts_for_ten_dollars = max(1, math.floor(10.0 / yes_ask))
        ten_dollar_fillable = isinstance(yes_ask_size, float) and yes_ask_size >= contracts_for_ten_dollars
    two_sided_book = (
        isinstance(yes_bid, float)
        and yes_bid > 0
        and isinstance(yes_ask, float)
        and yes_ask > 0
        and isinstance(yes_bid_size, float)
        and yes_bid_size > 0
        and isinstance(yes_ask_size, float)
        and yes_ask_size > 0
    )
    close_time_text = str(market_payload.get("close_time") or "").strip()
    hours_to_close = ""
    close_time_dt = _parse_iso(close_time_text)
    if isinstance(close_time_dt, datetime):
        hours_to_close = round((close_time_dt - captured_at.astimezone(timezone.utc)).total_seconds() / 3600.0, 4)
    return {
        "captured_at": captured_at.isoformat(),
        "summary_file": "",
        "scan_csv": "",
        "category": "Climate and Weather",
        "market_family": "weather_climate",
        "resolution_source_type": "official_source",
        "series_ticker": str(market_payload.get("series_ticker") or "").strip(),
        "event_ticker": str(market_payload.get("event_ticker") or "").strip(),
        "market_ticker": market_ticker,
        "event_title": str(market_payload.get("event_ticker") or "").strip(),
        "event_sub_title": "",
        "market_title": str(market_payload.get("title") or "").strip(),
        "yes_sub_title": str(market_payload.get("yes_sub_title") or "").strip(),
        "rules_primary": str(market_payload.get("rules_primary") or "").strip(),
        "close_time": close_time_text,
        "hours_to_close": hours_to_close,
        "yes_bid_dollars": yes_bid if yes_bid is not None else "",
        "yes_bid_size_contracts": yes_bid_size if yes_bid_size is not None else "",
        "yes_ask_dollars": yes_ask if yes_ask is not None else "",
        "yes_ask_size_contracts": yes_ask_size if yes_ask_size is not None else "",
        "no_bid_dollars": no_bid if no_bid is not None else "",
        "no_ask_dollars": no_ask if no_ask is not None else "",
        "last_price_dollars": last_price if last_price is not None else "",
        "spread_dollars": spread,
        "liquidity_dollars": liquidity if liquidity is not None else "",
        "volume_24h_contracts": volume_24h if volume_24h is not None else "",
        "open_interest_contracts": open_interest if open_interest is not None else "",
        "ten_dollar_fillable_at_best_ask": ten_dollar_fillable,
        "two_sided_book": two_sided_book,
        "execution_fit_score": "",
    }


def _run_daily_weather_ticker_refresh(
    *,
    env_values: dict[str, str],
    priors_csv: Path,
    history_csv: Path,
    allowed_contract_families: list[str],
    timeout_seconds: float,
    max_markets: int,
    captured_at: datetime,
    explicit_market_tickers: list[str] | None = None,
) -> dict[str, Any]:
    started_at = _now_iso()
    started_monotonic = time.monotonic()
    api_roots = _kalshi_api_roots_for_env(env_values)
    ticker_plan = _load_daily_weather_tickers_from_priors(
        priors_csv=priors_csv,
        allowed_contract_families=allowed_contract_families,
        max_markets=max_markets,
    )
    refresh_mode = "prior_selected"
    tickers = list(ticker_plan.get("selected_tickers") or [])
    if explicit_market_tickers:
        refresh_mode = "explicit_tickers"
        seen_explicit: set[str] = set()
        explicit_clean: list[str] = []
        for value in explicit_market_tickers:
            ticker = str(value or "").strip().upper()
            if not ticker or ticker in seen_explicit:
                continue
            seen_explicit.add(ticker)
            explicit_clean.append(ticker)
        tickers = explicit_clean
    rows: list[dict[str, Any]] = []
    fetch_errors: list[dict[str, Any]] = []
    tickers_succeeded = 0
    requests_total = 0
    for ticker in tickers:
        market_payload: dict[str, Any] | None = None
        last_status: int | None = None
        last_error: str | None = None
        for api_root in api_roots:
            requests_total += 1
            status_code, payload = _http_get_json_url(
                f"{api_root}/markets/{ticker}",
                timeout_seconds=max(1.0, float(timeout_seconds)),
            )
            last_status = int(status_code)
            if status_code == 200 and isinstance(payload, dict) and isinstance(payload.get("market"), dict):
                market_payload = dict(payload.get("market") or {})
                break
            if isinstance(payload, dict):
                error_text = str(payload.get("error") or payload.get("errorMessage") or "").strip()
                if error_text:
                    last_error = error_text
        if not isinstance(market_payload, dict):
            fetch_errors.append(
                {
                    "market_ticker": ticker,
                    "http_status": last_status,
                    "error": last_error or "market_fetch_failed",
                }
            )
            continue
        rows.append(
            _history_row_from_market_payload(
                market_ticker=ticker,
                market_payload=market_payload,
                captured_at=captured_at,
            )
        )
        tickers_succeeded += 1

    append_error = None
    rows_appended = 0
    if rows:
        try:
            _append_history_rows(history_csv, rows)
            rows_appended = len(rows)
        except Exception as exc:
            append_error = str(exc)

    lane_by_ticker = dict(ticker_plan.get("lane_by_ticker") or {})
    has_orderable_side_by_ticker = dict(ticker_plan.get("has_orderable_side_by_ticker") or {})
    ticker_stats_by_ticker = dict(ticker_plan.get("ticker_stats_by_ticker") or {})
    observed_at_utc = _now_iso()
    observed_now_utc = datetime.now(timezone.utc)
    watch_selected_set = {
        str(value or "").strip().upper()
        for value in (ticker_plan.get("watch_tickers_selected") or [])
        if str(value or "").strip()
    }
    for ticker in tickers:
        if ticker not in watch_selected_set:
            continue
        stats = dict(ticker_stats_by_ticker.get(ticker) or {})
        stats["last_watch_selected_at_utc"] = observed_at_utc
        stats["last_watch_selected_run_counter"] = int(ticker_plan.get("state_run_counter") or 0)
        stats["watch_selected_count"] = max(0, _coerce_int(stats.get("watch_selected_count"), 0)) + 1
        ticker_stats_by_ticker[ticker] = stats
    for row in rows:
        ticker = str(row.get("market_ticker") or "").strip().upper()
        if not ticker:
            continue
        observed_lane = _classify_daily_weather_lane_from_quotes(
            yes_bid=_parse_float(row.get("yes_bid_dollars")),
            no_bid=_parse_float(row.get("no_bid_dollars")),
            yes_ask=_parse_float(row.get("yes_ask_dollars")),
            no_ask=_parse_float(row.get("no_ask_dollars")),
        )
        observed_lane_text = str(observed_lane.get("lane") or "tradable")
        observed_has_orderable_side = bool(observed_lane.get("has_orderable_side"))
        lane_by_ticker[ticker] = observed_lane_text
        has_orderable_side_by_ticker[ticker] = observed_has_orderable_side
        stats = dict(ticker_stats_by_ticker.get(ticker) or {})
        stats["last_observed_at_utc"] = observed_at_utc
        stats["last_observed_lane"] = observed_lane_text
        if observed_lane_text == "watch_endpoint_only":
            stats["last_endpoint_only_at_utc"] = observed_at_utc
            stats["endpoint_only_streak_count"] = max(0, _coerce_int(stats.get("endpoint_only_streak_count"), 0)) + 1
        else:
            stats["last_non_endpoint_at_utc"] = observed_at_utc
            stats["endpoint_only_streak_count"] = 0
        if observed_has_orderable_side:
            stats["had_orderable_side_ever"] = True
            stats["last_orderable_side_at_utc"] = observed_at_utc
        hours_to_close = _parse_float(row.get("hours_to_close"))
        if isinstance(hours_to_close, float):
            stats["last_hours_to_close"] = round(hours_to_close, 6)
        ticker_stats_by_ticker[ticker] = stats
    for ticker, stats in list(ticker_stats_by_ticker.items()):
        if not isinstance(stats, dict):
            ticker_stats_by_ticker[ticker] = {}
            continue
        minutes_since_last_wakeup = _minutes_since_iso(stats.get("last_wakeup_at_utc"), now_utc=observed_now_utc)
        if isinstance(minutes_since_last_wakeup, float):
            stats["minutes_since_last_wakeup"] = round(minutes_since_last_wakeup, 3)
        else:
            stats["minutes_since_last_wakeup"] = None
        ticker_stats_by_ticker[ticker] = stats
    lane_counts: dict[str, int] = {}
    for lane in lane_by_ticker.values():
        lane_text = str(lane or "").strip() or "unknown"
        lane_counts[lane_text] = int(lane_counts.get(lane_text, 0)) + 1
    prev_lane_by_ticker = dict(ticker_plan.get("previous_lane_by_ticker") or {})
    prev_has_orderable_side_by_ticker = dict(ticker_plan.get("previous_has_orderable_side_by_ticker") or {})
    endpoint_to_orderable_transition_tickers: list[str] = []
    for ticker, has_orderable_side in has_orderable_side_by_ticker.items():
        if (
            str(prev_lane_by_ticker.get(ticker) or "") == "watch_endpoint_only"
            and bool(prev_has_orderable_side_by_ticker.get(ticker)) is False
            and bool(has_orderable_side)
        ):
            endpoint_to_orderable_transition_tickers.append(ticker)
    endpoint_to_orderable_transition_tickers = sorted(set(endpoint_to_orderable_transition_tickers))
    endpoint_to_orderable_transition_count = len(endpoint_to_orderable_transition_tickers)
    if endpoint_to_orderable_transition_tickers:
        for ticker in endpoint_to_orderable_transition_tickers:
            stats = dict(ticker_stats_by_ticker.get(ticker) or {})
            stats["last_wakeup_at_utc"] = observed_at_utc
            stats["wakeup_count"] = max(0, _coerce_int(stats.get("wakeup_count"), 0)) + 1
            ticker_stats_by_ticker[ticker] = stats

    state_file_text = str(ticker_plan.get("state_file") or "").strip()
    state_write_error = None
    if state_file_text:
        state_tickers = set(lane_by_ticker.keys()) | set(ticker_stats_by_ticker.keys())
        if state_tickers:
            ticker_stats_by_ticker = {
                ticker: dict(ticker_stats_by_ticker.get(ticker) or {})
                for ticker in sorted(state_tickers)
            }
        state_write_error = _write_daily_weather_refresh_state(
            Path(state_file_text),
            {
                "run_counter": int(ticker_plan.get("state_run_counter") or 0),
                "watch_cursor": int(ticker_plan.get("watch_cursor_after") or 0),
                "watch_interval_runs": int(ticker_plan.get("watch_refresh_interval_runs") or 1),
                "watch_max_markets": int(ticker_plan.get("watch_max_markets") or 0),
                "lane_by_ticker": lane_by_ticker,
                "has_orderable_side_by_ticker": has_orderable_side_by_ticker,
                "ticker_stats_by_ticker": ticker_stats_by_ticker,
                "lane_counts": lane_counts,
                "updated_at_utc": _now_iso(),
                "endpoint_to_orderable_transition_count_last": endpoint_to_orderable_transition_count,
                "endpoint_to_orderable_transition_tickers_last": endpoint_to_orderable_transition_tickers[:25],
            },
        )

    if not tickers:
        status = "skipped_no_daily_weather_tickers"
        reason = "no_daily_weather_tickers_in_priors"
    elif rows_appended > 0 and not append_error:
        status = "ready"
        reason = "daily_weather_tickers_refreshed"
    elif append_error:
        status = "append_failed"
        reason = "history_append_failed"
    else:
        status = "upstream_error"
        reason = "market_refresh_failed"

    finished_at = _now_iso()
    duration_seconds = round(time.monotonic() - started_monotonic, 3)
    return {
        "name": "daily_weather_ticker_refresh",
        "command": ["kalshi-market-refresh-by-ticker"],
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "duration_seconds": duration_seconds,
        "exit_code": 0,
        "stdout_json_file": None,
        "stderr_log_file": None,
        "stdout_json_parse_error": None,
        "status": status,
        "output_file": None,
        "ok": True,
        "reason": reason,
        "history_csv": str(history_csv),
        "priors_csv": str(priors_csv),
        "api_roots": list(api_roots),
        "market_tickers_attempted": tickers,
        "market_tickers_attempted_count": len(tickers),
        "market_tickers_succeeded_count": tickers_succeeded,
        "refresh_mode": refresh_mode,
        "watch_selection_mode": ticker_plan.get("watch_selection_mode"),
        "market_tickers_tradable": list(ticker_plan.get("tradable_tickers") or []),
        "market_tickers_tradable_count": len(list(ticker_plan.get("tradable_tickers") or [])),
        "market_tickers_watch": list(ticker_plan.get("watch_tickers") or []),
        "market_tickers_watch_count": len(list(ticker_plan.get("watch_tickers") or [])),
        "market_tickers_watch_selected": list(ticker_plan.get("watch_tickers_selected") or []),
        "market_tickers_watch_selected_count": len(list(ticker_plan.get("watch_tickers_selected") or [])),
        "watch_priority_candidates": list(ticker_plan.get("watch_priority_candidates") or []),
        "ticker_refresh_lane_counts": dict(lane_counts),
        "ticker_refresh_watch_due": bool(ticker_plan.get("watch_refresh_due")),
        "ticker_refresh_watch_reason": str(ticker_plan.get("watch_refresh_reason") or "").strip() or None,
        "ticker_refresh_watch_interval_runs": ticker_plan.get("watch_refresh_interval_runs"),
        "ticker_refresh_watch_max_markets": ticker_plan.get("watch_max_markets"),
        "ticker_refresh_watch_cursor_before": ticker_plan.get("watch_cursor_before"),
        "ticker_refresh_watch_cursor_after": ticker_plan.get("watch_cursor_after"),
        "ticker_refresh_state_run_counter": ticker_plan.get("state_run_counter"),
        "ticker_refresh_state_file": ticker_plan.get("state_file"),
        "ticker_refresh_state_write_error": state_write_error,
        "endpoint_to_orderable_transition_count": endpoint_to_orderable_transition_count,
        "endpoint_to_orderable_transition_tickers": endpoint_to_orderable_transition_tickers[:25],
        "requests_total": requests_total,
        "rows_appended": rows_appended,
        "append_error": append_error,
        "fetch_errors": fetch_errors[:20],
    }


def _run_daily_weather_micro_watch(
    *,
    env_values: dict[str, str],
    priors_csv: Path,
    history_csv: Path,
    allowed_contract_families: list[str],
    timeout_seconds: float,
    max_markets: int,
    captured_at: datetime,
    poll_interval_seconds: float,
    max_polls: int,
    active_hours_to_close: float,
    include_unknown_hours_to_close: bool,
) -> dict[str, Any]:
    watch_plan = _load_daily_weather_tickers_from_priors(
        priors_csv=priors_csv,
        allowed_contract_families=allowed_contract_families,
        max_markets=max_markets,
    )
    ranked_watch_tickers = list(watch_plan.get("ranked_watch_tickers") or watch_plan.get("watch_tickers") or [])
    watch_hours_to_close_by_ticker_raw = dict(watch_plan.get("watch_hours_to_close_by_ticker") or {})
    ticker_stats_by_ticker = dict(watch_plan.get("ticker_stats_by_ticker") or {})
    watch_hours_to_close_by_ticker = {
        str(ticker).strip().upper(): _parse_float(value)
        for ticker, value in watch_hours_to_close_by_ticker_raw.items()
        if str(ticker).strip()
    }
    watch_hours_effective_by_ticker: dict[str, float | None] = {}
    for ticker in ranked_watch_tickers:
        ticker_key = str(ticker or "").strip().upper()
        if not ticker_key:
            continue
        ticker_hours = watch_hours_to_close_by_ticker.get(ticker_key)
        if not isinstance(ticker_hours, float):
            ticker_hours = _parse_float(
                dict(ticker_stats_by_ticker.get(ticker_key) or {}).get("last_hours_to_close")
            )
        watch_hours_effective_by_ticker[ticker_key] = ticker_hours
    watch_total = len(ranked_watch_tickers)
    active_watch_tickers: list[str] = []
    for ticker in ranked_watch_tickers:
        ticker_hours = watch_hours_effective_by_ticker.get(ticker)
        if isinstance(ticker_hours, float):
            if ticker_hours < 0.0:
                continue
            if ticker_hours <= max(0.0, float(active_hours_to_close)):
                active_watch_tickers.append(ticker)
            continue
        if include_unknown_hours_to_close:
            active_watch_tickers.append(ticker)

    selected_watch_tickers = active_watch_tickers[: max(1, int(max_markets))]
    polls_planned = max(1, int(max_polls))
    poll_steps: list[dict[str, Any]] = []
    wakeup_transition_tickers: list[str] = []
    wakeup_transition_seen: set[str] = set()
    wakeup_detected_at_poll: int | None = None

    for poll_index, _ in enumerate(range(polls_planned), start=1):
        if not selected_watch_tickers:
            break
        poll_step = _run_daily_weather_ticker_refresh(
            env_values=env_values,
            priors_csv=priors_csv,
            history_csv=history_csv,
            allowed_contract_families=allowed_contract_families,
            timeout_seconds=timeout_seconds,
            max_markets=max(1, len(selected_watch_tickers)),
            captured_at=captured_at,
            explicit_market_tickers=selected_watch_tickers,
        )
        poll_step["name"] = f"daily_weather_ticker_refresh_micro_watch_{poll_index}"
        poll_step["micro_watch_poll_index"] = poll_index
        poll_step["micro_watch_poll_total"] = polls_planned
        poll_step["micro_watch_selected_tickers"] = selected_watch_tickers
        poll_steps.append(poll_step)

        for value in poll_step.get("endpoint_to_orderable_transition_tickers") or []:
            ticker = str(value or "").strip().upper()
            if not ticker or ticker in wakeup_transition_seen:
                continue
            wakeup_transition_seen.add(ticker)
            wakeup_transition_tickers.append(ticker)
        if wakeup_transition_tickers and wakeup_detected_at_poll is None:
            wakeup_detected_at_poll = poll_index
            break

        if poll_index < polls_planned and float(poll_interval_seconds) > 0.0:
            time.sleep(float(poll_interval_seconds))

    status = "ready_no_wakeup"
    reason = "micro_watch_completed_without_wakeup"
    if watch_total == 0:
        status = "skipped_no_watch_tickers"
        reason = "no_watch_tickers_available"
    elif not active_watch_tickers:
        status = "skipped_no_active_window"
        reason = "watch_tickers_outside_active_window"
    elif wakeup_transition_tickers:
        status = "wakeup_detected"
        reason = "endpoint_to_orderable_transition_detected"
    elif poll_steps and all(str(step.get("status") or "").strip().lower() == "upstream_error" for step in poll_steps):
        status = "upstream_error"
        reason = "micro_watch_all_polls_upstream_error"

    summary_step = _synthetic_step(
        name="daily_weather_micro_watch",
        status=status,
        ok=True,
        reason=reason,
        payload={
            "watch_total": watch_total,
            "active_watch_tickers_count": len(active_watch_tickers),
            "active_watch_tickers": active_watch_tickers[:30],
            "selected_watch_tickers_count": len(selected_watch_tickers),
            "selected_watch_tickers": selected_watch_tickers,
            "polls_planned": polls_planned,
            "polls_completed": len(poll_steps),
            "poll_interval_seconds": round(float(poll_interval_seconds), 3),
            "active_hours_to_close": round(float(active_hours_to_close), 6),
            "include_unknown_hours_to_close": bool(include_unknown_hours_to_close),
            "watch_hours_to_close_by_ticker": {
                ticker: watch_hours_effective_by_ticker.get(ticker)
                for ticker in selected_watch_tickers
            },
            "watch_priority_candidates": list(watch_plan.get("watch_priority_candidates") or []),
            "wakeup_transition_count": len(wakeup_transition_tickers),
            "wakeup_transition_tickers": wakeup_transition_tickers[:25],
            "wakeup_detected_at_poll": wakeup_detected_at_poll,
        },
    )
    return {
        "poll_steps": poll_steps,
        "summary_step": summary_step,
        "wakeup_transition_tickers": wakeup_transition_tickers,
        "watch_plan": watch_plan,
    }


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


def _as_bool(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _weather_history_token_state(env_values: dict[str, str]) -> dict[str, Any]:
    keys = ["BETBOT_NOAA_CDO_TOKEN", "NOAA_CDO_TOKEN", "NCEI_CDO_TOKEN"]
    for key in keys:
        value = env_values.get(key)
        if value is None:
            continue
        if _is_placeholder_value(value):
            continue
        if str(value).strip():
            return {
                "weather_history_token_present": True,
                "weather_history_token_env_key": key,
                "weather_history_token_source": f"env:{key}",
                "weather_history_token_file_used": None,
            }
    return {
        "weather_history_token_present": False,
        "weather_history_token_env_key": None,
        "weather_history_token_source": "missing",
        "weather_history_token_file_used": None,
    }


def _resolve_weather_history_token(
    *,
    env_values: dict[str, str],
    repo_root: Path,
) -> dict[str, Any]:
    direct_state = _weather_history_token_state(env_values)
    if bool(direct_state.get("weather_history_token_present")):
        return direct_state

    file_candidates: list[Path] = []
    explicit_file = str(env_values.get("BETBOT_WEATHER_CDO_TOKEN_FILE") or "").strip()
    if explicit_file:
        explicit_path = Path(explicit_file).expanduser()
        if not explicit_path.is_absolute():
            explicit_path = (repo_root / explicit_path).resolve()
        file_candidates.append(explicit_path)
    else:
        default_from_env = str(os.environ.get("BETBOT_WEATHER_CDO_TOKEN_FILE") or "").strip()
        if default_from_env:
            default_path = Path(default_from_env).expanduser()
            if not default_path.is_absolute():
                default_path = (repo_root / default_path).resolve()
            file_candidates.append(default_path)
    fallback_paths = [
        repo_root / ".secrets" / "noaa_cdo_token.txt",
        repo_root / ".secrets" / "ncei_cdo_token.txt",
        repo_root / ".secrets" / "cdo_token.txt",
    ]
    for fallback in fallback_paths:
        if fallback not in file_candidates:
            file_candidates.append(fallback)

    for candidate in file_candidates:
        if not candidate.exists() or not candidate.is_file():
            continue
        try:
            token_text = candidate.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if _is_placeholder_value(token_text):
            continue
        if not token_text:
            continue
        env_values["BETBOT_NOAA_CDO_TOKEN"] = token_text
        return {
            "weather_history_token_present": True,
            "weather_history_token_env_key": "BETBOT_NOAA_CDO_TOKEN",
            "weather_history_token_source": "token_file",
            "weather_history_token_file_used": str(candidate),
        }

    direct_state["weather_history_token_source"] = "missing"
    direct_state["weather_history_token_file_used"] = None
    return direct_state


def _weather_history_readiness_state(
    *,
    priors_csv: Path,
    allowed_contract_families: list[str],
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "weather_history_rows_total": 0,
        "weather_history_live_ready_rows": 0,
        "weather_history_unhealthy_rows": 0,
        "weather_history_status_counts": {},
        "weather_history_live_ready_reason_counts": {},
        "weather_history_missing_token_count": 0,
        "weather_history_station_mapping_missing_count": 0,
        "weather_history_sample_depth_block_count": 0,
        "weather_history_parse_error": None,
    }
    if not priors_csv.exists():
        state["weather_history_parse_error"] = "priors_missing"
        return state
    try:
        with priors_csv.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                family = str((row or {}).get("contract_family") or "").strip()
                if family not in allowed_contract_families:
                    continue
                state["weather_history_rows_total"] += 1
                status_text = str((row or {}).get("weather_station_history_status") or "").strip()
                reason_text = str((row or {}).get("weather_station_history_live_ready_reason") or "").strip()
                live_ready = _as_bool((row or {}).get("weather_station_history_live_ready"))
                if status_text:
                    counts = state["weather_history_status_counts"]
                    counts[status_text] = int(counts.get(status_text, 0)) + 1
                if live_ready:
                    state["weather_history_live_ready_rows"] += 1
                else:
                    state["weather_history_unhealthy_rows"] += 1
                    if reason_text:
                        reason_counts = state["weather_history_live_ready_reason_counts"]
                        reason_counts[reason_text] = int(reason_counts.get(reason_text, 0)) + 1
                    if status_text == "disabled_missing_token" or reason_text.startswith(
                        "status_disabled_missing_token"
                    ):
                        state["weather_history_missing_token_count"] += 1
                    if status_text == "station_mapping_missing" or reason_text.startswith(
                        "status_station_mapping_missing"
                    ):
                        state["weather_history_station_mapping_missing_count"] += 1
                    if "sample_years_below_min" in reason_text:
                        state["weather_history_sample_depth_block_count"] += 1
    except Exception as exc:  # pragma: no cover - best effort diagnostics
        state["weather_history_parse_error"] = str(exc)
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


def _plan_skip_diagnostics_from_summary(
    *,
    plan_summary_file: Any,
    top_n: int = 5,
) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {
        "plan_summary_file": None,
        "plan_summary_status": "missing",
        "plan_summary_error": None,
        "plan_summary_planned_orders": None,
        "plan_skip_counts_total": 0,
        "plan_skip_counts_nonzero_total": 0,
        "plan_skip_counts_top": [],
        "plan_skip_reason_dominant": None,
        "plan_skip_reason_dominant_count": None,
        "plan_skip_reason_dominant_share": None,
        "allowed_universe_candidate_pool_size": None,
        "allowed_universe_skip_counts_total": 0,
        "allowed_universe_skip_counts_nonzero_total": 0,
        "allowed_universe_skip_counts_top": [],
        "allowed_universe_skip_reason_dominant": None,
        "allowed_universe_skip_reason_dominant_count": None,
        "allowed_universe_skip_reason_dominant_share": None,
        "daily_weather_candidate_pool_size": None,
        "daily_weather_rows_total": None,
        "daily_weather_skip_counts_total": 0,
        "daily_weather_skip_counts_nonzero_total": 0,
        "daily_weather_skip_counts_top": [],
        "daily_weather_skip_reason_dominant": None,
        "daily_weather_skip_reason_dominant_count": None,
        "daily_weather_skip_reason_dominant_share": None,
        "daily_weather_rows_with_conservative_candidate": None,
        "daily_weather_rows_with_both_sides_candidate": None,
        "daily_weather_rows_with_one_side_failed": None,
        "daily_weather_rows_with_both_sides_failed": None,
        "daily_weather_orderable_bid_rows": None,
        "daily_weather_rows_with_any_orderable_bid": None,
        "daily_weather_rows_with_any_orderable_ask": None,
        "daily_weather_rows_with_fair_probabilities": None,
        "daily_weather_rows_with_both_quote_and_fair_value": None,
        "daily_weather_quote_orderability_counts": None,
        "daily_weather_quote_age_rows_with_timestamp": None,
        "daily_weather_quote_stale_max_age_seconds": None,
        "daily_weather_shadow_taker_rows_total": None,
        "daily_weather_shadow_taker_rows_with_orderable_yes_ask": None,
        "daily_weather_shadow_taker_rows_with_orderable_no_ask": None,
        "daily_weather_shadow_taker_rows_with_any_orderable_ask": None,
        "daily_weather_shadow_taker_edge_above_min_count": None,
        "daily_weather_shadow_taker_edge_net_fees_above_min_count": None,
        "daily_weather_shadow_taker_endpoint_orderbook_rows": None,
        "daily_weather_best_shadow_taker_candidate": None,
        "daily_weather_allowed_universe_rows_with_conservative_candidate": None,
        "daily_weather_endpoint_orderbook_filtered": None,
        "daily_weather_conservative_candidate_failure_counts": None,
        "daily_weather_planned_orders": None,
    }
    path_text = str(plan_summary_file or "").strip()
    diagnostics["plan_summary_file"] = path_text or None
    if not path_text:
        diagnostics["plan_summary_error"] = "plan_summary_file_missing"
        return diagnostics

    path = Path(path_text)
    if not path.exists():
        diagnostics["plan_summary_error"] = "plan_summary_file_not_found"
        return diagnostics

    payload = _load_json(path)
    if not isinstance(payload, dict):
        diagnostics["plan_summary_status"] = "invalid"
        diagnostics["plan_summary_error"] = "plan_summary_payload_invalid"
        return diagnostics

    diagnostics["plan_summary_status"] = str(payload.get("status") or "").strip() or "unknown"
    planned_orders_raw = payload.get("planned_orders")
    if isinstance(planned_orders_raw, (int, float)):
        diagnostics["plan_summary_planned_orders"] = int(planned_orders_raw)

    def _apply_skip_profile(
        *,
        counts_key: str,
        output_prefix: str,
    ) -> None:
        raw_counts = payload.get(counts_key)
        if not isinstance(raw_counts, dict):
            return
        normalized_counts: list[tuple[str, int]] = []
        for raw_reason, raw_count in raw_counts.items():
            reason = str(raw_reason or "").strip()
            if not reason:
                continue
            count_value: int | None = None
            if isinstance(raw_count, bool):
                count_value = int(raw_count)
            elif isinstance(raw_count, (int, float)):
                count_value = int(raw_count)
            else:
                count_parsed = _parse_float(raw_count)
                if isinstance(count_parsed, float):
                    count_value = int(count_parsed)
            if count_value is None:
                continue
            normalized_counts.append((reason, max(0, count_value)))

        total = int(sum(count for _, count in normalized_counts))
        nonzero = [(reason, count) for reason, count in normalized_counts if count > 0]
        nonzero.sort(key=lambda item: (-item[1], item[0]))
        top_limit = max(1, int(top_n))
        diagnostics[f"{output_prefix}_skip_counts_total"] = total
        diagnostics[f"{output_prefix}_skip_counts_nonzero_total"] = len(nonzero)
        diagnostics[f"{output_prefix}_skip_counts_top"] = [
            {"reason": reason, "count": count}
            for reason, count in nonzero[:top_limit]
        ]
        if nonzero:
            dominant_reason, dominant_count = nonzero[0]
            diagnostics[f"{output_prefix}_skip_reason_dominant"] = dominant_reason
            diagnostics[f"{output_prefix}_skip_reason_dominant_count"] = dominant_count
            if total > 0:
                diagnostics[f"{output_prefix}_skip_reason_dominant_share"] = round(
                    float(dominant_count) / float(total),
                    4,
                )

    _apply_skip_profile(counts_key="skip_counts", output_prefix="plan")
    if diagnostics["plan_skip_counts_total"] == 0 and not isinstance(payload.get("skip_counts"), dict):
        diagnostics["plan_summary_error"] = "skip_counts_missing"
        return diagnostics

    allowed_pool = payload.get("allowed_universe_candidate_pool_size")
    if isinstance(allowed_pool, (int, float)):
        diagnostics["allowed_universe_candidate_pool_size"] = int(allowed_pool)
    _apply_skip_profile(counts_key="allowed_universe_skip_counts", output_prefix="allowed_universe")

    daily_pool = payload.get("daily_weather_candidate_pool_size")
    if isinstance(daily_pool, (int, float)):
        diagnostics["daily_weather_candidate_pool_size"] = int(daily_pool)
    daily_rows_total = payload.get("daily_weather_rows_total")
    if isinstance(daily_rows_total, (int, float)):
        diagnostics["daily_weather_rows_total"] = int(daily_rows_total)
    _apply_skip_profile(counts_key="daily_weather_skip_counts", output_prefix="daily_weather")
    daily_conservative_rows = payload.get("daily_weather_rows_with_conservative_candidate")
    if isinstance(daily_conservative_rows, (int, float)):
        diagnostics["daily_weather_rows_with_conservative_candidate"] = int(daily_conservative_rows)
    daily_both_sides_rows = payload.get("daily_weather_rows_with_both_sides_candidate")
    if isinstance(daily_both_sides_rows, (int, float)):
        diagnostics["daily_weather_rows_with_both_sides_candidate"] = int(daily_both_sides_rows)
    daily_one_side_failed_rows = payload.get("daily_weather_rows_with_one_side_failed")
    if isinstance(daily_one_side_failed_rows, (int, float)):
        diagnostics["daily_weather_rows_with_one_side_failed"] = int(daily_one_side_failed_rows)
    daily_both_sides_failed_rows = payload.get("daily_weather_rows_with_both_sides_failed")
    if isinstance(daily_both_sides_failed_rows, (int, float)):
        diagnostics["daily_weather_rows_with_both_sides_failed"] = int(daily_both_sides_failed_rows)
    daily_orderable_bid_rows = payload.get("daily_weather_orderable_bid_rows")
    if isinstance(daily_orderable_bid_rows, (int, float)):
        diagnostics["daily_weather_orderable_bid_rows"] = int(daily_orderable_bid_rows)
    daily_rows_with_any_orderable_bid = payload.get("daily_weather_rows_with_any_orderable_bid")
    if isinstance(daily_rows_with_any_orderable_bid, (int, float)):
        diagnostics["daily_weather_rows_with_any_orderable_bid"] = int(daily_rows_with_any_orderable_bid)
    daily_rows_with_any_orderable_ask = payload.get("daily_weather_rows_with_any_orderable_ask")
    if isinstance(daily_rows_with_any_orderable_ask, (int, float)):
        diagnostics["daily_weather_rows_with_any_orderable_ask"] = int(daily_rows_with_any_orderable_ask)
    daily_rows_with_fair = payload.get("daily_weather_rows_with_fair_probabilities")
    if isinstance(daily_rows_with_fair, (int, float)):
        diagnostics["daily_weather_rows_with_fair_probabilities"] = int(daily_rows_with_fair)
    daily_rows_with_quote_and_fair = payload.get("daily_weather_rows_with_both_quote_and_fair_value")
    if isinstance(daily_rows_with_quote_and_fair, (int, float)):
        diagnostics["daily_weather_rows_with_both_quote_and_fair_value"] = int(daily_rows_with_quote_and_fair)
    daily_quote_orderability_raw = payload.get("daily_weather_quote_orderability_counts")
    if isinstance(daily_quote_orderability_raw, dict):
        normalized_daily_quote_orderability: dict[str, int] = {}
        for raw_reason, raw_count in daily_quote_orderability_raw.items():
            reason_text = str(raw_reason or "").strip()
            if not reason_text:
                continue
            count_value: int | None = None
            if isinstance(raw_count, bool):
                count_value = int(raw_count)
            elif isinstance(raw_count, (int, float)):
                count_value = int(raw_count)
            else:
                parsed_count = _parse_float(raw_count)
                if isinstance(parsed_count, float):
                    count_value = int(parsed_count)
            if count_value is None:
                continue
            normalized_daily_quote_orderability[reason_text] = max(0, count_value)
        diagnostics["daily_weather_quote_orderability_counts"] = normalized_daily_quote_orderability
    daily_quote_age_rows = payload.get("daily_weather_quote_age_rows_with_timestamp")
    if isinstance(daily_quote_age_rows, (int, float)):
        diagnostics["daily_weather_quote_age_rows_with_timestamp"] = int(daily_quote_age_rows)
    daily_quote_stale_max_age = payload.get("daily_weather_quote_stale_max_age_seconds")
    if isinstance(daily_quote_stale_max_age, (int, float)):
        diagnostics["daily_weather_quote_stale_max_age_seconds"] = round(float(daily_quote_stale_max_age), 3)
    daily_shadow_rows_total = payload.get("daily_weather_shadow_taker_rows_total")
    if isinstance(daily_shadow_rows_total, (int, float)):
        diagnostics["daily_weather_shadow_taker_rows_total"] = int(daily_shadow_rows_total)
    daily_shadow_rows_yes_ask = payload.get("daily_weather_shadow_taker_rows_with_orderable_yes_ask")
    if isinstance(daily_shadow_rows_yes_ask, (int, float)):
        diagnostics["daily_weather_shadow_taker_rows_with_orderable_yes_ask"] = int(daily_shadow_rows_yes_ask)
    daily_shadow_rows_no_ask = payload.get("daily_weather_shadow_taker_rows_with_orderable_no_ask")
    if isinstance(daily_shadow_rows_no_ask, (int, float)):
        diagnostics["daily_weather_shadow_taker_rows_with_orderable_no_ask"] = int(daily_shadow_rows_no_ask)
    daily_shadow_rows_any_ask = payload.get("daily_weather_shadow_taker_rows_with_any_orderable_ask")
    if isinstance(daily_shadow_rows_any_ask, (int, float)):
        diagnostics["daily_weather_shadow_taker_rows_with_any_orderable_ask"] = int(daily_shadow_rows_any_ask)
    daily_shadow_edge_count = payload.get("daily_weather_shadow_taker_edge_above_min_count")
    if isinstance(daily_shadow_edge_count, (int, float)):
        diagnostics["daily_weather_shadow_taker_edge_above_min_count"] = int(daily_shadow_edge_count)
    daily_shadow_edge_net_count = payload.get("daily_weather_shadow_taker_edge_net_fees_above_min_count")
    if isinstance(daily_shadow_edge_net_count, (int, float)):
        diagnostics["daily_weather_shadow_taker_edge_net_fees_above_min_count"] = int(daily_shadow_edge_net_count)
    daily_shadow_endpoint_rows = payload.get("daily_weather_shadow_taker_endpoint_orderbook_rows")
    if isinstance(daily_shadow_endpoint_rows, (int, float)):
        diagnostics["daily_weather_shadow_taker_endpoint_orderbook_rows"] = int(daily_shadow_endpoint_rows)
    daily_shadow_best_candidate = payload.get("daily_weather_best_shadow_taker_candidate")
    if isinstance(daily_shadow_best_candidate, dict):
        diagnostics["daily_weather_best_shadow_taker_candidate"] = daily_shadow_best_candidate
    daily_allowed_conservative_rows = payload.get("daily_weather_allowed_universe_rows_with_conservative_candidate")
    if isinstance(daily_allowed_conservative_rows, (int, float)):
        diagnostics["daily_weather_allowed_universe_rows_with_conservative_candidate"] = int(
            daily_allowed_conservative_rows
        )
    daily_endpoint_filtered = payload.get("daily_weather_endpoint_orderbook_filtered")
    if isinstance(daily_endpoint_filtered, (int, float)):
        diagnostics["daily_weather_endpoint_orderbook_filtered"] = int(daily_endpoint_filtered)
    daily_failure_counts_raw = payload.get("daily_weather_conservative_candidate_failure_counts")
    if isinstance(daily_failure_counts_raw, dict):
        normalized_daily_failure_counts: dict[str, int] = {}
        for raw_reason, raw_count in daily_failure_counts_raw.items():
            reason_text = str(raw_reason or "").strip()
            if not reason_text:
                continue
            count_value: int | None = None
            if isinstance(raw_count, bool):
                count_value = int(raw_count)
            elif isinstance(raw_count, (int, float)):
                count_value = int(raw_count)
            else:
                parsed_count = _parse_float(raw_count)
                if isinstance(parsed_count, float):
                    count_value = int(parsed_count)
            if count_value is None:
                continue
            normalized_daily_failure_counts[reason_text] = max(0, count_value)
        diagnostics["daily_weather_conservative_candidate_failure_counts"] = normalized_daily_failure_counts
    daily_planned_orders = payload.get("daily_weather_planned_orders")
    if isinstance(daily_planned_orders, (int, float)):
        diagnostics["daily_weather_planned_orders"] = int(daily_planned_orders)
    return diagnostics


def _run_step(
    *,
    name: str,
    launcher: list[str],
    args: list[str],
    cwd: Path,
    run_dir: Path,
    env_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    started_at = _now_iso()
    started_monotonic = time.monotonic()
    stdout_file = run_dir / f"{name}.stdout.json"
    stderr_file = run_dir / f"{name}.stderr.log"

    cmd = launcher + args
    merged_env = os.environ.copy()
    if isinstance(env_overrides, dict):
        for key, value in env_overrides.items():
            key_text = str(key or "").strip()
            if not key_text:
                continue
            merged_env[key_text] = str(value or "")

    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
        env=merged_env,
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
        "reason": (parsed or {}).get("reason") if isinstance(parsed, dict) else None,
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
        elif name.startswith("capture"):
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
        elif name == "prior_plan_wakeup_reprioritize":
            step["planned_orders"] = parsed.get("planned_orders")
            step["top_market_ticker"] = parsed.get("top_market_ticker")
            step["daily_weather_candidate_pool_size"] = parsed.get("daily_weather_candidate_pool_size")
            step["daily_weather_rows_with_conservative_candidate"] = parsed.get(
                "daily_weather_rows_with_conservative_candidate"
            )
            step["daily_weather_planned_orders"] = parsed.get("daily_weather_planned_orders")
            top_plans = parsed.get("top_plans")
            if isinstance(top_plans, list):
                step["top_plans_count"] = len(top_plans)
                step["top_plans_tickers"] = [
                    str(item.get("market_ticker") or "").strip().upper()
                    for item in top_plans
                    if isinstance(item, dict) and str(item.get("market_ticker") or "").strip()
                ][:20]
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
        elif name.startswith("prior_trader_dry_run"):
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
            step["prior_trade_gate_blockers"] = parsed.get("prior_trade_gate_blockers")
            step["prior_plan_summary_file"] = parsed.get("prior_plan_summary_file")
            step["top_market_ticker"] = parsed.get("top_market_ticker")
            step["top_market_contract_family"] = parsed.get("top_market_contract_family")
            step["top_market_weather_history_status"] = parsed.get("top_market_weather_history_status")
            step["top_market_weather_history_live_ready"] = parsed.get("top_market_weather_history_live_ready")
            step["top_market_weather_history_live_ready_reason"] = parsed.get("top_market_weather_history_live_ready_reason")
            step["daily_weather_board_fresh"] = parsed.get("daily_weather_board_fresh")
            step["daily_weather_board_age_seconds"] = parsed.get("daily_weather_board_age_seconds")
            step["daily_weather_markets"] = parsed.get("daily_weather_markets")
            step["daily_weather_family_counts"] = parsed.get("daily_weather_family_counts")
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
            step["enable_untrusted_bucket_probe_exploration"] = parsed.get(
                "enable_untrusted_bucket_probe_exploration"
            )
            step["untrusted_bucket_probe_exploration_enabled"] = parsed.get(
                "untrusted_bucket_probe_exploration_enabled"
            )
            step["untrusted_bucket_probe_max_orders_per_run"] = parsed.get(
                "untrusted_bucket_probe_max_orders_per_run"
            )
            step["untrusted_bucket_probe_required_edge_buffer_dollars"] = parsed.get(
                "untrusted_bucket_probe_required_edge_buffer_dollars"
            )
            step["untrusted_bucket_probe_contracts_cap"] = parsed.get(
                "untrusted_bucket_probe_contracts_cap"
            )
            step["untrusted_bucket_probe_submitted_attempts"] = parsed.get(
                "untrusted_bucket_probe_submitted_attempts"
            )
            step["untrusted_bucket_probe_blocked_attempts"] = parsed.get(
                "untrusted_bucket_probe_blocked_attempts"
            )
            step["untrusted_bucket_probe_reason_counts"] = parsed.get(
                "untrusted_bucket_probe_reason_counts"
            )
            step["frontier_artifact_path"] = parsed.get("frontier_artifact_path")
            step["frontier_artifact_sha256"] = parsed.get("frontier_artifact_sha256")
            step["frontier_artifact_file_sha256"] = parsed.get("frontier_artifact_file_sha256")
            step["frontier_artifact_payload_sha256"] = parsed.get("frontier_artifact_payload_sha256")
            step["frontier_artifact_as_of_utc"] = parsed.get("frontier_artifact_as_of_utc")
            step["frontier_artifact_age_seconds"] = parsed.get("frontier_artifact_age_seconds")
            skip_diagnostics = _plan_skip_diagnostics_from_summary(
                plan_summary_file=parsed.get("prior_plan_summary_file")
            )
            step["plan_summary_status"] = skip_diagnostics.get("plan_summary_status")
            step["plan_summary_error"] = skip_diagnostics.get("plan_summary_error")
            step["plan_summary_planned_orders"] = skip_diagnostics.get("plan_summary_planned_orders")
            step["plan_skip_counts_total"] = skip_diagnostics.get("plan_skip_counts_total")
            step["plan_skip_counts_nonzero_total"] = skip_diagnostics.get("plan_skip_counts_nonzero_total")
            step["plan_skip_counts_top"] = skip_diagnostics.get("plan_skip_counts_top")
            step["plan_skip_reason_dominant"] = skip_diagnostics.get("plan_skip_reason_dominant")
            step["plan_skip_reason_dominant_count"] = skip_diagnostics.get("plan_skip_reason_dominant_count")
            step["plan_skip_reason_dominant_share"] = skip_diagnostics.get("plan_skip_reason_dominant_share")
            step["allowed_universe_candidate_pool_size"] = skip_diagnostics.get("allowed_universe_candidate_pool_size")
            step["allowed_universe_skip_counts_total"] = skip_diagnostics.get("allowed_universe_skip_counts_total")
            step["allowed_universe_skip_counts_nonzero_total"] = skip_diagnostics.get(
                "allowed_universe_skip_counts_nonzero_total"
            )
            step["allowed_universe_skip_counts_top"] = skip_diagnostics.get("allowed_universe_skip_counts_top")
            step["allowed_universe_skip_reason_dominant"] = skip_diagnostics.get(
                "allowed_universe_skip_reason_dominant"
            )
            step["allowed_universe_skip_reason_dominant_count"] = skip_diagnostics.get(
                "allowed_universe_skip_reason_dominant_count"
            )
            step["allowed_universe_skip_reason_dominant_share"] = skip_diagnostics.get(
                "allowed_universe_skip_reason_dominant_share"
            )
            step["daily_weather_candidate_pool_size"] = skip_diagnostics.get("daily_weather_candidate_pool_size")
            step["daily_weather_rows_total"] = skip_diagnostics.get("daily_weather_rows_total")
            step["daily_weather_skip_counts_total"] = skip_diagnostics.get("daily_weather_skip_counts_total")
            step["daily_weather_skip_counts_nonzero_total"] = skip_diagnostics.get(
                "daily_weather_skip_counts_nonzero_total"
            )
            step["daily_weather_skip_counts_top"] = skip_diagnostics.get("daily_weather_skip_counts_top")
            step["daily_weather_skip_reason_dominant"] = skip_diagnostics.get(
                "daily_weather_skip_reason_dominant"
            )
            step["daily_weather_skip_reason_dominant_count"] = skip_diagnostics.get(
                "daily_weather_skip_reason_dominant_count"
            )
            step["daily_weather_skip_reason_dominant_share"] = skip_diagnostics.get(
                "daily_weather_skip_reason_dominant_share"
            )
            step["daily_weather_rows_with_conservative_candidate"] = skip_diagnostics.get(
                "daily_weather_rows_with_conservative_candidate"
            )
            step["daily_weather_rows_with_both_sides_candidate"] = skip_diagnostics.get(
                "daily_weather_rows_with_both_sides_candidate"
            )
            step["daily_weather_rows_with_one_side_failed"] = skip_diagnostics.get(
                "daily_weather_rows_with_one_side_failed"
            )
            step["daily_weather_rows_with_both_sides_failed"] = skip_diagnostics.get(
                "daily_weather_rows_with_both_sides_failed"
            )
            step["daily_weather_orderable_bid_rows"] = skip_diagnostics.get("daily_weather_orderable_bid_rows")
            step["daily_weather_rows_with_any_orderable_bid"] = skip_diagnostics.get(
                "daily_weather_rows_with_any_orderable_bid"
            )
            step["daily_weather_rows_with_any_orderable_ask"] = skip_diagnostics.get(
                "daily_weather_rows_with_any_orderable_ask"
            )
            step["daily_weather_rows_with_fair_probabilities"] = skip_diagnostics.get(
                "daily_weather_rows_with_fair_probabilities"
            )
            step["daily_weather_rows_with_both_quote_and_fair_value"] = skip_diagnostics.get(
                "daily_weather_rows_with_both_quote_and_fair_value"
            )
            step["daily_weather_quote_orderability_counts"] = skip_diagnostics.get(
                "daily_weather_quote_orderability_counts"
            )
            step["daily_weather_quote_age_rows_with_timestamp"] = skip_diagnostics.get(
                "daily_weather_quote_age_rows_with_timestamp"
            )
            step["daily_weather_quote_stale_max_age_seconds"] = skip_diagnostics.get(
                "daily_weather_quote_stale_max_age_seconds"
            )
            step["daily_weather_shadow_taker_rows_total"] = skip_diagnostics.get("daily_weather_shadow_taker_rows_total")
            step["daily_weather_shadow_taker_rows_with_orderable_yes_ask"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_rows_with_orderable_yes_ask"
            )
            step["daily_weather_shadow_taker_rows_with_orderable_no_ask"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_rows_with_orderable_no_ask"
            )
            step["daily_weather_shadow_taker_rows_with_any_orderable_ask"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_rows_with_any_orderable_ask"
            )
            step["daily_weather_shadow_taker_edge_above_min_count"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_edge_above_min_count"
            )
            step["daily_weather_shadow_taker_edge_net_fees_above_min_count"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_edge_net_fees_above_min_count"
            )
            step["daily_weather_shadow_taker_endpoint_orderbook_rows"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_endpoint_orderbook_rows"
            )
            step["daily_weather_best_shadow_taker_candidate"] = skip_diagnostics.get(
                "daily_weather_best_shadow_taker_candidate"
            )
            step["daily_weather_allowed_universe_rows_with_conservative_candidate"] = skip_diagnostics.get(
                "daily_weather_allowed_universe_rows_with_conservative_candidate"
            )
            step["daily_weather_endpoint_orderbook_filtered"] = skip_diagnostics.get(
                "daily_weather_endpoint_orderbook_filtered"
            )
            step["daily_weather_conservative_candidate_failure_counts"] = skip_diagnostics.get(
                "daily_weather_conservative_candidate_failure_counts"
            )
            step["daily_weather_planned_orders"] = skip_diagnostics.get("daily_weather_planned_orders")

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


def _daily_weather_recovery_failure_kind(
    *,
    stale_recovery_triggered: bool,
    stale_recovery_resolved: bool,
    recovery_capture_step: dict[str, Any] | None,
    prior_trader_step: dict[str, Any] | None,
) -> str | None:
    if not stale_recovery_triggered or stale_recovery_resolved:
        return None
    if isinstance(recovery_capture_step, dict):
        status = str(recovery_capture_step.get("status") or "").strip().lower()
        if status and status not in {"ready", "ok"}:
            return f"capture_{status}"
        scan_status = str(recovery_capture_step.get("scan_status") or "").strip().lower()
        if scan_status and scan_status not in {"ready", "ok"}:
            return f"capture_scan_{scan_status}"
    if isinstance(prior_trader_step, dict):
        gate_status = str(prior_trader_step.get("prior_trade_gate_status") or "").strip().lower()
        if gate_status:
            return f"gate_{gate_status}"
    return "unknown"


def _daily_weather_recovery_alert_state(
    *,
    run_root: Path,
    run_id: str,
    run_started_at_utc: str,
    stale_recovery_triggered: bool,
    stale_recovery_resolved: bool,
    failure_kind: str | None,
    recovery_capture_status: str | None,
    recovery_capture_scan_status: str | None,
    window_hours: float,
    threshold: int,
    max_events: int,
) -> dict[str, Any]:
    now_dt = _parse_iso(run_started_at_utc) or datetime.now(timezone.utc)
    window_seconds = max(0.0, float(window_hours)) * 3600.0
    threshold_effective = max(1, int(threshold))
    max_events_effective = max(25, int(max_events))
    state_path = run_root / "daily_weather_recovery_health.json"
    existing_events: list[dict[str, Any]] = []
    parse_error: str | None = None
    write_error: str | None = None

    existing_payload = _load_json(state_path)
    if isinstance(existing_payload, dict):
        raw_events = existing_payload.get("events")
        if isinstance(raw_events, list):
            for item in raw_events:
                if isinstance(item, dict):
                    existing_events.append(dict(item))

    cutoff = now_dt.timestamp() - window_seconds
    pruned_events: list[dict[str, Any]] = []
    for event in existing_events:
        occurred_text = str(event.get("occurred_at_utc") or "").strip()
        occurred_dt = _parse_iso(occurred_text)
        if occurred_dt is None:
            continue
        if occurred_dt.timestamp() < cutoff:
            continue
        pruned_events.append(event)

    if stale_recovery_triggered and not stale_recovery_resolved:
        pruned_events.append(
            {
                "run_id": run_id,
                "occurred_at_utc": now_dt.isoformat(),
                "failure_kind": str(failure_kind or "unknown"),
                "capture_status": str(recovery_capture_status or "") or None,
                "capture_scan_status": str(recovery_capture_scan_status or "") or None,
            }
        )

    if len(pruned_events) > max_events_effective:
        pruned_events = pruned_events[-max_events_effective:]

    failure_counts_by_kind: dict[str, int] = {}
    for event in pruned_events:
        kind = str(event.get("failure_kind") or "unknown").strip().lower() or "unknown"
        failure_counts_by_kind[kind] = failure_counts_by_kind.get(kind, 0) + 1
    failure_count_window = len(pruned_events)
    alert_triggered = failure_count_window >= threshold_effective

    payload = {
        "updated_at_utc": now_dt.isoformat(),
        "window_hours": max(0.0, float(window_hours)),
        "threshold": threshold_effective,
        "max_events": max_events_effective,
        "events": pruned_events,
    }
    try:
        state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as exc:
        write_error = str(exc)

    return {
        "state_file": str(state_path),
        "window_hours": max(0.0, float(window_hours)),
        "threshold": threshold_effective,
        "max_events": max_events_effective,
        "failure_count_window": failure_count_window,
        "failure_counts_by_kind": dict(sorted(failure_counts_by_kind.items())),
        "alert_triggered": alert_triggered,
        "last_failure_kind": str(failure_kind or "") or None,
        "parse_error": parse_error,
        "write_error": write_error,
    }


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


def _top_level_probe_policy(step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(step, dict):
        return {
            "enable_untrusted_bucket_probe_exploration": None,
            "untrusted_bucket_probe_exploration_enabled": None,
            "untrusted_bucket_probe_max_orders_per_run": None,
            "untrusted_bucket_probe_required_edge_buffer_dollars": None,
            "untrusted_bucket_probe_contracts_cap": None,
            "untrusted_bucket_probe_submitted_attempts": None,
            "untrusted_bucket_probe_blocked_attempts": None,
            "untrusted_bucket_probe_reason_counts": None,
        }
    return {
        "enable_untrusted_bucket_probe_exploration": step.get("enable_untrusted_bucket_probe_exploration"),
        "untrusted_bucket_probe_exploration_enabled": step.get("untrusted_bucket_probe_exploration_enabled"),
        "untrusted_bucket_probe_max_orders_per_run": step.get("untrusted_bucket_probe_max_orders_per_run"),
        "untrusted_bucket_probe_required_edge_buffer_dollars": _parse_float(
            step.get("untrusted_bucket_probe_required_edge_buffer_dollars")
        ),
        "untrusted_bucket_probe_contracts_cap": step.get("untrusted_bucket_probe_contracts_cap"),
        "untrusted_bucket_probe_submitted_attempts": step.get("untrusted_bucket_probe_submitted_attempts"),
        "untrusted_bucket_probe_blocked_attempts": step.get("untrusted_bucket_probe_blocked_attempts"),
        "untrusted_bucket_probe_reason_counts": step.get("untrusted_bucket_probe_reason_counts"),
    }


def _top_level_no_candidates_diagnostics(step: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(step, dict):
        return None
    gate_status = str(step.get("prior_trade_gate_status") or "").strip().lower()
    if gate_status != "no_candidates":
        return None
    return {
        "prior_trade_gate_status": gate_status,
        "prior_trade_gate_blockers": step.get("prior_trade_gate_blockers"),
        "plan_summary_file": step.get("prior_plan_summary_file"),
        "plan_summary_status": step.get("plan_summary_status"),
        "plan_summary_error": step.get("plan_summary_error"),
        "plan_summary_planned_orders": step.get("plan_summary_planned_orders"),
        "dominant_skip_reason": step.get("plan_skip_reason_dominant"),
        "dominant_skip_count": step.get("plan_skip_reason_dominant_count"),
        "dominant_skip_share": step.get("plan_skip_reason_dominant_share"),
        "skip_counts_total": step.get("plan_skip_counts_total"),
        "skip_counts_nonzero_total": step.get("plan_skip_counts_nonzero_total"),
        "top_skip_counts": step.get("plan_skip_counts_top"),
        "allowed_universe_candidate_pool_size": step.get("allowed_universe_candidate_pool_size"),
        "allowed_universe_dominant_skip_reason": step.get("allowed_universe_skip_reason_dominant"),
        "allowed_universe_dominant_skip_count": step.get("allowed_universe_skip_reason_dominant_count"),
        "allowed_universe_dominant_skip_share": step.get("allowed_universe_skip_reason_dominant_share"),
        "allowed_universe_skip_counts_total": step.get("allowed_universe_skip_counts_total"),
        "allowed_universe_skip_counts_nonzero_total": step.get("allowed_universe_skip_counts_nonzero_total"),
        "allowed_universe_top_skip_counts": step.get("allowed_universe_skip_counts_top"),
        "daily_weather_candidate_pool_size": step.get("daily_weather_candidate_pool_size"),
        "daily_weather_rows_total": step.get("daily_weather_rows_total"),
        "daily_weather_dominant_skip_reason": step.get("daily_weather_skip_reason_dominant"),
        "daily_weather_dominant_skip_count": step.get("daily_weather_skip_reason_dominant_count"),
        "daily_weather_dominant_skip_share": step.get("daily_weather_skip_reason_dominant_share"),
        "daily_weather_skip_counts_total": step.get("daily_weather_skip_counts_total"),
        "daily_weather_skip_counts_nonzero_total": step.get("daily_weather_skip_counts_nonzero_total"),
        "daily_weather_top_skip_counts": step.get("daily_weather_skip_counts_top"),
        "daily_weather_rows_with_conservative_candidate": step.get("daily_weather_rows_with_conservative_candidate"),
        "daily_weather_rows_with_both_sides_candidate": step.get("daily_weather_rows_with_both_sides_candidate"),
        "daily_weather_rows_with_one_side_failed": step.get("daily_weather_rows_with_one_side_failed"),
        "daily_weather_rows_with_both_sides_failed": step.get("daily_weather_rows_with_both_sides_failed"),
        "daily_weather_orderable_bid_rows": step.get("daily_weather_orderable_bid_rows"),
        "daily_weather_rows_with_any_orderable_bid": step.get("daily_weather_rows_with_any_orderable_bid"),
        "daily_weather_rows_with_any_orderable_ask": step.get("daily_weather_rows_with_any_orderable_ask"),
        "daily_weather_rows_with_fair_probabilities": step.get("daily_weather_rows_with_fair_probabilities"),
        "daily_weather_rows_with_both_quote_and_fair_value": step.get(
            "daily_weather_rows_with_both_quote_and_fair_value"
        ),
        "daily_weather_quote_orderability_counts": step.get("daily_weather_quote_orderability_counts"),
        "daily_weather_quote_age_rows_with_timestamp": step.get("daily_weather_quote_age_rows_with_timestamp"),
        "daily_weather_quote_stale_max_age_seconds": step.get("daily_weather_quote_stale_max_age_seconds"),
        "daily_weather_shadow_taker_rows_total": step.get("daily_weather_shadow_taker_rows_total"),
        "daily_weather_shadow_taker_rows_with_orderable_yes_ask": step.get(
            "daily_weather_shadow_taker_rows_with_orderable_yes_ask"
        ),
        "daily_weather_shadow_taker_rows_with_orderable_no_ask": step.get(
            "daily_weather_shadow_taker_rows_with_orderable_no_ask"
        ),
        "daily_weather_shadow_taker_rows_with_any_orderable_ask": step.get(
            "daily_weather_shadow_taker_rows_with_any_orderable_ask"
        ),
        "daily_weather_shadow_taker_edge_above_min_count": step.get(
            "daily_weather_shadow_taker_edge_above_min_count"
        ),
        "daily_weather_shadow_taker_edge_net_fees_above_min_count": step.get(
            "daily_weather_shadow_taker_edge_net_fees_above_min_count"
        ),
        "daily_weather_shadow_taker_endpoint_orderbook_rows": step.get(
            "daily_weather_shadow_taker_endpoint_orderbook_rows"
        ),
        "daily_weather_best_shadow_taker_candidate": step.get("daily_weather_best_shadow_taker_candidate"),
        "daily_weather_allowed_universe_rows_with_conservative_candidate": step.get(
            "daily_weather_allowed_universe_rows_with_conservative_candidate"
        ),
        "daily_weather_endpoint_orderbook_filtered": step.get("daily_weather_endpoint_orderbook_filtered"),
        "daily_weather_conservative_candidate_failure_counts": step.get(
            "daily_weather_conservative_candidate_failure_counts"
        ),
        "daily_weather_planned_orders": step.get("daily_weather_planned_orders"),
    }


def _top_level_daily_weather_funnel(
    *,
    prior_trader_step: dict[str, Any] | None,
    weather_prior_state_after: dict[str, Any],
    weather_history_state: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(prior_trader_step, dict):
        return {
            "captured_daily_weather_markets_total": None,
            "captured_daily_weather_markets_with_fresh_snapshot": None,
            "captured_daily_weather_family_counts": None,
            "daily_weather_board_age_seconds": None,
            "priors_allowed_daily_weather_rows_total": weather_prior_state_after.get("allowed_rows_total"),
            "priors_allowed_daily_weather_family_counts": weather_prior_state_after.get("allowed_family_counts"),
            "priors_weather_history_live_ready_rows": weather_history_state.get("weather_history_live_ready_rows"),
            "priors_weather_history_unhealthy_rows": weather_history_state.get("weather_history_unhealthy_rows"),
            "allowed_universe_candidate_pool_size": None,
            "daily_weather_candidate_pool_size": None,
            "daily_weather_rows_total": None,
            "daily_weather_rows_with_conservative_candidate": None,
            "daily_weather_rows_with_both_sides_candidate": None,
            "daily_weather_rows_with_one_side_failed": None,
            "daily_weather_rows_with_both_sides_failed": None,
            "daily_weather_orderable_bid_rows": None,
            "daily_weather_rows_with_any_orderable_bid": None,
            "daily_weather_rows_with_any_orderable_ask": None,
            "daily_weather_rows_with_fair_probabilities": None,
            "daily_weather_rows_with_both_quote_and_fair_value": None,
            "daily_weather_quote_orderability_counts": None,
            "daily_weather_quote_age_rows_with_timestamp": None,
            "daily_weather_quote_stale_max_age_seconds": None,
            "daily_weather_shadow_taker_rows_total": None,
            "daily_weather_shadow_taker_rows_with_orderable_yes_ask": None,
            "daily_weather_shadow_taker_rows_with_orderable_no_ask": None,
            "daily_weather_shadow_taker_rows_with_any_orderable_ask": None,
            "daily_weather_shadow_taker_edge_above_min_count": None,
            "daily_weather_shadow_taker_edge_net_fees_above_min_count": None,
            "daily_weather_shadow_taker_endpoint_orderbook_rows": None,
            "daily_weather_best_shadow_taker_candidate": None,
            "daily_weather_allowed_universe_rows_with_conservative_candidate": None,
            "daily_weather_endpoint_orderbook_filtered": None,
            "daily_weather_conservative_candidate_failure_counts": None,
            "daily_weather_skip_reason_dominant": None,
            "daily_weather_skip_counts_top": None,
            "daily_weather_planned_orders": None,
        }
    return {
        "captured_daily_weather_markets_total": prior_trader_step.get("daily_weather_markets"),
        "captured_daily_weather_markets_with_fresh_snapshot": prior_trader_step.get(
            "daily_weather_markets_with_fresh_snapshot"
        ),
        "captured_daily_weather_family_counts": prior_trader_step.get("daily_weather_family_counts"),
        "daily_weather_board_age_seconds": prior_trader_step.get("daily_weather_board_age_seconds"),
        "priors_allowed_daily_weather_rows_total": weather_prior_state_after.get("allowed_rows_total"),
        "priors_allowed_daily_weather_family_counts": weather_prior_state_after.get("allowed_family_counts"),
        "priors_weather_history_live_ready_rows": weather_history_state.get("weather_history_live_ready_rows"),
        "priors_weather_history_unhealthy_rows": weather_history_state.get("weather_history_unhealthy_rows"),
        "allowed_universe_candidate_pool_size": prior_trader_step.get("allowed_universe_candidate_pool_size"),
        "daily_weather_candidate_pool_size": prior_trader_step.get("daily_weather_candidate_pool_size"),
        "daily_weather_rows_total": prior_trader_step.get("daily_weather_rows_total"),
        "daily_weather_rows_with_conservative_candidate": prior_trader_step.get(
            "daily_weather_rows_with_conservative_candidate"
        ),
        "daily_weather_rows_with_both_sides_candidate": prior_trader_step.get(
            "daily_weather_rows_with_both_sides_candidate"
        ),
        "daily_weather_rows_with_one_side_failed": prior_trader_step.get("daily_weather_rows_with_one_side_failed"),
        "daily_weather_rows_with_both_sides_failed": prior_trader_step.get(
            "daily_weather_rows_with_both_sides_failed"
        ),
        "daily_weather_orderable_bid_rows": prior_trader_step.get("daily_weather_orderable_bid_rows"),
        "daily_weather_rows_with_any_orderable_bid": prior_trader_step.get(
            "daily_weather_rows_with_any_orderable_bid"
        ),
        "daily_weather_rows_with_any_orderable_ask": prior_trader_step.get(
            "daily_weather_rows_with_any_orderable_ask"
        ),
        "daily_weather_rows_with_fair_probabilities": prior_trader_step.get(
            "daily_weather_rows_with_fair_probabilities"
        ),
        "daily_weather_rows_with_both_quote_and_fair_value": prior_trader_step.get(
            "daily_weather_rows_with_both_quote_and_fair_value"
        ),
        "daily_weather_quote_orderability_counts": prior_trader_step.get("daily_weather_quote_orderability_counts"),
        "daily_weather_quote_age_rows_with_timestamp": prior_trader_step.get(
            "daily_weather_quote_age_rows_with_timestamp"
        ),
        "daily_weather_quote_stale_max_age_seconds": prior_trader_step.get(
            "daily_weather_quote_stale_max_age_seconds"
        ),
        "daily_weather_shadow_taker_rows_total": prior_trader_step.get("daily_weather_shadow_taker_rows_total"),
        "daily_weather_shadow_taker_rows_with_orderable_yes_ask": prior_trader_step.get(
            "daily_weather_shadow_taker_rows_with_orderable_yes_ask"
        ),
        "daily_weather_shadow_taker_rows_with_orderable_no_ask": prior_trader_step.get(
            "daily_weather_shadow_taker_rows_with_orderable_no_ask"
        ),
        "daily_weather_shadow_taker_rows_with_any_orderable_ask": prior_trader_step.get(
            "daily_weather_shadow_taker_rows_with_any_orderable_ask"
        ),
        "daily_weather_shadow_taker_edge_above_min_count": prior_trader_step.get(
            "daily_weather_shadow_taker_edge_above_min_count"
        ),
        "daily_weather_shadow_taker_edge_net_fees_above_min_count": prior_trader_step.get(
            "daily_weather_shadow_taker_edge_net_fees_above_min_count"
        ),
        "daily_weather_shadow_taker_endpoint_orderbook_rows": prior_trader_step.get(
            "daily_weather_shadow_taker_endpoint_orderbook_rows"
        ),
        "daily_weather_best_shadow_taker_candidate": prior_trader_step.get(
            "daily_weather_best_shadow_taker_candidate"
        ),
        "daily_weather_allowed_universe_rows_with_conservative_candidate": prior_trader_step.get(
            "daily_weather_allowed_universe_rows_with_conservative_candidate"
        ),
        "daily_weather_endpoint_orderbook_filtered": prior_trader_step.get("daily_weather_endpoint_orderbook_filtered"),
        "daily_weather_conservative_candidate_failure_counts": prior_trader_step.get(
            "daily_weather_conservative_candidate_failure_counts"
        ),
        "daily_weather_skip_reason_dominant": prior_trader_step.get("daily_weather_skip_reason_dominant"),
        "daily_weather_skip_counts_top": prior_trader_step.get("daily_weather_skip_counts_top"),
        "daily_weather_planned_orders": prior_trader_step.get("daily_weather_planned_orders"),
    }


def _collect_wakeup_transition_tickers(steps: list[dict[str, Any]]) -> list[str]:
    collected: list[str] = []
    seen: set[str] = set()
    for step in steps:
        name = str(step.get("name") or "").strip()
        if not name.startswith("daily_weather_ticker_refresh"):
            continue
        values = step.get("endpoint_to_orderable_transition_tickers")
        if not isinstance(values, list):
            continue
        for value in values:
            ticker = str(value or "").strip().upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            collected.append(ticker)
    return collected


def _estimate_wakeup_to_candidate_count(
    *,
    wakeup_tickers: list[str],
    priors_csv: Path,
    history_csv: Path,
) -> int:
    if not wakeup_tickers:
        return 0
    wakeup_set = {str(value or "").strip().upper() for value in wakeup_tickers if str(value or "").strip()}
    if not wakeup_set:
        return 0

    prior_by_ticker: dict[str, dict[str, Any]] = {}
    if priors_csv.exists():
        try:
            with priors_csv.open("r", newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    ticker = str(row.get("market_ticker") or "").strip().upper()
                    if ticker and ticker in wakeup_set:
                        prior_by_ticker[ticker] = row
        except Exception:
            prior_by_ticker = {}

    latest_by_ticker: dict[str, dict[str, Any]] = {}
    latest_ts_by_ticker: dict[str, datetime] = {}
    if history_csv.exists():
        try:
            with history_csv.open("r", newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    ticker = str(row.get("market_ticker") or "").strip().upper()
                    if not ticker or ticker not in wakeup_set:
                        continue
                    captured = _parse_iso(row.get("captured_at"))
                    if not isinstance(captured, datetime):
                        captured = datetime.fromtimestamp(0, tz=timezone.utc)
                    prev_ts = latest_ts_by_ticker.get(ticker)
                    if prev_ts is None or captured > prev_ts:
                        latest_ts_by_ticker[ticker] = captured
                        latest_by_ticker[ticker] = row
        except Exception:
            latest_by_ticker = {}

    candidate_count = 0
    for ticker in wakeup_set:
        prior_row = prior_by_ticker.get(ticker) or {}
        history_row = latest_by_ticker.get(ticker) or {}
        if not prior_row or not history_row:
            continue
        yes_bid = _parse_float(history_row.get("yes_bid_dollars"))
        no_bid = _parse_float(history_row.get("no_bid_dollars"))
        has_orderable_bid = _is_orderable_price(yes_bid) or _is_orderable_price(no_bid)
        fair_yes = _parse_float(prior_row.get("fair_yes_probability"))
        fair_no = _parse_float(prior_row.get("fair_no_probability"))
        if has_orderable_bid and (isinstance(fair_yes, float) or isinstance(fair_no, float)):
            candidate_count += 1
    return candidate_count


def _estimate_wakeup_to_planned_order_count(
    *,
    wakeup_tickers: list[str],
    wakeup_plan_step: dict[str, Any] | None,
) -> int:
    if not wakeup_tickers or not isinstance(wakeup_plan_step, dict):
        return 0
    wakeup_set = {str(value or "").strip().upper() for value in wakeup_tickers if str(value or "").strip()}
    if not wakeup_set:
        return 0
    top_plan_tickers = wakeup_plan_step.get("top_plans_tickers")
    if not isinstance(top_plan_tickers, list):
        return 0
    count = 0
    for value in top_plan_tickers:
        ticker = str(value or "").strip().upper()
        if ticker and ticker in wakeup_set:
            count += 1
    return count


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
    requested_env_file = Path(os.environ["BETBOT_ENV_FILE"])
    env_resolution = _resolve_env_file(requested_path=requested_env_file, repo_root=repo_root)
    env_file = Path(str(env_resolution.get("env_file_effective") or requested_env_file))
    env_file_values: dict[str, str] = {}
    env_file_parse_error: str | None = None
    try:
        env_file_values = _parse_env_values(env_file)
    except Exception as exc:  # pragma: no cover - defensive fallback
        env_file_values = {}
        env_file_parse_error = str(exc)
    weather_history_token_state = _resolve_weather_history_token(
        env_values=env_file_values,
        repo_root=repo_root,
    )
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
                    "env_file_requested": env_resolution.get("env_file_requested"),
                    "env_file_effective": env_resolution.get("env_file_effective"),
                    "env_file_source": env_resolution.get("env_file_source"),
                    "env_file_resolution_reason": env_resolution.get("env_file_resolution_reason"),
                    "env_file_kalshi_ready": env_resolution.get("env_file_kalshi_ready"),
                    "env_file_kalshi_ready_reason": env_resolution.get("env_file_kalshi_ready_reason"),
                    "env_file_loaded_key_count": len(env_file_values),
                    "env_file_parse_error": env_file_parse_error,
                    "weather_history_token_present": weather_history_token_state.get("weather_history_token_present"),
                    "weather_history_token_env_key": weather_history_token_state.get("weather_history_token_env_key"),
                    "weather_history_token_source": weather_history_token_state.get("weather_history_token_source"),
                    "weather_history_token_file_used": weather_history_token_state.get("weather_history_token_file_used"),
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
                    "probe_policy": _top_level_probe_policy(None),
                    "no_candidates_diagnostics": None,
                    "daily_weather_funnel": _top_level_daily_weather_funnel(
                        prior_trader_step=None,
                        weather_prior_state_after={},
                        weather_history_state={},
                    ),
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
            "env_file_requested": env_resolution.get("env_file_requested"),
            "env_file_effective": env_resolution.get("env_file_effective"),
            "env_file_source": env_resolution.get("env_file_source"),
            "env_file_resolution_reason": env_resolution.get("env_file_resolution_reason"),
            "env_file_kalshi_ready": env_resolution.get("env_file_kalshi_ready"),
            "env_file_kalshi_ready_reason": env_resolution.get("env_file_kalshi_ready_reason"),
            "env_file_loaded_key_count": len(env_file_values),
            "env_file_parse_error": env_file_parse_error,
            "weather_history_token_present": weather_history_token_state.get("weather_history_token_present"),
            "weather_history_token_env_key": weather_history_token_state.get("weather_history_token_env_key"),
            "weather_history_token_source": weather_history_token_state.get("weather_history_token_source"),
            "weather_history_token_file_used": weather_history_token_state.get("weather_history_token_file_used"),
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
            "probe_policy": _top_level_probe_policy(None),
            "no_candidates_diagnostics": None,
            "daily_weather_funnel": _top_level_daily_weather_funnel(
                prior_trader_step=None,
                weather_prior_state_after={},
                weather_history_state={},
            ),
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
            env_overrides=env_file_values,
        )
    )

    capture_max_hours_to_close = max(
        1.0,
        float(os.environ.get("BETBOT_CAPTURE_MAX_HOURS_TO_CLOSE", "4000")),
    )
    capture_page_limit = max(
        1,
        int(os.environ.get("BETBOT_CAPTURE_PAGE_LIMIT", "200")),
    )
    capture_max_pages = max(
        1,
        int(os.environ.get("BETBOT_CAPTURE_MAX_PAGES", "12")),
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
                str(capture_max_hours_to_close),
                "--page-limit",
                str(capture_page_limit),
                "--max-pages",
                str(capture_max_pages),
            ],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
    )

    allowed_weather_contract_families = _parse_csv_list(
        os.environ.get("BETBOT_WEATHER_ALLOWED_CONTRACT_FAMILIES")
    )
    if not allowed_weather_contract_families:
        allowed_weather_contract_families = ["daily_rain", "daily_temperature"]
    daily_weather_ticker_refresh_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_ENABLED"),
        default=True,
    )
    daily_weather_ticker_refresh_max_markets = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_MAX_MARKETS", "20")),
    )
    daily_weather_ticker_refresh_watch_max_markets = max(
        0,
        int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_MAX_MARKETS", "4")),
    )
    daily_weather_ticker_refresh_watch_interval_runs = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_INTERVAL_RUNS", "3")),
    )
    daily_weather_ticker_refresh_state_file = str(
        os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_STATE_FILE")
        or (output_dir / "overnight_alpha" / "daily_weather_ticker_refresh_state.json")
    )
    daily_weather_wakeup_burst_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_WAKEUP_BURST_ENABLED"),
        default=True,
    )
    daily_weather_wakeup_burst_max_markets = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_WAKEUP_BURST_MAX_MARKETS", "4")),
    )
    daily_weather_wakeup_burst_sleep_seconds = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_WAKEUP_BURST_SLEEP_SECONDS", "1")),
    )
    daily_weather_wakeup_reprioritize_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_WAKEUP_REPRIORITIZE_ENABLED"),
        default=True,
    )
    daily_weather_micro_watch_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_ENABLED"),
        default=True,
    )
    daily_weather_micro_watch_interval_seconds = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_INTERVAL_SECONDS", "180")),
    )
    daily_weather_micro_watch_max_polls = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_MAX_POLLS", "4")),
    )
    daily_weather_micro_watch_active_hours_to_close = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_ACTIVE_HOURS_TO_CLOSE", "2")),
    )
    daily_weather_micro_watch_max_markets = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_MAX_MARKETS", "12")),
    )
    daily_weather_micro_watch_include_unknown_hours_to_close = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_INCLUDE_UNKNOWN_HOURS_TO_CLOSE"),
        default=False,
    )
    daily_weather_ticker_refresh_on_base_capture = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_ON_BASE_CAPTURE"),
        default=True,
    )
    base_refresh_step: dict[str, Any] | None = None
    micro_watch_step: dict[str, Any] | None = None
    micro_watch_poll_steps: list[dict[str, Any]] = []
    wakeup_burst_step: dict[str, Any] | None = None
    if daily_weather_ticker_refresh_enabled and daily_weather_ticker_refresh_on_base_capture:
        base_refresh_step = _run_daily_weather_ticker_refresh(
            env_values=env_file_values,
            priors_csv=priors_csv,
            history_csv=history_csv,
            allowed_contract_families=allowed_weather_contract_families,
            timeout_seconds=max(1.0, float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            max_markets=daily_weather_ticker_refresh_max_markets,
            captured_at=datetime.now(timezone.utc),
        )
        base_refresh_step["name"] = "daily_weather_ticker_refresh_base"
        base_refresh_step["base_capture_refresh"] = True
        steps.append(base_refresh_step)
    elif daily_weather_ticker_refresh_enabled:
        steps.append(
            _synthetic_step(
                name="daily_weather_ticker_refresh_base",
                status="skipped_base_refresh_disabled",
                ok=True,
                reason="daily_weather_ticker_refresh_on_base_capture_disabled",
            )
        )
    else:
        steps.append(
            _synthetic_step(
                name="daily_weather_ticker_refresh_base",
                status="skipped_disabled",
                ok=True,
                reason="daily_weather_ticker_refresh_disabled",
            )
        )

    if daily_weather_micro_watch_enabled and daily_weather_ticker_refresh_enabled:
        micro_watch_result = _run_daily_weather_micro_watch(
            env_values=env_file_values,
            priors_csv=priors_csv,
            history_csv=history_csv,
            allowed_contract_families=allowed_weather_contract_families,
            timeout_seconds=max(1.0, float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            max_markets=daily_weather_micro_watch_max_markets,
            captured_at=datetime.now(timezone.utc),
            poll_interval_seconds=daily_weather_micro_watch_interval_seconds,
            max_polls=daily_weather_micro_watch_max_polls,
            active_hours_to_close=daily_weather_micro_watch_active_hours_to_close,
            include_unknown_hours_to_close=daily_weather_micro_watch_include_unknown_hours_to_close,
        )
        micro_watch_poll_steps = list(micro_watch_result.get("poll_steps") or [])
        for poll_step in micro_watch_poll_steps:
            steps.append(poll_step)
        micro_watch_step = (
            micro_watch_result.get("summary_step")
            if isinstance(micro_watch_result.get("summary_step"), dict)
            else None
        )
        if isinstance(micro_watch_step, dict):
            steps.append(micro_watch_step)
    elif daily_weather_micro_watch_enabled:
        micro_watch_step = _synthetic_step(
            name="daily_weather_micro_watch",
            status="skipped_daily_weather_ticker_refresh_disabled",
            ok=True,
            reason="daily_weather_ticker_refresh_disabled",
        )
        steps.append(micro_watch_step)
    else:
        micro_watch_step = _synthetic_step(
            name="daily_weather_micro_watch",
            status="skipped_disabled",
            ok=True,
            reason="daily_weather_micro_watch_disabled",
        )
        steps.append(micro_watch_step)

    wakeup_transition_tickers_before_burst = _collect_wakeup_transition_tickers(steps)
    if (
        daily_weather_wakeup_burst_enabled
        and daily_weather_ticker_refresh_enabled
        and wakeup_transition_tickers_before_burst
    ):
        if daily_weather_wakeup_burst_sleep_seconds > 0:
            time.sleep(daily_weather_wakeup_burst_sleep_seconds)
        wakeup_burst_tickers = wakeup_transition_tickers_before_burst[:daily_weather_wakeup_burst_max_markets]
        wakeup_burst_step = _run_daily_weather_ticker_refresh(
            env_values=env_file_values,
            priors_csv=priors_csv,
            history_csv=history_csv,
            allowed_contract_families=allowed_weather_contract_families,
            timeout_seconds=max(1.0, float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            max_markets=max(1, len(wakeup_burst_tickers)),
            captured_at=datetime.now(timezone.utc),
            explicit_market_tickers=wakeup_burst_tickers,
        )
        wakeup_burst_step["name"] = "daily_weather_ticker_refresh_wakeup_burst"
        wakeup_burst_step["wakeup_transition_source"] = "base_refresh"
        wakeup_burst_step["wakeup_transition_tickers"] = wakeup_burst_tickers
        steps.append(wakeup_burst_step)
    elif daily_weather_wakeup_burst_enabled:
        steps.append(
            _synthetic_step(
                name="daily_weather_ticker_refresh_wakeup_burst",
                status="skipped_no_wakeup_transition",
                ok=True,
                reason="no_endpoint_to_orderable_transition_detected",
            )
        )
    else:
        steps.append(
            _synthetic_step(
                name="daily_weather_ticker_refresh_wakeup_burst",
                status="skipped_disabled",
                ok=True,
                reason="daily_weather_wakeup_burst_disabled",
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
            env_overrides=env_file_values,
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

    weather_prior_state_before = _weather_prior_state(
        priors_csv=priors_csv,
        max_age_hours=float(os.environ.get("BETBOT_WEATHER_PRIOR_MAX_AGE_HOURS", "6")),
        allowed_contract_families=allowed_weather_contract_families,
    )
    force_weather_prior_refresh_reasons: list[str] = []
    if isinstance(base_refresh_step, dict):
        base_rows_appended = base_refresh_step.get("rows_appended")
        if isinstance(base_rows_appended, (int, float)) and int(base_rows_appended) > 0:
            force_weather_prior_refresh_reasons.append("base_daily_weather_refresh_appended_rows")
    if isinstance(wakeup_burst_step, dict):
        burst_rows_appended = wakeup_burst_step.get("rows_appended")
        if isinstance(burst_rows_appended, (int, float)) and int(burst_rows_appended) > 0:
            force_weather_prior_refresh_reasons.append("wakeup_transition_burst_refresh_appended_rows")
        burst_transition_count = wakeup_burst_step.get("endpoint_to_orderable_transition_count")
        if isinstance(burst_transition_count, (int, float)) and int(burst_transition_count) > 0:
            force_weather_prior_refresh_reasons.append("wakeup_transition_detected")
    force_weather_prior_refresh_reasons = list(dict.fromkeys(force_weather_prior_refresh_reasons))
    force_weather_prior_refresh_reason = (
        ",".join(force_weather_prior_refresh_reasons)
        if force_weather_prior_refresh_reasons
        else None
    )

    if bool(weather_prior_state_before.get("stale")) or force_weather_prior_refresh_reason is not None:
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
            env_overrides=env_file_values,
        )
        weather_prior_step["prior_state_before"] = weather_prior_state_before
        weather_prior_step["forced_refresh_reason"] = force_weather_prior_refresh_reason
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

    wakeup_reprioritize_step: dict[str, Any] | None = None
    wakeup_transition_tickers_for_reprioritize = _collect_wakeup_transition_tickers(steps)
    if daily_weather_wakeup_reprioritize_enabled and wakeup_transition_tickers_for_reprioritize:
        wakeup_reprioritize_step = _run_step(
            name="prior_plan_wakeup_reprioritize",
            launcher=launcher,
            args=[
                "kalshi-micro-prior-plan",
                "--env-file",
                str(env_file),
                "--priors-csv",
                str(priors_csv),
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
            ],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        wakeup_reprioritize_step["wakeup_transition_tickers"] = wakeup_transition_tickers_for_reprioritize
        wakeup_reprioritize_step["wakeup_transition_count"] = len(wakeup_transition_tickers_for_reprioritize)
        steps.append(wakeup_reprioritize_step)
    elif daily_weather_wakeup_reprioritize_enabled:
        steps.append(
            _synthetic_step(
                name="prior_plan_wakeup_reprioritize",
                status="skipped_no_wakeup_transition",
                ok=True,
                reason="no_endpoint_to_orderable_transition_detected",
            )
        )
    else:
        steps.append(
            _synthetic_step(
                name="prior_plan_wakeup_reprioritize",
                status="skipped_disabled",
                ok=True,
                reason="daily_weather_wakeup_reprioritize_disabled",
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
        env_overrides=env_file_values,
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
            env_overrides=env_file_values,
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

    stale_recovery_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_STALE_RECOVERY_ENABLED"),
        default=True,
    )
    stale_recovery_max_retries = max(
        0,
        int(os.environ.get("BETBOT_DAILY_WEATHER_STALE_RECOVERY_MAX_RETRIES", "1")),
    )
    stale_recovery_sleep_seconds = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_STALE_RECOVERY_SLEEP_SECONDS", "2")),
    )
    stale_recovery_attempts = 0
    stale_recovery_triggered = False
    stale_recovery_resolved = False
    stale_recovery_capture_max_hours_to_close = max(
        capture_max_hours_to_close,
        float(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_MAX_HOURS_TO_CLOSE", "6000")),
    )
    stale_recovery_capture_page_limit = max(
        capture_page_limit,
        int(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_PAGE_LIMIT", "300")),
    )
    stale_recovery_capture_max_pages = max(
        capture_max_pages,
        int(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_MAX_PAGES", "24")),
    )

    prior_trader_step = _run_step(
        name="prior_trader_dry_run",
        launcher=launcher,
        args=prior_trader_args,
        cwd=repo_root,
        run_dir=run_logs,
        env_overrides=env_file_values,
    )
    steps.append(prior_trader_step)

    while (
        stale_recovery_enabled
        and stale_recovery_attempts < stale_recovery_max_retries
        and _is_daily_weather_board_stale_gate(prior_trader_step)
    ):
        stale_recovery_triggered = True
        stale_recovery_attempts += 1
        if stale_recovery_sleep_seconds > 0:
            time.sleep(stale_recovery_sleep_seconds)
        capture_retry_step = _run_step(
            name=f"capture_recovery_daily_weather_{stale_recovery_attempts}",
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
                str(stale_recovery_capture_max_hours_to_close),
                "--page-limit",
                str(stale_recovery_capture_page_limit),
                "--max-pages",
                str(stale_recovery_capture_max_pages),
            ],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        capture_retry_step["stale_recovery_attempt"] = stale_recovery_attempts
        capture_retry_step["stale_recovery_trigger_gate_status"] = "daily_weather_board_stale"
        capture_retry_step["stale_recovery_capture_max_hours_to_close"] = stale_recovery_capture_max_hours_to_close
        capture_retry_step["stale_recovery_capture_page_limit"] = stale_recovery_capture_page_limit
        capture_retry_step["stale_recovery_capture_max_pages"] = stale_recovery_capture_max_pages
        steps.append(capture_retry_step)

        if daily_weather_ticker_refresh_enabled:
            ticker_refresh_step = _run_daily_weather_ticker_refresh(
                env_values=env_file_values,
                priors_csv=priors_csv,
                history_csv=history_csv,
                allowed_contract_families=allowed_weather_contract_families,
                timeout_seconds=float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15")),
                max_markets=daily_weather_ticker_refresh_max_markets,
                captured_at=datetime.now(timezone.utc),
            )
            ticker_refresh_step["stale_recovery_attempt"] = stale_recovery_attempts
            ticker_refresh_step["stale_recovery_trigger_gate_status"] = "daily_weather_board_stale"
            steps.append(ticker_refresh_step)
        else:
            steps.append(
                _synthetic_step(
                    name=f"daily_weather_ticker_refresh_{stale_recovery_attempts}",
                    status="skipped_disabled",
                    ok=True,
                    reason="daily_weather_ticker_refresh_disabled",
                    payload={
                        "stale_recovery_attempt": stale_recovery_attempts,
                        "stale_recovery_trigger_gate_status": "daily_weather_board_stale",
                    },
                )
            )

        prior_trader_step = _run_step(
            name=f"prior_trader_dry_run_retry_{stale_recovery_attempts}",
            launcher=launcher,
            args=prior_trader_args,
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        prior_trader_step["stale_recovery_attempt"] = stale_recovery_attempts
        prior_trader_step["stale_recovery_trigger_gate_status"] = "daily_weather_board_stale"
        steps.append(prior_trader_step)

    if stale_recovery_triggered and not _is_daily_weather_board_stale_gate(prior_trader_step):
        stale_recovery_resolved = True

    recovery_capture_step = _latest_step_with_prefix(steps, "capture_recovery_daily_weather")
    daily_weather_recovery_failure_kind = _daily_weather_recovery_failure_kind(
        stale_recovery_triggered=stale_recovery_triggered,
        stale_recovery_resolved=stale_recovery_resolved,
        recovery_capture_step=recovery_capture_step if isinstance(recovery_capture_step, dict) else None,
        prior_trader_step=prior_trader_step if isinstance(prior_trader_step, dict) else None,
    )
    daily_weather_recovery_alert_window_hours = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_ALERT_WINDOW_HOURS", "6")),
    )
    daily_weather_recovery_alert_threshold = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_ALERT_THRESHOLD", "3")),
    )
    daily_weather_recovery_alert_max_events = max(
        25,
        int(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_ALERT_MAX_EVENTS", "500")),
    )
    daily_weather_recovery_alert_state = _daily_weather_recovery_alert_state(
        run_root=run_root,
        run_id=run_id,
        run_started_at_utc=started_at,
        stale_recovery_triggered=stale_recovery_triggered,
        stale_recovery_resolved=stale_recovery_resolved,
        failure_kind=daily_weather_recovery_failure_kind,
        recovery_capture_status=(
            str(recovery_capture_step.get("status") or "").strip()
            if isinstance(recovery_capture_step, dict)
            else None
        ),
        recovery_capture_scan_status=(
            str(recovery_capture_step.get("scan_status") or "").strip()
            if isinstance(recovery_capture_step, dict)
            else None
        ),
        window_hours=daily_weather_recovery_alert_window_hours,
        threshold=daily_weather_recovery_alert_threshold,
        max_events=daily_weather_recovery_alert_max_events,
    )
    steps.append(
        _synthetic_step(
            name="daily_weather_stale_recovery_health",
            status="alert" if bool(daily_weather_recovery_alert_state.get("alert_triggered")) else "ready",
            ok=True,
            reason=(
                f"failure_burst:{daily_weather_recovery_failure_kind or 'none'}"
                if bool(daily_weather_recovery_alert_state.get("alert_triggered"))
                else "within_threshold"
            ),
            payload=dict(daily_weather_recovery_alert_state),
        )
    )
    daily_weather_ticker_refresh_step = next(
        (
            step
            for step in reversed(steps)
            if str(step.get("name") or "").startswith("daily_weather_ticker_refresh")
            and str(step.get("name") or "").strip()
            not in {"daily_weather_ticker_refresh_base", "daily_weather_ticker_refresh_wakeup_burst"}
        ),
        None,
    )
    daily_weather_ticker_refresh_base_step = next(
        (step for step in steps if str(step.get("name") or "").strip() == "daily_weather_ticker_refresh_base"),
        None,
    )
    micro_watch_step = next(
        (step for step in reversed(steps) if str(step.get("name") or "").strip() == "daily_weather_micro_watch"),
        None,
    )
    wakeup_burst_step = next(
        (step for step in reversed(steps) if str(step.get("name") or "").strip() == "daily_weather_ticker_refresh_wakeup_burst"),
        None,
    )

    failed_steps = [step["name"] for step in steps if not bool(step.get("ok"))]
    degraded_reasons: list[str] = []

    prior_trader_step = _latest_step_with_prefix(steps, "prior_trader_dry_run")
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
    weather_history_state = _weather_history_readiness_state(
        priors_csv=priors_csv,
        allowed_contract_families=allowed_weather_contract_families,
    )
    steps.append(
        _synthetic_step(
            name="weather_history_health",
            status="ready" if not weather_history_state.get("weather_history_parse_error") else "degraded",
            ok=True,
            reason=(
                None
                if not weather_history_state.get("weather_history_parse_error")
                else f"weather_history_parse_error:{weather_history_state.get('weather_history_parse_error')}"
            ),
            payload=dict(weather_history_state),
        )
    )

    live_blockers: list[str] = []
    if isinstance(balance_step, dict) and not bool(balance_step.get("balance_live_ready")):
        live_blockers.extend(list(balance_step.get("balance_blockers") or []))
    if isinstance(balance_smoke_step, dict) and not bool(balance_smoke_step.get("kalshi_ok")):
        failure_kind = str(balance_smoke_step.get("kalshi_failure_kind") or "").strip()
        if failure_kind:
            live_blockers.append(f"balance_smoke_{failure_kind}")
    if not bool(env_resolution.get("env_file_kalshi_ready")):
        live_blockers.append("env_file_missing_kalshi_credentials")
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
    if not bool(weather_history_token_state.get("weather_history_token_present")) and int(
        weather_history_state.get("weather_history_missing_token_count") or 0
    ) > 0:
        live_blockers.append("weather_history_missing_noaa_token")
    if int(weather_history_state.get("weather_history_station_mapping_missing_count") or 0) > 0:
        live_blockers.append("weather_history_station_mapping_missing")
    if int(weather_history_state.get("weather_history_sample_depth_block_count") or 0) > 0:
        live_blockers.append("weather_history_sample_depth_insufficient")
    if (
        bool(daily_weather_recovery_alert_state.get("alert_triggered"))
        and stale_recovery_triggered
        and not stale_recovery_resolved
    ):
        live_blockers.append("daily_weather_recovery_failure_burst")
        if str(daily_weather_recovery_failure_kind or "").strip().lower().startswith("capture_upstream_error"):
            live_blockers.append("daily_weather_recovery_upstream_error_burst")
    live_blockers = _dedupe(live_blockers)
    pipeline_ready = overall_status == "ok"
    live_ready = pipeline_ready and not live_blockers
    top_level_balance = _top_level_balance_heartbeat(balance_step if isinstance(balance_step, dict) else None)
    top_level_frontier = _top_level_execution_frontier(frontier_step if isinstance(frontier_step, dict) else None)
    decision_identity = _top_level_decision_identity(prior_trader_step if isinstance(prior_trader_step, dict) else None)
    probe_policy = _top_level_probe_policy(prior_trader_step if isinstance(prior_trader_step, dict) else None)
    no_candidates_diagnostics = _top_level_no_candidates_diagnostics(
        prior_trader_step if isinstance(prior_trader_step, dict) else None
    )
    wakeup_transition_tickers = _collect_wakeup_transition_tickers(steps)
    wakeup_plan_step = next(
        (step for step in reversed(steps) if str(step.get("name") or "").strip() == "prior_plan_wakeup_reprioritize"),
        None,
    )
    wakeup_to_candidate_count = _estimate_wakeup_to_candidate_count(
        wakeup_tickers=wakeup_transition_tickers,
        priors_csv=priors_csv,
        history_csv=history_csv,
    )
    wakeup_to_planned_order_count = _estimate_wakeup_to_planned_order_count(
        wakeup_tickers=wakeup_transition_tickers,
        wakeup_plan_step=wakeup_plan_step if isinstance(wakeup_plan_step, dict) else None,
    )
    wakeup_funnel = {
        "daily_weather_watch_wakeup_count": len(wakeup_transition_tickers),
        "daily_weather_watch_wakeup_tickers": wakeup_transition_tickers[:25],
        "daily_weather_watch_wakeup_to_candidate_count": wakeup_to_candidate_count,
        "daily_weather_watch_wakeup_to_planned_order_count": wakeup_to_planned_order_count,
    }
    daily_weather_funnel = _top_level_daily_weather_funnel(
        prior_trader_step=prior_trader_step if isinstance(prior_trader_step, dict) else None,
        weather_prior_state_after=weather_prior_state_after,
        weather_history_state=weather_history_state,
    )
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
        "env_file_requested": env_resolution.get("env_file_requested"),
        "env_file_effective": env_resolution.get("env_file_effective"),
        "env_file_source": env_resolution.get("env_file_source"),
        "env_file_resolution_reason": env_resolution.get("env_file_resolution_reason"),
        "env_file_kalshi_ready": env_resolution.get("env_file_kalshi_ready"),
        "env_file_kalshi_ready_reason": env_resolution.get("env_file_kalshi_ready_reason"),
        "env_file_loaded_key_count": len(env_file_values),
        "env_file_parse_error": env_file_parse_error,
        "weather_history_token_present": weather_history_token_state.get("weather_history_token_present"),
        "weather_history_token_env_key": weather_history_token_state.get("weather_history_token_env_key"),
        "weather_history_token_source": weather_history_token_state.get("weather_history_token_source"),
        "weather_history_token_file_used": weather_history_token_state.get("weather_history_token_file_used"),
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
        "probe_policy": probe_policy,
        "no_candidates_diagnostics": no_candidates_diagnostics,
        "daily_weather_funnel": daily_weather_funnel,
        "wakeup_funnel": wakeup_funnel,
        "prior_trade_gate_status": (
            prior_trader_step.get("prior_trade_gate_status")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "prior_trade_gate_blockers": (
            prior_trader_step.get("prior_trade_gate_blockers")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "prior_plan_summary_file": (
            prior_trader_step.get("prior_plan_summary_file")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_summary_status": (
            prior_trader_step.get("plan_summary_status")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_summary_error": (
            prior_trader_step.get("plan_summary_error")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_summary_planned_orders": (
            prior_trader_step.get("plan_summary_planned_orders")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_reason_dominant": (
            prior_trader_step.get("plan_skip_reason_dominant")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_reason_dominant_count": (
            prior_trader_step.get("plan_skip_reason_dominant_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_reason_dominant_share": (
            prior_trader_step.get("plan_skip_reason_dominant_share")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_counts_total": (
            prior_trader_step.get("plan_skip_counts_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_counts_nonzero_total": (
            prior_trader_step.get("plan_skip_counts_nonzero_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_counts_top": (
            prior_trader_step.get("plan_skip_counts_top")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_candidate_pool_size": (
            prior_trader_step.get("allowed_universe_candidate_pool_size")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_reason_dominant": (
            prior_trader_step.get("allowed_universe_skip_reason_dominant")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_reason_dominant_count": (
            prior_trader_step.get("allowed_universe_skip_reason_dominant_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_reason_dominant_share": (
            prior_trader_step.get("allowed_universe_skip_reason_dominant_share")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_counts_total": (
            prior_trader_step.get("allowed_universe_skip_counts_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_counts_nonzero_total": (
            prior_trader_step.get("allowed_universe_skip_counts_nonzero_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_counts_top": (
            prior_trader_step.get("allowed_universe_skip_counts_top")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_candidate_pool_size": (
            prior_trader_step.get("daily_weather_candidate_pool_size")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_total": (
            prior_trader_step.get("daily_weather_rows_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_reason_dominant": (
            prior_trader_step.get("daily_weather_skip_reason_dominant")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_reason_dominant_count": (
            prior_trader_step.get("daily_weather_skip_reason_dominant_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_reason_dominant_share": (
            prior_trader_step.get("daily_weather_skip_reason_dominant_share")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_counts_total": (
            prior_trader_step.get("daily_weather_skip_counts_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_counts_nonzero_total": (
            prior_trader_step.get("daily_weather_skip_counts_nonzero_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_counts_top": (
            prior_trader_step.get("daily_weather_skip_counts_top")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_conservative_candidate": (
            prior_trader_step.get("daily_weather_rows_with_conservative_candidate")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_both_sides_candidate": (
            prior_trader_step.get("daily_weather_rows_with_both_sides_candidate")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_one_side_failed": (
            prior_trader_step.get("daily_weather_rows_with_one_side_failed")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_both_sides_failed": (
            prior_trader_step.get("daily_weather_rows_with_both_sides_failed")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_orderable_bid_rows": (
            prior_trader_step.get("daily_weather_orderable_bid_rows")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_any_orderable_bid": (
            prior_trader_step.get("daily_weather_rows_with_any_orderable_bid")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_any_orderable_ask": (
            prior_trader_step.get("daily_weather_rows_with_any_orderable_ask")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_fair_probabilities": (
            prior_trader_step.get("daily_weather_rows_with_fair_probabilities")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_both_quote_and_fair_value": (
            prior_trader_step.get("daily_weather_rows_with_both_quote_and_fair_value")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_quote_orderability_counts": (
            prior_trader_step.get("daily_weather_quote_orderability_counts")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_quote_age_rows_with_timestamp": (
            prior_trader_step.get("daily_weather_quote_age_rows_with_timestamp")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_quote_stale_max_age_seconds": (
            prior_trader_step.get("daily_weather_quote_stale_max_age_seconds")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_rows_total": (
            prior_trader_step.get("daily_weather_shadow_taker_rows_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_rows_with_orderable_yes_ask": (
            prior_trader_step.get("daily_weather_shadow_taker_rows_with_orderable_yes_ask")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_rows_with_orderable_no_ask": (
            prior_trader_step.get("daily_weather_shadow_taker_rows_with_orderable_no_ask")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_rows_with_any_orderable_ask": (
            prior_trader_step.get("daily_weather_shadow_taker_rows_with_any_orderable_ask")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_edge_above_min_count": (
            prior_trader_step.get("daily_weather_shadow_taker_edge_above_min_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_edge_net_fees_above_min_count": (
            prior_trader_step.get("daily_weather_shadow_taker_edge_net_fees_above_min_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_endpoint_orderbook_rows": (
            prior_trader_step.get("daily_weather_shadow_taker_endpoint_orderbook_rows")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_best_shadow_taker_candidate": (
            prior_trader_step.get("daily_weather_best_shadow_taker_candidate")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_allowed_universe_rows_with_conservative_candidate": (
            prior_trader_step.get("daily_weather_allowed_universe_rows_with_conservative_candidate")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_endpoint_orderbook_filtered": (
            prior_trader_step.get("daily_weather_endpoint_orderbook_filtered")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_conservative_candidate_failure_counts": (
            prior_trader_step.get("daily_weather_conservative_candidate_failure_counts")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_planned_orders": (
            prior_trader_step.get("daily_weather_planned_orders")
            if isinstance(prior_trader_step, dict)
            else None
        ),
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
        "weather_prior_refresh_forced_reason": (
            weather_prior_step.get("forced_refresh_reason")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "daily_weather_stale_recovery_enabled": stale_recovery_enabled,
        "daily_weather_stale_recovery_max_retries": stale_recovery_max_retries,
        "daily_weather_stale_recovery_sleep_seconds": stale_recovery_sleep_seconds,
        "daily_weather_ticker_refresh_enabled": daily_weather_ticker_refresh_enabled,
        "daily_weather_ticker_refresh_on_base_capture": daily_weather_ticker_refresh_on_base_capture,
        "daily_weather_ticker_refresh_max_markets": daily_weather_ticker_refresh_max_markets,
        "daily_weather_ticker_refresh_watch_max_markets": daily_weather_ticker_refresh_watch_max_markets,
        "daily_weather_ticker_refresh_watch_interval_runs": daily_weather_ticker_refresh_watch_interval_runs,
        "daily_weather_ticker_refresh_state_file": daily_weather_ticker_refresh_state_file,
        "daily_weather_wakeup_burst_enabled": daily_weather_wakeup_burst_enabled,
        "daily_weather_wakeup_burst_max_markets": daily_weather_wakeup_burst_max_markets,
        "daily_weather_wakeup_burst_sleep_seconds": daily_weather_wakeup_burst_sleep_seconds,
        "daily_weather_wakeup_reprioritize_enabled": daily_weather_wakeup_reprioritize_enabled,
        "daily_weather_micro_watch_enabled": daily_weather_micro_watch_enabled,
        "daily_weather_micro_watch_interval_seconds": daily_weather_micro_watch_interval_seconds,
        "daily_weather_micro_watch_max_polls": daily_weather_micro_watch_max_polls,
        "daily_weather_micro_watch_active_hours_to_close": daily_weather_micro_watch_active_hours_to_close,
        "daily_weather_micro_watch_max_markets": daily_weather_micro_watch_max_markets,
        "daily_weather_micro_watch_include_unknown_hours_to_close": (
            daily_weather_micro_watch_include_unknown_hours_to_close
        ),
        "daily_weather_watch_wakeup_count": len(wakeup_transition_tickers),
        "daily_weather_watch_wakeup_tickers": wakeup_transition_tickers[:25],
        "daily_weather_watch_wakeup_to_candidate_count": wakeup_to_candidate_count,
        "daily_weather_watch_wakeup_to_planned_order_count": wakeup_to_planned_order_count,
        "daily_weather_micro_watch_status": (
            micro_watch_step.get("status")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_reason": (
            micro_watch_step.get("reason")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_watch_total": (
            micro_watch_step.get("watch_total")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_active_watch_tickers_count": (
            micro_watch_step.get("active_watch_tickers_count")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_selected_watch_tickers_count": (
            micro_watch_step.get("selected_watch_tickers_count")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_selected_watch_tickers": (
            micro_watch_step.get("selected_watch_tickers")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_polls_planned": (
            micro_watch_step.get("polls_planned")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_polls_completed": (
            micro_watch_step.get("polls_completed")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_wakeup_transition_count": (
            micro_watch_step.get("wakeup_transition_count")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_wakeup_transition_tickers": (
            micro_watch_step.get("wakeup_transition_tickers")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_status": (
            daily_weather_ticker_refresh_base_step.get("status")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_rows_appended": (
            daily_weather_ticker_refresh_base_step.get("rows_appended")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_refresh_mode": (
            daily_weather_ticker_refresh_base_step.get("refresh_mode")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_selection_mode": (
            daily_weather_ticker_refresh_base_step.get("watch_selection_mode")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_priority_candidates": (
            daily_weather_ticker_refresh_base_step.get("watch_priority_candidates")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_tickers_attempted_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_attempted_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_tickers_succeeded_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_succeeded_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_tradable_tickers_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_tradable_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_tickers_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_watch_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_selected_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_watch_selected_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_due": (
            daily_weather_ticker_refresh_base_step.get("ticker_refresh_watch_due")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_reason": (
            daily_weather_ticker_refresh_base_step.get("ticker_refresh_watch_reason")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_endpoint_to_orderable_transition_count": (
            daily_weather_ticker_refresh_base_step.get("endpoint_to_orderable_transition_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_endpoint_to_orderable_transition_tickers": (
            daily_weather_ticker_refresh_base_step.get("endpoint_to_orderable_transition_tickers")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_status": (
            wakeup_burst_step.get("status")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_rows_appended": (
            wakeup_burst_step.get("rows_appended")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_tickers_attempted_count": (
            wakeup_burst_step.get("market_tickers_attempted_count")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_tickers_attempted": (
            wakeup_burst_step.get("market_tickers_attempted")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_endpoint_to_orderable_transition_count": (
            wakeup_burst_step.get("endpoint_to_orderable_transition_count")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_endpoint_to_orderable_transition_tickers": (
            wakeup_burst_step.get("endpoint_to_orderable_transition_tickers")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_status": (
            wakeup_plan_step.get("status")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_output_file": (
            wakeup_plan_step.get("output_file")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_planned_orders": (
            wakeup_plan_step.get("planned_orders")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_top_plans_count": (
            wakeup_plan_step.get("top_plans_count")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_top_plans_tickers": (
            wakeup_plan_step.get("top_plans_tickers")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_status": (
            daily_weather_ticker_refresh_step.get("status")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_rows_appended": (
            daily_weather_ticker_refresh_step.get("rows_appended")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_refresh_mode": (
            daily_weather_ticker_refresh_step.get("refresh_mode")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_selection_mode": (
            daily_weather_ticker_refresh_step.get("watch_selection_mode")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_priority_candidates": (
            daily_weather_ticker_refresh_step.get("watch_priority_candidates")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_tickers_attempted_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_attempted_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_tickers_succeeded_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_succeeded_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_tradable_tickers_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_tradable_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_tickers_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_watch_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_selected_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_watch_selected_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_due": (
            daily_weather_ticker_refresh_step.get("ticker_refresh_watch_due")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_reason": (
            daily_weather_ticker_refresh_step.get("ticker_refresh_watch_reason")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_endpoint_to_orderable_transition_count": (
            daily_weather_ticker_refresh_step.get("endpoint_to_orderable_transition_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_endpoint_to_orderable_transition_tickers": (
            daily_weather_ticker_refresh_step.get("endpoint_to_orderable_transition_tickers")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_stale_recovery_triggered": stale_recovery_triggered,
        "daily_weather_stale_recovery_attempts": stale_recovery_attempts,
        "daily_weather_stale_recovery_resolved": stale_recovery_resolved,
        "daily_weather_stale_recovery_failure_kind": daily_weather_recovery_failure_kind,
        "daily_weather_recovery_alert_window_hours": daily_weather_recovery_alert_state.get("window_hours"),
        "daily_weather_recovery_alert_threshold": daily_weather_recovery_alert_state.get("threshold"),
        "daily_weather_recovery_alert_max_events": daily_weather_recovery_alert_state.get("max_events"),
        "daily_weather_recovery_failure_count_window": daily_weather_recovery_alert_state.get("failure_count_window"),
        "daily_weather_recovery_failure_counts_by_kind": daily_weather_recovery_alert_state.get("failure_counts_by_kind"),
        "daily_weather_recovery_alert_triggered": daily_weather_recovery_alert_state.get("alert_triggered"),
        "daily_weather_recovery_health_file": daily_weather_recovery_alert_state.get("state_file"),
        "daily_weather_recovery_alert_parse_error": daily_weather_recovery_alert_state.get("parse_error"),
        "daily_weather_recovery_alert_write_error": daily_weather_recovery_alert_state.get("write_error"),
        "capture_max_hours_to_close": capture_max_hours_to_close,
        "capture_page_limit": capture_page_limit,
        "capture_max_pages": capture_max_pages,
        "daily_weather_recovery_capture_max_hours_to_close": stale_recovery_capture_max_hours_to_close,
        "daily_weather_recovery_capture_page_limit": stale_recovery_capture_page_limit,
        "daily_weather_recovery_capture_max_pages": stale_recovery_capture_max_pages,
        "weather_prior_allowed_contract_families": list(allowed_weather_contract_families),
        "weather_prior_allowed_family_counts": weather_prior_state_after.get("allowed_family_counts"),
        "weather_prior_allowed_rows_total": weather_prior_state_after.get("allowed_rows_total"),
        "weather_prior_state_reason": weather_prior_state_after.get("reason"),
        "weather_prior_stale": bool(weather_prior_state_after.get("stale")),
        "weather_prior_age_seconds": weather_prior_state_after.get("priors_age_seconds"),
        "weather_history_rows_total": weather_history_state.get("weather_history_rows_total"),
        "weather_history_live_ready_rows": weather_history_state.get("weather_history_live_ready_rows"),
        "weather_history_unhealthy_rows": weather_history_state.get("weather_history_unhealthy_rows"),
        "weather_history_status_counts": weather_history_state.get("weather_history_status_counts"),
        "weather_history_live_ready_reason_counts": weather_history_state.get(
            "weather_history_live_ready_reason_counts"
        ),
        "weather_history_missing_token_count": weather_history_state.get("weather_history_missing_token_count"),
        "weather_history_station_mapping_missing_count": weather_history_state.get(
            "weather_history_station_mapping_missing_count"
        ),
        "weather_history_sample_depth_block_count": weather_history_state.get(
            "weather_history_sample_depth_block_count"
        ),
        "weather_history_parse_error": weather_history_state.get("weather_history_parse_error"),
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
