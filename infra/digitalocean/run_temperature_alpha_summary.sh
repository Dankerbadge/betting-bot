#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-/etc/betbot/temperature-shadow.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing runtime env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -r "$ENV_FILE" ]]; then
  echo "runtime env file is not readable (check owner/group/perms): $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

: "${BETBOT_ROOT:?BETBOT_ROOT is required}"
: "${OUTPUT_DIR:?OUTPUT_DIR is required}"

PYTHON_BIN="$BETBOT_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing python venv executable: $PYTHON_BIN" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs" "$OUTPUT_DIR/checkpoints"
CHECKPOINTS_DIR="$OUTPUT_DIR/checkpoints"
HEALTH_DIR="$OUTPUT_DIR/health"
mkdir -p "$HEALTH_DIR"
LOG_FILE="$OUTPUT_DIR/logs/alpha_summary.log"
LOCK_FILE="$OUTPUT_DIR/.alpha_summary.lock"

exec 9>"$LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
  if ! flock -n 9; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] alpha summary skipped: lock busy" >> "$LOG_FILE"
    exit 0
  fi
else
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] alpha summary lock warning: flock not found; continuing without lock" >> "$LOG_FILE"
fi

ALPHA_SUMMARY_HOURS="${ALPHA_SUMMARY_HOURS:-12}"
ALPHA_SUMMARY_TOP_N="${ALPHA_SUMMARY_TOP_N:-${REPORTING_TOP_N:-10}}"
ALPHA_SUMMARY_REFERENCE_BANKROLL_DOLLARS="${ALPHA_SUMMARY_REFERENCE_BANKROLL_DOLLARS:-${REFERENCE_BANKROLL_DOLLARS:-1000}}"
ALPHA_SUMMARY_SLIPPAGE_BPS_LIST="${ALPHA_SUMMARY_SLIPPAGE_BPS_LIST:-${REPORTING_SLIPPAGE_BPS_LIST:-0,5,10}}"
ALPHA_SUMMARY_SUGGESTION_COUNT="${ALPHA_SUMMARY_SUGGESTION_COUNT:-5}"
ALPHA_SUMMARY_SELECTION_QUALITY_LOOKBACK_HOURS="${ALPHA_SUMMARY_SELECTION_QUALITY_LOOKBACK_HOURS:-336}"
ALPHA_SUMMARY_SELECTION_QUALITY_INTENT_HOURS="${ALPHA_SUMMARY_SELECTION_QUALITY_INTENT_HOURS:-$ALPHA_SUMMARY_HOURS}"
ALPHA_SUMMARY_SELECTION_QUALITY_TOP_N="${ALPHA_SUMMARY_SELECTION_QUALITY_TOP_N:-$ALPHA_SUMMARY_TOP_N}"
ALPHA_SUMMARY_SELECTION_QUALITY_MIN_BUCKET_SAMPLES="${ALPHA_SUMMARY_SELECTION_QUALITY_MIN_BUCKET_SAMPLES:-4}"
ALPHA_SUMMARY_SELECTION_QUALITY_ADAPTIVE_MIN_BUCKET_SAMPLES_ENABLED="${ALPHA_SUMMARY_SELECTION_QUALITY_ADAPTIVE_MIN_BUCKET_SAMPLES_ENABLED:-1}"
ALPHA_SUMMARY_APPROVAL_RATE_MIN="${ALPHA_SUMMARY_APPROVAL_RATE_MIN:-0.03}"
ALPHA_SUMMARY_APPROVAL_RATE_MAX="${ALPHA_SUMMARY_APPROVAL_RATE_MAX:-0.20}"
ALPHA_SUMMARY_APPROVAL_RATE_MIN_INTENTS="${ALPHA_SUMMARY_APPROVAL_RATE_MIN_INTENTS:-100}"
ALPHA_SUMMARY_APPROVAL_RATE_CRITICAL_HIGH="${ALPHA_SUMMARY_APPROVAL_RATE_CRITICAL_HIGH:-0.50}"
ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_RATIO_TO_WINDOW="${ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_RATIO_TO_WINDOW:-0.25}"
ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_ABS_INTENTS="${ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_ABS_INTENTS:-1000}"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_ENABLED="${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_ENABLED:-0}"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STREAK_REQUIRED="${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STREAK_REQUIRED:-2}"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_MIN_ROWS="${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_MIN_ROWS:-$ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_ABS_INTENTS}"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_ENABLED="${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_ENABLED:-1}"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_WINDOWS_REQUIRED="${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_WINDOWS_REQUIRED:-2}"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_MAX_APPROVAL_DELTA_PP="${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_MAX_APPROVAL_DELTA_PP:-2.0}"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_MAX_STALE_DELTA_PP="${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_MAX_STALE_DELTA_PP:-3.0}"
ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ENABLED="${ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ENABLED:-1}"
ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_STREAK_REQUIRED="${ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_STREAK_REQUIRED:-3}"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STATE_FILE="${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STATE_FILE:-}"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_PROFILE_FILE="${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_PROFILE_FILE:-}"
ALPHA_SUMMARY_QUALITY_DRIFT_APPROVAL_DELTA_PP_MIN="${ALPHA_SUMMARY_QUALITY_DRIFT_APPROVAL_DELTA_PP_MIN:-3.0}"
ALPHA_SUMMARY_QUALITY_DRIFT_MIN_INTENTS_PER_WINDOW="${ALPHA_SUMMARY_QUALITY_DRIFT_MIN_INTENTS_PER_WINDOW:-1000}"
ALPHA_SUMMARY_QUALITY_DRIFT_MAX_RESOLVED_SIDES_DELTA="${ALPHA_SUMMARY_QUALITY_DRIFT_MAX_RESOLVED_SIDES_DELTA:-0}"
ALPHA_SUMMARY_GATE_COVERAGE_ALERT_MIN_APPROVED_ROWS="${ALPHA_SUMMARY_GATE_COVERAGE_ALERT_MIN_APPROVED_ROWS:-$ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_ABS_INTENTS}"
ALPHA_SUMMARY_GATE_COVERAGE_EXPECTED_EDGE_MIN="${ALPHA_SUMMARY_GATE_COVERAGE_EXPECTED_EDGE_MIN:-0.60}"
ALPHA_SUMMARY_GATE_COVERAGE_PROBABILITY_MIN="${ALPHA_SUMMARY_GATE_COVERAGE_PROBABILITY_MIN:-0.60}"
ALPHA_SUMMARY_GATE_COVERAGE_ALPHA_MIN="${ALPHA_SUMMARY_GATE_COVERAGE_ALPHA_MIN:-0.30}"
ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_ENABLED="${ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_ENABLED:-0}"
ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_STREAK_REQUIRED="${ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_STREAK_REQUIRED:-2}"
ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_MIN_ROWS="${ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_MIN_ROWS:-$ALPHA_SUMMARY_GATE_COVERAGE_ALERT_MIN_APPROVED_ROWS}"
ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_MIN_LEVEL="${ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_MIN_LEVEL:-red}"
ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ZERO_APPROVED_STREAK_REQUIRED="${ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ZERO_APPROVED_STREAK_REQUIRED:-2}"
ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ZERO_APPROVED_MIN_INTENTS="${ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ZERO_APPROVED_MIN_INTENTS:-$ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_ABS_INTENTS}"
ALPHA_SUMMARY_QUALITY_RISK_STREAK_RED_REQUIRED="${ALPHA_SUMMARY_QUALITY_RISK_STREAK_RED_REQUIRED:-3}"
ALPHA_SUMMARY_QUALITY_RISK_AUTO_APPLY_STREAK_REQUIRED="${ALPHA_SUMMARY_QUALITY_RISK_AUTO_APPLY_STREAK_REQUIRED:-2}"
ALPHA_SUMMARY_QUALITY_RISK_STATE_FILE="${ALPHA_SUMMARY_QUALITY_RISK_STATE_FILE:-}"
ALPHA_SUMMARY_DISCORD_MODE="${ALPHA_SUMMARY_DISCORD_MODE:-concise}"
ALPHA_SUMMARY_CONCISE_MAX_LINES="${ALPHA_SUMMARY_CONCISE_MAX_LINES:-18}"
ALPHA_SUMMARY_SEND_WEBHOOK="${ALPHA_SUMMARY_SEND_WEBHOOK:-1}"
ALPHA_SUMMARY_WEBHOOK_URL="${ALPHA_SUMMARY_WEBHOOK_URL:-${ALERT_WEBHOOK_URL:-}}"
ALPHA_SUMMARY_WEBHOOK_THREAD_ID="${ALPHA_SUMMARY_WEBHOOK_THREAD_ID:-${ALERT_WEBHOOK_THREAD_ID:-}}"
ALPHA_SUMMARY_WEBHOOK_TIMEOUT_SECONDS="${ALPHA_SUMMARY_WEBHOOK_TIMEOUT_SECONDS:-5}"
ALPHA_SUMMARY_SEND_ALPHA_WEBHOOK="${ALPHA_SUMMARY_SEND_ALPHA_WEBHOOK:-0}"
ALPHA_SUMMARY_WEBHOOK_ALPHA_URL="${ALPHA_SUMMARY_WEBHOOK_ALPHA_URL:-}"
ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID="${ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID:-${ALPHA_SUMMARY_WEBHOOK_THREAD_ID:-}}"
ALPHA_SUMMARY_WEBHOOK_ALPHA_TIMEOUT_SECONDS="${ALPHA_SUMMARY_WEBHOOK_ALPHA_TIMEOUT_SECONDS:-$ALPHA_SUMMARY_WEBHOOK_TIMEOUT_SECONDS}"
ALPHA_SUMMARY_SEND_OPS_WEBHOOK="${ALPHA_SUMMARY_SEND_OPS_WEBHOOK:-0}"
ALPHA_SUMMARY_WEBHOOK_OPS_URL="${ALPHA_SUMMARY_WEBHOOK_OPS_URL:-}"
ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID="${ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID:-${ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID:-${ALPHA_SUMMARY_WEBHOOK_THREAD_ID:-}}}"
ALPHA_SUMMARY_WEBHOOK_OPS_TIMEOUT_SECONDS="${ALPHA_SUMMARY_WEBHOOK_OPS_TIMEOUT_SECONDS:-$ALPHA_SUMMARY_WEBHOOK_TIMEOUT_SECONDS}"
ALPHA_SUMMARY_LIVE_STATUS_FILE="${ALPHA_SUMMARY_LIVE_STATUS_FILE:-$OUTPUT_DIR/health/live_status_latest.json}"
WINDOW_SUMMARIZE_SCRIPT="${WINDOW_SUMMARIZE_SCRIPT:-$BETBOT_ROOT/infra/digitalocean/summarize_window.py}"

build_discord_target_url() {
  local base_url="${1:-}"
  local thread_id="${2:-}"
  if [[ -z "$base_url" || -z "$thread_id" ]]; then
    echo "$base_url"
    return
  fi
  if [[ "$base_url" == *"thread_id="* ]]; then
    echo "$base_url"
    return
  fi
  if [[ "$base_url" == *\?* ]]; then
    echo "${base_url}&thread_id=${thread_id}"
  else
    echo "${base_url}?thread_id=${thread_id}"
  fi
}

ALPHA_SUMMARY_WEBHOOK_TARGET_URL="$(build_discord_target_url "$ALPHA_SUMMARY_WEBHOOK_URL" "$ALPHA_SUMMARY_WEBHOOK_THREAD_ID")"
ALPHA_SUMMARY_WEBHOOK_ALPHA_TARGET_URL="$(build_discord_target_url "$ALPHA_SUMMARY_WEBHOOK_ALPHA_URL" "$ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID")"
ALPHA_SUMMARY_WEBHOOK_OPS_TARGET_URL="$(build_discord_target_url "$ALPHA_SUMMARY_WEBHOOK_OPS_URL" "$ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID")"

if [[ ! -f "$WINDOW_SUMMARIZE_SCRIPT" ]]; then
  echo "missing summarize script: $WINDOW_SUMMARIZE_SCRIPT" >&2
  exit 1
fi

cd "$BETBOT_ROOT"
window_end_epoch="$(date +%s)"
window_meta="$("$PYTHON_BIN" - "$ALPHA_SUMMARY_HOURS" "$window_end_epoch" <<'PY'
from __future__ import annotations

import sys

hours = max(0.0, float(sys.argv[1]))
end_epoch = int(float(sys.argv[2]))
start_epoch = max(0, int(end_epoch - (hours * 3600.0)))
if abs(hours - round(hours)) < 1e-9:
    label = f"{int(round(hours))}h"
else:
    label = f"{hours:g}h"
print(f"{start_epoch}|{label}|{hours:g}")
PY
)"
IFS='|' read -r window_start_epoch window_label window_hours_safe <<<"$window_meta"
window_ts="$(date -u +"%Y%m%d_%H%M%S")"

window_summary_file="$CHECKPOINTS_DIR/station_tuning_window_${window_label}_${window_ts}.json"
alpha_summary_file="$CHECKPOINTS_DIR/alpha_summary_${window_label}_${window_ts}.json"
alpha_summary_latest_file="$CHECKPOINTS_DIR/alpha_summary_${window_label}_latest.json"
alpha_summary_health_latest_file="$HEALTH_DIR/alpha_summary_latest.json"

cycle_start_epoch="$(date +%s)"

_run_timed_stage() {
  local stage_name="$1"
  shift
  local stage_start_epoch stage_end_epoch stage_elapsed stage_status stage_rc
  stage_start_epoch="$(date +%s)"
  if "$@" >> "$LOG_FILE" 2>&1; then
    stage_status="ok"
    stage_rc=0
  else
    stage_status="error"
    stage_rc=$?
  fi
  stage_end_epoch="$(date +%s)"
  stage_elapsed=$((stage_end_epoch - stage_start_epoch))
  local stage_name_upper
  stage_name_upper="$(
    printf '%s' "$stage_name" \
      | tr '[:lower:]-' '[:upper:]_'
  )"
  local stage_var="ALPHA_SUMMARY_STAGE_SECONDS_${stage_name_upper}"
  printf -v "$stage_var" "%s" "$stage_elapsed"
  export "$stage_var"
  echo "alpha_summary_stage stage=$stage_name status=$stage_status exit=$stage_rc duration_sec=$stage_elapsed" >> "$LOG_FILE"
  if [[ "$stage_status" != "ok" ]]; then
    return "$stage_rc"
  fi
  return 0
}

echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") alpha summary cycle start (window=${window_label}) ===" >> "$LOG_FILE"

_run_timed_stage summarize_window \
  "$PYTHON_BIN" "$WINDOW_SUMMARIZE_SCRIPT" \
  --out-dir "$OUTPUT_DIR" \
  --start-epoch "$window_start_epoch" \
  --end-epoch "$window_end_epoch" \
  --label "$window_label" \
  --output "$window_summary_file"

_run_timed_stage bankroll_validation \
  "$PYTHON_BIN" -m betbot.cli kalshi-temperature-bankroll-validation \
  --output-dir "$OUTPUT_DIR" \
  --hours "$window_hours_safe" \
  --reference-bankroll-dollars "$ALPHA_SUMMARY_REFERENCE_BANKROLL_DOLLARS" \
  --slippage-bps-list "$ALPHA_SUMMARY_SLIPPAGE_BPS_LIST" \
  --top-n "$ALPHA_SUMMARY_TOP_N"

latest_bankroll_file="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_bankroll_validation_*.json 2>/dev/null | head -n 1 || true)"

alpha_gap_cmd=(
  "$PYTHON_BIN" -m betbot.cli kalshi-temperature-alpha-gap-report
  --output-dir "$OUTPUT_DIR" \
  --hours "$window_hours_safe" \
  --reference-bankroll-dollars "$ALPHA_SUMMARY_REFERENCE_BANKROLL_DOLLARS" \
  --slippage-bps-list "$ALPHA_SUMMARY_SLIPPAGE_BPS_LIST" \
  --top-n "$ALPHA_SUMMARY_TOP_N"
)
if [[ -n "$latest_bankroll_file" ]]; then
  alpha_gap_cmd+=(--source-bankroll-validation-file "$latest_bankroll_file")
fi
_run_timed_stage alpha_gap_report "${alpha_gap_cmd[@]}"

selection_quality_min_bucket_samples_effective="$ALPHA_SUMMARY_SELECTION_QUALITY_MIN_BUCKET_SAMPLES"
if [[ "$ALPHA_SUMMARY_SELECTION_QUALITY_ADAPTIVE_MIN_BUCKET_SAMPLES_ENABLED" == "1" ]]; then
  selection_quality_min_bucket_samples_effective="$(
    "$PYTHON_BIN" - "$latest_bankroll_file" "$ALPHA_SUMMARY_SELECTION_QUALITY_MIN_BUCKET_SAMPLES" <<'PY'
from __future__ import annotations

import json
import math
from pathlib import Path
import sys

path = str(sys.argv[1] or "").strip()
try:
    configured = int(float(sys.argv[2]))
except Exception:
    configured = 4
configured = max(1, configured)
resolved_sides: int | None = None
if path:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        opportunity = payload.get("opportunity_breadth") if isinstance(payload, dict) else {}
        if isinstance(opportunity, dict):
            raw_value = opportunity.get("resolved_unique_market_sides")
            if raw_value is not None:
                parsed = float(raw_value)
                if math.isfinite(parsed):
                    resolved_sides = int(parsed)
    except Exception:
        resolved_sides = None

effective = configured
if isinstance(resolved_sides, int) and resolved_sides > 0:
    if resolved_sides < 40:
        effective = 1
    elif resolved_sides < 120:
        effective = 2
    elif resolved_sides < 250:
        effective = min(configured, 3)

print(max(1, min(configured, effective)))
PY
  )"
fi

_run_timed_stage selection_quality \
  "$PYTHON_BIN" -m betbot.cli kalshi-temperature-selection-quality \
  --output-dir "$OUTPUT_DIR" \
  --lookback-hours "$ALPHA_SUMMARY_SELECTION_QUALITY_LOOKBACK_HOURS" \
  --intent-hours "$ALPHA_SUMMARY_SELECTION_QUALITY_INTENT_HOURS" \
  --min-bucket-samples "$selection_quality_min_bucket_samples_effective" \
  --top-n "$ALPHA_SUMMARY_SELECTION_QUALITY_TOP_N"

latest_alpha_gap_file="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_alpha_gap_report_*.json 2>/dev/null | head -n 1 || true)"
latest_live_readiness_file="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_live_readiness_*.json 2>/dev/null | head -n 1 || true)"
latest_go_live_gate_file="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_go_live_gate_*.json 2>/dev/null | head -n 1 || true)"
latest_blocker_audit_file="$(ls -1t "$CHECKPOINTS_DIR"/blocker_audit_*_latest.json 2>/dev/null | head -n 1 || true)"
latest_plan_summary_file="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_trade_plan_summary_*.json 2>/dev/null | head -n 1 || true)"
latest_intents_summary_file="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_trade_intents_summary_*.json 2>/dev/null | head -n 1 || true)"
export ALPHA_SUMMARY_DISCORD_MODE
export ALPHA_SUMMARY_CONCISE_MAX_LINES
export ALPHA_SUMMARY_QUALITY_RISK_STREAK_RED_REQUIRED
export ALPHA_SUMMARY_QUALITY_RISK_AUTO_APPLY_STREAK_REQUIRED
export ALPHA_SUMMARY_QUALITY_RISK_STATE_FILE
export ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_ENABLED
export ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_WINDOWS_REQUIRED
export ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_MAX_APPROVAL_DELTA_PP
export ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_MAX_STALE_DELTA_PP

render_stage_start_epoch="$(date +%s)"
"$PYTHON_BIN" - "$window_summary_file" "$latest_bankroll_file" "$latest_alpha_gap_file" "$ALPHA_SUMMARY_LIVE_STATUS_FILE" "$latest_live_readiness_file" "$latest_go_live_gate_file" "$latest_blocker_audit_file" "$latest_plan_summary_file" "$latest_intents_summary_file" "$alpha_summary_file" "$alpha_summary_latest_file" "$alpha_summary_health_latest_file" "$ALPHA_SUMMARY_REFERENCE_BANKROLL_DOLLARS" "$ALPHA_SUMMARY_SUGGESTION_COUNT" "$window_label" "$ALPHA_SUMMARY_APPROVAL_RATE_MIN" "$ALPHA_SUMMARY_APPROVAL_RATE_MAX" "$ALPHA_SUMMARY_APPROVAL_RATE_MIN_INTENTS" "$ALPHA_SUMMARY_APPROVAL_RATE_CRITICAL_HIGH" "$ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_ENABLED" "$ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STREAK_REQUIRED" "$ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STATE_FILE" "$ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_PROFILE_FILE" "$ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_MIN_ROWS" "$ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ENABLED" "$ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_STREAK_REQUIRED" "$ALPHA_SUMMARY_QUALITY_DRIFT_APPROVAL_DELTA_PP_MIN" "$ALPHA_SUMMARY_QUALITY_DRIFT_MIN_INTENTS_PER_WINDOW" "$ALPHA_SUMMARY_QUALITY_DRIFT_MAX_RESOLVED_SIDES_DELTA" "$ALPHA_SUMMARY_GATE_COVERAGE_ALERT_MIN_APPROVED_ROWS" "$ALPHA_SUMMARY_GATE_COVERAGE_EXPECTED_EDGE_MIN" "$ALPHA_SUMMARY_GATE_COVERAGE_PROBABILITY_MIN" "$ALPHA_SUMMARY_GATE_COVERAGE_ALPHA_MIN" "$ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_RATIO_TO_WINDOW" "$ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_ABS_INTENTS" "$ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_ENABLED" "$ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_STREAK_REQUIRED" "$ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_MIN_ROWS" "$ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_MIN_LEVEL" "$ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ZERO_APPROVED_STREAK_REQUIRED" "$ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ZERO_APPROVED_MIN_INTENTS" <<'PY'
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from typing import Any

python_stage_started_at = datetime.now(timezone.utc)


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _parse_int(value: Any) -> int | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return int(parsed)


def _to_json_number(value: Any) -> float | None:
    parsed = _parse_float(value)
    if not isinstance(parsed, float):
        return None
    return round(float(parsed), 6)


def _parse_bool(value: Any, default: bool = False) -> bool:
    text = _normalize(value).lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _normalize(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fmt_age_compact(seconds: Any) -> str:
    parsed = _parse_float(seconds)
    if not isinstance(parsed, float) or parsed < 0:
        return "n/a"
    whole = int(round(parsed))
    if whole < 60:
        return f"{whole}s"
    minutes = whole // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h"
    days = hours // 24
    return f"{days}d"


def _load_json_with_status(path: str) -> tuple[dict[str, Any], str]:
    text = _normalize(path)
    if not text:
        return {}, "path_missing"
    file_path = Path(text)
    if not file_path.exists():
        return {}, "file_missing"
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return {}, "parse_error"
    if not isinstance(payload, dict):
        return {}, "non_object"
    return payload, "ok"


def _load_json(path: str) -> dict[str, Any]:
    payload, _status = _load_json_with_status(path)
    return payload


def _latest_file_from_output_dir(output_dir: Path, pattern: str) -> str:
    try:
        candidates = sorted(
            output_dir.glob(pattern),
            key=lambda path: float(path.stat().st_mtime) if path.exists() else 0.0,
            reverse=True,
        )
    except Exception:
        return ""
    for candidate in candidates:
        try:
            if candidate.is_file():
                return str(candidate)
        except Exception:
            continue
    return ""


def _resolve_artifact_file(
    *,
    live_status_payload: dict[str, Any],
    artifact_key: str,
    output_dir: Path,
    fallback_pattern: str,
) -> tuple[str, str]:
    latest_artifacts = (
        live_status_payload.get("latest_artifacts")
        if isinstance(live_status_payload.get("latest_artifacts"), dict)
        else {}
    )
    artifact_row = latest_artifacts.get(artifact_key) if isinstance(latest_artifacts, dict) else {}
    live_path = _normalize(artifact_row.get("path")) if isinstance(artifact_row, dict) else ""
    if live_path:
        try:
            if Path(live_path).is_file():
                return live_path, "live_status_latest_artifacts"
        except Exception:
            pass
    fallback_path = _latest_file_from_output_dir(output_dir, fallback_pattern)
    if fallback_path:
        return fallback_path, "output_dir_glob_fallback"
    if live_path:
        return live_path, "live_status_path_missing"
    return "", "missing"


def _fmt_money(value: Any, *, signed: bool = True) -> str:
    parsed = _parse_float(value)
    if not isinstance(parsed, float):
        return "n/a"
    if signed:
        sign = "+" if parsed > 0 else ""
        return f"{sign}${parsed:,.2f}"
    return f"${parsed:,.2f}"


def _fmt_money_compact(value: Any) -> str:
    parsed = _parse_float(value)
    if not isinstance(parsed, float):
        return "n/a"
    amount = abs(parsed)
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    if amount >= 10_000:
        return f"${amount / 1_000:.1f}k"
    if amount >= 1_000:
        return f"${amount / 1_000:.2f}k"
    return f"${amount:,.0f}"


def _fmt_percent_ratio(value: Any) -> str:
    parsed = _parse_float(value)
    if not isinstance(parsed, float):
        return "n/a"
    return f"{parsed * 100.0:.2f}%"


def _fmt_percent_value(value: Any) -> str:
    parsed = _parse_float(value)
    if not isinstance(parsed, float):
        return "n/a"
    return f"{parsed:.2f}%"


def _fmt_int(value: Any) -> str:
    parsed = _parse_int(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:,}"


def _fmt_signed_int(value: Any) -> str:
    parsed = _parse_int(value)
    if parsed is None:
        return "n/a"
    if parsed > 0:
        return f"+{parsed:,}"
    return f"{parsed:,}"


def _fmt_signed_percent_points(value: Any) -> str:
    parsed = _parse_float(value)
    if not isinstance(parsed, float):
        return "n/a"
    if parsed > 0:
        return f"+{parsed:.2f}pp"
    return f"{parsed:.2f}pp"


def _clip_text(value: Any, max_len: int) -> str:
    text = _normalize(value)
    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3].rstrip() + "..."


def _clip_text_plain(value: Any, max_len: int) -> str:
    text = _normalize(value)
    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text
    clipped = text[:max_len].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0].rstrip()
    clipped = clipped.rstrip(" ,;:-(")
    # Avoid dangling partial parentheticals after boundary clipping
    # (e.g., "... 95.7% (target") which hurts Discord readability.
    while clipped.count("(") > clipped.count(")") and "(" in clipped:
        clipped = clipped.rsplit("(", 1)[0].rstrip(" ,;:-")
    while clipped.count("[") > clipped.count("]") and "[" in clipped:
        clipped = clipped.rsplit("[", 1)[0].rstrip(" ,;:-")
    return clipped


def _humanize_operator_text(value: Any) -> str:
    text = _normalize(value)
    if not text:
        return ""
    replacements = (
        ("local-hour h23", "11PM local hour"),
        ("edge/freshness gates", "quality/freshness filters"),
        ("before global changes", "before global threshold updates"),
        ("Replan on true weather shifts.", "Recompute plans only after real weather shifts."),
        ("Balance repeat cap vs valid updates.", "Allow valid updates without repeat-entry spam."),
    )
    for source, target in replacements:
        text = text.replace(source, target)
    return text


def _compact_suggestion_line(value: Any, max_len: int) -> str:
    text = _humanize_operator_text(value)
    if len(text) <= max_len:
        return text
    pipe_parts = [part.strip() for part in text.split("|") if _normalize(part)]
    # In concise Discord mode we prefer plain-English actions over compact
    # scoring tokens like "conf MID", which add noise without operator value.
    pipe_parts_no_conf = [
        part for part in pipe_parts
        if not part.lower().startswith("conf ")
    ]
    if pipe_parts_no_conf:
        pipe_parts = pipe_parts_no_conf
    if pipe_parts:
        action_part = pipe_parts[0]
        impact_part = next((part for part in pipe_parts[1:] if part.lower().startswith("impact ")), "")
        conf_part = ""
        why_part = next((part for part in pipe_parts[1:] if part.lower().startswith("why ")), "")
        metric_parts = [
            part
            for part in pipe_parts[1:]
            if part not in {impact_part, conf_part, why_part}
        ]
        candidate_tiers = []
        if impact_part and conf_part and why_part:
            candidate_tiers.append(" | ".join([action_part, impact_part, conf_part, why_part]))
        if impact_part and conf_part:
            candidate_tiers.append(" | ".join([action_part, impact_part, conf_part]))
        if conf_part and why_part:
            candidate_tiers.append(" | ".join([action_part, conf_part, why_part]))
        if impact_part and why_part:
            candidate_tiers.append(" | ".join([action_part, impact_part, why_part]))
        if conf_part:
            candidate_tiers.append(" | ".join([action_part, conf_part]))
        if impact_part:
            candidate_tiers.append(" | ".join([action_part, impact_part]))
        if why_part:
            candidate_tiers.append(" | ".join([action_part, why_part]))
        if metric_parts:
            candidate_tiers.append(" | ".join([action_part, metric_parts[0]]))
        candidate_tiers.append(action_part)

        # Prefer preserving impact/confidence/rationale by clipping the action
        # segment before dropping metadata tokens.
        def _with_clipped_action(parts: list[str]) -> str:
            if not parts:
                return ""
            if len(parts) == 1:
                return _clip_text(parts[0], max_len)
            action_full = _normalize(parts[0])
            suffix = " | ".join(parts[1:])
            # 3 chars for separator between action and suffix. Keep at least a
            # minimal action stub and trim suffix as needed so impact/confidence
            # tokens survive under tight budgets.
            min_action_budget = 4
            available_for_action = max_len - len(suffix) - 3
            if available_for_action < min_action_budget:
                suffix_budget = max_len - min_action_budget - 3
                if suffix_budget <= 0:
                    return _clip_text(parts[0], max_len)
                suffix = _clip_text_plain(suffix, suffix_budget)
                available_for_action = max_len - len(suffix) - 3
            if available_for_action <= 0:
                return _clip_text(parts[0], max_len)
            action_clipped = _clip_text_plain(parts[0], max(1, available_for_action))
            candidate = f"{action_clipped} | {suffix}"
            if len(candidate) <= max_len:
                return candidate
            return _clip_text(candidate, max_len)

        clipped_priority_tiers = []
        if impact_part and conf_part and why_part:
            clipped_priority_tiers.append(_with_clipped_action([action_part, impact_part, conf_part, why_part]))
        if impact_part and conf_part:
            clipped_priority_tiers.append(_with_clipped_action([action_part, impact_part, conf_part]))
        if conf_part and why_part:
            clipped_priority_tiers.append(_with_clipped_action([action_part, conf_part, why_part]))
        if impact_part and why_part:
            clipped_priority_tiers.append(_with_clipped_action([action_part, impact_part, why_part]))
        if conf_part:
            clipped_priority_tiers.append(_with_clipped_action([action_part, conf_part]))
        if impact_part:
            clipped_priority_tiers.append(_with_clipped_action([action_part, impact_part]))
        if why_part:
            clipped_priority_tiers.append(_with_clipped_action([action_part, why_part]))

        for candidate in clipped_priority_tiers:
            if len(candidate) <= max_len:
                return candidate
        for candidate in candidate_tiers:
            if len(candidate) <= max_len:
                return candidate
        return _clip_text_plain(action_part, max_len)
    # Prefer dropping metric parentheticals over emitting partial clauses.
    if " (" in text:
        action_only = text.split(" (", 1)[0].strip()
        if len(action_only) <= max_len:
            return action_only
        return _clip_text_plain(action_only, max_len)
    return _clip_text_plain(text, max_len)


def _humanize_reason(reason: str) -> str:
    mapping = {
        "metar_observation_stale": "METAR stale",
        "metar_freshness_boundary_quality_insufficient": "METAR near-stale quality insufficient",
        "settlement_finalization_blocked": "settlement finalization blocked",
        "inside_cutoff_window": "inside cutoff window",
        "no_side_interval_overlap_still_possible": "range still possible",
        "yes_side_not_impossible": "yes-side not impossible",
        "alpha_strength_below_min": "alpha strength below min",
        "probability_confidence_below_min": "probability confidence below min",
        "expected_edge_below_min": "expected edge below min",
        "historical_quality_global_only_pressure": "historical quality global-only pressure",
        "market_side_replan_cooldown": "market-side cooldown",
        "below_min_alpha_strength": "below alpha threshold",
        "underlying_submission_cap_reached": "underlying submission cap reached",
        "approval_quality_drift_without_breadth": "approval-quality drift without breadth growth",
        "selection_quality_global_only_drift": "selection-quality global-only drift",
        "approval_gate_coverage_gap": "approval gate coverage gap",
        "live_status_missing": "live status file missing (summary-only mode)",
        "live_status_status_missing": "live status status missing",
        "trial_balance_cache_write_failed": "trial balance cache write failed",
        "profitability_csv_cache_degraded": "profitability cache degraded",
        "profitability_csv_cache_readonly": "profitability cache readonly",
        "profitability_csv_cache_fallback_runtime_failure": "profitability cache fallback runtime failure",
        "profitability_csv_cache_fallback_parent_unwritable": "profitability cache fallback parent unwritable",
        "settlement_state_load_failures": "settlement-state load failures observed",
        "window_summary_no_recent_shadow_or_intent_files": "no recent shadow/intent files in window",
        "artifact_parse_error": "artifact parse errors detected",
        "critical_artifact_parse_error": "critical alpha artifact parse errors detected",
    }
    key = _normalize(reason)
    if not key:
        return ""
    if key.startswith("intent_status_"):
        return "intent status " + key.removeprefix("intent_status_").replace("_", " ")
    if key.startswith("shadow_cycle_"):
        return "shadow cycle " + key.removeprefix("shadow_cycle_").replace("_", " ")
    if key in mapping:
        return mapping[key]
    return key.replace("_", " ")


def _humanize_quality_gate_source(source: str) -> str:
    key = _normalize(source).lower()
    if not key:
        return "unknown"
    mapping = {
        "manual": "manual thresholds",
        "manual+approval_guardrail": "manual + guardrail",
        "auto_profile": "auto profile",
        "auto_profile+approval_guardrail": "auto profile + guardrail",
        "derived_from_intents_plan": "derived from intents/plan",
        "live_status": "live status",
    }
    if key in mapping:
        return mapping[key]
    return key.replace("_", " ")


def _humanize_guardrail_sample_source(source: str) -> str:
    key = _normalize(source).lower()
    if not key:
        return "unknown"
    mapping = {
        "shadow_loop_blocker_snapshot": "shadow loop snapshot",
        "window_intent_summaries": "rolling intent summaries",
        "window_summary": "rolling window summary",
        "live_status": "live status",
        "manual": "manual",
    }
    if key in mapping:
        return mapping[key]
    return key.replace("_", " ")


def _derive_live_status_from_window_summary(window_summary: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(window_summary, dict):
        return {}

    settlement_loaded_false = int(_parse_int(window_summary.get("settlement_state_loaded_false_files")) or 0)
    intent_status_counts = (
        window_summary.get("intent_status_counts")
        if isinstance(window_summary.get("intent_status_counts"), dict)
        else {}
    )
    shadow_cycle_status_counts = (
        window_summary.get("shadow_cycle_status_counts")
        if isinstance(window_summary.get("shadow_cycle_status_counts"), dict)
        else {}
    )
    files_count = (
        window_summary.get("files_count")
        if isinstance(window_summary.get("files_count"), dict)
        else {}
    )

    status = "GREEN"
    reasons: list[str] = []

    if settlement_loaded_false > 0:
        status = "RED"
        reasons.append("settlement_state_load_failures")

    for key, value in intent_status_counts.items():
        key_norm = _normalize(key).lower()
        count = int(_parse_int(value) or 0)
        if count <= 0:
            continue
        if key_norm not in {"ready", "ok", "success"}:
            if status != "RED":
                status = "YELLOW"
            reasons.append(f"intent_status_{key_norm}")

    for key, value in shadow_cycle_status_counts.items():
        key_norm = _normalize(key).lower()
        count = int(_parse_int(value) or 0)
        if count <= 0:
            continue
        if any(token in key_norm for token in ("error", "fail", "exception")):
            status = "RED"
            reasons.append(f"shadow_cycle_{key_norm}")
            continue
        if key_norm in {"bootstrap", "unknown"}:
            if status != "RED":
                status = "YELLOW"
            reasons.append(f"shadow_cycle_{key_norm}")

    intents_file_count = int(_parse_int(files_count.get("intents")) or 0)
    shadow_file_count = int(_parse_int(files_count.get("shadow")) or 0)
    if intents_file_count <= 0 and shadow_file_count <= 0:
        if status != "RED":
            status = "YELLOW"
        reasons.append("window_summary_no_recent_shadow_or_intent_files")

    payload: dict[str, Any] = {
        "status": status,
        "derived_from_window_summary": True,
        "source": "window_summary_derived",
    }
    captured_at_text = _normalize(window_summary.get("captured_at"))
    if captured_at_text:
        payload["captured_at"] = captured_at_text
    reason_key = "red_reasons" if status == "RED" else "yellow_reasons"
    payload[reason_key] = sorted(set(reasons))
    return payload


def _humanize_guardrail_basis_reason(reason: str) -> str:
    key = _normalize(reason).lower()
    if not key:
        return ""
    mapping = {
        "window_default": "using rolling window by default",
        "latest_sample_eligible": "latest sample is large enough to use",
        "recent_rollup_eligible": "recent rollup is large enough to use",
        "latest_sample_below_guardrail_min_intents": "latest sample below minimum intents",
        "latest_sample_below_abs_floor": "latest sample below absolute intent floor",
        "latest_sample_below_window_ratio_floor": "latest sample too small vs rolling window",
        "latest_sample_missing_rate": "latest sample missing approval rate",
        "recent_rollup_insufficient_files": "recent rollup has too few files",
        "recent_rollup_below_abs_floor": "recent rollup below absolute intent floor",
        "recent_rollup_missing_rate": "recent rollup missing approval rate",
    }
    if key in mapping:
        return mapping[key]
    return key.replace("_", " ")


def _summarize_quality_gate_detail(
    detail: str,
    *,
    escalation_status: str,
    escalation_multiplier: float | None,
    escalation_sample_approval_rate: float | None,
    escalation_sample_intents_total: int | None,
    escalation_min_intents_required: int | None = None,
    escalation_min_intents_base: int | None = None,
    escalation_basis_min_abs_intents: int | None = None,
    escalation_sample_source: str | None = None,
    escalation_sample_age_seconds: float | None = None,
    include_status_and_multiplier: bool = True,
) -> str:
    text = _normalize(detail)
    if not text:
        return ""
    parts = [part.strip() for part in text.split(";") if part.strip()]
    if not parts:
        return ""

    flags: set[str] = set()
    kv: dict[str, str] = {}
    for part in parts:
        if "=" in part:
            key, value = part.split("=", 1)
            kv[_normalize(key).lower()] = _normalize(value)
        else:
            flags.add(_normalize(part).lower())

    notes: list[str] = []
    if "auto_profile_not_found" in flags:
        notes.append("auto profile unavailable")
    guardrail_reason_map = {
        "approval_rate_below_guardrail": "approval below guardrail",
        "approval_guardrail_below_basis_floor": "sample below guardrail basis floor",
        "approval_guardrail_insufficient_sample": "insufficient sample for guardrail escalation",
        "approval_guardrail_metrics_stale": "guardrail metrics stale",
        "approval_guardrail_escalation_disabled": "guardrail escalation disabled",
        "approval_guardrail_not_triggered": "guardrail within escalation band",
        "approval_guardrail_escalation_no_effect": "guardrail escalation had no threshold effect",
        "approval_guardrail_starvation_relief_waiting_streak": "low-approval relief waiting streak",
        "approval_guardrail_starvation_relief_blocked_by_quality": "low-approval relief blocked by quality",
        "approval_guardrail_starvation_relief_no_effect": "low-approval relief had no threshold effect",
        "approval_guardrail_starvation_relief_applied": "low-approval relief applied",
    }
    guardrail_reason_label = ""
    for key, label in guardrail_reason_map.items():
        if key in flags:
            guardrail_reason_label = label
            break
    if guardrail_reason_label:
        notes.append(guardrail_reason_label)

    status_text = _normalize(kv.get("status")) or _normalize(escalation_status)
    multiplier_value = _parse_float(kv.get("multiplier"))
    if not isinstance(multiplier_value, float):
        multiplier_value = escalation_multiplier if isinstance(escalation_multiplier, float) else None

    approval_rate_value = (
        escalation_sample_approval_rate
        if isinstance(escalation_sample_approval_rate, float)
        else _parse_float(kv.get("approval_rate"))
    )
    intents_total_value = (
        escalation_sample_intents_total
        if isinstance(escalation_sample_intents_total, int)
        else _parse_int(kv.get("intents_total"))
    )
    min_intents_required_value = (
        escalation_min_intents_required
        if isinstance(escalation_min_intents_required, int)
        else _parse_int(kv.get("min_intents"))
    )
    min_intents_base_value = (
        escalation_min_intents_base
        if isinstance(escalation_min_intents_base, int)
        else _parse_int(kv.get("min_intents_base"))
    )
    basis_min_abs_intents_value = (
        escalation_basis_min_abs_intents
        if isinstance(escalation_basis_min_abs_intents, int)
        else _parse_int(kv.get("basis_min_abs_intents"))
    )
    sample_source_text = _normalize(escalation_sample_source)
    sample_age_text = _fmt_age_compact(escalation_sample_age_seconds)

    escalation_triggered = (
        "approval_guardrail_escalation_applied" in flags
        or (_normalize(status_text).lower() not in {"", "none", "n/a"})
        or (isinstance(multiplier_value, float) and multiplier_value > 1.000001)
    )
    if escalation_triggered:
        escalation_parts: list[str] = []
        if include_status_and_multiplier and status_text and status_text.lower() not in {"none", "n/a"}:
            escalation_parts.append(status_text.replace("_", " "))
        if (
            include_status_and_multiplier
            and isinstance(multiplier_value, float)
            and multiplier_value > 1.000001
        ):
            escalation_parts.append(f"x{multiplier_value:.2f}")
        if isinstance(approval_rate_value, float) and isinstance(intents_total_value, int) and intents_total_value > 0:
            escalation_parts.append(
                f"loop sample {approval_rate_value*100.0:.2f}% on {intents_total_value:,} intents"
            )
        else:
            if isinstance(approval_rate_value, float):
                escalation_parts.append(f"loop sample {approval_rate_value*100.0:.2f}%")
            if isinstance(intents_total_value, int) and intents_total_value > 0:
                escalation_parts.append(f"loop sample {intents_total_value:,} intents")
        if escalation_parts:
            notes.append("guardrail " + " | ".join(escalation_parts))
    elif guardrail_reason_label and (
        isinstance(approval_rate_value, float) or (isinstance(intents_total_value, int) and intents_total_value > 0)
    ):
        sample_parts: list[str] = []
        if isinstance(approval_rate_value, float):
            sample_parts.append(f"{approval_rate_value*100.0:.2f}%")
        if isinstance(intents_total_value, int) and intents_total_value > 0:
            sample_parts.append(f"{intents_total_value:,} intents")
        if sample_parts:
            notes.append("loop sample " + " on ".join(sample_parts))
        if isinstance(min_intents_required_value, int) and min_intents_required_value > 0:
            requirement_text = f"requires >= {min_intents_required_value:,} intents"
            if (
                isinstance(min_intents_base_value, int)
                and min_intents_base_value > 0
                and isinstance(basis_min_abs_intents_value, int)
                and basis_min_abs_intents_value > 0
            ):
                requirement_text += (
                    f" (base {min_intents_base_value:,}, floor {basis_min_abs_intents_value:,})"
                )
            notes.append(requirement_text)
    sample_source_key = sample_source_text.lower()
    if sample_source_text and sample_source_key not in {"unknown", "none", "shadow_loop_blocker_snapshot"}:
        source_label = sample_source_text.replace("_", " ")
        if sample_age_text != "n/a":
            notes.append(f"sample src {source_label} ({sample_age_text} old)")
        else:
            notes.append(f"sample src {source_label}")

    if not notes:
        return _clip_text(text, 140)
    return _clip_text(", ".join(notes), 140)


def _sorted_blockers(reason_counts: dict[str, Any]) -> list[tuple[str, int]]:
    rows: list[tuple[str, int]] = []
    for key, value in reason_counts.items():
        if _normalize(key).lower() == "approved":
            continue
        count = _parse_int(value)
        if count is None or count <= 0:
            continue
        rows.append((_normalize(key), count))
    rows.sort(key=lambda item: item[1], reverse=True)
    return rows


def _limiting_factor_from_reason(reason: str) -> str:
    key = _normalize(reason).lower()
    if not key:
        return "insufficient_breadth"
    if "stale" in key:
        return "stale_suppression"
    if "cutoff" in key:
        return "cutoff_timing"
    if "final" in key or "pending_final" in key or "settlement" in key:
        return "settlement_finalization"
    if any(
        token in key
        for token in (
            "overlap",
            "still_possible",
            "range_possible",
            "range_still_possible",
            "no_side_interval_overlap",
            "family_conflict",
            "monotonic",
            "breadth",
            "concentration",
            "underlying",
            "exposure_cap",
        )
    ):
        return "insufficient_breadth"
    return "insufficient_breadth"


def _add_suggestion(
    items: list[str],
    structured_items: list[dict[str, Any]],
    seen: set[str],
    key: str,
    text: str,
    *,
    target: str | None = None,
    expected_impact: str | None = None,
    impact_points: float | None = None,
    eta_hours: float | None = None,
    metric_key: str | None = None,
    metric_current: float | int | None = None,
    metric_target: float | int | None = None,
    metric_direction: str | None = None,
    priority_boost_points: float | None = None,
    priority_boost_reason: str | None = None,
) -> None:
    if key in seen:
        return
    seen.add(key)
    action_text = text.strip()
    target_text = _normalize(target)
    expected_impact_text = _normalize(expected_impact)
    parts = [action_text]
    if target_text:
        parts.append(f"Target: {target_text}.")
    if expected_impact_text:
        parts.append(f"Expected impact: {expected_impact_text}.")
    items.append(" ".join(parts).strip())
    impact_value = _parse_float(impact_points)
    eta_value = _parse_float(eta_hours)
    metric_key_text = _normalize(metric_key)
    metric_direction_text = _normalize(metric_direction).lower()
    boost_reason_text = _normalize(priority_boost_reason)
    if metric_direction_text not in {"up", "down"}:
        metric_direction_text = ""
    metric_current_value = _parse_float(metric_current)
    metric_target_value = _parse_float(metric_target)
    priority_boost_value = _parse_float(priority_boost_points)
    if not isinstance(impact_value, float):
        impact_value = 5.0
    if not isinstance(eta_value, float):
        eta_value = 72.0
    if not isinstance(priority_boost_value, float):
        priority_boost_value = 0.0
    impact_value = max(0.1, min(100.0, impact_value))
    eta_value = max(1.0, min(24.0 * 30.0, eta_value))
    priority_boost_value = max(0.0, min(100.0, priority_boost_value))
    priority_score_base = impact_value / eta_value
    priority_score = (impact_value + priority_boost_value) / eta_value
    structured_items.append(
        {
            "key": key,
            "action": action_text,
            "target": target_text or None,
            "expected_impact": expected_impact_text or None,
            "impact_points": round(impact_value, 3),
            "eta_hours": round(eta_value, 3),
            "priority_boost_points": round(priority_boost_value, 3),
            "priority_boost_reason": boost_reason_text or None,
            "priority_score_base": round(priority_score_base, 6),
            "priority_score": round(priority_score, 6),
            "metric_key": metric_key_text or None,
            "metric_current": (
                round(metric_current_value, 6)
                if isinstance(metric_current_value, float)
                else None
            ),
            "metric_target": (
                round(metric_target_value, 6)
                if isinstance(metric_target_value, float)
                else None
            ),
            "metric_direction": metric_direction_text or None,
        }
    )


def _format_suggestion_line(row: dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    action_text = _normalize(row.get("action"))
    target_text = _normalize(row.get("target"))
    expected_impact_text = _normalize(row.get("expected_impact"))
    parts = [action_text]
    if target_text:
        parts.append(f"Target: {target_text}.")
    if expected_impact_text:
        parts.append(f"Expected impact: {expected_impact_text}.")
    return " ".join(part for part in parts if part).strip()


def _format_suggestion_brief(row: dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    key_text = _normalize(row.get("key"))
    short_action_map = {
        "approval_flood_guard": "Cut false-positive approvals.",
        "approval_starvation_guard": "Recover quality-gated approvals.",
        "approval_gate_drift": "Close approval parameter drift.",
        "edge_floor_bucket_tuning": "Retune expected-edge floors by pocket.",
        "stale_freshness": "Cut stale blocks with station/hour freshness tuning.",
        "settlement_pressure": "Reduce settlement finalization lag.",
        "interval_overlap_pressure": "Reduce overlap pressure on range constraints.",
        "weak_station": "Tune weakest station profile.",
        "weak_hour": "Tune weakest local-hour profile.",
        "replan_cooldown_pressure": "Replan on true weather shifts.",
        "replan_repeat_cap_balance": "Balance repeat cap vs valid updates.",
        "replan_backstop_dependency": "Reduce backstop-data dependence.",
        "replan_breadth_bottleneck": "Increase replan breadth.",
        "replan_backstop_near_cap": "Protect backstop headroom.",
        "duplicate_order_reuse": "Cut repeated entries on the same outcome.",
        "breadth": "Increase independent breadth.",
        "weekly_blocker_close": "Close this week's largest blocker first.",
        "live_readiness_blocker": "Close top live-readiness blocker.",
        "pilot_gate_open_reason": "Protect pilot-gate conditions.",
        "next_signal": "Implement the next alpha signal layer.",
    }
    action_text = short_action_map.get(key_text) or _clip_text_plain(_normalize(row.get("action")), 74)
    if key_text == "dominant_blocker_focus":
        target_hint = _normalize(row.get("target")).lower()
        if "expected edge below min" in target_hint:
            action_text = "Fix expected-edge blocker first."
        elif "edge to risk" in target_hint or "edge/risk" in target_hint:
            action_text = "Fix edge-to-risk blocker first."
        elif "metar_observation_stale" in target_hint or "stale" in target_hint:
            action_text = "Fix weather freshness blocker first."
        elif "cutoff" in target_hint:
            action_text = "Fix cutoff-timing blocker first."
        elif "settlement" in target_hint or "finalization" in target_hint:
            action_text = "Fix settlement blocker first."
        else:
            action_text = "Fix dominant blocker first."
    if not action_text:
        return ""
    metric_key = _normalize(row.get("metric_key"))
    metric_direction = _normalize(row.get("metric_direction")).lower()
    metric_current = _parse_float(row.get("metric_current"))
    metric_target = _parse_float(row.get("metric_target"))
    target_text = _normalize(row.get("target"))

    metric_clause = ""
    metric_key_labels = {
        "approval_rate_window": "approval",
        "replan_blocked_ratio": "replan block",
        "replan_repeat_cap_blocked_ratio": "repeat-cap block",
        "dominant_blocker_share_of_blocked": "dominant blocker share",
        "edge_gate_blocked_share_of_blocked": "edge-gate share",
        "resolved_unique_market_sides": "resolved unique sides",
        "trial_duplicate_rows_ratio_since_reset": "duplicate row ratio",
    }
    display_metric_key = metric_key_labels.get(metric_key, metric_key or "metric")
    if metric_key.startswith("station_approval_rate_"):
        station_code = metric_key.replace("station_approval_rate_", "").strip()
        display_metric_key = f"{station_code} approval" if station_code else "station approval"
    if metric_key.startswith("hour_approval_rate_"):
        hour_code = metric_key.replace("hour_approval_rate_", "").strip()
        display_metric_key = f"h{hour_code} approval" if hour_code else "hour approval"

    is_ratio_metric = (
        "rate" in metric_key
        or "ratio" in metric_key
        or "share" in metric_key
        or metric_key.endswith("_pct")
    )
    is_count_metric = metric_key in {
        "resolved_unique_market_sides",
        "resolved_unique_underlying_families",
        "resolved_planned_rows",
        "planned_orders",
        "intents_total",
    }

    def _fmt_metric_value(value: float) -> str:
        if is_ratio_metric:
            return f"{value * 100.0:.1f}%"
        if is_count_metric:
            return f"{int(round(value)):,}"
        return f"{value:.2f}"

    if (
        metric_direction in {"up", "down"}
        and isinstance(metric_current, float)
        and isinstance(metric_target, float)
    ):
        current_text = _fmt_metric_value(metric_current)
        target_text = _fmt_metric_value(metric_target)
        if metric_direction == "up":
            metric_clause = (
                f"{display_metric_key} {current_text} "
                f"(target >= {target_text})"
            )
        else:
            metric_clause = (
                f"{display_metric_key} {current_text} "
                f"(target <= {target_text})"
            )
    elif target_text:
        metric_clause = _clip_text_plain(target_text, 52)

    metric_hint = _clip_text_plain(metric_clause, 34) if metric_clause else ""

    impact_estimate = _parse_float(row.get("impact_dollars_estimate"))
    impact_basis = _normalize(row.get("impact_dollars_basis"))
    impact_hint = ""
    if isinstance(impact_estimate, float) and impact_estimate > 0:
        impact_hint = f"impact ~{_fmt_money_compact(impact_estimate)}"
        if "proxy" in impact_basis:
            impact_hint += " est"

    confidence_label = _normalize(row.get("confidence_label")).upper()
    confidence_hint = f"conf {confidence_label}" if confidence_label else ""

    brief_parts = [action_text]
    if metric_hint:
        brief_parts.append(metric_hint)
    if impact_hint:
        brief_parts.append(impact_hint)
    if confidence_hint:
        brief_parts.append(confidence_hint)
    return " | ".join(part for part in brief_parts if part)


def _suggestion_owner(key: str) -> str:
    mapping = {
        "dominant_blocker_focus": "bot-policy",
        "approval_gate_drift": "bot-policy",
        "edge_floor_bucket_tuning": "bot-policy",
        "stale_freshness": "bot-policy",
        "interval_overlap_pressure": "bot-policy",
        "concentration": "bot-policy",
        "duplicate_order_reuse": "bot-policy",
        "replan_cooldown_pressure": "bot-policy",
        "replan_backstop_dependency": "bot-policy",
        "replan_breadth_bottleneck": "bot-policy",
        "replan_backstop_near_cap": "bot-policy",
        "weak_station": "bot-policy",
        "weak_hour": "bot-policy",
        "breadth": "bot-policy",
        "settlement_pressure": "bot-policy",
        "live_readiness_blocker": "bot+research",
        "pilot_gate_open_reason": "bot+research",
        "next_signal": "research",
        "weekly_blocker_close": "bot+research",
    }
    key_text = _normalize(key)
    if not key_text:
        return "bot-policy"
    return mapping.get(key_text, "bot-policy")


def _suggestion_file_hint(key: str) -> str:
    mapping = {
        "dominant_blocker_focus": "betbot/kalshi_temperature_trader.py + betbot/kalshi_temperature_constraints.py",
        "approval_gate_drift": "infra/digitalocean/summarize_window.py + betbot/kalshi_temperature_trader.py",
        "edge_floor_bucket_tuning": "betbot/kalshi_temperature_trader.py + outputs/.../runtime/metar_age_policy_auto.json",
        "stale_freshness": "outputs/.../runtime/metar_age_policy_auto.json + infra/digitalocean/run_temperature_shadow_loop.sh",
        "interval_overlap_pressure": "betbot/kalshi_temperature_constraints.py + betbot/kalshi_weather_priors.py",
        "concentration": "betbot/kalshi_temperature_trader.py",
        "duplicate_order_reuse": "betbot/kalshi_temperature_profitability.py + infra/digitalocean/run_temperature_shadow_loop.sh",
        "replan_cooldown_pressure": "betbot/kalshi_temperature_trader.py",
        "replan_backstop_dependency": "betbot/kalshi_temperature_trader.py + infra/digitalocean/run_temperature_shadow_loop.sh",
        "replan_breadth_bottleneck": "betbot/kalshi_temperature_contract_specs.py + infra/digitalocean/run_temperature_shadow_loop.sh",
        "replan_backstop_near_cap": "betbot/kalshi_temperature_trader.py",
        "weak_station": "outputs/.../runtime/metar_age_policy_auto.json",
        "weak_hour": "outputs/.../runtime/metar_age_policy_auto.json + infra/digitalocean/run_temperature_shadow_loop.sh",
        "breadth": "infra/digitalocean/run_temperature_alpha_workers.sh + run_temperature_shadow_loop.sh",
        "settlement_pressure": "infra/digitalocean/run_temperature_shadow_loop.sh + betbot/kalshi_temperature_settlement_state.py",
        "live_readiness_blocker": "betbot/kalshi_temperature_live_readiness.py",
        "pilot_gate_open_reason": "betbot/kalshi_temperature_live_readiness.py",
        "next_signal": "betbot/kalshi_weather_priors.py",
        "weekly_blocker_close": "infra/digitalocean/run_temperature_blocker_audit.sh",
    }
    key_text = _normalize(key)
    if not key_text:
        return "n/a"
    return mapping.get(key_text, "n/a")


def _safe_write_json(path: Path, payload: dict[str, Any]) -> bool:
    encoded = json.dumps(payload, indent=2)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        return False
    except Exception:
        return False
    tmp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}-{time.time_ns()}")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        return True
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        return False


def _approval_profile_signature(profile: dict[str, Any]) -> tuple[float | None, float | None, float | None, float | None]:
    if not isinstance(profile, dict):
        return (None, None, None, None)
    settlement = _parse_float(profile.get("min_settlement_confidence"))
    alpha = _parse_float(profile.get("min_alpha_strength"))
    prob = _parse_float(profile.get("min_probability_confidence"))
    edge = _parse_float(profile.get("min_expected_edge_net"))
    return (
        round(settlement, 6) if isinstance(settlement, float) else None,
        round(alpha, 6) if isinstance(alpha, float) else None,
        round(prob, 6) if isinstance(prob, float) else None,
        round(edge, 6) if isinstance(edge, float) else None,
    )


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    q = max(0.0, min(1.0, float(q)))
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = q * float(len(ordered) - 1)
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    frac = position - float(lower)
    if upper == lower:
        return float(ordered[lower])
    return float(ordered[lower]) * (1.0 - frac) + float(ordered[upper]) * frac


def _read_intent_quality_rows_from_csv(path: Path) -> list[dict[str, float]]:
    if not path.exists():
        return []
    rows: list[dict[str, float]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            alpha = _parse_float(row.get("policy_alpha_strength"))
            prob = _parse_float(row.get("policy_probability_confidence"))
            edge = _parse_float(row.get("policy_expected_edge_net"))
            settle = _parse_float(row.get("settlement_confidence_score"))
            if not all(isinstance(value, float) for value in (alpha, prob, edge, settle)):
                continue
            rows.append(
                {
                    "alpha": float(alpha),
                    "probability": max(0.0, min(1.0, float(prob))),
                    "edge": float(edge),
                    "settlement": max(0.0, min(1.0, float(settle))),
                }
            )
    return rows


def _load_intent_quality_rows(intents_summary_payload: dict[str, Any]) -> list[dict[str, float]]:
    if not isinstance(intents_summary_payload, dict):
        return []
    csv_path = _normalize(intents_summary_payload.get("output_csv"))
    if not csv_path:
        return []
    return _read_intent_quality_rows_from_csv(Path(csv_path))


def _parse_file_timestamp_epoch(path: Path) -> float:
    match = re.search(r"(\d{8}_\d{6})", path.name)
    if match:
        token = match.group(1)
        try:
            dt = datetime.strptime(token, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
            return float(dt.timestamp())
        except ValueError:
            pass
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return 0.0


def _intent_summary_files_in_window(output_dir: Path, start_epoch: float, end_epoch: float) -> list[Path]:
    files: list[tuple[float, Path]] = []
    for path in output_dir.glob("kalshi_temperature_trade_intents_summary_*.json"):
        if path.name.endswith("_latest.json"):
            continue
        ts_epoch = _parse_file_timestamp_epoch(path)
        if ts_epoch < float(start_epoch) or ts_epoch > float(end_epoch):
            continue
        files.append((ts_epoch, path))
    files.sort(key=lambda item: (item[0], item[1].name))
    return [path for _, path in files]


def _load_intent_quality_rows_from_window(
    summary_files: list[Path],
) -> tuple[list[dict[str, float]], list[str], list[str]]:
    rows: list[dict[str, float]] = []
    summary_names: list[str] = []
    csv_names: list[str] = []
    seen_csv_paths: set[str] = set()
    for summary_file in summary_files:
        payload = _load_json(str(summary_file))
        csv_path_text = _normalize(payload.get("output_csv"))
        if not csv_path_text:
            continue
        csv_path = Path(csv_path_text)
        if not csv_path.is_absolute():
            csv_path = (summary_file.parent / csv_path).resolve()
        csv_key = str(csv_path)
        if csv_key in seen_csv_paths:
            continue
        seen_csv_paths.add(csv_key)
        csv_rows = _read_intent_quality_rows_from_csv(csv_path)
        if not csv_rows:
            continue
        rows.extend(csv_rows)
        summary_names.append(summary_file.name)
        csv_names.append(csv_path.name)
    return rows, summary_names, csv_names


def _aggregate_recent_intent_summary_sample(
    summary_files: list[Path],
    *,
    max_files: int,
) -> dict[str, Any]:
    if not summary_files:
        return {}
    file_limit = max(1, int(max_files))
    selected = summary_files[-file_limit:]
    intents_total = 0
    intents_approved = 0
    files_used: list[str] = []
    for summary_file in selected:
        payload = _load_json(str(summary_file))
        if not payload:
            continue
        file_intents_total = _parse_int(payload.get("intents_total")) or 0
        if file_intents_total <= 0:
            continue
        file_intents_approved = _parse_int(payload.get("intents_revalidated"))
        if file_intents_approved is None:
            file_intents_approved = _parse_int(payload.get("intents_approved"))
        if file_intents_approved is None:
            file_intents_approved = _parse_int(payload.get("intents_selected_for_plan"))
        approved_value = max(0, int(file_intents_approved or 0))
        if approved_value > file_intents_total:
            approved_value = int(file_intents_total)
        intents_total += int(file_intents_total)
        intents_approved += int(approved_value)
        files_used.append(summary_file.name)
    if intents_total <= 0:
        return {}
    approval_rate = float(intents_approved) / float(intents_total)
    return {
        "status": "ready",
        "files_considered": int(len(selected)),
        "files_used_count": int(len(files_used)),
        "files_used_preview": files_used[-10:],
        "intents_total": int(intents_total),
        "intents_approved": int(intents_approved),
        "approval_rate": round(float(approval_rate), 6),
    }


def _aggregate_sparse_hardening_from_summaries(
    summary_files: list[Path],
) -> dict[str, Any]:
    intents_total = 0
    applied_count = 0
    approved_count = 0
    blocked_count = 0
    blocked_probability_below_min_count = 0
    blocked_expected_edge_below_min_count = 0
    blocked_edge_to_risk_below_min_count = 0
    files_used: list[str] = []
    weighted_probability_raise_sum = 0.0
    weighted_probability_raise_weight = 0
    weighted_expected_edge_raise_sum = 0.0
    weighted_expected_edge_raise_weight = 0
    weighted_support_score_sum = 0.0
    weighted_support_score_weight = 0
    weighted_volatility_penalty_sum = 0.0
    weighted_volatility_penalty_weight = 0
    probability_raise_max: float | None = None
    expected_edge_raise_max: float | None = None

    for summary_file in summary_files:
        payload = _load_json(str(summary_file))
        if not payload:
            continue
        file_intents_total = max(0, _parse_int(payload.get("intents_total")) or 0)
        if file_intents_total <= 0:
            continue
        file_applied = max(0, _parse_int(payload.get("sparse_evidence_hardening_applied_count")) or 0)
        file_approved = max(0, _parse_int(payload.get("sparse_evidence_hardening_approved_count")) or 0)
        file_blocked = max(0, _parse_int(payload.get("sparse_evidence_hardening_blocked_count")) or 0)

        if file_applied <= 0:
            file_applied = 0
            file_approved = 0
            file_blocked = 0
        else:
            file_approved = min(file_approved, file_applied)
            file_blocked = min(file_blocked, file_applied)

        file_prob_blocked = max(
            0,
            _parse_int(payload.get("sparse_evidence_hardening_blocked_probability_below_min_count")) or 0,
        )
        file_edge_blocked = max(
            0,
            _parse_int(payload.get("sparse_evidence_hardening_blocked_expected_edge_below_min_count")) or 0,
        )
        file_e2r_blocked = max(
            0,
            _parse_int(payload.get("sparse_evidence_hardening_blocked_edge_to_risk_below_min_count")) or 0,
        )
        if file_blocked <= 0:
            file_prob_blocked = 0
            file_edge_blocked = 0
            file_e2r_blocked = 0
        else:
            file_prob_blocked = min(file_prob_blocked, file_blocked)
            file_edge_blocked = min(file_edge_blocked, file_blocked)
            file_e2r_blocked = min(file_e2r_blocked, file_blocked)

        intents_total += int(file_intents_total)
        applied_count += int(file_applied)
        approved_count += int(file_approved)
        blocked_count += int(file_blocked)
        blocked_probability_below_min_count += int(file_prob_blocked)
        blocked_expected_edge_below_min_count += int(file_edge_blocked)
        blocked_edge_to_risk_below_min_count += int(file_e2r_blocked)
        files_used.append(summary_file.name)

        probability_raise_avg = _parse_float(payload.get("sparse_evidence_probability_raise_avg"))
        if isinstance(probability_raise_avg, float) and file_applied > 0:
            weighted_probability_raise_sum += float(probability_raise_avg) * float(file_applied)
            weighted_probability_raise_weight += int(file_applied)
        probability_raise_file_max = _parse_float(payload.get("sparse_evidence_probability_raise_max"))
        if isinstance(probability_raise_file_max, float):
            probability_raise_max = (
                float(probability_raise_file_max)
                if probability_raise_max is None
                else max(float(probability_raise_max), float(probability_raise_file_max))
            )

        expected_edge_raise_avg = _parse_float(payload.get("sparse_evidence_expected_edge_raise_avg"))
        if isinstance(expected_edge_raise_avg, float) and file_applied > 0:
            weighted_expected_edge_raise_sum += float(expected_edge_raise_avg) * float(file_applied)
            weighted_expected_edge_raise_weight += int(file_applied)
        expected_edge_raise_file_max = _parse_float(payload.get("sparse_evidence_expected_edge_raise_max"))
        if isinstance(expected_edge_raise_file_max, float):
            expected_edge_raise_max = (
                float(expected_edge_raise_file_max)
                if expected_edge_raise_max is None
                else max(float(expected_edge_raise_max), float(expected_edge_raise_file_max))
            )

        support_score_avg = _parse_float(payload.get("sparse_evidence_support_score_avg"))
        if isinstance(support_score_avg, float) and file_applied > 0:
            weighted_support_score_sum += float(support_score_avg) * float(file_applied)
            weighted_support_score_weight += int(file_applied)

        volatility_penalty_avg = _parse_float(payload.get("sparse_evidence_volatility_penalty_avg"))
        if isinstance(volatility_penalty_avg, float) and file_applied > 0:
            weighted_volatility_penalty_sum += float(volatility_penalty_avg) * float(file_applied)
            weighted_volatility_penalty_weight += int(file_applied)

    if intents_total <= 0:
        return {}

    return {
        "status": "ready",
        "files_used_count": int(len(files_used)),
        "files_used_preview": files_used[-10:],
        "intents_total": int(intents_total),
        "applied_count": int(applied_count),
        "approved_count": int(approved_count),
        "blocked_count": int(blocked_count),
        "blocked_probability_below_min_count": int(blocked_probability_below_min_count),
        "blocked_expected_edge_below_min_count": int(blocked_expected_edge_below_min_count),
        "blocked_edge_to_risk_below_min_count": int(blocked_edge_to_risk_below_min_count),
        "applied_rate": (
            round(float(applied_count) / float(intents_total), 6)
            if intents_total > 0
            else None
        ),
        "blocked_share_of_hardened": (
            round(float(blocked_count) / float(applied_count), 6)
            if applied_count > 0
            else None
        ),
        "blocked_probability_share": (
            round(float(blocked_probability_below_min_count) / float(blocked_count), 6)
            if blocked_count > 0
            else None
        ),
        "blocked_expected_edge_share": (
            round(float(blocked_expected_edge_below_min_count) / float(blocked_count), 6)
            if blocked_count > 0
            else None
        ),
        "blocked_edge_to_risk_share": (
            round(float(blocked_edge_to_risk_below_min_count) / float(blocked_count), 6)
            if blocked_count > 0
            else None
        ),
        "probability_raise_avg": (
            round(float(weighted_probability_raise_sum) / float(weighted_probability_raise_weight), 6)
            if weighted_probability_raise_weight > 0
            else None
        ),
        "probability_raise_max": (
            round(float(probability_raise_max), 6)
            if isinstance(probability_raise_max, float)
            else None
        ),
        "expected_edge_raise_avg": (
            round(float(weighted_expected_edge_raise_sum) / float(weighted_expected_edge_raise_weight), 6)
            if weighted_expected_edge_raise_weight > 0
            else None
        ),
        "expected_edge_raise_max": (
            round(float(expected_edge_raise_max), 6)
            if isinstance(expected_edge_raise_max, float)
            else None
        ),
        "support_score_avg": (
            round(float(weighted_support_score_sum) / float(weighted_support_score_weight), 6)
            if weighted_support_score_weight > 0
            else None
        ),
        "volatility_penalty_avg": (
            round(float(weighted_volatility_penalty_sum) / float(weighted_volatility_penalty_weight), 6)
            if weighted_volatility_penalty_weight > 0
            else None
        ),
    }


def _estimate_approval_rate_for_profile(
    rows: list[dict[str, float]],
    *,
    min_settlement_confidence: float | None,
    min_alpha_strength: float | None,
    min_probability_confidence: float | None,
    min_expected_edge_net: float | None,
) -> tuple[int, float]:
    if not rows:
        return 0, 0.0
    approved = 0
    for row in rows:
        if isinstance(min_settlement_confidence, float) and row["settlement"] < min_settlement_confidence:
            continue
        if isinstance(min_alpha_strength, float) and row["alpha"] < min_alpha_strength:
            continue
        if isinstance(min_probability_confidence, float) and row["probability"] < min_probability_confidence:
            continue
        if isinstance(min_expected_edge_net, float) and row["edge"] < min_expected_edge_net:
            continue
        approved += 1
    return approved, (approved / float(len(rows)))


def _build_approval_guardrail_recommendation(
    rows: list[dict[str, float]],
    *,
    guard_min: float,
    guard_max: float,
    current_approval_rate: float,
    min_settlement_confidence: float | None,
    min_alpha_strength: float | None,
    min_probability_confidence: float | None,
    min_expected_edge_net: float | None,
) -> dict[str, Any]:
    if not rows:
        return {}

    row_count = len(rows)
    row_count_float = float(row_count)
    alpha_values = [row["alpha"] for row in rows]
    prob_values = [row["probability"] for row in rows]
    edge_values = [row["edge"] for row in rows]
    settlement_values = [row["settlement"] for row in rows]
    alpha_min = min(alpha_values)
    alpha_max = max(alpha_values)
    prob_min = min(prob_values)
    prob_max = max(prob_values)
    edge_min = min(edge_values)
    edge_max = max(edge_values)
    alpha_span = max(1e-9, alpha_max - alpha_min)
    prob_span = max(1e-9, prob_max - prob_min)
    edge_span = max(1e-9, edge_max - edge_min)
    quantiles = [0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90, 0.92, 0.95, 0.97, 0.98, 0.99]

    word_size = 64
    word_count = (row_count + (word_size - 1)) // word_size
    full_word = (1 << word_size) - 1
    all_words = [full_word] * word_count
    trailing_bits = row_count % word_size
    if word_count > 0 and trailing_bits:
        all_words[-1] = (1 << trailing_bits) - 1
    all_words_tuple = tuple(all_words)

    def _build_mask_words(values: list[float], threshold: float | None) -> tuple[int, ...]:
        if not isinstance(threshold, float):
            return all_words_tuple
        words = [0] * word_count
        for idx, value in enumerate(values):
            if value >= threshold:
                words[idx >> 6] |= 1 << (idx & 63)
        return tuple(words)

    def _count_intersection_words(
        mask_settlement: tuple[int, ...],
        mask_alpha: tuple[int, ...],
        mask_prob: tuple[int, ...],
        mask_edge: tuple[int, ...],
    ) -> int:
        total = 0
        for set_word, alpha_word, prob_word, edge_word in zip(
            mask_settlement, mask_alpha, mask_prob, mask_edge
        ):
            total += (set_word & alpha_word & prob_word & edge_word).bit_count()
        return total

    current_alpha = min_alpha_strength if isinstance(min_alpha_strength, float) else alpha_min
    current_prob = min_probability_confidence if isinstance(min_probability_confidence, float) else prob_min
    current_edge = min_expected_edge_net if isinstance(min_expected_edge_net, float) else edge_min
    settlement_mask_words = _build_mask_words(settlement_values, min_settlement_confidence)
    current_alpha_mask_words = _build_mask_words(alpha_values, float(current_alpha))
    current_prob_mask_words = _build_mask_words(prob_values, float(current_prob))
    current_edge_mask_words = _build_mask_words(edge_values, float(current_edge))
    current_approved_count = _count_intersection_words(
        settlement_mask_words,
        current_alpha_mask_words,
        current_prob_mask_words,
        current_edge_mask_words,
    )
    current_profile_approval_rate = current_approved_count / row_count_float

    if current_profile_approval_rate > guard_max:
        mode = "tighten"
        target_rate = guard_max
    elif current_profile_approval_rate < guard_min:
        mode = "loosen"
        target_rate = guard_min
    else:
        mode = "hold"
        target_rate = current_profile_approval_rate

    alpha_candidates = {float(current_alpha)}
    prob_candidates = {float(current_prob)}
    edge_candidates = {float(current_edge)}
    for q in quantiles:
        aq = _quantile(alpha_values, q)
        pq = _quantile(prob_values, q)
        eq = _quantile(edge_values, q)
        if isinstance(aq, float):
            alpha_candidates.add(float(aq))
        if isinstance(pq, float):
            prob_candidates.add(float(pq))
        if isinstance(eq, float):
            edge_candidates.add(float(eq))

    if mode == "tighten":
        alpha_candidates = {value for value in alpha_candidates if value >= current_alpha}
        prob_candidates = {value for value in prob_candidates if value >= current_prob}
        edge_candidates = {value for value in edge_candidates if value >= current_edge}
    elif mode == "loosen":
        alpha_candidates = {value for value in alpha_candidates if value <= current_alpha}
        prob_candidates = {value for value in prob_candidates if value <= current_prob}
        edge_candidates = {value for value in edge_candidates if value <= current_edge}

    alpha_candidates_sorted = sorted(alpha_candidates)
    prob_candidates_sorted = sorted(prob_candidates)
    edge_candidates_sorted = sorted(edge_candidates)
    if not alpha_candidates_sorted:
        alpha_candidates_sorted = [float(current_alpha)]
    if not prob_candidates_sorted:
        prob_candidates_sorted = [float(current_prob)]
    if not edge_candidates_sorted:
        edge_candidates_sorted = [float(current_edge)]

    alpha_mask_words_by_threshold = {
        float(alpha_value): _build_mask_words(alpha_values, float(alpha_value))
        for alpha_value in alpha_candidates_sorted
    }
    prob_mask_words_by_threshold = {
        float(prob_value): _build_mask_words(prob_values, float(prob_value))
        for prob_value in prob_candidates_sorted
    }
    edge_mask_words_by_threshold = {
        float(edge_value): _build_mask_words(edge_values, float(edge_value))
        for edge_value in edge_candidates_sorted
    }

    target_mid = (guard_min + guard_max) * 0.5
    target_for_score = target_mid if mode in {"tighten", "loosen"} else current_profile_approval_rate
    feasible: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    for alpha_value in alpha_candidates_sorted:
        alpha_mask_words = alpha_mask_words_by_threshold[float(alpha_value)]
        for prob_value in prob_candidates_sorted:
            prob_mask_words = prob_mask_words_by_threshold[float(prob_value)]
            for edge_value in edge_candidates_sorted:
                edge_mask_words = edge_mask_words_by_threshold[float(edge_value)]
                approved_count = _count_intersection_words(
                    settlement_mask_words,
                    alpha_mask_words,
                    prob_mask_words,
                    edge_mask_words,
                )
                projected_rate = approved_count / row_count_float
                distance = (
                    abs(float(alpha_value) - float(current_alpha)) / alpha_span
                    + abs(float(prob_value) - float(current_prob)) / prob_span
                    + abs(float(edge_value) - float(current_edge)) / edge_span
                )
                score = abs(projected_rate - target_for_score) + (0.12 * distance)
                row = {
                    "min_alpha_strength": round(float(alpha_value), 6),
                    "min_probability_confidence": round(float(prob_value), 6),
                    "min_expected_edge_net": round(float(edge_value), 6),
                    "projected_approved_count": int(approved_count),
                    "projected_approval_rate": round(float(projected_rate), 6),
                    "distance_from_current": round(float(distance), 6),
                    "score": round(float(score), 6),
                }
                if guard_min <= projected_rate <= guard_max:
                    feasible.append(row)
                all_rows.append(row)

    candidate_rows = feasible if feasible else all_rows
    candidate_rows.sort(
        key=lambda row: (
            float(row.get("score") or 0.0),
            float(row.get("distance_from_current") or 0.0),
            abs(float(row.get("projected_approval_rate") or 0.0) - target_for_score),
        )
    )
    best = candidate_rows[0] if candidate_rows else {}
    if not best:
        return {}
    estimated_delta = int(best["projected_approved_count"]) - int(current_approved_count)
    return {
        "status": "ready",
        "mode": mode,
        "intents_evaluated": int(row_count),
        "guardrail_min_rate": round(float(guard_min), 6),
        "guardrail_max_rate": round(float(guard_max), 6),
        "current_profile": {
            "min_settlement_confidence": (
                round(float(min_settlement_confidence), 6)
                if isinstance(min_settlement_confidence, float)
                else None
            ),
            "min_alpha_strength": round(float(current_alpha), 6),
            "min_probability_confidence": round(float(current_prob), 6),
            "min_expected_edge_net": round(float(current_edge), 6),
            "current_approval_rate": round(float(current_profile_approval_rate), 6),
            "current_approved_count_estimate": int(current_approved_count),
            "current_approval_rate_context": round(float(current_approval_rate), 6),
        },
        "recommended_profile": {
            "min_settlement_confidence": (
                round(float(min_settlement_confidence), 6)
                if isinstance(min_settlement_confidence, float)
                else None
            ),
            "min_alpha_strength": float(best["min_alpha_strength"]),
            "min_probability_confidence": float(best["min_probability_confidence"]),
            "min_expected_edge_net": float(best["min_expected_edge_net"]),
            "projected_approved_count": int(best["projected_approved_count"]),
            "projected_approval_rate": float(best["projected_approval_rate"]),
            "estimated_approved_count_delta": int(estimated_delta),
        },
        "fit_within_guardrail": bool(guard_min <= float(best["projected_approval_rate"]) <= guard_max),
        "search_candidates_considered": int(len(all_rows)),
        "search_candidates_within_guardrail": int(len(feasible)),
    }


window_summary_path = Path(sys.argv[1])
bankroll_path = _normalize(sys.argv[2])
alpha_gap_path = _normalize(sys.argv[3])
live_status_path = _normalize(sys.argv[4])
live_readiness_path = _normalize(sys.argv[5])
go_live_gate_path = _normalize(sys.argv[6])
blocker_audit_path = _normalize(sys.argv[7])
plan_summary_path = _normalize(sys.argv[8])
intents_summary_path = _normalize(sys.argv[9])
output_path = Path(sys.argv[10])
latest_path = Path(sys.argv[11])
health_latest_path = Path(sys.argv[12])
reference_bankroll = max(1.0, float(sys.argv[13]))
suggestion_count = max(3, min(5, int(float(sys.argv[14]))))
window_label = _normalize(sys.argv[15]) or "12h"
approval_rate_guardrail_min = max(0.0, min(1.0, float(sys.argv[16])))
approval_rate_guardrail_max = max(approval_rate_guardrail_min, min(1.0, float(sys.argv[17])))
approval_rate_guardrail_min_intents = max(1, int(float(sys.argv[18])))
approval_rate_guardrail_critical_high = max(
    approval_rate_guardrail_max,
    min(1.0, float(sys.argv[19])),
)
auto_apply_enabled_input = _normalize(sys.argv[20])
auto_apply_streak_required_input = _normalize(sys.argv[21])
auto_apply_state_file_input = _normalize(sys.argv[22])
auto_apply_profile_file_input = _normalize(sys.argv[23])
auto_apply_min_rows_input = _normalize(sys.argv[24])
auto_apply_release_enabled_input = _normalize(sys.argv[25])
auto_apply_release_streak_required_input = _normalize(sys.argv[26])
quality_drift_delta_pp_min_input = _normalize(sys.argv[27])
quality_drift_min_intents_input = _normalize(sys.argv[28])
quality_drift_max_resolved_delta_input = _normalize(sys.argv[29])
gate_coverage_alert_min_approved_rows_input = _normalize(sys.argv[30])
gate_coverage_expected_edge_min_input = _normalize(sys.argv[31])
gate_coverage_probability_min_input = _normalize(sys.argv[32])
gate_coverage_alpha_min_input = _normalize(sys.argv[33])
guardrail_basis_min_ratio_to_window_input = _normalize(sys.argv[34])
guardrail_basis_min_abs_intents_input = _normalize(sys.argv[35])
gate_coverage_auto_apply_enabled_input = _normalize(sys.argv[36])
gate_coverage_auto_apply_streak_required_input = _normalize(sys.argv[37])
gate_coverage_auto_apply_min_rows_input = _normalize(sys.argv[38])
gate_coverage_auto_apply_min_level_input = _normalize(sys.argv[39])
auto_apply_zero_approved_streak_required_input = _normalize(sys.argv[40])
auto_apply_zero_approved_min_intents_input = _normalize(sys.argv[41])
discord_mode = _normalize(os.environ.get("ALPHA_SUMMARY_DISCORD_MODE")).lower() or "concise"
if discord_mode not in {"concise", "detailed"}:
    discord_mode = "concise"
concise_max_lines = max(
    12,
    int(float(_normalize(os.environ.get("ALPHA_SUMMARY_CONCISE_MAX_LINES")) or 16)),
)
replan_cooldown_min_input_count = max(
    1,
    int(float(_normalize(os.environ.get("REPLAN_COOLDOWN_ADAPTIVE_MIN_INPUT_COUNT")) or 25)),
)
quality_risk_streak_red_required = max(
    1,
    int(float(_normalize(os.environ.get("ALPHA_SUMMARY_QUALITY_RISK_STREAK_RED_REQUIRED")) or 3)),
)
quality_risk_auto_apply_streak_required = max(
    1,
    int(float(_normalize(os.environ.get("ALPHA_SUMMARY_QUALITY_RISK_AUTO_APPLY_STREAK_REQUIRED")) or 2)),
)
auto_apply_stability_enabled = _parse_bool(
    _normalize(os.environ.get("ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_ENABLED")),
    True,
)
auto_apply_stability_windows_required = max(
    1,
    int(float(_normalize(os.environ.get("ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_WINDOWS_REQUIRED")) or 2)),
)
auto_apply_stability_max_approval_delta_pp = max(
    0.0,
    float(_normalize(os.environ.get("ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_MAX_APPROVAL_DELTA_PP")) or 2.0),
)
auto_apply_stability_max_stale_delta_pp = max(
    0.0,
    float(_normalize(os.environ.get("ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STABILITY_MAX_STALE_DELTA_PP")) or 3.0),
)
quality_risk_state_file_input = _normalize(os.environ.get("ALPHA_SUMMARY_QUALITY_RISK_STATE_FILE"))
guardrail_recent_summary_files = max(
    3,
    int(float(_normalize(os.environ.get("ALPHA_SUMMARY_APPROVAL_GUARDRAIL_RECENT_FILES")) or 12)),
)
settled_evidence_full_at = max(
    1,
    int(float(_normalize(os.environ.get("ALPHA_SUMMARY_SETTLED_EVIDENCE_FULL_AT")) or 30)),
)
expected_edge_proxy_discount_min = max(
    0.0,
    min(1.0, float(_normalize(os.environ.get("ALPHA_SUMMARY_EXPECTED_EDGE_PROXY_DISCOUNT_MIN")) or 0.2)),
)
expected_edge_proxy_discount_max = max(
    expected_edge_proxy_discount_min,
    min(1.0, float(_normalize(os.environ.get("ALPHA_SUMMARY_EXPECTED_EDGE_PROXY_DISCOUNT_MAX")) or 1.0)),
)
deploy_confidence_negative_pnl_cap = max(
    0.0,
    min(100.0, float(_normalize(os.environ.get("ALPHA_SUMMARY_DEPLOY_CONFIDENCE_NEGATIVE_PNL_CAP")) or 49.0)),
)
deploy_confidence_hysa_fail_cap = max(
    deploy_confidence_negative_pnl_cap,
    min(100.0, float(_normalize(os.environ.get("ALPHA_SUMMARY_DEPLOY_CONFIDENCE_HYSA_FAIL_CAP")) or 54.0)),
)
deploy_confidence_hysa_fail_requires_settled = _parse_bool(
    _normalize(os.environ.get("ALPHA_SUMMARY_DEPLOY_CONFIDENCE_HYSA_FAIL_REQUIRES_SETTLED")),
    True,
)
stage_seconds_summarize_window = _parse_int(
    os.environ.get("ALPHA_SUMMARY_STAGE_SECONDS_SUMMARIZE_WINDOW")
)
stage_seconds_bankroll_validation = _parse_int(
    os.environ.get("ALPHA_SUMMARY_STAGE_SECONDS_BANKROLL_VALIDATION")
)
stage_seconds_alpha_gap_report = _parse_int(
    os.environ.get("ALPHA_SUMMARY_STAGE_SECONDS_ALPHA_GAP_REPORT")
)
stage_seconds_selection_quality = _parse_int(
    os.environ.get("ALPHA_SUMMARY_STAGE_SECONDS_SELECTION_QUALITY")
)
stage_seconds_render_summary = _parse_int(
    os.environ.get("ALPHA_SUMMARY_STAGE_SECONDS_RENDER_SUMMARY")
)
stage_seconds_webhook_send = _parse_int(
    os.environ.get("ALPHA_SUMMARY_STAGE_SECONDS_WEBHOOK_SEND")
)
stage_seconds_total = _parse_int(
    os.environ.get("ALPHA_SUMMARY_STAGE_SECONDS_TOTAL")
)
runtime_stage_seconds = {
    "summarize_window": stage_seconds_summarize_window,
    "bankroll_validation": stage_seconds_bankroll_validation,
    "alpha_gap_report": stage_seconds_alpha_gap_report,
    "selection_quality": stage_seconds_selection_quality,
    "render_summary": stage_seconds_render_summary,
    "webhook_send": stage_seconds_webhook_send,
    "total": stage_seconds_total,
}

window_summary, window_summary_load_status = _load_json_with_status(str(window_summary_path))
profitability_file = _normalize(window_summary.get("profitability_file"))
profitability, profitability_load_status = _load_json_with_status(profitability_file)
bankroll, bankroll_load_status = _load_json_with_status(bankroll_path)
alpha_gap, alpha_gap_load_status = _load_json_with_status(alpha_gap_path)
live_status, live_status_primary_load_status = _load_json_with_status(live_status_path)
live_status_effective_load_status = live_status_primary_load_status
if not live_status:
    live_status_fallback_candidates = [
        str(window_summary_path.parent / "live_status_latest.json"),
        str(window_summary_path.parent.parent / "checkpoints" / "live_status_latest.json"),
        str(window_summary_path.parent.parent / "health" / "live_status_latest.json"),
    ]
    for candidate in live_status_fallback_candidates:
        candidate_text = _normalize(candidate)
        if not candidate_text or candidate_text == _normalize(live_status_path):
            continue
        candidate_payload, candidate_load_status = _load_json_with_status(candidate_text)
        if candidate_payload:
            live_status = candidate_payload
            live_status_path = candidate_text
            live_status_effective_load_status = candidate_load_status
            break
if not live_status:
    derived_live_status = _derive_live_status_from_window_summary(window_summary)
    if derived_live_status:
        live_status = derived_live_status
        live_status_path = f"derived:{window_summary_path}"
        live_status_effective_load_status = "derived_window_summary"
live_readiness, live_readiness_load_status = _load_json_with_status(live_readiness_path)
go_live_gate, go_live_gate_load_status = _load_json_with_status(go_live_gate_path)
blocker_audit, blocker_audit_load_status = _load_json_with_status(blocker_audit_path)
latest_plan_summary, latest_plan_summary_load_status = _load_json_with_status(plan_summary_path)
intents_summary, intents_summary_load_status = _load_json_with_status(intents_summary_path)
previous_alpha_summary, previous_alpha_summary_load_status = _load_json_with_status(str(health_latest_path))
artifact_load_status = {
    "window_summary": window_summary_load_status,
    "profitability": profitability_load_status,
    "bankroll_validation": bankroll_load_status,
    "alpha_gap": alpha_gap_load_status,
    "live_status_primary": live_status_primary_load_status,
    "live_status_effective": live_status_effective_load_status,
    "live_readiness": live_readiness_load_status,
    "go_live_gate": go_live_gate_load_status,
    "blocker_audit": blocker_audit_load_status,
    "latest_plan_summary": latest_plan_summary_load_status,
    "latest_intents_summary": intents_summary_load_status,
    "previous_alpha_summary": previous_alpha_summary_load_status,
}
artifact_parse_error_statuses = {"parse_error", "non_object"}
artifact_parse_error_keys = sorted(
    key for key, status in artifact_load_status.items() if status in artifact_parse_error_statuses
)
critical_artifact_parse_error_keys = sorted(
    key
    for key in artifact_parse_error_keys
    if key in {
        "window_summary",
        "bankroll_validation",
        "alpha_gap",
        "live_readiness",
        "go_live_gate",
        "latest_intents_summary",
    }
)
previous_headline_metrics = (
    previous_alpha_summary.get("headline_metrics")
    if isinstance(previous_alpha_summary.get("headline_metrics"), dict)
    else {}
)
window_meta = window_summary.get("window") if isinstance(window_summary.get("window"), dict) else {}
window_start_epoch = _parse_float(window_meta.get("start_epoch"))
window_end_epoch = _parse_float(window_meta.get("end_epoch"))
window_output_dir = window_summary_path.parent.parent

metar_summary_source_file, metar_summary_source_resolution = _resolve_artifact_file(
    live_status_payload=live_status,
    artifact_key="metar_summary",
    output_dir=window_output_dir,
    fallback_pattern="kalshi_temperature_metar_summary_*.json",
)
settlement_summary_source_file, settlement_summary_source_resolution = _resolve_artifact_file(
    live_status_payload=live_status,
    artifact_key="settlement_summary",
    output_dir=window_output_dir,
    fallback_pattern="kalshi_temperature_settlement_state_*.json",
)
shadow_summary_source_file, shadow_summary_source_resolution = _resolve_artifact_file(
    live_status_payload=live_status,
    artifact_key="shadow_summary",
    output_dir=window_output_dir,
    fallback_pattern="kalshi_temperature_shadow_watch_summary_*.json",
)

selection_quality_path = ""
selection_quality: dict[str, Any] = {}
selection_quality_candidates = sorted(
    window_output_dir.glob("kalshi_temperature_selection_quality_*.json"),
    key=lambda path: float(path.stat().st_mtime) if path.exists() else 0.0,
    reverse=True,
)
for selection_candidate in selection_quality_candidates:
    candidate_payload = _load_json(str(selection_candidate))
    if candidate_payload:
        selection_quality = candidate_payload
        selection_quality_path = str(selection_candidate)
        break

window_intent_summary_files: list[Path] = []
if isinstance(window_start_epoch, float) and isinstance(window_end_epoch, float):
    window_intent_summary_files = _intent_summary_files_in_window(
        window_output_dir,
        window_start_epoch,
        window_end_epoch,
    )

window_intent_quality_rows: list[dict[str, float]] = []
window_intent_quality_summary_files_used: list[str] = []
window_intent_quality_csv_files_used: list[str] = []
if window_intent_summary_files:
    (
        window_intent_quality_rows,
        window_intent_quality_summary_files_used,
        window_intent_quality_csv_files_used,
    ) = _load_intent_quality_rows_from_window(window_intent_summary_files)
window_sparse_hardening_rollup = (
    _aggregate_sparse_hardening_from_summaries(window_intent_summary_files)
    if window_intent_summary_files
    else {}
)

totals = window_summary.get("totals") if isinstance(window_summary.get("totals"), dict) else {}
rates = window_summary.get("rates") if isinstance(window_summary.get("rates"), dict) else {}
window_comparison = (
    window_summary.get("window_comparison")
    if isinstance(window_summary.get("window_comparison"), dict)
    else {}
)
window_comparison_metrics = (
    window_comparison.get("metrics")
    if isinstance(window_comparison.get("metrics"), dict)
    else {}
)
reason_counts = (
    window_summary.get("policy_reason_counts")
    if isinstance(window_summary.get("policy_reason_counts"), dict)
    else {}
)
top_blockers = _sorted_blockers(reason_counts)

quality_gate_profile = (
    live_status.get("quality_gate_profile")
    if isinstance(live_status.get("quality_gate_profile"), dict)
    else {}
)
quality_gate_profile_data_source = "live_status"


def _derive_quality_gate_profile(*rows: dict[str, Any]) -> dict[str, Any]:
    derived: dict[str, Any] = {}
    source: str = ""
    detail_parts: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_source = _normalize(row.get("quality_gate_source")) or _normalize(row.get("source_profile"))
        if row_source and not source:
            source = row_source
        row_detail = _normalize(row.get("quality_gate_detail"))
        if row_detail:
            detail_parts.append(row_detail)
        if "auto_applied" not in derived:
            auto_value = row.get("quality_gate_auto_applied")
            if auto_value is not None:
                derived["auto_applied"] = _parse_bool(auto_value, False)
        if "effective_min_settlement_confidence" not in derived:
            value = _parse_float(row.get("effective_min_settlement_confidence"))
            if not isinstance(value, float):
                value = _parse_float(row.get("min_settlement_confidence"))
            if isinstance(value, float):
                derived["effective_min_settlement_confidence"] = value
        if "effective_min_alpha_strength" not in derived:
            value = _parse_float(row.get("effective_min_alpha_strength"))
            if not isinstance(value, float):
                value = _parse_float(row.get("min_alpha_strength"))
            if isinstance(value, float):
                derived["effective_min_alpha_strength"] = value
        if "effective_min_probability_confidence" not in derived:
            value = _parse_float(row.get("effective_min_probability_confidence"))
            if not isinstance(value, float):
                value = _parse_float(row.get("min_probability_confidence"))
            if isinstance(value, float):
                derived["effective_min_probability_confidence"] = value
        if "effective_min_expected_edge_net" not in derived:
            value = _parse_float(row.get("effective_min_expected_edge_net"))
            if not isinstance(value, float):
                value = _parse_float(row.get("min_expected_edge_net"))
            if isinstance(value, float):
                derived["effective_min_expected_edge_net"] = value
        if "approval_guardrail_escalation_applied" not in derived:
            escalation_applied = row.get("approval_guardrail_escalation_applied")
            if escalation_applied is not None:
                derived["approval_guardrail_escalation_applied"] = _parse_bool(
                    escalation_applied,
                    False,
                )
        if "approval_guardrail_escalation_status" not in derived:
            escalation_status = _normalize(row.get("approval_guardrail_escalation_status"))
            if escalation_status:
                derived["approval_guardrail_escalation_status"] = escalation_status
        if "approval_guardrail_escalation_multiplier" not in derived:
            escalation_multiplier = _parse_float(row.get("approval_guardrail_escalation_multiplier"))
            if isinstance(escalation_multiplier, float):
                derived["approval_guardrail_escalation_multiplier"] = escalation_multiplier
        if "approval_guardrail_escalation_detail" not in derived:
            escalation_detail = _normalize(row.get("approval_guardrail_escalation_detail"))
            if escalation_detail:
                derived["approval_guardrail_escalation_detail"] = escalation_detail
        if "approval_guardrail_escalation_sample_source" not in derived:
            escalation_sample_source = _normalize(row.get("approval_guardrail_escalation_sample_source"))
            if escalation_sample_source:
                derived["approval_guardrail_escalation_sample_source"] = escalation_sample_source
        if "approval_guardrail_escalation_sample_captured_at" not in derived:
            escalation_sample_captured_at = _normalize(
                row.get("approval_guardrail_escalation_sample_captured_at")
            )
            if escalation_sample_captured_at:
                derived["approval_guardrail_escalation_sample_captured_at"] = escalation_sample_captured_at
        if "approval_guardrail_escalation_min_intents_required" not in derived:
            escalation_min_intents_required = _parse_int(
                row.get("approval_guardrail_escalation_min_intents_required")
            )
            if isinstance(escalation_min_intents_required, int):
                derived["approval_guardrail_escalation_min_intents_required"] = escalation_min_intents_required
        if "approval_guardrail_escalation_min_intents_base" not in derived:
            escalation_min_intents_base = _parse_int(
                row.get("approval_guardrail_escalation_min_intents_base")
            )
            if isinstance(escalation_min_intents_base, int):
                derived["approval_guardrail_escalation_min_intents_base"] = escalation_min_intents_base
        if "approval_guardrail_escalation_basis_min_abs_intents" not in derived:
            escalation_basis_floor = _parse_int(
                row.get("approval_guardrail_escalation_basis_min_abs_intents")
            )
            if isinstance(escalation_basis_floor, int):
                derived["approval_guardrail_escalation_basis_min_abs_intents"] = escalation_basis_floor
    if source:
        derived["source"] = source
    if detail_parts:
        deduped_details = []
        seen_details: set[str] = set()
        for detail in detail_parts:
            if detail in seen_details:
                continue
            seen_details.add(detail)
            deduped_details.append(detail)
        derived["detail"] = "; ".join(deduped_details)
    return derived


derived_quality_gate_profile = _derive_quality_gate_profile(
    intents_summary if isinstance(intents_summary, dict) else {},
    latest_plan_summary if isinstance(latest_plan_summary, dict) else {},
)

if not quality_gate_profile:
    quality_gate_profile = dict(derived_quality_gate_profile)
    if quality_gate_profile:
        quality_gate_profile_data_source = "derived_from_intents_plan"
elif derived_quality_gate_profile:
    merged_quality_gate_profile = dict(quality_gate_profile)
    filled_any = False
    for key, value in derived_quality_gate_profile.items():
        current_value = merged_quality_gate_profile.get(key)
        if current_value is None or (
            isinstance(current_value, str) and not _normalize(current_value)
        ):
            merged_quality_gate_profile[key] = value
            filled_any = True
    if filled_any:
        quality_gate_profile = merged_quality_gate_profile
        quality_gate_profile_data_source = "live_status+derived_from_intents_plan"

quality_gate_source = _normalize(quality_gate_profile.get("source"))
quality_gate_auto_applied = _parse_bool(quality_gate_profile.get("auto_applied"), False)
quality_gate_detail = _normalize(quality_gate_profile.get("detail"))
quality_gate_min_settlement = _parse_float(quality_gate_profile.get("effective_min_settlement_confidence"))
quality_gate_min_alpha = _parse_float(quality_gate_profile.get("effective_min_alpha_strength"))
quality_gate_min_probability = _parse_float(quality_gate_profile.get("effective_min_probability_confidence"))
quality_gate_min_edge = _parse_float(quality_gate_profile.get("effective_min_expected_edge_net"))
quality_gate_escalation_applied = _parse_bool(
    quality_gate_profile.get("approval_guardrail_escalation_applied"),
    False,
)
quality_gate_escalation_status = _normalize(
    quality_gate_profile.get("approval_guardrail_escalation_status")
) or "none"
quality_gate_escalation_multiplier = _parse_float(
    quality_gate_profile.get("approval_guardrail_escalation_multiplier")
)
quality_gate_escalation_sample_approval_rate = _parse_float(
    quality_gate_profile.get("approval_guardrail_escalation_sample_approval_rate")
)
quality_gate_escalation_sample_intents_total = _parse_int(
    quality_gate_profile.get("approval_guardrail_escalation_sample_intents_total")
)
quality_gate_escalation_sample_source = _normalize(
    quality_gate_profile.get("approval_guardrail_escalation_sample_source")
) or "unknown"
quality_gate_escalation_sample_captured_at = _normalize(
    quality_gate_profile.get("approval_guardrail_escalation_sample_captured_at")
)
quality_gate_escalation_sample_captured_dt = _parse_iso_datetime(
    quality_gate_escalation_sample_captured_at
)
quality_gate_escalation_sample_age_seconds = (
    max(
        0.0,
        (datetime.now(timezone.utc) - quality_gate_escalation_sample_captured_dt).total_seconds(),
    )
    if isinstance(quality_gate_escalation_sample_captured_dt, datetime)
    else None
)
quality_gate_escalation_min_intents_required = _parse_int(
    quality_gate_profile.get("approval_guardrail_escalation_min_intents_required")
)
quality_gate_escalation_min_intents_base = _parse_int(
    quality_gate_profile.get("approval_guardrail_escalation_min_intents_base")
)
quality_gate_escalation_basis_min_abs_intents = _parse_int(
    quality_gate_profile.get("approval_guardrail_escalation_basis_min_abs_intents")
)
quality_gate_escalation_detail = _normalize(
    quality_gate_profile.get("approval_guardrail_escalation_detail")
)
quality_gate_guardrail_watch_only = (
    (not quality_gate_escalation_applied)
    and (quality_gate_escalation_status not in {"", "none"})
)
if not quality_gate_source:
    has_any_quality_threshold = any(
        isinstance(value, float)
        for value in (
            quality_gate_min_settlement,
            quality_gate_min_alpha,
            quality_gate_min_probability,
            quality_gate_min_edge,
        )
    )
    if has_any_quality_threshold:
        quality_gate_source = quality_gate_profile_data_source
    else:
        quality_gate_source = "unknown"

expected_shadow = profitability.get("expected_shadow") if isinstance(profitability.get("expected_shadow"), dict) else {}
shadow_settled = (
    profitability.get("shadow_settled_reference")
    if isinstance(profitability.get("shadow_settled_reference"), dict)
    else {}
)
trial_balance = profitability.get("trial_balance") if isinstance(profitability.get("trial_balance"), dict) else {}
attribution = profitability.get("attribution") if isinstance(profitability.get("attribution"), dict) else {}
csv_parse_cache = (
    profitability.get("csv_parse_cache")
    if isinstance(profitability.get("csv_parse_cache"), dict)
    else {}
)
csv_cache_enabled = _parse_bool(csv_parse_cache.get("enabled"), False)
csv_cache_write_access = _parse_bool(csv_parse_cache.get("write_access"), False)
csv_cache_commit_ok = _parse_bool(csv_parse_cache.get("commit_ok"), False)
csv_cache_puts_failed = max(0, _parse_int(csv_parse_cache.get("puts_failed")) or 0)
csv_cache_puts_failed_readonly = max(0, _parse_int(csv_parse_cache.get("puts_failed_readonly")) or 0)
csv_cache_path_fallback_reason = _normalize(csv_parse_cache.get("path_fallback_reason"))
csv_cache_degraded = bool(
    (not csv_cache_enabled)
    or (not csv_cache_write_access)
    or (not csv_cache_commit_ok)
    or csv_cache_puts_failed > 0
)
model_lineage = (
    profitability.get("model_lineage")
    if isinstance(profitability.get("model_lineage"), dict)
    else (
        expected_shadow.get("model_lineage")
        if isinstance(expected_shadow.get("model_lineage"), dict)
        else {}
    )
)
model_plan_versions = (
    model_lineage.get("plan_policy_versions")
    if isinstance(model_lineage.get("plan_policy_versions"), dict)
    else {}
)
model_intent_versions = (
    model_lineage.get("intent_policy_versions")
    if isinstance(model_lineage.get("intent_policy_versions"), dict)
    else {}
)
model_lineage_warnings = model_lineage.get("warnings") if isinstance(model_lineage.get("warnings"), list) else []
model_mixed_plan = bool(model_lineage.get("mixed_plan_policy_versions"))
model_mixed_intent = bool(model_lineage.get("mixed_intent_policy_versions"))
model_version_mismatch = bool(model_lineage.get("plan_intent_policy_version_mismatch"))
model_rows_missing_plan = _parse_int(model_lineage.get("plan_rows_missing_policy_version")) or 0
model_rows_missing_intent = _parse_int(model_lineage.get("intent_rows_missing_policy_version")) or 0
model_dominant_plan_version = next(iter(model_plan_versions.keys()), "")
model_plan_version_count = len(model_plan_versions)
approval_parameter_audit = (
    profitability.get("approval_parameter_audit")
    if isinstance(profitability.get("approval_parameter_audit"), dict)
    else {}
)
approval_gate_metrics = (
    approval_parameter_audit.get("gate_metrics")
    if isinstance(approval_parameter_audit.get("gate_metrics"), dict)
    else {}
)
approval_gate_mismatch_by_gate = (
    approval_parameter_audit.get("mismatch_by_gate")
    if isinstance(approval_parameter_audit.get("mismatch_by_gate"), dict)
    else {}
)
approval_audit_status = _normalize(approval_parameter_audit.get("status")) or "unknown"
approval_audit_approved_rows = _parse_int(approval_parameter_audit.get("approved_rows")) or 0
approval_audit_mismatch_rows = _parse_int(approval_parameter_audit.get("approved_rows_with_gate_mismatch")) or 0
approval_audit_mismatch_rate = _parse_float(approval_parameter_audit.get("approved_rows_with_gate_mismatch_rate"))
approval_audit_no_evaluable_rows = _parse_int(approval_parameter_audit.get("approved_rows_with_no_evaluable_gates")) or 0
approval_audit_no_evaluable_rate = _parse_float(
    approval_parameter_audit.get("approved_rows_with_no_evaluable_gates_rate")
)
approval_audit_revalidation_conflicts = _parse_int(
    approval_parameter_audit.get("approved_rows_with_revalidation_conflict")
) or 0
approval_audit_revalidation_conflicts_rate = _parse_float(
    approval_parameter_audit.get("approved_rows_with_revalidation_conflict_rate")
)

def _gate_coverage_ratio(metric_name: str, *, basis: str = "approved_rows") -> float | None:
    metric = approval_gate_metrics.get(metric_name) if isinstance(approval_gate_metrics.get(metric_name), dict) else {}
    evaluated = _parse_int(metric.get("approved_rows_evaluated")) or 0
    if basis == "active_threshold":
        approved_with_threshold = _parse_int(metric.get("approved_rows_with_threshold")) or 0
        # Mixed schema windows (cached rows from older summaries) can miss this
        # counter; never allow an impossible denominator below evaluated rows.
        if approved_with_threshold < evaluated:
            approved_with_threshold = evaluated
        if approved_with_threshold <= 0:
            return None
        return evaluated / float(approved_with_threshold)
    if approval_audit_approved_rows <= 0:
        return None
    return evaluated / float(approval_audit_approved_rows)


def _gate_coverage(metric_name: str) -> str:
    ratio_all = _gate_coverage_ratio(metric_name, basis="approved_rows")
    ratio_active = _gate_coverage_ratio(metric_name, basis="active_threshold")
    if not isinstance(ratio_all, float) and not isinstance(ratio_active, float):
        return "n/a"
    if isinstance(ratio_all, float) and isinstance(ratio_active, float):
        return f"{ratio_all * 100.0:.1f}% all / {ratio_active * 100.0:.1f}% active"
    if isinstance(ratio_active, float):
        return f"{ratio_active * 100.0:.1f}% active"
    return f"{ratio_all * 100.0:.1f}% all"


approval_gate_coverage_metar_ratio = _gate_coverage_ratio("metar_age", basis="approved_rows")
approval_gate_coverage_expected_edge_ratio = _gate_coverage_ratio("expected_edge", basis="approved_rows")
approval_gate_coverage_probability_ratio = _gate_coverage_ratio("probability_confidence", basis="approved_rows")
approval_gate_coverage_alpha_ratio = _gate_coverage_ratio("alpha_strength", basis="approved_rows")
approval_gate_coverage_metar_active_ratio = _gate_coverage_ratio("metar_age", basis="active_threshold")
approval_gate_coverage_expected_edge_active_ratio = _gate_coverage_ratio(
    "expected_edge",
    basis="active_threshold",
)
approval_gate_coverage_probability_active_ratio = _gate_coverage_ratio(
    "probability_confidence",
    basis="active_threshold",
)
approval_gate_coverage_alpha_active_ratio = _gate_coverage_ratio("alpha_strength", basis="active_threshold")

approval_gate_coverage_line = (
    "metar "
    + _gate_coverage("metar_age")
    + " | edge "
    + _gate_coverage("expected_edge")
    + " | prob "
    + _gate_coverage("probability_confidence")
    + " | alpha "
    + _gate_coverage("alpha_strength")
)

def _gate_activity_share(metric_name: str) -> str:
    ratio_all = _gate_coverage_ratio(metric_name, basis="approved_rows")
    if not isinstance(ratio_all, float):
        return "n/a"
    return f"{ratio_all * 100.0:.1f}%"


approval_gate_activity_line = (
    "edge "
    + _gate_activity_share("expected_edge")
    + " | prob "
    + _gate_activity_share("probability_confidence")
    + " | alpha "
    + _gate_activity_share("alpha_strength")
)
gate_coverage_alert_min_approved_rows = max(
    1,
    int(float(gate_coverage_alert_min_approved_rows_input or 1000)),
)
gate_coverage_expected_edge_min = max(
    0.0,
    min(1.0, float(gate_coverage_expected_edge_min_input or 0.60)),
)
gate_coverage_probability_min = max(
    0.0,
    min(1.0, float(gate_coverage_probability_min_input or 0.60)),
)
gate_coverage_alpha_min = max(
    0.0,
    min(1.0, float(gate_coverage_alpha_min_input or 0.30)),
)
gate_coverage_alert_rows_eligible = approval_audit_approved_rows >= gate_coverage_alert_min_approved_rows
gate_coverage_thresholds = {
    "expected_edge": gate_coverage_expected_edge_min,
    "probability_confidence": gate_coverage_probability_min,
    "alpha_strength": gate_coverage_alpha_min,
}
gate_coverage_values = {
    "expected_edge": (
        approval_gate_coverage_expected_edge_active_ratio
        if isinstance(approval_gate_coverage_expected_edge_active_ratio, float)
        else approval_gate_coverage_expected_edge_ratio
    ),
    "probability_confidence": (
        approval_gate_coverage_probability_active_ratio
        if isinstance(approval_gate_coverage_probability_active_ratio, float)
        else approval_gate_coverage_probability_ratio
    ),
    "alpha_strength": (
        approval_gate_coverage_alpha_active_ratio
        if isinstance(approval_gate_coverage_alpha_active_ratio, float)
        else approval_gate_coverage_alpha_ratio
    ),
}
gate_coverage_label_map = {
    "expected_edge": "edge",
    "probability_confidence": "prob",
    "alpha_strength": "alpha",
}
gate_coverage_alert_gaps: list[dict[str, Any]] = []
for gate_key, threshold in gate_coverage_thresholds.items():
    ratio = gate_coverage_values.get(gate_key)
    if isinstance(ratio, float) and ratio < threshold:
        shortfall = threshold - ratio
        shortfall_ratio = (shortfall / threshold) if threshold > 0 else 0.0
        gate_coverage_alert_gaps.append(
            {
                "gate": gate_key,
                "label": gate_coverage_label_map.get(gate_key, gate_key),
                "ratio": ratio,
                "threshold": threshold,
                "shortfall": shortfall,
                "shortfall_ratio": shortfall_ratio,
            }
        )
gate_coverage_alert_active = bool(gate_coverage_alert_rows_eligible and gate_coverage_alert_gaps)
gate_coverage_alert_level = "none"
gate_coverage_alert_reason = ""
gate_coverage_alert_summary = ""
gate_coverage_alert_worst_shortfall_ratio = None
if gate_coverage_alert_active:
    gate_coverage_alert_reason = "approval_gate_coverage_gap"
    worst_gap = max(
        gate_coverage_alert_gaps,
        key=lambda row: float(row.get("shortfall_ratio") or 0.0),
    )
    gate_coverage_alert_worst_shortfall_ratio = _parse_float(worst_gap.get("shortfall_ratio"))
    if (
        len(gate_coverage_alert_gaps) >= 2
        or (
            isinstance(gate_coverage_alert_worst_shortfall_ratio, float)
            and gate_coverage_alert_worst_shortfall_ratio >= 0.50
        )
    ):
        gate_coverage_alert_level = "red"
    else:
        gate_coverage_alert_level = "yellow"
    gap_segments = []
    for row in gate_coverage_alert_gaps:
        ratio = _parse_float(row.get("ratio"))
        threshold = _parse_float(row.get("threshold"))
        label = _normalize(row.get("label")) or _normalize(row.get("gate"))
        if isinstance(ratio, float) and isinstance(threshold, float):
            gap_segments.append(f"{label} active {ratio*100.0:.1f}%<{threshold*100.0:.1f}%")
    gate_coverage_alert_summary = (
        ", ".join(gap_segments)
        + f" (approved {approval_audit_approved_rows:,})"
    )

approval_mismatch_top_gates = sorted(
    (
        (_normalize(key), _parse_int(value) or 0)
        for key, value in approval_gate_mismatch_by_gate.items()
        if _normalize(key)
    ),
    key=lambda item: (-int(item[1]), str(item[0])),
)
approval_mismatch_top_gates_text = ", ".join(
    f"{_humanize_reason(key)}={count:,}" for key, count in approval_mismatch_top_gates[:3]
)
if not approval_mismatch_top_gates_text:
    approval_mismatch_top_gates_text = "none"

viability = bankroll.get("viability_summary") if isinstance(bankroll.get("viability_summary"), dict) else {}
opportunity_breadth = (
    bankroll.get("opportunity_breadth")
    if isinstance(bankroll.get("opportunity_breadth"), dict)
    else {}
)
concentration = (
    bankroll.get("concentration_checks")
    if isinstance(bankroll.get("concentration_checks"), dict)
    else {}
)
concentration_metrics = (
    concentration.get("concentration_metrics")
    if isinstance(concentration.get("concentration_metrics"), dict)
    else {}
)
hit_rate_quality = (
    bankroll.get("hit_rate_quality")
    if isinstance(bankroll.get("hit_rate_quality"), dict)
    else {}
)
hit_rate_quality_unique_market_side = (
    hit_rate_quality.get("unique_market_side")
    if isinstance(hit_rate_quality.get("unique_market_side"), dict)
    else {}
)
expected_vs_shadow_settled_summary = (
    bankroll.get("expected_vs_shadow_settled")
    if isinstance(bankroll.get("expected_vs_shadow_settled"), dict)
    else {}
)
expected_vs_shadow_settled_layers = (
    expected_vs_shadow_settled_summary.get("by_aggregation_layer")
    if isinstance(expected_vs_shadow_settled_summary.get("by_aggregation_layer"), dict)
    else {}
)
expected_vs_shadow_settled_unique_market_side = (
    expected_vs_shadow_settled_layers.get("unique_market_side")
    if isinstance(expected_vs_shadow_settled_layers.get("unique_market_side"), dict)
    else {}
)
bankroll_resolved_predictions = _parse_int(opportunity_breadth.get("resolved_unique_market_sides"))
if not isinstance(bankroll_resolved_predictions, int):
    bankroll_resolved_predictions = _parse_int(expected_vs_shadow_settled_unique_market_side.get("trade_count"))
bankroll_settled_wins = _parse_int(hit_rate_quality_unique_market_side.get("wins"))
bankroll_settled_losses = _parse_int(hit_rate_quality_unique_market_side.get("losses"))
bankroll_settled_pushes = _parse_int(hit_rate_quality_unique_market_side.get("pushes"))
bankroll_prediction_win_rate = _parse_float(hit_rate_quality_unique_market_side.get("win_rate"))
bankroll_settled_profit_factor = _parse_float(hit_rate_quality_unique_market_side.get("profit_factor"))
bankroll_settled_avg_win = _parse_float(hit_rate_quality_unique_market_side.get("avg_win"))
bankroll_settled_avg_loss = _parse_float(hit_rate_quality_unique_market_side.get("avg_loss"))
bankroll_settled_expectancy_per_trade = _parse_float(
    hit_rate_quality_unique_market_side.get("expectancy_per_trade")
)
if (
    not isinstance(bankroll_prediction_win_rate, float)
    and isinstance(bankroll_settled_wins, int)
    and isinstance(bankroll_settled_losses, int)
    and isinstance(bankroll_settled_pushes, int)
):
    bankroll_resolved_denominator = (
        bankroll_settled_wins + bankroll_settled_losses + bankroll_settled_pushes
    )
    if bankroll_resolved_denominator > 0:
        bankroll_prediction_win_rate = bankroll_settled_wins / float(bankroll_resolved_denominator)
bankroll_settled_counterfactual_pnl = _parse_float(
    expected_vs_shadow_settled_summary.get("shadow_settled_pnl")
)
if not isinstance(bankroll_settled_counterfactual_pnl, float):
    bankroll_settled_counterfactual_pnl = _parse_float(
        expected_vs_shadow_settled_unique_market_side.get("shadow_settled_pnl")
    )

next_signal = (
    alpha_gap.get("likely_next_highest_impact_signal_expansion")
    if isinstance(alpha_gap.get("likely_next_highest_impact_signal_expansion"), dict)
    else {}
)
weekly_blockers = blocker_audit.get("top_blockers") if isinstance(blocker_audit.get("top_blockers"), list) else []
if not weekly_blockers:
    weekly_blockers = (
        blocker_audit.get("top_blockers_raw")
        if isinstance(blocker_audit.get("top_blockers_raw"), list)
        else []
    )
weekly_top = weekly_blockers[0] if weekly_blockers and isinstance(weekly_blockers[0], dict) else {}
weekly_top_reason_key = _normalize(weekly_top.get("reason_key"))
blocker_audit_headline = (
    blocker_audit.get("headline")
    if isinstance(blocker_audit.get("headline"), dict)
    else {}
)
settlement_refresh_plan = (
    live_status.get("settlement_refresh_plan")
    if isinstance(live_status.get("settlement_refresh_plan"), dict)
    else {}
)
current_settlement_blocked_underlyings = _parse_int(
    blocker_audit_headline.get("current_settlement_blocked_underlyings")
)
if current_settlement_blocked_underlyings is None:
    current_settlement_blocked_underlyings = _parse_int(
        settlement_refresh_plan.get("settlement_finalization_blocked_underlyings")
    )
if current_settlement_blocked_underlyings is None:
    current_settlement_blocked_underlyings = 0
current_settlement_pending_final_report = _parse_int(
    blocker_audit_headline.get("current_settlement_pending_final_report")
)
if current_settlement_pending_final_report is None:
    current_settlement_pending_final_report = _parse_int(
        settlement_refresh_plan.get("pending_final_report_count")
    )
if current_settlement_pending_final_report is None:
    current_settlement_pending_final_report = 0
current_settlement_unresolved = _parse_int(
    blocker_audit_headline.get("current_settlement_unresolved")
)
if current_settlement_unresolved is None:
    current_settlement_unresolved = (
        int(current_settlement_blocked_underlyings)
        + int(current_settlement_pending_final_report)
    )
settlement_pressure_mode = bool(settlement_refresh_plan.get("pressure_mode"))
settlement_pressure_reason = _normalize(settlement_refresh_plan.get("pressure_reason"))
settlement_pressure_active = bool(int(current_settlement_unresolved) > 0 or settlement_pressure_mode)
settlement_backlog_clear = int(current_settlement_unresolved) <= 0
display_top_blockers = top_blockers
if settlement_backlog_clear and top_blockers:
    display_top_blockers = [
        row for row in top_blockers if _normalize(row[0]).lower() != "settlement_finalization_blocked"
    ]

overall_live_readiness = (
    live_readiness.get("overall_live_readiness")
    if isinstance(live_readiness.get("overall_live_readiness"), dict)
    else {}
)
readiness_by_horizon_rows = (
    live_readiness.get("readiness_by_horizon")
    if isinstance(live_readiness.get("readiness_by_horizon"), list)
    else []
)
readiness_by_horizon: dict[str, dict[str, Any]] = {}
for row in readiness_by_horizon_rows:
    if not isinstance(row, dict):
        continue
    horizon = _normalize(row.get("horizon"))
    if horizon:
        readiness_by_horizon[horizon] = row

horizon_order = ("1d", "7d", "14d", "21d", "28d", "3mo", "6mo", "1yr")
horizon_weights: dict[str, float] = {
    "1d": 0.08,
    "7d": 0.10,
    "14d": 0.16,
    "21d": 0.16,
    "28d": 0.16,
    "3mo": 0.14,
    "6mo": 0.12,
    "1yr": 0.08,
}

def _deployment_confidence_profile(score: float) -> tuple[str, str]:
    if score >= 80.0:
        return (
            "SCALE_CANDIDATE",
            "Readiness is strong across horizons; scaled live can be considered if risk controls remain green.",
        )
    if score >= 65.0:
        return (
            "PILOT_CANDIDATE",
            "Readiness is moderate; tiny controlled live pilot may be considered while keeping strict guardrails.",
        )
    if score >= 45.0:
        return (
            "SHADOW_PLUS",
            "Progress is visible but still below live threshold; stay shadow-only and close top blockers.",
        )
    return (
        "SHADOW_ONLY",
        "Readiness is low; focus on breadth/quality improvements before risking capital.",
    )


def _selection_confidence_profile(score: float) -> tuple[str, str]:
    if score >= 85.0:
        return (
            "STRONG",
            "Selection integrity is strong; edge/probability/alpha gates are being enforced consistently.",
        )
    if score >= 70.0:
        return (
            "SOLID",
            "Selection integrity is solid; continue tuning breadth and conversion while keeping thresholds stable.",
        )
    if score >= 55.0:
        return (
            "WATCH",
            "Selection integrity is mixed; keep shadow-only and tighten weak gate or guardrail areas.",
        )
    if score >= 40.0:
        return (
            "WEAK",
            "Selection integrity is weak; improve probability/edge gate adherence before increasing throughput.",
        )
    return (
        "FRAGILE",
        "Selection integrity is fragile; prioritize gate correctness and data-quality remediation first.",
    )

def _readiness_compact(label: str) -> str:
    row = readiness_by_horizon.get(label, {})
    if not isinstance(row, dict) or not row:
        return f"{label}:n/a"
    status = _normalize(row.get("readiness_status")).lower() or "unknown"
    gates = row.get("gates") if isinstance(row.get("gates"), dict) else {}
    gate_score = _parse_float(gates.get("gate_score"))
    score_text = f"{int(round(gate_score * 100.0)):d}%" if isinstance(gate_score, float) else "n/a"
    return f"{label}:{status}/{score_text}"

readiness_line_left = " | ".join(_readiness_compact(label) for label in horizon_order[:4])
readiness_line_right = " | ".join(_readiness_compact(label) for label in horizon_order[4:])
settled_evidence_confidence_panel = " | ".join(
    _readiness_compact(label) for label in ("1d", "7d", "14d")
)
settled_evidence_primary_row = readiness_by_horizon.get("14d", {})
settled_evidence_primary_gates = (
    settled_evidence_primary_row.get("gates")
    if isinstance(settled_evidence_primary_row, dict) and isinstance(settled_evidence_primary_row.get("gates"), dict)
    else {}
)
settled_evidence_confidence_score = None
if isinstance(settled_evidence_primary_gates, dict):
    settled_gate_score = _parse_float(settled_evidence_primary_gates.get("gate_score"))
    if isinstance(settled_gate_score, float):
        settled_evidence_confidence_score = max(0.0, min(100.0, settled_gate_score * 100.0))
settled_evidence_confidence_band = None
if isinstance(settled_evidence_primary_row, dict):
    settled_status_text = _normalize(settled_evidence_primary_row.get("readiness_status"))
    if settled_status_text:
        settled_evidence_confidence_band = settled_status_text.upper()
live_recommendation = _normalize(overall_live_readiness.get("recommendation")) or _normalize(go_live_gate.get("recommendation"))
ready_small_live = bool(overall_live_readiness.get("ready_for_small_live_pilot"))
ready_scaled_live = bool(overall_live_readiness.get("ready_for_scaled_live"))
earliest_passing_horizon = _normalize(overall_live_readiness.get("earliest_passing_horizon"))
most_common_blockers = (
    overall_live_readiness.get("most_common_blockers")
    if isinstance(overall_live_readiness.get("most_common_blockers"), list)
    else []
)
top_live_readiness_blocker = (
    most_common_blockers[0]
    if most_common_blockers and isinstance(most_common_blockers[0], dict)
    else {}
)
top_live_readiness_blocker_reason = _normalize(top_live_readiness_blocker.get("reason"))
top_live_readiness_blocker_action = _normalize(top_live_readiness_blocker.get("recommended_action"))

weighted_gate_score_sum = 0.0
available_weight_sum = 0.0
for label in horizon_order:
    row = readiness_by_horizon.get(label, {})
    if not isinstance(row, dict):
        continue
    gates = row.get("gates") if isinstance(row.get("gates"), dict) else {}
    gate_score = _parse_float(gates.get("gate_score"))
    if not isinstance(gate_score, float):
        continue
    weight = float(horizon_weights.get(label, 0.0))
    if weight <= 0:
        continue
    weighted_gate_score_sum += (gate_score * weight)
    available_weight_sum += weight

horizon_coverage_ratio = (available_weight_sum / sum(horizon_weights.values())) if sum(horizon_weights.values()) > 0 else 0.0
normalized_weighted_score = (weighted_gate_score_sum / available_weight_sum) if available_weight_sum > 0 else 0.0
deployment_confidence_score_uncapped = max(0.0, min(100.0, normalized_weighted_score * horizon_coverage_ratio * 100.0))
deployment_confidence_score = deployment_confidence_score_uncapped
deployment_confidence_band, deployment_confidence_guidance = _deployment_confidence_profile(deployment_confidence_score)
deployment_confidence_cap_reason = ""
deployment_confidence_cap_applied = False
deployment_confidence_cap_value = None
deployment_confidence_cap_detail = ""

live_status_status_raw = _normalize(live_status.get("status"))
live_status_file_exists = bool(_normalize(live_status_path) and Path(live_status_path).exists())
health_status = live_status_status_raw.upper() or "UNKNOWN"
health_reasons = live_status.get("red_reasons") if health_status == "RED" else live_status.get("yellow_reasons")
if not isinstance(health_reasons, list):
    health_reasons = []
health_reasons = [str(item) for item in health_reasons if _normalize(item)]
if health_status == "UNKNOWN":
    if not live_status_file_exists:
        health_reasons.append("live_status_missing")
    elif not live_status_status_raw:
        health_reasons.append("live_status_status_missing")
    if health_reasons:
        health_status = "YELLOW"
trial_balance_cache_write_ok = _parse_bool(trial_balance.get("cache_write_ok"), True)
trial_balance_cache_write_error = _normalize(trial_balance.get("cache_write_error"))
if not trial_balance_cache_write_ok:
    health_reasons.append("trial_balance_cache_write_failed")
if csv_cache_degraded:
    health_reasons.append("profitability_csv_cache_degraded")
if csv_cache_puts_failed_readonly > 0:
    health_reasons.append("profitability_csv_cache_readonly")
if csv_cache_path_fallback_reason:
    health_reasons.append(f"profitability_csv_cache_fallback_{csv_cache_path_fallback_reason}")
if artifact_parse_error_keys:
    health_reasons.append("artifact_parse_error")
if critical_artifact_parse_error_keys:
    health_reasons.append("critical_artifact_parse_error")

intents_total = _parse_int(totals.get("intents_total")) or 0
intents_approved = _parse_int(totals.get("intents_approved")) or 0
planned_orders = _parse_int(totals.get("planned_orders_total")) or 0
approval_rate = _parse_float(rates.get("approval_rate")) or 0.0
stale_rate = _parse_float(rates.get("stale_block_rate")) or 0.0
policy_reason_total = 0
for _reason_key, _reason_value in reason_counts.items():
    _reason_count = _parse_int(_reason_value)
    if _reason_count is None or _reason_count < 0:
        continue
    policy_reason_total += _reason_count
policy_reason_approved = _parse_int(reason_counts.get("approved")) or 0
policy_reason_blocked_total = max(0, policy_reason_total - policy_reason_approved)
blocked_total_from_counts = max(0, intents_total - intents_approved)
intents_reason_totals_consistent = (
    (policy_reason_total == intents_total)
    if intents_total > 0
    else True
)
blocked_totals_consistent = (
    (policy_reason_blocked_total == blocked_total_from_counts)
    if intents_total > 0
    else True
)
dominant_blocker_reason_key = ""
dominant_blocker_count = 0
dominant_blocker_share_of_blocked = 0.0
if display_top_blockers and blocked_total_from_counts > 0:
    dominant_blocker_reason_key = _normalize(display_top_blockers[0][0])
    dominant_blocker_count = int(_parse_int(display_top_blockers[0][1]) or 0)
    dominant_blocker_share_of_blocked = (
        float(dominant_blocker_count) / float(max(1, blocked_total_from_counts))
    )
approval_rate_computed = (
    (float(intents_approved) / float(intents_total))
    if intents_total > 0
    else None
)
approval_rate_consistent = (
    abs(float(approval_rate) - float(approval_rate_computed)) <= 0.0005
    if isinstance(approval_rate_computed, float)
    else True
)
window_has_previous = _parse_bool(window_comparison.get("has_previous"), False)
window_previous_file = _normalize(window_comparison.get("previous_file"))
window_delta_intents = _parse_int(
    (
        window_comparison_metrics.get("intents_total")
        if isinstance(window_comparison_metrics.get("intents_total"), dict)
        else {}
    ).get("delta")
)
window_delta_approved = _parse_int(
    (
        window_comparison_metrics.get("intents_approved")
        if isinstance(window_comparison_metrics.get("intents_approved"), dict)
        else {}
    ).get("delta")
)
window_delta_planned = _parse_int(
    (
        window_comparison_metrics.get("planned_orders_total")
        if isinstance(window_comparison_metrics.get("planned_orders_total"), dict)
        else {}
    ).get("delta")
)
window_delta_approval_pp = _parse_float(
    (
        window_comparison_metrics.get("approval_rate")
        if isinstance(window_comparison_metrics.get("approval_rate"), dict)
        else {}
    ).get("delta_percentage_points")
)
window_delta_stale_pp = _parse_float(
    (
        window_comparison_metrics.get("stale_block_rate")
        if isinstance(window_comparison_metrics.get("stale_block_rate"), dict)
        else {}
    ).get("delta_percentage_points")
)
window_delta_resolved_market_sides = _parse_int(
    (
        window_comparison_metrics.get("resolved_unique_market_sides")
        if isinstance(window_comparison_metrics.get("resolved_unique_market_sides"), dict)
        else {}
    ).get("delta")
)
window_delta_trial_1d_pnl = _parse_float(
    (
        window_comparison_metrics.get("trial_window_1d_pnl_dollars")
        if isinstance(window_comparison_metrics.get("trial_window_1d_pnl_dollars"), dict)
        else {}
    ).get("delta")
)
window_previous_intents = _parse_int(
    (
        window_comparison_metrics.get("intents_total")
        if isinstance(window_comparison_metrics.get("intents_total"), dict)
        else {}
    ).get("previous")
)
window_previous_approval_rate = _parse_float(
    (
        window_comparison_metrics.get("approval_rate")
        if isinstance(window_comparison_metrics.get("approval_rate"), dict)
        else {}
    ).get("previous")
)
quality_drift_delta_pp_min = max(0.1, float(quality_drift_delta_pp_min_input or 3.0))
quality_drift_min_intents_per_window = max(1, int(float(quality_drift_min_intents_input or 1000)))
quality_drift_max_resolved_sides_delta = int(float(quality_drift_max_resolved_delta_input or 0))
metar_observation_stale_count = _parse_int(reason_counts.get("metar_observation_stale")) or 0
metar_freshness_boundary_quality_count = _parse_int(
    reason_counts.get("metar_freshness_boundary_quality_insufficient")
) or 0
stale_count = metar_observation_stale_count + metar_freshness_boundary_quality_count
settlement_blocked_count = _parse_int(reason_counts.get("settlement_finalization_blocked")) or 0
settlement_blocked_count_actionable = settlement_blocked_count if settlement_pressure_active else 0
overlap_no_side_count = _parse_int(reason_counts.get("no_side_interval_overlap_still_possible")) or 0
overlap_yes_side_count = _parse_int(reason_counts.get("yes_range_still_possible")) or 0
overlap_total_count = overlap_no_side_count + overlap_yes_side_count
overlap_rate = (overlap_total_count / float(intents_total)) if intents_total > 0 else 0.0
stale_rate_computed = (stale_count / float(intents_total)) if intents_total > 0 else None
stale_rate_consistent = (
    abs(float(stale_rate) - float(stale_rate_computed)) <= 0.0005
    if isinstance(stale_rate_computed, float)
    else True
)
edge_too_low_count = _parse_int(reason_counts.get("expected_edge_below_min")) or 0
edge_to_risk_blocked_count = _parse_int(reason_counts.get("edge_to_risk_ratio_below_min")) or 0
cutoff_blocked_count = _parse_int(reason_counts.get("inside_cutoff_window")) or 0
edge_gate_blocked_count = edge_too_low_count + edge_to_risk_blocked_count
edge_gate_blocked_share_of_blocked = (
    (float(edge_gate_blocked_count) / float(blocked_total_from_counts))
    if blocked_total_from_counts > 0
    else 0.0
)
edge_gate_blocked_dominant = bool(
    edge_gate_blocked_count >= max(100, int(intents_total * 0.05))
    and edge_gate_blocked_share_of_blocked >= 0.75
)
blocked_mix_pairs_raw = [
    ("edge-gates", int(edge_gate_blocked_count)),
    ("freshness", int(stale_count)),
    ("overlap", int(overlap_total_count)),
    ("cutoff", int(cutoff_blocked_count)),
    ("settlement", int(settlement_blocked_count_actionable)),
]
blocked_mix_pairs = [
    (label, count)
    for label, count in blocked_mix_pairs_raw
    if isinstance(count, int) and count > 0
]
blocked_mix_pairs.sort(key=lambda item: (-int(item[1]), str(item[0])))
blocked_mix_top_text = "none"
if blocked_mix_pairs and blocked_total_from_counts > 0:
    mix_segments = []
    for label, count in blocked_mix_pairs[:3]:
        mix_share = (float(count) / float(max(1, blocked_total_from_counts))) * 100.0
        mix_segments.append(f"{label} {count:,} ({mix_share:.1f}% blocked)")
    blocked_mix_top_text = ", ".join(mix_segments)
window_intents_total = intents_total
window_approval_rate = approval_rate
latest_guardrail_intents_total = _parse_int(intents_summary.get("intents_total")) or 0
latest_guardrail_approved = _parse_int(intents_summary.get("intents_revalidated"))
if latest_guardrail_approved is None:
    latest_guardrail_approved = _parse_int(intents_summary.get("intents_approved"))
latest_guardrail_approval_rate = (
    (float(latest_guardrail_approved) / float(latest_guardrail_intents_total))
    if latest_guardrail_intents_total > 0 and isinstance(latest_guardrail_approved, int)
    else None
)
guardrail_basis_min_ratio_to_window = max(
    0.0,
    min(1.0, float(guardrail_basis_min_ratio_to_window_input or 0.25)),
)
guardrail_basis_min_abs_intents = max(
    int(approval_rate_guardrail_min_intents),
    int(float(guardrail_basis_min_abs_intents_input or 1000)),
)
latest_guardrail_ratio_to_window = (
    (float(latest_guardrail_intents_total) / float(window_intents_total))
    if window_intents_total > 0
    else None
)
recent_guardrail_sample = _aggregate_recent_intent_summary_sample(
    window_intent_summary_files,
    max_files=guardrail_recent_summary_files,
)
recent_guardrail_intents_total = _parse_int(recent_guardrail_sample.get("intents_total")) or 0
recent_guardrail_intents_approved = _parse_int(recent_guardrail_sample.get("intents_approved")) or 0
recent_guardrail_approval_rate = _parse_float(recent_guardrail_sample.get("approval_rate"))
recent_guardrail_files_used_count = _parse_int(recent_guardrail_sample.get("files_used_count")) or 0
recent_guardrail_sample_eligible = bool(
    recent_guardrail_files_used_count >= 3
    and recent_guardrail_intents_total >= int(approval_rate_guardrail_min_intents)
    and recent_guardrail_intents_total >= int(guardrail_basis_min_abs_intents)
    and isinstance(recent_guardrail_approval_rate, float)
)
latest_guardrail_sample_eligible = bool(
    latest_guardrail_intents_total >= int(approval_rate_guardrail_min_intents)
    and latest_guardrail_intents_total >= int(guardrail_basis_min_abs_intents)
    and (
        not isinstance(latest_guardrail_ratio_to_window, float)
        or latest_guardrail_ratio_to_window >= float(guardrail_basis_min_ratio_to_window)
    )
    and isinstance(latest_guardrail_approval_rate, float)
)
guardrail_basis = "window"
guardrail_basis_reason = "window_default"
guardrail_intents_total = int(window_intents_total)
guardrail_approval_rate = float(window_approval_rate)
if latest_guardrail_sample_eligible:
    guardrail_basis = "latest_intents_summary"
    guardrail_basis_reason = "latest_sample_eligible"
    guardrail_intents_total = int(latest_guardrail_intents_total)
    guardrail_approval_rate = float(latest_guardrail_approval_rate)
elif recent_guardrail_sample_eligible:
    guardrail_basis = "recent_intent_rollup"
    guardrail_basis_reason = "recent_rollup_eligible"
    guardrail_intents_total = int(recent_guardrail_intents_total)
    guardrail_approval_rate = float(recent_guardrail_approval_rate)
else:
    if latest_guardrail_intents_total < int(approval_rate_guardrail_min_intents):
        guardrail_basis_reason = "latest_sample_below_guardrail_min_intents"
    elif latest_guardrail_intents_total < int(guardrail_basis_min_abs_intents):
        guardrail_basis_reason = "latest_sample_below_abs_floor"
    elif isinstance(latest_guardrail_ratio_to_window, float) and (
        latest_guardrail_ratio_to_window < float(guardrail_basis_min_ratio_to_window)
    ):
        guardrail_basis_reason = "latest_sample_below_window_ratio_floor"
    elif not isinstance(latest_guardrail_approval_rate, float):
        guardrail_basis_reason = "latest_sample_missing_rate"
    elif recent_guardrail_files_used_count < 3:
        guardrail_basis_reason = "recent_rollup_insufficient_files"
    elif recent_guardrail_intents_total < int(guardrail_basis_min_abs_intents):
        guardrail_basis_reason = "recent_rollup_below_abs_floor"
    elif not isinstance(recent_guardrail_approval_rate, float):
        guardrail_basis_reason = "recent_rollup_missing_rate"
approval_guardrail_min_required_intents = max(
    int(approval_rate_guardrail_min_intents),
    int(guardrail_basis_min_abs_intents),
)
approval_guardrail_evaluated = guardrail_intents_total >= int(approval_guardrail_min_required_intents)
approval_guardrail_status = "insufficient_sample"
approval_guardrail_reason = "insufficient_intents_for_guardrail"
approval_guardrail_alert_level = "none"
if not approval_guardrail_evaluated and int(guardrail_intents_total) >= int(approval_rate_guardrail_min_intents):
    approval_guardrail_reason = "approval_guardrail_below_basis_floor"
if approval_guardrail_evaluated:
    if guardrail_approval_rate > approval_rate_guardrail_critical_high:
        approval_guardrail_status = "critical_high"
        approval_guardrail_reason = "approval_rate_critical_high"
        approval_guardrail_alert_level = "red"
    elif guardrail_approval_rate > approval_rate_guardrail_max:
        approval_guardrail_status = "above_band"
        approval_guardrail_reason = "approval_rate_above_guardrail"
        approval_guardrail_alert_level = "yellow"
    elif guardrail_approval_rate < approval_rate_guardrail_min:
        approval_guardrail_status = "below_band"
        approval_guardrail_reason = "approval_rate_below_guardrail"
        approval_guardrail_alert_level = "yellow"
    else:
        approval_guardrail_status = "within_band"
        approval_guardrail_reason = "approval_rate_within_guardrail"

current_min_settlement_confidence = _parse_float(intents_summary.get("min_settlement_confidence"))
current_min_alpha_strength_profile = _parse_float(intents_summary.get("min_alpha_strength"))
current_min_probability_confidence_profile = _parse_float(intents_summary.get("min_probability_confidence"))
current_min_expected_edge_net_profile = _parse_float(intents_summary.get("min_expected_edge_net"))
latest_sparse_hardening_applied_count = _parse_int(
    intents_summary.get("sparse_evidence_hardening_applied_count")
) or 0
latest_sparse_hardening_approved_count = _parse_int(
    intents_summary.get("sparse_evidence_hardening_approved_count")
) or 0
latest_sparse_hardening_blocked_count = _parse_int(
    intents_summary.get("sparse_evidence_hardening_blocked_count")
) or 0
latest_sparse_hardening_blocked_probability_below_min_count = _parse_int(
    intents_summary.get("sparse_evidence_hardening_blocked_probability_below_min_count")
) or 0
latest_sparse_hardening_blocked_expected_edge_below_min_count = _parse_int(
    intents_summary.get("sparse_evidence_hardening_blocked_expected_edge_below_min_count")
) or 0
latest_sparse_hardening_blocked_edge_to_risk_below_min_count = _parse_int(
    intents_summary.get("sparse_evidence_hardening_blocked_edge_to_risk_below_min_count")
) or 0
latest_sparse_hardening_applied_rate = _parse_float(
    intents_summary.get("sparse_evidence_hardening_applied_rate")
)
if not isinstance(latest_sparse_hardening_applied_rate, float) and latest_guardrail_intents_total > 0:
    latest_sparse_hardening_applied_rate = latest_sparse_hardening_applied_count / float(latest_guardrail_intents_total)
latest_sparse_hardening_probability_raise_avg = _parse_float(
    intents_summary.get("sparse_evidence_probability_raise_avg")
)
latest_sparse_hardening_probability_raise_max = _parse_float(
    intents_summary.get("sparse_evidence_probability_raise_max")
)
latest_sparse_hardening_expected_edge_raise_avg = _parse_float(
    intents_summary.get("sparse_evidence_expected_edge_raise_avg")
)
latest_sparse_hardening_expected_edge_raise_max = _parse_float(
    intents_summary.get("sparse_evidence_expected_edge_raise_max")
)
latest_sparse_hardening_support_score_avg = _parse_float(
    intents_summary.get("sparse_evidence_support_score_avg")
)
latest_sparse_hardening_volatility_penalty_avg = _parse_float(
    intents_summary.get("sparse_evidence_volatility_penalty_avg")
)
rolling_sparse_hardening_files_used_count = _parse_int(
    window_sparse_hardening_rollup.get("files_used_count")
) or 0
rolling_sparse_hardening_applied_count = _parse_int(
    window_sparse_hardening_rollup.get("applied_count")
) or 0
sparse_hardening_use_rolling = (
    rolling_sparse_hardening_files_used_count > 0
    and rolling_sparse_hardening_applied_count > 0
)
sparse_hardening_basis = "rolling_intent_summaries" if sparse_hardening_use_rolling else "latest_intents_summary"
sparse_hardening_basis_label = (
    "rolling window intents"
    if sparse_hardening_use_rolling
    else "latest intents summary"
)
sparse_hardening_sample_intents_total = (
    _parse_int(window_sparse_hardening_rollup.get("intents_total")) or 0
    if sparse_hardening_use_rolling
    else int(latest_guardrail_intents_total)
)
sparse_hardening_applied_count = (
    rolling_sparse_hardening_applied_count
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_applied_count
)
sparse_hardening_approved_count = (
    _parse_int(window_sparse_hardening_rollup.get("approved_count")) or 0
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_approved_count
)
sparse_hardening_blocked_count = (
    _parse_int(window_sparse_hardening_rollup.get("blocked_count")) or 0
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_blocked_count
)
sparse_hardening_blocked_probability_below_min_count = (
    _parse_int(window_sparse_hardening_rollup.get("blocked_probability_below_min_count")) or 0
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_blocked_probability_below_min_count
)
sparse_hardening_blocked_expected_edge_below_min_count = (
    _parse_int(window_sparse_hardening_rollup.get("blocked_expected_edge_below_min_count")) or 0
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_blocked_expected_edge_below_min_count
)
sparse_hardening_blocked_edge_to_risk_below_min_count = (
    _parse_int(window_sparse_hardening_rollup.get("blocked_edge_to_risk_below_min_count")) or 0
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_blocked_edge_to_risk_below_min_count
)
sparse_hardening_applied_rate = (
    _parse_float(window_sparse_hardening_rollup.get("applied_rate"))
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_applied_rate
)
sparse_hardening_probability_raise_avg = (
    _parse_float(window_sparse_hardening_rollup.get("probability_raise_avg"))
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_probability_raise_avg
)
sparse_hardening_probability_raise_max = (
    _parse_float(window_sparse_hardening_rollup.get("probability_raise_max"))
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_probability_raise_max
)
sparse_hardening_expected_edge_raise_avg = (
    _parse_float(window_sparse_hardening_rollup.get("expected_edge_raise_avg"))
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_expected_edge_raise_avg
)
sparse_hardening_expected_edge_raise_max = (
    _parse_float(window_sparse_hardening_rollup.get("expected_edge_raise_max"))
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_expected_edge_raise_max
)
sparse_hardening_support_score_avg = (
    _parse_float(window_sparse_hardening_rollup.get("support_score_avg"))
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_support_score_avg
)
sparse_hardening_volatility_penalty_avg = (
    _parse_float(window_sparse_hardening_rollup.get("volatility_penalty_avg"))
    if sparse_hardening_use_rolling
    else latest_sparse_hardening_volatility_penalty_avg
)
sparse_hardening_blocked_share_of_blocked = (
    sparse_hardening_blocked_count / float(blocked_total_from_counts)
    if blocked_total_from_counts > 0
    else None
)
sparse_hardening_blocked_share_of_hardened = (
    sparse_hardening_blocked_count / float(sparse_hardening_applied_count)
    if sparse_hardening_applied_count > 0
    else None
)
sparse_hardening_sample_share_of_window_raw = (
    sparse_hardening_sample_intents_total / float(window_intents_total)
    if window_intents_total > 0 and sparse_hardening_sample_intents_total > 0
    else None
)
sparse_hardening_sample_share_of_window = (
    min(1.0, sparse_hardening_sample_share_of_window_raw)
    if isinstance(sparse_hardening_sample_share_of_window_raw, float)
    else None
)
sparse_hardening_probability_block_share = (
    sparse_hardening_blocked_probability_below_min_count / float(sparse_hardening_blocked_count)
    if sparse_hardening_blocked_count > 0
    else None
)
sparse_hardening_expected_edge_block_share = (
    sparse_hardening_blocked_expected_edge_below_min_count / float(sparse_hardening_blocked_count)
    if sparse_hardening_blocked_count > 0
    else None
)
sparse_hardening_edge_to_risk_block_share = (
    sparse_hardening_blocked_edge_to_risk_below_min_count / float(sparse_hardening_blocked_count)
    if sparse_hardening_blocked_count > 0
    else None
)
intent_quality_rows = list(window_intent_quality_rows)
intent_quality_source = "window_intents_summaries"
if not intent_quality_rows:
    intent_quality_rows = _load_intent_quality_rows(intents_summary)
    intent_quality_source = "latest_intents_summary"
intent_quality_source_summary_files_count = int(len(window_intent_quality_summary_files_used))
intent_quality_source_csv_files_count = int(len(window_intent_quality_csv_files_used))
intent_quality_source_summary_files_preview = window_intent_quality_summary_files_used[-10:]
intent_quality_source_csv_files_preview = window_intent_quality_csv_files_used[-10:]
approval_guardrail_recommendation = _build_approval_guardrail_recommendation(
    intent_quality_rows,
    guard_min=approval_rate_guardrail_min,
    guard_max=approval_rate_guardrail_max,
    current_approval_rate=guardrail_approval_rate,
    min_settlement_confidence=current_min_settlement_confidence,
    min_alpha_strength=current_min_alpha_strength_profile,
    min_probability_confidence=current_min_probability_confidence_profile,
    min_expected_edge_net=current_min_expected_edge_net_profile,
)
strict_guardrail_max = max(
    approval_rate_guardrail_min + 0.005,
    min(
        approval_rate_guardrail_max - 0.02,
        approval_rate_guardrail_max * 0.85,
    ),
)
approval_guardrail_recommendation_strict: dict[str, Any] = {}
if strict_guardrail_max < approval_rate_guardrail_max:
    approval_guardrail_recommendation_strict = _build_approval_guardrail_recommendation(
        intent_quality_rows,
        guard_min=approval_rate_guardrail_min,
        guard_max=strict_guardrail_max,
        current_approval_rate=guardrail_approval_rate,
        min_settlement_confidence=current_min_settlement_confidence,
        min_alpha_strength=current_min_alpha_strength_profile,
        min_probability_confidence=current_min_probability_confidence_profile,
        min_expected_edge_net=current_min_expected_edge_net_profile,
    )
approval_guardrail_recommendation_for_auto = approval_guardrail_recommendation
auto_apply_recommendation_mode = "standard"
quality_risk_projected_streak_for_auto = 0
quality_risk_persistent_for_auto = False
runtime_dir = window_summary_path.parent.parent / "runtime"
auto_apply_enabled = _parse_bool(auto_apply_enabled_input, False)
auto_apply_streak_required = max(
    1,
    int(float(auto_apply_streak_required_input or 2)),
)
auto_apply_state_path = Path(
    auto_apply_state_file_input
    or str(runtime_dir / "approval_guardrail_auto_apply_state.json")
)
auto_apply_profile_path = Path(
    auto_apply_profile_file_input
    or str(runtime_dir / "approval_gate_profile_auto.json")
)
quality_risk_state_path = Path(
    quality_risk_state_file_input
    or str(runtime_dir / "approval_quality_risk_state.json")
)
auto_apply_state_previous = _load_json(str(auto_apply_state_path))
auto_apply_previous_streak = _parse_int(auto_apply_state_previous.get("breach_streak")) or 0
auto_apply_previous_clear_streak = _parse_int(auto_apply_state_previous.get("clear_streak")) or 0
auto_apply_previous_gate_breach_streak = _parse_int(
    auto_apply_state_previous.get("gate_coverage_breach_streak")
) or 0
auto_apply_previous_zero_approved_streak = _parse_int(
    auto_apply_state_previous.get("zero_approved_streak")
) or 0
auto_apply_previous_quality_risk_streak = _parse_int(
    auto_apply_state_previous.get("quality_risk_streak")
) or 0
auto_apply_previous_stability_streak = _parse_int(
    auto_apply_state_previous.get("stability_streak")
) or 0
quality_risk_state_previous = _load_json(str(quality_risk_state_path))
quality_risk_previous_streak = _parse_int(quality_risk_state_previous.get("streak")) or 0
quality_risk_previous_red_streak = _parse_int(quality_risk_state_previous.get("red_streak")) or 0
auto_apply_min_rows = max(
    int(guardrail_basis_min_abs_intents),
    int(float(auto_apply_min_rows_input or guardrail_basis_min_abs_intents)),
)
auto_apply_release_enabled = _parse_bool(auto_apply_release_enabled_input, True)
auto_apply_release_streak_required = max(
    1,
    int(float(auto_apply_release_streak_required_input or 3)),
)
auto_apply_zero_approved_streak_required = max(
    1,
    int(float(auto_apply_zero_approved_streak_required_input or 2)),
)
auto_apply_zero_approved_min_intents = max(
    int(guardrail_basis_min_abs_intents),
    int(float(auto_apply_zero_approved_min_intents_input or guardrail_basis_min_abs_intents)),
)
auto_apply_rows_available = int(len(intent_quality_rows))
approval_guardrail_breach_active = bool(
    approval_guardrail_evaluated and approval_guardrail_status in {"above_band", "critical_high", "below_band"}
)
gate_coverage_auto_apply_enabled = _parse_bool(gate_coverage_auto_apply_enabled_input, True)
gate_coverage_auto_apply_streak_required = max(
    1,
    int(float(gate_coverage_auto_apply_streak_required_input or 2)),
)
gate_coverage_auto_apply_min_rows = max(
    int(guardrail_basis_min_abs_intents),
    int(float(gate_coverage_auto_apply_min_rows_input or gate_coverage_alert_min_approved_rows)),
)
level_rank = {"none": 0, "yellow": 1, "red": 2}
gate_coverage_auto_apply_min_level = _normalize(gate_coverage_auto_apply_min_level_input).lower() or "red"
if gate_coverage_auto_apply_min_level not in level_rank:
    gate_coverage_auto_apply_min_level = "red"
gate_coverage_auto_apply_breach_active = bool(
    gate_coverage_auto_apply_enabled
    and gate_coverage_alert_active
    and approval_audit_approved_rows >= int(gate_coverage_auto_apply_min_rows)
    and level_rank.get(gate_coverage_alert_level, 0) >= level_rank.get(gate_coverage_auto_apply_min_level, 2)
)
quality_risk_signal_active_for_auto_apply = bool(
    approval_guardrail_status in {"above_band", "critical_high"}
    and (
        edge_gate_blocked_dominant
        or gate_coverage_alert_active
        or (
            isinstance(approval_audit_mismatch_rate, float)
            and approval_audit_mismatch_rate >= 0.02
            and approval_audit_mismatch_rows >= max(25, int(approval_audit_approved_rows * 0.005))
        )
        or (
            isinstance(approval_audit_revalidation_conflicts_rate, float)
            and approval_audit_revalidation_conflicts_rate >= 0.01
            and approval_audit_revalidation_conflicts >= max(10, int(approval_audit_approved_rows * 0.002))
        )
    )
)
quality_risk_projected_streak_for_auto = (
    max(1, int(quality_risk_previous_streak) + 1)
    if quality_risk_signal_active_for_auto_apply
    else 0
)
quality_risk_persistent_for_auto = bool(
    quality_risk_projected_streak_for_auto >= int(quality_risk_streak_red_required)
)
strict_recommendation_ready = bool(
    isinstance(approval_guardrail_recommendation_strict, dict)
    and approval_guardrail_recommendation_strict.get("status") == "ready"
    and approval_guardrail_recommendation_strict.get("fit_within_guardrail")
    and isinstance(approval_guardrail_recommendation_strict.get("recommended_profile"), dict)
)
standard_recommendation_ready = bool(
    isinstance(approval_guardrail_recommendation, dict)
    and approval_guardrail_recommendation.get("status") == "ready"
    and approval_guardrail_recommendation.get("fit_within_guardrail")
    and isinstance(approval_guardrail_recommendation.get("recommended_profile"), dict)
)
if quality_risk_persistent_for_auto and strict_recommendation_ready:
    approval_guardrail_recommendation_for_auto = approval_guardrail_recommendation_strict
    auto_apply_recommendation_mode = "strict"
else:
    approval_guardrail_recommendation_for_auto = approval_guardrail_recommendation
    auto_apply_recommendation_mode = "standard"
auto_apply_recommendation_ready = bool(
    isinstance(approval_guardrail_recommendation_for_auto, dict)
    and approval_guardrail_recommendation_for_auto.get("status") == "ready"
    and approval_guardrail_recommendation_for_auto.get("fit_within_guardrail")
    and isinstance(approval_guardrail_recommendation_for_auto.get("recommended_profile"), dict)
)
auto_apply_recommendation_fit_for_streak = bool(auto_apply_recommendation_ready)
approval_trigger_rows_satisfied = auto_apply_rows_available >= int(auto_apply_min_rows)
gate_trigger_rows_satisfied = auto_apply_rows_available >= int(gate_coverage_auto_apply_min_rows)
auto_apply_stability_window_comparable = bool(
    window_has_previous
    and isinstance(window_delta_approval_pp, float)
    and isinstance(window_delta_stale_pp, float)
)
auto_apply_stability_within_approval = bool(
    auto_apply_stability_window_comparable
    and isinstance(window_delta_approval_pp, float)
    and abs(float(window_delta_approval_pp)) <= float(auto_apply_stability_max_approval_delta_pp)
)
auto_apply_stability_within_stale = bool(
    auto_apply_stability_window_comparable
    and isinstance(window_delta_stale_pp, float)
    and abs(float(window_delta_stale_pp)) <= float(auto_apply_stability_max_stale_delta_pp)
)
if auto_apply_stability_enabled:
    auto_apply_stability_window_stable = bool(
        auto_apply_stability_window_comparable
        and auto_apply_stability_within_approval
        and auto_apply_stability_within_stale
    )
    if not auto_apply_stability_window_comparable:
        auto_apply_stability_reason = "no_comparable_previous_window"
    elif not auto_apply_stability_within_approval and not auto_apply_stability_within_stale:
        auto_apply_stability_reason = "approval_and_stale_delta_out_of_band"
    elif not auto_apply_stability_within_approval:
        auto_apply_stability_reason = "approval_delta_out_of_band"
    elif not auto_apply_stability_within_stale:
        auto_apply_stability_reason = "stale_delta_out_of_band"
    else:
        auto_apply_stability_reason = "stable_window"
else:
    auto_apply_stability_window_stable = True
    auto_apply_stability_reason = "stability_gate_disabled"
auto_apply_stability_streak = (
    max(1, int(auto_apply_previous_stability_streak) + 1)
    if auto_apply_stability_window_stable
    else 0
)
auto_apply_stability_ready = bool(
    (not auto_apply_stability_enabled)
    or (auto_apply_stability_streak >= int(auto_apply_stability_windows_required))
)
auto_apply_breach_streak = 0
if (
    approval_guardrail_breach_active
    and auto_apply_recommendation_fit_for_streak
    and approval_trigger_rows_satisfied
):
    auto_apply_breach_streak = max(1, int(auto_apply_previous_streak) + 1)
auto_apply_gate_breach_streak = 0
if (
    gate_coverage_auto_apply_breach_active
    and auto_apply_recommendation_fit_for_streak
    and gate_trigger_rows_satisfied
):
    auto_apply_gate_breach_streak = max(1, int(auto_apply_previous_gate_breach_streak) + 1)
auto_apply_quality_risk_streak = 0
if (
    quality_risk_signal_active_for_auto_apply
    and auto_apply_recommendation_fit_for_streak
    and approval_trigger_rows_satisfied
):
    auto_apply_quality_risk_streak = max(1, int(auto_apply_previous_quality_risk_streak) + 1)
auto_apply_clear_streak = int(auto_apply_previous_clear_streak)
if approval_guardrail_breach_active or gate_coverage_auto_apply_breach_active:
    auto_apply_clear_streak = 0
elif approval_guardrail_evaluated:
    auto_apply_clear_streak = max(1, int(auto_apply_previous_clear_streak) + 1)
auto_apply_trigger = "none"
auto_apply_trigger_streak_current = 0
auto_apply_trigger_streak_required = int(auto_apply_streak_required)
if gate_coverage_auto_apply_breach_active:
    auto_apply_trigger = "gate_coverage_alert"
    auto_apply_trigger_streak_current = int(auto_apply_gate_breach_streak)
    auto_apply_trigger_streak_required = int(gate_coverage_auto_apply_streak_required)
elif quality_risk_signal_active_for_auto_apply:
    auto_apply_trigger = "quality_risk"
    auto_apply_trigger_streak_current = int(auto_apply_quality_risk_streak)
    auto_apply_trigger_streak_required = int(quality_risk_auto_apply_streak_required)
elif approval_guardrail_breach_active:
    auto_apply_trigger = "approval_guardrail"
    auto_apply_trigger_streak_current = int(auto_apply_breach_streak)
    auto_apply_trigger_streak_required = int(auto_apply_streak_required)
auto_apply_required_rows_for_trigger = int(auto_apply_min_rows)
if auto_apply_trigger == "gate_coverage_alert":
    auto_apply_required_rows_for_trigger = int(gate_coverage_auto_apply_min_rows)
auto_apply_min_rows_satisfied = auto_apply_rows_available >= int(auto_apply_required_rows_for_trigger)
latest_approved_for_zero_guard = (
    int(latest_guardrail_approved)
    if isinstance(latest_guardrail_approved, int)
    else 0
)
auto_apply_zero_approved_candidate = bool(
    latest_guardrail_sample_eligible
    and latest_guardrail_intents_total >= int(auto_apply_zero_approved_min_intents)
    and latest_approved_for_zero_guard == 0
)
auto_apply_zero_approved_streak = (
    max(1, int(auto_apply_previous_zero_approved_streak) + 1)
    if auto_apply_zero_approved_candidate
    else 0
)
auto_apply_recommendation_fit = bool(auto_apply_recommendation_ready and auto_apply_min_rows_satisfied)
auto_apply_candidate_ready = bool(
    auto_apply_enabled
    and auto_apply_recommendation_fit
    and auto_apply_stability_ready
    and auto_apply_trigger in {"approval_guardrail", "gate_coverage_alert", "quality_risk"}
)
auto_apply_recommendation_mode_effective = (
    auto_apply_recommendation_mode if auto_apply_enabled else "disabled"
)
auto_apply_should_apply = bool(
    auto_apply_candidate_ready and auto_apply_trigger_streak_current >= int(auto_apply_trigger_streak_required)
)
auto_apply_applied = False
auto_apply_released = False
auto_apply_profile_write_ok = True
auto_apply_profile_write_attempted = False
auto_apply_apply_reason = "not_applicable"
auto_applied_profile: dict[str, Any] | None = None
if not auto_apply_enabled:
    auto_apply_apply_reason = "auto_apply_disabled"
elif gate_coverage_alert_active and not gate_coverage_auto_apply_enabled and not approval_guardrail_breach_active:
    auto_apply_apply_reason = "gate_coverage_auto_apply_disabled"
elif auto_apply_trigger == "none" and not approval_guardrail_evaluated:
    if approval_guardrail_reason == "approval_guardrail_below_basis_floor":
        auto_apply_apply_reason = "guardrail_sample_below_basis_floor"
    else:
        auto_apply_apply_reason = "guardrail_insufficient_sample"
elif auto_apply_trigger == "none" and approval_guardrail_evaluated:
    if gate_coverage_alert_active:
        auto_apply_apply_reason = "gate_coverage_trigger_not_eligible"
    else:
        auto_apply_apply_reason = "within_guardrail_band"
elif auto_apply_trigger in {"approval_guardrail", "gate_coverage_alert", "quality_risk"} and not auto_apply_min_rows_satisfied:
    auto_apply_apply_reason = "insufficient_intent_quality_rows"
elif auto_apply_trigger in {"approval_guardrail", "gate_coverage_alert", "quality_risk"} and not auto_apply_recommendation_ready:
    auto_apply_apply_reason = "recommendation_not_fit"
elif (
    auto_apply_trigger in {"approval_guardrail", "gate_coverage_alert", "quality_risk"}
    and auto_apply_stability_enabled
    and not auto_apply_stability_ready
):
    auto_apply_apply_reason = "waiting_for_stability"
elif auto_apply_candidate_ready and auto_apply_trigger_streak_current < int(auto_apply_trigger_streak_required):
    auto_apply_apply_reason = "waiting_for_streak"
if auto_apply_should_apply:
    recommendation_profile = (
        approval_guardrail_recommendation_for_auto.get("recommended_profile")
        if isinstance(approval_guardrail_recommendation_for_auto.get("recommended_profile"), dict)
        else {}
    )
    if recommendation_profile:
        existing_auto_profile = _load_json(str(auto_apply_profile_path))
        existing_signature = _approval_profile_signature(existing_auto_profile)
        recommended_signature = _approval_profile_signature(recommendation_profile)
        auto_applied_profile = {
            "status": "ready",
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source": "alpha_summary_approval_guardrail_auto_apply",
            "source_summary_file": str(latest_path),
            "trigger": auto_apply_trigger,
            "recommendation_mode": auto_apply_recommendation_mode,
            "recommendation_mode_effective": auto_apply_recommendation_mode_effective,
            "quality_risk_projected_streak_for_auto": int(quality_risk_projected_streak_for_auto),
            "quality_risk_persistent_for_auto": bool(quality_risk_persistent_for_auto),
            "strict_guardrail_max": round(float(strict_guardrail_max), 6),
            "guardrail_status": approval_guardrail_status,
            "guardrail_reason": approval_guardrail_reason,
            "breach_streak": int(auto_apply_breach_streak),
            "gate_coverage_breach_streak": int(auto_apply_gate_breach_streak),
            "streak_required": int(auto_apply_trigger_streak_required),
            "gate_coverage_alert_level": gate_coverage_alert_level,
            "gate_coverage_alert_summary": gate_coverage_alert_summary or None,
            "projected_approval_rate": _parse_float(recommendation_profile.get("projected_approval_rate")),
            "projected_approved_count": _parse_int(recommendation_profile.get("projected_approved_count")),
            "min_settlement_confidence": _parse_float(recommendation_profile.get("min_settlement_confidence")),
            "min_alpha_strength": _parse_float(recommendation_profile.get("min_alpha_strength")),
            "min_probability_confidence": _parse_float(
                recommendation_profile.get("min_probability_confidence")
            ),
            "min_expected_edge_net": _parse_float(recommendation_profile.get("min_expected_edge_net")),
        }
        if existing_signature == recommended_signature and _normalize(existing_auto_profile.get("status")) == "ready":
            auto_apply_applied = False
            auto_apply_apply_reason = "already_applied_same_profile"
        else:
            auto_apply_profile_write_attempted = True
            auto_apply_profile_write_ok = _safe_write_json(auto_apply_profile_path, auto_applied_profile)
            if auto_apply_profile_write_ok:
                auto_apply_applied = True
                auto_apply_apply_reason = "applied_profile"
            else:
                auto_apply_applied = False
                auto_apply_apply_reason = "apply_profile_write_failed"
if (
    auto_apply_enabled
    and auto_apply_trigger == "gate_coverage_alert"
    and not auto_apply_min_rows_satisfied
    and auto_apply_profile_path.exists()
):
    existing_auto_profile_for_release = _load_json(str(auto_apply_profile_path))
    existing_trigger = _normalize(existing_auto_profile_for_release.get("trigger"))
    existing_source = _normalize(existing_auto_profile_for_release.get("source"))
    if existing_trigger == "gate_coverage_alert" and existing_source == "alpha_summary_approval_guardrail_auto_apply":
        try:
            auto_apply_profile_path.unlink()
            auto_apply_released = True
            auto_apply_apply_reason = "released_profile_insufficient_rows_for_trigger"
        except Exception:
            auto_apply_released = False
if (
    auto_apply_release_enabled
    and auto_apply_enabled
    and auto_apply_profile_path.exists()
    and auto_apply_zero_approved_streak >= int(auto_apply_zero_approved_streak_required)
):
    existing_auto_profile_for_zero_release = _load_json(str(auto_apply_profile_path))
    existing_source = _normalize(existing_auto_profile_for_zero_release.get("source"))
    if existing_source == "alpha_summary_approval_guardrail_auto_apply":
        try:
            auto_apply_profile_path.unlink()
            auto_apply_released = True
            auto_apply_apply_reason = "released_profile_zero_approved_streak"
            auto_apply_breach_streak = 0
            auto_apply_gate_breach_streak = 0
            auto_apply_trigger_streak_current = 0
        except Exception:
            auto_apply_released = False
if (
    auto_apply_release_enabled
    and auto_apply_enabled
    and not approval_guardrail_breach_active
    and not gate_coverage_auto_apply_breach_active
    and approval_guardrail_evaluated
    and auto_apply_profile_path.exists()
    and auto_apply_clear_streak >= int(auto_apply_release_streak_required)
):
    try:
        auto_apply_profile_path.unlink()
        auto_apply_released = True
        auto_apply_apply_reason = "released_profile_clear_streak"
    except Exception:
        auto_apply_released = False
        auto_apply_apply_reason = "release_profile_unlink_failed"

auto_apply_state_next = {
    "status": "ready",
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "breach_streak": int(auto_apply_breach_streak),
    "gate_coverage_breach_streak": int(auto_apply_gate_breach_streak),
    "quality_risk_streak": int(auto_apply_quality_risk_streak),
    "quality_risk_signal_active": bool(quality_risk_signal_active_for_auto_apply),
    "quality_risk_auto_apply_streak_required": int(quality_risk_auto_apply_streak_required),
    "stability_enabled": bool(auto_apply_stability_enabled),
    "stability_windows_required": int(auto_apply_stability_windows_required),
    "stability_streak": int(auto_apply_stability_streak),
    "stability_ready": bool(auto_apply_stability_ready),
    "stability_window_comparable": bool(auto_apply_stability_window_comparable),
    "stability_window_stable": bool(auto_apply_stability_window_stable),
    "stability_reason": auto_apply_stability_reason,
    "stability_max_approval_delta_pp": round(float(auto_apply_stability_max_approval_delta_pp), 6),
    "stability_max_stale_delta_pp": round(float(auto_apply_stability_max_stale_delta_pp), 6),
    "stability_delta_approval_pp": (
        round(float(window_delta_approval_pp), 6)
        if isinstance(window_delta_approval_pp, float)
        else None
    ),
    "stability_delta_stale_pp": (
        round(float(window_delta_stale_pp), 6)
        if isinstance(window_delta_stale_pp, float)
        else None
    ),
    "clear_streak": int(auto_apply_clear_streak),
    "zero_approved_streak": int(auto_apply_zero_approved_streak),
    "zero_approved_streak_required": int(auto_apply_zero_approved_streak_required),
    "zero_approved_min_intents": int(auto_apply_zero_approved_min_intents),
    "zero_approved_candidate": bool(auto_apply_zero_approved_candidate),
    "latest_summary_intents_total": int(latest_guardrail_intents_total),
    "latest_summary_intents_approved": int(latest_approved_for_zero_guard),
    "latest_summary_sample_eligible": bool(latest_guardrail_sample_eligible),
    "streak_required": int(auto_apply_streak_required),
    "trigger": auto_apply_trigger,
    "trigger_streak_current": int(auto_apply_trigger_streak_current),
    "trigger_streak_required": int(auto_apply_trigger_streak_required),
    "rows_available": int(auto_apply_rows_available),
    "required_rows_for_trigger": int(auto_apply_required_rows_for_trigger),
    "approval_trigger_rows_satisfied": bool(approval_trigger_rows_satisfied),
    "gate_trigger_rows_satisfied": bool(gate_trigger_rows_satisfied),
    "min_rows": int(auto_apply_min_rows),
    "min_rows_satisfied": bool(auto_apply_min_rows_satisfied),
    "guardrail_basis_min_abs_intents": int(guardrail_basis_min_abs_intents),
    "guardrail_min_required_intents": int(approval_guardrail_min_required_intents),
    "recommendation_ready": bool(auto_apply_recommendation_ready),
    "auto_apply_enabled": bool(auto_apply_enabled),
    "release_enabled": bool(auto_apply_release_enabled),
    "release_streak_required": int(auto_apply_release_streak_required),
    "breach_active": bool(approval_guardrail_breach_active),
    "gate_coverage_auto_apply_enabled": bool(gate_coverage_auto_apply_enabled),
    "gate_coverage_breach_active": bool(gate_coverage_auto_apply_breach_active),
    "gate_coverage_auto_apply_streak_required": int(gate_coverage_auto_apply_streak_required),
    "gate_coverage_auto_apply_min_rows": int(gate_coverage_auto_apply_min_rows),
    "gate_coverage_auto_apply_min_level": gate_coverage_auto_apply_min_level,
    "candidate_ready": bool(auto_apply_candidate_ready),
    "should_apply": bool(auto_apply_should_apply),
    "recommendation_fit": bool(auto_apply_recommendation_fit),
    "apply_reason": auto_apply_apply_reason,
    "applied_in_this_run": bool(auto_apply_applied),
    "released_in_this_run": bool(auto_apply_released),
    "guardrail_status": approval_guardrail_status,
    "guardrail_reason": approval_guardrail_reason,
    "guardrail_basis": guardrail_basis,
    "guardrail_approval_rate": round(float(guardrail_approval_rate), 6),
    "guardrail_intents_total": int(guardrail_intents_total),
    "profile_path": str(auto_apply_profile_path),
    "profile_write_attempted": bool(auto_apply_profile_write_attempted),
    "profile_write_ok": bool(auto_apply_profile_write_ok),
}
auto_apply_state_write_ok = _safe_write_json(auto_apply_state_path, auto_apply_state_next)

# Ensure report messaging reflects the effective runtime profile, including
# profiles applied during this summary run.
effective_auto_apply_profile: dict[str, Any] | None = None
if auto_apply_enabled and auto_apply_profile_path.exists() and not auto_apply_released:
    loaded_auto_profile = _load_json(str(auto_apply_profile_path))
    if isinstance(loaded_auto_profile, dict):
        if (
            _normalize(loaded_auto_profile.get("status")) == "ready"
            and _normalize(loaded_auto_profile.get("source"))
            == "alpha_summary_approval_guardrail_auto_apply"
        ):
            effective_auto_apply_profile = loaded_auto_profile
if auto_apply_applied and isinstance(auto_applied_profile, dict):
    effective_auto_apply_profile = dict(auto_applied_profile)
if isinstance(effective_auto_apply_profile, dict):
    quality_gate_source = "auto_profile"
    quality_gate_auto_applied = True
    quality_gate_profile_data_source = "auto_profile"
    detail_source = _normalize(effective_auto_apply_profile.get("source"))
    if detail_source:
        quality_gate_detail = detail_source
    profile_settlement = _parse_float(effective_auto_apply_profile.get("min_settlement_confidence"))
    if isinstance(profile_settlement, float):
        quality_gate_min_settlement = profile_settlement
    profile_alpha = _parse_float(effective_auto_apply_profile.get("min_alpha_strength"))
    if isinstance(profile_alpha, float):
        quality_gate_min_alpha = profile_alpha
    profile_probability = _parse_float(effective_auto_apply_profile.get("min_probability_confidence"))
    if isinstance(profile_probability, float):
        quality_gate_min_probability = profile_probability
    profile_edge = _parse_float(effective_auto_apply_profile.get("min_expected_edge_net"))
    if isinstance(profile_edge, float):
        quality_gate_min_edge = profile_edge
elif auto_apply_enabled and auto_apply_released and not auto_apply_profile_path.exists():
    # Keep reporting aligned with runtime when auto profile was explicitly released.
    quality_gate_source = "manual_thresholds"
    quality_gate_auto_applied = False
    if not quality_gate_detail:
        quality_gate_detail = "auto_profile_released"
    quality_gate_profile_data_source = "live_status"

replan_meta = (
    latest_plan_summary.get("replan_market_side_cooldown")
    if isinstance(latest_plan_summary.get("replan_market_side_cooldown"), dict)
    else {}
)
plan_blocked_reason_counts = (
    latest_plan_summary.get("blocked_reason_counts")
    if isinstance(latest_plan_summary.get("blocked_reason_counts"), dict)
    else {}
)
plan_override_reason_counts = (
    latest_plan_summary.get("override_reason_counts")
    if isinstance(latest_plan_summary.get("override_reason_counts"), dict)
    else {}
)
live_replan = (
    live_status.get("replan_cooldown")
    if isinstance(live_status.get("replan_cooldown"), dict)
    else {}
)
replan_cooldown_minutes = _parse_float(latest_plan_summary.get("replan_market_side_cooldown_minutes"))
replan_input_count = _parse_int(replan_meta.get("input_count")) or 0
replan_kept_count = _parse_int(replan_meta.get("kept_count")) or 0
replan_blocked_count = _parse_int(latest_plan_summary.get("replan_market_side_cooldown_blocked_count"))
if replan_blocked_count is None:
    replan_blocked_count = _parse_int(replan_meta.get("blocked_count")) or 0
replan_override_count = _parse_int(latest_plan_summary.get("replan_market_side_cooldown_override_count"))
if replan_override_count is None:
    replan_override_count = _parse_int(replan_meta.get("override_count")) or 0
replan_backstop_released_count = _parse_int(
    latest_plan_summary.get("replan_market_side_cooldown_backstop_released_count")
)
if replan_backstop_released_count is None:
    replan_backstop_released_count = _parse_int(replan_meta.get("backstop_released_count")) or 0
replan_blocked_ratio = (
    (replan_blocked_count / float(replan_input_count))
    if replan_input_count > 0
    else 0.0
)
replan_repeat_window_minutes = _parse_float(latest_plan_summary.get("replan_market_side_repeat_window_minutes"))
if not isinstance(replan_repeat_window_minutes, float):
    replan_repeat_window_minutes = _parse_float(replan_meta.get("repeat_window_minutes"))
replan_repeat_max_plans_per_window = _parse_int(
    latest_plan_summary.get("replan_market_side_max_plans_per_window")
)
if replan_repeat_max_plans_per_window is None:
    replan_repeat_max_plans_per_window = _parse_int(replan_meta.get("max_plans_per_window"))
replan_repeat_window_input_count = _parse_int(
    latest_plan_summary.get("replan_market_side_repeat_window_input_count")
)
if replan_repeat_window_input_count is None:
    replan_repeat_window_input_count = _parse_int(replan_meta.get("input_count"))
if replan_repeat_window_input_count is None:
    replan_repeat_window_input_count = int(replan_input_count)
replan_repeat_window_kept_count = _parse_int(
    latest_plan_summary.get("replan_market_side_repeat_window_kept_count")
)
if replan_repeat_window_kept_count is None:
    replan_repeat_window_kept_count = _parse_int(replan_meta.get("kept_count"))
if replan_repeat_window_kept_count is None:
    replan_repeat_window_kept_count = int(replan_kept_count)
replan_repeat_cap_blocked_count = _parse_int(
    latest_plan_summary.get("replan_market_side_repeat_cap_blocked_count")
)
if replan_repeat_cap_blocked_count is None:
    replan_repeat_cap_blocked_count = _parse_int(replan_meta.get("repeat_cap_blocked_count"))
if replan_repeat_cap_blocked_count is None:
    replan_repeat_cap_blocked_count = _parse_int(plan_blocked_reason_counts.get("market_side_repeat_cap"))
if replan_repeat_cap_blocked_count is None:
    replan_repeat_cap_blocked_count = 0
replan_repeat_cap_override_count = _parse_int(
    latest_plan_summary.get("replan_market_side_repeat_cap_override_count")
)
if replan_repeat_cap_override_count is None:
    replan_repeat_cap_override_count = _parse_int(replan_meta.get("repeat_cap_override_count"))
if replan_repeat_cap_override_count is None:
    replan_repeat_cap_override_count = int(
        sum(
            int(_parse_int(v) or 0)
            for k, v in plan_override_reason_counts.items()
            if str(k).startswith("repeat_cap_override_")
        )
    )
replan_repeat_cap_blocked_ratio = (
    (replan_repeat_cap_blocked_count / float(replan_repeat_window_input_count))
    if replan_repeat_window_input_count > 0
    else 0.0
)
replan_repeat_cap_override_ratio = (
    (replan_repeat_cap_override_count / float(replan_repeat_window_input_count))
    if replan_repeat_window_input_count > 0
    else 0.0
)
replan_repeat_cap_config = "disabled"
if (
    isinstance(replan_repeat_max_plans_per_window, int)
    and replan_repeat_max_plans_per_window > 0
    and isinstance(replan_repeat_window_minutes, float)
    and replan_repeat_window_minutes > 0.0
):
    replan_repeat_cap_config = (
        f"{replan_repeat_max_plans_per_window} plans/{replan_repeat_window_minutes:g}m"
    )
live_replan_effective_minutes = _parse_float(live_replan.get("effective_minutes"))
if not isinstance(live_replan_effective_minutes, float):
    live_replan_effective_minutes = replan_cooldown_minutes
live_replan_next_minutes = _parse_float(live_replan.get("next_minutes"))
if not isinstance(live_replan_next_minutes, float):
    live_replan_next_minutes = live_replan_effective_minutes
live_replan_min_backstop = _parse_int(live_replan.get("min_orders_backstop_effective"))
if live_replan_min_backstop is None:
    live_replan_min_backstop = _parse_int(latest_plan_summary.get("replan_market_side_min_orders_backstop"))
if live_replan_min_backstop is None:
    live_replan_min_backstop = _parse_int(replan_meta.get("min_orders_backstop"))
live_replan_unique_market_sides = _parse_int(live_replan.get("unique_market_sides")) or 0
live_replan_unique_underlyings = _parse_int(live_replan.get("unique_underlyings")) or 0
live_replan_adaptive_action = _normalize(live_replan.get("adaptive_action")) or "hold"
live_replan_adaptive_reason = _normalize(live_replan.get("adaptive_reason")) or "n/a"
replan_sample_reliable = (
    replan_input_count >= replan_cooldown_min_input_count
    and live_replan_adaptive_reason.lower() not in {"insufficient_input", "metrics_stale"}
)

profitability_guardrail_global_applied_rate = _parse_float(
    latest_plan_summary.get("historical_profitability_guardrail_applied_rate")
)
if not isinstance(profitability_guardrail_global_applied_rate, float):
    profitability_guardrail_global_applied_rate = _parse_float(
        intents_summary.get("historical_profitability_guardrail_applied_rate")
    )
profitability_guardrail_global_penalty_ratio_avg = _parse_float(
    latest_plan_summary.get("historical_profitability_guardrail_penalty_ratio_avg")
)
if not isinstance(profitability_guardrail_global_penalty_ratio_avg, float):
    profitability_guardrail_global_penalty_ratio_avg = _parse_float(
        intents_summary.get("historical_profitability_guardrail_penalty_ratio_avg")
    )
profitability_guardrail_global_blocked_expected_edge_count = _parse_int(
    latest_plan_summary.get("historical_profitability_guardrail_blocked_expected_edge_below_min_count")
)
if profitability_guardrail_global_blocked_expected_edge_count is None:
    profitability_guardrail_global_blocked_expected_edge_count = _parse_int(
        intents_summary.get("historical_profitability_guardrail_blocked_expected_edge_below_min_count")
    )
if profitability_guardrail_global_blocked_expected_edge_count is None:
    profitability_guardrail_global_blocked_expected_edge_count = 0

profitability_guardrail_bucket_applied_rate = _parse_float(
    latest_plan_summary.get("historical_profitability_bucket_guardrail_applied_rate")
)
if not isinstance(profitability_guardrail_bucket_applied_rate, float):
    profitability_guardrail_bucket_applied_rate = _parse_float(
        intents_summary.get("historical_profitability_bucket_guardrail_applied_rate")
    )
profitability_guardrail_bucket_penalty_ratio_avg = _parse_float(
    latest_plan_summary.get("historical_profitability_bucket_guardrail_penalty_ratio_avg")
)
if not isinstance(profitability_guardrail_bucket_penalty_ratio_avg, float):
    profitability_guardrail_bucket_penalty_ratio_avg = _parse_float(
        intents_summary.get("historical_profitability_bucket_guardrail_penalty_ratio_avg")
    )
profitability_guardrail_bucket_blocked_expected_edge_count = _parse_int(
    latest_plan_summary.get("historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count")
)
if profitability_guardrail_bucket_blocked_expected_edge_count is None:
    profitability_guardrail_bucket_blocked_expected_edge_count = _parse_int(
        intents_summary.get("historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count")
    )
if profitability_guardrail_bucket_blocked_expected_edge_count is None:
    profitability_guardrail_bucket_blocked_expected_edge_count = 0

allocation_summary_latest = (
    latest_plan_summary.get("allocation_summary")
    if isinstance(latest_plan_summary.get("allocation_summary"), dict)
    else {}
)
top_candidate_scores_latest = (
    allocation_summary_latest.get("top_candidate_scores")
    if isinstance(allocation_summary_latest.get("top_candidate_scores"), list)
    else []
)
profitability_guardrail_rank_global_terms: list[float] = []
profitability_guardrail_rank_bucket_terms: list[float] = []
for candidate in top_candidate_scores_latest:
    if not isinstance(candidate, dict):
        continue
    score_breakdown = candidate.get("score_breakdown")
    if not isinstance(score_breakdown, dict):
        continue
    global_term = _parse_float(score_breakdown.get("historical_profitability_guardrail_penalty_term"))
    if isinstance(global_term, float):
        profitability_guardrail_rank_global_terms.append(float(global_term))
    bucket_term = _parse_float(score_breakdown.get("historical_profitability_bucket_guardrail_penalty_term"))
    if isinstance(bucket_term, float):
        profitability_guardrail_rank_bucket_terms.append(float(bucket_term))
profitability_guardrail_rank_scored_candidate_count = int(
    max(len(profitability_guardrail_rank_global_terms), len(profitability_guardrail_rank_bucket_terms))
)
profitability_guardrail_rank_global_penalty_term_avg = (
    (sum(profitability_guardrail_rank_global_terms) / float(len(profitability_guardrail_rank_global_terms)))
    if profitability_guardrail_rank_global_terms
    else None
)
profitability_guardrail_rank_bucket_penalty_term_avg = (
    (sum(profitability_guardrail_rank_bucket_terms) / float(len(profitability_guardrail_rank_bucket_terms)))
    if profitability_guardrail_rank_bucket_terms
    else None
)

profitability_guardrail_line = (
    "Profitability guardrails: "
    f"global {_fmt_percent_ratio(profitability_guardrail_global_applied_rate)} "
    f"(penalty avg {profitability_guardrail_global_penalty_ratio_avg:.4f})"
    if isinstance(profitability_guardrail_global_applied_rate, float)
    and isinstance(profitability_guardrail_global_penalty_ratio_avg, float)
    else (
        "Profitability guardrails: "
        f"global {_fmt_percent_ratio(profitability_guardrail_global_applied_rate)}"
        if isinstance(profitability_guardrail_global_applied_rate, float)
        else "Profitability guardrails: global n/a"
    )
)
profitability_guardrail_line += (
    " | "
    f"station/hour {_fmt_percent_ratio(profitability_guardrail_bucket_applied_rate)} "
    f"(penalty avg {profitability_guardrail_bucket_penalty_ratio_avg:.4f})"
    if isinstance(profitability_guardrail_bucket_applied_rate, float)
    and isinstance(profitability_guardrail_bucket_penalty_ratio_avg, float)
    else (
        " | "
        f"station/hour {_fmt_percent_ratio(profitability_guardrail_bucket_applied_rate)}"
        if isinstance(profitability_guardrail_bucket_applied_rate, float)
        else " | station/hour n/a"
    )
)
if (
    isinstance(profitability_guardrail_rank_global_penalty_term_avg, float)
    or isinstance(profitability_guardrail_rank_bucket_penalty_term_avg, float)
):
    profitability_guardrail_line += (
        " | "
        f"rank pressure global {float(profitability_guardrail_rank_global_penalty_term_avg or 0.0):.4f}, "
        f"station/hour {float(profitability_guardrail_rank_bucket_penalty_term_avg or 0.0):.4f} "
        f"(n={profitability_guardrail_rank_scored_candidate_count})"
    )
else:
    profitability_guardrail_line += " | rank pressure n/a (no scored candidates)"
profitability_guardrail_line += (
    " | "
    f"edge blocks global {int(profitability_guardrail_global_blocked_expected_edge_count):,}, "
    f"station/hour {int(profitability_guardrail_bucket_blocked_expected_edge_count):,}"
)
profitability_guardrail_compact_line = (
    "Guardrails (profitability): "
    f"global {_fmt_percent_ratio(profitability_guardrail_global_applied_rate)} | "
    f"station/hour {_fmt_percent_ratio(profitability_guardrail_bucket_applied_rate)}"
)
if (
    isinstance(profitability_guardrail_rank_global_penalty_term_avg, float)
    or isinstance(profitability_guardrail_rank_bucket_penalty_term_avg, float)
):
    profitability_guardrail_compact_line += (
        f" | rank g {float(profitability_guardrail_rank_global_penalty_term_avg or 0.0):.3f} "
        f"h {float(profitability_guardrail_rank_bucket_penalty_term_avg or 0.0):.3f}"
    )
else:
    profitability_guardrail_compact_line += " | rank n/a"
profitability_guardrail_compact_line += (
    f" | edge blocks g {int(profitability_guardrail_global_blocked_expected_edge_count):,} "
    f"h {int(profitability_guardrail_bucket_blocked_expected_edge_count):,}"
)

resolved_market_sides = _parse_int(opportunity_breadth.get("resolved_unique_market_sides")) or 0
resolved_families = _parse_int(opportunity_breadth.get("resolved_unique_underlying_families")) or 0
unresolved_market_sides = _parse_int(opportunity_breadth.get("unresolved_unique_market_sides")) or 0
unresolved_families = _parse_int(opportunity_breadth.get("unresolved_unique_underlying_families")) or 0
repeat_multiplier = _parse_float(opportunity_breadth.get("repeated_entry_multiplier"))
concentration_warning = bool(concentration.get("concentration_warning"))
top_market_side_share = _parse_float(concentration_metrics.get("top_market_side_share"))
approval_to_plan_rate = (
    (planned_orders / float(intents_approved))
    if intents_approved > 0
    else None
)
selection_quality_intent_window = (
    selection_quality.get("intent_window")
    if isinstance(selection_quality.get("intent_window"), dict)
    else {}
)
selection_quality_rows_total = int(
    _parse_int(selection_quality_intent_window.get("rows_total")) or 0
)
selection_quality_rows_adjusted = int(
    _parse_int(selection_quality_intent_window.get("rows_adjusted")) or 0
)
selection_quality_rows_adjusted_bucket_backed = int(
    _parse_int(selection_quality_intent_window.get("rows_adjusted_bucket_backed")) or 0
)
selection_quality_rows_adjusted_global_only = int(
    _parse_int(selection_quality_intent_window.get("rows_adjusted_global_only")) or 0
)
selection_quality_global_only_adjusted_share = (
    (float(selection_quality_rows_adjusted_global_only) / float(selection_quality_rows_adjusted))
    if selection_quality_rows_adjusted > 0
    else None
)
selection_quality_global_only_total_share = (
    (float(selection_quality_rows_adjusted_global_only) / float(selection_quality_rows_total))
    if selection_quality_rows_total > 0
    else None
)
selection_quality_adjusted_rate = _parse_float(
    selection_quality_intent_window.get("adjusted_rate")
)
if not isinstance(selection_quality_adjusted_rate, float):
    selection_quality_adjusted_rate = (
        (float(selection_quality_rows_adjusted) / float(selection_quality_rows_total))
        if selection_quality_rows_total > 0
        else None
    )
selection_quality_bucket_backed_rate = _parse_float(
    selection_quality_intent_window.get("adjusted_bucket_backed_rate")
)
if not isinstance(selection_quality_bucket_backed_rate, float):
    selection_quality_bucket_backed_rate = (
        (float(selection_quality_rows_adjusted_bucket_backed) / float(selection_quality_rows_adjusted))
        if selection_quality_rows_adjusted > 0
        else None
    )
previous_selection_quality_rows_adjusted = int(
    _parse_int(previous_headline_metrics.get("selection_quality_rows_adjusted")) or 0
)
previous_selection_quality_rows_adjusted_global_only = int(
    _parse_int(previous_headline_metrics.get("selection_quality_rows_adjusted_global_only")) or 0
)
previous_selection_quality_global_only_adjusted_share = (
    (float(previous_selection_quality_rows_adjusted_global_only) / float(previous_selection_quality_rows_adjusted))
    if previous_selection_quality_rows_adjusted > 0
    else _parse_float(previous_headline_metrics.get("selection_quality_global_only_adjusted_share"))
)
selection_quality_global_only_share_delta_pp = (
    (
        float(selection_quality_global_only_adjusted_share)
        - float(previous_selection_quality_global_only_adjusted_share)
    )
    * 100.0
    if isinstance(selection_quality_global_only_adjusted_share, float)
    and isinstance(previous_selection_quality_global_only_adjusted_share, float)
    else None
)
selection_quality_global_only_target_share = 0.10
selection_quality_global_only_drift_alert_delta_pp = 1.50
selection_quality_min_rows_for_global_only_alert = max(100, int(float(intents_total) * 0.01))
selection_quality_global_only_pressure_active = bool(
    selection_quality_rows_adjusted >= selection_quality_min_rows_for_global_only_alert
    and isinstance(selection_quality_global_only_adjusted_share, float)
    and selection_quality_global_only_adjusted_share >= selection_quality_global_only_target_share
)
selection_quality_global_only_drift_rising = bool(
    selection_quality_global_only_pressure_active
    and isinstance(selection_quality_global_only_share_delta_pp, float)
    and selection_quality_global_only_share_delta_pp >= selection_quality_global_only_drift_alert_delta_pp
)
selection_quality_pressure_line = ""
selection_quality_pressure_line_concise = ""
if selection_quality_rows_total > 0:
    adjusted_rate_text = (
        f"{selection_quality_adjusted_rate * 100.0:.1f}%"
        if isinstance(selection_quality_adjusted_rate, float)
        else "n/a"
    )
    bucket_rate_text = (
        f"{selection_quality_bucket_backed_rate * 100.0:.1f}%"
        if isinstance(selection_quality_bucket_backed_rate, float)
        else "n/a"
    )
    selection_quality_pressure_line = (
        f"Selection quality pressure: adjusted {selection_quality_rows_adjusted:,}/{selection_quality_rows_total:,} "
        f"({adjusted_rate_text}), bucket-backed {selection_quality_rows_adjusted_bucket_backed:,} "
        f"({bucket_rate_text}), global-only {selection_quality_rows_adjusted_global_only:,}."
    )
    if isinstance(selection_quality_global_only_adjusted_share, float):
        selection_quality_pressure_line += (
            f" Global-only share {selection_quality_global_only_adjusted_share*100.0:.1f}% of adjusted "
            f"(target <= {selection_quality_global_only_target_share*100.0:.1f}%)."
        )
    if isinstance(selection_quality_global_only_share_delta_pp, float):
        selection_quality_pressure_line += (
            f" Drift vs prior {_fmt_signed_percent_points(selection_quality_global_only_share_delta_pp)}."
        )
    global_only_share_text = (
        f"{selection_quality_global_only_adjusted_share*100.0:.1f}%"
        if isinstance(selection_quality_global_only_adjusted_share, float)
        else "n/a"
    )
    drift_text = (
        f" | drift {_fmt_signed_percent_points(selection_quality_global_only_share_delta_pp)}"
        if isinstance(selection_quality_global_only_share_delta_pp, float)
        else ""
    )
    selection_quality_pressure_line_concise = (
        f"Selection pressure: bucket-backed {bucket_rate_text} | "
        f"global-only {global_only_share_text}{drift_text}"
    )

selection_quality_global_only_alert_active = False
selection_quality_global_only_alert_level = "none"
selection_quality_global_only_alert_reason = ""
selection_quality_global_only_alert_summary = ""
if selection_quality_global_only_drift_rising:
    selection_quality_global_only_alert_active = True
    selection_quality_global_only_alert_reason = "selection_quality_global_only_drift"
    selection_quality_global_only_alert_level = "yellow"
    if (
        isinstance(selection_quality_global_only_adjusted_share, float)
        and selection_quality_global_only_adjusted_share >= 0.20
        and isinstance(selection_quality_global_only_share_delta_pp, float)
        and selection_quality_global_only_share_delta_pp >= 4.0
    ):
        selection_quality_global_only_alert_level = "red"
    selection_quality_global_only_alert_summary = (
        "global-only selection-adjustment share rising "
        f"({_fmt_percent_ratio(selection_quality_global_only_adjusted_share)} "
        f"of adjusted, drift {_fmt_signed_percent_points(selection_quality_global_only_share_delta_pp)})"
    )

quality_drift_alert_active = False
quality_drift_alert_level = "none"
quality_drift_alert_reason = ""
quality_drift_alert_summary = ""
quality_drift_direction = "none"
quality_drift_approval_delta_abs_pp = (
    abs(float(window_delta_approval_pp))
    if isinstance(window_delta_approval_pp, float)
    else None
)
quality_drift_previous_intents_ok = (
    isinstance(window_previous_intents, int)
    and window_previous_intents >= quality_drift_min_intents_per_window
)
quality_drift_current_intents_ok = intents_total >= quality_drift_min_intents_per_window
quality_drift_breadth_flat = (
    isinstance(window_delta_resolved_market_sides, int)
    and window_delta_resolved_market_sides <= quality_drift_max_resolved_sides_delta
)
quality_drift_window_eligible = bool(
    window_has_previous
    and quality_drift_current_intents_ok
    and quality_drift_previous_intents_ok
    and isinstance(window_delta_approval_pp, float)
)
if quality_drift_window_eligible and isinstance(quality_drift_approval_delta_abs_pp, float):
    if quality_drift_approval_delta_abs_pp >= quality_drift_delta_pp_min and quality_drift_breadth_flat:
        quality_drift_alert_active = True
        quality_drift_alert_reason = "approval_quality_drift_without_breadth"
        quality_drift_direction = "surge" if float(window_delta_approval_pp) > 0 else "drop"
        quality_drift_alert_level = (
            "red"
            if quality_drift_approval_delta_abs_pp >= (quality_drift_delta_pp_min * 2.0)
            else "yellow"
        )
        quality_drift_alert_summary = (
            f"{quality_drift_direction}: approval {_fmt_signed_percent_points(window_delta_approval_pp)} with "
            f"resolved-side delta {_fmt_signed_int(window_delta_resolved_market_sides)} "
            f"(threshold {quality_drift_delta_pp_min:.2f}pp, min intents {quality_drift_min_intents_per_window:,})"
        )

if quality_drift_alert_active:
    health_reasons.append(quality_drift_alert_reason)
    if quality_drift_alert_level == "red":
        health_status = "RED"
if gate_coverage_alert_active:
    health_reasons.append(gate_coverage_alert_reason)
    if gate_coverage_alert_level == "red":
        health_status = "RED"
if selection_quality_global_only_alert_active:
    health_reasons.append(selection_quality_global_only_alert_reason)
    if selection_quality_global_only_alert_level == "red":
        health_status = "RED"
    elif health_status == "GREEN":
        health_status = "YELLOW"

health_reasons = sorted(set(health_reasons))
if health_status == "GREEN" and health_reasons:
    health_status = "YELLOW"
health_reason_text = ", ".join(_humanize_reason(str(item)) for item in health_reasons if _normalize(item))
if not health_reason_text:
    health_reason_text = "none"

projected_roi_ratio = _parse_float(viability.get("what_return_would_have_been_produced_on_bankroll")) or 0.0
projected_pnl_dollars = projected_roi_ratio * reference_bankroll
projected_utilization = _parse_float(viability.get("what_pct_of_bankroll_would_have_been_utilized_avg")) or 0.0
beat_hysa = bool(viability.get("would_plausibly_beat_hysa_after_slippage_fees"))
limiting_factor = _normalize(viability.get("main_limiting_factor")) or "unknown"
projected_pnl_consistent = (
    abs(float(projected_pnl_dollars) - (float(projected_roi_ratio) * float(reference_bankroll))) <= 0.01
)

expected_edge_total = _parse_float(expected_shadow.get("expected_edge_total")) or 0.0
expected_edge_per_planned_order = (
    (expected_edge_total / float(planned_orders))
    if planned_orders > 0
    else None
)
repeat_multiplier_for_expected = repeat_multiplier if isinstance(repeat_multiplier, float) and repeat_multiplier > 0 else None
expected_edge_total_breadth_normalized = (
    (expected_edge_total / max(1.0, float(repeat_multiplier_for_expected)))
    if repeat_multiplier_for_expected is not None
    else None
)
expected_edge_per_unique_market_side_proxy = (
    (expected_edge_total_breadth_normalized / float(resolved_market_sides))
    if isinstance(expected_edge_total_breadth_normalized, float) and resolved_market_sides > 0
    else None
)
expected_edge_ref_ratio_breadth_normalized = (
    (expected_edge_total_breadth_normalized / float(reference_bankroll))
    if isinstance(expected_edge_total_breadth_normalized, float) and reference_bankroll > 0
    else None
)
expected_edge_ref_ratio_row_audit = (
    (expected_edge_total / float(reference_bankroll))
    if reference_bankroll > 0
    else None
)

trial_start = _parse_float(trial_balance.get("starting_balance_dollars"))
trial_legacy_current = _parse_float(trial_balance.get("current_balance_dollars"))
trial_legacy_growth = _parse_float(trial_balance.get("growth_since_reset_dollars"))
trial_legacy_growth_pct = _parse_float(trial_balance.get("growth_since_reset_percent"))
trial_legacy_win_rate = _parse_float(trial_balance.get("win_rate_since_reset"))
trial_legacy_wins_since_reset = _parse_int(trial_balance.get("wins_since_reset")) or 0
trial_legacy_losses_since_reset = _parse_int(trial_balance.get("losses_since_reset")) or 0
trial_legacy_resolved_since_reset = _parse_int(trial_balance.get("resolved_counterfactual_trades_since_reset")) or 0
trial_legacy_cumulative_counterfactual_pnl = _parse_float(
    trial_balance.get("cumulative_counterfactual_pnl_dollars")
) or 0.0
trial_cash_constrained = (
    trial_balance.get("cash_constrained")
    if isinstance(trial_balance.get("cash_constrained"), dict)
    else {}
)
trial_cash_constrained_current = _parse_float(trial_cash_constrained.get("current_balance_dollars"))
trial_cash_constrained_growth = _parse_float(trial_cash_constrained.get("growth_since_reset_dollars"))
trial_cash_constrained_growth_pct = _parse_float(trial_cash_constrained.get("growth_since_reset_percent"))
trial_cash_constrained_win_rate = _parse_float(trial_cash_constrained.get("win_rate_since_reset"))
trial_cash_constrained_wins = _parse_int(trial_cash_constrained.get("wins_since_reset"))
trial_cash_constrained_losses = _parse_int(trial_cash_constrained.get("losses_since_reset"))
trial_cash_constrained_resolved = _parse_int(
    trial_cash_constrained.get("resolved_counterfactual_trades_since_reset")
)
trial_cash_constrained_pnl = _parse_float(
    trial_cash_constrained.get("cumulative_counterfactual_pnl_dollars")
)
trial_cash_constrained_pushes = _parse_int(trial_cash_constrained.get("pushes_since_reset"))
trial_cash_constrained_skipped_for_cash = _parse_int(
    trial_cash_constrained.get("skipped_for_insufficient_cash_count")
)
trial_cash_constrained_execution_rate = _parse_float(
    trial_cash_constrained.get("execution_rate_vs_unconstrained")
)
trial_balance_mode = "unconstrained_legacy"
trial_current = trial_legacy_current
trial_growth = trial_legacy_growth
trial_growth_pct = trial_legacy_growth_pct
trial_win_rate = trial_legacy_win_rate
trial_wins_since_reset = trial_legacy_wins_since_reset
trial_losses_since_reset = trial_legacy_losses_since_reset
trial_resolved_since_reset = trial_legacy_resolved_since_reset
trial_cumulative_counterfactual_pnl = trial_legacy_cumulative_counterfactual_pnl
if (
    isinstance(trial_cash_constrained_current, float)
    and isinstance(trial_cash_constrained_growth, float)
    and isinstance(trial_cash_constrained_resolved, int)
):
    trial_balance_mode = "cash_constrained"
    trial_current = trial_cash_constrained_current
    trial_growth = trial_cash_constrained_growth
    trial_growth_pct = trial_cash_constrained_growth_pct
    trial_win_rate = trial_cash_constrained_win_rate
    trial_wins_since_reset = trial_cash_constrained_wins if isinstance(trial_cash_constrained_wins, int) else 0
    trial_losses_since_reset = (
        trial_cash_constrained_losses if isinstance(trial_cash_constrained_losses, int) else 0
    )
    trial_resolved_since_reset = trial_cash_constrained_resolved
    trial_cumulative_counterfactual_pnl = trial_cash_constrained_pnl or 0.0
trial_planned_rows_since_reset = _parse_int(trial_balance.get("planned_rows_since_reset")) or 0
trial_unique_shadow_orders_since_reset = _parse_int(trial_balance.get("unique_shadow_orders_since_reset")) or 0
trial_duplicate_count_since_reset = _parse_int(trial_balance.get("duplicate_count")) or 0
trial_duplicate_unique_ids_since_reset = _parse_int(trial_balance.get("duplicate_shadow_order_ids_total_unique"))
trial_resolved_share_unique_orders = (
    (trial_resolved_since_reset / float(trial_unique_shadow_orders_since_reset))
    if trial_unique_shadow_orders_since_reset > 0
    else None
)
trial_balance_negative = bool(
    isinstance(trial_current, float) and trial_current < 0.0
)
trial_duplicate_rows_ratio_since_reset = (
    (trial_duplicate_count_since_reset / float(trial_planned_rows_since_reset))
    if trial_planned_rows_since_reset > 0
    else 0.0
)
trial_windows = (
    trial_cash_constrained.get("windows")
    if trial_balance_mode == "cash_constrained" and isinstance(trial_cash_constrained.get("windows"), dict)
    else trial_balance.get("windows")
)
if not isinstance(trial_windows, dict):
    trial_windows = {}
trial_window_keys = ("1d", "7d", "14d", "21d", "28d", "30d", "3mo", "6mo", "1yr")
trial_window_snapshots: dict[str, dict[str, Any]] = {}
for key in trial_window_keys:
    value = trial_windows.get(key)
    trial_window_snapshots[key] = value if isinstance(value, dict) else {}

shadow_headline = shadow_settled.get("headline") if isinstance(shadow_settled.get("headline"), dict) else {}
profitability_resolved_predictions = _parse_int(shadow_headline.get("resolved_predictions"))
profitability_prediction_win_rate = _parse_float(shadow_headline.get("win_rate"))
profitability_settled_wins = _parse_int(shadow_headline.get("wins"))
profitability_settled_losses = _parse_int(shadow_headline.get("losses"))
profitability_settled_pushes = _parse_int(shadow_settled.get("pushes_unique_market_sides"))
profitability_settled_counterfactual_pnl = _parse_float(
    shadow_settled.get("counterfactual_pnl_total_unique_market_sides_dollars_if_live")
)

settled_metrics_source = "profitability"
settled_metrics_source_reason = ""
settled_metrics_source_mismatch = False
settled_metrics_source_profitability_resolved_predictions = profitability_resolved_predictions
settled_metrics_source_bankroll_resolved_predictions = bankroll_resolved_predictions
data_consistency_notes: list[str] = []

if isinstance(bankroll_resolved_predictions, int):
    resolved_predictions = bankroll_resolved_predictions
    prediction_win_rate = bankroll_prediction_win_rate
    settled_wins = bankroll_settled_wins if isinstance(bankroll_settled_wins, int) else 0
    settled_losses = bankroll_settled_losses if isinstance(bankroll_settled_losses, int) else 0
    settled_pushes = bankroll_settled_pushes if isinstance(bankroll_settled_pushes, int) else 0
    settled_counterfactual_pnl = (
        bankroll_settled_counterfactual_pnl
        if isinstance(bankroll_settled_counterfactual_pnl, float)
        else (
            profitability_settled_counterfactual_pnl
            if isinstance(profitability_settled_counterfactual_pnl, float)
            else 0.0
        )
    )
    settled_metrics_source = "bankroll_validation"
    if (
        isinstance(profitability_resolved_predictions, int)
        and profitability_resolved_predictions != resolved_predictions
    ):
        settled_metrics_source_mismatch = True
        settled_metrics_source_reason = "resolved_prediction_count_mismatch"
        data_consistency_notes.append(
            "settled_prediction_count_mismatch: "
            f"profitability={profitability_resolved_predictions:,} "
            f"bankroll_validation={resolved_predictions:,}"
        )
else:
    resolved_predictions = profitability_resolved_predictions if isinstance(profitability_resolved_predictions, int) else 0
    prediction_win_rate = profitability_prediction_win_rate
    settled_wins = profitability_settled_wins if isinstance(profitability_settled_wins, int) else 0
    settled_losses = profitability_settled_losses if isinstance(profitability_settled_losses, int) else 0
    settled_pushes = profitability_settled_pushes if isinstance(profitability_settled_pushes, int) else 0
    settled_counterfactual_pnl = (
        profitability_settled_counterfactual_pnl
        if isinstance(profitability_settled_counterfactual_pnl, float)
        else 0.0
    )
    settled_metrics_source = "profitability"
    settled_metrics_source_reason = (
        "profitability_missing_settled_metrics"
        if not isinstance(profitability_resolved_predictions, int)
        else ""
    )

if (
    not isinstance(prediction_win_rate, float)
    and resolved_predictions > 0
    and settled_wins >= 0
    and settled_losses >= 0
    and settled_pushes >= 0
):
    settled_denominator = settled_wins + settled_losses + settled_pushes
    if settled_denominator > 0:
        prediction_win_rate = settled_wins / float(settled_denominator)

settled_profit_factor = (
    bankroll_settled_profit_factor
    if isinstance(bankroll_settled_profit_factor, float)
    else None
)
settled_avg_win = (
    bankroll_settled_avg_win
    if isinstance(bankroll_settled_avg_win, float)
    else None
)
settled_avg_loss = (
    bankroll_settled_avg_loss
    if isinstance(bankroll_settled_avg_loss, float)
    else None
)
settled_expectancy_per_trade = (
    bankroll_settled_expectancy_per_trade
    if isinstance(bankroll_settled_expectancy_per_trade, float)
    else None
)
if (
    not isinstance(settled_expectancy_per_trade, float)
    and isinstance(settled_counterfactual_pnl, float)
    and resolved_predictions > 0
):
    settled_expectancy_per_trade = settled_counterfactual_pnl / float(resolved_predictions)
settled_payoff_ratio = (
    (settled_avg_win / abs(settled_avg_loss))
    if isinstance(settled_avg_win, float)
    and isinstance(settled_avg_loss, float)
    and abs(settled_avg_loss) > 1e-9
    else None
)

economics_sanity_note = ""
if (
    resolved_predictions > 0
    and isinstance(prediction_win_rate, float)
    and prediction_win_rate >= 0.60
    and isinstance(settled_counterfactual_pnl, float)
    and settled_counterfactual_pnl < 0.0
):
    detail_bits = []
    if isinstance(settled_expectancy_per_trade, float):
        detail_bits.append(f"expectancy/trade {_fmt_money(settled_expectancy_per_trade)}")
    if isinstance(settled_payoff_ratio, float):
        detail_bits.append(f"payoff ratio {settled_payoff_ratio:.2f}x")
    if isinstance(settled_avg_win, float) and isinstance(settled_avg_loss, float):
        detail_bits.append(f"avgW {_fmt_money(settled_avg_win)} vs avgL {_fmt_money(settled_avg_loss)}")
    economics_sanity_note = "High settled hit rate but negative PnL: loss size outweighed win size."
    if detail_bits:
        economics_sanity_note += " " + "; ".join(detail_bits) + "."
    economics_sanity_note += " Tighten entry price and edge floor."

has_settled_predictions = resolved_predictions > 0
settled_evidence_count = max(int(resolved_predictions), int(resolved_market_sides))
settled_evidence_strength = max(
    0.0,
    min(1.0, float(settled_evidence_count) / float(max(1, settled_evidence_full_at))),
)
expected_edge_proxy_discount_factor = (
    float(expected_edge_proxy_discount_min)
    + (
        (float(expected_edge_proxy_discount_max) - float(expected_edge_proxy_discount_min))
        * float(settled_evidence_strength)
    )
)
settled_denominator = settled_wins + settled_losses + settled_pushes
settled_count_breakdown_consistent = (
    (resolved_predictions == settled_denominator)
    if has_settled_predictions
    else True
)
settled_vs_breadth_consistent = (
    (resolved_predictions == resolved_market_sides)
    if has_settled_predictions and resolved_market_sides > 0
    else True
)
if not has_settled_predictions or resolved_market_sides <= 0:
    # Keep no-settled windows conservative, but avoid a flat cap that ignores
    # proxy-readiness quality and unresolved breadth progress.
    readiness_proxy = max(0.0, min(1.0, deployment_confidence_score_uncapped / 100.0))
    unresolved_breadth_ratio = max(0.0, min(1.0, float(unresolved_market_sides) / 25.0))
    no_settled_cap = 20.0 + (20.0 * readiness_proxy) + (5.0 * unresolved_breadth_ratio)
    no_settled_cap = max(20.0, min(45.0, no_settled_cap))
    deployment_confidence_cap_value = float(no_settled_cap)
    deployment_confidence_score = min(deployment_confidence_score, no_settled_cap)
    deployment_confidence_band = "SHADOW_ONLY"
    deployment_confidence_guidance = (
        "No settled independent outcomes yet; keep capital confidence conservative and stay shadow-only until settlement-aged evidence appears."
    )
    deployment_confidence_cap_reason = "no_settled_independent_outcomes"
    deployment_confidence_cap_applied = True
    deployment_confidence_cap_detail = (
        f"dynamic_cap={no_settled_cap:.1f} from readiness_proxy={deployment_confidence_score_uncapped:.1f} "
        f"and unresolved_sides={unresolved_market_sides:,}"
    )

if isinstance(projected_pnl_dollars, float) and projected_pnl_dollars < 0.0:
    negative_pnl_cap = max(0.0, min(100.0, float(deploy_confidence_negative_pnl_cap)))
    if deployment_confidence_score > negative_pnl_cap:
        deployment_confidence_score = float(negative_pnl_cap)
        deployment_confidence_cap_applied = True
    deployment_confidence_band = "SHADOW_ONLY"
    deployment_confidence_guidance = (
        "Projected bankroll PnL is negative under current sizing and constraints; stay shadow-only until economics improve."
    )
    if not _normalize(deployment_confidence_cap_reason):
        deployment_confidence_cap_reason = "negative_projected_bankroll_pnl"
    elif "negative_projected_bankroll_pnl" not in deployment_confidence_cap_reason.split("+"):
        deployment_confidence_cap_reason = f"{deployment_confidence_cap_reason}+negative_projected_bankroll_pnl"
    if not isinstance(deployment_confidence_cap_value, float) or negative_pnl_cap < deployment_confidence_cap_value:
        deployment_confidence_cap_value = float(negative_pnl_cap)
    negative_pnl_detail = f"projected_pnl={projected_pnl_dollars:.2f}"
    if _normalize(deployment_confidence_cap_detail):
        if negative_pnl_detail not in deployment_confidence_cap_detail:
            deployment_confidence_cap_detail = f"{deployment_confidence_cap_detail}; {negative_pnl_detail}"
    else:
        deployment_confidence_cap_detail = negative_pnl_detail

hysa_cap_condition = (
    (not bool(beat_hysa))
    and (
        bool(has_settled_predictions)
        or (not bool(deploy_confidence_hysa_fail_requires_settled))
    )
)
if hysa_cap_condition:
    hysa_fail_cap = max(0.0, min(100.0, float(deploy_confidence_hysa_fail_cap)))
    if deployment_confidence_score > hysa_fail_cap:
        deployment_confidence_score = float(hysa_fail_cap)
        deployment_confidence_cap_applied = True
    deployment_confidence_band = "SHADOW_ONLY"
    if not _normalize(deployment_confidence_guidance) or "Projected bankroll PnL is negative" not in deployment_confidence_guidance:
        deployment_confidence_guidance = (
            "Bankroll return does not clear the HYSA hurdle under configured slippage/fees; stay shadow-only."
        )
    if not _normalize(deployment_confidence_cap_reason):
        deployment_confidence_cap_reason = "does_not_exceed_hysa_for_window"
    elif "does_not_exceed_hysa_for_window" not in deployment_confidence_cap_reason.split("+"):
        deployment_confidence_cap_reason = f"{deployment_confidence_cap_reason}+does_not_exceed_hysa_for_window"
    if not isinstance(deployment_confidence_cap_value, float) or hysa_fail_cap < deployment_confidence_cap_value:
        deployment_confidence_cap_value = float(hysa_fail_cap)
    hysa_detail = "fails_hysa_hurdle"
    if _normalize(deployment_confidence_cap_detail):
        if hysa_detail not in deployment_confidence_cap_detail:
            deployment_confidence_cap_detail = f"{deployment_confidence_cap_detail}; {hysa_detail}"
    else:
        deployment_confidence_cap_detail = hysa_detail

if critical_artifact_parse_error_keys:
    parse_error_cap = 35.0
    if deployment_confidence_score > parse_error_cap:
        deployment_confidence_score = float(parse_error_cap)
    deployment_confidence_cap_applied = True
    deployment_confidence_band = "SHADOW_ONLY"
    deployment_confidence_guidance = (
        "Critical alpha artifacts failed to parse; keep shadow-only until core runtime artifacts are valid."
    )
    if not _normalize(deployment_confidence_cap_reason):
        deployment_confidence_cap_reason = "critical_artifact_parse_error"
    elif "critical_artifact_parse_error" not in deployment_confidence_cap_reason.split("+"):
        deployment_confidence_cap_reason = (
            f"{deployment_confidence_cap_reason}+critical_artifact_parse_error"
        )
    if (
        not isinstance(deployment_confidence_cap_value, float)
        or parse_error_cap < deployment_confidence_cap_value
    ):
        deployment_confidence_cap_value = float(parse_error_cap)
    parse_error_detail = (
        "parse_error_artifacts=" + ",".join(str(item) for item in critical_artifact_parse_error_keys)
    )
    if _normalize(deployment_confidence_cap_detail):
        if parse_error_detail not in deployment_confidence_cap_detail:
            deployment_confidence_cap_detail = (
                f"{deployment_confidence_cap_detail}; {parse_error_detail}"
            )
    else:
        deployment_confidence_cap_detail = parse_error_detail

deployment_confidence_score = max(0.0, min(100.0, float(deployment_confidence_score)))
if deployment_confidence_cap_reason in {"negative_projected_bankroll_pnl", "does_not_exceed_hysa_for_window"} or (
    isinstance(deployment_confidence_cap_reason, str)
    and (
        "negative_projected_bankroll_pnl" in deployment_confidence_cap_reason.split("+")
        or "does_not_exceed_hysa_for_window" in deployment_confidence_cap_reason.split("+")
    )
):
    deployment_confidence_band = "SHADOW_ONLY"
display_limiting_factor = limiting_factor
top_blocker_limiting_factor = (
    _limiting_factor_from_reason(display_top_blockers[0][0])
    if display_top_blockers
    else ""
)
factor_blocked_counts: dict[str, int] = {}
if display_top_blockers:
    for blocker_reason, blocker_count_raw in display_top_blockers:
        blocker_count = _parse_int(blocker_count_raw)
        if blocker_count is None or blocker_count <= 0:
            continue
        factor_key = _limiting_factor_from_reason(blocker_reason)
        factor_blocked_counts[factor_key] = int(factor_blocked_counts.get(factor_key, 0)) + int(blocker_count)
factor_blocked_denominator = max(1, int(blocked_total_from_counts))
factor_stale_share = float(factor_blocked_counts.get("stale_suppression", 0)) / float(factor_blocked_denominator)
factor_breadth_share = float(factor_blocked_counts.get("insufficient_breadth", 0)) / float(factor_blocked_denominator)

if resolved_market_sides <= 0:
    display_limiting_factor = "insufficient_settled_breadth"
elif display_limiting_factor == "settlement_finalization" and settlement_backlog_clear:
    if top_blocker_limiting_factor:
        display_limiting_factor = top_blocker_limiting_factor
    else:
        display_limiting_factor = "insufficient_breadth"

if (
    display_limiting_factor in {"stale_suppression", "unknown", "settlement_finalization"}
    and top_blocker_limiting_factor == "insufficient_breadth"
    and factor_breadth_share >= max(0.25, factor_stale_share * 1.15)
):
    display_limiting_factor = "insufficient_breadth"
factor_split_mismatch = bool(
    top_blocker_limiting_factor
    and display_limiting_factor
    and top_blocker_limiting_factor != display_limiting_factor
)
factor_split_note = ""
if factor_split_mismatch:
    factor_split_note = (
        "blockers="
        f"{top_blocker_limiting_factor.replace('_', ' ')}; "
        "viability="
        f"{display_limiting_factor.replace('_', ' ')}."
    )
last_resolved_unique_market_side = (
    shadow_settled.get("last_resolved_unique_market_side")
    if isinstance(shadow_settled.get("last_resolved_unique_market_side"), dict)
    else {}
)
last_resolved_unique_shadow_order = (
    shadow_settled.get("last_resolved_unique_shadow_order")
    if isinstance(shadow_settled.get("last_resolved_unique_shadow_order"), dict)
    else {}
)

def _format_last_outcome(
    prefix: str,
    row: dict[str, Any],
    *,
    include_planned_at: bool = True,
    empty_label: str = "pending",
) -> str:
    if not isinstance(row, dict) or not row:
        return f"{prefix}: {empty_label}"
    market_ticker = _normalize(row.get("market_ticker")) or "n/a"
    side = _normalize(row.get("side")) or "n/a"
    win = row.get("win")
    if win is True:
        outcome = "WIN"
    elif win is False:
        outcome = "LOSS"
    else:
        outcome = "PUSH/UNKNOWN"
    pnl = _fmt_money(row.get("counterfactual_pnl_dollars_if_live"))
    if include_planned_at:
        planned_at = _normalize(row.get("planned_at_utc")) or "n/a"
        return (
            f"{prefix}: {market_ticker} {side} -> {outcome} | "
            f"simulated-live pnl {pnl} | planned_at {planned_at}"
        )
    return f"{prefix}: {market_ticker} {side} -> {outcome} | simulated-live pnl {pnl}"

confidence_level = "LOW"
if resolved_market_sides >= 25 and isinstance(prediction_win_rate, float):
    confidence_level = "HIGH"
elif resolved_market_sides >= 10:
    confidence_level = "MEDIUM"

cap_reason_tokens = [
    token
    for token in str(deployment_confidence_cap_reason or "").split("+")
    if _normalize(token)
]
if (
    "negative_projected_bankroll_pnl" in cap_reason_tokens
    or "does_not_exceed_hysa_for_window" in cap_reason_tokens
):
    confidence_level = "LOW"
elif "no_settled_independent_outcomes" in cap_reason_tokens and confidence_level == "HIGH":
    confidence_level = "MEDIUM"

pilot_target_score = 65.0
pilot_target_gate_score = pilot_target_score / 100.0
pilot_gap_effective = max(0.0, pilot_target_score - deployment_confidence_score)
pilot_gap_uncapped = max(0.0, pilot_target_score - deployment_confidence_score_uncapped)
horizon_delta_rows: list[dict[str, Any]] = []
for label in horizon_order:
    row = readiness_by_horizon.get(label, {})
    if not isinstance(row, dict):
        continue
    gates = row.get("gates") if isinstance(row.get("gates"), dict) else {}
    gate_score = _parse_float(gates.get("gate_score"))
    if not isinstance(gate_score, float):
        continue
    deficit = max(0.0, pilot_target_gate_score - gate_score)
    weighted_impact_points = deficit * float(horizon_weights.get(label, 0.0)) * 100.0
    if deficit <= 0:
        continue
    horizon_delta_rows.append(
        {
            "horizon": label,
            "current_gate_score": round(gate_score, 6),
            "target_gate_score": round(pilot_target_gate_score, 6),
            "deficit_gate_score": round(deficit, 6),
            "weighted_impact_points": round(weighted_impact_points, 6),
        }
    )
horizon_delta_rows.sort(key=lambda row: float(row.get("weighted_impact_points") or 0.0), reverse=True)
horizon_delta_top = horizon_delta_rows[:3]
pilot_gap_line = "Pilot threshold delta: already at/above 65.0."
if pilot_gap_effective > 0:
    pilot_gap_line = f"Pilot threshold delta to 65.0: +{pilot_gap_effective:.1f} points."
    if deployment_confidence_cap_applied:
        pilot_gap_line += f" (post-cap gap: +{pilot_gap_uncapped:.1f})"
pilot_horizon_delta_line = "Pilot gap horizon drivers: none."
if horizon_delta_top:
    segments = []
    for row in horizon_delta_top:
        horizon = _normalize(row.get("horizon"))
        current_gate_score = _parse_float(row.get("current_gate_score")) or 0.0
        weighted_impact_points = _parse_float(row.get("weighted_impact_points")) or 0.0
        segments.append(
            f"{horizon} +{weighted_impact_points:.1f}pts ({current_gate_score*100.0:.0f}%→65%)"
        )
    pilot_horizon_delta_line = "Pilot gap horizon drivers: " + ", ".join(segments)

pilot_required_horizons = ("14d", "21d")
pilot_gate_rows: list[dict[str, Any]] = []
pilot_open_reason_counts: dict[str, int] = {}
pilot_checks_total = 0
pilot_checks_passed = 0
for horizon in pilot_required_horizons:
    row = readiness_by_horizon.get(horizon, {})
    if not isinstance(row, dict) or not row:
        pilot_checks_total += 1
        reason = "missing_horizon_readiness_row"
        pilot_open_reason_counts[reason] = int(pilot_open_reason_counts.get(reason, 0)) + 1
        pilot_gate_rows.append(
            {
                "horizon": horizon,
                "available": False,
                "gate_score": None,
                "checks_total": 1,
                "checks_passed": 0,
                "checks_open": 1,
                "failed_reasons": [reason],
            }
        )
        continue

    gates = row.get("gates") if isinstance(row.get("gates"), dict) else {}
    gate_score = _parse_float(gates.get("gate_score"))
    checks = gates.get("checks") if isinstance(gates.get("checks"), list) else []
    row_checks_total = 0
    row_checks_passed = 0
    row_failed_reasons: list[str] = []
    for check in checks:
        if not isinstance(check, dict):
            continue
        row_checks_total += 1
        if bool(check.get("passed")):
            row_checks_passed += 1
            continue
        reason = _normalize(check.get("reason")) or "unknown_gate_failure"
        row_failed_reasons.append(reason)
        pilot_open_reason_counts[reason] = int(pilot_open_reason_counts.get(reason, 0)) + 1

    if row_checks_total == 0:
        row_checks_total = 1
        if bool(row.get("ready_for_real_money")):
            row_checks_passed = 1
        else:
            reason = "unknown_gate_failure"
            row_failed_reasons.append(reason)
            pilot_open_reason_counts[reason] = int(pilot_open_reason_counts.get(reason, 0)) + 1

    pilot_checks_total += row_checks_total
    pilot_checks_passed += row_checks_passed
    pilot_gate_rows.append(
        {
            "horizon": horizon,
            "available": True,
            "gate_score": round(gate_score, 6) if isinstance(gate_score, float) else None,
            "checks_total": row_checks_total,
            "checks_passed": row_checks_passed,
            "checks_open": max(0, row_checks_total - row_checks_passed),
            "failed_reasons": row_failed_reasons,
        }
    )

pilot_checks_open = max(0, pilot_checks_total - pilot_checks_passed)
pilot_minimum_flips_needed = pilot_checks_open
pilot_checklist_line = (
    f"Pilot gate checklist (14d+21d): {pilot_checks_passed}/{pilot_checks_total} checks passing; "
    f"open {pilot_checks_open}; minimum flips needed {pilot_minimum_flips_needed}."
    if pilot_checks_total > 0
    else "Pilot gate checklist (14d+21d): n/a."
)
pilot_open_reasons_sorted = sorted(
    pilot_open_reason_counts.items(),
    key=lambda item: (-int(item[1]), str(item[0])),
)
pilot_open_reasons_line = "Pilot open checks (top): none."
if pilot_open_reasons_sorted:
    segments = [
        f"{_clip_text(reason.replace('_', ' '), 36)} x{count}"
        for reason, count in pilot_open_reasons_sorted[:3]
    ]
    pilot_open_reasons_line = "Pilot open checks (top): " + ", ".join(segments)
pilot_top_open_reason = pilot_open_reasons_sorted[0][0] if pilot_open_reasons_sorted else ""

suggestions: list[str] = []
suggestion_rows: list[dict[str, Any]] = []
suggestion_keys: set[str] = set()
stale_target_rate = 0.005 if stale_rate >= 0.010 else 0.003
overlap_target_rate = 0.030
repeat_target_multiplier = 25.0
duplicate_target_ratio = 0.25
replan_blocked_target_ratio = 0.45
stale_pressure_points = max(2.0, min(20.0, stale_rate * 240.0))
overlap_pressure_points = max(2.0, min(24.0, overlap_rate * 300.0))
repeat_pressure_points = max(
    2.0,
    min(
        28.0,
        (
            (float(repeat_multiplier) - repeat_target_multiplier)
            if isinstance(repeat_multiplier, float)
            else 4.0
        )
        * 0.22,
    ),
)
duplicate_pressure_points = max(2.0, min(20.0, trial_duplicate_rows_ratio_since_reset * 60.0))
pilot_gap_pressure_points = max(2.0, min(22.0, pilot_gap_effective * 0.40))
breadth_pressure_points = max(2.0, min(24.0, max(0.0, 10.0 - float(resolved_market_sides)) * 1.7))

if quality_drift_alert_active:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "approval_quality_drift",
        "Investigate approval-rate drift immediately; keep thresholds fixed until breadth or settled quality confirms the shift.",
        target=(
            f"|approval delta| <= {quality_drift_delta_pp_min:.2f}pp "
            f"with resolved market-side delta > {quality_drift_max_resolved_sides_delta}"
        ),
        expected_impact=(
            "prevents silent quality drift where throughput changes faster than independent breadth growth"
        ),
        impact_points=max(
            12.0,
            min(
                30.0,
                float(quality_drift_approval_delta_abs_pp or 0.0) * 4.0 + 8.0,
            ),
        ),
        eta_hours=8.0,
        metric_key="window_delta_approval_rate_percentage_points_abs",
        metric_current=float(quality_drift_approval_delta_abs_pp or 0.0),
        metric_target=float(quality_drift_delta_pp_min),
        metric_direction="down",
    )

if gate_coverage_alert_active:
    gate_coverage_alert_gaps_sorted = sorted(
        gate_coverage_alert_gaps,
        key=lambda row: float(row.get("shortfall_ratio") or 0.0),
        reverse=True,
    )
    gate_coverage_primary_gap = (
        gate_coverage_alert_gaps_sorted[0]
        if gate_coverage_alert_gaps_sorted
        else {}
    )
    gate_coverage_primary_label = _normalize(gate_coverage_primary_gap.get("label")) or "edge"
    gate_coverage_primary_gate = _normalize(gate_coverage_primary_gap.get("gate")) or "expected_edge"
    gate_coverage_primary_ratio = _parse_float(gate_coverage_primary_gap.get("ratio")) or 0.0
    gate_coverage_primary_threshold = _parse_float(gate_coverage_primary_gap.get("threshold")) or 0.0
    gate_coverage_alert_gap_summary = ", ".join(
        f"{_normalize(item.get('label'))} {(_parse_float(item.get('ratio')) or 0.0)*100.0:.1f}%<{(_parse_float(item.get('threshold')) or 0.0)*100.0:.1f}%"
        for item in gate_coverage_alert_gaps_sorted[:3]
    )
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "approval_gate_enforcement",
        "Enforce approval gate coverage so approved intents are fully parameter-driven before further throughput tuning.",
        target=(
            f"{gate_coverage_primary_label} coverage >= {gate_coverage_primary_threshold*100.0:.1f}% "
            f"(now {gate_coverage_primary_ratio*100.0:.1f}%)"
        ),
        expected_impact=(
            "reduces false-confidence approvals and keeps policy decisions aligned with probability/edge thresholds"
            + (f"; gaps: {gate_coverage_alert_gap_summary}" if gate_coverage_alert_gap_summary else "")
        ),
        impact_points=max(
            10.0,
            min(
                30.0,
                (float(gate_coverage_alert_worst_shortfall_ratio or 0.0) * 35.0) + 10.0,
            ),
        ),
        eta_hours=12.0,
        metric_key=f"gate_coverage_{gate_coverage_primary_gate}",
        metric_current=gate_coverage_primary_ratio,
        metric_target=gate_coverage_primary_threshold,
        metric_direction="up",
    )

if (
    dominant_blocker_share_of_blocked >= 0.80
    and dominant_blocker_count >= max(500, int(intents_total * 0.05))
):
    dominant_blocker_label = _humanize_reason(dominant_blocker_reason_key) or "dominant blocker"
    dominant_action_map = {
        "expected_edge_below_min": "Recalibrate expected-edge scoring and threshold calibration on settled unique market-sides before broad policy changes.",
        "edge_to_risk_ratio_below_min": "Tighten edge-to-risk gating and lot-size risk assumptions before increasing throughput.",
        "alpha_strength_below_min": "Recalibrate alpha-strength gating inputs before broad threshold changes.",
        "probability_confidence_below_min": "Recalibrate probability-confidence model and confidence floor before broad tuning.",
        "metar_observation_stale": "Prioritize freshness remediation first; other tuning has lower ROI until stale share declines.",
        "metar_freshness_boundary_quality_insufficient": "Prioritize near-stale quality handling first; other tuning has lower ROI until this blocker drops.",
        "inside_cutoff_window": "Shift scans earlier and tighten near-close cadence before broader threshold tuning.",
        "settlement_finalization_blocked": "Prioritize settlement-finalization throughput and pressure-mode reliability before signal retuning.",
        "historical_quality_global_only_pressure": "Reduce global-only selection pressure by expanding bucket-backed evidence and tightening weak-evidence approvals before threshold changes.",
    }
    dominant_action = dominant_action_map.get(
        dominant_blocker_reason_key,
        f"Prioritize the dominant blocker ({dominant_blocker_label}) before broad policy tuning.",
    )
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "dominant_blocker_focus",
        dominant_action,
        target=(
            f"{dominant_blocker_label} <= 70% of blocked flow "
            f"(now {dominant_blocker_share_of_blocked*100.0:.1f}%)"
        ),
        expected_impact="concentrates effort on the main suppressor and avoids low-yield parallel tuning",
        impact_points=max(
            18.0,
            min(
                42.0,
                (dominant_blocker_share_of_blocked * 42.0)
                + (8.0 if dominant_blocker_share_of_blocked >= 0.90 else 0.0),
            ),
        ),
        eta_hours=12.0,
        metric_key="dominant_blocker_share_of_blocked",
        metric_current=dominant_blocker_share_of_blocked,
        metric_target=0.70,
        metric_direction="down",
    )

edge_floor_tuning_needed = bool(
    edge_too_low_count >= max(50, int(intents_total * 0.02))
    and (
        edge_gate_blocked_dominant
        or edge_gate_blocked_share_of_blocked >= 0.35
        or dominant_blocker_reason_key == "expected_edge_below_min"
        or weekly_top_reason_key == "expected_edge_below_min"
    )
)
if edge_floor_tuning_needed:
    edge_gate_target_share = 0.55 if edge_gate_blocked_share_of_blocked >= 0.75 else 0.45
    sparse_edge_block_share_text = (
        f"{sparse_hardening_expected_edge_block_share*100.0:.1f}%"
        if isinstance(sparse_hardening_expected_edge_block_share, float)
        else "n/a"
    )
    sparse_edge_basis_text = (
        f"{sparse_hardening_basis_label} edge-block share {sparse_edge_block_share_text}"
        if _normalize(sparse_hardening_basis_label)
        else f"edge-block share {sparse_edge_block_share_text}"
    )
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "edge_floor_bucket_tuning",
        "Retune expected-edge floors by station/hour buckets using settled outcomes; keep global edge floor unchanged.",
        target=(
            f"edge-gate blocked share <= {edge_gate_target_share*100.0:.1f}% "
            f"(now {edge_gate_blocked_share_of_blocked*100.0:.1f}%; expected-edge blocked {edge_too_low_count:,})"
        ),
        expected_impact=(
            "frees high-quality blocked flow while preserving global guardrails"
            + (f"; {sparse_edge_basis_text}" if sparse_edge_block_share_text != "n/a" else "")
        ),
        impact_points=max(
            10.0,
            min(
                32.0,
                (edge_gate_blocked_share_of_blocked * 28.0)
                + (6.0 if isinstance(sparse_hardening_expected_edge_block_share, float) and sparse_hardening_expected_edge_block_share >= 0.70 else 0.0)
                + 4.0,
            ),
        ),
        eta_hours=24.0,
        metric_key="edge_gate_blocked_share_of_blocked",
        metric_current=edge_gate_blocked_share_of_blocked,
        metric_target=edge_gate_target_share,
        metric_direction="down",
        priority_boost_points=max(
            0.0,
            min(
                14.0,
                (edge_gate_blocked_share_of_blocked - edge_gate_target_share) * 28.0
                + (4.0 if dominant_blocker_reason_key == "expected_edge_below_min" else 0.0),
            ),
        ),
        priority_boost_reason=(
            "edge_gate_blocked_dominant"
            if edge_gate_blocked_dominant
            else "edge_gate_blocked_elevated"
        ),
    )

if stale_rate >= 0.20 or stale_count >= max(50, int(intents_total * 0.10)):
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "stale_freshness",
        "Cut stale blocks first with station/hour freshness tuning and faster rescans in stale clusters.",
        target=f"stale rate <= {stale_target_rate*100.0:.2f}% (now {stale_rate*100.0:.2f}%)",
        expected_impact="recover roughly 1-3 approval points in stale-heavy windows",
        impact_points=stale_pressure_points,
        eta_hours=36.0,
        metric_key="stale_block_rate",
        metric_current=stale_rate,
        metric_target=stale_target_rate,
        metric_direction="down",
    )

if selection_quality_global_only_pressure_active:
    selection_quality_global_only_target_pct = selection_quality_global_only_target_share * 100.0
    selection_quality_global_only_share_pct = (
        float(selection_quality_global_only_adjusted_share or 0.0) * 100.0
    )
    drift_clause = ""
    if isinstance(selection_quality_global_only_share_delta_pp, float):
        drift_clause = (
            f"; drift vs prior {_fmt_signed_percent_points(selection_quality_global_only_share_delta_pp)}"
        )
    global_only_priority_boost = max(
        0.0,
        min(
            24.0,
            (selection_quality_global_only_share_pct - selection_quality_global_only_target_pct) * 1.8
            + (8.0 if selection_quality_global_only_drift_rising else 0.0),
        ),
    )
    global_only_priority_reason = (
        "selection_quality_global_only_drift_rising"
        if selection_quality_global_only_drift_rising
        else "selection_quality_global_only_pressure"
    )
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "selection_quality_global_only_drift",
        "Cut global-only selection-pressure drift by expanding station/hour evidence coverage before widening throughput.",
        target=(
            f"global-only adjusted share <= {selection_quality_global_only_target_pct:.1f}% "
            f"(now {selection_quality_global_only_share_pct:.1f}% across {selection_quality_rows_adjusted:,} adjusted rows)"
        ),
        expected_impact=(
            "keeps selection updates data-backed and limits model-only drift in approval decisions"
            f"{drift_clause}"
        ),
        impact_points=max(
            8.0,
            min(
                30.0,
                (selection_quality_global_only_share_pct - selection_quality_global_only_target_pct) * 1.4
                + (6.0 if selection_quality_global_only_drift_rising else 2.0),
            ),
        ),
        eta_hours=18.0 if selection_quality_global_only_drift_rising else 30.0,
        metric_key="selection_quality_global_only_adjusted_share",
        metric_current=float(selection_quality_global_only_adjusted_share or 0.0),
        metric_target=float(selection_quality_global_only_target_share),
        metric_direction="down",
        priority_boost_points=global_only_priority_boost,
        priority_boost_reason=global_only_priority_reason,
    )

if approval_audit_approved_rows >= 100 and (
    approval_audit_mismatch_rows > 0
    or approval_audit_revalidation_conflicts > 0
):
    mismatch_pressure_points = max(
        8.0,
        min(
            30.0,
            (
                (
                    (approval_audit_mismatch_rate if isinstance(approval_audit_mismatch_rate, float) else 0.0)
                    + (
                        approval_audit_revalidation_conflicts_rate
                        if isinstance(approval_audit_revalidation_conflicts_rate, float)
                        else 0.0
                    )
                )
                * 300.0
            )
            + 8.0,
        ),
    )
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "approval_gate_drift",
        "Fix approval gate drift: approved rows must pass active thresholds and revalidation cleanly.",
        target=(
            f"approved gate mismatch = 0 (now {approval_audit_mismatch_rows:,}), "
            f"revalidation conflicts = 0 (now {approval_audit_revalidation_conflicts:,})"
        ),
        expected_impact="prevents false-positive approvals and keeps projected alpha aligned to policy intent",
        impact_points=mismatch_pressure_points,
        eta_hours=24.0,
        metric_key="approved_rows_with_gate_mismatch",
        metric_current=float(approval_audit_mismatch_rows),
        metric_target=0.0,
        metric_direction="down",
    )

if settlement_pressure_active and settlement_blocked_count_actionable >= max(20, int(intents_total * 0.10)):
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "settlement_pressure",
        "Speed up settlement refresh while backlog is active.",
        target=f"settlement unresolved now = 0 (now {int(current_settlement_unresolved):,})",
        expected_impact="reduce finalization-blocked candidates and improve late-cycle throughput",
        impact_points=max(6.0, min(24.0, float(current_settlement_unresolved) * 0.7 + 8.0)),
        eta_hours=12.0,
        metric_key="settlement_unresolved_now",
        metric_current=float(current_settlement_unresolved),
        metric_target=0.0,
        metric_direction="down",
    )

if overlap_rate >= 0.05 or overlap_total_count >= max(500, int(intents_total * 0.02)):
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "interval_overlap_pressure",
        "Prioritize bracket consistency and path-aware weather filters on worst overlap stations/hours.",
        target=f"interval-overlap rate <= {overlap_target_rate*100.0:.2f}% (now {overlap_rate*100.0:.2f}%)",
        expected_impact="convert overlap rejects into independent actionable calls",
        impact_points=overlap_pressure_points,
        eta_hours=72.0,
        metric_key="interval_overlap_blocked_rate",
        metric_current=overlap_rate,
        metric_target=overlap_target_rate,
        metric_direction="down",
    )

if concentration_warning or (isinstance(repeat_multiplier, float) and repeat_multiplier >= 20.0):
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "concentration",
        "Reduce repeat stacking on the same market-side and prioritize new families first.",
        target=(
            f"repeat multiplier <= {repeat_target_multiplier:.0f}x "
            f"(now {repeat_multiplier:.2f}x)"
            if isinstance(repeat_multiplier, float)
            else f"repeat multiplier <= {repeat_target_multiplier:.0f}x"
        ),
        expected_impact="improve independent alpha breadth and reduce over-counted hit-rate illusion",
        impact_points=repeat_pressure_points,
        eta_hours=96.0,
        metric_key="repeated_entry_multiplier",
        metric_current=repeat_multiplier if isinstance(repeat_multiplier, float) else None,
        metric_target=repeat_target_multiplier,
        metric_direction="down",
    )

if trial_duplicate_rows_ratio_since_reset >= 0.25 and trial_planned_rows_since_reset >= 1000:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "duplicate_order_reuse",
        "Reduce duplicate plans by skipping unchanged market-sides across consecutive cycles.",
        target=(
            f"duplicate row ratio <= {duplicate_target_ratio*100.0:.1f}% "
            f"(now {trial_duplicate_rows_ratio_since_reset*100.0:.2f}%)"
        ),
        expected_impact="cleaner settlement scoring and less noisy expected-edge inflation",
        impact_points=duplicate_pressure_points,
        eta_hours=48.0,
        metric_key="trial_duplicate_rows_ratio_since_reset",
        metric_current=trial_duplicate_rows_ratio_since_reset,
        metric_target=duplicate_target_ratio,
        metric_direction="down",
    )

gate_coverage_targets = {
    "expected_edge": 0.90,
    "probability_confidence": 0.90,
    "alpha_strength": 0.50,
}
gate_coverage_current = {
    "expected_edge": (
        approval_gate_coverage_expected_edge_active_ratio
        if isinstance(approval_gate_coverage_expected_edge_active_ratio, float)
        else approval_gate_coverage_expected_edge_ratio
    ),
    "probability_confidence": (
        approval_gate_coverage_probability_active_ratio
        if isinstance(approval_gate_coverage_probability_active_ratio, float)
        else approval_gate_coverage_probability_ratio
    ),
    "alpha_strength": (
        approval_gate_coverage_alpha_active_ratio
        if isinstance(approval_gate_coverage_alpha_active_ratio, float)
        else approval_gate_coverage_alpha_ratio
    ),
}
low_gate_coverage_items: list[tuple[str, float, float]] = []
for gate_key, target_ratio in gate_coverage_targets.items():
    current_ratio = gate_coverage_current.get(gate_key)
    if isinstance(current_ratio, float) and current_ratio < target_ratio:
        low_gate_coverage_items.append((gate_key, current_ratio, target_ratio))
if approval_audit_approved_rows >= 1000 and low_gate_coverage_items:
    low_gate_coverage_items.sort(key=lambda item: (item[1] - item[2]))
    top_gate_key, top_current_ratio, top_target_ratio = low_gate_coverage_items[0]
    low_gate_summary = ", ".join(
        f"{gate.replace('_', ' ')} {current*100.0:.1f}%/{target*100.0:.1f}%"
        for gate, current, target in low_gate_coverage_items[:3]
    )
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "gate_coverage_gaps",
        "Increase approved-row gate coverage for edge/probability/alpha so approvals are fully parameter-driven.",
        target=f"{top_gate_key.replace('_', ' ')} coverage >= {top_target_ratio*100.0:.1f}% (now {top_current_ratio*100.0:.1f}%)",
        expected_impact=f"improves decision traceability and reduces approvals based on partial gate evidence ({low_gate_summary})",
        impact_points=max(6.0, min(22.0, (top_target_ratio - top_current_ratio) * 40.0 + 5.0)),
        eta_hours=24.0,
        metric_key=f"gate_coverage_{top_gate_key}",
        metric_current=top_current_ratio,
        metric_target=top_target_ratio,
        metric_direction="up",
    )

if csv_cache_degraded:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "profitability_cache_hardening",
        "Repair profitability cache persistence and permissions to keep alpha evidence consistent.",
        target="csv cache puts_failed = 0 and commit_ok = true on every 12h summary",
        expected_impact="prevents silent parse-cache drift and keeps reporting throughput stable",
        impact_points=18.0 if csv_cache_puts_failed_readonly > 0 else 12.0,
        eta_hours=4.0,
        metric_key="csv_cache_puts_failed",
        metric_current=float(csv_cache_puts_failed),
        metric_target=0.0,
        metric_direction="down",
    )

if replan_sample_reliable and replan_input_count >= 10 and replan_blocked_ratio >= 0.7:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "replan_cooldown_pressure",
        "Tune replan overrides so genuine weather-state shifts pass but noise stays blocked.",
        target=f"replan blocked ratio <= {replan_blocked_target_ratio*100.0:.0f}% (now {replan_blocked_ratio*100.0:.2f}%)",
        expected_impact="increase planned throughput without lowering quality gates",
        impact_points=max(5.0, min(22.0, replan_blocked_ratio * 30.0)),
        eta_hours=24.0,
        metric_key="replan_blocked_ratio",
        metric_current=replan_blocked_ratio,
        metric_target=replan_blocked_target_ratio,
        metric_direction="down",
    )

if replan_sample_reliable and replan_backstop_released_count >= 1 and replan_blocked_ratio >= 0.5:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "replan_backstop_dependency",
        "Reduce dependency on backstop releases by increasing independent opportunities.",
        target=f"backstop-released share < 20% (now {replan_backstop_released_count:,} released in-window)",
        expected_impact="healthier organic throughput and less fallback-driven planning",
        impact_points=max(5.0, min(20.0, float(replan_backstop_released_count) * 1.2)),
        eta_hours=72.0,
        metric_key="replan_backstop_released_count",
        metric_current=float(replan_backstop_released_count),
        metric_target=0.0,
        metric_direction="down",
    )

if replan_repeat_window_input_count >= 20 and replan_repeat_cap_blocked_ratio >= 0.15:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "replan_repeat_cap_balance",
        "Rebalance repeat-cap policy against breadth so it blocks true recycling without starving valid updates.",
        target=(
            f"repeat-cap blocked ratio <= 10% (now {replan_repeat_cap_blocked_ratio*100.0:.2f}%) "
            f"at config {replan_repeat_cap_config}"
        ),
        expected_impact="maintains anti-repeat discipline while preserving actionable throughput",
        impact_points=max(5.0, min(18.0, replan_repeat_cap_blocked_ratio * 35.0)),
        eta_hours=24.0,
        metric_key="replan_repeat_cap_blocked_ratio",
        metric_current=replan_repeat_cap_blocked_ratio,
        metric_target=0.10,
        metric_direction="down",
    )
if (
    replan_sample_reliable
    and replan_input_count >= 10
    and replan_blocked_ratio >= 0.7
    and live_replan_unique_market_sides <= 8
):
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "replan_breadth_bottleneck",
        "Expand profile diversity and station/hour coverage before raising caps.",
        target=f"replan unique market-sides >= 12 (now {live_replan_unique_market_sides:,})",
        expected_impact="reduce concentration and improve independent market-side flow",
        impact_points=max(6.0, min(18.0, float(max(0, 12 - live_replan_unique_market_sides)) * 1.8 + 6.0)),
        eta_hours=96.0,
        metric_key="replan_unique_market_sides",
        metric_current=float(live_replan_unique_market_sides),
        metric_target=12.0,
        metric_direction="up",
    )
if (
    isinstance(live_replan_min_backstop, int)
    and live_replan_min_backstop >= 8
    and replan_sample_reliable
    and replan_input_count >= 10
    and replan_blocked_ratio >= 0.7
):
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "replan_backstop_near_cap",
        "Widen independent opportunities before increasing order caps.",
        target=f"min backstop <= 8 with stable throughput (now {live_replan_min_backstop})",
        expected_impact="prevents cap growth from masking narrow opportunity breadth",
        impact_points=max(4.0, min(14.0, float(max(0, live_replan_min_backstop - 8)) * 2.0 + 4.0)),
        eta_hours=96.0,
        metric_key="replan_min_orders_backstop_effective",
        metric_current=float(live_replan_min_backstop),
        metric_target=8.0,
        metric_direction="down",
    )

if resolved_market_sides < 10:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "breadth",
        "Increase independent breadth by expanding profile diversity and station/hour coverage.",
        target=f"resolved unique market-sides >= 10 (now {resolved_market_sides:,})",
        expected_impact="enables meaningful bankroll deployment and more reliable quality scoring",
        impact_points=breadth_pressure_points,
        eta_hours=120.0,
        metric_key="resolved_unique_market_sides",
        metric_current=float(resolved_market_sides),
        metric_target=10.0,
        metric_direction="up",
    )

by_station = attribution.get("by_station") if isinstance(attribution.get("by_station"), dict) else {}
weak_station: tuple[str, float, int, float | None] | None = None
station_target_rate: float | None = None
station_min_intents = max(500, int(max(1, intents_total) * 0.01))
station_approval_reference = min(
    approval_rate_guardrail_max,
    max(approval_rate_guardrail_min, approval_rate),
)
station_target_floor = max(
    approval_rate_guardrail_min * 0.8,
    station_approval_reference * 0.65,
)
for station, row in sorted(
    by_station.items(),
    key=lambda item: (
        float(item[1].get("approval_rate") or 1.0)
        if isinstance(item[1], dict)
        else 1.0,
        -float(item[1].get("intents_total") or 0.0)
        if isinstance(item[1], dict)
        else 0.0,
    ),
):
    if not isinstance(row, dict):
        continue
    station_intents = _parse_int(row.get("intents_total")) or 0
    station_approval = _parse_float(row.get("approval_rate"))
    if station_intents < station_min_intents or not isinstance(station_approval, float):
        continue
    if station_approval >= max(station_target_floor, approval_rate * 0.90):
        continue
    weak_station = (
        _normalize(station),
        station_approval,
        station_intents,
        _parse_float(row.get("expected_roi")),
    )
    break
if weak_station:
    station_target_rate = min(
        approval_rate_guardrail_max,
        max(
            float(weak_station[1]) + 0.03,
            station_target_floor,
        ),
    )
    station_target_rate = max(station_target_rate, float(weak_station[1]) + 0.02)
    station_target_rate = min(station_target_rate, approval_rate_guardrail_max)
    weak_station_roi_clause = (
        f"; expected ROI {weak_station[3]*100.0:.2f}%"
        if isinstance(weak_station[3], float)
        else ""
    )
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "weak_station",
        (
            f"Retune {weak_station[0]} edge/freshness gates before global changes."
        ),
        target=(
            f"{weak_station[0]} approval >= {station_target_rate*100.0:.2f}% "
            f"(now {weak_station[1]*100.0:.2f}% across {weak_station[2]:,} intents)"
        ),
        expected_impact=(
            "improves one of the weakest high-volume station pockets while keeping global policy stable"
            f"{weak_station_roi_clause}"
        ),
        impact_points=max(
            5.0,
            min(20.0, (station_target_rate - float(weak_station[1])) * 180.0 + 4.0),
        ),
        eta_hours=36.0,
        metric_key=f"station_approval_rate_{weak_station[0]}",
        metric_current=float(weak_station[1]),
        metric_target=float(station_target_rate),
        metric_direction="up",
        priority_boost_points=max(
            0.0,
            min(12.0, (station_target_rate - float(weak_station[1])) * 80.0),
        ),
        priority_boost_reason="high_volume_station_pocket",
    )

by_local_hour = (
    attribution.get("by_local_hour")
    if isinstance(attribution.get("by_local_hour"), dict)
    else {}
)
weak_hour: tuple[str, float, int, float | None] | None = None
hour_target_rate: float | None = None
hour_min_intents = max(300, int(max(1, intents_total) * 0.005))
hour_target_floor = max(
    approval_rate_guardrail_min * 0.8,
    station_approval_reference * 0.75,
)
for hour_key, row in sorted(
    by_local_hour.items(),
    key=lambda item: (
        float(item[1].get("approval_rate") or 1.0)
        if isinstance(item[1], dict)
        else 1.0,
        -float(item[1].get("intents_total") or 0.0)
        if isinstance(item[1], dict)
        else 0.0,
    ),
):
    if not isinstance(row, dict):
        continue
    hour_intents = _parse_int(row.get("intents_total")) or 0
    hour_approval = _parse_float(row.get("approval_rate"))
    if hour_intents < hour_min_intents or not isinstance(hour_approval, float):
        continue
    if hour_approval >= max(hour_target_floor, approval_rate * 0.92):
        continue
    weak_hour = (
        _normalize(hour_key),
        hour_approval,
        hour_intents,
        _parse_float(row.get("expected_roi")),
    )
    break
if weak_hour:
    hour_target_rate = min(
        approval_rate_guardrail_max,
        max(
            float(weak_hour[1]) + 0.025,
            hour_target_floor,
        ),
    )
    hour_target_rate = max(hour_target_rate, float(weak_hour[1]) + 0.015)
    hour_target_rate = min(hour_target_rate, approval_rate_guardrail_max)
    weak_hour_roi_clause = (
        f"; expected ROI {weak_hour[3]*100.0:.2f}%"
        if isinstance(weak_hour[3], float)
        else ""
    )
    hour_label = weak_hour[0].zfill(2) if weak_hour[0].isdigit() else weak_hour[0]
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "weak_hour",
        f"Retune local-hour h{hour_label} edge/freshness gates before global changes.",
        target=(
            f"h{hour_label} approval >= {hour_target_rate*100.0:.2f}% "
            f"(now {weak_hour[1]*100.0:.2f}% across {weak_hour[2]:,} intents)"
        ),
        expected_impact=(
            "improves recurring intraday weak pocket without broad policy loosening"
            f"{weak_hour_roi_clause}"
        ),
        impact_points=max(
            5.0,
            min(18.0, (hour_target_rate - float(weak_hour[1])) * 170.0 + 3.0),
        ),
        eta_hours=24.0,
        metric_key=f"hour_approval_rate_{hour_label}",
        metric_current=float(weak_hour[1]),
        metric_target=float(hour_target_rate),
        metric_direction="up",
        priority_boost_points=max(
            0.0,
            min(10.0, (hour_target_rate - float(weak_hour[1])) * 90.0),
        ),
        priority_boost_reason="recurring_local_hour_pocket",
    )

if top_live_readiness_blocker_reason:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "live_readiness_blocker",
        (
            "Close top live-readiness blocker "
            f"{top_live_readiness_blocker_reason.replace('_', ' ')}: "
            f"{top_live_readiness_blocker_action or 'clear this before live promotion.'}"
        ),
        target=f"pilot open checks = 0 (now {pilot_checks_open})",
        expected_impact="directly raises live-readiness confidence score",
        impact_points=pilot_gap_pressure_points,
        eta_hours=168.0,
        metric_key="pilot_checks_open",
        metric_current=float(pilot_checks_open),
        metric_target=0.0,
        metric_direction="down",
    )

if pilot_top_open_reason:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "pilot_gate_open_reason",
        (
            "Pilot checklist priority: close "
            f"{pilot_top_open_reason.replace('_', ' ')} failures first to reduce minimum flips needed "
            "for a controlled live pilot."
        ),
        target=f"minimum flips needed = 0 (now {pilot_minimum_flips_needed})",
        expected_impact="moves 14d/21d horizons toward pilot-pass thresholds",
        impact_points=max(4.0, min(18.0, float(pilot_minimum_flips_needed) * 1.5 + 3.0)),
        eta_hours=168.0,
        metric_key="pilot_minimum_flips_needed",
        metric_current=float(pilot_minimum_flips_needed),
        metric_target=0.0,
        metric_direction="down",
    )

next_signal_name = _normalize(next_signal.get("name"))
next_signal_reason = _normalize(next_signal.get("reason"))
if next_signal_name:
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "next_signal",
        f"Next alpha layer: {next_signal_name.replace('_', ' ')} ({_clip_text(next_signal_reason or 'highest-impact missing signal', 96)}).",
        target="higher resolved unique market-side breadth with stable quality",
        expected_impact="largest expected step-up in independent opportunity coverage",
        impact_points=16.0,
        eta_hours=240.0,
    )

weekly_reason_human = _normalize(weekly_top.get("reason_human"))
weekly_reason_key = weekly_top_reason_key
weekly_reason_count = _parse_int(weekly_top.get("count")) or 0
weekly_action = _normalize(weekly_top.get("recommended_action"))
if weekly_reason_human and weekly_reason_count > 0:
    weekly_reduction_target = max(0, int(round(weekly_reason_count * 0.70)))
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "weekly_blocker_close",
        f"Weekly blocker priority: {_clip_text(weekly_reason_human, 60)} ({weekly_reason_count:,}) — {_clip_text(weekly_action or 'close this before changing global thresholds.', 88)}",
        target=f"reduce this blocker to <= {weekly_reduction_target:,} over next rolling week",
        expected_impact="frees blocked flow without broad threshold risk",
        impact_points=max(4.0, min(20.0, float(weekly_reason_count) / 4000.0)),
        eta_hours=168.0,
        metric_key="weekly_top_blocker_count",
        metric_current=float(weekly_reason_count),
        metric_target=float(weekly_reduction_target),
        metric_direction="down",
    )

if approval_guardrail_evaluated and approval_guardrail_status in {"above_band", "critical_high"}:
    recommendation_profile = (
        approval_guardrail_recommendation.get("recommended_profile")
        if isinstance(approval_guardrail_recommendation.get("recommended_profile"), dict)
        else {}
    )
    recommendation_target_text = ""
    if recommendation_profile:
        rec_alpha = _parse_float(recommendation_profile.get("min_alpha_strength"))
        rec_prob = _parse_float(recommendation_profile.get("min_probability_confidence"))
        rec_edge = _parse_float(recommendation_profile.get("min_expected_edge_net"))
        rec_rate = _parse_float(recommendation_profile.get("projected_approval_rate"))
        if all(isinstance(value, float) for value in (rec_alpha, rec_prob, rec_edge)):
            recommendation_target_text = (
                f"use alpha>={rec_alpha:.3f}, prob>={rec_prob:.3f}, edge>={rec_edge:.4f}"
            )
            if isinstance(rec_rate, float):
                recommendation_target_text += f" (projected approval {rec_rate*100.0:.2f}%)"
    approval_flood_excess_ratio = max(
        0.0,
        (guardrail_approval_rate - approval_rate_guardrail_max) / max(1e-9, approval_rate_guardrail_max),
    )
    approval_flood_priority_boost = min(24.0, 4.0 + approval_flood_excess_ratio * 18.0)
    approval_flood_priority_reason = "approval_guardrail_above_band"
    if approval_guardrail_status == "critical_high":
        approval_flood_priority_boost = min(30.0, approval_flood_priority_boost + 6.0)
        approval_flood_priority_reason = "approval_guardrail_critical_high"
    if guardrail_basis == "window":
        approval_flood_priority_boost = min(30.0, approval_flood_priority_boost + 2.0)
        approval_flood_priority_reason += "+window_basis"
    if bool(latest_guardrail_sample_eligible):
        approval_flood_priority_boost = min(30.0, approval_flood_priority_boost + 1.0)
        approval_flood_priority_reason += "+sample_eligible"
    if edge_gate_blocked_dominant:
        approval_flood_priority_boost = min(30.0, approval_flood_priority_boost + 4.0)
        approval_flood_priority_reason += "+edge_gate_dominant"
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "approval_flood_guard",
        "Tighten approval quality gates to keep throughput in the target band.",
        target=(
            f"approval rate <= {approval_rate_guardrail_max*100.0:.2f}% "
            f"(now {guardrail_approval_rate*100.0:.2f}% on {guardrail_basis})"
        ),
        expected_impact=(
            "reduces over-approval risk and preserves higher-conviction selections"
            + ("; edge gates dominate blocked flow, so raise precision before adding volume" if edge_gate_blocked_dominant else "")
            + (f"; {recommendation_target_text}" if recommendation_target_text else "")
        ),
        impact_points=max(6.0, min(26.0, (guardrail_approval_rate - approval_rate_guardrail_max) * 180.0 + 6.0)),
        eta_hours=12.0,
        metric_key=f"approval_rate_{guardrail_basis}",
        metric_current=guardrail_approval_rate,
        metric_target=approval_rate_guardrail_max,
        metric_direction="down",
        priority_boost_points=approval_flood_priority_boost,
        priority_boost_reason=approval_flood_priority_reason,
    )

if approval_guardrail_evaluated and approval_guardrail_status == "below_band":
    recommendation_profile = (
        approval_guardrail_recommendation.get("recommended_profile")
        if isinstance(approval_guardrail_recommendation.get("recommended_profile"), dict)
        else {}
    )
    recommendation_target_text = ""
    if recommendation_profile:
        rec_alpha = _parse_float(recommendation_profile.get("min_alpha_strength"))
        rec_prob = _parse_float(recommendation_profile.get("min_probability_confidence"))
        rec_edge = _parse_float(recommendation_profile.get("min_expected_edge_net"))
        rec_rate = _parse_float(recommendation_profile.get("projected_approval_rate"))
        if all(isinstance(value, float) for value in (rec_alpha, rec_prob, rec_edge)):
            recommendation_target_text = (
                f"use alpha>={rec_alpha:.3f}, prob>={rec_prob:.3f}, edge>={rec_edge:.4f}"
            )
            if isinstance(rec_rate, float):
                recommendation_target_text += f" (projected approval {rec_rate*100.0:.2f}%)"
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        "approval_starvation_guard",
        "Recover selective throughput by tuning bottleneck stations/hours before loosening global gates.",
        target=(
            f"approval rate >= {approval_rate_guardrail_min*100.0:.2f}% "
            f"(now {guardrail_approval_rate*100.0:.2f}% on {guardrail_basis})"
        ),
        expected_impact=(
            "reduces starvation risk while preserving quality discipline"
            + (f"; {recommendation_target_text}" if recommendation_target_text else "")
        ),
        impact_points=max(5.0, min(20.0, (approval_rate_guardrail_min - guardrail_approval_rate) * 220.0 + 4.0)),
        eta_hours=24.0,
        metric_key=f"approval_rate_{guardrail_basis}",
        metric_current=guardrail_approval_rate,
        metric_target=approval_rate_guardrail_min,
        metric_direction="up",
    )

fallbacks = [
    (
        "Review top non-approved reasons weekly and close the largest blocker before changing global thresholds.",
        "weekly largest blocker count trending down week-over-week",
        "keeps tuning data-backed instead of reactive",
    ),
    (
        "Use unique market-side outcomes (not row counts) as the default quality headline.",
        "row-based metrics only for audit, never as headline",
        "prevents false confidence from repeated entries",
    ),
    (
        "Only move to controlled live after repeated windows beat HYSA after slippage with enough breadth.",
        "positive excess return vs HYSA with sufficient independent breadth",
        "reduces risk of promoting on noisy shadow signals",
    ),
]
for idx, (text, target, expected_impact) in enumerate(fallbacks):
    _add_suggestion(
        suggestions,
        suggestion_rows,
        suggestion_keys,
        f"fallback_{idx}",
        text,
        target=target,
        expected_impact=expected_impact,
        impact_points=3.0,
        eta_hours=240.0,
    )
    if len(suggestions) >= suggestion_count:
        break
suggestion_rows_ranked_initial = sorted(
    suggestion_rows,
    key=lambda row: (
        -float(_parse_float(row.get("priority_score")) or 0.0),
        -float(_parse_float(row.get("impact_points")) or 0.0),
        float(_parse_float(row.get("eta_hours")) or 9999.0),
        _normalize(row.get("key")),
    ),
)
previous_suggestion_rows = (
    previous_alpha_summary.get("suggestions_structured_ranked_all")
    if isinstance(previous_alpha_summary.get("suggestions_structured_ranked_all"), list)
    else previous_alpha_summary.get("suggestions_structured")
)
if not isinstance(previous_suggestion_rows, list):
    previous_suggestion_rows = []
previous_suggestion_by_key: dict[str, dict[str, Any]] = {}
for prev_row in previous_suggestion_rows:
    if not isinstance(prev_row, dict):
        continue
    prev_key = _normalize(prev_row.get("key"))
    if not prev_key:
        continue
    previous_suggestion_by_key[prev_key] = prev_row


def _suggestion_gap_to_target(row: dict[str, Any]) -> float | None:
    if not isinstance(row, dict):
        return None
    direction = _normalize(row.get("metric_direction")).lower()
    if direction not in {"up", "down"}:
        return None
    current_value = _parse_float(row.get("metric_current"))
    target_value = _parse_float(row.get("metric_target"))
    if not isinstance(current_value, float) or not isinstance(target_value, float):
        return None
    if direction == "down":
        return max(0.0, current_value - target_value)
    return max(0.0, target_value - current_value)


tracking_counts = {
    "improving": 0,
    "stalled": 0,
    "regressing": 0,
    "closed": 0,
    "new": 0,
    "unknown": 0,
}
top_improving: tuple[str, float] | None = None
top_regressing: tuple[str, float] | None = None
tracking_escalated_count = 0
tracking_escalated_keys: list[str] = []
tracking_measured_delta_count = 0
tracking_measured_delta_regressing_count = 0
tracking_top_regressing_key = ""
tracking_top_regressing_points = 0.0
for row in suggestion_rows_ranked_initial:
    if not isinstance(row, dict):
        continue
    key = _normalize(row.get("key"))
    previous_row = previous_suggestion_by_key.get(key, {})
    current_gap = _suggestion_gap_to_target(row)
    previous_gap = _parse_float(previous_row.get("gap_to_target"))
    gap_delta = None
    tracking_status = "unknown"
    tracking_trend = "unknown"
    closed = None
    previous_regression_streak = _parse_int(previous_row.get("regression_streak")) or 0
    regression_streak = 0
    if isinstance(current_gap, float):
        closed = current_gap <= 0.0
        tracking_status = "closed" if closed else "open"
        if isinstance(previous_gap, float):
            gap_delta = previous_gap - current_gap
            threshold = max(1e-6, 0.01 * max(1.0, abs(previous_gap)))
            if closed:
                tracking_trend = "closed"
            elif current_gap < previous_gap - threshold:
                tracking_trend = "improving"
            elif current_gap > previous_gap + threshold:
                tracking_trend = "regressing"
            else:
                tracking_trend = "stalled"
        else:
            tracking_trend = "new"
    if tracking_trend == "regressing" and tracking_status == "open":
        regression_streak = max(1, int(previous_regression_streak) + 1)
    measurable_delta = bool(isinstance(current_gap, float) and isinstance(previous_gap, float))
    measured_delta_priority_points = 0.0
    if measurable_delta:
        tracking_measured_delta_count += 1
        if tracking_trend == "regressing" and isinstance(gap_delta, float):
            tracking_measured_delta_regressing_count += 1
            regression_mag = abs(gap_delta)
            measured_delta_priority_points = 4.0 + min(8.0, regression_mag * 120.0)
            if isinstance(previous_gap, float) and previous_gap > 1e-6:
                measured_delta_priority_points += min(6.0, (regression_mag / previous_gap) * 10.0)
            if regression_streak >= 2:
                measured_delta_priority_points += 2.0
            measured_delta_priority_points = min(24.0, measured_delta_priority_points)
            if measured_delta_priority_points > tracking_top_regressing_points:
                tracking_top_regressing_points = measured_delta_priority_points
                tracking_top_regressing_key = key
        elif tracking_trend == "stalled":
            measured_delta_priority_points = 1.5
        elif tracking_trend in {"improving", "closed"} and isinstance(gap_delta, float):
            measured_delta_priority_points = min(3.0, max(0.0, gap_delta) * 60.0)
    priority_score_value = float(_parse_float(row.get("priority_score")) or 0.0)
    priority_score_effective = priority_score_value + (measured_delta_priority_points / 24.0)
    escalated = regression_streak >= 2
    if escalated:
        tracking_escalated_count += 1
        if key:
            tracking_escalated_keys.append(key)
    if tracking_trend not in tracking_counts:
        tracking_trend = "unknown"
    tracking_counts[tracking_trend] = int(tracking_counts.get(tracking_trend, 0)) + 1
    if isinstance(gap_delta, float):
        if gap_delta > 0 and tracking_trend in {"improving", "closed"}:
            if top_improving is None or gap_delta > top_improving[1]:
                top_improving = (key, gap_delta)
        elif gap_delta < 0 and tracking_trend == "regressing":
            regression_mag = abs(gap_delta)
            if top_regressing is None or regression_mag > top_regressing[1]:
                top_regressing = (key, regression_mag)
    row["gap_to_target"] = round(current_gap, 6) if isinstance(current_gap, float) else None
    row["previous_gap_to_target"] = (
        round(previous_gap, 6) if isinstance(previous_gap, float) else None
    )
    row["gap_delta"] = round(gap_delta, 6) if isinstance(gap_delta, float) else None
    row["closed"] = closed
    row["tracking_status"] = tracking_status
    row["tracking_trend"] = tracking_trend
    row["regression_streak"] = int(regression_streak)
    row["escalated"] = bool(escalated)
    row["escalation_reason"] = "regression_streak>=2" if escalated else None
    row["measurable_12h_delta"] = bool(measurable_delta)
    row["measured_delta_priority_points"] = round(float(measured_delta_priority_points), 6)
    row["priority_score_effective"] = round(float(priority_score_effective), 6)

suggestion_rows_ranked = sorted(
    suggestion_rows_ranked_initial,
    key=lambda row: (
        -int(bool(row.get("escalated"))),
        -float(_parse_float(row.get("priority_score_effective")) or _parse_float(row.get("priority_score")) or 0.0),
        -float(_parse_float(row.get("measured_delta_priority_points")) or 0.0),
        -float(_parse_float(row.get("priority_score")) or 0.0),
        -float(_parse_float(row.get("impact_points")) or 0.0),
        float(_parse_float(row.get("eta_hours")) or 9999.0),
        _normalize(row.get("key")),
    ),
)

tracking_total = len(suggestion_rows_ranked)
suggestion_tracking_summary = {
    "total_tracked": tracking_total,
    "counts": {key: int(value) for key, value in tracking_counts.items()},
    "top_improving_key": top_improving[0] if top_improving else None,
    "top_improving_delta": round(top_improving[1], 6) if top_improving else None,
    "top_regressing_key": top_regressing[0] if top_regressing else None,
    "top_regressing_delta": round(top_regressing[1], 6) if top_regressing else None,
    "escalated_count": int(tracking_escalated_count),
    "escalated_keys": tracking_escalated_keys,
    "measured_delta_count": int(tracking_measured_delta_count),
    "measured_delta_regressing_count": int(tracking_measured_delta_regressing_count),
    "top_measured_delta_regressing_key": tracking_top_regressing_key or None,
    "top_measured_delta_regressing_points": (
        round(float(tracking_top_regressing_points), 6)
        if tracking_top_regressing_points > 0
        else None
    ),
    "display_changed_only": True,
}

impact_pool_dollars = None
impact_pool_raw_dollars = None
impact_pool_basis = "unavailable"
impact_pool_proxy_discount_applied = False
impact_pool_proxy_discount_factor_effective = 1.0
if has_settled_predictions and isinstance(projected_pnl_dollars, float) and abs(projected_pnl_dollars) > 0:
    impact_pool_raw_dollars = abs(projected_pnl_dollars)
    impact_pool_dollars = impact_pool_raw_dollars
    impact_pool_basis = "settled_projection"
elif isinstance(expected_edge_total_breadth_normalized, float) and expected_edge_total_breadth_normalized > 0:
    impact_pool_raw_dollars = float(expected_edge_total_breadth_normalized)
    impact_pool_proxy_discount_factor_effective = float(expected_edge_proxy_discount_factor)
    impact_pool_dollars = impact_pool_raw_dollars * impact_pool_proxy_discount_factor_effective
    impact_pool_proxy_discount_applied = (
        impact_pool_proxy_discount_factor_effective < 0.999999
    )
    impact_pool_basis = "expected_edge_proxy_breadth_normalized_discounted"
elif isinstance(expected_edge_total, float) and expected_edge_total > 0:
    impact_pool_raw_dollars = float(expected_edge_total)
    impact_pool_proxy_discount_factor_effective = float(expected_edge_proxy_discount_factor)
    impact_pool_dollars = impact_pool_raw_dollars * impact_pool_proxy_discount_factor_effective
    impact_pool_proxy_discount_applied = (
        impact_pool_proxy_discount_factor_effective < 0.999999
    )
    impact_pool_basis = "expected_edge_proxy_row_audit_discounted"

impact_pool_basis_label_map = {
    "settled_projection": "settled projection",
    "expected_edge_proxy_breadth_normalized_discounted": "planned-order estimate (breadth-normalized, evidence-discounted)",
    "expected_edge_proxy_breadth_normalized": "planned-order estimate (breadth-normalized)",
    "expected_edge_proxy_row_audit_discounted": "planned-order estimate (row-audit, evidence-discounted)",
    "expected_edge_proxy_row_audit": "planned-order estimate (row-audit)",
    "unavailable": "unavailable",
}
impact_pool_basis_label_compact_map = {
    "settled_projection": "settled projection",
    "expected_edge_proxy_breadth_normalized_discounted": "planned-order estimate breadth (discounted)",
    "expected_edge_proxy_breadth_normalized": "planned-order estimate breadth",
    "expected_edge_proxy_row_audit_discounted": "planned-order estimate (discounted)",
    "expected_edge_proxy_row_audit": "planned-order estimate",
    "unavailable": "unavailable",
}
impact_pool_basis_label = impact_pool_basis_label_map.get(
    impact_pool_basis,
    impact_pool_basis.replace("_", " "),
)
impact_pool_basis_label_compact = impact_pool_basis_label_compact_map.get(
    impact_pool_basis,
    impact_pool_basis.replace("_", " "),
)
if isinstance(impact_pool_dollars, float) and impact_pool_dollars > 0:
    suggestion_impact_basis_line = (
        "Suggestion impact basis: "
        f"{impact_pool_basis_label}, pool {_fmt_money(impact_pool_dollars, signed=False)} "
        "(prioritization guidance, not realized PnL)."
    )
    if impact_pool_proxy_discount_applied and isinstance(impact_pool_raw_dollars, float):
        suggestion_impact_basis_line += (
            " "
            f"Applied settled-evidence discount {impact_pool_proxy_discount_factor_effective*100.0:.1f}% "
            f"(raw {_fmt_money(impact_pool_raw_dollars, signed=False)}, "
            f"evidence {settled_evidence_count}/{settled_evidence_full_at})."
        )
else:
    suggestion_impact_basis_line = (
        "Suggestion impact basis: unavailable (no settled projection or positive expected-edge pool yet)."
    )

impact_points_total = sum(
    max(0.0, float(_parse_float(row.get("impact_points")) or 0.0))
    for row in suggestion_rows_ranked
    if isinstance(row, dict)
)
if impact_points_total <= 0 and suggestion_rows_ranked:
    impact_points_total = float(len(suggestion_rows_ranked))

for row in suggestion_rows_ranked:
    if not isinstance(row, dict):
        continue
    points_value = max(0.0, float(_parse_float(row.get("impact_points")) or 0.0))
    if impact_points_total > 0:
        if points_value <= 0:
            impact_weight = 1.0 / float(max(1, len(suggestion_rows_ranked)))
        else:
            impact_weight = points_value / impact_points_total
    else:
        impact_weight = 0.0

    impact_dollars_estimate = None
    if isinstance(impact_pool_dollars, float) and impact_pool_dollars > 0 and impact_weight > 0:
        impact_dollars_estimate = impact_pool_dollars * impact_weight

    has_metric_pair = isinstance(_parse_float(row.get("metric_current")), float) and isinstance(
        _parse_float(row.get("metric_target")), float
    )
    confidence_score_value = 0.40 + min(0.30, points_value / 100.0)
    if has_metric_pair:
        confidence_score_value += 0.10
    if bool(row.get("escalated")):
        confidence_score_value += 0.05
    settled_confidence_adjustment = (-0.08 + (0.20 * float(settled_evidence_strength)))
    confidence_score_value += settled_confidence_adjustment
    proxy_confidence_penalty = 0.0
    if impact_pool_basis.startswith("expected_edge_proxy"):
        proxy_confidence_penalty = 0.08 * (1.0 - float(settled_evidence_strength))
        confidence_score_value -= proxy_confidence_penalty
    confidence_score_value = max(0.10, min(0.95, confidence_score_value))
    confidence_label = "LOW"
    if confidence_score_value >= 0.72:
        confidence_label = "HIGH"
    elif confidence_score_value >= 0.50:
        confidence_label = "MEDIUM"

    confidence_rationale_tokens: list[str] = []
    confidence_rationale_tokens.append("metric" if has_metric_pair else "heuristic")
    confidence_rationale_tokens.append("settled" if has_settled_predictions else "no-settled")
    if settled_evidence_strength < 0.35:
        confidence_rationale_tokens.append("evidence-sparse")
    elif settled_evidence_strength < 0.80:
        confidence_rationale_tokens.append("evidence-building")
    else:
        confidence_rationale_tokens.append("evidence-mature")
    if impact_pool_basis.startswith("expected_edge_proxy"):
        confidence_rationale_tokens.append("proxy")
    elif impact_pool_basis == "settled_projection":
        confidence_rationale_tokens.append("settled-pool")
    if bool(row.get("escalated")):
        confidence_rationale_tokens.append("escalated")
    confidence_rationale_compact = "+".join(confidence_rationale_tokens)
    confidence_rationale_display_map = {
        "metric": "M",
        "heuristic": "H",
        "settled": "ST",
        "no-settled": "NS",
        "proxy": "PX",
        "settled-pool": "SP",
        "escalated": "ESC",
        "evidence-sparse": "ES",
        "evidence-building": "EB",
        "evidence-mature": "EM",
    }
    confidence_rationale_display_tokens = [
        confidence_rationale_display_map.get(token, _clip_text_plain(token.upper(), 4))
        for token in confidence_rationale_tokens
    ]
    confidence_rationale_display = "+".join(
        token for token in confidence_rationale_display_tokens if _normalize(token)
    )

    row["impact_dollars_estimate"] = (
        round(float(impact_dollars_estimate), 6)
        if isinstance(impact_dollars_estimate, float)
        else None
    )
    row["impact_dollars_basis"] = impact_pool_basis if isinstance(impact_dollars_estimate, float) else None
    row["confidence_score"] = round(float(confidence_score_value), 6)
    row["confidence_label"] = confidence_label
    row["confidence_settled_adjustment"] = round(float(settled_confidence_adjustment), 6)
    row["confidence_proxy_penalty"] = (
        round(float(proxy_confidence_penalty), 6)
        if isinstance(proxy_confidence_penalty, float)
        else None
    )
    row["confidence_rationale_tokens"] = confidence_rationale_tokens
    row["confidence_rationale_compact"] = confidence_rationale_compact
    row["confidence_rationale_display"] = confidence_rationale_display

suggestion_rows = suggestion_rows_ranked[:suggestion_count]
if dominant_blocker_share_of_blocked >= 0.80 and suggestion_count > 0:
    dominant_focus_row = next(
        (
            row
            for row in suggestion_rows_ranked
            if _normalize(row.get("key")) == "dominant_blocker_focus"
        ),
        None,
    )
    if isinstance(dominant_focus_row, dict):
        dominant_already_present = any(
            _normalize(row.get("key")) == "dominant_blocker_focus"
            for row in suggestion_rows
            if isinstance(row, dict)
        )
        if not dominant_already_present:
            if suggestion_rows:
                suggestion_rows[-1] = dominant_focus_row
            else:
                suggestion_rows = [dominant_focus_row]

            # Keep keys unique while preserving order.
            deduped_rows: list[dict[str, Any]] = []
            seen_keys: set[str] = set()
            for row in suggestion_rows:
                if not isinstance(row, dict):
                    continue
                row_key = _normalize(row.get("key"))
                if row_key and row_key in seen_keys:
                    continue
                if row_key:
                    seen_keys.add(row_key)
                deduped_rows.append(row)
            suggestion_rows = deduped_rows[:suggestion_count]

if dominant_blocker_share_of_blocked >= 0.80 and len(suggestion_rows) >= 3:
    dominant_index = next(
        (
            idx
            for idx, row in enumerate(suggestion_rows)
            if isinstance(row, dict) and _normalize(row.get("key")) == "dominant_blocker_focus"
        ),
        None,
    )
    if isinstance(dominant_index, int) and dominant_index > 2:
        dominant_row = suggestion_rows.pop(dominant_index)
        suggestion_rows.insert(2, dominant_row)

# When approval is above the configured guardrail band, force the
# flood-control action into top-2 so operator attention remains aligned
# with quality-risk containment.
if (
    approval_guardrail_evaluated
    and approval_guardrail_status in {"above_band", "critical_high"}
    and len(suggestion_rows) >= 2
):
    flood_index = next(
        (
            idx
            for idx, row in enumerate(suggestion_rows)
            if isinstance(row, dict) and _normalize(row.get("key")) == "approval_flood_guard"
        ),
        None,
    )
    if isinstance(flood_index, int) and flood_index > 1:
        flood_row = suggestion_rows.pop(flood_index)
        suggestion_rows.insert(1, flood_row)

# If expected-edge suppression is dominant, surface the worst high-volume
# station/hour tuning pockets in top-3 so operator attention is tied to
# actionable alpha bottlenecks rather than only generic controls.
def _promote_suggestion_key(rows: list[dict[str, Any]], key: str, max_index: int) -> None:
    target_key = _normalize(key)
    if not target_key:
        return
    target_index = next(
        (
            idx
            for idx, row in enumerate(rows)
            if isinstance(row, dict) and _normalize(row.get("key")) == target_key
        ),
        None,
    )
    if isinstance(target_index, int) and target_index > max_index:
        promoted = rows.pop(target_index)
        rows.insert(max_index, promoted)


# If global-only selection pressure is high (especially when rising), keep
# the remediation action near the top so operators do not miss data-coverage
# drift behind aggregate blocker summaries.
if selection_quality_global_only_pressure_active and len(suggestion_rows) >= 3:
    _promote_suggestion_key(
        suggestion_rows,
        "selection_quality_global_only_drift",
        1 if selection_quality_global_only_drift_rising else 2,
    )

if (
    dominant_blocker_reason_key == "expected_edge_below_min"
    and len(suggestion_rows) >= 3
):
    weak_station_gap = (
        float(station_target_rate - float(weak_station[1]))
        if isinstance(weak_station, tuple) and len(weak_station) >= 3 and isinstance(station_target_rate, float)
        else 0.0
    )
    weak_station_is_material = (
        isinstance(weak_station, tuple)
        and len(weak_station) >= 3
        and int(weak_station[2]) >= station_min_intents
        and weak_station_gap >= 0.04
    )
    weak_hour_gap = (
        float(hour_target_rate - float(weak_hour[1]))
        if isinstance(weak_hour, tuple) and len(weak_hour) >= 3 and isinstance(hour_target_rate, float)
        else 0.0
    )
    weak_hour_is_material = (
        isinstance(weak_hour, tuple)
        and len(weak_hour) >= 3
        and int(weak_hour[2]) >= hour_min_intents
        and weak_hour_gap >= 0.020
    )
    # When both pockets are materially weak, surface both in top-3 so
    # the operator gets explicit station + hour actions in the same cycle.
    # Top-3 order becomes:
    #   1) dominant blocker
    #   2) weak station
    #   3) weak hour
    if weak_station_is_material and weak_hour_is_material:
        _promote_suggestion_key(suggestion_rows, "weak_station", 1)
        _promote_suggestion_key(suggestion_rows, "weak_hour", 2)
    elif weak_station_is_material:
        _promote_suggestion_key(suggestion_rows, "weak_station", 2)
    elif weak_hour_is_material:
        _promote_suggestion_key(suggestion_rows, "weak_hour", 2)
for rank, row in enumerate(suggestion_rows, start=1):
    row["rank"] = rank
suggestions = [_format_suggestion_line(row) for row in suggestion_rows]
now_utc_dt = datetime.now(timezone.utc)

action_checklist_top: list[dict[str, Any]] = []
for row in suggestion_rows[:3]:
    if not isinstance(row, dict):
        continue
    row_key = _normalize(row.get("key"))
    row_rank = int(_parse_int(row.get("rank")) or 0)
    row_eta = _parse_float(row.get("eta_hours"))
    if not isinstance(row_eta, float):
        row_eta = 72.0
    if row_eta <= 24.0:
        due_hours = 24
    elif row_eta <= 72.0:
        due_hours = 48
    elif row_eta <= 168.0:
        due_hours = 72
    else:
        due_hours = 168
    due_at = now_utc_dt + timedelta(hours=due_hours)
    priority_label = "P1"
    if row_rank == 1:
        priority_label = "P0"
    action_checklist_top.append(
        {
            "rank": row_rank,
            "key": row_key or None,
            "priority": priority_label,
            "owner": _suggestion_owner(row_key),
            "due_in_hours": due_hours,
            "due_at_utc": due_at.isoformat(),
            "metric_key": row.get("metric_key"),
            "metric_current": row.get("metric_current"),
            "metric_target": row.get("metric_target"),
            "metric_direction": row.get("metric_direction"),
            "action": row.get("action"),
            "target": row.get("target"),
            "expected_impact": row.get("expected_impact"),
            "impact_dollars_estimate": row.get("impact_dollars_estimate"),
            "impact_dollars_basis": row.get("impact_dollars_basis"),
            "priority_boost_points": row.get("priority_boost_points"),
            "priority_boost_reason": row.get("priority_boost_reason"),
            "priority_score_base": row.get("priority_score_base"),
            "priority_score": row.get("priority_score"),
            "confidence_score": row.get("confidence_score"),
            "confidence_label": row.get("confidence_label"),
            "confidence_rationale_display": row.get("confidence_rationale_display"),
            "confidence_rationale_compact": row.get("confidence_rationale_compact"),
            "confidence_rationale_tokens": row.get("confidence_rationale_tokens"),
            "file_hint": _suggestion_file_hint(row_key),
            "escalated": bool(row.get("escalated")),
            "escalation_reason": row.get("escalation_reason"),
        }
    )

action_checklist_summary = {
    "count": len(action_checklist_top),
    "owners": sorted({str(item.get("owner") or "") for item in action_checklist_top if _normalize(item.get("owner"))}),
    "next_due_at_utc": min((item.get("due_at_utc") for item in action_checklist_top if _normalize(item.get("due_at_utc"))), default=None),
    "escalated_count": sum(1 for item in action_checklist_top if bool(item.get("escalated"))),
}
top_action_checklist = (
    action_checklist_top[0]
    if action_checklist_top and isinstance(action_checklist_top[0], dict)
    else {}
)
top_action_escalated = bool(top_action_checklist.get("escalated"))

top_blocker_text = "none"
top3_blockers_compact_text = "none"
top_blocker_key = ""
top_blocker_reason = ""
top_blocker_count = 0
top_blocker_share_of_blocked = 0.0
if display_top_blockers:
    blocker_short_labels = {
        "alpha_strength_below_min": "alpha too weak",
        "probability_confidence_below_min": "probability too low",
        "expected_edge_below_min": "expected edge too low",
        "settlement_confidence_below_min": "settlement conf low",
        "edge_to_risk_ratio_below_min": "edge/risk too low",
        "historical_quality_global_only_pressure": "global-only pressure hard block",
        "historical_expectancy_hard_block": "historical expectancy block",
        "metar_observation_stale": "weather data stale",
        "metar_freshness_boundary_quality_insufficient": "weather near-stale quality",
        "inside_cutoff_window": "inside cutoff window",
        "range_still_possible": "range still possible",
        "underlying_exposure_cap_reached": "underlying cap reached",
        "max_intents_per_underlying_reached": "max intents per underlying",
        "replan_cooldown_blocked_market_side": "replan cooldown blocked",
    }
    blocker_compact_labels = {
        "alpha_strength_below_min": "alpha low",
        "probability_confidence_below_min": "prob low",
        "expected_edge_below_min": "edge low",
        "settlement_confidence_below_min": "settlement low",
        "edge_to_risk_ratio_below_min": "edge/risk low",
        "historical_quality_global_only_pressure": "global-only block",
        "historical_expectancy_hard_block": "history block",
        "metar_observation_stale": "stale",
        "metar_freshness_boundary_quality_insufficient": "near-stale quality",
        "inside_cutoff_window": "cutoff",
        "range_still_possible": "range possible",
        "underlying_exposure_cap_reached": "underlying cap",
        "max_intents_per_underlying_reached": "underlying max intents",
        "replan_cooldown_blocked_market_side": "replan cooldown",
    }
    blocker_denominator = max(1, int(blocked_total_from_counts))
    first_reason, first_count = display_top_blockers[0]
    top_blocker_key = _normalize(first_reason).lower()
    top_blocker_reason = blocker_short_labels.get(str(first_reason), _humanize_reason(first_reason))
    top_blocker_count = int(first_count)
    top_blocker_share_of_blocked = float(top_blocker_count) / float(blocker_denominator)
    pairs = []
    compact_pairs = []
    for k, v in display_top_blockers[:3]:
        blocker_label = blocker_short_labels.get(str(k), _humanize_reason(k))
        blocker_label_compact = blocker_compact_labels.get(
            str(k), blocker_short_labels.get(str(k), _humanize_reason(k))
        )
        blocker_share = (float(v) / float(blocker_denominator)) * 100.0
        pairs.append(f"{blocker_label}={v:,} ({blocker_share:.1f}% blocked)")
        compact_pairs.append(f"{str(blocker_label_compact)} {blocker_share:.0f}%")
    top_blocker_text = ", ".join(pairs)
    top3_blockers_compact_text = " | ".join(compact_pairs) if compact_pairs else "none"
blocker_concentration_suffix = ""
if top_blocker_count > 0 and top_blocker_share_of_blocked >= 0.80:
    blocker_concentration_suffix = (
        " | dominant blocker "
        f"{_clip_text(top_blocker_reason, 24)} "
        f"({top_blocker_share_of_blocked*100.0:.1f}% blocked)"
    )

approval_above_guardrail = approval_guardrail_status in {"above_band", "critical_high"}
quality_gate_dominant_blockers = {
    "expected_edge_below_min",
    "edge_to_risk_ratio_below_min",
    "probability_confidence_below_min",
    "alpha_strength_below_min",
    "historical_quality_global_only_pressure",
}
quality_gate_blocker_dominant = top_blocker_key in quality_gate_dominant_blockers
approval_audit_mismatch_threshold = max(
    25,
    int(approval_audit_approved_rows * 0.005),
)
approval_audit_mismatch_active = bool(
    isinstance(approval_audit_mismatch_rate, float)
    and approval_audit_mismatch_rate >= 0.02
    and approval_audit_mismatch_rows >= approval_audit_mismatch_threshold
)
approval_revalidation_conflict_active = bool(
    isinstance(approval_audit_revalidation_conflicts_rate, float)
    and approval_audit_revalidation_conflicts_rate >= 0.01
    and approval_audit_revalidation_conflicts >= max(10, int(approval_audit_approved_rows * 0.002))
)
quality_risk_alert_reasons: list[str] = []
if approval_above_guardrail and (quality_gate_blocker_dominant or edge_gate_blocked_dominant):
    quality_risk_alert_reasons.append("quality gates dominate blocked flow")
if approval_above_guardrail and gate_coverage_alert_active:
    quality_risk_alert_reasons.append("approved-row gate coverage below threshold")
if approval_above_guardrail and approval_audit_mismatch_active:
    quality_risk_alert_reasons.append("approved rows disagree with active gate thresholds")
if approval_above_guardrail and approval_revalidation_conflict_active:
    quality_risk_alert_reasons.append("approved rows have revalidation conflicts")
quality_risk_alert_active = bool(quality_risk_alert_reasons)
quality_risk_has_corroborating_signal = bool(
    gate_coverage_alert_active
    or approval_audit_mismatch_active
    or approval_revalidation_conflict_active
)
quality_risk_streak = (
    max(1, int(quality_risk_previous_streak) + 1)
    if quality_risk_alert_active
    else 0
)
quality_risk_red_streak = (
    max(1, int(quality_risk_previous_red_streak) + 1)
    if quality_risk_alert_active and approval_guardrail_status == "critical_high"
    else 0
)
quality_risk_alert_level = "none"
if quality_risk_alert_active:
    quality_risk_alert_level = "yellow"
    if approval_guardrail_status == "critical_high" and (
        quality_risk_streak >= int(quality_risk_streak_red_required)
        or quality_risk_red_streak >= 2
    ):
        quality_risk_alert_level = "red"
    elif (
        quality_risk_streak >= int(quality_risk_streak_red_required)
        and quality_risk_has_corroborating_signal
    ):
        quality_risk_alert_level = "red"
if quality_risk_alert_active:
    runtime_health_status = health_status
    health_reasons.append("approval_quality_risk")
    if quality_risk_alert_level == "red":
        health_reasons.append("approval_quality_risk_persistent")
        health_status = "RED"
    elif health_status != "RED":
        health_status = "YELLOW"
else:
    runtime_health_status = health_status
health_reasons = sorted(set(health_reasons))
if health_status == "GREEN" and health_reasons:
    health_status = "YELLOW"
health_reason_text = ", ".join(_humanize_reason(str(item)) for item in health_reasons if _normalize(item))
if not health_reason_text:
    health_reason_text = "none"

quality_risk_state_next = {
    "status": "ready",
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "active": bool(quality_risk_alert_active),
    "level": quality_risk_alert_level,
    "streak": int(quality_risk_streak),
    "red_streak": int(quality_risk_red_streak),
    "streak_red_required": int(quality_risk_streak_red_required),
    "guardrail_status": approval_guardrail_status,
    "guardrail_rate": round(float(guardrail_approval_rate), 6),
    "guardrail_max_rate": round(float(approval_rate_guardrail_max), 6),
    "edge_gate_blocked_share_of_blocked": round(float(edge_gate_blocked_share_of_blocked), 6),
    "edge_gate_blocked_dominant": bool(edge_gate_blocked_dominant),
    "reasons": list(quality_risk_alert_reasons),
    "state_file": str(quality_risk_state_path),
}
quality_risk_state_write_ok = _safe_write_json(quality_risk_state_path, quality_risk_state_next)

selection_gate_components: dict[str, float] = {}
for gate_key, ratio_value in (
    (
        "expected_edge",
        approval_gate_coverage_expected_edge_active_ratio
        if isinstance(approval_gate_coverage_expected_edge_active_ratio, float)
        else approval_gate_coverage_expected_edge_ratio,
    ),
    (
        "probability_confidence",
        approval_gate_coverage_probability_active_ratio
        if isinstance(approval_gate_coverage_probability_active_ratio, float)
        else approval_gate_coverage_probability_ratio,
    ),
    (
        "alpha_strength",
        approval_gate_coverage_alpha_active_ratio
        if isinstance(approval_gate_coverage_alpha_active_ratio, float)
        else approval_gate_coverage_alpha_ratio,
    ),
):
    if isinstance(ratio_value, float):
        selection_gate_components[gate_key] = max(0.0, min(1.0, float(ratio_value)))

selection_gate_component_values = list(selection_gate_components.values())
selection_gate_coverage_basis = "measured"
if selection_gate_component_values:
    selection_gate_coverage_ratio = (
        sum(selection_gate_component_values) / float(len(selection_gate_component_values))
    )
elif intents_total > 0 and approval_audit_approved_rows == 0:
    # No approved rows means active-gate coverage can't be measured directly.
    # Keep confidence interpretable with a conservative fallback rather than
    # collapsing to hard zero.
    selection_gate_coverage_ratio = 0.50
    selection_gate_coverage_basis = "fallback_no_approved_rows"
elif intents_total > 0:
    selection_gate_coverage_ratio = 0.35
    selection_gate_coverage_basis = "fallback_no_gate_data"
else:
    selection_gate_coverage_ratio = 0.0
    selection_gate_coverage_basis = "no_intents"
selection_sample_scale = max(
    0.30,
    min(1.0, float(approval_audit_approved_rows) / 1000.0),
)
selection_guardrail_multiplier = {
    "within_band": 1.00,
    "above_band": 0.82,
    "critical_high": 0.65,
    "below_band": 0.88,
    "insufficient_sample": 0.76,
}.get(approval_guardrail_status, 0.80)
selection_mismatch_penalty = min(
    25.0,
    max(0.0, float(approval_audit_mismatch_rate or 0.0)) * 1250.0,
)
selection_revalidation_penalty = min(
    20.0,
    max(0.0, float(approval_audit_revalidation_conflicts_rate or 0.0)) * 1000.0,
)
selection_no_evaluable_penalty = min(
    15.0,
    max(0.0, float(approval_audit_no_evaluable_rate or 0.0)) * 1000.0,
)
selection_risk_penalty = {
    "red": 18.0,
    "yellow": 8.0,
    "none": 0.0,
}.get(quality_risk_alert_level, 4.0 if quality_risk_alert_active else 0.0)
selection_economics_penalty = 0.0
if has_settled_predictions:
    if isinstance(settled_expectancy_per_trade, float) and settled_expectancy_per_trade < 0.0:
        selection_economics_penalty += min(20.0, abs(settled_expectancy_per_trade) * 30.0)
    if isinstance(settled_profit_factor, float) and settled_profit_factor < 1.0:
        selection_economics_penalty += min(15.0, max(0.0, 1.0 - settled_profit_factor) * 20.0)
    if (
        isinstance(settled_payoff_ratio, float)
        and isinstance(prediction_win_rate, float)
        and prediction_win_rate >= 0.70
        and settled_payoff_ratio < 0.20
    ):
        selection_economics_penalty += min(10.0, max(0.0, 0.20 - settled_payoff_ratio) * 50.0)
selection_economics_penalty = min(25.0, max(0.0, selection_economics_penalty))
selection_outcome_multiplier = 1.0
selection_settled_win_rate_lb95 = None
selection_settled_confidence_strength = 0.0
if has_settled_predictions and isinstance(prediction_win_rate, float):
    settled_n = max(1, int(resolved_predictions))
    settled_p = max(0.0, min(1.0, float(prediction_win_rate)))
    # Wilson lower-bound (95%) prevents overconfident selection scores on small samples.
    z_value = 1.959963984540054
    denom = 1.0 + ((z_value * z_value) / float(settled_n))
    center = settled_p + ((z_value * z_value) / (2.0 * float(settled_n)))
    margin = z_value * math.sqrt(
        max(
            0.0,
            (
                (settled_p * (1.0 - settled_p) / float(settled_n))
                + ((z_value * z_value) / (4.0 * float(settled_n * settled_n)))
            ),
        )
    )
    selection_settled_win_rate_lb95 = max(0.0, min(1.0, (center - margin) / denom))
    selection_settled_confidence_strength = max(
        0.25,
        min(1.0, float(settled_evidence_strength)),
    )
    selection_outcome_multiplier = max(
        0.60,
        min(
            1.0,
            (0.72 + (0.28 * float(selection_settled_win_rate_lb95)))
            * (0.78 + (0.22 * float(selection_settled_confidence_strength))),
        ),
    )
selection_confidence_score_raw = (
    100.0
    * selection_gate_coverage_ratio
    * selection_guardrail_multiplier
    * selection_sample_scale
    * selection_outcome_multiplier
)
selection_confidence_penalties_total = (
    selection_mismatch_penalty
    + selection_revalidation_penalty
    + selection_no_evaluable_penalty
    + selection_risk_penalty
    + selection_economics_penalty
)
selection_confidence_score = max(
    0.0,
    min(
        100.0,
        selection_confidence_score_raw
        - selection_confidence_penalties_total,
    ),
)
selection_confidence_band, selection_confidence_guidance = _selection_confidence_profile(
    selection_confidence_score
)
selection_settled_lb95_text = (
    f"{(selection_settled_win_rate_lb95*100.0):.1f}%"
    if isinstance(selection_settled_win_rate_lb95, float)
    else "n/a"
)
selection_confidence_component_summary = (
    "gates="
    f"{selection_gate_coverage_ratio*100.0:.1f}%, "
    "gate_basis="
    f"{selection_gate_coverage_basis}, "
    "guardrail_mult="
    f"{selection_guardrail_multiplier:.2f}, "
    "sample_scale="
    f"{selection_sample_scale:.2f}, "
    "outcome_mult="
    f"{selection_outcome_multiplier:.2f}, "
    "settled_lb95="
    f"{selection_settled_lb95_text}, "
    "settled_strength="
    f"{selection_settled_confidence_strength:.2f}, "
    "penalties="
    f"{selection_confidence_penalties_total:.1f}"
)
selection_driver_candidates: list[tuple[str, float]] = []
if selection_gate_coverage_basis != "measured":
    selection_basis_driver_text = {
        "fallback_no_approved_rows": "gate coverage not directly measured (no approved rows)",
        "fallback_no_gate_data": "gate coverage data unavailable",
        "no_intents": "no intents in-window",
    }.get(selection_gate_coverage_basis, f"gate coverage basis={selection_gate_coverage_basis}")
    selection_gate_basis_points = max(
        5.0,
        (1.0 - max(0.0, min(1.0, float(selection_gate_coverage_ratio)))) * 25.0,
    )
    selection_driver_candidates.append((selection_basis_driver_text, selection_gate_basis_points))
if selection_mismatch_penalty > 0.0:
    selection_driver_candidates.append(
        ("approved rows fail active gate thresholds", float(selection_mismatch_penalty))
    )
if selection_revalidation_penalty > 0.0:
    selection_driver_candidates.append(
        ("approved rows show revalidation conflicts", float(selection_revalidation_penalty))
    )
if selection_no_evaluable_penalty > 0.0:
    selection_driver_candidates.append(
        ("approved rows missing evaluable gate inputs", float(selection_no_evaluable_penalty))
    )
if selection_risk_penalty > 0.0:
    selection_driver_candidates.append(
        ("approval-quality risk alert active", float(selection_risk_penalty))
    )
if selection_outcome_multiplier < 0.98:
    selection_driver_candidates.append(
        (
            "settled-outcome confidence discount",
            max(0.0, (1.0 - float(selection_outcome_multiplier))) * 35.0,
        )
    )
if selection_economics_penalty > 0.0:
    selection_driver_candidates.append(
        ("settled payoff-shape risk", float(selection_economics_penalty))
    )
if stale_rate >= 0.50:
    selection_driver_candidates.append(
        ("weather freshness dominates blocked flow", float(stale_rate) * 20.0)
    )
selection_driver_candidates.sort(key=lambda item: float(item[1]), reverse=True)
selection_confidence_top_drivers = selection_driver_candidates[:3]
selection_confidence_driver_line = ""
if selection_confidence_top_drivers:
    selection_confidence_driver_line = "Selection confidence drivers: " + "; ".join(
        f"{label}: {points:.1f}" for label, points in selection_confidence_top_drivers
    )

repeat_text = f"{repeat_multiplier:.2f}x" if isinstance(repeat_multiplier, float) else "n/a"
top_share_text = f"{(top_market_side_share * 100.0):.2f}%" if isinstance(top_market_side_share, float) else "n/a"
summary_time = now_utc_dt.strftime("%Y-%m-%d %H:%M UTC")
weekly_blocker_text = "none"
if weekly_reason_human and weekly_reason_count > 0:
    weekly_blocker_text = f"{weekly_reason_human}={weekly_reason_count:,}"
settlement_now_text = f"settlement unresolved now {int(current_settlement_unresolved):,}"
if not settlement_pressure_active and settlement_blocked_count > 0:
    settlement_now_text += f" (rolling blocked {settlement_blocked_count:,})"
if settlement_pressure_mode and settlement_pressure_reason:
    settlement_now_text += f" [{_clip_text(settlement_pressure_reason.replace('_', ' '), 42)}]"
live_mode = "SHADOW_ONLY"
if ready_scaled_live:
    live_mode = "SCALED_LIVE_CANDIDATE"
elif ready_small_live:
    live_mode = "SMALL_LIVE_PILOT_CANDIDATE"
live_mode_display = {
    "SHADOW_ONLY": "shadow only (no live orders)",
    "SMALL_LIVE_PILOT_CANDIDATE": "small live pilot candidate",
    "SCALED_LIVE_CANDIDATE": "scaled live candidate",
}.get(live_mode, live_mode.replace("_", " ").lower())

decision_now = "Stay shadow-only."
if ready_scaled_live:
    decision_now = "Scaled live candidate; keep strict risk caps and monitor drift."
elif ready_small_live:
    decision_now = "Tiny live pilot candidate; maintain strict guardrails."
if (
    isinstance(deployment_confidence_cap_reason, str)
    and "negative_projected_bankroll_pnl" in deployment_confidence_cap_reason.split("+")
):
    decision_now = "Stay shadow-only: projected bankroll PnL is negative under current deployment model."
elif (
    isinstance(deployment_confidence_cap_reason, str)
    and "does_not_exceed_hysa_for_window" in deployment_confidence_cap_reason.split("+")
):
    decision_now = "Stay shadow-only: projected bankroll return does not clear HYSA baseline yet."
elif deployment_confidence_cap_reason == "no_settled_independent_outcomes":
    decision_now = (
        "Stay shadow-only until independent settled outcomes accumulate."
    )
confidence_cap_line = ""
if deployment_confidence_cap_applied:
    cap_reason_tokens = [
        token
        for token in str(deployment_confidence_cap_reason or "").split("+")
        if _normalize(token)
    ]
    cap_reason_labels = {
        "no_settled_independent_outcomes": "no settled independent outcomes in this rolling window",
        "negative_projected_bankroll_pnl": "projected bankroll PnL is negative",
        "does_not_exceed_hysa_for_window": "return does not exceed HYSA baseline",
    }
    confidence_cap_reason_label = (
        "; ".join(cap_reason_labels.get(token, token.replace("_", " ")) for token in cap_reason_tokens)
        if cap_reason_tokens
        else "risk cap applied"
    )
    confidence_cap_line = (
        "Confidence cap: "
        f"{confidence_cap_reason_label}; "
        f"uncapped {deployment_confidence_score_uncapped:.1f} -> displayed {deployment_confidence_score:.1f}."
    )
    if _normalize(deployment_confidence_cap_detail):
        confidence_cap_line += f" ({deployment_confidence_cap_detail})"

guardrail_status_line = (
    f"basis {guardrail_basis}: "
    f"{guardrail_approval_rate*100.0:.2f}% over {guardrail_intents_total:,}, "
    f"target {approval_rate_guardrail_min*100.0:.2f}%–{approval_rate_guardrail_max*100.0:.2f}% "
    f"(critical>{approval_rate_guardrail_critical_high*100.0:.2f}%, min intents {approval_guardrail_min_required_intents:,}), status {approval_guardrail_status}, "
    f"basis_reason {guardrail_basis_reason}"
)
approval_guardrail_status_label = {
    "within_band": "OK",
    "above_band": "HIGH",
    "critical_high": "CRITICAL",
    "below_band": "LOW",
    "insufficient_sample": "SMALL_SAMPLE",
}.get(approval_guardrail_status, approval_guardrail_status.replace("_", " ").upper())
approval_guardrail_action = {
    "within_band": "hold thresholds",
    "above_band": "tighten approval gates",
    "critical_high": "tighten immediately",
    "below_band": "loosen only if edge quality holds",
    "insufficient_sample": "hold (sample too small)",
}.get(approval_guardrail_status, "review guardrail state")
if approval_guardrail_status in {"above_band", "critical_high"} and edge_gate_blocked_dominant:
    approval_guardrail_action = "tighten approvals + raise edge/risk floors"

quality_gate_profile_label = "unknown"
quality_gate_source_key = _normalize(quality_gate_source)
if quality_gate_source_key in {"manual_thresholds", "manual"}:
    quality_gate_profile_label = "manual"
elif quality_gate_source_key in {"auto_profile", "auto"}:
    quality_gate_profile_label = "auto"
elif quality_gate_source_key in {"auto_profile+approval_guardrail", "auto_profile_guardrail"}:
    quality_gate_profile_label = "auto+guardrail"
elif quality_gate_source_key == "live_status":
    quality_gate_profile_label = "live"
elif quality_gate_source_key == "derived_from_intents_plan":
    quality_gate_profile_label = "derived"
elif quality_gate_source_key == "live_status+derived_from_intents_plan":
    quality_gate_profile_label = "live+derived"
elif quality_gate_source_key:
    quality_gate_profile_label = _clip_text(_humanize_quality_gate_source(quality_gate_source), 24)

quality_gate_mode_label = "stable"
if quality_gate_escalation_applied:
    quality_gate_mode_label = "tight mode"
elif (
    quality_gate_escalation_status in {"insufficient_sample", "sample_gated", "guardrail_sample_gated"}
    or "approval_guardrail_" in quality_gate_escalation_detail
):
    quality_gate_mode_label = "sample small"
elif quality_gate_escalation_status not in {"", "none"}:
    quality_gate_mode_label = "watch"

quality_threshold_bits = []
if isinstance(quality_gate_min_settlement, float):
    quality_threshold_bits.append(f"settle>={quality_gate_min_settlement:.3f}")
if isinstance(quality_gate_min_probability, float):
    quality_threshold_bits.append(f"prob>={quality_gate_min_probability:.3f}")
if isinstance(quality_gate_min_alpha, float):
    quality_threshold_bits.append(f"alpha>={quality_gate_min_alpha:.3f}")
if isinstance(quality_gate_min_edge, float):
    quality_threshold_bits.append(f"edge>={quality_gate_min_edge:.4f}")
quality_thresholds_compact = ", ".join(quality_threshold_bits) if quality_threshold_bits else "unavailable"
quality_gate_compact_line = (
    f"{quality_thresholds_compact} | profile {quality_gate_profile_label} | "
    f"auto {'on' if quality_gate_auto_applied else 'off'} | {quality_gate_mode_label}"
)
operational_approval_sample_line = (
    f"Operational approval sample: {recent_guardrail_approval_rate*100.0:.2f}% on "
    f"{recent_guardrail_intents_total:,} intents ({recent_guardrail_files_used_count} files)."
    if isinstance(recent_guardrail_approval_rate, float)
    and recent_guardrail_intents_total > 0
    and recent_guardrail_files_used_count > 0
    else "Operational approval sample: unavailable."
)
auto_apply_profile_write_text = "n/a"
if auto_apply_profile_write_attempted:
    auto_apply_profile_write_text = "ok" if auto_apply_profile_write_ok else "failed"
auto_apply_state_write_text = "ok" if auto_apply_state_write_ok else "failed"
auto_apply_stability_reason_text = _clip_text(
    auto_apply_stability_reason.replace("_", " "),
    30,
)
auto_apply_action_text = (
    "applied"
    if auto_apply_applied
    else ("released" if auto_apply_released else auto_apply_apply_reason.replace("_", " "))
)
auto_apply_status_line = (
    f"disabled | guardrail floor {guardrail_basis_min_abs_intents:,} intents | "
    f"stability {'on' if auto_apply_stability_enabled else 'off'} | "
    f"rows {auto_apply_rows_available:,} | action {auto_apply_action_text} | "
    f"state-write {auto_apply_state_write_text}"
    if not auto_apply_enabled
    else (
        f"enabled | "
        f"trigger {auto_apply_trigger} {auto_apply_trigger_streak_current}/{auto_apply_trigger_streak_required} | "
        f"mode {auto_apply_recommendation_mode_effective} | "
        f"approval-streak {auto_apply_breach_streak}/{auto_apply_streak_required} | "
        f"gate-streak {auto_apply_gate_breach_streak}/{gate_coverage_auto_apply_streak_required} | "
        f"quality-risk-streak {auto_apply_quality_risk_streak}/{quality_risk_auto_apply_streak_required} | "
        f"clear {auto_apply_clear_streak}/{auto_apply_release_streak_required} | "
        f"stability {auto_apply_stability_streak}/{auto_apply_stability_windows_required} "
        f"({auto_apply_stability_reason_text}) | "
        f"zero-approved {auto_apply_zero_approved_streak}/{auto_apply_zero_approved_streak_required} "
        f"(min-intents {auto_apply_zero_approved_min_intents}) | "
        f"rows {auto_apply_rows_available}/{auto_apply_required_rows_for_trigger} "
        f"(approval {auto_apply_min_rows}, gate {gate_coverage_auto_apply_min_rows}) | "
        f"candidate {'yes' if auto_apply_candidate_ready else 'no'} | "
        f"action {auto_apply_action_text} | "
        f"profile-write {auto_apply_profile_write_text} | state-write {auto_apply_state_write_text}"
    )
)
if auto_apply_applied and isinstance(auto_applied_profile, dict):
    projected = _parse_float(auto_applied_profile.get("projected_approval_rate"))
    if isinstance(projected, float):
        auto_apply_status_line += f" (projected approval {projected*100.0:.2f}%)"
if auto_apply_enabled and auto_apply_recommendation_mode == "strict":
    auto_apply_status_line += f" [strict max {strict_guardrail_max*100.0:.2f}%]"

quality_gate_status_line = (
    f"source {_humanize_quality_gate_source(quality_gate_source)} | "
    f"auto-applied {'yes' if quality_gate_auto_applied else 'no'} | "
    f"min settlement {quality_gate_min_settlement:.3f} | "
    f"min alpha {quality_gate_min_alpha:.3f} | "
    f"min prob {quality_gate_min_probability:.3f} | "
    f"min edge {quality_gate_min_edge:.4f}"
    if all(
        isinstance(value, float)
        for value in (
            quality_gate_min_settlement,
            quality_gate_min_alpha,
            quality_gate_min_probability,
            quality_gate_min_edge,
        )
    )
    else (
        f"source {_humanize_quality_gate_source(quality_gate_source)} | "
        f"auto-applied {'yes' if quality_gate_auto_applied else 'no'}"
    )
)
quality_gate_guardrail_sample_gated = False
if quality_gate_escalation_applied:
    escalation_status_readable = quality_gate_escalation_status.replace("_", " ")
    multiplier_text = (
        f"x{quality_gate_escalation_multiplier:.2f}"
        if isinstance(quality_gate_escalation_multiplier, float)
        else "x1.00"
    )
    quality_gate_status_line += (
        f" | guardrail escalation {escalation_status_readable} {multiplier_text}"
    )
    detail_summary = _summarize_quality_gate_detail(
        quality_gate_escalation_detail,
        escalation_status=quality_gate_escalation_status,
        escalation_multiplier=quality_gate_escalation_multiplier,
        escalation_sample_approval_rate=quality_gate_escalation_sample_approval_rate,
        escalation_sample_intents_total=quality_gate_escalation_sample_intents_total,
        escalation_min_intents_required=quality_gate_escalation_min_intents_required,
        escalation_min_intents_base=quality_gate_escalation_min_intents_base,
        escalation_basis_min_abs_intents=quality_gate_escalation_basis_min_abs_intents,
        escalation_sample_source=quality_gate_escalation_sample_source,
        escalation_sample_age_seconds=quality_gate_escalation_sample_age_seconds,
        include_status_and_multiplier=False,
    )
    if detail_summary:
        quality_gate_status_line += f" ({detail_summary})"
elif quality_gate_escalation_status not in {"", "none"}:
    detail_summary = _summarize_quality_gate_detail(
        quality_gate_escalation_detail,
        escalation_status=quality_gate_escalation_status,
        escalation_multiplier=quality_gate_escalation_multiplier,
        escalation_sample_approval_rate=quality_gate_escalation_sample_approval_rate,
        escalation_sample_intents_total=quality_gate_escalation_sample_intents_total,
        escalation_min_intents_required=quality_gate_escalation_min_intents_required,
        escalation_min_intents_base=quality_gate_escalation_min_intents_base,
        escalation_basis_min_abs_intents=quality_gate_escalation_basis_min_abs_intents,
        escalation_sample_source=quality_gate_escalation_sample_source,
        escalation_sample_age_seconds=quality_gate_escalation_sample_age_seconds,
        include_status_and_multiplier=False,
    )
    quality_gate_status_line += (
        f" | guardrail watch {quality_gate_escalation_status.replace('_', ' ')} (no threshold change)"
    )
    if detail_summary:
        quality_gate_status_line += f" ({detail_summary})"
elif "approval_guardrail_" in quality_gate_escalation_detail:
    detail_summary = _summarize_quality_gate_detail(
        quality_gate_escalation_detail,
        escalation_status=quality_gate_escalation_status,
        escalation_multiplier=quality_gate_escalation_multiplier,
        escalation_sample_approval_rate=quality_gate_escalation_sample_approval_rate,
        escalation_sample_intents_total=quality_gate_escalation_sample_intents_total,
        escalation_min_intents_required=quality_gate_escalation_min_intents_required,
        escalation_min_intents_base=quality_gate_escalation_min_intents_base,
        escalation_basis_min_abs_intents=quality_gate_escalation_basis_min_abs_intents,
        escalation_sample_source=quality_gate_escalation_sample_source,
        escalation_sample_age_seconds=quality_gate_escalation_sample_age_seconds,
        include_status_and_multiplier=False,
    )
    quality_gate_guardrail_sample_gated = True
    quality_gate_status_line += " | guardrail sample gated (no threshold change)"
    if detail_summary:
        quality_gate_status_line += f" ({detail_summary})"
elif quality_gate_detail:
    detail_summary = _summarize_quality_gate_detail(
        quality_gate_detail,
        escalation_status=quality_gate_escalation_status,
        escalation_multiplier=quality_gate_escalation_multiplier,
        escalation_sample_approval_rate=quality_gate_escalation_sample_approval_rate,
        escalation_sample_intents_total=quality_gate_escalation_sample_intents_total,
        escalation_min_intents_required=quality_gate_escalation_min_intents_required,
        escalation_min_intents_base=quality_gate_escalation_min_intents_base,
        escalation_basis_min_abs_intents=quality_gate_escalation_basis_min_abs_intents,
        escalation_sample_source=quality_gate_escalation_sample_source,
        escalation_sample_age_seconds=quality_gate_escalation_sample_age_seconds,
        include_status_and_multiplier=True,
    )
    if detail_summary:
        quality_gate_status_line += f" | note {detail_summary}"

sparse_hardening_line = (
    f"Sparse hardening: sample {sparse_hardening_sample_intents_total:,} intents ({sparse_hardening_basis_label})"
    + (
        f" ({sparse_hardening_sample_share_of_window*100.0:.2f}% of window)"
        if isinstance(sparse_hardening_sample_share_of_window, float)
        else ""
    )
    + f" | applied {sparse_hardening_applied_count:,}"
    + (
        f" ({sparse_hardening_applied_rate*100.0:.2f}% sample)"
        if isinstance(sparse_hardening_applied_rate, float)
        else ""
    )
    + f" | blocked {sparse_hardening_blocked_count:,}"
    + (
        f" ({sparse_hardening_blocked_share_of_hardened*100.0:.2f}% hardened)"
        if isinstance(sparse_hardening_blocked_share_of_hardened, float)
        else ""
    )
    + f" | approved {sparse_hardening_approved_count:,}"
)
sparse_hardening_detail_parts: list[str] = []
if isinstance(sparse_hardening_probability_raise_avg, float):
    sparse_hardening_detail_parts.append(
        f"prob_raise_avg +{sparse_hardening_probability_raise_avg:.4f}"
    )
if isinstance(sparse_hardening_probability_raise_max, float):
    sparse_hardening_detail_parts.append(
        f"prob_raise_max +{sparse_hardening_probability_raise_max:.4f}"
    )
if isinstance(sparse_hardening_expected_edge_raise_avg, float):
    sparse_hardening_detail_parts.append(
        f"edge_raise_avg +{sparse_hardening_expected_edge_raise_avg:.4f}"
    )
if isinstance(sparse_hardening_expected_edge_raise_max, float):
    sparse_hardening_detail_parts.append(
        f"edge_raise_max +{sparse_hardening_expected_edge_raise_max:.4f}"
    )
if isinstance(sparse_hardening_support_score_avg, float):
    sparse_hardening_detail_parts.append(
        f"support_avg {sparse_hardening_support_score_avg:.3f}"
    )
if isinstance(sparse_hardening_volatility_penalty_avg, float):
    sparse_hardening_detail_parts.append(
        f"vol_penalty_avg {sparse_hardening_volatility_penalty_avg:.3f}"
    )
if isinstance(sparse_hardening_probability_block_share, float):
    sparse_hardening_detail_parts.append(
        f"prob_block_share {sparse_hardening_probability_block_share*100.0:.1f}%"
    )
if isinstance(sparse_hardening_expected_edge_block_share, float):
    sparse_hardening_detail_parts.append(
        f"edge_block_share {sparse_hardening_expected_edge_block_share*100.0:.1f}%"
    )
if isinstance(sparse_hardening_edge_to_risk_block_share, float):
    sparse_hardening_detail_parts.append(
        f"e2r_block_share {sparse_hardening_edge_to_risk_block_share*100.0:.1f}%"
    )
if sparse_hardening_detail_parts:
    sparse_hardening_line += " | " + " | ".join(sparse_hardening_detail_parts)

lines = [
    f"BetBot Alpha Summary ({window_label}, rolling) — {summary_time}",
    (
        f"Health: runtime {runtime_health_status} / alpha {health_status} (issues: {health_reason_text}) | "
        f"Mode: {live_mode_display} | "
        f"Deploy confidence: {deployment_confidence_score:.1f}/100 | "
        f"Selection confidence: {selection_confidence_score:.1f}/100 ({selection_confidence_band})"
    ),
    f"Decision: {decision_now}",
    (
        "Performance basis: shadow-settled counterfactual (not live fills)."
        if has_settled_predictions
        else "Performance basis: shadow planning only (no settled independent outcomes yet)."
    ),
    suggestion_impact_basis_line,
    (
        f"Flow: intents {intents_total:,} | approved {intents_approved:,} ({approval_rate*100.0:.2f}%) | "
        f"planned {planned_orders:,} | gate-qualified {approval_audit_approved_rows:,} "
        f"({(approval_audit_approved_rows / float(max(1, intents_total))) * 100.0:.2f}%) | "
        f"mismatch {approval_audit_mismatch_rows:,}"
        + (
            f" ({approval_audit_mismatch_rate*100.0:.2f}%)"
            if isinstance(approval_audit_mismatch_rate, float)
            else ""
        )
    ),
    operational_approval_sample_line,
    (
        f"Plan conversion: {approval_to_plan_rate*100.0:.2f}% of approved became plans."
        if isinstance(approval_to_plan_rate, float)
        else "Plan conversion: n/a (no approved intents in-window)."
    ),
    (
        "Replan pressure: "
        f"cooldown blocked {replan_blocked_count:,}/{replan_input_count:,} ({replan_blocked_ratio*100.0:.2f}%) | "
        f"repeat-cap blocked {replan_repeat_cap_blocked_count:,}/{replan_repeat_window_input_count:,} "
        f"({replan_repeat_cap_blocked_ratio*100.0:.2f}%) | "
        f"repeat overrides {replan_repeat_cap_override_count:,} ({replan_repeat_cap_override_ratio*100.0:.2f}%) | "
        f"cap {replan_repeat_cap_config}"
        + (
            ""
            if replan_sample_reliable
            else (
                f" | sample below adaptive minimum ({replan_input_count:,} < "
                f"{replan_cooldown_min_input_count:,}; reason {live_replan_adaptive_reason.replace('_', ' ')})"
            )
        )
    ),
    (
        "Approval guardrail: "
        f"{approval_guardrail_status_label} | "
        f"{guardrail_approval_rate*100.0:.2f}% vs "
        f"target {approval_rate_guardrail_min*100.0:.2f}%–{approval_rate_guardrail_max*100.0:.2f}% "
        f"(critical>{approval_rate_guardrail_critical_high*100.0:.2f}%) | "
        f"basis {guardrail_basis.replace('_', ' ')} | action {approval_guardrail_action}"
    ),
    f"Selection thresholds: {quality_gate_compact_line}",
    profitability_guardrail_line,
    sparse_hardening_line,
    (
        "Selection integrity: "
        f"{selection_confidence_band} ({selection_confidence_score:.1f}/100) | "
        f"components {selection_confidence_component_summary}"
    ),
    f"Settled-evidence confidence (1d/7d/14d): {settled_evidence_confidence_panel}",
]
if (
    _normalize(health_status).upper() != "GREEN"
    and _normalize(health_reason_text).lower() not in {"", "none", "n/a", "unknown"}
):
    lines.insert(2, f"Health reason: {_clip_text(health_reason_text, 140)}")
if confidence_cap_line:
    lines.insert(2, confidence_cap_line)
quality_risk_alert_line = ""
if quality_risk_alert_active:
    quality_risk_alert_line = (
        "Quality-risk alert: approvals are above guardrail while "
        + "; ".join(quality_risk_alert_reasons)
        + "; tighten selection thresholds before increasing throughput."
    )
if quality_risk_alert_line:
    lines.append(quality_risk_alert_line)
if not trial_balance_cache_write_ok:
    lines.append(
        "Data reliability alert: trial-balance cache write failed; "
        f"state persistence may drift ({_clip_text(trial_balance_cache_write_error, 96) or 'unknown error'})."
    )
if not intents_reason_totals_consistent:
    lines.append(
        "Data sanity alert: policy-reason totals do not match intents_total; verify window aggregation before acting."
    )
if not blocked_totals_consistent:
    lines.append(
        "Data sanity alert: blocked totals differ between reason counts and flow totals; verify blocker rollups."
    )
if not approval_rate_consistent:
    lines.append(
        "Data sanity alert: approval_rate does not match intents_approved/intents_total; verify rate calculation source."
    )
if not stale_rate_consistent:
    lines.append(
        "Data sanity alert: stale_block_rate does not match stale blocker counts; verify stale-rate aggregation source."
    )
if not projected_pnl_consistent:
    lines.append(
        "Data sanity alert: projected bankroll PnL does not match ROI x bankroll; verify bankroll simulation inputs."
    )
latest_guardrail_context = (
    f"{latest_guardrail_approval_rate*100.0:.2f}% on {latest_guardrail_intents_total:,} intents"
    if isinstance(latest_guardrail_approval_rate, float) and latest_guardrail_intents_total > 0
    else "unavailable"
)
recent_guardrail_context = (
    f"{recent_guardrail_approval_rate*100.0:.2f}% on {recent_guardrail_intents_total:,} intents "
    f"({recent_guardrail_files_used_count} files)"
    if isinstance(recent_guardrail_approval_rate, float)
    and recent_guardrail_intents_total > 0
    and recent_guardrail_files_used_count > 0
    else "unavailable"
)
window_guardrail_context = (
    f"{window_approval_rate*100.0:.2f}% on {window_intents_total:,} intents"
    if window_intents_total > 0
    else "unavailable"
)
guardrail_context_line = (
    "Guardrail context: "
    f"latest {latest_guardrail_context} | recent {recent_guardrail_context} | "
    f"window {window_guardrail_context}"
)
if guardrail_basis_reason and guardrail_basis_reason != "window_default":
    guardrail_context_line += (
        f" | basis reason {_humanize_guardrail_basis_reason(guardrail_basis_reason)}"
    )
lines.append(guardrail_context_line)
guardrail_sample_parts: list[str] = []
if (
    isinstance(quality_gate_escalation_sample_approval_rate, float)
    and isinstance(quality_gate_escalation_sample_intents_total, int)
    and quality_gate_escalation_sample_intents_total > 0
):
    guardrail_sample_parts.append(
        (
            f"{quality_gate_escalation_sample_approval_rate*100.0:.2f}% "
            f"on {quality_gate_escalation_sample_intents_total:,} intents"
        )
    )
elif isinstance(quality_gate_escalation_sample_approval_rate, float):
    guardrail_sample_parts.append(
        f"{quality_gate_escalation_sample_approval_rate*100.0:.2f}% (intents unavailable)"
    )
elif isinstance(quality_gate_escalation_sample_intents_total, int) and quality_gate_escalation_sample_intents_total > 0:
    guardrail_sample_parts.append(f"{quality_gate_escalation_sample_intents_total:,} intents")
else:
    guardrail_sample_parts.append("unavailable")

guardrail_required_value = (
    quality_gate_escalation_min_intents_required
    if isinstance(quality_gate_escalation_min_intents_required, int)
    else approval_guardrail_min_required_intents
)
required_text = f"required >= {guardrail_required_value:,} intents"
if (
    isinstance(quality_gate_escalation_min_intents_base, int)
    and isinstance(quality_gate_escalation_basis_min_abs_intents, int)
):
    required_text += (
        f" (base {quality_gate_escalation_min_intents_base:,}, "
        f"floor {quality_gate_escalation_basis_min_abs_intents:,})"
    )
guardrail_sample_parts.append(required_text)

sample_age_text = _fmt_age_compact(quality_gate_escalation_sample_age_seconds)
if sample_age_text:
    guardrail_sample_parts.append(f"age {sample_age_text}")

sample_source_text = _humanize_guardrail_sample_source(quality_gate_escalation_sample_source)
if sample_source_text and sample_source_text != "unknown":
    guardrail_sample_parts.append(f"source {sample_source_text}")

lines.append("Guardrail sample: " + " | ".join(guardrail_sample_parts))
if window_has_previous:
    lines.append(
        "Window delta vs prior run: "
        f"intents {_fmt_signed_int(window_delta_intents)} | "
        f"approved {_fmt_signed_int(window_delta_approved)} | "
        f"approval {_fmt_signed_percent_points(window_delta_approval_pp)} | "
        f"planned {_fmt_signed_int(window_delta_planned)} | "
        f"stale {_fmt_signed_percent_points(window_delta_stale_pp)} | "
        f"resolved sides {_fmt_signed_int(window_delta_resolved_market_sides)} | "
        f"1d trial PnL Δ {_fmt_money(window_delta_trial_1d_pnl)}"
    )
elif window_previous_file:
    lines.append("Window delta vs prior run: unavailable (prior summary missing metric payload).")
if quality_drift_alert_active:
    lines.append(
        f"Quality drift alert ({quality_drift_alert_level.upper()}): {quality_drift_alert_summary}"
    )
if gate_coverage_alert_active:
    lines.append(
        f"Parameter enforcement alert ({gate_coverage_alert_level.upper()}): {gate_coverage_alert_summary}"
    )
if csv_cache_degraded or csv_cache_path_fallback_reason:
    cache_line = (
        "Data quality: profitability cache "
        f"enabled={csv_cache_enabled} write_access={csv_cache_write_access} "
        f"commit_ok={csv_cache_commit_ok} puts_failed={csv_cache_puts_failed:,}"
    )
    if csv_cache_path_fallback_reason:
        cache_line += f" | fallback={csv_cache_path_fallback_reason}"
    lines.append(cache_line)
# Model-version lineage stays in JSON artifacts; omit from Discord to keep
# alpha summaries concise and suggestion-rich.
if approval_audit_approved_rows > 0:
    lines.append(
        "Approval parameter audit: "
        f"{approval_audit_status.upper()} | "
        f"mismatch {approval_audit_mismatch_rows:,}/{approval_audit_approved_rows:,} "
        f"({_fmt_percent_ratio(approval_audit_mismatch_rate)}) | "
        f"revalidation conflicts {approval_audit_revalidation_conflicts:,} "
        f"({_fmt_percent_ratio(approval_audit_revalidation_conflicts_rate)}) | "
        f"no-evaluable {approval_audit_no_evaluable_rows:,} ({_fmt_percent_ratio(approval_audit_no_evaluable_rate)})"
    )
    lines.append(f"Gate activity share (approved rows): {approval_gate_activity_line}")
freshness_blocker_text = (
    f"freshness total {stale_count:,} ({stale_rate*100.0:.2f}%)"
    f" [hard-stale={metar_observation_stale_count:,}, near-stale quality={metar_freshness_boundary_quality_count:,}]"
)
blocker_mix_inline_text = (
    f"mix top {blocked_mix_top_text}" if blocked_mix_top_text != "none" else "mix top none"
)
lines += [
    (
        "Blockers (actionable): "
        f"{top_blocker_text} | {freshness_blocker_text} | "
        f"{blocker_mix_inline_text} | "
        f"interval-overlap {overlap_total_count:,} ({overlap_rate*100.0:.2f}%)"
        f"{blocker_concentration_suffix}"
    ),
    (
        "Breadth: "
        f"independent settled sides {resolved_market_sides:,}, families {resolved_families:,}, unresolved sides {unresolved_market_sides:,}, "
        f"repeat multiplier {repeat_text}, concentration warning={'yes' if concentration_warning else 'no'} "
        f"(top share {top_share_text})"
    ),
]
if has_settled_predictions:
    lines.append(
        "Projected bankroll PnL model ($" + f"{reference_bankroll:,.0f}" + ", deployment model): "
        f"{_fmt_money(projected_pnl_dollars)} ({_fmt_percent_ratio(projected_roi_ratio)}), "
        f"avg utilization {_fmt_percent_ratio(projected_utilization / 100.0)}"
    )
else:
    lines.append(
        "Projected bankroll PnL model ($" + f"{reference_bankroll:,.0f}" + "): n/a "
        "(need settled independent outcomes; current window has none)."
    )
if has_settled_predictions:
    lines.append(
        "Alpha verdict: "
        f"{resolved_predictions:,} settled independent calls this window; "
        f"simulated-live counterfactual PnL {_fmt_money(settled_counterfactual_pnl)}."
    )
else:
    lines.append(
        "Alpha verdict: no settled independent calls in-window yet; profitability remains unvalidated."
    )
if settled_metrics_source_mismatch:
    mismatch_reason_text = "source mismatch"
    if settled_metrics_source_reason == "resolved_prediction_count_mismatch":
        mismatch_reason_text = "settlement-window semantics mismatch"
    elif settled_metrics_source_reason:
        mismatch_reason_text = settled_metrics_source_reason.replace("_", " ")
    lines.append(
        "Data consistency note: settled counts differed between profitability and bankroll validation; "
        "using bankroll-validation settled metrics in this summary "
        f"({mismatch_reason_text}; profitability {_fmt_int(profitability_resolved_predictions)} vs "
        f"bankroll {_fmt_int(bankroll_resolved_predictions)})."
    )
if economics_sanity_note:
    lines.append(f"Economics sanity: {economics_sanity_note}")
if not settled_count_breakdown_consistent:
    lines.append(
        "Data sanity alert: settled win/loss/push breakdown does not match resolved independent count; "
        "verify settlement aggregation before acting on this summary."
    )
if not settled_vs_breadth_consistent:
    lines.append(
        "Data sanity alert: settled independent count differs from breadth resolved market-side count; "
        "verify source alignment before acting on this summary."
    )

expected_edge_line = "Expected edge proxy (diagnostic, not realized): "
if isinstance(expected_edge_total_breadth_normalized, float):
    expected_edge_line += (
        f"breadth-normalized {_fmt_money(expected_edge_total_breadth_normalized)} "
        f"({_fmt_percent_ratio(expected_edge_ref_ratio_breadth_normalized)} on bankroll)"
    )
    expected_edge_line += f"; row-audit {_fmt_money(expected_edge_total)}"
else:
    expected_edge_line += f"row-audit {_fmt_money(expected_edge_total)} (breadth-normalized n/a)"
if not has_settled_predictions:
    expected_edge_line += "; provisional until settled independent outcomes accumulate."
expected_edge_line += " Row-audit includes repeats (not independent alpha)."
# Keep expected-edge diagnostics in JSON artifacts; omit from Discord to
# preserve space for ranked optimization actions.

if isinstance(trial_start, float) and isinstance(trial_current, float):
    if trial_balance_mode == "cash_constrained":
        executed_ratio_text = (
            _fmt_percent_ratio(trial_cash_constrained_execution_rate)
            if isinstance(trial_cash_constrained_execution_rate, float)
            else "n/a"
        )
        lines.append(
            "Persistent trial balance (stress replay, cash-constrained rows): "
            f"cash {_fmt_money(trial_current, signed=False)} | "
            f"counterfactual PnL {_fmt_money(trial_growth)} | "
            f"win rate since reset {_fmt_percent_ratio(trial_win_rate)} | "
            f"executed {executed_ratio_text} of unconstrained"
        )
    else:
        lines.append(
            f"Persistent trial balance ({trial_balance_mode.replace('_', '-')}): "
            f"{_fmt_money(trial_start, signed=False)} -> {_fmt_money(trial_current, signed=False)} "
            f"(Δ {_fmt_money(trial_growth)} / {_fmt_percent_value(trial_growth_pct)}), "
            f"win rate since reset {_fmt_percent_ratio(trial_win_rate)}"
        )
trial_balance_depleted = bool(
    trial_balance_mode == "cash_constrained"
    and isinstance(trial_current, float)
    and trial_current <= 5.0
)
if trial_balance_depleted:
    lines.append(
        "Trial balance status: stress-replay cash is nearly depleted; "
        "treat this as replay pressure, not deployment-grade bankroll simulation."
    )

trial_pushes_since_reset = (
    int(trial_cash_constrained_pushes)
    if trial_balance_mode == "cash_constrained" and isinstance(trial_cash_constrained_pushes, int)
    else max(0, trial_resolved_since_reset - trial_wins_since_reset - trial_losses_since_reset)
)
trial_cash_constraint_note = ""
if trial_balance_mode == "cash_constrained":
    skipped_text = (
        f" | skipped for cash {trial_cash_constrained_skipped_for_cash:,}"
        if isinstance(trial_cash_constrained_skipped_for_cash, int) and trial_cash_constrained_skipped_for_cash > 0
        else ""
    )
    execution_rate_text = (
        f" | executed {_fmt_percent_ratio(trial_cash_constrained_execution_rate)} of unconstrained"
        if isinstance(trial_cash_constrained_execution_rate, float)
        else ""
    )
    trial_cash_constraint_note = skipped_text + execution_rate_text
elif trial_balance_negative:
    trial_cash_constraint_note = " | cash model unconstrained"
lines.append(
    (
        "Since reset (cash-constrained order instances): "
        if trial_balance_mode == "cash_constrained"
        else "Since reset (order instances): "
    ) +
    f"resolved {trial_resolved_since_reset:,} of {trial_unique_shadow_orders_since_reset:,} unique | "
    f"W {trial_wins_since_reset:,} | L {trial_losses_since_reset:,} | P {trial_pushes_since_reset:,} | "
    f"counterfactual PnL {_fmt_money(trial_cumulative_counterfactual_pnl)}"
    f"{trial_cash_constraint_note}"
)
if (
    trial_balance_mode == "cash_constrained"
    and isinstance(trial_legacy_current, float)
    and isinstance(trial_legacy_cumulative_counterfactual_pnl, float)
):
    lines.append(
        "Since reset (unconstrained reference): "
        f"balance {_fmt_money(trial_legacy_current)} | "
        f"counterfactual PnL {_fmt_money(trial_legacy_cumulative_counterfactual_pnl)}"
    )
lines.append(
    "Order-instance breadth since reset: "
    f"unique orders {trial_unique_shadow_orders_since_reset:,} | "
    f"resolved share {_fmt_percent_ratio(trial_resolved_share_unique_orders)} | "
    f"duplicate rows {trial_duplicate_count_since_reset:,} ({trial_duplicate_rows_ratio_since_reset*100.0:.2f}%)"
)

def _format_window_pnl(label: str) -> str:
    return f"{label} {_fmt_money(trial_window_snapshots.get(label, {}).get('pnl_dollars'))}"

horizon_pnl_parts_compact = [
    _format_window_pnl(label)
    for label in ("1d", "7d", "14d", "21d", "28d", "3mo", "6mo", "1yr")
]
lines.append("Persistent balance horizons (PnL): " + " | ".join(horizon_pnl_parts_compact))
monthly_label = "30d" if "30d" in trial_window_snapshots else "28d"
monthly_snapshot = trial_window_snapshots.get(monthly_label, {})
monthly_pnl = _parse_float(monthly_snapshot.get("pnl_dollars"))
lines.append(
    "Check-in (counterfactual PnL): "
    f"1d {_fmt_money(trial_window_snapshots.get('1d', {}).get('pnl_dollars'))} | "
    f"7d {_fmt_money(trial_window_snapshots.get('7d', {}).get('pnl_dollars'))} | "
    f"{monthly_label} {_fmt_money(monthly_pnl)}"
    + (" | mode stress replay" if trial_balance_mode == "cash_constrained" else "")
)
if isinstance(trial_growth, float) and trial_growth < 0:
    lines.append(
        "Since-reset reality: "
        f"trial balance still down {_fmt_money(abs(trial_growth), signed=False)} from start."
    )
if (
    has_settled_predictions
    and trial_balance_mode == "cash_constrained"
    and isinstance(trial_growth, float)
    and isinstance(projected_pnl_dollars, float)
    and (trial_growth * projected_pnl_dollars) < 0.0
):
    lines.append(
        "Reconciliation note: deployment model is the live proxy; "
        "stress replay is a repeat-entry pressure test."
    )
takeaway_line = ""
if (
    has_settled_predictions
    and isinstance(prediction_win_rate, float)
    and isinstance(settled_counterfactual_pnl, float)
    and isinstance(trial_growth, float)
):
    if prediction_win_rate >= 0.70 and trial_growth < 0:
        takeaway_line = (
            "Takeaway: high hit rate, but payout shape and sizing still keep bankroll below start."
        )
    elif settled_counterfactual_pnl > 0 and trial_growth < 0:
        takeaway_line = (
            "Takeaway: recent settled calls are positive, but since-reset bankroll is still in drawdown."
        )
    elif settled_counterfactual_pnl <= 0 and trial_growth < 0:
        takeaway_line = (
            "Takeaway: edge quality remains weak; keep shadow-only and tighten selection quality."
        )
if takeaway_line:
    lines.append(takeaway_line)
if has_settled_predictions:
    lines.append(
        "Settled quality (unique market-side): "
        f"resolved {_fmt_int(resolved_predictions)} | win rate {_fmt_percent_ratio(prediction_win_rate)}"
    )
    if (
        isinstance(prediction_win_rate, float)
        and isinstance(settled_counterfactual_pnl, float)
        and isinstance(settled_avg_win, float)
        and isinstance(settled_avg_loss, float)
        and prediction_win_rate >= 0.70
        and settled_counterfactual_pnl <= 0
    ):
        lines.append(
            "Risk-shape warning: high win rate but net PnL is non-positive "
            "(losses outweigh typical wins)."
        )
    settled_economics_segments = []
    if isinstance(settled_expectancy_per_trade, float):
        settled_economics_segments.append(f"expectancy/trade {_fmt_money(settled_expectancy_per_trade)}")
    if isinstance(settled_avg_win, float):
        settled_economics_segments.append(f"avg win {_fmt_money(settled_avg_win)}")
    if isinstance(settled_avg_loss, float):
        settled_economics_segments.append(f"avg loss {_fmt_money(settled_avg_loss)}")
    if isinstance(settled_payoff_ratio, float):
        settled_economics_segments.append(f"payoff ratio {settled_payoff_ratio:.2f}x")
    if isinstance(settled_profit_factor, float):
        settled_economics_segments.append(f"profit factor {settled_profit_factor:.2f}")
    if settled_economics_segments:
        lines.append(
            "Settled economics (unique market-side): " + " | ".join(settled_economics_segments)
        )
    lines.append(
        _format_last_outcome(
            "Last settled selection (unique market-side)",
            last_resolved_unique_market_side,
            include_planned_at=False,
            empty_label="unavailable (not provided by settled source)",
        )
    )
    lines.append(
        _format_last_outcome(
            "Last settled selection (unique order instance)",
            last_resolved_unique_shadow_order,
            empty_label="unavailable (not provided by settled source)",
        )
    )
    lines.append(
        "Window settled (simulated-live counterfactual): "
        f"resolved {_fmt_int(resolved_predictions)} | "
        f"wins {_fmt_int(settled_wins)} | losses {_fmt_int(settled_losses)} | pushes {_fmt_int(settled_pushes)} | "
        f"PnL {_fmt_money(settled_counterfactual_pnl)}"
    )
else:
    lines.append(
        "Settlement progress: no settled independent calls in-window yet "
        "(need at least one to validate realized edge)."
    )
if has_settled_predictions:
    lines.append(
        f"Benchmark vs HYSA (window, model projection): {'yes' if beat_hysa else 'no'} | limiting factor: {display_limiting_factor.replace('_', ' ')}"
    )
else:
    lines.append(
        f"Benchmark vs HYSA (window, model projection): pending settled outcomes | limiting factor: {display_limiting_factor.replace('_', ' ')}"
    )
if factor_split_note:
    lines.append(f"Factor split: {factor_split_note}")

if has_settled_predictions and not any(
    line.startswith("Settled quality (unique market-side):")
    for line in lines
):
    lines.append(
        "Settled quality (unique market-side): "
        f"resolved {_fmt_int(resolved_predictions)} | win rate {_fmt_percent_ratio(prediction_win_rate)}"
    )
if has_settled_predictions and not any(
    line.startswith("Settled economics (unique market-side):")
    for line in lines
):
    settled_economics_fallback_bits: list[str] = []
    if isinstance(settled_expectancy_per_trade, float):
        settled_economics_fallback_bits.append(f"expectancy/trade {_fmt_money(settled_expectancy_per_trade)}")
    if isinstance(settled_avg_win, float):
        settled_economics_fallback_bits.append(f"avg win {_fmt_money(settled_avg_win)}")
    if isinstance(settled_avg_loss, float):
        settled_economics_fallback_bits.append(f"avg loss {_fmt_money(settled_avg_loss)}")
    if isinstance(settled_payoff_ratio, float):
        settled_economics_fallback_bits.append(f"payoff ratio {settled_payoff_ratio:.2f}x")
    if isinstance(settled_profit_factor, float):
        settled_economics_fallback_bits.append(f"profit factor {settled_profit_factor:.2f}")
    if settled_economics_fallback_bits:
        lines.append(
            "Settled economics (unique market-side): "
            + " | ".join(settled_economics_fallback_bits)
        )

tracking_counts_line = suggestion_tracking_summary.get("counts", {})
tracking_improving = int(tracking_counts_line.get("improving", 0))
tracking_stalled = int(tracking_counts_line.get("stalled", 0))
tracking_regressing = int(tracking_counts_line.get("regressing", 0))
tracking_closed = int(tracking_counts_line.get("closed", 0))
tracking_new = int(tracking_counts_line.get("new", 0))
tracking_escalated = int(suggestion_tracking_summary.get("escalated_count") or 0)
suggestion_heading = "Next optimization actions:"
checklist_parts: list[str] = []
for item in action_checklist_top:
    try:
        rank_text = str(int(item.get("rank") or 0))
    except Exception:
        rank_text = "?"
    owner_text = _normalize(item.get("owner")) or "bot-policy"
    due_text = _normalize(item.get("due_in_hours")) or "n/a"
    checklist_parts.append(f"#{rank_text}:{owner_text}@{due_text}h")

display_source_rows = [row for row in suggestion_rows_ranked if isinstance(row, dict)]
changed_suggestion_rows = [
    row
    for row in display_source_rows
    if (
        _normalize(row.get("tracking_trend")).lower() in {"improving", "regressing", "new", "closed"}
        or bool(row.get("escalated"))
    )
]
showing_changed_suggestions_only = bool(changed_suggestion_rows)
if showing_changed_suggestions_only:
    display_source_rows = changed_suggestion_rows[:suggestion_count]
    minimum_display_suggestions = min(3, len(suggestion_rows_ranked))
    if len(display_source_rows) < minimum_display_suggestions:
        selected_keys = {
            _normalize(row.get("key"))
            for row in display_source_rows
            if isinstance(row, dict) and _normalize(row.get("key"))
        }
        for row in suggestion_rows_ranked:
            if not isinstance(row, dict):
                continue
            row_key = _normalize(row.get("key"))
            if row_key and row_key in selected_keys:
                continue
            display_source_rows.append(row)
            if row_key:
                selected_keys.add(row_key)
            if len(display_source_rows) >= minimum_display_suggestions:
                break
    display_source_rows = display_source_rows[:suggestion_count]
else:
    display_source_rows = display_source_rows[:suggestion_count]
suppressed_stale_suggestions_count = max(0, len(suggestion_rows) - len(display_source_rows))
suggestions_unchanged = bool(suggestion_rows) and not showing_changed_suggestions_only
display_suggestions = [_format_suggestion_brief(row) for row in display_source_rows]
unchanged_actions_line = ""
if suggestions_unchanged:
    unchanged_actions_line = (
        "Action queue stable: no material metric movement since prior run; carrying forward top actions."
    )
suggestion_tracking_summary["displayed_count"] = int(len(display_source_rows))
suggestion_tracking_summary["suppressed_stale_count"] = int(suppressed_stale_suggestions_count)
suggestion_tracking_summary["suggestions_unchanged"] = bool(suggestions_unchanged)
suggestion_tracking_summary["displayed_keys"] = [
    _normalize(row.get("key"))
    for row in display_source_rows
    if isinstance(row, dict) and _normalize(row.get("key"))
]

shared_suggestion_rationale_line = ""
shared_rationale_code = ""
top3_action_impact_proxy_line = ""
top3_action_impact_proxy = sum(
    float(_parse_float(row.get("impact_dollars_estimate")) or 0.0)
    for row in display_source_rows[:3]
    if isinstance(row, dict)
)
if top3_action_impact_proxy > 0:
    top3_action_impact_proxy_line = (
        "Top-3 action impact estimate: "
        f"{_fmt_money(top3_action_impact_proxy, signed=False)}"
    )
    if isinstance(impact_pool_dollars, float) and impact_pool_dollars > 0:
        top3_pool_share = (top3_action_impact_proxy / impact_pool_dollars) * 100.0
        top3_action_impact_proxy_line += f" ({top3_pool_share:.1f}% of pool)"
    top3_action_impact_proxy_line += "."
elif display_source_rows:
    top3_action_impact_proxy_line = "Top-3 action impact estimate: unavailable."
display_rationale_values = [
    _normalize(row.get("confidence_rationale_display"))
    for row in display_source_rows
    if isinstance(row, dict)
]
display_rationale_values = [value for value in display_rationale_values if value]

def _strip_why_clause(text: str) -> str:
    parts = [part.strip() for part in _normalize(text).split("|") if _normalize(part)]
    parts = [part for part in parts if not part.lower().startswith("why ")]
    return " | ".join(parts)


def _strip_metric_clause(text: str) -> str:
    parts = [part.strip() for part in _normalize(text).split("|") if _normalize(part)]
    if not parts:
        return ""
    action = parts[0]
    keep_parts = [
        part
        for part in parts[1:]
        if part.lower().startswith("impact ")
    ]
    cleaned = " | ".join([action] + keep_parts).strip()
    return cleaned or action

if display_suggestions and len(set(display_rationale_values)) == 1 and len(display_rationale_values) >= 2:
    shared_rationale_code = display_rationale_values[0]
    shared_rationale_tokens = [
        part.strip().upper()
        for part in shared_rationale_code.split("+")
        if _normalize(part)
    ]
    # Skip routine rationale text when ranking evidence is the default
    # shadow regime (metric + no-settled + sparse-evidence + proxy-impact).
    # Keep the explanation only for unusual/decision-relevant conditions.
    baseline_rationale_tokens = {"M", "NS", "ES", "PX"}
    if set(shared_rationale_tokens).issubset(baseline_rationale_tokens):
        shared_rationale_code = ""

    rationale_decode_map = {
        "M": "based on measured metrics",
        "H": "uses heuristic scoring",
        "ST": "uses settled outcome evidence",
        "NS": "no settled outcomes yet",
        "PX": "impact is estimated from planning only",
        "SP": "impact tied to settled impact pool",
        "ESC": "priority escalated by trend",
        "ES": "settled sample is still small",
        "EB": "settled evidence is still building",
        "EM": "settled evidence is mature",
    }
    if shared_rationale_code:
        decoded_tokens = [
            part.strip().upper()
            for part in shared_rationale_code.split("+")
            if _normalize(part)
        ]
        token_set = set(decoded_tokens)
        rationale_bits: list[str] = []
        if "M" in token_set:
            rationale_bits.append("measured blocker impact")
        elif "H" in token_set:
            rationale_bits.append("heuristic impact model")

        if token_set.intersection({"ST", "SP", "EM"}):
            rationale_bits.append("settled-outcome evidence")
        elif token_set.intersection({"NS", "ES", "EB", "PX"}):
            rationale_bits.append("limited settled evidence")

        if "ESC" in token_set:
            rationale_bits.append("escalated by worsening trend")

        if rationale_bits:
            shared_suggestion_rationale_line = (
                "Why these rank high: " + "; ".join(rationale_bits) + "."
            )
        else:
            decoded_parts = []
            for token in decoded_tokens:
                decoded_label = rationale_decode_map.get(token, token.lower().replace("_", " "))
                decoded_parts.append(decoded_label)
            if decoded_parts:
                shared_suggestion_rationale_line = "Why these rank high: " + "; ".join(decoded_parts)
display_suggestions = [_strip_metric_clause(_strip_why_clause(item)) for item in display_suggestions]
if display_suggestions:
    if suggestions_unchanged and unchanged_actions_line:
        lines.append(unchanged_actions_line)
    lines.append(suggestion_heading)
    if shared_suggestion_rationale_line:
        lines.append(shared_suggestion_rationale_line)
    if top3_action_impact_proxy_line:
        lines.append(top3_action_impact_proxy_line)
    for idx, item in enumerate(display_suggestions, start=1):
        lines.append(f"{idx}. {item}")
elif suggestions_unchanged:
    lines.append(unchanged_actions_line)
discord_message = "\n".join(lines)
if len(discord_message) > 1900:
    # Keep Discord payload readable and line-safe. Preserve core alpha lines,
    # then progressively compress lower-value context.
    base_lines = [
        line
        for line in lines
        if line != suggestion_heading
        and line != shared_suggestion_rationale_line
        and line != top3_action_impact_proxy_line
        and not line.startswith(("1. ", "2. ", "3. ", "4. ", "5. "))
    ]

    def _build_message(active_lines: list[str], suggestion_items: list[str]) -> str:
        message_lines = active_lines[:]
        if suggestion_items:
            message_lines.append(suggestion_heading)
            if shared_suggestion_rationale_line:
                message_lines.append(shared_suggestion_rationale_line)
            if top3_action_impact_proxy_line:
                message_lines.append(top3_action_impact_proxy_line)
            for idx, item in enumerate(suggestion_items, start=1):
                message_lines.append(f"{idx}. {item}")
        return "\n".join(message_lines)

    required_suggestion_tiers: list[list[str]] = []
    fallback_suggestion_tiers: list[list[str]] = []
    minimum_suggestion_count = min(3, len(display_suggestions))
    if display_suggestions:
        required_suggestion_tiers.append(display_suggestions[:suggestion_count])
        # Keep at least 3 optimization suggestions whenever possible.
        if minimum_suggestion_count > 0:
            required_suggestion_tiers.append(display_suggestions[:minimum_suggestion_count])
    fallback_suggestion_tiers.append([])

    drop_prefix_order = [
        "Pilot open checks (top):",
        "Pilot gap horizon drivers:",
        "Pilot gate checklist",
        "Top live-readiness blocker:",
        "Confidence cap:",
        "Confidence guidance:",
        "Live readiness horizon score:",
        "Live recommendation:",
        "Weekly blocker audit:",
        "Resolution lag:",
        "Note:",
        "Order reuse since reset:",
        "Last settled selection (unique order instance):",
        "Last settled selection (unique market-side):",
        "Settled shadow outcomes",
    ]
    protected_line_prefixes = (
        "BetBot Alpha Summary",
        "Health:",
        "Decision:",
        "Performance basis:",
        "Confidence cap:",
        "Suggestion impact basis:",
        "Why(all):",
        "Top-3 action impact estimate:",
        "Flow:",
        "Operational approval sample:",
        "Plan conversion:",
        "Replan pressure:",
        "Data reliability alert:",
        "Data sanity alert:",
        "Approval guardrail:",
        "Selection thresholds:",
        "Quality-risk alert:",
        "Window delta vs prior run:",
        "Quality drift alert",
        "Parameter enforcement alert",
        "Approval parameter audit:",
        "Gate coverage (approved all/active):",
        "Gate activity share (approved rows):",
        "Blockers (actionable):",
        "Blocker mix (top):",
        "Breadth:",
        "Projected bankroll PnL",
        "Alpha verdict:",
        "Data consistency note:",
        "Expected edge proxy",
        "Persistent trial balance",
        "Trial balance status:",
        "Since reset (order instances):",
        "Since reset (cash-constrained order instances):",
        "Since reset (unconstrained reference):",
        "Order-instance breadth since reset:",
        "Simulation realism note:",
        "Check-in (counterfactual PnL):",
        "Persistent balance horizons (PnL):",
        "Persistent balance horizons (extended):",
        "Settled quality (unique market-side):",
        "Settled economics (unique market-side):",
        "Window settled (simulated-live counterfactual):",
        "Settlement progress:",
        "Benchmark vs HYSA (window, model projection):",
        "Factor split:",
    )

    def _line_is_protected(line: str) -> bool:
        return any(line.startswith(prefix) for prefix in protected_line_prefixes)

    def _pop_low_priority_line(lines_in: list[str]) -> bool:
        for idx in range(len(lines_in) - 1, -1, -1):
            if _line_is_protected(lines_in[idx]):
                continue
            lines_in.pop(idx)
            return True
        return False

    working_lines = base_lines[:]
    fitted_message = _build_message(
        working_lines,
        required_suggestion_tiers[0] if required_suggestion_tiers else [],
    )
    if len(fitted_message) <= 1900:
        discord_message = fitted_message
    else:
        for drop_prefix in drop_prefix_order:
            # Try to preserve at least 2 suggestions first.
            for suggestion_items in required_suggestion_tiers:
                candidate = _build_message(working_lines, suggestion_items)
                if len(candidate) <= 1900:
                    discord_message = candidate
                    break
            if len(discord_message) <= 1900:
                break

            drop_index = next((idx for idx, line in enumerate(working_lines) if line.startswith(drop_prefix)), None)
            if drop_index is not None:
                working_lines.pop(drop_index)

        if len(discord_message) > 1900:
            for suggestion_items in fallback_suggestion_tiers:
                candidate = _build_message(working_lines, suggestion_items)
                if len(candidate) <= 1900:
                    discord_message = candidate
                    break

        if len(discord_message) > 1900:
            # Last resort: remove trailing lines whole-line only.
            for suggestion_items in required_suggestion_tiers + fallback_suggestion_tiers:
                candidate_lines = working_lines[:]
                while candidate_lines:
                    candidate = _build_message(candidate_lines, suggestion_items)
                    if len(candidate) <= 1900:
                        discord_message = candidate
                        break
                    if not _pop_low_priority_line(candidate_lines):
                        break
                if len(discord_message) <= 1900:
                    break

    if len(discord_message) > 1900:
        hard_lines = discord_message.splitlines()
        while hard_lines and len("\n".join(hard_lines)) > 1900:
            hard_lines.pop()
        discord_message = "\n".join(hard_lines)

# Prefer a minimum of 3 suggestions when space allows.
if len(display_suggestions) >= 3:
    suggestion_line_prefixes = ("1. ", "2. ", "3. ", "4. ", "5. ")
    message_lines_no_suggestions = [
        line
        for line in discord_message.splitlines()
        if line != suggestion_heading
        and line != shared_suggestion_rationale_line
        and line != top3_action_impact_proxy_line
        and not line.startswith(suggestion_line_prefixes)
    ]
    minimum_suggestion_lines_full = [suggestion_heading]
    if shared_suggestion_rationale_line:
        minimum_suggestion_lines_full.append(shared_suggestion_rationale_line)
    if top3_action_impact_proxy_line:
        minimum_suggestion_lines_full.append(top3_action_impact_proxy_line)
    for idx, item in enumerate(display_suggestions[:3], start=1):
        minimum_suggestion_lines_full.append(f"{idx}. {item}")
    minimum_candidate_full = "\n".join(message_lines_no_suggestions + minimum_suggestion_lines_full)
    if len(minimum_candidate_full) <= 1900:
        discord_message = minimum_candidate_full
    else:
        # First, try preserving full suggestion text by trimming lower-priority
        # non-suggestion lines.
        trimmed_lines_full = message_lines_no_suggestions[:]
        minimum_core_lines_full = 9
        while len(trimmed_lines_full) > minimum_core_lines_full:
            candidate_full = "\n".join(trimmed_lines_full + minimum_suggestion_lines_full)
            if len(candidate_full) <= 1900:
                discord_message = candidate_full
                break
            if not _pop_low_priority_line(trimmed_lines_full):
                break
    if len(discord_message) > 1900:
        minimum_suggestion_lines_clipped = [suggestion_heading]
        if shared_suggestion_rationale_line:
            minimum_suggestion_lines_clipped.append(shared_suggestion_rationale_line)
        if top3_action_impact_proxy_line:
            minimum_suggestion_lines_clipped.append(_clip_text(top3_action_impact_proxy_line, 96))
        for idx, item in enumerate(display_suggestions[:3], start=1):
            minimum_suggestion_lines_clipped.append(f"{idx}. {_clip_text(item, 110)}")
        minimum_candidate = "\n".join(message_lines_no_suggestions + minimum_suggestion_lines_clipped)
        if len(minimum_candidate) <= 1900:
            discord_message = minimum_candidate
        else:
            # Hard guarantee: keep 3 suggestions by trimming lower-priority tail
            # lines before dropping suggestions entirely.
            trimmed_lines = message_lines_no_suggestions[:]
            minimum_core_lines = 9
            while len(trimmed_lines) > minimum_core_lines:
                candidate = "\n".join(trimmed_lines + minimum_suggestion_lines_clipped)
                if len(candidate) <= 1900:
                    discord_message = candidate
                    break
                if not _pop_low_priority_line(trimmed_lines):
                    break

discord_message_detailed = discord_message
if has_settled_predictions:
    def _append_required_detailed_line(
        message_text: str,
        required_line: str,
        *,
        required_prefixes: tuple[str, ...],
    ) -> str:
        if any(message_text.splitlines() and line.startswith(required_prefixes) for line in message_text.splitlines()):
            return message_text
        if len(message_text) + len(required_line) + 1 <= 1900:
            return message_text + "\n" + required_line

        lines_local = message_text.splitlines()
        drop_prefixes = (
            "Profitability guardrails:",
            "Selection thresholds:",
            "Plan conversion:",
            "Operational approval sample:",
            "Replan pressure:",
            "Approval guardrail:",
            "Last settled selection (unique order instance):",
            "Last settled selection (unique market-side):",
            "Window delta vs prior run:",
            "Guardrail sample:",
            "Guardrail context:",
            "Selection integrity:",
            "Approval parameter audit:",
            "Gate activity share (approved rows):",
            "Window settled (simulated-live counterfactual):",
            "Benchmark vs HYSA (window, model projection):",
            "Top-3 action impact estimate:",
            "Why these rank high:",
            "5. ",
            "4. ",
        )
        for prefix in drop_prefixes:
            if len("\n".join(lines_local + [required_line])) <= 1900:
                break
            idx = next(
                (
                    i
                    for i, line in enumerate(lines_local)
                    if line.startswith(prefix)
                ),
                None,
            )
            if idx is not None:
                lines_local.pop(idx)

        candidate_text = "\n".join(lines_local + [required_line])
        if len(candidate_text) <= 1900:
            return candidate_text
        return message_text

    projected_roi_text_detailed = (
        _fmt_percent_ratio(projected_roi_ratio)
        if has_settled_predictions
        else "n/a"
    )
    projected_util_text_detailed = (
        _fmt_percent_ratio(projected_utilization / 100.0)
        if has_settled_predictions
        else "n/a"
    )
    projected_required_line = (
        "Projected bankroll PnL model ($"
        + f"{reference_bankroll:,.0f}"
        + ", deployment model): "
        + (
            f"{_fmt_money(projected_pnl_dollars)} ({projected_roi_text_detailed}), "
            f"avg utilization {projected_util_text_detailed}"
            if has_settled_predictions
            else "n/a (need settled independent outcomes; current window has none)."
        )
    )
    discord_message_detailed = _append_required_detailed_line(
        discord_message_detailed,
        projected_required_line,
        required_prefixes=("Projected bankroll PnL model ($",),
    )

    if isinstance(trial_start, float) and isinstance(trial_current, float):
        if trial_balance_mode == "cash_constrained":
            trial_exec_ratio_text = (
                _fmt_percent_ratio(trial_cash_constrained_execution_rate)
                if isinstance(trial_cash_constrained_execution_rate, float)
                else "n/a"
            )
            trial_required_line = (
                "Persistent trial balance (stress replay, cash-constrained rows): "
                f"cash {_fmt_money(trial_current, signed=False)} | "
                f"counterfactual PnL {_fmt_money(trial_growth)} | "
                f"win rate since reset {_fmt_percent_ratio(trial_win_rate)} | "
                f"executed {trial_exec_ratio_text} of unconstrained"
            )
        else:
            trial_required_line = (
                f"Persistent trial balance ({trial_balance_mode.replace('_', '-')}): "
                f"{_fmt_money(trial_start, signed=False)} -> {_fmt_money(trial_current, signed=False)} "
                f"(Δ {_fmt_money(trial_growth)} / {_fmt_percent_value(trial_growth_pct)}), "
                f"win rate since reset {_fmt_percent_ratio(trial_win_rate)}"
            )
        discord_message_detailed = _append_required_detailed_line(
            discord_message_detailed,
            trial_required_line,
            required_prefixes=("Persistent trial balance",),
        )

    checkin_required_line = (
        "Check-in (counterfactual PnL): "
        f"1d {_fmt_money(trial_window_snapshots.get('1d', {}).get('pnl_dollars'))} | "
        f"7d {_fmt_money(trial_window_snapshots.get('7d', {}).get('pnl_dollars'))} | "
        f"{monthly_label} {_fmt_money(monthly_pnl)}"
        + (" | mode stress replay" if trial_balance_mode == "cash_constrained" else "")
    )
    discord_message_detailed = _append_required_detailed_line(
        discord_message_detailed,
        checkin_required_line,
        required_prefixes=("Check-in (counterfactual PnL):",),
    )

    if (
        has_settled_predictions
        and trial_balance_mode == "cash_constrained"
        and isinstance(trial_growth, float)
        and isinstance(projected_pnl_dollars, float)
        and (trial_growth * projected_pnl_dollars) < 0.0
    ):
        discord_message_detailed = _append_required_detailed_line(
            discord_message_detailed,
            "Reconciliation note: deployment model is the live proxy; "
            "stress replay is a repeat-entry pressure test.",
            required_prefixes=("Reconciliation note:",),
        )

    if "Settled quality (unique market-side):" not in discord_message_detailed:
        settled_quality_line = (
            "Settled quality (unique market-side): "
            f"resolved {_fmt_int(resolved_predictions)} | win rate {_fmt_percent_ratio(prediction_win_rate)}"
        )
        discord_message_detailed = _append_required_detailed_line(
            discord_message_detailed,
            settled_quality_line,
            required_prefixes=("Settled quality (unique market-side):",),
        )
    if "Settled economics (unique market-side):" not in discord_message_detailed:
        settled_economics_bits: list[str] = []
        if isinstance(settled_expectancy_per_trade, float):
            settled_economics_bits.append(f"expectancy/trade {_fmt_money(settled_expectancy_per_trade)}")
        if isinstance(settled_avg_win, float):
            settled_economics_bits.append(f"avg win {_fmt_money(settled_avg_win)}")
        if isinstance(settled_avg_loss, float):
            settled_economics_bits.append(f"avg loss {_fmt_money(settled_avg_loss)}")
        if isinstance(settled_payoff_ratio, float):
            settled_economics_bits.append(f"payoff {settled_payoff_ratio:.2f}x")
        if isinstance(settled_profit_factor, float):
            settled_economics_bits.append(f"profit factor {settled_profit_factor:.2f}")
        if settled_economics_bits:
            settled_economics_line = (
                "Settled economics (unique market-side): " + " | ".join(settled_economics_bits)
            )
            discord_message_detailed = _append_required_detailed_line(
                discord_message_detailed,
                settled_economics_line,
                required_prefixes=(
                    "Settled economics (unique market-side):",
                    "Settled economics:",
                ),
            )
    if settled_metrics_source_mismatch and "Data consistency note:" not in discord_message_detailed:
        consistency_required_line = next(
            (line for line in lines if line.startswith("Data consistency note:")),
            "",
        )
        if consistency_required_line:
            discord_message_detailed = _append_required_detailed_line(
                discord_message_detailed,
                consistency_required_line,
                required_prefixes=("Data consistency note:",),
            )
    if "Risk-shape warning:" not in discord_message_detailed:
        risk_required_line = next(
            (line for line in lines if line.startswith("Risk-shape warning:")),
            "",
        )
        if risk_required_line:
            discord_message_detailed = _append_required_detailed_line(
                discord_message_detailed,
                risk_required_line,
                required_prefixes=("Risk-shape warning:",),
            )
concise_prefixes = [
    "BetBot Alpha Summary",
    "Health:",
    "Health reason:",
    "Decision:",
    "Performance basis:",
    "Confidence cap:",
    "Suggestion impact basis:",
    "Flow:",
    "Plan conversion:",
    "Approval guardrail:",
    "Selection thresholds:",
    "Sparse hardening:",
    "Blockers (actionable):",
    "Breadth:",
    "Projected bankroll PnL",
    "Alpha verdict:",
    "Persistent trial balance",
    "Check-in (counterfactual PnL):",
    "Since-reset reality:",
    "Takeaway:",
    "Settled quality (unique market-side):",
    "Risk-shape warning:",
    "Settlement progress:",
    "Benchmark vs HYSA (window, model projection):",
    "Data reliability alert:",
    "Data sanity alert:",
    "Quality-risk alert:",
    "Data consistency note:",
    "Economics sanity:",
    "Trial balance status:",
    "Settled economics (unique market-side):",
    "Last settled selection (unique market-side):",
]
concise_lines: list[str] = []
for prefix in concise_prefixes:
    match = next((line for line in lines if line.startswith(prefix)), "")
    if not match:
        continue
    if prefix == "Health:":
        mode_compact = "shadow-only" if "shadow" in live_mode_display.lower() else "live"
        match = (
            f"Health: {health_status} | {mode_compact} | "
            f"deploy {deployment_confidence_score:.1f}/100 | "
            f"selection {selection_confidence_score:.1f}/100 ({selection_confidence_band.lower()})"
        )
    if prefix == "Decision:":
        if "Stay shadow-only until independent settled outcomes accumulate." in match:
            match = "Decision: shadow-only (awaiting first settled independent outcomes)."
    if prefix == "Health reason:":
        reason_text = _normalize(match.replace("Health reason:", "", 1))
        if reason_text:
            match = "Health reason: " + _clip_text(reason_text, 96)
        else:
            continue
    if prefix == "Flow:":
        match = (
            f"Flow: intents {intents_total:,} | "
            f"approved {intents_approved:,} ({approval_rate*100.0:.2f}%) | "
            f"planned {planned_orders:,}"
        )
    if prefix == "Confidence cap:":
        m = re.search(
            r"Confidence cap:\s*(.+?);\s*uncapped\s*([0-9.]+)\s*->\s*displayed\s*([0-9.]+)\.(?:\s*\(.+\))?",
            match,
        )
        if m:
            reason_text_raw = _normalize(m.group(1))
            reason_aliases = {
                "no settled independent outcomes in this rolling window": "no settled independent outcomes yet",
            }
            reason_text = reason_aliases.get(reason_text_raw.lower(), reason_text_raw)
            reason_text = _clip_text(reason_text, 64)
            uncapped_text = _normalize(m.group(2))
            displayed_text = _normalize(m.group(3))
            match = (
                f"Confidence cap: {reason_text} | {uncapped_text}->{displayed_text}"
            )
    if prefix == "Plan conversion:":
        m = re.search(
            r"Plan conversion:\s*([0-9.]+%)\s+of approved became plans\.",
            match,
        )
        if m:
            match = f"Plan conversion: {m.group(1)} approved -> planned"
    if prefix == "Suggestion impact basis:":
        if has_settled_predictions:
            match = "Impact model: based on settled-window projection (not realized PnL)."
        else:
            match = "Impact model: provisional (insufficient settled outcomes)."
    if prefix == "Replan pressure:":
        m = re.search(
            r"Replan pressure:\s*cooldown blocked [0-9,]+/[0-9,]+ \(([0-9.]+%)\)\s+\|\s+"
            r"repeat-cap blocked [0-9,]+/[0-9,]+ \(([0-9.]+%)\)\s+\|\s+"
            r"repeat overrides ([0-9,]+) \(([0-9.]+%)\)\s+\|\s+cap (.+)$",
            match,
        )
        if m:
            cooldown_ratio = _normalize(m.group(1))
            repeat_ratio = _normalize(m.group(2))
            repeat_overrides = _normalize(m.group(3))
            override_ratio = _normalize(m.group(4))
            repeat_cap_config_text = _normalize(m.group(5))
            match = (
                "Replan pressure: "
                f"cooldown {cooldown_ratio} | repeat-cap {repeat_ratio} | "
                f"overrides {repeat_overrides} ({override_ratio}) | cap {repeat_cap_config_text}"
            )
    if prefix == "Approval guardrail:":
        guardrail_action_compact = _clip_text(approval_guardrail_action, 42)
        guardrail_status_human = {
            "LOW": "low approval regime",
            "HIGH": "high approval regime",
            "CRITICAL": "critical high approval regime",
            "NORMAL": "in target band",
            "INSUFFICIENT_SAMPLE": "insufficient sample",
        }.get(approval_guardrail_status_label, approval_guardrail_status_label.lower())
        match = (
            "Approval guardrail: "
            f"{guardrail_status_human} | "
            f"action {guardrail_action_compact}"
        )
    if prefix == "Selection thresholds:":
        profile_text = _normalize(quality_gate_profile_label) or "n/a"
        auto_text = "auto on" if quality_gate_auto_applied else "auto off"
        mode_text = _normalize(quality_gate_mode_label) or "n/a"
        match = (
            "Selection gate: "
            f"profile {profile_text} | {auto_text} | mode {mode_text}"
        )
    if prefix == "Sparse hardening:":
        applied_rate_text = (
            f"{sparse_hardening_applied_rate*100.0:.1f}%"
            if isinstance(sparse_hardening_applied_rate, float)
            else "n/a"
        )
        blocked_share_hardened_text = (
            f"{sparse_hardening_blocked_share_of_hardened*100.0:.1f}%"
            if isinstance(sparse_hardening_blocked_share_of_hardened, float)
            else "n/a"
        )
        blocked_share_window_text = (
            f"{sparse_hardening_blocked_share_of_blocked*100.0:.1f}%"
            if isinstance(sparse_hardening_blocked_share_of_blocked, float)
            else "n/a"
        )
        sample_share_window_text = (
            f"{sparse_hardening_sample_share_of_window*100.0:.1f}%"
            if isinstance(sparse_hardening_sample_share_of_window, float)
            else "n/a"
        )
        prob_block_text = (
            f"{sparse_hardening_probability_block_share*100.0:.1f}%"
            if isinstance(sparse_hardening_probability_block_share, float)
            else "n/a"
        )
        edge_block_text = (
            f"{sparse_hardening_expected_edge_block_share*100.0:.1f}%"
            if isinstance(sparse_hardening_expected_edge_block_share, float)
            else "n/a"
        )
        e2r_block_text = (
            f"{sparse_hardening_edge_to_risk_block_share*100.0:.1f}%"
            if isinstance(sparse_hardening_edge_to_risk_block_share, float)
            else "n/a"
        )
        match = (
            "Sparse hardening: "
            f"sample {sparse_hardening_sample_intents_total:,} intents ({sparse_hardening_basis_label}, {sample_share_window_text} of window) | "
            f"applied {sparse_hardening_applied_count:,} ({applied_rate_text} sample) | "
            f"blocked {sparse_hardening_blocked_count:,} ({blocked_share_hardened_text} hardened; {blocked_share_window_text} window blocked) | "
            f"block mix prob {prob_block_text} / edge {edge_block_text} / e2r {e2r_block_text}"
        )
    if prefix == "Window delta vs prior run:":
        m = re.search(
            r"approval\s+([+\-]?[0-9.]+pp)\s+\|\s+planned\s+([+\-]?[0-9,]+)\s+\|\s+stale\s+([+\-]?[0-9.]+pp)\s+\|\s+resolved sides\s+([+\-]?[0-9,]+)",
            match,
        )
        if m:
            approval_delta = _normalize(m.group(1))
            planned_delta = _normalize(m.group(2))
            stale_delta = _normalize(m.group(3))
            resolved_sides_delta = _normalize(m.group(4))
            stale_text = "stale flat" if stale_delta in {"0.00pp", "+0.00pp", "-0.00pp"} else f"stale {stale_delta}"
            match = (
                "Trend vs prior: "
                f"approval {approval_delta} | planned {planned_delta} | {stale_text} | "
                f"resolved sides {resolved_sides_delta}"
            )
    if prefix == "Guardrail sample:":
        m = re.search(
            r"Guardrail sample:\s*([0-9.]+% on [0-9,]+ intents)\s*\|\s*required >=\s*([0-9,]+)\s*intents(?:\s*\(base [0-9,]+, floor [0-9,]+\))?\s*\|\s*age\s*([^|]+)",
            match,
        )
        if m:
            sample_text = _normalize(m.group(1))
            required_text = _normalize(m.group(2))
            age_text = _normalize(m.group(3))
            match = f"Guardrail sample: {sample_text} | need >= {required_text} intents | age {age_text}"
    if prefix == "Projected bankroll PnL" and "need settled independent outcomes" in match:
        match = "Projected bankroll PnL model ($1,000): pending settled outcomes."
    if prefix == "Check-in (counterfactual PnL):" and not has_settled_predictions:
        one_day_pnl = _fmt_money(trial_window_snapshots.get("1d", {}).get("pnl_dollars"))
        seven_day_pnl = _fmt_money(trial_window_snapshots.get("7d", {}).get("pnl_dollars"))
        month_window_pnl = _fmt_money(trial_window_snapshots.get(monthly_label, {}).get("pnl_dollars"))
        match = (
            "Check-in (counterfactual PnL): "
            f"1d {one_day_pnl} | "
            f"7d {seven_day_pnl} | "
            f"{monthly_label} {month_window_pnl} | settled n/a"
        )
    if prefix == "Blockers (actionable):":
        top_label = _clip_text(top_blocker_reason or "none", 28)
        match = (
            "Blockers: "
            f"top {top_label} ({top_blocker_share_of_blocked*100.0:.1f}% blocked) | "
            f"freshness {stale_rate*100.0:.2f}% | "
            f"overlap {overlap_rate*100.0:.2f}%"
        )
    if prefix == "Breadth:":
        match = (
            "Breadth: "
            f"settled sides {resolved_market_sides:,} | "
            f"unresolved sides {unresolved_market_sides:,} | "
            f"families {resolved_families:,} | "
            f"concentration {'warn' if concentration_warning else 'ok'}"
        )
    if prefix == "Persistent trial balance":
        if isinstance(trial_start, float) and isinstance(trial_current, float):
            if trial_balance_mode == "cash_constrained":
                exec_ratio_text = (
                    _fmt_percent_ratio(trial_cash_constrained_execution_rate)
                    if isinstance(trial_cash_constrained_execution_rate, float)
                    else "n/a"
                )
                match = (
                    "Scenario balance (stress replay): "
                    f"cash {_fmt_money(trial_current, signed=False)} | "
                    f"PnL {_fmt_money(trial_growth)} | "
                    f"executed {exec_ratio_text} of unconstrained"
                )
            else:
                mode_label = "legacy"
                match = (
                    f"Persistent trial balance ({mode_label}): "
                    f"{_fmt_money(trial_start, signed=False)} -> {_fmt_money(trial_current, signed=False)} | "
                    f"Δ {_fmt_money(trial_growth)} ({_fmt_percent_value(trial_growth_pct)})"
                )
    if prefix in {"Since reset (order instances):", "Since reset (cash-constrained order instances):"}:
        match = (
            "Since reset: "
            f"resolved {trial_resolved_since_reset:,}/{trial_unique_shadow_orders_since_reset:,} | "
            f"PnL {_fmt_money(trial_cumulative_counterfactual_pnl)} | "
            f"win rate {_fmt_percent_ratio(trial_win_rate)}"
        )
        if trial_balance_mode == "cash_constrained":
            cash_bits = []
            if isinstance(trial_cash_constrained_skipped_for_cash, int):
                cash_bits.append(f"skipped for cash {trial_cash_constrained_skipped_for_cash:,}")
            if isinstance(trial_cash_constrained_execution_rate, float):
                cash_bits.append(
                    f"executed {_fmt_percent_ratio(trial_cash_constrained_execution_rate)} of unconstrained reference"
                )
            if cash_bits:
                match += " | " + " | ".join(cash_bits)
    if prefix == "Since reset (unconstrained reference):":
        if isinstance(trial_legacy_current, float) and isinstance(trial_legacy_cumulative_counterfactual_pnl, float):
            match = (
                "Unconstrained reference: "
                f"balance {_fmt_money(trial_legacy_current)} | "
                f"PnL {_fmt_money(trial_legacy_cumulative_counterfactual_pnl)}"
            )
        else:
            match = "Unconstrained reference: unavailable"
    if prefix.startswith("Since reset ("):
        m = re.search(r"resolved\s+([0-9,]+)", match)
        if m:
            try:
                resolved_since_reset = int(m.group(1).replace(",", ""))
            except Exception:
                resolved_since_reset = -1
            if resolved_since_reset == 0:
                continue
    if prefix == "Settled economics (unique market-side):":
        economics_bits = []
        if isinstance(settled_expectancy_per_trade, float):
            economics_bits.append(f"expectancy/trade {_fmt_money(settled_expectancy_per_trade)}")
        if isinstance(settled_avg_win, float):
            economics_bits.append(f"avg win {_fmt_money(settled_avg_win)}")
        if isinstance(settled_avg_loss, float):
            economics_bits.append(f"avg loss {_fmt_money(settled_avg_loss)}")
        if isinstance(settled_payoff_ratio, float):
            economics_bits.append(f"payoff {settled_payoff_ratio:.2f}x")
        if isinstance(settled_profit_factor, float):
            economics_bits.append(f"profit factor {settled_profit_factor:.2f}")
        if economics_bits:
            match = "Settled economics: " + " | ".join(economics_bits)
        else:
            continue
    if prefix == "Benchmark vs HYSA (window, model projection):" and "pending settled outcomes" in match:
        limiting = ""
        if "|" in match:
            limiting = _normalize(match.split("|", 1)[1])
        match = "Benchmark vs HYSA (window, model projection): pending (needs settled outcomes)"
        if limiting:
            match += f" | {limiting}"
    concise_lines.append(match)

concise_suggestions = [_compact_suggestion_line(item, 96) for item in display_suggestions[:3] if item]
if concise_suggestions:
    concise_lines.append(suggestion_heading)
    if shared_suggestion_rationale_line:
        concise_lines.append(shared_suggestion_rationale_line)
    if top3_action_impact_proxy_line:
        concise_lines.append(_clip_text(top3_action_impact_proxy_line, 96))
    for idx, item in enumerate(concise_suggestions, start=1):
        concise_lines.append(f"{idx}. {item}")

concise_working_lines = concise_lines[:]
discord_message_concise = "\n".join(concise_working_lines)
hard_truncated_concise = False
if len(discord_message_concise) > 1900:
    # Drop lower-priority lines in a stable order when concise payloads exceed
    # Discord limits. Prefix groups allow transformed/legacy labels to map to
    # the same logical line (for example "Trend vs prior" vs
    # "Window delta vs prior run").
    optional_prefix_groups = [
        ("Guardrail sample:",),
        ("Blocker mix (top):",),
        ("Persistent balance horizons (PnL):",),
        ("Unconstrained reference:", "Since reset (unconstrained reference):"),
        ("Since reset:", "Since reset (cash-constrained order instances):", "Since reset (order instances):"),
        ("Persistent trial balance:",),
        ("Settlement progress:",),
        ("Benchmark vs HYSA (window, model projection):",),
        ("Factor split:",),
        ("Simulation realism note:",),
        ("Last settled selection (unique market-side):",),
        ("Trend vs prior:", "Window delta vs prior run:"),
    ]
    trimmed_lines = concise_working_lines[:]
    for prefix_group in optional_prefix_groups:
        if len("\n".join(trimmed_lines)) <= 1900:
            break
        drop_index = next(
            (
                idx
                for idx, line in enumerate(trimmed_lines)
                if any(line.startswith(prefix) for prefix in prefix_group)
            ),
            None,
        )
        if drop_index is not None:
            trimmed_lines.pop(drop_index)
    concise_working_lines = trimmed_lines
    discord_message_concise = "\n".join(trimmed_lines)

if len(discord_message_concise) > 1900:
    short_suggestions = [_compact_suggestion_line(item, 84) for item in display_suggestions[:3]]
    trimmed_lines = [
        line
        for line in concise_working_lines
        if line != suggestion_heading
        and line != shared_suggestion_rationale_line
        and line != top3_action_impact_proxy_line
        and not re.match(r"^\d+\.\s", line)
    ]
    if short_suggestions:
        trimmed_lines.append(suggestion_heading)
        if shared_suggestion_rationale_line:
            trimmed_lines.append(shared_suggestion_rationale_line)
        if top3_action_impact_proxy_line:
            trimmed_lines.append(_clip_text(top3_action_impact_proxy_line, 84))
        for idx, item in enumerate(short_suggestions, start=1):
            trimmed_lines.append(f"{idx}. {item}")
    concise_working_lines = trimmed_lines
    discord_message_concise = "\n".join(trimmed_lines)

if len(discord_message_concise) > 1900:
    overflow_drop_prefix_groups = [
        ("Blocker mix (top):",),
        ("Unconstrained reference:", "Since reset (unconstrained reference):"),
        ("Since reset:", "Since reset (cash-constrained order instances):", "Since reset (order instances):"),
        ("Persistent trial balance:",),
        ("Order-instance breadth since reset:",),
        ("Settlement progress:",),
        ("Benchmark vs HYSA (window, model projection):",),
        ("Factor split:",),
        ("Simulation realism note:",),
        ("Last settled selection (unique market-side):",),
        ("Guardrail sample:",),
        ("Trend vs prior:", "Window delta vs prior run:"),
    ]
    trimmed_lines = concise_working_lines[:]
    for prefix_group in overflow_drop_prefix_groups:
        while len("\n".join(trimmed_lines)) > 1900:
            drop_index = next(
                (
                    idx
                    for idx, line in enumerate(trimmed_lines)
                    if any(line.startswith(prefix) for prefix in prefix_group)
                ),
                None,
            )
            if drop_index is None:
                break
            trimmed_lines.pop(drop_index)
    concise_working_lines = trimmed_lines
    discord_message_concise = "\n".join(trimmed_lines)

if len(discord_message_concise) > 1900:
    terminal_drop_prefix_groups = [
        ("Factor split:",),
        ("Window settled (simulated-live counterfactual):",),
        ("Settled quality (unique market-side):",),
        ("Settled economics (unique market-side):", "Settled economics:"),
        ("Economics sanity:",),
        ("Data consistency note:",),
        ("3. ",),
        ("2. ",),
        ("1. ",),
        (suggestion_heading,),
    ]
    trimmed_lines = concise_working_lines[:]
    for prefix_group in terminal_drop_prefix_groups:
        while len("\n".join(trimmed_lines)) > 1900:
            drop_index = next(
                (
                    idx
                    for idx, line in enumerate(trimmed_lines)
                    if any(line.startswith(prefix) for prefix in prefix_group)
                ),
                None,
            )
            if drop_index is None:
                break
            trimmed_lines.pop(drop_index)
    concise_working_lines = trimmed_lines
    discord_message_concise = "\n".join(trimmed_lines)

# Guarantee at least 3 concise suggestions when available by trimming
# lower-priority context before dropping action items.
if len(display_suggestions) >= 3:
    current_suggestion_count = sum(
        1 for line in discord_message_concise.splitlines() if re.match(r"^\d+\.\s", line)
    )
    if current_suggestion_count < 3:
        forced_suggestion_lines = [
            f"{idx}. {_compact_suggestion_line(display_suggestions[idx - 1], 72)}"
            for idx in range(1, 4)
        ]
        force_lines = [
            line
            for line in concise_working_lines
            if line != suggestion_heading
            and line != shared_suggestion_rationale_line
            and line != top3_action_impact_proxy_line
            and not re.match(r"^\d+\.\s", line)
        ]
        force_lines.append(suggestion_heading)
        if shared_suggestion_rationale_line:
            force_lines.append(shared_suggestion_rationale_line)
        if top3_action_impact_proxy_line:
            force_lines.append(_clip_text(top3_action_impact_proxy_line, 72))
        force_lines.extend(forced_suggestion_lines)

        force_drop_prefix_groups = [
            ("Projected bankroll PnL",),
            ("Alpha verdict:",),
            ("Breadth:",),
            ("Blockers (actionable):",),
            ("Approval guardrail:",),
            ("Plan conversion:",),
            ("Flow:",),
            ("Factor split:",),
            ("Benchmark vs HYSA (window, model projection):",),
            ("Settlement progress:",),
            ("Unconstrained reference:", "Since reset (unconstrained reference):"),
            ("Since reset:", "Since reset (cash-constrained order instances):", "Since reset (order instances):"),
            ("Order-instance breadth since reset:",),
            ("Persistent trial balance:",),
            ("Guardrail sample:",),
            ("Trend vs prior:", "Window delta vs prior run:"),
            ("Selection thresholds:",),
        ]
        for prefix_group in force_drop_prefix_groups:
            while len("\n".join(force_lines)) > 1900:
                drop_index = next(
                    (
                        idx
                        for idx, line in enumerate(force_lines)
                        if any(line.startswith(prefix) for prefix in prefix_group)
                    ),
                    None,
                )
                if drop_index is None:
                    break
                force_lines.pop(drop_index)
        force_message = "\n".join(force_lines)
        if len(force_message) <= 1900:
            concise_working_lines = force_lines
            discord_message_concise = force_message

# Deterministic concise format for readability: metrics-first, low jargon.
tldr_mode_label = (
    "GO (scaled candidate)"
    if (ready_scaled_live and not quality_risk_alert_active)
    else (
        "GO (small pilot candidate)"
        if (ready_small_live and not quality_risk_alert_active)
        else "NO-GO (shadow-only)"
    )
)
tldr_top_blocker_key = _normalize(top_blocker_reason).lower().replace(" ", "_")
tldr_reason = ""
if quality_risk_alert_active:
    tldr_reason = "approval quality risk remains active"
elif not has_settled_predictions:
    tldr_reason = "no settled independent outcomes yet"
elif isinstance(settled_counterfactual_pnl, float) and settled_counterfactual_pnl <= 0:
    tldr_reason = "settled counterfactual PnL is non-positive"
elif _normalize(top_blocker_reason):
    tldr_reason = f"top blocker is {_humanize_reason(top_blocker_reason)}"
else:
    tldr_reason = f"limiting factor is {display_limiting_factor.replace('_', ' ')}"
tldr_risk_line = "Risk now: no immediate red-flag drift; keep strict risk caps."
if (
    has_settled_predictions
    and isinstance(prediction_win_rate, float)
    and isinstance(settled_counterfactual_pnl, float)
    and isinstance(settled_avg_win, float)
    and isinstance(settled_avg_loss, float)
    and prediction_win_rate >= 0.70
    and settled_counterfactual_pnl <= 0
):
    tldr_risk_line = "Risk now: high hit rate but payout shape is negative (losses outweigh wins)."
elif quality_risk_alert_active:
    tldr_risk_line = "Risk now: approval quality risk is active; throughput too permissive for current edge."
elif stale_rate >= 0.50 or tldr_top_blocker_key in {"metar_observation_stale", "weather_data_stale"}:
    tldr_risk_line = (
        "Risk now: weather freshness blocker remains dominant "
        f"({stale_rate*100.0:.1f}% stale blocks)."
    )
elif trial_balance_depleted:
    tldr_risk_line = "Risk now: cash-constrained trial balance is near depleted; avoid interpretation drift."
tldr_best_action_fallback = (
    "Cut weather freshness blocks first (station/hour policy + ingest cadence)."
    if tldr_top_blocker_key in {"metar_observation_stale", "weather_data_stale"}
    else (
        "Reduce settlement finalization lag first (pressure mode + final-report refresh)."
        if tldr_top_blocker_key in {"settlement_finalization_blocked", "settlement_finalization"}
        else (
            "Shift scans earlier to avoid cutoff timing loss."
            if tldr_top_blocker_key in {"inside_cutoff_window", "cutoff_window"}
            else (
                "Increase independent breadth before policy expansion."
                if tldr_top_blocker_key in {
                    "range_still_possible",
                    "interval_overlap_gate",
                    "range_still_possible_within_envelope",
                }
                else "Collect more settled independent outcomes before policy expansion."
            )
        )
    )
)
tldr_best_action = _normalize(tldr_best_action_fallback)
top_blocker_compact_label = (
    _humanize_reason(top_blocker_reason)
    if _normalize(top_blocker_reason)
    else "none"
)


def _action_key(text: Any) -> str:
    normalized = _normalize(text).lower()
    if not normalized:
        return ""
    normalized = re.sub(r"[^a-z0-9\s]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _first_line_with_prefix(lines: list[str], prefix: str) -> str:
    for line in lines:
        if line.startswith(prefix):
            return line
    return ""


concise_suggestion_source_rows: list[dict[str, Any]] = []
for row in suggestion_rows_ranked:
    if not isinstance(row, dict):
        continue
    row_key = _normalize(row.get("key"))
    candidate_text = _format_suggestion_brief(row)
    candidate_text = _strip_metric_clause(_strip_why_clause(candidate_text))
    if _normalize(candidate_text):
        impact_estimate = _parse_float(row.get("impact_dollars_estimate"))
        impact_points = _parse_float(row.get("impact_points"))
        measurable_delta = bool(row.get("measurable_12h_delta"))
        quantified = bool(
            (isinstance(impact_estimate, float) and impact_estimate > 0.0)
            or (isinstance(impact_points, float) and impact_points > 0.0)
            or measurable_delta
        )
        concise_suggestion_source_rows.append(
            {
                "key": row_key,
                "text": candidate_text,
                "impact_estimate": impact_estimate,
                "impact_points": impact_points,
                "measurable_delta": measurable_delta,
                "quantified": quantified,
            }
        )
if not concise_suggestion_source_rows:
    concise_suggestion_source_rows = [
        {
            "key": "",
            "text": item,
            "impact_estimate": None,
            "measurable_delta": False,
            "quantified": False,
        }
        for item in (display_suggestions or [])
        if _normalize(item)
    ]
if not concise_suggestion_source_rows:
    concise_suggestion_source_rows = [
        {
            "key": "",
            "text": item,
            "impact_estimate": None,
            "measurable_delta": False,
            "quantified": False,
        }
        for item in (suggestions or [])
        if _normalize(item)
    ]
target_suggestion_count = min(3, len(concise_suggestion_source_rows))
concise_suggestion_candidates: list[dict[str, Any]] = []
for row in concise_suggestion_source_rows:
    candidate_key = _normalize(row.get("key"))
    candidate_text = _normalize(row.get("text"))
    compact_text = _compact_suggestion_line(candidate_text, 92)
    if _normalize(compact_text):
        concise_suggestion_candidates.append(
            {
                "key": candidate_key,
                "text": compact_text,
                "impact_estimate": _parse_float(row.get("impact_estimate")),
                "measurable_delta": bool(row.get("measurable_delta")),
                "quantified": bool(row.get("quantified")),
            }
        )
concise_quantified_candidates = [
    row for row in concise_suggestion_candidates if bool(row.get("quantified"))
]
concise_quantified_available_count = len(concise_quantified_candidates)
concise_quantified_preferred = bool(concise_quantified_available_count > 0)
concise_primary_candidates = (
    concise_quantified_candidates
    if concise_quantified_preferred
    else concise_suggestion_candidates
)
tldr_best_action_source_key = ""
tldr_best_action_source_quantified = False
if concise_primary_candidates:
    tldr_best_action = _normalize(concise_primary_candidates[0].get("text"))
    tldr_best_action_source_key = _normalize(concise_primary_candidates[0].get("key"))
    tldr_best_action_source_quantified = bool(concise_primary_candidates[0].get("quantified"))
elif concise_suggestion_candidates:
    tldr_best_action = _normalize(concise_suggestion_candidates[0].get("text"))
    tldr_best_action_source_key = _normalize(concise_suggestion_candidates[0].get("key"))
    tldr_best_action_source_quantified = bool(concise_suggestion_candidates[0].get("quantified"))
tldr_best_action = re.sub(r"\s*\|\s*conf\s+[A-Z]+$", "", tldr_best_action, flags=re.IGNORECASE).strip()
if not tldr_best_action:
    tldr_best_action = _normalize(tldr_best_action_fallback)
    tldr_best_action_source_key = ""
    tldr_best_action_source_quantified = False
best_action_key = _action_key(tldr_best_action)
concise_display_suggestions: list[str] = []
concise_display_suggestion_keys: list[str] = []
concise_display_suggestion_quantified: list[bool] = []
seen_suggestion_keys: set[str] = set()

# First pass: preserve strict ranked order while removing exact duplicates and
# avoiding a noisy repeat of the already-promoted "Best next action".
for row in concise_primary_candidates:
    suggestion_key = _normalize(row.get("key"))
    candidate = _normalize(row.get("text"))
    candidate_key = _action_key(candidate)
    if candidate_key and candidate_key == best_action_key:
        continue
    if candidate_key and candidate_key in seen_suggestion_keys:
        continue
    if candidate_key:
        seen_suggestion_keys.add(candidate_key)
    concise_display_suggestions.append(candidate)
    concise_display_suggestion_keys.append(suggestion_key)
    concise_display_suggestion_quantified.append(bool(row.get("quantified")))
    if len(concise_display_suggestions) >= target_suggestion_count:
        break

# Second pass: allow remaining unique candidates regardless of best-action
# overlap so we still preserve top-3 count when option diversity is low.
if len(concise_display_suggestions) < target_suggestion_count:
    for row in concise_suggestion_candidates:
        suggestion_key = _normalize(row.get("key"))
        candidate = _normalize(row.get("text"))
        candidate_key = _action_key(candidate)
        if candidate_key and candidate_key in seen_suggestion_keys:
            continue
        if candidate_key:
            seen_suggestion_keys.add(candidate_key)
        concise_display_suggestions.append(candidate)
        concise_display_suggestion_keys.append(suggestion_key)
        concise_display_suggestion_quantified.append(bool(row.get("quantified")))
        if len(concise_display_suggestions) >= target_suggestion_count:
            break

# Final pass: if everything is repetitive, pad from original candidates to
# keep deterministic suggestion-count expectations intact.
if len(concise_display_suggestions) < target_suggestion_count:
    for row in concise_suggestion_candidates:
        suggestion_key = _normalize(row.get("key"))
        candidate = _normalize(row.get("text"))
        concise_display_suggestions.append(candidate)
        concise_display_suggestion_keys.append(suggestion_key)
        concise_display_suggestion_quantified.append(bool(row.get("quantified")))
        if len(concise_display_suggestions) >= target_suggestion_count:
            break

simple_concise_lines: list[str] = [
    f"BetBot Alpha Summary ({window_label}, rolling) — {summary_time}",
    "",
    (
        f"Status now: {tldr_mode_label} | runtime {runtime_health_status} | "
        f"alpha {health_status} | {'shadow-only' if 'shadow' in live_mode_display.lower() else 'live'}"
    ),
    f"Why this status: {tldr_reason}",
    f"Decision: {decision_now}",
    f"Best next action: {tldr_best_action}",
    "Execution mode: shadow-only simulation (counterfactual basis; live fills not included).",
    f"Primary risk: {tldr_risk_line.replace('Risk now: ', '')}",
    "",
    (
        f"Confidence: deploy {deployment_confidence_score:.1f}/100 | "
        f"selection {selection_confidence_score:.1f}/100 ({selection_confidence_band.lower()})"
    ),
    (
        f"Settled confidence (1d/7d/14d): {settled_evidence_confidence_panel}"
        if has_settled_predictions
        else "Settled confidence: pending (no settled independent outcomes yet)."
    ),
    "",
    (
        f"12h flow: signals {intents_total:,} | approved {intents_approved:,} ({approval_rate*100.0:.2f}%) | "
        f"planned {planned_orders:,} | plan rate {approval_to_plan_rate*100.0:.2f}%"
        if isinstance(approval_to_plan_rate, float)
        else (
            f"12h flow: signals {intents_total:,} | approved {intents_approved:,} ({approval_rate*100.0:.2f}%) | "
            f"planned {planned_orders:,} | plan rate n/a"
        )
    ),
    (
        "12h blockers: "
        f"top {top_blocker_compact_label} ({top_blocker_share_of_blocked*100.0:.1f}% blocked) | "
        f"top3 {top3_blockers_compact_text} | "
        f"freshness {stale_rate*100.0:.2f}% | overlap {overlap_rate*100.0:.2f}%"
    ),
    (
        "Limiting factor: "
        f"{display_limiting_factor.replace('_', ' ')}"
    ),
    (
        "Breadth: "
        f"settled sides {resolved_market_sides:,} | unresolved {unresolved_market_sides:,} | "
        f"families {resolved_families:,} | concentration {'warn' if concentration_warning else 'ok'}"
    ),
]
if quality_risk_alert_active:
    if _normalize(quality_risk_alert_line):
        simple_concise_lines.append(_clip_text_plain(quality_risk_alert_line, 140))
    else:
        simple_concise_lines.append(
            "Quality-risk alert: approvals are above guardrail; tighten selection thresholds before increasing throughput."
        )
    simple_concise_lines.append(_clip_text_plain(profitability_guardrail_compact_line, 140))
if (
    _normalize(health_status).upper() != "GREEN"
    and _normalize(health_reason_text).lower() not in {"", "none", "n/a", "unknown"}
):
    simple_concise_lines.insert(
        2,
        "Health note: " + _clip_text_plain(health_reason_text, 110),
    )
if confidence_cap_line:
    confidence_cap_match = re.search(
        r"Confidence cap:\s*(.+?);\s*uncapped\s*([0-9.]+)\s*->\s*displayed\s*([0-9.]+)\.(?:\s*\(.+\))?",
        confidence_cap_line,
    )
    if confidence_cap_match:
        cap_reason = _clip_text(_normalize(confidence_cap_match.group(1)), 84)
        cap_display = _normalize(confidence_cap_match.group(3))
        simple_concise_lines.append(
            f"Confidence cap: {cap_display}/100 while {cap_reason}."
        )
    else:
        simple_concise_lines.append(
            _clip_text_plain(confidence_cap_line.replace("Confidence cap:", "Confidence cap:"), 120)
        )
if isinstance(trial_start, float) and isinstance(trial_current, float):
    if trial_balance_mode == "cash_constrained":
        executed_ratio_text = (
            _fmt_percent_ratio(trial_cash_constrained_execution_rate)
            if isinstance(trial_cash_constrained_execution_rate, float)
            else "n/a"
        )
        simple_concise_lines.append(
            "Scenario balance (stress replay): "
            f"cash {_fmt_money(trial_current, signed=False)} | "
            f"PnL {_fmt_money(trial_growth)} | "
            f"executed {executed_ratio_text} of unconstrained"
        )
    else:
        concise_trial_mode_label = "unconstrained"
        simple_concise_lines.append(
            "Persistent trial (since reset): "
            f"{_fmt_money(trial_start, signed=False)} -> {_fmt_money(trial_current, signed=False)} | "
            f"Δ {_fmt_money(trial_growth)} ({_fmt_percent_value(trial_growth_pct)}) | "
            f"mode {concise_trial_mode_label}"
        )
else:
    simple_concise_lines.append("Persistent trial (since reset): unavailable")
checkin_prefix = (
    "Scenario check-in PnL (stress replay)"
    if trial_balance_mode == "cash_constrained"
    else "Check-in PnL (counterfactual)"
)
simple_concise_lines.append(
    f"{checkin_prefix}: "
    f"1d {_fmt_money(trial_window_snapshots.get('1d', {}).get('pnl_dollars'))} | "
    f"7d {_fmt_money(trial_window_snapshots.get('7d', {}).get('pnl_dollars'))} | "
    f"{monthly_label} {_fmt_money(monthly_pnl)}"
)
if selection_gate_coverage_basis != "measured":
    selection_basis_note = {
        "fallback_no_approved_rows": "no approved rows in-window",
        "fallback_no_gate_data": "gate coverage data unavailable",
        "no_intents": "no intents in-window",
    }.get(selection_gate_coverage_basis, selection_gate_coverage_basis.replace("_", " "))
    simple_concise_lines.append(
        "Selection confidence note: "
        + _clip_text(
            f"fallback basis ({selection_basis_note}); treat as provisional until measured approvals return.",
            120,
        )
    )
if selection_quality_pressure_line_concise:
    if selection_quality_global_only_alert_active or selection_confidence_score < 85.0:
        simple_concise_lines.append(_clip_text(selection_quality_pressure_line_concise, 120))
if selection_confidence_score < 70.0 and selection_confidence_driver_line:
    simple_concise_lines.append(_clip_text_plain(selection_confidence_driver_line, 140))
if has_settled_predictions:
    projected_roi_text = "n/a"
    if reference_bankroll > 0:
        projected_roi_text = f"{projected_roi_ratio*100.0:.2f}%"
    utilization_text = _fmt_percent_ratio(projected_utilization / 100.0)
    simple_concise_lines.append(
        f"Bankroll sim (${reference_bankroll:,.0f}, deployment model): "
        f"PnL {_fmt_money(projected_pnl_dollars)} | ROI {projected_roi_text} | util {utilization_text}"
    )
    if (
        trial_balance_mode == "cash_constrained"
        and isinstance(trial_growth, float)
        and isinstance(projected_pnl_dollars, float)
        and (trial_growth * projected_pnl_dollars) < 0.0
    ):
        simple_concise_lines.append(
            "Reconciliation: deployment model is the live proxy; stress replay is a repeat-entry pressure test."
        )
    settled_line = (
        "Settled selection (unique market-side): "
        f"{resolved_predictions:,} sides | "
        f"win {_fmt_percent_ratio(prediction_win_rate)} | "
        f"counterfactual PnL {_fmt_money(settled_counterfactual_pnl)}"
    )
    if isinstance(settled_expectancy_per_trade, float):
        settled_line += f" | expectancy/trade {_fmt_money(settled_expectancy_per_trade)}"
    simple_concise_lines.append(settled_line)
    settled_economics_simple_bits: list[str] = []
    if isinstance(settled_expectancy_per_trade, float):
        settled_economics_simple_bits.append(f"expectancy/trade {_fmt_money(settled_expectancy_per_trade)}")
    if isinstance(settled_avg_win, float):
        settled_economics_simple_bits.append(f"avg win {_fmt_money(settled_avg_win)}")
    if isinstance(settled_avg_loss, float):
        settled_economics_simple_bits.append(f"avg loss {_fmt_money(settled_avg_loss)}")
    if isinstance(settled_payoff_ratio, float):
        settled_economics_simple_bits.append(f"payoff {settled_payoff_ratio:.2f}x")
    if isinstance(settled_profit_factor, float):
        settled_economics_simple_bits.append(f"profit factor {settled_profit_factor:.2f}")
    if settled_economics_simple_bits:
        simple_concise_lines.append(
            "Payoff shape: " + " | ".join(settled_economics_simple_bits)
        )
    if (
        isinstance(settled_counterfactual_pnl, float)
        and isinstance(trial_growth, float)
        and settled_counterfactual_pnl > 0.0
        and trial_growth < 0.0
    ):
        simple_concise_lines.append(
            "Context: recent settled window is positive, but persistent trial is still below start."
        )
else:
    simple_concise_lines.append(
        f"Bankroll sim (${reference_bankroll:,.0f}, deployment model): pending "
        "(need settled independent outcomes for calibrated projection)."
    )
if (
    has_settled_predictions
    and isinstance(prediction_win_rate, float)
    and isinstance(settled_counterfactual_pnl, float)
    and isinstance(settled_avg_win, float)
    and isinstance(settled_avg_loss, float)
    and prediction_win_rate >= 0.70
    and settled_counterfactual_pnl <= 0
):
    if "payout shape is negative" not in tldr_risk_line.lower():
        simple_concise_lines.append(
            "Risk-shape warning: high win rate but net PnL is non-positive (losses outweigh wins)."
        )
if quality_risk_alert_active and not any(
    line.startswith("Quality-risk alert:") for line in simple_concise_lines
):
    quality_risk_compact_line = (
        "Quality-risk alert: approvals are above guardrail while edge/probability gates dominate blocked flow; "
        "tighten thresholds before raising throughput."
    )
    insert_at = next(
        (
            idx + 1
            for idx, line in enumerate(simple_concise_lines)
            if line.startswith("Primary risk:")
        ),
        4 if len(simple_concise_lines) >= 4 else len(simple_concise_lines),
    )
    simple_concise_lines.insert(insert_at, quality_risk_compact_line)
if len(concise_display_suggestions) > 0:
    simple_concise_lines.append("Top 3 optimization moves:")
    for idx, item in enumerate(concise_display_suggestions, start=1):
        simple_concise_lines.append(f"{idx}. {item}")
elif suggestions_unchanged:
    simple_concise_lines.append(unchanged_actions_line)

if len(simple_concise_lines) > concise_max_lines:
    suggestion_header_line = _first_line_with_prefix(simple_concise_lines, "Top 3 optimization moves:")
    suggestion_item_lines = [line for line in simple_concise_lines if re.match(r"^\d+\.\s", line)]
    suggestion_bundle: list[str] = []
    if suggestion_header_line and suggestion_item_lines:
        suggestion_bundle = [suggestion_header_line, *suggestion_item_lines[:3]]
    elif suggestion_header_line:
        suggestion_bundle = [suggestion_header_line]

    reserved_suggestion_lines = len(suggestion_bundle)
    non_suggestion_budget = max(0, concise_max_lines - reserved_suggestion_lines)
    non_suggestion_pool = [
        line
        for line in simple_concise_lines
        if line not in suggestion_bundle and not re.match(r"^\d+\.\s", line)
    ]

    core_prefixes = [
        "BetBot Alpha Summary",
        "Status now:",
        "Execution mode:",
        "Best next action:",
        "Confidence:",
        "Settled confidence",
        "12h flow:",
        "Bankroll sim ($",
        "Check-in PnL (counterfactual):",
        "Scenario check-in PnL (stress replay):",
        "Scenario balance (stress replay):",
        "12h blockers:",
        "Limiting factor:",
        "Settled selection (unique market-side):",
        "Primary risk:",
        "Health note:",
        "Quality-risk alert:",
        "Selection confidence note:",
        "Persistent trial (since reset):",
    ]
    optional_prefixes = [
        "Health note:",
        "Why this status:",
        "Selection confidence note:",
        "Execution mode:",
        "Breadth:",
        "Selection confidence drivers:",
        "Confidence cap:",
        "Bankroll sim ($",
        "Scenario balance (stress replay):",
        "Scenario check-in PnL (stress replay):",
        "Quality-risk alert:",
    ]

    selected_non_suggestion: list[str] = []
    seen_lines: set[str] = set()
    for prefix in core_prefixes:
        if len(selected_non_suggestion) >= non_suggestion_budget:
            break
        line = _first_line_with_prefix(non_suggestion_pool, prefix)
        if line and line not in seen_lines:
            selected_non_suggestion.append(line)
            seen_lines.add(line)
    if quality_risk_alert_active and len(selected_non_suggestion) > 0:
        quality_line_required = _first_line_with_prefix(non_suggestion_pool, "Quality-risk alert:")
        if quality_line_required and quality_line_required not in seen_lines:
            if len(selected_non_suggestion) >= non_suggestion_budget:
                trimmed = max(1, non_suggestion_budget - 1)
                selected_non_suggestion = selected_non_suggestion[:trimmed]
                seen_lines = set(selected_non_suggestion)
            selected_non_suggestion.append(quality_line_required)
            seen_lines.add(quality_line_required)
    for prefix in optional_prefixes:
        if len(selected_non_suggestion) >= non_suggestion_budget:
            break
        line = _first_line_with_prefix(non_suggestion_pool, prefix)
        if line and line not in seen_lines:
            selected_non_suggestion.append(line)
            seen_lines.add(line)
    for line in non_suggestion_pool:
        if len(selected_non_suggestion) >= non_suggestion_budget:
            break
        if line in seen_lines:
            continue
        selected_non_suggestion.append(line)
        seen_lines.add(line)

    simple_concise_lines = selected_non_suggestion + suggestion_bundle
    if len(simple_concise_lines) > concise_max_lines:
        simple_concise_lines = simple_concise_lines[:concise_max_lines]

discord_message_concise = "\n".join(simple_concise_lines)
concise_working_lines = simple_concise_lines[:]

if len(discord_message_concise) > 1900:
    hard_truncated_concise = True
    discord_message_concise = discord_message_concise[:1897].rstrip() + "..."

if discord_mode == "concise":
    discord_message = discord_message_concise
else:
    discord_message = discord_message_detailed

suggestion_tracking_summary["concise_displayed_count"] = int(len(concise_display_suggestions))
suggestion_tracking_summary["concise_displayed_keys"] = [
    _normalize(key)
    for key in concise_display_suggestion_keys[: len(concise_display_suggestions)]
    if _normalize(key)
]
concise_quantified_selected_count = int(
    sum(1 for flag in concise_display_suggestion_quantified if bool(flag))
)
suggestion_tracking_summary["concise_quantified_available_count"] = int(
    concise_quantified_available_count
)
suggestion_tracking_summary["concise_quantified_selected_count"] = concise_quantified_selected_count
suggestion_tracking_summary["concise_quantified_preferred"] = bool(
    concise_quantified_preferred
)
suggestion_tracking_summary["concise_quantified_shortfall_count"] = int(
    max(0, min(target_suggestion_count, concise_quantified_available_count) - concise_quantified_selected_count)
)
suggestion_tracking_summary["best_next_action_key"] = (
    tldr_best_action_source_key if _normalize(tldr_best_action_source_key) else None
)
suggestion_tracking_summary["best_next_action_quantified"] = bool(
    tldr_best_action_source_quantified
)

selected_suggestion_count = sum(
    1 for line in discord_message.splitlines() if re.match(r"^\d+\.\s", line)
)
minimum_suggestion_expected = min(3, len(display_suggestions))
tokenized_why_pattern = r"\|\s*why\s+[A-Z]{1,3}(?:\+[A-Z]{1,3})+"
quality_risk_alert_needed = bool(_normalize(quality_risk_alert_line))
confidence_cap_needed = bool(_normalize(confidence_cap_line))
trial_cache_reliability_alert_needed = not trial_balance_cache_write_ok
trial_balance_depletion_line_needed = bool(trial_balance_depleted)
settled_economics_line_needed = any(
    line.startswith("Settled economics (unique market-side):")
    for line in lines
)
trial_balance_status_markers = (
    "Trial balance status:",
    "Persistent trial (since reset):",
    "Stress replay (since reset, no sizing):",
    "Since reset (cash-constrained order instances):",
    "Daily/weekly/monthly check-in:",
)
has_trial_balance_status_marker_selected = any(
    marker in discord_message for marker in trial_balance_status_markers
)
has_trial_balance_status_marker_detailed = any(
    marker in discord_message_detailed for marker in trial_balance_status_markers
)
has_trial_balance_cash_mode_selected = (
    ("mode cash-constrained" in discord_message)
    or ("stress replay" in discord_message.lower())
)
has_trial_balance_cash_mode_detailed = (
    ("mode cash-constrained" in discord_message_detailed)
    or ("stress replay" in discord_message_detailed.lower())
)
risk_shape_warning_needed = bool(
    has_settled_predictions
    and isinstance(prediction_win_rate, float)
    and isinstance(settled_counterfactual_pnl, float)
    and isinstance(settled_avg_win, float)
    and isinstance(settled_avg_loss, float)
    and prediction_win_rate >= 0.70
    and settled_counterfactual_pnl <= 0
)
message_quality_checks = {
    "selected_mode": discord_mode,
    "selected_message_length_ok": len(discord_message) <= 1900,
    "concise_message_length_ok": len(discord_message_concise) <= 1900,
    "detailed_message_length_ok": len(discord_message_detailed) <= 1900,
    "not_hard_truncated": (not hard_truncated_concise) if discord_mode == "concise" else True,
    "contains_performance_basis_line": (
        ("Performance basis:" in discord_message)
        or (
            discord_mode == "concise"
            and ("Performance basis:" in discord_message_detailed)
        )
    ),
    "contains_impact_basis_line": (
        ("Suggestion impact basis:" in discord_message)
        or (
            discord_mode == "concise"
            and ("Suggestion impact basis:" in discord_message_detailed)
        )
    ),
    "contains_top3_action_impact_line_if_suggestions": (
        (
            ("Top-3 action impact estimate:" in discord_message)
            or (
                discord_mode == "concise"
                and ("Top-3 action impact estimate:" in discord_message_detailed)
            )
        )
        if (
            discord_mode != "concise"
            and len(display_suggestions) > 0
            and bool(_normalize(top3_action_impact_proxy_line))
        )
        else True
    ),
    "contains_replan_pressure_line": (
        ("Replan pressure:" in discord_message) if discord_mode != "concise" else True
    ),
    "contains_confidence_cap_if_needed": (
        (
            ("Confidence cap:" in discord_message)
            or (
                discord_mode == "concise"
                and ("Confidence cap:" in discord_message_detailed)
            )
        )
        if confidence_cap_needed
        else True
    ),
    "contains_trial_cache_reliability_alert_if_needed": (
        ("Data reliability alert:" in discord_message)
        if trial_cache_reliability_alert_needed
        else True
    ),
    "contains_trial_balance_status_if_needed": (
        (
            has_trial_balance_status_marker_selected
            or has_trial_balance_cash_mode_selected
            or (
                discord_mode == "concise"
                and (
                    has_trial_balance_status_marker_detailed
                    or has_trial_balance_cash_mode_detailed
                )
            )
        )
        if trial_balance_depletion_line_needed
        else True
    ),
    "contains_quality_risk_alert_if_needed": (
        ("Quality-risk alert:" in discord_message) if quality_risk_alert_needed else True
    ),
    "contains_settled_economics_if_needed": (
        (
            ("Settled economics (unique market-side):" in discord_message)
            or ("Settled economics:" in discord_message)
            or (
                discord_mode == "concise"
                and (
                    ("Settled economics (unique market-side):" in discord_message_detailed)
                    or ("Settled economics:" in discord_message_detailed)
                )
            )
            or (
                discord_mode == "concise"
                and ("Settled quality (unique market-side):" in discord_message)
            )
            or ("Settled quality (unique market-side):" in discord_message_detailed)
            or (
                has_settled_predictions
                and (
                    "Performance basis: shadow-settled counterfactual" in discord_message
                    or "Performance basis: shadow-settled counterfactual" in discord_message_detailed
                )
            )
        )
    ) if settled_economics_line_needed else True,
    "contains_risk_shape_warning_if_needed": (
        (
            ("Risk-shape warning:" in discord_message)
            or (
                discord_mode == "concise"
                and "Risk now: high hit rate but payout shape is negative" in discord_message
            )
            or (
                discord_mode == "concise"
                and "Primary risk: high hit rate but payout shape is negative" in discord_message
            )
            or (
                discord_mode == "concise"
                and ("Risk-shape warning:" in discord_message_detailed)
            )
        )
        if risk_shape_warning_needed
        else True
    ),
    "contains_counterfactual_qualifier": (
        (
            ("counterfactual" in discord_message.lower())
            or (
                discord_mode == "concise"
                and ("counterfactual" in discord_message_detailed.lower())
            )
        ) if has_settled_predictions else True
    ),
    "no_legacy_if_live_phrase": ("if-live" not in discord_message.lower()),
    "contains_consistency_note_if_needed": (
        (
            ("Data consistency note:" in discord_message)
            or (
                discord_mode == "concise"
                and (
                    ("Data consistency note:" in discord_message_detailed)
                    or ("Performance basis:" in discord_message)
                    or ("Limiting factor:" in discord_message)
                    or ("Execution mode:" in discord_message)
                )
            )
        )
        if settled_metrics_source_mismatch
        else True
    ),
    "contains_factor_split_if_needed": (
        ("Factor split:" in discord_message)
        if (factor_split_mismatch and discord_mode != "concise")
        else True
    ),
    "contains_min_suggestions": (
        selected_suggestion_count >= minimum_suggestion_expected
    ),
    "contains_human_readable_why_hints": (
        re.search(tokenized_why_pattern, discord_message) is None
    ),
    "settled_count_breakdown_consistent": bool(settled_count_breakdown_consistent),
    "settled_vs_breadth_consistent": bool(settled_vs_breadth_consistent),
    "intents_reason_totals_consistent": bool(intents_reason_totals_consistent),
    "blocked_totals_consistent": bool(blocked_totals_consistent),
    "approval_rate_consistent": bool(approval_rate_consistent),
    "stale_rate_consistent": bool(stale_rate_consistent),
    "projected_pnl_consistent": bool(projected_pnl_consistent),
}
message_quality_checks["overall_pass"] = bool(
    message_quality_checks["selected_message_length_ok"]
    and message_quality_checks["concise_message_length_ok"]
    and message_quality_checks["detailed_message_length_ok"]
    and message_quality_checks["not_hard_truncated"]
    and message_quality_checks["contains_performance_basis_line"]
    and message_quality_checks["contains_impact_basis_line"]
    and message_quality_checks["contains_top3_action_impact_line_if_suggestions"]
    and message_quality_checks["contains_replan_pressure_line"]
    and message_quality_checks["contains_confidence_cap_if_needed"]
    and message_quality_checks["contains_trial_cache_reliability_alert_if_needed"]
    and message_quality_checks["contains_trial_balance_status_if_needed"]
    and message_quality_checks["contains_quality_risk_alert_if_needed"]
    and message_quality_checks["contains_settled_economics_if_needed"]
    and message_quality_checks["contains_risk_shape_warning_if_needed"]
    and message_quality_checks["contains_counterfactual_qualifier"]
    and message_quality_checks["no_legacy_if_live_phrase"]
    and message_quality_checks["contains_consistency_note_if_needed"]
    and message_quality_checks["contains_factor_split_if_needed"]
    and message_quality_checks["contains_min_suggestions"]
    and message_quality_checks["contains_human_readable_why_hints"]
    and message_quality_checks["settled_count_breakdown_consistent"]
    and message_quality_checks["settled_vs_breadth_consistent"]
    and message_quality_checks["intents_reason_totals_consistent"]
    and message_quality_checks["blocked_totals_consistent"]
    and message_quality_checks["approval_rate_consistent"]
    and message_quality_checks["stale_rate_consistent"]
    and message_quality_checks["projected_pnl_consistent"]
)

captured_at_utc = datetime.now(timezone.utc)
render_stage_elapsed_seconds = max(
    0,
    int((captured_at_utc - python_stage_started_at).total_seconds()),
)
runtime_stage_seconds["render_summary"] = render_stage_elapsed_seconds
if not isinstance(runtime_stage_seconds.get("total"), int):
    pre_stages_total = sum(
        int(value)
        for key, value in runtime_stage_seconds.items()
        if key != "total" and isinstance(value, int) and value >= 0
    )
    runtime_stage_seconds["total"] = pre_stages_total
runtime_stage_seconds_known_total = sum(
    int(value)
    for key, value in runtime_stage_seconds.items()
    if key != "total" and isinstance(value, int) and value >= 0
)
trader_checkin_month_label = "30d" if "30d" in trial_window_snapshots else "28d"
trader_checkin_1d_pnl = _parse_float(
    (trial_window_snapshots.get("1d") or {}).get("pnl_dollars")
)
trader_checkin_7d_pnl = _parse_float(
    (trial_window_snapshots.get("7d") or {}).get("pnl_dollars")
)
trader_checkin_month_pnl = _parse_float(
    (trial_window_snapshots.get(trader_checkin_month_label) or {}).get("pnl_dollars")
)
trader_view = {
    "decision_now": decision_now,
    "mode": "shadow_only" if "shadow" in live_mode_display.lower() else "live",
    "confidence_score": round(float(deployment_confidence_score), 3),
    "confidence_score_uncapped": round(float(deployment_confidence_score_uncapped), 3),
    "confidence_cap_applied": bool(deployment_confidence_cap_applied),
    "confidence_cap_reason": deployment_confidence_cap_reason or None,
    "confidence_cap_value": (
        round(float(deployment_confidence_cap_value), 3)
        if isinstance(deployment_confidence_cap_value, float)
        else None
    ),
    "confidence_cap_detail": deployment_confidence_cap_detail or None,
    "confidence_band": deployment_confidence_band,
    "selection_confidence_score": round(float(selection_confidence_score), 3),
    "selection_confidence_band": selection_confidence_band,
    "selection_confidence_guidance": selection_confidence_guidance,
    "selection_confidence_outcome_multiplier": round(
        float(selection_outcome_multiplier),
        6,
    ),
    "selection_confidence_settled_win_rate_lb95": (
        round(float(selection_settled_win_rate_lb95), 6)
        if isinstance(selection_settled_win_rate_lb95, float)
        else None
    ),
    "selection_confidence_settled_strength": round(
        float(selection_settled_confidence_strength),
        6,
    ),
    "selection_confidence_gate_coverage_ratio": round(
        float(selection_gate_coverage_ratio),
        6,
    ),
    "selection_confidence_gate_coverage_basis": selection_gate_coverage_basis,
    "selection_confidence_component_summary": selection_confidence_component_summary,
    "selection_confidence_top_drivers": [
        {"driver": label, "points": round(float(points), 3)}
        for label, points in selection_confidence_top_drivers
    ],
    "selection_confidence_driver_line": selection_confidence_driver_line or None,
    "selection_quality_source_file": selection_quality_path or None,
    "selection_quality_rows_total": int(selection_quality_rows_total),
    "selection_quality_rows_adjusted": int(selection_quality_rows_adjusted),
    "selection_quality_rows_adjusted_bucket_backed": int(selection_quality_rows_adjusted_bucket_backed),
    "selection_quality_rows_adjusted_global_only": int(selection_quality_rows_adjusted_global_only),
    "selection_quality_global_only_adjusted_share": (
        round(float(selection_quality_global_only_adjusted_share), 6)
        if isinstance(selection_quality_global_only_adjusted_share, float)
        else None
    ),
    "selection_quality_global_only_total_share": (
        round(float(selection_quality_global_only_total_share), 6)
        if isinstance(selection_quality_global_only_total_share, float)
        else None
    ),
    "selection_quality_global_only_share_delta_pp": (
        round(float(selection_quality_global_only_share_delta_pp), 6)
        if isinstance(selection_quality_global_only_share_delta_pp, float)
        else None
    ),
    "selection_quality_global_only_pressure_active": bool(selection_quality_global_only_pressure_active),
    "selection_quality_global_only_drift_rising": bool(selection_quality_global_only_drift_rising),
    "selection_quality_adjusted_rate": (
        round(float(selection_quality_adjusted_rate), 6)
        if isinstance(selection_quality_adjusted_rate, float)
        else None
    ),
    "selection_quality_adjusted_bucket_backed_rate": (
        round(float(selection_quality_bucket_backed_rate), 6)
        if isinstance(selection_quality_bucket_backed_rate, float)
        else None
    ),
    "live_recommendation": live_recommendation,
    "limiting_factor": display_limiting_factor,
    "approval_rate": round(float(approval_rate), 6),
    "plan_conversion_rate": (
        round(float(approval_to_plan_rate), 6)
        if isinstance(approval_to_plan_rate, float)
        else None
    ),
    "top_blocker_reason": top_blocker_reason or None,
    "top_blocker_share_of_blocked": round(float(top_blocker_share_of_blocked), 6),
    "freshness_block_rate": round(float(stale_rate), 6),
    "sparse_hardening_applied_count": int(sparse_hardening_applied_count),
    "sparse_hardening_applied_rate": (
        round(float(sparse_hardening_applied_rate), 6)
        if isinstance(sparse_hardening_applied_rate, float)
        else None
    ),
    "sparse_hardening_basis": sparse_hardening_basis,
    "sparse_hardening_files_used_count": int(rolling_sparse_hardening_files_used_count),
    "sparse_hardening_sample_intents_total": int(sparse_hardening_sample_intents_total),
    "sparse_hardening_sample_share_of_window": (
        round(float(sparse_hardening_sample_share_of_window), 6)
        if isinstance(sparse_hardening_sample_share_of_window, float)
        else None
    ),
    "sparse_hardening_blocked_count": int(sparse_hardening_blocked_count),
    "sparse_hardening_approved_count": int(sparse_hardening_approved_count),
    "sparse_hardening_blocked_share_of_hardened": (
        round(float(sparse_hardening_blocked_share_of_hardened), 6)
        if isinstance(sparse_hardening_blocked_share_of_hardened, float)
        else None
    ),
    "sparse_hardening_probability_raise_avg": (
        round(float(sparse_hardening_probability_raise_avg), 6)
        if isinstance(sparse_hardening_probability_raise_avg, float)
        else None
    ),
    "sparse_hardening_expected_edge_raise_avg": (
        round(float(sparse_hardening_expected_edge_raise_avg), 6)
        if isinstance(sparse_hardening_expected_edge_raise_avg, float)
        else None
    ),
    "historical_profitability_guardrail_applied_rate": (
        round(float(profitability_guardrail_global_applied_rate), 6)
        if isinstance(profitability_guardrail_global_applied_rate, float)
        else None
    ),
    "historical_profitability_guardrail_penalty_ratio_avg": (
        round(float(profitability_guardrail_global_penalty_ratio_avg), 6)
        if isinstance(profitability_guardrail_global_penalty_ratio_avg, float)
        else None
    ),
    "historical_profitability_guardrail_blocked_expected_edge_count": int(
        profitability_guardrail_global_blocked_expected_edge_count
    ),
    "historical_profitability_guardrail_blocked_expected_edge_below_min_count": int(
        profitability_guardrail_global_blocked_expected_edge_count
    ),
    "historical_profitability_bucket_guardrail_applied_rate": (
        round(float(profitability_guardrail_bucket_applied_rate), 6)
        if isinstance(profitability_guardrail_bucket_applied_rate, float)
        else None
    ),
    "historical_profitability_bucket_guardrail_penalty_ratio_avg": (
        round(float(profitability_guardrail_bucket_penalty_ratio_avg), 6)
        if isinstance(profitability_guardrail_bucket_penalty_ratio_avg, float)
        else None
    ),
    "historical_profitability_bucket_guardrail_blocked_expected_edge_count": int(
        profitability_guardrail_bucket_blocked_expected_edge_count
    ),
    "historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count": int(
        profitability_guardrail_bucket_blocked_expected_edge_count
    ),
    "historical_profitability_rank_penalty_global_avg": (
        round(float(profitability_guardrail_rank_global_penalty_term_avg), 6)
        if isinstance(profitability_guardrail_rank_global_penalty_term_avg, float)
        else None
    ),
    "historical_profitability_rank_penalty_bucket_avg": (
        round(float(profitability_guardrail_rank_bucket_penalty_term_avg), 6)
        if isinstance(profitability_guardrail_rank_bucket_penalty_term_avg, float)
        else None
    ),
    "historical_profitability_rank_scored_candidate_count": int(
        profitability_guardrail_rank_scored_candidate_count
    ),
    "profitability_guardrail_line": profitability_guardrail_line,
    "resolved_unique_market_sides": int(resolved_market_sides),
    "unresolved_unique_market_sides": int(unresolved_market_sides),
    "resolved_unique_underlying_families": int(resolved_families),
    "projected_pnl_reference_bankroll_dollars": (
        round(float(projected_pnl_dollars), 6)
        if isinstance(projected_pnl_dollars, float)
        else None
    ),
    "projected_roi_reference_bankroll": round(float(projected_roi_ratio), 6),
    "trial_balance_current_dollars": (
        round(float(trial_current), 6) if isinstance(trial_current, float) else None
    ),
    "trial_balance_delta_dollars": (
        round(float(trial_growth), 6) if isinstance(trial_growth, float) else None
    ),
    "checkin_pnl_1d_dollars": (
        round(float(trader_checkin_1d_pnl), 6)
        if isinstance(trader_checkin_1d_pnl, float)
        else None
    ),
    "checkin_pnl_7d_dollars": (
        round(float(trader_checkin_7d_pnl), 6)
        if isinstance(trader_checkin_7d_pnl, float)
        else None
    ),
    "checkin_month_window_label": trader_checkin_month_label,
    "checkin_month_pnl_dollars": (
        round(float(trader_checkin_month_pnl), 6)
        if isinstance(trader_checkin_month_pnl, float)
        else None
    ),
    "settled_prediction_win_rate": (
        round(float(prediction_win_rate), 6)
        if isinstance(prediction_win_rate, float)
        else None
    ),
    "settled_predictions_available": bool(has_settled_predictions),
    "suggestion_impact_pool_basis": impact_pool_basis,
    "suggestion_impact_pool_basis_label": impact_pool_basis_label_compact,
    "top3_action_impact_proxy_dollars": (
        round(float(top3_action_impact_proxy), 6)
        if isinstance(top3_action_impact_proxy, float)
        else None
    ),
    "top3_action_impact_estimate_dollars": (
        round(float(top3_action_impact_proxy), 6)
        if isinstance(top3_action_impact_proxy, float)
        else None
    ),
    "next_actions_top3": [
        _compact_suggestion_line(item, 96)
        for item in concise_display_suggestions
        if _normalize(item)
    ],
    "best_next_action": _normalize(tldr_best_action) or None,
    "best_next_action_key": _normalize(tldr_best_action_source_key) or None,
    "best_next_action_quantified": bool(tldr_best_action_source_quantified),
}
payload = {
    "status": "ready",
    "captured_at": captured_at_utc.isoformat(),
    "generated_at_utc": captured_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "label": f"alpha_summary_{window_label}",
    "window_label": window_label,
    "health": {
        "status": health_status,
        "runtime_status": runtime_health_status,
        "issues": health_reasons,
        "reason_text": health_reason_text,
        "issue_count": int(len(health_reasons)),
    },
    "runtime_stage_seconds": runtime_stage_seconds,
    "runtime_stage_seconds_known_total": int(runtime_stage_seconds_known_total),
    "trader_view": trader_view,
    "source_files": {
        "window_summary_file": str(window_summary_path),
        "metar_summary_file": metar_summary_source_file or None,
        "settlement_summary_file": settlement_summary_source_file or None,
        "shadow_summary_file": shadow_summary_source_file or None,
        "metar_summary_file_resolution": metar_summary_source_resolution,
        "settlement_summary_file_resolution": settlement_summary_source_resolution,
        "shadow_summary_file_resolution": shadow_summary_source_resolution,
        "profitability_file": profitability_file,
        "bankroll_validation_file": bankroll_path,
        "alpha_gap_file": alpha_gap_path,
        "live_status_file": live_status_path,
        "live_readiness_file": live_readiness_path,
        "go_live_gate_file": go_live_gate_path,
        "blocker_audit_file": blocker_audit_path,
        "latest_plan_summary_file": plan_summary_path,
        "latest_intents_summary_file": intents_summary_path,
        "selection_quality_file": selection_quality_path,
        "window_summary_load_status": window_summary_load_status,
        "profitability_load_status": profitability_load_status,
        "bankroll_validation_load_status": bankroll_load_status,
        "alpha_gap_load_status": alpha_gap_load_status,
        "live_status_primary_load_status": live_status_primary_load_status,
        "live_status_effective_load_status": live_status_effective_load_status,
        "live_readiness_load_status": live_readiness_load_status,
        "go_live_gate_load_status": go_live_gate_load_status,
        "blocker_audit_load_status": blocker_audit_load_status,
        "latest_plan_summary_load_status": latest_plan_summary_load_status,
        "latest_intents_summary_load_status": intents_summary_load_status,
        "previous_alpha_summary_load_status": previous_alpha_summary_load_status,
        "artifact_parse_error_keys": artifact_parse_error_keys,
        "critical_artifact_parse_error_keys": critical_artifact_parse_error_keys,
        "window_intent_summary_files_in_window_count": int(len(window_intent_summary_files)),
        "window_intent_summary_files_used_count": int(intent_quality_source_summary_files_count),
        "window_intent_summary_files_used_preview": intent_quality_source_summary_files_preview,
        "window_intent_csv_files_used_count": int(intent_quality_source_csv_files_count),
        "window_intent_csv_files_used_preview": intent_quality_source_csv_files_preview,
        "guardrail_recent_summary_files_target": int(guardrail_recent_summary_files),
        "guardrail_recent_summary_files_used_count": int(recent_guardrail_files_used_count),
        "guardrail_recent_summary_files_used_preview": list(
            recent_guardrail_sample.get("files_used_preview") or []
        ),
    },
    "headline_metrics": {
        "health_status": health_status,
        "health_runtime_status": runtime_health_status,
        "best_next_action": _normalize(tldr_best_action) or None,
        "best_next_action_key": _normalize(tldr_best_action_source_key) or None,
        "best_next_action_quantified": bool(tldr_best_action_source_quantified),
        "confidence_level": confidence_level,
        "intents_total": intents_total,
        "intents_approved": intents_approved,
        "approval_rate": round(approval_rate, 6),
        "selection_quality_rows_total": int(selection_quality_rows_total),
        "selection_quality_rows_adjusted": int(selection_quality_rows_adjusted),
        "selection_quality_rows_adjusted_bucket_backed": int(selection_quality_rows_adjusted_bucket_backed),
        "selection_quality_rows_adjusted_global_only": int(selection_quality_rows_adjusted_global_only),
        "selection_quality_global_only_adjusted_share": (
            round(float(selection_quality_global_only_adjusted_share), 6)
            if isinstance(selection_quality_global_only_adjusted_share, float)
            else None
        ),
        "selection_quality_global_only_total_share": (
            round(float(selection_quality_global_only_total_share), 6)
            if isinstance(selection_quality_global_only_total_share, float)
            else None
        ),
        "selection_quality_global_only_share_delta_pp": (
            round(float(selection_quality_global_only_share_delta_pp), 6)
            if isinstance(selection_quality_global_only_share_delta_pp, float)
            else None
        ),
        "selection_quality_global_only_pressure_active": bool(selection_quality_global_only_pressure_active),
        "selection_quality_global_only_drift_rising": bool(selection_quality_global_only_drift_rising),
        "selection_quality_adjusted_rate": (
            round(float(selection_quality_adjusted_rate), 6)
            if isinstance(selection_quality_adjusted_rate, float)
            else None
        ),
        "selection_quality_adjusted_bucket_backed_rate": (
            round(float(selection_quality_bucket_backed_rate), 6)
            if isinstance(selection_quality_bucket_backed_rate, float)
            else None
        ),
        "approval_guardrail_min_rate": round(approval_rate_guardrail_min, 6),
        "approval_guardrail_max_rate": round(approval_rate_guardrail_max, 6),
        "approval_guardrail_critical_high_rate": round(approval_rate_guardrail_critical_high, 6),
        "approval_guardrail_min_intents": int(approval_rate_guardrail_min_intents),
        "approval_guardrail_min_required_intents": int(approval_guardrail_min_required_intents),
        "approval_guardrail_evaluated": bool(approval_guardrail_evaluated),
        "approval_guardrail_status": approval_guardrail_status,
        "approval_guardrail_reason": approval_guardrail_reason,
        "approval_guardrail_alert_level": approval_guardrail_alert_level,
        "approval_guardrail_basis": guardrail_basis,
        "approval_guardrail_basis_reason": guardrail_basis_reason,
        "approval_guardrail_basis_intents_total": int(guardrail_intents_total),
        "approval_guardrail_basis_rate": round(float(guardrail_approval_rate), 6),
        "approval_guardrail_basis_min_ratio_to_window": round(float(guardrail_basis_min_ratio_to_window), 6),
        "approval_guardrail_basis_min_abs_intents": int(guardrail_basis_min_abs_intents),
        "approval_guardrail_latest_ratio_to_window": (
            round(float(latest_guardrail_ratio_to_window), 6)
            if isinstance(latest_guardrail_ratio_to_window, float)
            else None
        ),
        "approval_guardrail_latest_sample_eligible": bool(latest_guardrail_sample_eligible),
        "approval_guardrail_window_intents_total": int(window_intents_total),
        "approval_guardrail_window_rate": round(float(window_approval_rate), 6),
        "approval_guardrail_latest_intents_total": int(latest_guardrail_intents_total),
        "approval_guardrail_latest_rate": (
            round(float(latest_guardrail_approval_rate), 6)
            if isinstance(latest_guardrail_approval_rate, float)
            else None
        ),
        "approval_guardrail_recent_intents_total": int(recent_guardrail_intents_total),
        "approval_guardrail_recent_intents_approved": int(recent_guardrail_intents_approved),
        "approval_guardrail_recent_rate": (
            round(float(recent_guardrail_approval_rate), 6)
            if isinstance(recent_guardrail_approval_rate, float)
            else None
        ),
        "approval_guardrail_recent_files_used_count": int(recent_guardrail_files_used_count),
        "approval_guardrail_recent_sample_eligible": bool(recent_guardrail_sample_eligible),
        "approval_guardrail_intent_quality_source": intent_quality_source,
        "approval_guardrail_intent_quality_rows": int(len(intent_quality_rows)),
        "approval_guardrail_intent_quality_summary_files_count": int(intent_quality_source_summary_files_count),
        "approval_guardrail_intent_quality_csv_files_count": int(intent_quality_source_csv_files_count),
        "approval_guardrail_status_line": guardrail_status_line,
        "approval_guardrail_context_line": guardrail_context_line,
        "approval_auto_apply_enabled": bool(auto_apply_enabled),
        "approval_auto_apply_candidate_ready": bool(auto_apply_candidate_ready),
        "approval_auto_apply_recommendation_fit": bool(auto_apply_recommendation_fit),
        "approval_auto_apply_breach_active": bool(approval_guardrail_breach_active),
        "approval_auto_apply_breach_streak": int(auto_apply_breach_streak),
        "approval_auto_apply_trigger": auto_apply_trigger,
        "approval_auto_apply_trigger_streak_current": int(auto_apply_trigger_streak_current),
        "approval_auto_apply_trigger_streak_required": int(auto_apply_trigger_streak_required),
        "approval_auto_apply_rows_available": int(auto_apply_rows_available),
        "approval_auto_apply_required_rows_for_trigger": int(auto_apply_required_rows_for_trigger),
        "approval_auto_apply_approval_trigger_rows_satisfied": bool(approval_trigger_rows_satisfied),
        "approval_auto_apply_gate_trigger_rows_satisfied": bool(gate_trigger_rows_satisfied),
        "approval_auto_apply_gate_breach_active": bool(gate_coverage_auto_apply_breach_active),
        "approval_auto_apply_gate_breach_streak": int(auto_apply_gate_breach_streak),
        "approval_auto_apply_gate_min_level": gate_coverage_auto_apply_min_level,
        "approval_auto_apply_gate_min_rows": int(gate_coverage_auto_apply_min_rows),
        "approval_auto_apply_gate_streak_required": int(gate_coverage_auto_apply_streak_required),
        "approval_auto_apply_recommendation_mode": auto_apply_recommendation_mode_effective,
        "approval_auto_apply_recommendation_mode_preferred": auto_apply_recommendation_mode,
        "approval_auto_apply_strict_guardrail_max": round(float(strict_guardrail_max), 6),
        "approval_auto_apply_quality_risk_projected_streak_for_auto": int(
            quality_risk_projected_streak_for_auto
        ),
        "approval_auto_apply_quality_risk_persistent_for_auto": bool(quality_risk_persistent_for_auto),
        "approval_auto_apply_clear_streak": int(auto_apply_clear_streak),
        "approval_auto_apply_streak_required": int(auto_apply_streak_required),
        "approval_auto_apply_min_rows": int(auto_apply_min_rows),
        "approval_auto_apply_guardrail_basis_min_abs_intents": int(guardrail_basis_min_abs_intents),
        "approval_auto_apply_guardrail_min_required_intents": int(approval_guardrail_min_required_intents),
        "approval_auto_apply_min_rows_satisfied": bool(auto_apply_min_rows_satisfied),
        "approval_auto_apply_recommendation_ready": bool(auto_apply_recommendation_ready),
        "approval_auto_apply_release_enabled": bool(auto_apply_release_enabled),
        "approval_auto_apply_release_streak_required": int(auto_apply_release_streak_required),
        "approval_auto_apply_zero_approved_streak": int(auto_apply_zero_approved_streak),
        "approval_auto_apply_zero_approved_streak_required": int(auto_apply_zero_approved_streak_required),
        "approval_auto_apply_zero_approved_min_intents": int(auto_apply_zero_approved_min_intents),
        "approval_auto_apply_zero_approved_candidate": bool(auto_apply_zero_approved_candidate),
        "approval_auto_apply_latest_sample_eligible": bool(latest_guardrail_sample_eligible),
        "approval_auto_apply_latest_summary_intents_total": int(latest_guardrail_intents_total),
        "approval_auto_apply_latest_summary_intents_approved": int(latest_approved_for_zero_guard),
        "approval_auto_apply_applied": bool(auto_apply_applied),
        "approval_auto_apply_released": bool(auto_apply_released),
        "approval_auto_apply_apply_reason": auto_apply_apply_reason,
        "approval_auto_apply_status_line": auto_apply_status_line,
        "approval_auto_apply_profile_write_attempted": bool(auto_apply_profile_write_attempted),
        "approval_auto_apply_profile_write_ok": bool(auto_apply_profile_write_ok),
        "approval_auto_apply_state_write_ok": bool(auto_apply_state_write_ok),
        "approval_parameter_audit_status": approval_audit_status,
        "approval_parameter_audit_approved_rows": int(approval_audit_approved_rows),
        "approval_parameter_audit_mismatch_rows": int(approval_audit_mismatch_rows),
        "approval_parameter_audit_mismatch_rate": (
            round(float(approval_audit_mismatch_rate), 6)
            if isinstance(approval_audit_mismatch_rate, float)
            else None
        ),
        "approval_parameter_audit_revalidation_conflicts": int(approval_audit_revalidation_conflicts),
        "approval_parameter_audit_revalidation_conflicts_rate": (
            round(float(approval_audit_revalidation_conflicts_rate), 6)
            if isinstance(approval_audit_revalidation_conflicts_rate, float)
            else None
        ),
        # Legacy aliases retained for downstream consumers still expecting
        # pre-refactor key names.
        "approval_audit_approved_rows": int(approval_audit_approved_rows),
        "approval_audit_mismatch_rows": int(approval_audit_mismatch_rows),
        "approval_audit_mismatch_rate": (
            round(float(approval_audit_mismatch_rate), 6)
            if isinstance(approval_audit_mismatch_rate, float)
            else None
        ),
        "approval_audit_revalidation_conflicts": int(approval_audit_revalidation_conflicts),
        "approval_audit_revalidation_conflicts_rate": (
            round(float(approval_audit_revalidation_conflicts_rate), 6)
            if isinstance(approval_audit_revalidation_conflicts_rate, float)
            else None
        ),
        "approval_gate_coverage_metar_ratio": (
            round(float(approval_gate_coverage_metar_ratio), 6)
            if isinstance(approval_gate_coverage_metar_ratio, float)
            else None
        ),
        "approval_gate_coverage_metar_active_ratio": (
            round(float(approval_gate_coverage_metar_active_ratio), 6)
            if isinstance(approval_gate_coverage_metar_active_ratio, float)
            else None
        ),
        "approval_gate_coverage_expected_edge_ratio": (
            round(float(approval_gate_coverage_expected_edge_ratio), 6)
            if isinstance(approval_gate_coverage_expected_edge_ratio, float)
            else None
        ),
        "approval_gate_coverage_expected_edge_active_ratio": (
            round(float(approval_gate_coverage_expected_edge_active_ratio), 6)
            if isinstance(approval_gate_coverage_expected_edge_active_ratio, float)
            else None
        ),
        "approval_gate_coverage_probability_ratio": (
            round(float(approval_gate_coverage_probability_ratio), 6)
            if isinstance(approval_gate_coverage_probability_ratio, float)
            else None
        ),
        "approval_gate_coverage_probability_active_ratio": (
            round(float(approval_gate_coverage_probability_active_ratio), 6)
            if isinstance(approval_gate_coverage_probability_active_ratio, float)
            else None
        ),
        "approval_gate_coverage_alpha_ratio": (
            round(float(approval_gate_coverage_alpha_ratio), 6)
            if isinstance(approval_gate_coverage_alpha_ratio, float)
            else None
        ),
        "approval_gate_coverage_alpha_active_ratio": (
            round(float(approval_gate_coverage_alpha_active_ratio), 6)
            if isinstance(approval_gate_coverage_alpha_active_ratio, float)
            else None
        ),
        "approval_gate_coverage_overall_ratio": round(float(selection_gate_coverage_ratio), 6),
        "approval_parameter_gate_coverage_line": approval_gate_coverage_line,
        "approval_parameter_gate_activity_line": approval_gate_activity_line,
        "approval_gate_coverage_active_line": approval_gate_coverage_line,
        "approval_gate_coverage_active_summary": approval_gate_activity_line,
        "approval_parameter_mismatch_top_gates": [
            {"gate": key, "count": int(count)}
            for key, count in approval_mismatch_top_gates
        ],
        "profitability_csv_cache_degraded": bool(csv_cache_degraded),
        "profitability_csv_cache_enabled": bool(csv_cache_enabled),
        "profitability_csv_cache_write_access": bool(csv_cache_write_access),
        "profitability_csv_cache_commit_ok": bool(csv_cache_commit_ok),
        "profitability_csv_cache_puts_failed": int(csv_cache_puts_failed),
        "profitability_csv_cache_puts_failed_readonly": int(csv_cache_puts_failed_readonly),
        "profitability_csv_cache_path_fallback_reason": csv_cache_path_fallback_reason,
        "quality_gate_source": quality_gate_source,
        "quality_gate_profile_data_source": quality_gate_profile_data_source,
        "quality_gate_auto_applied": bool(quality_gate_auto_applied),
        "quality_gate_detail": quality_gate_detail,
        "quality_gate_min_settlement_confidence": (
            round(float(quality_gate_min_settlement), 6)
            if isinstance(quality_gate_min_settlement, float)
            else None
        ),
        "quality_gate_min_alpha_strength": (
            round(float(quality_gate_min_alpha), 6)
            if isinstance(quality_gate_min_alpha, float)
            else None
        ),
        "quality_gate_min_probability_confidence": (
            round(float(quality_gate_min_probability), 6)
            if isinstance(quality_gate_min_probability, float)
            else None
        ),
        "quality_gate_min_expected_edge_net": (
            round(float(quality_gate_min_edge), 6)
            if isinstance(quality_gate_min_edge, float)
            else None
        ),
        "quality_gate_guardrail_escalation_applied": bool(quality_gate_escalation_applied),
        "quality_gate_guardrail_escalation_status": quality_gate_escalation_status,
        "quality_gate_guardrail_watch_only": bool(quality_gate_guardrail_watch_only),
        "quality_gate_guardrail_sample_gated": bool(quality_gate_guardrail_sample_gated),
        "quality_gate_guardrail_escalation_multiplier": (
            round(float(quality_gate_escalation_multiplier), 6)
            if isinstance(quality_gate_escalation_multiplier, float)
            else None
        ),
        "quality_gate_guardrail_escalation_sample_approval_rate": (
            round(float(quality_gate_escalation_sample_approval_rate), 6)
            if isinstance(quality_gate_escalation_sample_approval_rate, float)
            else None
        ),
        "quality_gate_guardrail_escalation_sample_intents_total": (
            int(quality_gate_escalation_sample_intents_total)
            if isinstance(quality_gate_escalation_sample_intents_total, int)
            else None
        ),
        "quality_gate_guardrail_escalation_sample_source": quality_gate_escalation_sample_source,
        "quality_gate_guardrail_escalation_sample_captured_at": (
            quality_gate_escalation_sample_captured_at or None
        ),
        "quality_gate_guardrail_escalation_sample_age_seconds": (
            round(float(quality_gate_escalation_sample_age_seconds), 1)
            if isinstance(quality_gate_escalation_sample_age_seconds, float)
            else None
        ),
        "quality_gate_guardrail_escalation_min_intents_required": (
            int(quality_gate_escalation_min_intents_required)
            if isinstance(quality_gate_escalation_min_intents_required, int)
            else None
        ),
        "quality_gate_guardrail_escalation_min_intents_base": (
            int(quality_gate_escalation_min_intents_base)
            if isinstance(quality_gate_escalation_min_intents_base, int)
            else None
        ),
        "quality_gate_guardrail_escalation_basis_min_abs_intents": (
            int(quality_gate_escalation_basis_min_abs_intents)
            if isinstance(quality_gate_escalation_basis_min_abs_intents, int)
            else None
        ),
        "quality_gate_guardrail_escalation_detail": quality_gate_escalation_detail,
        "quality_risk_alert_active": bool(quality_risk_alert_active),
        "quality_risk_alert_level": quality_risk_alert_level,
        "quality_risk_alert_reasons": list(quality_risk_alert_reasons),
        "selection_confidence_score": round(float(selection_confidence_score), 3),
        "selection_confidence_band": selection_confidence_band,
        "selection_confidence_guidance": selection_confidence_guidance,
        "selection_confidence_outcome_multiplier": round(
            float(selection_outcome_multiplier),
            6,
        ),
        "selection_confidence_settled_win_rate_lb95": (
            round(float(selection_settled_win_rate_lb95), 6)
            if isinstance(selection_settled_win_rate_lb95, float)
            else None
        ),
        "selection_confidence_settled_strength": round(
            float(selection_settled_confidence_strength),
            6,
        ),
        "selection_confidence_gate_coverage_ratio": round(
            float(selection_gate_coverage_ratio),
            6,
        ),
        "selection_confidence_gate_coverage_basis": selection_gate_coverage_basis,
        "selection_confidence_component_summary": selection_confidence_component_summary,
        "selection_confidence_top_drivers": [
            {"driver": label, "points": round(float(points), 3)}
            for label, points in selection_confidence_top_drivers
        ],
        "selection_confidence_driver_line": selection_confidence_driver_line or None,
        "deploy_confidence_score": round(float(deployment_confidence_score), 3),
        "deploy_confidence_score_uncapped": round(float(deployment_confidence_score_uncapped), 3),
        "settled_evidence_confidence_score": (
            round(float(settled_evidence_confidence_score), 3)
            if has_settled_predictions and isinstance(settled_evidence_confidence_score, float)
            else None
        ),
        "settled_evidence_confidence_band": (
            settled_evidence_confidence_band
            if has_settled_predictions and _normalize(settled_evidence_confidence_band)
            else None
        ),
        "planned_orders": planned_orders,
        "planned_orders_total": planned_orders,
        "trial_resolved_since_reset": trial_resolved_since_reset,
        "trial_wins_since_reset": trial_wins_since_reset,
        "trial_losses_since_reset": trial_losses_since_reset,
        "trial_resolved_share_unique_orders": (
            round(float(trial_resolved_share_unique_orders), 6)
            if isinstance(trial_resolved_share_unique_orders, float)
            else None
        ),
        "trial_counterfactual_pnl_since_reset_dollars": (
            round(float(trial_cumulative_counterfactual_pnl), 6)
            if isinstance(trial_cumulative_counterfactual_pnl, float)
            else None
        ),
        "trial_balance_negative": bool(trial_balance_negative),
        "window_comparison_available": bool(window_has_previous),
        "window_delta_intents_total": int(window_delta_intents) if isinstance(window_delta_intents, int) else None,
        "window_delta_intents_approved": int(window_delta_approved)
        if isinstance(window_delta_approved, int)
        else None,
        "window_delta_planned_orders_total": int(window_delta_planned)
        if isinstance(window_delta_planned, int)
        else None,
        "window_delta_approval_rate_percentage_points": (
            round(float(window_delta_approval_pp), 6)
            if isinstance(window_delta_approval_pp, float)
            else None
        ),
        "window_delta_stale_block_rate_percentage_points": (
            round(float(window_delta_stale_pp), 6)
            if isinstance(window_delta_stale_pp, float)
            else None
        ),
        "window_delta_resolved_unique_market_sides": (
            int(window_delta_resolved_market_sides)
            if isinstance(window_delta_resolved_market_sides, int)
            else None
        ),
        "window_delta_trial_window_1d_pnl_dollars": (
            round(float(window_delta_trial_1d_pnl), 6)
            if isinstance(window_delta_trial_1d_pnl, float)
            else None
        ),
        "quality_drift_alert_active": bool(quality_drift_alert_active),
        "quality_drift_alert_level": quality_drift_alert_level,
        "quality_drift_alert_reason": quality_drift_alert_reason or None,
        "quality_drift_alert_summary": quality_drift_alert_summary or None,
        "quality_drift_direction": quality_drift_direction,
        "quality_drift_delta_threshold_pp": round(float(quality_drift_delta_pp_min), 6),
        "quality_drift_delta_approval_pp": (
            round(float(window_delta_approval_pp), 6)
            if isinstance(window_delta_approval_pp, float)
            else None
        ),
        "quality_drift_delta_approval_abs_pp": (
            round(float(quality_drift_approval_delta_abs_pp), 6)
            if isinstance(quality_drift_approval_delta_abs_pp, float)
            else None
        ),
        "quality_drift_window_eligible": bool(quality_drift_window_eligible),
        "quality_drift_min_intents_per_window": int(quality_drift_min_intents_per_window),
        "quality_drift_previous_intents": (
            int(window_previous_intents)
            if isinstance(window_previous_intents, int)
            else None
        ),
        "quality_drift_current_intents": int(intents_total),
        "quality_drift_max_resolved_sides_delta": int(quality_drift_max_resolved_sides_delta),
        "quality_drift_resolved_sides_delta": (
            int(window_delta_resolved_market_sides)
            if isinstance(window_delta_resolved_market_sides, int)
            else None
        ),
        "quality_drift_previous_approval_rate": (
            round(float(window_previous_approval_rate), 6)
            if isinstance(window_previous_approval_rate, float)
            else None
        ),
        "quality_drift_current_approval_rate": round(float(approval_rate), 6),
        "selection_quality_global_only_alert_active": bool(selection_quality_global_only_alert_active),
        "selection_quality_global_only_alert_level": selection_quality_global_only_alert_level,
        "selection_quality_global_only_alert_reason": selection_quality_global_only_alert_reason or None,
        "selection_quality_global_only_alert_summary": selection_quality_global_only_alert_summary or None,
        "selection_quality_global_only_alert_min_rows": int(selection_quality_min_rows_for_global_only_alert),
        "selection_quality_global_only_alert_target_share": round(
            float(selection_quality_global_only_target_share),
            6,
        ),
        "selection_quality_global_only_alert_drift_delta_pp_threshold": round(
            float(selection_quality_global_only_drift_alert_delta_pp),
            6,
        ),
        "gate_coverage_alert_active": bool(gate_coverage_alert_active),
        "gate_coverage_alert_level": gate_coverage_alert_level,
        "gate_coverage_alert_reason": gate_coverage_alert_reason or None,
        "gate_coverage_alert_summary": gate_coverage_alert_summary or None,
        "gate_coverage_alert_rows_eligible": bool(gate_coverage_alert_rows_eligible),
        "gate_coverage_alert_min_approved_rows": int(gate_coverage_alert_min_approved_rows),
        "gate_coverage_alert_worst_shortfall_ratio": (
            round(float(gate_coverage_alert_worst_shortfall_ratio), 6)
            if isinstance(gate_coverage_alert_worst_shortfall_ratio, float)
            else None
        ),
        "gate_coverage_alert_gap_count": int(len(gate_coverage_alert_gaps)),
        "gate_coverage_alert_expected_edge_min": round(float(gate_coverage_expected_edge_min), 6),
        "gate_coverage_alert_probability_min": round(float(gate_coverage_probability_min), 6),
        "gate_coverage_alert_alpha_min": round(float(gate_coverage_alpha_min), 6),
        "stale_block_rate": round(stale_rate, 6),
        "metar_observation_stale_count": int(metar_observation_stale_count),
        "metar_freshness_boundary_quality_insufficient_count": int(metar_freshness_boundary_quality_count),
        "metar_freshness_total_blocked_count": int(stale_count),
        "sparse_hardening_applied_count": int(sparse_hardening_applied_count),
        "sparse_hardening_applied_rate": (
            round(float(sparse_hardening_applied_rate), 6)
            if isinstance(sparse_hardening_applied_rate, float)
            else None
        ),
        "sparse_hardening_basis": sparse_hardening_basis,
        "sparse_hardening_files_used_count": int(rolling_sparse_hardening_files_used_count),
        "sparse_hardening_sample_intents_total": int(sparse_hardening_sample_intents_total),
        "sparse_hardening_sample_share_of_window": (
            round(float(sparse_hardening_sample_share_of_window), 6)
            if isinstance(sparse_hardening_sample_share_of_window, float)
            else None
        ),
        "sparse_hardening_approved_count": int(sparse_hardening_approved_count),
        "sparse_hardening_blocked_count": int(sparse_hardening_blocked_count),
        "sparse_hardening_blocked_share_of_hardened": (
            round(float(sparse_hardening_blocked_share_of_hardened), 6)
            if isinstance(sparse_hardening_blocked_share_of_hardened, float)
            else None
        ),
        "sparse_hardening_blocked_share_of_blocked": (
            round(float(sparse_hardening_blocked_share_of_blocked), 6)
            if isinstance(sparse_hardening_blocked_share_of_blocked, float)
            else None
        ),
        "sparse_hardening_probability_block_share": (
            round(float(sparse_hardening_probability_block_share), 6)
            if isinstance(sparse_hardening_probability_block_share, float)
            else None
        ),
        "sparse_hardening_expected_edge_block_share": (
            round(float(sparse_hardening_expected_edge_block_share), 6)
            if isinstance(sparse_hardening_expected_edge_block_share, float)
            else None
        ),
        "sparse_hardening_edge_to_risk_block_share": (
            round(float(sparse_hardening_edge_to_risk_block_share), 6)
            if isinstance(sparse_hardening_edge_to_risk_block_share, float)
            else None
        ),
        "sparse_hardening_probability_raise_avg": (
            round(float(sparse_hardening_probability_raise_avg), 6)
            if isinstance(sparse_hardening_probability_raise_avg, float)
            else None
        ),
        "sparse_hardening_probability_raise_max": (
            round(float(sparse_hardening_probability_raise_max), 6)
            if isinstance(sparse_hardening_probability_raise_max, float)
            else None
        ),
        "sparse_hardening_expected_edge_raise_avg": (
            round(float(sparse_hardening_expected_edge_raise_avg), 6)
            if isinstance(sparse_hardening_expected_edge_raise_avg, float)
            else None
        ),
        "sparse_hardening_expected_edge_raise_max": (
            round(float(sparse_hardening_expected_edge_raise_max), 6)
            if isinstance(sparse_hardening_expected_edge_raise_max, float)
            else None
        ),
        "sparse_hardening_support_score_avg": (
            round(float(sparse_hardening_support_score_avg), 6)
            if isinstance(sparse_hardening_support_score_avg, float)
            else None
        ),
        "sparse_hardening_volatility_penalty_avg": (
            round(float(sparse_hardening_volatility_penalty_avg), 6)
            if isinstance(sparse_hardening_volatility_penalty_avg, float)
            else None
        ),
        "historical_profitability_guardrail_applied_rate": (
            round(float(profitability_guardrail_global_applied_rate), 6)
            if isinstance(profitability_guardrail_global_applied_rate, float)
            else None
        ),
        "historical_profitability_guardrail_penalty_ratio_avg": (
            round(float(profitability_guardrail_global_penalty_ratio_avg), 6)
            if isinstance(profitability_guardrail_global_penalty_ratio_avg, float)
            else None
        ),
        "historical_profitability_guardrail_blocked_expected_edge_below_min_count": int(
            profitability_guardrail_global_blocked_expected_edge_count
        ),
        "historical_profitability_guardrail_blocked_expected_edge_count": int(
            profitability_guardrail_global_blocked_expected_edge_count
        ),
        "historical_profitability_bucket_guardrail_applied_rate": (
            round(float(profitability_guardrail_bucket_applied_rate), 6)
            if isinstance(profitability_guardrail_bucket_applied_rate, float)
            else None
        ),
        "historical_profitability_bucket_guardrail_penalty_ratio_avg": (
            round(float(profitability_guardrail_bucket_penalty_ratio_avg), 6)
            if isinstance(profitability_guardrail_bucket_penalty_ratio_avg, float)
            else None
        ),
        "historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count": int(
            profitability_guardrail_bucket_blocked_expected_edge_count
        ),
        "historical_profitability_bucket_guardrail_blocked_expected_edge_count": int(
            profitability_guardrail_bucket_blocked_expected_edge_count
        ),
        "historical_profitability_rank_penalty_global_avg": (
            round(float(profitability_guardrail_rank_global_penalty_term_avg), 6)
            if isinstance(profitability_guardrail_rank_global_penalty_term_avg, float)
            else None
        ),
        "historical_profitability_rank_penalty_bucket_avg": (
            round(float(profitability_guardrail_rank_bucket_penalty_term_avg), 6)
            if isinstance(profitability_guardrail_rank_bucket_penalty_term_avg, float)
            else None
        ),
        "historical_profitability_rank_scored_candidate_count": int(
            profitability_guardrail_rank_scored_candidate_count
        ),
        "profitability_guardrail_line": profitability_guardrail_line,
        "profitability_guardrail_compact_line": profitability_guardrail_compact_line,
        "settlement_finalization_blocked_count": int(settlement_blocked_count),
        "settlement_finalization_blocked_count_actionable": int(settlement_blocked_count_actionable),
        "settlement_finalization_blocked_rate": (
            round((settlement_blocked_count / float(intents_total)), 6)
            if intents_total > 0
            else 0.0
        ),
        "interval_overlap_blocked_count": int(overlap_total_count),
        "interval_overlap_blocked_rate": round(overlap_rate, 6),
        "resolved_unique_market_sides": resolved_market_sides,
        "resolved_unique_underlying_families": resolved_families,
        "unresolved_unique_market_sides": unresolved_market_sides,
        "unresolved_unique_underlying_families": unresolved_families,
        "repeated_entry_multiplier": repeat_multiplier,
        "concentration_warning": concentration_warning,
        "top_blocker_reason": top_blocker_reason or None,
        "top_blocker_count": int(top_blocker_count),
        "top_blocker_share_of_blocked": round(float(top_blocker_share_of_blocked), 6),
        "edge_gate_blocked_count": int(edge_gate_blocked_count),
        "edge_gate_blocked_share_of_blocked": round(float(edge_gate_blocked_share_of_blocked), 6),
        "edge_gate_blocked_dominant": bool(edge_gate_blocked_dominant),
        "approval_quality_risk_alert_active": bool(quality_risk_alert_active),
        "approval_quality_risk_alert_level": quality_risk_alert_level,
        "approval_quality_risk_streak": int(quality_risk_streak),
        "approval_quality_risk_red_streak": int(quality_risk_red_streak),
        "approval_quality_risk_streak_red_required": int(quality_risk_streak_red_required),
        "approval_quality_risk_alert_reasons": list(quality_risk_alert_reasons),
        "projected_roi_on_bankroll": round(projected_roi_ratio, 6),
        "projected_pnl_on_reference_bankroll_dollars": round(projected_pnl_dollars, 6),
        "expected_edge_total_row_based_dollars": round(expected_edge_total, 6),
        "expected_edge_total_breadth_normalized_dollars": (
            round(expected_edge_total_breadth_normalized, 6)
            if isinstance(expected_edge_total_breadth_normalized, float)
            else None
        ),
        "expected_edge_per_planned_order_row_dollars": (
            round(expected_edge_per_planned_order, 6)
            if isinstance(expected_edge_per_planned_order, float)
            else None
        ),
        "expected_edge_per_resolved_unique_market_side_proxy_dollars": (
            round(expected_edge_per_unique_market_side_proxy, 6)
            if isinstance(expected_edge_per_unique_market_side_proxy, float)
            else None
        ),
        "settled_evidence_count": int(settled_evidence_count),
        "settled_evidence_full_at": int(settled_evidence_full_at),
        "settled_evidence_strength": round(float(settled_evidence_strength), 6),
        "expected_edge_proxy_discount_factor": round(
            float(impact_pool_proxy_discount_factor_effective), 6
        ),
        "expected_edge_proxy_discount_applied": bool(impact_pool_proxy_discount_applied),
        "suggestion_impact_pool_raw_dollars": (
            round(float(impact_pool_raw_dollars), 6)
            if isinstance(impact_pool_raw_dollars, float)
            else None
        ),
        "suggestion_impact_pool_discounted_dollars": (
            round(float(impact_pool_dollars), 6)
            if isinstance(impact_pool_dollars, float)
            else None
        ),
        "suggestion_impact_pool_basis": impact_pool_basis,
        "suggestion_impact_pool_basis_label": impact_pool_basis_label_compact,
        "beat_hysa": beat_hysa,
        "has_settled_predictions": has_settled_predictions,
        "settled_metrics_source": settled_metrics_source,
        "settled_metrics_source_reason": settled_metrics_source_reason or None,
        "settled_metrics_source_mismatch": bool(settled_metrics_source_mismatch),
        "settled_metrics_source_profitability_resolved_predictions": (
            int(profitability_resolved_predictions)
            if isinstance(profitability_resolved_predictions, int)
            else None
        ),
        "settled_metrics_source_bankroll_resolved_predictions": (
            int(bankroll_resolved_predictions)
            if isinstance(bankroll_resolved_predictions, int)
            else None
        ),
        "limiting_factor": limiting_factor,
        "display_limiting_factor": display_limiting_factor,
        "settled_unique_market_side_wins": settled_wins,
        "settled_unique_market_side_losses": settled_losses,
        "settled_unique_market_side_pushes": settled_pushes,
        "settled_unique_market_side_resolved_predictions": int(resolved_predictions),
        "settled_unique_market_side_prediction_win_rate": (
            round(float(prediction_win_rate), 6)
            if isinstance(prediction_win_rate, float)
            else None
        ),
        "settled_unique_market_side_expectancy_per_trade_dollars": (
            round(float(settled_expectancy_per_trade), 6)
            if isinstance(settled_expectancy_per_trade, float)
            else None
        ),
        "settled_unique_market_side_avg_win_dollars": (
            round(float(settled_avg_win), 6)
            if isinstance(settled_avg_win, float)
            else None
        ),
        "settled_unique_market_side_avg_loss_dollars": (
            round(float(settled_avg_loss), 6)
            if isinstance(settled_avg_loss, float)
            else None
        ),
        "settled_unique_market_side_payoff_ratio": (
            round(float(settled_payoff_ratio), 6)
            if isinstance(settled_payoff_ratio, float)
            else None
        ),
        "settled_unique_market_side_profit_factor": (
            round(float(settled_profit_factor), 6)
            if isinstance(settled_profit_factor, float)
            else None
        ),
        "settled_unique_market_side_counterfactual_pnl_dollars_if_live": settled_counterfactual_pnl,
        "last_resolved_unique_market_side": last_resolved_unique_market_side or None,
        "last_resolved_unique_shadow_order": last_resolved_unique_shadow_order or None,
        "trial_planned_rows_since_reset": int(trial_planned_rows_since_reset),
        "trial_unique_shadow_orders_since_reset": int(trial_unique_shadow_orders_since_reset),
        "trial_duplicate_rows_since_reset": int(trial_duplicate_count_since_reset),
        "trial_duplicate_unique_ids_since_reset": int(trial_duplicate_unique_ids_since_reset)
        if isinstance(trial_duplicate_unique_ids_since_reset, int)
        else None,
        "trial_duplicate_rows_ratio_since_reset": round(trial_duplicate_rows_ratio_since_reset, 6),
        "trial_balance_mode": trial_balance_mode,
        "trial_balance_starting_dollars": (
            round(float(trial_start), 6) if isinstance(trial_start, float) else None
        ),
        "trial_balance_current_dollars": (
            round(float(trial_current), 6) if isinstance(trial_current, float) else None
        ),
        "trial_balance_growth_dollars": (
            round(float(trial_growth), 6) if isinstance(trial_growth, float) else None
        ),
        "trial_balance_growth_rate": (
            round(float(trial_growth_pct), 6)
            if isinstance(trial_growth_pct, float)
            else None
        ),
        "settled_unique_market_side_total": int(resolved_predictions),
        "approval_to_plan_rate": (
            round(float(approval_to_plan_rate), 6)
            if isinstance(approval_to_plan_rate, float)
            else None
        ),
        "replan_cooldown_minutes": round(replan_cooldown_minutes, 6) if isinstance(replan_cooldown_minutes, float) else None,
        "replan_cooldown_input_count": int(replan_input_count),
        "replan_cooldown_kept_count": int(replan_kept_count),
        "replan_cooldown_blocked_count": int(replan_blocked_count),
        "replan_cooldown_override_count": int(replan_override_count),
        "replan_cooldown_backstop_released_count": int(replan_backstop_released_count),
        "replan_cooldown_blocked_ratio": round(replan_blocked_ratio, 6),
        "replan_cooldown_min_input_count": int(replan_cooldown_min_input_count),
        "replan_cooldown_sample_reliable": bool(replan_sample_reliable),
        "replan_repeat_window_minutes": (
            round(replan_repeat_window_minutes, 6)
            if isinstance(replan_repeat_window_minutes, float)
            else None
        ),
        "replan_repeat_max_plans_per_window": (
            int(replan_repeat_max_plans_per_window)
            if isinstance(replan_repeat_max_plans_per_window, int)
            else None
        ),
        "replan_repeat_window_input_count": int(replan_repeat_window_input_count),
        "replan_repeat_window_kept_count": int(replan_repeat_window_kept_count),
        "replan_repeat_cap_blocked_count": int(replan_repeat_cap_blocked_count),
        "replan_repeat_cap_override_count": int(replan_repeat_cap_override_count),
        "replan_repeat_cap_blocked_ratio": round(replan_repeat_cap_blocked_ratio, 6),
        "replan_repeat_cap_override_ratio": round(replan_repeat_cap_override_ratio, 6),
        "replan_repeat_cap_config": replan_repeat_cap_config,
        "replan_cooldown_adaptive_effective_minutes": (
            round(live_replan_effective_minutes, 6) if isinstance(live_replan_effective_minutes, float) else None
        ),
        "replan_cooldown_adaptive_next_minutes": (
            round(live_replan_next_minutes, 6) if isinstance(live_replan_next_minutes, float) else None
        ),
        "replan_cooldown_adaptive_action": live_replan_adaptive_action,
        "replan_cooldown_adaptive_reason": live_replan_adaptive_reason,
        "replan_cooldown_adaptive_min_orders_backstop": (
            int(live_replan_min_backstop) if isinstance(live_replan_min_backstop, int) else None
        ),
        "replan_cooldown_unique_market_sides": int(live_replan_unique_market_sides),
        "replan_cooldown_unique_underlyings": int(live_replan_unique_underlyings),
        "live_recommendation": live_recommendation,
        "ready_for_small_live_pilot": ready_small_live,
        "ready_for_scaled_live": ready_scaled_live,
        "earliest_passing_horizon": earliest_passing_horizon or None,
        "top_live_readiness_blocker_reason": top_live_readiness_blocker_reason or None,
        "deployment_confidence_score": round(deployment_confidence_score, 3),
        "deployment_confidence_score_uncapped": round(deployment_confidence_score_uncapped, 3),
        "deployment_confidence_band": deployment_confidence_band,
        "deployment_confidence_guidance": deployment_confidence_guidance,
        "deployment_confidence_cap_applied": bool(deployment_confidence_cap_applied),
        "deployment_confidence_cap_value": (
            round(float(deployment_confidence_cap_value), 3)
            if isinstance(deployment_confidence_cap_value, float)
            else None
        ),
        "deployment_confidence_cap_detail": deployment_confidence_cap_detail or None,
        "horizon_coverage_ratio": round(horizon_coverage_ratio, 6),
        "pilot_target_score": round(pilot_target_score, 3),
        "pilot_gap_effective_points": round(pilot_gap_effective, 3),
        "pilot_gap_uncapped_points": round(pilot_gap_uncapped, 3),
        "pilot_checks_total": int(pilot_checks_total),
        "pilot_checks_passed": int(pilot_checks_passed),
        "pilot_checks_open": int(pilot_checks_open),
        "pilot_minimum_flips_needed": int(pilot_minimum_flips_needed),
        "pilot_top_open_reason": pilot_top_open_reason or None,
        "deployment_confidence_cap_reason": deployment_confidence_cap_reason or None,
        "actions_escalated": int(tracking_escalated),
        "top_action_key": top_action_checklist.get("key") or None,
        "top_action_escalated": bool(top_action_escalated),
    },
    "trial_balance_checkin": {
        "mode": trial_balance_mode,
        "cache_write_ok": bool(trial_balance_cache_write_ok),
        "cache_write_error": trial_balance_cache_write_error or None,
        "starting_balance_dollars": trial_start,
        "current_balance_dollars": trial_current,
        "growth_dollars": trial_growth,
        "growth_percent": trial_growth_pct,
        "depleted": trial_balance_depleted,
        "win_rate_since_reset": trial_win_rate,
        "planned_rows_since_reset": int(trial_planned_rows_since_reset),
        "unique_shadow_orders_since_reset": int(trial_unique_shadow_orders_since_reset),
        "duplicate_rows_since_reset": int(trial_duplicate_count_since_reset),
        "duplicate_unique_ids_since_reset": int(trial_duplicate_unique_ids_since_reset)
        if isinstance(trial_duplicate_unique_ids_since_reset, int)
        else None,
        "duplicate_rows_ratio_since_reset": round(trial_duplicate_rows_ratio_since_reset, 6),
        "windows": {
            "1d": trial_window_snapshots.get("1d"),
            "7d": trial_window_snapshots.get("7d"),
            "14d": trial_window_snapshots.get("14d"),
            "21d": trial_window_snapshots.get("21d"),
            "28d": trial_window_snapshots.get("28d"),
            "30d": trial_window_snapshots.get("30d"),
            "3mo": trial_window_snapshots.get("3mo"),
            "6mo": trial_window_snapshots.get("6mo"),
            "1yr": trial_window_snapshots.get("1yr"),
        },
        "unconstrained_reference": {
            "current_balance_dollars": trial_legacy_current,
            "growth_dollars": trial_legacy_growth,
            "growth_percent": trial_legacy_growth_pct,
            "win_rate_since_reset": trial_legacy_win_rate,
            "resolved_counterfactual_trades_since_reset": int(trial_legacy_resolved_since_reset),
            "wins_since_reset": int(trial_legacy_wins_since_reset),
            "losses_since_reset": int(trial_legacy_losses_since_reset),
            "cumulative_counterfactual_pnl_dollars": trial_legacy_cumulative_counterfactual_pnl,
        },
        "cash_constrained": (
            {
                "current_balance_dollars": trial_cash_constrained_current,
                "growth_dollars": trial_cash_constrained_growth,
                "growth_percent": trial_cash_constrained_growth_pct,
                "win_rate_since_reset": trial_cash_constrained_win_rate,
                "resolved_counterfactual_trades_since_reset": (
                    int(trial_cash_constrained_resolved)
                    if isinstance(trial_cash_constrained_resolved, int)
                    else None
                ),
                "wins_since_reset": (
                    int(trial_cash_constrained_wins)
                    if isinstance(trial_cash_constrained_wins, int)
                    else None
                ),
                "losses_since_reset": (
                    int(trial_cash_constrained_losses)
                    if isinstance(trial_cash_constrained_losses, int)
                    else None
                ),
                "pushes_since_reset": (
                    int(trial_cash_constrained_pushes)
                    if isinstance(trial_cash_constrained_pushes, int)
                    else None
                ),
                "cumulative_counterfactual_pnl_dollars": trial_cash_constrained_pnl,
                "skipped_for_insufficient_cash_count": (
                    int(trial_cash_constrained_skipped_for_cash)
                    if isinstance(trial_cash_constrained_skipped_for_cash, int)
                    else None
                ),
                "execution_rate_vs_unconstrained": trial_cash_constrained_execution_rate,
            }
            if trial_balance_mode == "cash_constrained"
            else None
        ),
    },
    "settled_selection_detail": {
        "last_resolved_unique_market_side": last_resolved_unique_market_side or None,
        "last_resolved_unique_shadow_order": last_resolved_unique_shadow_order or None,
        "resolved_predictions": int(resolved_predictions),
        "wins": int(settled_wins),
        "losses": int(settled_losses),
        "pushes": int(settled_pushes),
        "win_rate": (
            round(float(prediction_win_rate), 6)
            if isinstance(prediction_win_rate, float)
            else None
        ),
    },
    "settled_economics": {
        "basis": "unique_market_side",
        "resolved_predictions": int(resolved_predictions),
        "wins": int(settled_wins),
        "losses": int(settled_losses),
        "pushes": int(settled_pushes),
        "win_rate": (
            round(float(prediction_win_rate), 6)
            if isinstance(prediction_win_rate, float)
            else None
        ),
        "expectancy_per_trade_dollars": (
            round(float(settled_expectancy_per_trade), 6)
            if isinstance(settled_expectancy_per_trade, float)
            else None
        ),
        "avg_win_dollars": (
            round(float(settled_avg_win), 6)
            if isinstance(settled_avg_win, float)
            else None
        ),
        "avg_loss_dollars": (
            round(float(settled_avg_loss), 6)
            if isinstance(settled_avg_loss, float)
            else None
        ),
        "payoff_ratio": (
            round(float(settled_payoff_ratio), 6)
            if isinstance(settled_payoff_ratio, float)
            else None
        ),
        "profit_factor": (
            round(float(settled_profit_factor), 6)
            if isinstance(settled_profit_factor, float)
            else None
        ),
        "counterfactual_pnl_dollars_if_live": (
            round(float(settled_counterfactual_pnl), 6)
            if isinstance(settled_counterfactual_pnl, float)
            else None
        ),
    },
    "top_blockers": [{"reason": key, "count": count} for key, count in display_top_blockers[:5]],
    "top_blockers_raw": [{"reason": key, "count": count} for key, count in top_blockers[:5]],
    "approval_quality_risk": {
        "active": bool(quality_risk_alert_active),
        "level": quality_risk_alert_level,
        "streak": int(quality_risk_streak),
        "red_streak": int(quality_risk_red_streak),
        "streak_red_required": int(quality_risk_streak_red_required),
        "state_file": str(quality_risk_state_path),
        "state_write_ok": bool(quality_risk_state_write_ok),
        "reasons": list(quality_risk_alert_reasons),
        "line": quality_risk_alert_line or None,
        "edge_gate_blocked_count": int(edge_gate_blocked_count),
        "edge_gate_blocked_share_of_blocked": round(float(edge_gate_blocked_share_of_blocked), 6),
        "edge_gate_blocked_dominant": bool(edge_gate_blocked_dominant),
        "guardrail_above_band": bool(approval_above_guardrail),
        "gate_coverage_alert_active": bool(gate_coverage_alert_active),
        "approval_parameter_mismatch_active": bool(approval_audit_mismatch_active),
        "approval_revalidation_conflict_active": bool(approval_revalidation_conflict_active),
    },
    "settlement_backlog_now": {
        "current_settlement_blocked_underlyings": int(current_settlement_blocked_underlyings),
        "current_settlement_pending_final_report": int(current_settlement_pending_final_report),
        "current_settlement_unresolved": int(current_settlement_unresolved),
        "settlement_backlog_clear": bool(settlement_backlog_clear),
        "settlement_pressure_mode": settlement_pressure_mode,
        "settlement_pressure_reason": settlement_pressure_reason or None,
        "settlement_pressure_active": settlement_pressure_active,
    },
    "weekly_blocker_audit_headline": {
        "reason_human": weekly_reason_human,
        "count": weekly_reason_count,
        "recommended_action": weekly_action,
    },
    "live_readiness_horizons": {
        label: readiness_by_horizon.get(label, {})
        for label in horizon_order
    },
    "live_readiness_overall": overall_live_readiness,
    "deployment_confidence": {
        "score": round(deployment_confidence_score, 3),
        "score_uncapped": round(deployment_confidence_score_uncapped, 3),
        "band": deployment_confidence_band,
        "guidance": deployment_confidence_guidance,
        "horizon_coverage_ratio": round(horizon_coverage_ratio, 6),
        "weighted_gate_score": round(normalized_weighted_score, 6),
        "cap_applied": bool(deployment_confidence_cap_applied),
        "cap_reason": deployment_confidence_cap_reason or None,
        "cap_value": (
            round(float(deployment_confidence_cap_value), 3)
            if isinstance(deployment_confidence_cap_value, float)
            else None
        ),
        "cap_detail": deployment_confidence_cap_detail or None,
        "pilot_target_score": round(pilot_target_score, 3),
        "pilot_gap_effective_points": round(pilot_gap_effective, 3),
        "pilot_gap_uncapped_points": round(pilot_gap_uncapped, 3),
        "pilot_gap_summary_line": pilot_gap_line,
        "pilot_horizon_drivers_line": pilot_horizon_delta_line,
        "pilot_horizon_drivers": horizon_delta_top,
        "pilot_gate_checklist": {
            "required_horizons": list(pilot_required_horizons),
            "checks_total": int(pilot_checks_total),
            "checks_passed": int(pilot_checks_passed),
            "checks_open": int(pilot_checks_open),
            "minimum_flips_needed": int(pilot_minimum_flips_needed),
            "top_open_reason": pilot_top_open_reason or None,
            "open_reason_counts": [
                {"reason": reason, "count": int(count)}
                for reason, count in pilot_open_reasons_sorted
            ],
            "rows": pilot_gate_rows,
            "summary_line": pilot_checklist_line,
            "top_open_line": pilot_open_reasons_line,
        },
        "weights": horizon_weights,
        "thresholds": {
            "scale_candidate_min": 80.0,
            "pilot_candidate_min": 65.0,
            "shadow_plus_min": 45.0,
        },
    },
    "selection_confidence": {
        "score": round(selection_confidence_score, 3),
        "band": selection_confidence_band,
        "guidance": selection_confidence_guidance,
        "component_summary": selection_confidence_component_summary,
        "gate_coverage_ratio": round(float(selection_gate_coverage_ratio), 6),
        "outcome_multiplier": round(float(selection_outcome_multiplier), 6),
        "settled_win_rate_lb95": (
            round(float(selection_settled_win_rate_lb95), 6)
            if isinstance(selection_settled_win_rate_lb95, float)
            else None
        ),
        "settled_confidence_strength": round(
            float(selection_settled_confidence_strength),
            6,
        ),
        "gate_components": {
            key: round(float(value), 6)
            for key, value in selection_gate_components.items()
        },
        "sample_scale": round(float(selection_sample_scale), 6),
        "guardrail_multiplier": round(float(selection_guardrail_multiplier), 6),
        "penalties": {
            "mismatch": round(float(selection_mismatch_penalty), 6),
            "revalidation_conflict": round(float(selection_revalidation_penalty), 6),
            "no_evaluable": round(float(selection_no_evaluable_penalty), 6),
            "quality_risk": round(float(selection_risk_penalty), 6),
            "economics_shape": round(float(selection_economics_penalty), 6),
            "total": round(float(selection_confidence_penalties_total), 6),
        },
        "raw_score": round(float(selection_confidence_score_raw), 6),
        "raw_score_before_penalties": round(float(selection_confidence_score_raw), 6),
        "top_drivers": [
            {"driver": label, "points": round(float(points), 3)}
            for label, points in selection_confidence_top_drivers
        ],
        "driver_line": selection_confidence_driver_line or None,
    },
    "approval_guardrail": {
        "evaluated": bool(approval_guardrail_evaluated),
        "status": approval_guardrail_status,
        "reason": approval_guardrail_reason,
        "alert_level": approval_guardrail_alert_level,
        "basis": guardrail_basis,
        "basis_reason": guardrail_basis_reason,
        "basis_min_ratio_to_window": round(float(guardrail_basis_min_ratio_to_window), 6),
        "basis_min_abs_intents": int(guardrail_basis_min_abs_intents),
        "intents_total": int(guardrail_intents_total),
        "approval_rate": round(float(guardrail_approval_rate), 6),
        "window_intents_total": int(window_intents_total),
        "window_approval_rate": round(float(window_approval_rate), 6),
        "latest_intents_total": int(latest_guardrail_intents_total),
        "latest_ratio_to_window": (
            round(float(latest_guardrail_ratio_to_window), 6)
            if isinstance(latest_guardrail_ratio_to_window, float)
            else None
        ),
        "latest_sample_eligible": bool(latest_guardrail_sample_eligible),
        "latest_approval_rate": (
            round(float(latest_guardrail_approval_rate), 6)
            if isinstance(latest_guardrail_approval_rate, float)
            else None
        ),
        "loop_sample_source": quality_gate_escalation_sample_source,
        "loop_sample_captured_at": quality_gate_escalation_sample_captured_at or None,
        "loop_sample_age_seconds": (
            round(float(quality_gate_escalation_sample_age_seconds), 1)
            if isinstance(quality_gate_escalation_sample_age_seconds, float)
            else None
        ),
        "loop_sample_intents_total": (
            int(quality_gate_escalation_sample_intents_total)
            if isinstance(quality_gate_escalation_sample_intents_total, int)
            else None
        ),
        "loop_sample_approval_rate": (
            round(float(quality_gate_escalation_sample_approval_rate), 6)
            if isinstance(quality_gate_escalation_sample_approval_rate, float)
            else None
        ),
        "min_intents": int(approval_rate_guardrail_min_intents),
        "min_required_intents": int(approval_guardrail_min_required_intents),
        "target_min_rate": round(approval_rate_guardrail_min, 6),
        "target_max_rate": round(approval_rate_guardrail_max, 6),
        "critical_high_rate": round(approval_rate_guardrail_critical_high, 6),
        "status_line": guardrail_status_line,
        "intents_quality_row_source": intent_quality_source,
        "intents_quality_rows": int(len(intent_quality_rows)),
        "intents_quality_summary_files_count": int(intent_quality_source_summary_files_count),
        "intents_quality_csv_files_count": int(intent_quality_source_csv_files_count),
        "intents_quality_summary_files_preview": intent_quality_source_summary_files_preview,
        "intents_quality_csv_files_preview": intent_quality_source_csv_files_preview,
        "recommendation_status": (
            approval_guardrail_recommendation.get("status")
            if isinstance(approval_guardrail_recommendation, dict)
            else None
        ),
        "recommendation": approval_guardrail_recommendation,
    },
    "approval_parameter_audit": approval_parameter_audit,
    "approval_parameter_gate_coverage": {
        "line": approval_gate_coverage_line,
        "activity_share_line": approval_gate_activity_line,
        "metar_age": _gate_coverage("metar_age"),
        "metar_age_all_ratio": (
            round(float(approval_gate_coverage_metar_ratio), 6)
            if isinstance(approval_gate_coverage_metar_ratio, float)
            else None
        ),
        "metar_age_active_ratio": (
            round(float(approval_gate_coverage_metar_active_ratio), 6)
            if isinstance(approval_gate_coverage_metar_active_ratio, float)
            else None
        ),
        "expected_edge": _gate_coverage("expected_edge"),
        "expected_edge_all_ratio": (
            round(float(approval_gate_coverage_expected_edge_ratio), 6)
            if isinstance(approval_gate_coverage_expected_edge_ratio, float)
            else None
        ),
        "expected_edge_active_ratio": (
            round(float(approval_gate_coverage_expected_edge_active_ratio), 6)
            if isinstance(approval_gate_coverage_expected_edge_active_ratio, float)
            else None
        ),
        "probability_confidence": _gate_coverage("probability_confidence"),
        "probability_confidence_all_ratio": (
            round(float(approval_gate_coverage_probability_ratio), 6)
            if isinstance(approval_gate_coverage_probability_ratio, float)
            else None
        ),
        "probability_confidence_active_ratio": (
            round(float(approval_gate_coverage_probability_active_ratio), 6)
            if isinstance(approval_gate_coverage_probability_active_ratio, float)
            else None
        ),
        "alpha_strength": _gate_coverage("alpha_strength"),
        "alpha_strength_all_ratio": (
            round(float(approval_gate_coverage_alpha_ratio), 6)
            if isinstance(approval_gate_coverage_alpha_ratio, float)
            else None
        ),
        "alpha_strength_active_ratio": (
            round(float(approval_gate_coverage_alpha_active_ratio), 6)
            if isinstance(approval_gate_coverage_alpha_active_ratio, float)
            else None
        ),
    },
    "approval_parameter_mismatch_top_gates": [
        {"gate": key, "count": int(count)}
        for key, count in approval_mismatch_top_gates
    ],
    "approval_auto_apply": {
        "enabled": bool(auto_apply_enabled),
        "candidate_ready": bool(auto_apply_candidate_ready),
        "recommendation_fit": bool(auto_apply_recommendation_fit),
        "breach_active": bool(approval_guardrail_breach_active),
        "breach_streak": int(auto_apply_breach_streak),
        "trigger": auto_apply_trigger,
        "trigger_streak_current": int(auto_apply_trigger_streak_current),
        "trigger_streak_required": int(auto_apply_trigger_streak_required),
        "rows_available": int(auto_apply_rows_available),
        "required_rows_for_trigger": int(auto_apply_required_rows_for_trigger),
        "approval_trigger_rows_satisfied": bool(approval_trigger_rows_satisfied),
        "gate_trigger_rows_satisfied": bool(gate_trigger_rows_satisfied),
        "recommendation_ready": bool(auto_apply_recommendation_ready),
        "gate_coverage_auto_apply_enabled": bool(gate_coverage_auto_apply_enabled),
        "gate_coverage_breach_active": bool(gate_coverage_auto_apply_breach_active),
        "gate_coverage_breach_streak": int(auto_apply_gate_breach_streak),
        "quality_risk_signal_active": bool(quality_risk_signal_active_for_auto_apply),
        "quality_risk_streak": int(auto_apply_quality_risk_streak),
        "quality_risk_auto_apply_streak_required": int(quality_risk_auto_apply_streak_required),
        "quality_risk_projected_streak_for_auto": int(quality_risk_projected_streak_for_auto),
        "quality_risk_persistent_for_auto": bool(quality_risk_persistent_for_auto),
        "stability_enabled": bool(auto_apply_stability_enabled),
        "stability_windows_required": int(auto_apply_stability_windows_required),
        "stability_streak": int(auto_apply_stability_streak),
        "stability_ready": bool(auto_apply_stability_ready),
        "stability_window_comparable": bool(auto_apply_stability_window_comparable),
        "stability_window_stable": bool(auto_apply_stability_window_stable),
        "stability_reason": auto_apply_stability_reason,
        "stability_max_approval_delta_pp": round(float(auto_apply_stability_max_approval_delta_pp), 6),
        "stability_max_stale_delta_pp": round(float(auto_apply_stability_max_stale_delta_pp), 6),
        "stability_delta_approval_pp": (
            round(float(window_delta_approval_pp), 6)
            if isinstance(window_delta_approval_pp, float)
            else None
        ),
        "stability_delta_stale_pp": (
            round(float(window_delta_stale_pp), 6)
            if isinstance(window_delta_stale_pp, float)
            else None
        ),
        "recommendation_mode": auto_apply_recommendation_mode_effective,
        "recommendation_mode_preferred": auto_apply_recommendation_mode,
        "strict_guardrail_max": round(float(strict_guardrail_max), 6),
        "gate_coverage_auto_apply_min_rows": int(gate_coverage_auto_apply_min_rows),
        "gate_coverage_auto_apply_streak_required": int(gate_coverage_auto_apply_streak_required),
        "gate_coverage_auto_apply_min_level": gate_coverage_auto_apply_min_level,
        "clear_streak": int(auto_apply_clear_streak),
        "streak_required": int(auto_apply_streak_required),
        "min_rows": int(auto_apply_min_rows),
        "guardrail_basis_min_abs_intents": int(guardrail_basis_min_abs_intents),
        "guardrail_min_required_intents": int(approval_guardrail_min_required_intents),
        "min_rows_satisfied": bool(auto_apply_min_rows_satisfied),
        "release_enabled": bool(auto_apply_release_enabled),
        "release_streak_required": int(auto_apply_release_streak_required),
        "zero_approved_streak": int(auto_apply_zero_approved_streak),
        "zero_approved_streak_required": int(auto_apply_zero_approved_streak_required),
        "zero_approved_min_intents": int(auto_apply_zero_approved_min_intents),
        "zero_approved_candidate": bool(auto_apply_zero_approved_candidate),
        "latest_summary_sample_eligible": bool(latest_guardrail_sample_eligible),
        "latest_summary_intents_total": int(latest_guardrail_intents_total),
        "latest_summary_intents_approved": int(latest_approved_for_zero_guard),
        "should_apply": bool(auto_apply_should_apply),
        "applied_in_this_run": bool(auto_apply_applied),
        "released_in_this_run": bool(auto_apply_released),
        "apply_reason": auto_apply_apply_reason,
        "status_line": auto_apply_status_line,
        "state_path": str(auto_apply_state_path),
        "profile_path": str(auto_apply_profile_path),
        "profile_write_attempted": bool(auto_apply_profile_write_attempted),
        "profile_write_ok": bool(auto_apply_profile_write_ok),
        "state_write_ok": bool(auto_apply_state_write_ok),
        "applied_profile": auto_applied_profile,
    },
    "quality_drift_alert": {
        "active": bool(quality_drift_alert_active),
        "level": quality_drift_alert_level,
        "reason": quality_drift_alert_reason or None,
        "summary": quality_drift_alert_summary or None,
        "direction": quality_drift_direction,
        "window_eligible": bool(quality_drift_window_eligible),
        "delta_approval_percentage_points": (
            round(float(window_delta_approval_pp), 6)
            if isinstance(window_delta_approval_pp, float)
            else None
        ),
        "delta_approval_abs_percentage_points": (
            round(float(quality_drift_approval_delta_abs_pp), 6)
            if isinstance(quality_drift_approval_delta_abs_pp, float)
            else None
        ),
        "delta_threshold_percentage_points": round(float(quality_drift_delta_pp_min), 6),
        "resolved_sides_delta": (
            int(window_delta_resolved_market_sides)
            if isinstance(window_delta_resolved_market_sides, int)
            else None
        ),
        "resolved_sides_delta_max_for_alert": int(quality_drift_max_resolved_sides_delta),
        "min_intents_per_window": int(quality_drift_min_intents_per_window),
        "current_intents": int(intents_total),
        "previous_intents": (
            int(window_previous_intents)
            if isinstance(window_previous_intents, int)
            else None
        ),
        "current_approval_rate": round(float(approval_rate), 6),
        "previous_approval_rate": (
            round(float(window_previous_approval_rate), 6)
            if isinstance(window_previous_approval_rate, float)
            else None
        ),
    },
    "approval_gate_coverage_alert": {
        "active": bool(gate_coverage_alert_active),
        "level": gate_coverage_alert_level,
        "reason": gate_coverage_alert_reason or None,
        "summary": gate_coverage_alert_summary or None,
        "basis": "approved_active_threshold",
        "rows_eligible": bool(gate_coverage_alert_rows_eligible),
        "min_approved_rows": int(gate_coverage_alert_min_approved_rows),
        "approved_rows": int(approval_audit_approved_rows),
        "expected_edge_ratio": (
            round(float(gate_coverage_values.get("expected_edge")), 6)
            if isinstance(gate_coverage_values.get("expected_edge"), float)
            else None
        ),
        "expected_edge_all_ratio": (
            round(float(approval_gate_coverage_expected_edge_ratio), 6)
            if isinstance(approval_gate_coverage_expected_edge_ratio, float)
            else None
        ),
        "expected_edge_active_ratio": (
            round(float(approval_gate_coverage_expected_edge_active_ratio), 6)
            if isinstance(approval_gate_coverage_expected_edge_active_ratio, float)
            else None
        ),
        "probability_ratio": (
            round(float(gate_coverage_values.get("probability_confidence")), 6)
            if isinstance(gate_coverage_values.get("probability_confidence"), float)
            else None
        ),
        "probability_all_ratio": (
            round(float(approval_gate_coverage_probability_ratio), 6)
            if isinstance(approval_gate_coverage_probability_ratio, float)
            else None
        ),
        "probability_active_ratio": (
            round(float(approval_gate_coverage_probability_active_ratio), 6)
            if isinstance(approval_gate_coverage_probability_active_ratio, float)
            else None
        ),
        "alpha_ratio": (
            round(float(gate_coverage_values.get("alpha_strength")), 6)
            if isinstance(gate_coverage_values.get("alpha_strength"), float)
            else None
        ),
        "alpha_all_ratio": (
            round(float(approval_gate_coverage_alpha_ratio), 6)
            if isinstance(approval_gate_coverage_alpha_ratio, float)
            else None
        ),
        "alpha_active_ratio": (
            round(float(approval_gate_coverage_alpha_active_ratio), 6)
            if isinstance(approval_gate_coverage_alpha_active_ratio, float)
            else None
        ),
        "expected_edge_min": round(float(gate_coverage_expected_edge_min), 6),
        "probability_min": round(float(gate_coverage_probability_min), 6),
        "alpha_min": round(float(gate_coverage_alpha_min), 6),
        "gap_count": int(len(gate_coverage_alert_gaps)),
        "gaps": [
            {
                "gate": _normalize(item.get("gate")),
                "label": _normalize(item.get("label")),
                "ratio": _to_json_number(item.get("ratio")),
                "threshold": _to_json_number(item.get("threshold")),
                "shortfall": _to_json_number(item.get("shortfall")),
                "shortfall_ratio": _to_json_number(item.get("shortfall_ratio")),
            }
            for item in gate_coverage_alert_gaps
        ],
        "worst_shortfall_ratio": (
            round(float(gate_coverage_alert_worst_shortfall_ratio), 6)
            if isinstance(gate_coverage_alert_worst_shortfall_ratio, float)
            else None
        ),
    },
    "suggestions_priority_model": {
        "name": "impact_over_eta",
        "formula": (
            "priority_score_effective = priority_score + measured_delta_priority_points/24 "
            "where priority_score = (impact_points + priority_boost_points) / eta_hours"
        ),
        "notes": (
            "Higher score means higher estimated near-term alpha impact per hour to close; "
            "priority_boost_points is used for guardrail-severity escalation, and "
            "measured_delta_priority_points raises urgency when 12h gap-to-target is regressing."
        ),
    },
    "suggestions_impact_model": {
        "settled_evidence_count": int(settled_evidence_count),
        "settled_evidence_full_at": int(settled_evidence_full_at),
        "settled_evidence_strength": round(float(settled_evidence_strength), 6),
        "expected_edge_proxy_discount_factor": round(
            float(impact_pool_proxy_discount_factor_effective), 6
        ),
        "expected_edge_proxy_discount_applied": bool(impact_pool_proxy_discount_applied),
        "pool_raw_dollars": (
            round(float(impact_pool_raw_dollars), 6)
            if isinstance(impact_pool_raw_dollars, float)
            else None
        ),
        "pool_dollars": (
            round(float(impact_pool_dollars), 6)
            if isinstance(impact_pool_dollars, float)
            else None
        ),
        "pool_basis": impact_pool_basis,
        "top3_action_impact_proxy_dollars": round(float(top3_action_impact_proxy), 6),
        "top3_action_impact_proxy_share_of_pool_pct": (
            round((float(top3_action_impact_proxy) / float(impact_pool_dollars)) * 100.0, 6)
            if isinstance(impact_pool_dollars, float) and impact_pool_dollars > 0 and top3_action_impact_proxy > 0
            else None
        ),
        "top3_action_impact_proxy_line": top3_action_impact_proxy_line or None,
        "top3_action_impact_estimate_dollars": round(float(top3_action_impact_proxy), 6),
        "top3_action_impact_estimate_share_of_pool_pct": (
            round((float(top3_action_impact_proxy) / float(impact_pool_dollars)) * 100.0, 6)
            if isinstance(impact_pool_dollars, float) and impact_pool_dollars > 0 and top3_action_impact_proxy > 0
            else None
        ),
        "top3_action_impact_estimate_line": top3_action_impact_proxy_line or None,
        "shared_rationale_code": shared_rationale_code or None,
        "shared_rationale_line": shared_suggestion_rationale_line or None,
        "allocation_rule": "row_impact_points / total_impact_points_ranked",
        "confidence_rule": (
            "confidence_score = base(impact_points)+metric_pair+escalation+settled_evidence_adjustment"
        ),
        "confidence_labels": {
            "HIGH": "score >= 0.72",
            "MEDIUM": "0.50 <= score < 0.72",
            "LOW": "score < 0.50",
        },
        "confidence_rationale_tokens": {
            "metric": "has metric current/target pair",
            "heuristic": "no metric pair (heuristic confidence)",
            "settled": "uses settled-evidence uplift",
            "no-settled": "no settled-evidence uplift yet",
            "proxy": "impact pool from planned-order estimate",
            "settled-pool": "impact pool from settled projection",
            "escalated": "regression/escalation flag active",
            "evidence-sparse": "very limited settled independent outcomes",
            "evidence-building": "settled evidence accumulating but not mature",
            "evidence-mature": "settled evidence at/above maturity target",
        },
        "confidence_rationale_display_codes": {
            "M": "metric",
            "H": "heuristic",
            "ST": "settled",
            "NS": "no-settled",
            "PX": "estimate",
            "SP": "settled-pool",
            "ESC": "escalated",
            "ES": "evidence-sparse",
            "EB": "evidence-building",
            "EM": "evidence-mature",
        },
    },
    "suggestion_tracking_summary": suggestion_tracking_summary,
    "action_checklist_top": action_checklist_top,
    "action_checklist_summary": action_checklist_summary,
    "suggestions": suggestions,
    "suggestions_structured": suggestion_rows,
    "suggestions_structured_ranked_all": suggestion_rows_ranked,
    "discord_mode": discord_mode,
    "discord_message": discord_message,
    "discord_summary": discord_message,
    "discord_summary_text": discord_message,
    "discord_message_concise": discord_message_concise,
    "discord_summary_concise": discord_message_concise,
    "discord_message_detailed": discord_message_detailed,
    "discord_summary_detailed": discord_message_detailed,
    "discord_summary_structured": {
        "mode": discord_mode,
        "selected": discord_message,
        "concise": discord_message_concise,
        "detailed": discord_message_detailed,
        "selected_line_count": int(len(discord_message.splitlines())),
        "shared_rationale_line": shared_suggestion_rationale_line or None,
        "top3_action_impact_estimate_line": top3_action_impact_proxy_line or None,
        "suggestion_heading": suggestion_heading,
        "suggestion_count": int(len(display_suggestions)),
    },
    "discord_messages": {
        "mode": discord_mode,
        "selected": discord_message,
        "concise": discord_message_concise,
        "detailed": discord_message_detailed,
    },
    "discord": {
        "mode": discord_mode,
        "message": discord_message,
        "message_concise": discord_message_concise,
        "message_detailed": discord_message_detailed,
        "selected": discord_message,
        "concise": discord_message_concise,
        "detailed": discord_message_detailed,
        "selected_line_count": int(len(discord_message.splitlines())),
    },
    "message_summary": {
        "mode": discord_mode,
        "mode_selected": discord_mode,
        "selected": discord_message,
        "concise": discord_message_concise,
        "detailed": discord_message_detailed,
        "selected_line_count": int(len(discord_message.splitlines())),
        "shared_rationale_line": shared_suggestion_rationale_line or None,
        "top3_action_impact_estimate_line": top3_action_impact_proxy_line or None,
        "suggestion_count": int(len(display_suggestions)),
        "msg_quality_pass": bool(message_quality_checks.get("overall_pass")),
    },
    "data_consistency_notes": data_consistency_notes,
    "message_quality_checks": message_quality_checks,
}

if isinstance(payload.get("headline_metrics"), dict):
    headline = payload["headline_metrics"]
    trial_checkin = (
        payload.get("trial_balance_checkin")
        if isinstance(payload.get("trial_balance_checkin"), dict)
        else {}
    )

    expected_edge_row = _parse_float(headline.get("expected_edge_total_row_based_dollars"))
    expected_edge_breadth = _parse_float(headline.get("expected_edge_total_breadth_normalized_dollars"))
    expected_edge_default = (
        expected_edge_breadth
        if isinstance(expected_edge_breadth, float)
        else expected_edge_row
    )
    settled_counterfactual = _parse_float(
        headline.get("settled_unique_market_side_counterfactual_pnl_dollars_if_live")
    )
    settled_resolved_predictions = _parse_int(
        headline.get("settled_unique_market_side_resolved_predictions")
    ) or 0
    expected_vs_shadow_delta = (
        (float(expected_edge_default) - float(settled_counterfactual))
        if isinstance(expected_edge_default, float) and isinstance(settled_counterfactual, float)
        else None
    )
    expected_vs_shadow_delta_per_trade = (
        (float(expected_vs_shadow_delta) / float(settled_resolved_predictions))
        if isinstance(expected_vs_shadow_delta, float) and settled_resolved_predictions > 0
        else None
    )
    expected_vs_shadow_calibration_ratio = (
        (float(settled_counterfactual) / float(expected_edge_default))
        if isinstance(settled_counterfactual, float)
        and isinstance(expected_edge_default, float)
        and float(expected_edge_default) > 0.0
        else None
    )

    trial_starting = _parse_float(trial_checkin.get("starting_balance_dollars"))
    trial_current_balance = _parse_float(trial_checkin.get("current_balance_dollars"))
    trial_growth_total = _parse_float(trial_checkin.get("growth_dollars"))
    trial_growth_percent = _parse_float(trial_checkin.get("growth_percent"))
    settled_win_rate = _parse_float(headline.get("settled_unique_market_side_prediction_win_rate"))

    alias_values = {
        "approval_guardrail_basis_approval_rate": _parse_float(headline.get("approval_guardrail_basis_rate")),
        "expected_shadow_edge_total": expected_edge_default,
        "shadow_settled_pnl_total": settled_counterfactual,
        "expected_vs_shadow_settled_delta_total": expected_vs_shadow_delta,
        "expected_vs_shadow_settled_delta_per_trade": expected_vs_shadow_delta_per_trade,
        "expected_vs_shadow_settled_calibration_ratio": expected_vs_shadow_calibration_ratio,
        "trial_balance_starting": trial_starting,
        "trial_balance_current": trial_current_balance,
        "trial_balance_delta_total": trial_growth_total,
        "trial_balance_delta_percent": trial_growth_percent,
        "trial_balance_starting_dollars": trial_starting,
        "trial_balance_current_dollars": trial_current_balance,
        "trial_balance_growth_dollars": trial_growth_total,
        "trial_balance_growth_rate": trial_growth_percent,
        "settled_unique_market_side_total": (
            int(settled_resolved_predictions)
            if isinstance(settled_resolved_predictions, int)
            else None
        ),
        "settled_unique_market_side_win_rate": settled_win_rate,
        "settled_win_rate": settled_win_rate,
    }
    for alias_key, alias_value in alias_values.items():
        if alias_key not in headline or headline.get(alias_key) is None:
            headline[alias_key] = alias_value

    # Root-level compatibility mirrors for simple jq pulls and older tooling.
    root_alias_values = {
        "quality_gate_source": _normalize(headline.get("quality_gate_source")),
        "quality_gate_auto_applied": headline.get("quality_gate_auto_applied"),
        "auto_apply_reason": _normalize(headline.get("approval_auto_apply_apply_reason")),
        "settled_evidence_count": _parse_int(headline.get("settled_evidence_count")),
        "settled_evidence_full_at": _parse_int(headline.get("settled_evidence_full_at")),
        "settled_evidence_strength": _parse_float(headline.get("settled_evidence_strength")),
        "expected_edge_proxy_discount_factor": _parse_float(
            headline.get("expected_edge_proxy_discount_factor")
        ),
        "expected_edge_proxy_discount_applied": headline.get(
            "expected_edge_proxy_discount_applied"
        ),
        "suggestion_impact_pool_basis": _normalize(headline.get("suggestion_impact_pool_basis")),
        "suggestion_impact_pool_raw_dollars": _parse_float(
            headline.get("suggestion_impact_pool_raw_dollars")
        ),
        "suggestion_impact_pool_discounted_dollars": _parse_float(
            headline.get("suggestion_impact_pool_discounted_dollars")
        ),
        "approval_guardrail_basis_approval_rate": _parse_float(
            headline.get("approval_guardrail_basis_approval_rate")
        ),
        "expected_shadow_edge_total": _parse_float(headline.get("expected_shadow_edge_total")),
        "shadow_settled_pnl_total": _parse_float(headline.get("shadow_settled_pnl_total")),
        "expected_vs_shadow_settled_delta_total": _parse_float(
            headline.get("expected_vs_shadow_settled_delta_total")
        ),
        "expected_vs_shadow_settled_delta_per_trade": _parse_float(
            headline.get("expected_vs_shadow_settled_delta_per_trade")
        ),
        "expected_vs_shadow_settled_calibration_ratio": _parse_float(
            headline.get("expected_vs_shadow_settled_calibration_ratio")
        ),
        "trial_balance_starting": _parse_float(headline.get("trial_balance_starting")),
        "trial_balance_current": _parse_float(headline.get("trial_balance_current")),
        "trial_balance_delta_total": _parse_float(headline.get("trial_balance_delta_total")),
        "trial_balance_delta_percent": _parse_float(headline.get("trial_balance_delta_percent")),
        "trial_balance_starting_dollars": _parse_float(
            headline.get("trial_balance_starting_dollars")
        ),
        "trial_balance_current_dollars": _parse_float(
            headline.get("trial_balance_current_dollars")
        ),
        "trial_balance_growth_dollars": _parse_float(
            headline.get("trial_balance_growth_dollars")
        ),
        "trial_balance_growth_rate": _parse_float(
            headline.get("trial_balance_growth_rate")
        ),
        "settled_unique_market_side_total": _parse_int(
            headline.get("settled_unique_market_side_total")
        ),
        "settled_win_rate": _parse_float(headline.get("settled_win_rate")),
        "historical_profitability_guardrail_applied_rate": _parse_float(
            headline.get("historical_profitability_guardrail_applied_rate")
        ),
        "historical_profitability_guardrail_penalty_ratio_avg": _parse_float(
            headline.get("historical_profitability_guardrail_penalty_ratio_avg")
        ),
        "historical_profitability_guardrail_blocked_expected_edge_below_min_count": _parse_int(
            headline.get("historical_profitability_guardrail_blocked_expected_edge_below_min_count")
        ),
        "historical_profitability_bucket_guardrail_applied_rate": _parse_float(
            headline.get("historical_profitability_bucket_guardrail_applied_rate")
        ),
        "historical_profitability_bucket_guardrail_penalty_ratio_avg": _parse_float(
            headline.get("historical_profitability_bucket_guardrail_penalty_ratio_avg")
        ),
        "historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count": _parse_int(
            headline.get("historical_profitability_bucket_guardrail_blocked_expected_edge_below_min_count")
        ),
        "historical_profitability_rank_penalty_global_avg": _parse_float(
            headline.get("historical_profitability_rank_penalty_global_avg")
        ),
        "historical_profitability_rank_penalty_bucket_avg": _parse_float(
            headline.get("historical_profitability_rank_penalty_bucket_avg")
        ),
        "historical_profitability_rank_scored_candidate_count": _parse_int(
            headline.get("historical_profitability_rank_scored_candidate_count")
        ),
    }
    for alias_key, alias_value in root_alias_values.items():
        if alias_key not in payload or payload.get(alias_key) is None:
            payload[alias_key] = alias_value

message_quality_failed_checks = sorted(
    key
    for key, value in message_quality_checks.items()
    if key not in {"overall_pass", "selected_mode"} and not bool(value)
)
payload["message_quality_summary"] = {
    "overall_pass": bool(message_quality_checks.get("overall_pass")),
    "failed_check_count": int(len(message_quality_failed_checks)),
    "failed_checks": message_quality_failed_checks,
}
if isinstance(payload.get("message_summary"), dict):
    payload["message_summary"]["msg_quality_pass"] = bool(
        message_quality_checks.get("overall_pass")
    )
    payload["message_summary"]["msg_quality_fail_count"] = int(
        len(message_quality_failed_checks)
    )
    payload["message_summary"]["msg_quality_failed_checks"] = message_quality_failed_checks
if isinstance(payload.get("headline_metrics"), dict):
    payload["headline_metrics"]["message_quality_overall_pass"] = bool(
        message_quality_checks.get("overall_pass")
    )
    payload["headline_metrics"]["message_quality_failed_check_count"] = int(
        len(message_quality_failed_checks)
    )
    payload["headline_metrics"]["message_quality_failed_checks"] = message_quality_failed_checks

if not bool(message_quality_checks.get("overall_pass")):
    def _humanize_message_quality_check(key: str) -> str:
        normalized = _normalize(key).replace("_", " ").strip()
        replacements = {
            "selected message length ok": "selected message length invalid",
            "concise message length ok": "concise message length invalid",
            "detailed message length ok": "detailed message length invalid",
            "contains min suggestions": "missing required suggestions",
            "contains human readable why hints": "non-human rationale tokens",
            "contains counterfactual qualifier": "missing counterfactual qualifier",
        }
        return replacements.get(normalized, normalized or key)

    failed_human = ", ".join(
        _humanize_message_quality_check(item) for item in message_quality_failed_checks[:6]
    ) or "unknown checks failed"
    note = (
        "discord summary quality checks failed: "
        + failed_human
        + "; verify summary readability/accuracy before acting on Discord output."
    )
    existing_notes = payload.get("data_consistency_notes")
    if isinstance(existing_notes, list):
        existing_notes.append(note)
    else:
        payload["data_consistency_notes"] = [note]

    critical_quality_checks = {
        "selected_message_length_ok",
        "not_hard_truncated",
        "contains_performance_basis_line",
        "contains_impact_basis_line",
        "contains_counterfactual_qualifier",
    }
    quality_failure_critical = any(
        key in critical_quality_checks for key in message_quality_failed_checks
    )

    if isinstance(payload.get("health"), dict):
        health_obj = payload["health"]
        issues = health_obj.get("issues")
        if isinstance(issues, list):
            if "discord_message_quality_failed" not in issues:
                issues.append("discord_message_quality_failed")
        else:
            issues = ["discord_message_quality_failed"]
            health_obj["issues"] = issues

        current_health = _normalize(health_obj.get("status")).upper() or "GREEN"
        if quality_failure_critical:
            health_obj["status"] = "RED"
        elif current_health == "GREEN":
            health_obj["status"] = "YELLOW"

        health_obj["issue_count"] = int(len(issues))
        reason_text = _normalize(health_obj.get("reason_text"))
        if reason_text and reason_text.lower() not in {"none", "n/a", "unknown"}:
            if "discord message quality failed" not in reason_text.lower():
                health_obj["reason_text"] = reason_text + ", discord message quality failed"
        else:
            health_obj["reason_text"] = "discord message quality failed"

    if isinstance(payload.get("headline_metrics"), dict):
        payload["headline_metrics"]["health_status"] = (
            payload.get("health", {}).get("status")
            if isinstance(payload.get("health"), dict)
            else payload["headline_metrics"].get("health_status")
        )

    # Keep concise/detailed Discord status line aligned with post-quality
    # health severity upgrades. Message bodies are assembled earlier, while
    # quality checks can escalate health from GREEN->YELLOW/RED.
    final_alpha_health_status = _normalize(
        ((payload.get("headline_metrics") or {}) if isinstance(payload.get("headline_metrics"), dict) else {}).get(
            "health_status"
        )
    ).upper()
    if final_alpha_health_status in {"GREEN", "YELLOW", "RED"}:
        def _sync_status_line_alpha(message_text: Any, alpha_status: str) -> str:
            raw = _normalize(message_text)
            if not raw:
                return ""
            lines = raw.splitlines()
            if len(lines) < 2:
                return raw
            status_line = lines[1]
            if not status_line.startswith("Status now:"):
                return raw
            status_line = re.sub(
                r"(\|\s*alpha\s+)[^|]+(\s*\|\s*)",
                rf"\1{alpha_status}\2",
                status_line,
                count=1,
            )
            status_line = re.sub(r"\b(GREEN|YELLOW|RED)\|", r"\1 |", status_line)
            lines[1] = status_line
            return "\n".join(lines)

        synced_concise = _sync_status_line_alpha(payload.get("discord_message_concise"), final_alpha_health_status)
        synced_detailed = _sync_status_line_alpha(payload.get("discord_message_detailed"), final_alpha_health_status)
        if synced_concise:
            payload["discord_message_concise"] = synced_concise
            payload["discord_summary_concise"] = synced_concise
            if isinstance(payload.get("message_summary"), dict):
                payload["message_summary"]["concise"] = synced_concise
            if isinstance(payload.get("discord"), dict):
                payload["discord"]["message_concise"] = synced_concise
                payload["discord"]["concise"] = synced_concise
            if isinstance(payload.get("discord_messages"), dict):
                payload["discord_messages"]["concise"] = synced_concise
        if synced_detailed:
            payload["discord_message_detailed"] = synced_detailed
            payload["discord_summary_detailed"] = synced_detailed
            if isinstance(payload.get("message_summary"), dict):
                payload["message_summary"]["detailed"] = synced_detailed
            if isinstance(payload.get("discord"), dict):
                payload["discord"]["message_detailed"] = synced_detailed
                payload["discord"]["detailed"] = synced_detailed
            if isinstance(payload.get("discord_messages"), dict):
                payload["discord_messages"]["detailed"] = synced_detailed

        if discord_mode == "concise" and synced_concise:
            payload["discord_message"] = synced_concise
            payload["discord_summary"] = synced_concise
            payload["discord_summary_text"] = synced_concise
            if isinstance(payload.get("message_summary"), dict):
                payload["message_summary"]["selected"] = synced_concise
                payload["message_summary"]["selected_line_count"] = int(len(synced_concise.splitlines()))
            if isinstance(payload.get("discord"), dict):
                payload["discord"]["message"] = synced_concise
                payload["discord"]["selected"] = synced_concise
                payload["discord"]["selected_line_count"] = int(len(synced_concise.splitlines()))
            if isinstance(payload.get("discord_messages"), dict):
                payload["discord_messages"]["selected"] = synced_concise
            if isinstance(payload.get("discord_summary_structured"), dict):
                payload["discord_summary_structured"]["selected"] = synced_concise
                payload["discord_summary_structured"]["selected_line_count"] = int(
                    len(synced_concise.splitlines())
                )
        elif discord_mode == "detailed" and synced_detailed:
            payload["discord_message"] = synced_detailed
            payload["discord_summary"] = synced_detailed
            payload["discord_summary_text"] = synced_detailed
            if isinstance(payload.get("message_summary"), dict):
                payload["message_summary"]["selected"] = synced_detailed
                payload["message_summary"]["selected_line_count"] = int(len(synced_detailed.splitlines()))
            if isinstance(payload.get("discord"), dict):
                payload["discord"]["message"] = synced_detailed
                payload["discord"]["selected"] = synced_detailed
                payload["discord"]["selected_line_count"] = int(len(synced_detailed.splitlines()))
            if isinstance(payload.get("discord_messages"), dict):
                payload["discord_messages"]["selected"] = synced_detailed
            if isinstance(payload.get("discord_summary_structured"), dict):
                payload["discord_summary_structured"]["selected"] = synced_detailed
                payload["discord_summary_structured"]["selected_line_count"] = int(
                    len(synced_detailed.splitlines())
                )

approval_auto_apply_payload_checks = {
    "ready": False,
    "overall_pass": False,
    "expected_mode_effective": auto_apply_recommendation_mode_effective,
    "expected_mode_preferred": auto_apply_recommendation_mode,
    "checks": {},
    "details": {},
}
approval_auto_apply_payload_obj = payload.get("approval_auto_apply")
if isinstance(approval_auto_apply_payload_obj, dict):
    observed_mode_effective = _normalize(approval_auto_apply_payload_obj.get("recommendation_mode"))
    observed_mode_preferred = _normalize(approval_auto_apply_payload_obj.get("recommendation_mode_preferred"))
    observed_quality_risk_projected_streak = _parse_int(
        approval_auto_apply_payload_obj.get("quality_risk_projected_streak_for_auto")
    )
    observed_quality_risk_persistent = bool(
        approval_auto_apply_payload_obj.get("quality_risk_persistent_for_auto")
    )
    observed_strict_guardrail_max = _parse_float(
        approval_auto_apply_payload_obj.get("strict_guardrail_max")
    )

    approval_auto_apply_payload_checks["ready"] = True
    approval_auto_apply_payload_checks["checks"] = {
        "mode_effective_matches_expected": (
            observed_mode_effective == _normalize(auto_apply_recommendation_mode_effective)
        ),
        "mode_preferred_matches_expected": (
            observed_mode_preferred == _normalize(auto_apply_recommendation_mode)
        ),
        "quality_risk_projected_streak_matches_expected": (
            observed_quality_risk_projected_streak == int(quality_risk_projected_streak_for_auto)
        ),
        "quality_risk_persistent_matches_expected": (
            observed_quality_risk_persistent == bool(quality_risk_persistent_for_auto)
        ),
        "strict_guardrail_max_matches_expected": (
            isinstance(observed_strict_guardrail_max, float)
            and abs(observed_strict_guardrail_max - float(strict_guardrail_max)) <= 1e-9
        ),
    }
    approval_auto_apply_payload_checks["details"] = {
        "observed_mode_effective": observed_mode_effective,
        "observed_mode_preferred": observed_mode_preferred,
        "observed_quality_risk_projected_streak": observed_quality_risk_projected_streak,
        "observed_quality_risk_persistent": observed_quality_risk_persistent,
        "observed_strict_guardrail_max": observed_strict_guardrail_max,
    }
    approval_auto_apply_payload_checks["overall_pass"] = bool(
        all(bool(v) for v in approval_auto_apply_payload_checks["checks"].values())
    )

trader_mode_expected = "shadow_only" if "shadow" in live_mode_display.lower() else "live"
trader_view_payload_checks = {
    "ready": isinstance(trader_view, dict),
    "checks": {},
    "details": {},
    "overall_pass": True,
}
if trader_view_payload_checks["ready"]:
    observed_mode = _normalize(trader_view.get("mode"))
    observed_recommendation = _normalize(trader_view.get("live_recommendation"))
    observed_confidence = _parse_float(trader_view.get("confidence_score"))
    observed_approval_rate = _parse_float(trader_view.get("approval_rate"))
    observed_plan_conversion = _parse_float(trader_view.get("plan_conversion_rate"))
    observed_top_blocker_reason = _normalize(trader_view.get("top_blocker_reason"))
    observed_top_blocker_share = _parse_float(trader_view.get("top_blocker_share_of_blocked"))
    observed_freshness_block_rate = _parse_float(trader_view.get("freshness_block_rate"))
    observed_settled_available = _parse_bool(trader_view.get("settled_predictions_available"), False)
    observed_actions = trader_view.get("next_actions_top3")

    trader_view_payload_checks["checks"] = {
        "mode_matches_expected": (observed_mode == trader_mode_expected),
        "recommendation_matches_expected": (observed_recommendation == _normalize(live_recommendation)),
        "confidence_matches_expected": (
            isinstance(observed_confidence, float)
            and abs(observed_confidence - float(deployment_confidence_score)) <= 0.001
        ),
        "approval_rate_matches_expected": (
            isinstance(observed_approval_rate, float)
            and abs(observed_approval_rate - float(approval_rate)) <= 1e-6
        ),
        "plan_conversion_matches_expected": (
            (
                isinstance(approval_to_plan_rate, float)
                and isinstance(observed_plan_conversion, float)
                and abs(observed_plan_conversion - float(approval_to_plan_rate)) <= 1e-6
            )
            or (
                not isinstance(approval_to_plan_rate, float)
                and observed_plan_conversion is None
            )
        ),
        "top_blocker_reason_matches_expected": (
            observed_top_blocker_reason == _normalize(top_blocker_reason)
        ),
        "top_blocker_share_matches_expected": (
            isinstance(observed_top_blocker_share, float)
            and abs(observed_top_blocker_share - float(top_blocker_share_of_blocked)) <= 1e-6
        ),
        "freshness_rate_matches_expected": (
            isinstance(observed_freshness_block_rate, float)
            and abs(observed_freshness_block_rate - float(stale_rate)) <= 1e-6
        ),
        "settled_available_matches_expected": (
            observed_settled_available == bool(has_settled_predictions)
        ),
        "next_actions_present_if_suggestions": (
            isinstance(observed_actions, list)
            and (
                len(display_suggestions) == 0
                or len(observed_actions) >= min(3, len(display_suggestions))
            )
        ),
    }
    trader_view_payload_checks["details"] = {
        "observed_mode": observed_mode,
        "expected_mode": trader_mode_expected,
        "observed_recommendation": observed_recommendation,
        "expected_recommendation": _normalize(live_recommendation),
        "observed_confidence": observed_confidence,
        "expected_confidence": round(float(deployment_confidence_score), 3),
        "observed_approval_rate": observed_approval_rate,
        "expected_approval_rate": round(float(approval_rate), 6),
        "observed_plan_conversion": observed_plan_conversion,
        "expected_plan_conversion": (
            round(float(approval_to_plan_rate), 6)
            if isinstance(approval_to_plan_rate, float)
            else None
        ),
        "observed_top_blocker_reason": observed_top_blocker_reason,
        "expected_top_blocker_reason": _normalize(top_blocker_reason),
        "observed_top_blocker_share": observed_top_blocker_share,
        "expected_top_blocker_share": round(float(top_blocker_share_of_blocked), 6),
        "observed_freshness_block_rate": observed_freshness_block_rate,
        "expected_freshness_block_rate": round(float(stale_rate), 6),
        "observed_settled_available": observed_settled_available,
        "expected_settled_available": bool(has_settled_predictions),
        "observed_actions_count": (
            len(observed_actions) if isinstance(observed_actions, list) else None
        ),
        "expected_actions_min_count": (
            min(3, len(display_suggestions)) if len(display_suggestions) > 0 else 0
        ),
    }
    trader_view_payload_checks["overall_pass"] = bool(
        all(bool(v) for v in trader_view_payload_checks["checks"].values())
    )
else:
    trader_view_payload_checks["overall_pass"] = False

payload["payload_consistency_checks"] = {
    "approval_auto_apply": approval_auto_apply_payload_checks,
    "trader_view": trader_view_payload_checks,
}
if isinstance(payload.get("headline_metrics"), dict):
    payload["headline_metrics"]["approval_auto_apply_payload_consistent"] = bool(
        approval_auto_apply_payload_checks["overall_pass"]
    )
    payload["headline_metrics"]["trader_view_payload_consistent"] = bool(
        trader_view_payload_checks["overall_pass"]
    )

if not approval_auto_apply_payload_checks["overall_pass"]:
    note = (
        "approval_auto_apply payload mismatch: recommendation/quality-risk fields differ from effective runtime state; "
        "verify serializer keys and payload assembly before trusting auto-apply diagnostics."
    )
    existing_notes = payload.get("data_consistency_notes")
    if isinstance(existing_notes, list):
        existing_notes.append(note)
    else:
        payload["data_consistency_notes"] = [note]
    if isinstance(payload.get("health"), dict):
        health_obj = payload["health"]
        issues = health_obj.get("issues")
        if isinstance(issues, list):
            if "auto_apply_payload_inconsistent" not in issues:
                issues.append("auto_apply_payload_inconsistent")
        else:
            issues = ["auto_apply_payload_inconsistent"]
            health_obj["issues"] = issues
        health_obj["status"] = "RED"
        health_obj["issue_count"] = int(len(issues))
        reason_text = _normalize(health_obj.get("reason_text"))
        if reason_text and reason_text.lower() not in {"none", "n/a", "unknown"}:
            if "auto apply payload inconsistent" not in reason_text:
                health_obj["reason_text"] = reason_text + ", auto apply payload inconsistent"
        else:
            health_obj["reason_text"] = "auto apply payload inconsistent"
    if isinstance(payload.get("headline_metrics"), dict):
        payload["headline_metrics"]["health_status"] = "RED"

if not trader_view_payload_checks["overall_pass"]:
    note = (
        "trader_view payload mismatch: decision/risk snapshot fields differ from runtime-derived headline metrics; "
        "verify trader_view serializer mappings before trusting quick decision outputs."
    )
    existing_notes = payload.get("data_consistency_notes")
    if isinstance(existing_notes, list):
        existing_notes.append(note)
    else:
        payload["data_consistency_notes"] = [note]
    if isinstance(payload.get("health"), dict):
        health_obj = payload["health"]
        issues = health_obj.get("issues")
        if isinstance(issues, list):
            if "trader_view_payload_inconsistent" not in issues:
                issues.append("trader_view_payload_inconsistent")
        else:
            issues = ["trader_view_payload_inconsistent"]
            health_obj["issues"] = issues
        health_obj["status"] = "RED"
        health_obj["issue_count"] = int(len(issues))
        reason_text = _normalize(health_obj.get("reason_text"))
        if reason_text and reason_text.lower() not in {"none", "n/a", "unknown"}:
            if "trader view payload inconsistent" not in reason_text:
                health_obj["reason_text"] = reason_text + ", trader view payload inconsistent"
        else:
            health_obj["reason_text"] = "trader view payload inconsistent"
    if isinstance(payload.get("headline_metrics"), dict):
        payload["headline_metrics"]["health_status"] = "RED"

payload["artifact_writes"] = {
    "auto_apply_state_write_ok": bool(auto_apply_state_write_ok),
    "auto_apply_profile_write_attempted": bool(auto_apply_profile_write_attempted),
    "auto_apply_profile_write_ok": bool(auto_apply_profile_write_ok),
    "quality_risk_state_write_ok": bool(quality_risk_state_write_ok),
    "summary_output_write_ok": None,
    "summary_latest_write_ok": None,
    "summary_health_latest_write_ok": None,
}
output_write_ok = _safe_write_json(output_path, payload)
latest_write_ok = _safe_write_json(latest_path, payload)
health_latest_write_ok = _safe_write_json(health_latest_path, payload)

payload["artifact_writes"]["summary_output_write_ok"] = bool(output_write_ok)
payload["artifact_writes"]["summary_latest_write_ok"] = bool(latest_write_ok)
payload["artifact_writes"]["summary_health_latest_write_ok"] = bool(health_latest_write_ok)

output_backfill_ok = bool(output_write_ok) and _safe_write_json(output_path, payload)
latest_backfill_ok = bool(latest_write_ok) and _safe_write_json(latest_path, payload)
health_latest_backfill_ok = bool(health_latest_write_ok) and _safe_write_json(health_latest_path, payload)
failed_artifacts = []
if not output_write_ok or not output_backfill_ok:
    failed_artifacts.append(str(output_path))
if not latest_write_ok or not latest_backfill_ok:
    failed_artifacts.append(str(latest_path))
if not health_latest_write_ok or not health_latest_backfill_ok:
    failed_artifacts.append(str(health_latest_path))
if failed_artifacts:
    print(
        "alpha_summary artifact write failure: " + ", ".join(failed_artifacts),
        file=sys.stderr,
    )
    raise SystemExit(2)

print(str(output_path))
PY
render_stage_end_epoch="$(date +%s)"
ALPHA_SUMMARY_STAGE_SECONDS_RENDER_SUMMARY=$((render_stage_end_epoch - render_stage_start_epoch))
export ALPHA_SUMMARY_STAGE_SECONDS_RENDER_SUMMARY
echo "alpha_summary_stage stage=render_summary status=ok exit=0 duration_sec=$ALPHA_SUMMARY_STAGE_SECONDS_RENDER_SUMMARY" >> "$LOG_FILE"

if [[ "$ALPHA_SUMMARY_SEND_WEBHOOK" == "1" && -n "$ALPHA_SUMMARY_WEBHOOK_URL" ]] \
  || [[ "$ALPHA_SUMMARY_SEND_ALPHA_WEBHOOK" == "1" && -n "$ALPHA_SUMMARY_WEBHOOK_ALPHA_URL" ]] \
  || [[ "$ALPHA_SUMMARY_SEND_OPS_WEBHOOK" == "1" && -n "$ALPHA_SUMMARY_WEBHOOK_OPS_URL" ]]; then
  webhook_stage_start_epoch="$(date +%s)"
  webhook_payload_dir="$(mktemp -d)"
  webhook_payload_selected_file="$webhook_payload_dir/selected.json"
  webhook_payload_alpha_file="$webhook_payload_dir/alpha.json"
  webhook_payload_ops_file="$webhook_payload_dir/ops.json"
  trap 'rm -rf "$webhook_payload_dir"' EXIT

  "$PYTHON_BIN" - "$alpha_summary_file" "$webhook_payload_selected_file" "$webhook_payload_alpha_file" "$webhook_payload_ops_file" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
selected_out = Path(sys.argv[2])
alpha_out = Path(sys.argv[3])
ops_out = Path(sys.argv[4])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    payload = {}

def _normalize(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()

def _to_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    if parsed != parsed:
        return None
    return parsed

def _to_int(value: object, default: int = 0) -> int:
    parsed = _to_float(value)
    if parsed is None:
        return default
    try:
        return int(parsed)
    except Exception:
        return default

def _fmt_pct(value: object) -> str:
    parsed = _to_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed*100.0:.2f}%"

def _fmt_secs(value: object) -> str:
    parsed = _to_float(value)
    if parsed is None:
        return "n/a"
    return f"{int(max(0, parsed))}s"

def _load_json_dict(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}

selected_text = _normalize(payload.get("discord_message"))
if not selected_text:
    selected_text = "BetBot alpha summary failed to render."
concise_text = _normalize(payload.get("discord_message_concise"))
if not concise_text:
    concise_text = selected_text

captured_text = _normalize(payload.get("captured_at"))
captured_label = captured_text
if captured_text:
    try:
        dt = datetime.fromisoformat(captured_text.replace("Z", "+00:00"))
        captured_label = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        captured_label = captured_text

window_label = _normalize(payload.get("window_label")) or _normalize(payload.get("label")) or "12h"
health = payload.get("health") if isinstance(payload.get("health"), dict) else {}
health_status = _normalize(health.get("status")).upper() or "UNKNOWN"
health_reason = _normalize(health.get("reason_text"))
if not health_reason or health_reason.lower() in {"none", "n/a", "unknown"}:
    health_reason = "none"
headline = payload.get("headline_metrics") if isinstance(payload.get("headline_metrics"), dict) else {}
intents_total = _to_int(headline.get("intents_total"))
intents_approved = _to_int(headline.get("intents_approved"))
planned_orders = _to_int(headline.get("planned_orders"))
approval_rate = _fmt_pct(headline.get("approval_rate"))
stale_rate = _fmt_pct(headline.get("stale_rate"))
top_blockers = payload.get("top_blockers") if isinstance(payload.get("top_blockers"), list) else []
top_blocker_reason = "none"
top_blocker_count = 0
if top_blockers:
    first = top_blockers[0] if isinstance(top_blockers[0], dict) else {}
    top_blocker_reason = _normalize(first.get("reason")) or "unknown"
    top_blocker_count = _to_int(first.get("count"))
runtime = payload.get("runtime_stage_seconds") if isinstance(payload.get("runtime_stage_seconds"), dict) else {}
runtime_total = _fmt_secs(runtime.get("total"))
runtime_render = _fmt_secs(runtime.get("render_summary"))
runtime_select = _fmt_secs(runtime.get("selection_quality"))
runtime_bankroll = _fmt_secs(runtime.get("bankroll_validation"))
artifacts = payload.get("artifact_writes") if isinstance(payload.get("artifact_writes"), dict) else {}
artifact_ok = all(bool(v) for v in artifacts.values()) if artifacts else True
msg_quality = payload.get("message_quality_checks") if isinstance(payload.get("message_quality_checks"), dict) else {}
msg_quality_ok = bool(msg_quality.get("overall_pass"))

output_dir = path.parent.parent
health_dir = output_dir / "health"
coldmath_hardening_payload = _load_json_dict(health_dir / "coldmath_hardening_latest.json")
lane_alert_state = _load_json_dict(health_dir / ".decision_matrix_lane_alert_state.json")
targeted_trading_support = (
    coldmath_hardening_payload.get("targeted_trading_support")
    if isinstance(coldmath_hardening_payload.get("targeted_trading_support"), dict)
    else {}
)
lane_checks = (
    targeted_trading_support.get("checks")
    if isinstance(targeted_trading_support.get("checks"), dict)
    else {}
)
lane_observed = (
    targeted_trading_support.get("observed")
    if isinstance(targeted_trading_support.get("observed"), dict)
    else {}
)
lane_thresholds = (
    targeted_trading_support.get("thresholds")
    if isinstance(targeted_trading_support.get("thresholds"), dict)
    else {}
)

decision_matrix_strict_signal = bool(lane_checks.get("decision_matrix_strict_signal") is True)
decision_matrix_bootstrap_signal_raw = bool(lane_checks.get("decision_matrix_bootstrap_signal_raw") is True)
decision_matrix_bootstrap_signal = bool(lane_checks.get("decision_matrix_bootstrap_signal") is True)
decision_matrix_bootstrap_guard_status = _normalize(
    lane_observed.get("decision_matrix_bootstrap_guard_status")
).lower()
decision_matrix_bootstrap_guard_reasons_raw = (
    lane_observed.get("decision_matrix_bootstrap_guard_reasons")
    if isinstance(lane_observed.get("decision_matrix_bootstrap_guard_reasons"), list)
    else []
)
decision_matrix_bootstrap_guard_reasons = [
    _normalize(item).lower().replace("_", " ")
    for item in decision_matrix_bootstrap_guard_reasons_raw
    if _normalize(item)
]
decision_matrix_bootstrap_elapsed_hours = _to_float(
    lane_observed.get("decision_matrix_bootstrap_guard_elapsed_hours")
)
decision_matrix_bootstrap_max_hours = _to_float(
    lane_thresholds.get("matrix_bootstrap_max_hours")
)
decision_matrix_bootstrap_hours_to_expiry = None
if (
    isinstance(decision_matrix_bootstrap_elapsed_hours, float)
    and isinstance(decision_matrix_bootstrap_max_hours, float)
    and decision_matrix_bootstrap_max_hours > 0
):
    decision_matrix_bootstrap_hours_to_expiry = max(
        0.0,
        float(decision_matrix_bootstrap_max_hours - decision_matrix_bootstrap_elapsed_hours),
    )

decision_matrix_lane_line = "Decision matrix lane: unavailable."
if decision_matrix_strict_signal:
    decision_matrix_lane_line = "Decision matrix lane: strict pass (bootstrap not used)."
elif decision_matrix_bootstrap_signal:
    if isinstance(decision_matrix_bootstrap_hours_to_expiry, float):
        decision_matrix_lane_line = (
            "Decision matrix lane: bootstrap pass "
            f"({decision_matrix_bootstrap_elapsed_hours:.1f}h elapsed, "
            f"~{decision_matrix_bootstrap_hours_to_expiry:.1f}h to expiry)."
        )
    elif isinstance(decision_matrix_bootstrap_elapsed_hours, float):
        decision_matrix_lane_line = (
            "Decision matrix lane: bootstrap pass "
            f"({decision_matrix_bootstrap_elapsed_hours:.1f}h elapsed)."
        )
    else:
        decision_matrix_lane_line = "Decision matrix lane: bootstrap pass."
elif decision_matrix_bootstrap_signal_raw and not decision_matrix_bootstrap_signal:
    blocker_reason = (
        ", ".join(decision_matrix_bootstrap_guard_reasons[:2])
        if decision_matrix_bootstrap_guard_reasons
        else (decision_matrix_bootstrap_guard_status or "guard blocked")
    )
    decision_matrix_lane_line = f"Decision matrix lane: bootstrap blocked ({blocker_reason})."
elif lane_checks:
    decision_matrix_lane_line = "Decision matrix lane: failed (strict and bootstrap both off)."

lane_alert_degraded_statuses_raw = (
    lane_alert_state.get("degraded_statuses")
    if isinstance(lane_alert_state.get("degraded_statuses"), list)
    else []
)
lane_alert_degraded_statuses = {
    _normalize(item).lower()
    for item in lane_alert_degraded_statuses_raw
    if _normalize(item)
}
lane_alert_streak_count = max(0, _to_int(lane_alert_state.get("degraded_streak_count")))
lane_alert_streak_threshold = max(0, _to_int(lane_alert_state.get("degraded_streak_threshold")))
lane_alert_streak_notify_every = max(0, _to_int(lane_alert_state.get("degraded_streak_notify_every")))
lane_alert_status = _normalize(lane_alert_state.get("last_lane_status")).lower()
lane_alert_notify_reason = _normalize(lane_alert_state.get("last_notify_reason")).lower()
decision_matrix_lane_streak_line = ""
lane_status_is_degraded = bool(
    lane_alert_status
    and (
        (lane_alert_status in lane_alert_degraded_statuses)
        if lane_alert_degraded_statuses
        else (lane_alert_status in {"matrix_failed", "bootstrap_blocked"})
    )
)
if lane_alert_streak_count > 0 and lane_status_is_degraded:
    threshold_label = f"{lane_alert_streak_threshold:,}" if lane_alert_streak_threshold > 0 else "off"
    every_label = f"{lane_alert_streak_notify_every:,}" if lane_alert_streak_notify_every > 0 else "n/a"
    decision_matrix_lane_streak_line = (
        "Decision matrix degraded streak: "
        f"{lane_alert_streak_count:,} run(s) "
        f"({lane_alert_status.replace('_', ' ')}, threshold {threshold_label}, every {every_label})"
    )
    if lane_alert_notify_reason == "degraded_streak":
        decision_matrix_lane_streak_line += " [streak alert fired]"
    decision_matrix_lane_streak_line += "."

ops_lines = [
    f"BetBot Ops Status ({window_label}, rolling) — {captured_label}",
    f"Health: {health_status} (reason: {health_reason})",
    f"Flow: intents {intents_total:,} | approved {intents_approved:,} ({approval_rate}) | planned {planned_orders:,}",
    f"Blockers: top {top_blocker_reason} ({top_blocker_count:,}) | stale {stale_rate}",
    decision_matrix_lane_line,
    decision_matrix_lane_streak_line,
    (
        "Runtime: "
        f"total {runtime_total} | summarize { _fmt_secs(runtime.get('summarize_window')) } | "
        f"bankroll {runtime_bankroll} | selection {runtime_select} | render {runtime_render}"
    ),
    f"Artifact writes: {'ok' if artifact_ok else 'check failed'} | message quality: {'ok' if msg_quality_ok else 'check failed'}",
    "Scope: ops heartbeat only; alpha decisions are posted by BetBot Alpha.",
]
ops_text = "\n".join([line[:240] for line in ops_lines if _normalize(line)])
if len(ops_text) > 1900:
    ops_text = ops_text[:1897].rstrip() + "..."

selected_out.write_text(
    json.dumps({"text": selected_text, "content": selected_text, "username": "BetBot"}),
    encoding="utf-8",
)
alpha_out.write_text(
    json.dumps({"text": concise_text, "content": concise_text, "username": "BetBot Alpha"}),
    encoding="utf-8",
)
ops_out.write_text(
    json.dumps({"text": ops_text, "content": ops_text, "username": "BetBot Ops"}),
    encoding="utf-8",
)
PY

  _send_webhook_payload() {
    local target_url="$1"
    local payload_file="$2"
    local timeout_seconds="$3"
    local stage_label="$4"
    local rc=0
    if [[ -z "$target_url" ]]; then
      return 0
    fi
    if ! curl --silent --show-error --fail \
      --max-time "$timeout_seconds" \
      --header "Content-Type: application/json" \
      --data-binary "@$payload_file" \
      "$target_url" >/dev/null 2>&1; then
      rc=$?
    fi
    if [[ "$rc" -eq 0 ]]; then
      echo "alpha_summary_stage stage=$stage_label status=ok exit=0" >> "$LOG_FILE"
    else
      echo "alpha_summary_stage stage=$stage_label status=error exit=$rc" >> "$LOG_FILE"
    fi
    return "$rc"
  }

  webhook_send_rc=0
  webhook_attempt_count=0
  webhook_success_count=0
  webhook_failure_count=0

  if [[ "$ALPHA_SUMMARY_SEND_WEBHOOK" == "1" && -n "$ALPHA_SUMMARY_WEBHOOK_TARGET_URL" ]]; then
    webhook_attempt_count=$((webhook_attempt_count + 1))
    if _send_webhook_payload "$ALPHA_SUMMARY_WEBHOOK_TARGET_URL" "$webhook_payload_selected_file" "$ALPHA_SUMMARY_WEBHOOK_TIMEOUT_SECONDS" "webhook_selected"; then
      webhook_success_count=$((webhook_success_count + 1))
    else
      webhook_failure_count=$((webhook_failure_count + 1))
      webhook_send_rc=1
    fi
  fi

  if [[ "$ALPHA_SUMMARY_SEND_ALPHA_WEBHOOK" == "1" && -n "$ALPHA_SUMMARY_WEBHOOK_ALPHA_TARGET_URL" ]]; then
    webhook_attempt_count=$((webhook_attempt_count + 1))
    if _send_webhook_payload "$ALPHA_SUMMARY_WEBHOOK_ALPHA_TARGET_URL" "$webhook_payload_alpha_file" "$ALPHA_SUMMARY_WEBHOOK_ALPHA_TIMEOUT_SECONDS" "webhook_alpha"; then
      webhook_success_count=$((webhook_success_count + 1))
    else
      webhook_failure_count=$((webhook_failure_count + 1))
      webhook_send_rc=1
    fi
  fi

  if [[ "$ALPHA_SUMMARY_SEND_OPS_WEBHOOK" == "1" && -n "$ALPHA_SUMMARY_WEBHOOK_OPS_TARGET_URL" ]]; then
    webhook_attempt_count=$((webhook_attempt_count + 1))
    if _send_webhook_payload "$ALPHA_SUMMARY_WEBHOOK_OPS_TARGET_URL" "$webhook_payload_ops_file" "$ALPHA_SUMMARY_WEBHOOK_OPS_TIMEOUT_SECONDS" "webhook_ops"; then
      webhook_success_count=$((webhook_success_count + 1))
    else
      webhook_failure_count=$((webhook_failure_count + 1))
      webhook_send_rc=1
    fi
  fi

  rm -rf "$webhook_payload_dir"
  trap - EXIT

  webhook_send_rc=0
  if [[ "$webhook_failure_count" -gt 0 ]]; then
    webhook_send_rc=1
  fi
  webhook_stage_end_epoch="$(date +%s)"
  ALPHA_SUMMARY_STAGE_SECONDS_WEBHOOK_SEND=$((webhook_stage_end_epoch - webhook_stage_start_epoch))
  export ALPHA_SUMMARY_STAGE_SECONDS_WEBHOOK_SEND
  if [[ "$webhook_send_rc" -eq 0 ]]; then
    echo "alpha_summary_stage stage=webhook_send status=ok exit=0 attempts=$webhook_attempt_count success=$webhook_success_count failures=$webhook_failure_count duration_sec=$ALPHA_SUMMARY_STAGE_SECONDS_WEBHOOK_SEND" >> "$LOG_FILE"
  else
    echo "alpha_summary_stage stage=webhook_send status=error exit=$webhook_send_rc attempts=$webhook_attempt_count success=$webhook_success_count failures=$webhook_failure_count duration_sec=$ALPHA_SUMMARY_STAGE_SECONDS_WEBHOOK_SEND" >> "$LOG_FILE"
  fi
fi

cycle_end_epoch="$(date +%s)"
ALPHA_SUMMARY_STAGE_SECONDS_TOTAL=$((cycle_end_epoch - cycle_start_epoch))
export ALPHA_SUMMARY_STAGE_SECONDS_TOTAL
echo "alpha_summary file=$alpha_summary_file window=$window_label webhook_sent=$ALPHA_SUMMARY_SEND_WEBHOOK duration_sec_total=$ALPHA_SUMMARY_STAGE_SECONDS_TOTAL duration_sec_summarize=${ALPHA_SUMMARY_STAGE_SECONDS_SUMMARIZE_WINDOW:-} duration_sec_bankroll=${ALPHA_SUMMARY_STAGE_SECONDS_BANKROLL_VALIDATION:-} duration_sec_alpha_gap=${ALPHA_SUMMARY_STAGE_SECONDS_ALPHA_GAP_REPORT:-} duration_sec_selection_quality=${ALPHA_SUMMARY_STAGE_SECONDS_SELECTION_QUALITY:-} duration_sec_render=${ALPHA_SUMMARY_STAGE_SECONDS_RENDER_SUMMARY:-} duration_sec_webhook=${ALPHA_SUMMARY_STAGE_SECONDS_WEBHOOK_SEND:-0}" >> "$LOG_FILE"
echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") alpha summary cycle end ===" >> "$LOG_FILE"
