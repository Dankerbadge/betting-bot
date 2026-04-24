#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

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

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs" "$OUTPUT_DIR/health" "$OUTPUT_DIR/health/recovery"

RECOVERY_DIR="${RECOVERY_DIR:-$OUTPUT_DIR/health/recovery}"
RECOVERY_STATE_FILE="${RECOVERY_STATE_FILE:-$RECOVERY_DIR/.recovery_state.json}"
RECOVERY_LATEST_FILE="${RECOVERY_LATEST_FILE:-$RECOVERY_DIR/recovery_latest.json}"
RECOVERY_LOG_FILE="${RECOVERY_LOG_FILE:-$OUTPUT_DIR/logs/pipeline_recovery.log}"
RECOVERY_MAX_EVENT_FILES="${RECOVERY_MAX_EVENT_FILES:-240}"

HEALTH_STATUS_FILE="${HEALTH_STATUS_FILE:-$OUTPUT_DIR/health/live_status_latest.json}"
READINESS_DIR="$OUTPUT_DIR"
RECOVERY_READINESS_RUNNER_STATE_FILE="${RECOVERY_READINESS_RUNNER_STATE_FILE:-$OUTPUT_DIR/health/readiness_runner_latest.json}"
RECOVERY_READINESS_RUNNER_FRESH_SECONDS="${RECOVERY_READINESS_RUNNER_FRESH_SECONDS:-900}"

RECOVERY_RUN_AS_USER="${RECOVERY_RUN_AS_USER:-betbot}"
if ! id "$RECOVERY_RUN_AS_USER" >/dev/null 2>&1; then
  RECOVERY_RUN_AS_USER="$(id -un)"
fi

RECOVERY_ENABLE_SERVICE_RESTARTS="${RECOVERY_ENABLE_SERVICE_RESTARTS:-1}"
RECOVERY_ENABLE_METAR_REFRESH="${RECOVERY_ENABLE_METAR_REFRESH:-1}"
RECOVERY_ENABLE_SETTLEMENT_REFRESH="${RECOVERY_ENABLE_SETTLEMENT_REFRESH:-1}"
RECOVERY_ENABLE_REPORTING_TRIGGER="${RECOVERY_ENABLE_REPORTING_TRIGGER:-1}"
RECOVERY_ENABLE_ALPHA_SUMMARY_TRIGGER="${RECOVERY_ENABLE_ALPHA_SUMMARY_TRIGGER:-1}"
RECOVERY_ENABLE_LOG_MAINTENANCE_TIMER_ENABLE="${RECOVERY_ENABLE_LOG_MAINTENANCE_TIMER_ENABLE:-1}"
RECOVERY_ENABLE_ALPHA_WORKER_RESTARTS="${RECOVERY_ENABLE_ALPHA_WORKER_RESTARTS:-1}"
RECOVERY_ENABLE_BREADTH_WORKER_RESTARTS="${RECOVERY_ENABLE_BREADTH_WORKER_RESTARTS:-1}"
RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER="${RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER:-1}"
RECOVERY_REQUIRE_ALPHA_WORKER="${RECOVERY_REQUIRE_ALPHA_WORKER:-${ALPHA_WORKER_ENABLED:-0}}"
RECOVERY_REQUIRE_BREADTH_WORKER="${RECOVERY_REQUIRE_BREADTH_WORKER:-${BREADTH_WORKER_ENABLED:-0}}"
RECOVERY_REQUIRE_LOG_MAINTENANCE_TIMER="${RECOVERY_REQUIRE_LOG_MAINTENANCE_TIMER:-1}"
RECOVERY_REQUIRE_ALPHA_SUMMARY_TIMER="${RECOVERY_REQUIRE_ALPHA_SUMMARY_TIMER:-1}"

RECOVERY_HEALTH_STALE_CRIT_SECONDS="${RECOVERY_HEALTH_STALE_CRIT_SECONDS:-900}"
RECOVERY_READINESS_STALE_CRIT_SECONDS="${RECOVERY_READINESS_STALE_CRIT_SECONDS:-21600}"
RECOVERY_ALPHA_SUMMARY_STALE_CRIT_SECONDS="${RECOVERY_ALPHA_SUMMARY_STALE_CRIT_SECONDS:-46800}"
RECOVERY_ALPHA_SUMMARY_TRIGGER_GRACE_SECONDS="${RECOVERY_ALPHA_SUMMARY_TRIGGER_GRACE_SECONDS:-300}"
RECOVERY_REPORTING_ACTIVATING_CRIT_SECONDS="${RECOVERY_REPORTING_ACTIVATING_CRIT_SECONDS:-2700}"
RECOVERY_LOG_MAINTENANCE_STALE_CRIT_SECONDS="${RECOVERY_LOG_MAINTENANCE_STALE_CRIT_SECONDS:-7200}"
RECOVERY_LOG_MAINTENANCE_TRIGGER_ON_YELLOW="${RECOVERY_LOG_MAINTENANCE_TRIGGER_ON_YELLOW:-0}"
# When 1, only restart a long-activating reporting job if readiness is stale/missing
# or pipeline is red. This avoids false restarts during heavy-but-progressing runs.
RECOVERY_REPORTING_ACTIVATING_RESTART_REQUIRE_STALE="${RECOVERY_REPORTING_ACTIVATING_RESTART_REQUIRE_STALE:-1}"
RECOVERY_RED_CONSECUTIVE_THRESHOLD="${RECOVERY_RED_CONSECUTIVE_THRESHOLD:-2}"
RECOVERY_PIPELINE_RED_CONSECUTIVE_THRESHOLD="${RECOVERY_PIPELINE_RED_CONSECUTIVE_THRESHOLD:-2}"
RECOVERY_FRESHNESS_PRESSURE_CONSECUTIVE_THRESHOLD="${RECOVERY_FRESHNESS_PRESSURE_CONSECUTIVE_THRESHOLD:-3}"
RECOVERY_RETRY_PRESSURE_CONSECUTIVE_THRESHOLD="${RECOVERY_RETRY_PRESSURE_CONSECUTIVE_THRESHOLD:-3}"
RECOVERY_FRESHNESS_STALE_RATE_CRIT="${RECOVERY_FRESHNESS_STALE_RATE_CRIT:-0.20}"
RECOVERY_FRESHNESS_MIN_INTENTS="${RECOVERY_FRESHNESS_MIN_INTENTS:-100}"

RECOVERY_SHADOW_RESTART_COOLDOWN_SECONDS="${RECOVERY_SHADOW_RESTART_COOLDOWN_SECONDS:-900}"
RECOVERY_REPORTING_RESTART_COOLDOWN_SECONDS="${RECOVERY_REPORTING_RESTART_COOLDOWN_SECONDS:-900}"
RECOVERY_ALPHA_WORKER_RESTART_COOLDOWN_SECONDS="${RECOVERY_ALPHA_WORKER_RESTART_COOLDOWN_SECONDS:-900}"
RECOVERY_BREADTH_WORKER_RESTART_COOLDOWN_SECONDS="${RECOVERY_BREADTH_WORKER_RESTART_COOLDOWN_SECONDS:-900}"
RECOVERY_METAR_REFRESH_COOLDOWN_SECONDS="${RECOVERY_METAR_REFRESH_COOLDOWN_SECONDS:-300}"
RECOVERY_SETTLEMENT_REFRESH_COOLDOWN_SECONDS="${RECOVERY_SETTLEMENT_REFRESH_COOLDOWN_SECONDS:-300}"
RECOVERY_REPORTING_TRIGGER_COOLDOWN_SECONDS="${RECOVERY_REPORTING_TRIGGER_COOLDOWN_SECONDS:-300}"
RECOVERY_ALPHA_SUMMARY_TRIGGER_COOLDOWN_SECONDS="${RECOVERY_ALPHA_SUMMARY_TRIGGER_COOLDOWN_SECONDS:-900}"
RECOVERY_LOG_MAINTENANCE_TIMER_ENABLE_COOLDOWN_SECONDS="${RECOVERY_LOG_MAINTENANCE_TIMER_ENABLE_COOLDOWN_SECONDS:-900}"
RECOVERY_LOG_MAINTENANCE_TRIGGER_COOLDOWN_SECONDS="${RECOVERY_LOG_MAINTENANCE_TRIGGER_COOLDOWN_SECONDS:-900}"
RECOVERY_SCAN_BUDGET_TRIM_COOLDOWN_SECONDS="${RECOVERY_SCAN_BUDGET_TRIM_COOLDOWN_SECONDS:-900}"
RECOVERY_SCAN_BUDGET_TRIM_FACTOR="${RECOVERY_SCAN_BUDGET_TRIM_FACTOR:-0.85}"
RECOVERY_SCAN_BUDGET_TRIM_MIN="${RECOVERY_SCAN_BUDGET_TRIM_MIN:-400}"
RECOVERY_ENABLE_SCAN_BUDGET_TRIM="${RECOVERY_ENABLE_SCAN_BUDGET_TRIM:-1}"

RECOVERY_WEBHOOK_URL="${RECOVERY_WEBHOOK_URL:-${ALERT_WEBHOOK_URL:-}}"
RECOVERY_WEBHOOK_THREAD_ID="${RECOVERY_WEBHOOK_THREAD_ID:-${ALERT_WEBHOOK_THREAD_ID:-}}"
RECOVERY_WEBHOOK_TIMEOUT_SECONDS="${RECOVERY_WEBHOOK_TIMEOUT_SECONDS:-5}"
RECOVERY_NOTIFY_STATUS_CHANGE_ONLY="${RECOVERY_NOTIFY_STATUS_CHANGE_ONLY:-1}"
RECOVERY_WEBHOOK_MESSAGE_MODE="${RECOVERY_WEBHOOK_MESSAGE_MODE:-concise}"
RECOVERY_WEBHOOK_USERNAME="${RECOVERY_WEBHOOK_USERNAME:-BetBot Recovery}"
RECOVERY_ALERT_STATE_FILE="${RECOVERY_ALERT_STATE_FILE:-$RECOVERY_DIR/.recovery_alert_state.json}"
RECOVERY_LOG_MAINTENANCE_SERVICE_NAME="${RECOVERY_LOG_MAINTENANCE_SERVICE_NAME:-betbot-temperature-log-maintenance.service}"
RECOVERY_LOG_MAINTENANCE_TIMER_NAME="${RECOVERY_LOG_MAINTENANCE_TIMER_NAME:-betbot-temperature-log-maintenance.timer}"
COLDMATH_HARDENING_STATUS_FILE="${COLDMATH_HARDENING_STATUS_FILE:-$OUTPUT_DIR/health/coldmath_hardening_latest.json}"
COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR="${COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR:-1}"
COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED="${COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED:-${COLDMATH_STAGE_TIMEOUT_GUARDRAILS_STRICT_REQUIRED:-1}}"
COLDMATH_HARDENING_ENABLED="${COLDMATH_HARDENING_ENABLED:-1}"
COLDMATH_STAGE_TIMEOUT_SECONDS="${COLDMATH_STAGE_TIMEOUT_SECONDS:-0}"
COLDMATH_SNAPSHOT_TIMEOUT_SECONDS="${COLDMATH_SNAPSHOT_TIMEOUT_SECONDS:-$COLDMATH_STAGE_TIMEOUT_SECONDS}"
COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS="${COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS:-$COLDMATH_STAGE_TIMEOUT_SECONDS}"
COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS="${COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS:-$COLDMATH_STAGE_TIMEOUT_SECONDS}"
COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS="${COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS:-$COLDMATH_STAGE_TIMEOUT_SECONDS}"
COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS="${COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS:-$COLDMATH_STAGE_TIMEOUT_SECONDS}"
COLDMATH_MARKET_INGEST_ENABLED="${COLDMATH_MARKET_INGEST_ENABLED:-1}"
COLDMATH_RECOVERY_ADVISOR_ENABLED="${COLDMATH_RECOVERY_ADVISOR_ENABLED:-1}"
COLDMATH_RECOVERY_LOOP_ENABLED="${COLDMATH_RECOVERY_LOOP_ENABLED:-1}"
COLDMATH_RECOVERY_CAMPAIGN_ENABLED="${COLDMATH_RECOVERY_CAMPAIGN_ENABLED:-1}"
RECOVERY_ENABLE_ENV_PERSISTENCE_REPAIR="${RECOVERY_ENABLE_ENV_PERSISTENCE_REPAIR:-1}"
RECOVERY_ENV_PERSISTENCE_REPAIR_COOLDOWN_SECONDS="${RECOVERY_ENV_PERSISTENCE_REPAIR_COOLDOWN_SECONDS:-900}"
RECOVERY_ENV_PERSISTENCE_REPAIR_SCRIPT="${RECOVERY_ENV_PERSISTENCE_REPAIR_SCRIPT:-$SCRIPT_DIR/set_coldmath_recovery_env_persistence_gate.sh}"
RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR="${RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR:-1}"
RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_COOLDOWN_SECONDS="${RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_COOLDOWN_SECONDS:-900}"
RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_SCRIPT="${RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_SCRIPT:-$SCRIPT_DIR/set_coldmath_stage_timeout_guardrails.sh}"
RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_GLOBAL_SECONDS="${RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_GLOBAL_SECONDS:-900}"
RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_ENV_REPAIR="${RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_ENV_REPAIR:-1}"
RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_STAGE_TIMEOUT_REPAIR="${RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_STAGE_TIMEOUT_REPAIR:-1}"
RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY="${RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY:-0}"
RECOVERY_EFFECTIVENESS_STALE_CRIT_SECONDS="${RECOVERY_EFFECTIVENESS_STALE_CRIT_SECONDS:-21600}"
RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_EFFECTIVENESS_GAP="${RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_EFFECTIVENESS_GAP:-1}"
RECOVERY_COLDMATH_HARDENING_TRIGGER_COOLDOWN_SECONDS="${RECOVERY_COLDMATH_HARDENING_TRIGGER_COOLDOWN_SECONDS:-900}"
RECOVERY_COLDMATH_HARDENING_SERVICE_NAME="${RECOVERY_COLDMATH_HARDENING_SERVICE_NAME:-betbot-temperature-coldmath-hardening.service}"

SETTLEMENT_TOP_N="${SETTLEMENT_TOP_N:-25}"
FINAL_REPORT_CACHE_TTL_MINUTES="${FINAL_REPORT_CACHE_TTL_MINUTES:-30}"
FINAL_REPORT_TIMEOUT_SECONDS="${FINAL_REPORT_TIMEOUT_SECONDS:-12}"
ADAPTIVE_MAX_MARKETS_STATE_FILE="${ADAPTIVE_MAX_MARKETS_STATE_FILE:-$OUTPUT_DIR/.adaptive_max_markets}"

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

normalize_binary_flag() {
  local raw="${1:-}"
  local default_value="${2:-0}"
  local lowered
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
  case "$lowered" in
    1|true|yes|on)
      echo "1"
      ;;
    0|false|no|off)
      echo "0"
      ;;
    *)
      echo "$default_value"
      ;;
  esac
}

normalize_reason_state_suffix() {
  local raw="${1:-}"
  local lowered
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]' | tr -d '\r' | xargs || true)"
  case "$lowered" in
    failed)
      echo "failed"
      ;;
    inactive|activating|deactivating|reloading|auto-restart|dead|exited|starting|stopping)
      echo "inactive"
      ;;
    unknown|"")
      echo "unknown"
      ;;
    *)
      echo "unknown"
      ;;
  esac
}

RECOVERY_WEBHOOK_TARGET_URL="$(build_discord_target_url "$RECOVERY_WEBHOOK_URL" "$RECOVERY_WEBHOOK_THREAD_ID")"
COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR="$(normalize_binary_flag "$COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR" "1")"
COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED="$(normalize_binary_flag "$COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED" "1")"
COLDMATH_HARDENING_ENABLED="$(normalize_binary_flag "$COLDMATH_HARDENING_ENABLED" "1")"
COLDMATH_MARKET_INGEST_ENABLED="$(normalize_binary_flag "$COLDMATH_MARKET_INGEST_ENABLED" "1")"
COLDMATH_RECOVERY_ADVISOR_ENABLED="$(normalize_binary_flag "$COLDMATH_RECOVERY_ADVISOR_ENABLED" "1")"
COLDMATH_RECOVERY_LOOP_ENABLED="$(normalize_binary_flag "$COLDMATH_RECOVERY_LOOP_ENABLED" "1")"
COLDMATH_RECOVERY_CAMPAIGN_ENABLED="$(normalize_binary_flag "$COLDMATH_RECOVERY_CAMPAIGN_ENABLED" "1")"
RECOVERY_ENABLE_ENV_PERSISTENCE_REPAIR="$(normalize_binary_flag "$RECOVERY_ENABLE_ENV_PERSISTENCE_REPAIR" "1")"
RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR="$(normalize_binary_flag "$RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR" "1")"
RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_ENV_REPAIR="$(normalize_binary_flag "$RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_ENV_REPAIR" "1")"
RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_STAGE_TIMEOUT_REPAIR="$(normalize_binary_flag "$RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_STAGE_TIMEOUT_REPAIR" "1")"
RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY="$(normalize_binary_flag "$RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY" "0")"
RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_EFFECTIVENESS_GAP="$(normalize_binary_flag "$RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_EFFECTIVENESS_GAP" "1")"
if [[ ! "$RECOVERY_ENV_PERSISTENCE_REPAIR_COOLDOWN_SECONDS" =~ ^[0-9]+$ ]]; then
  RECOVERY_ENV_PERSISTENCE_REPAIR_COOLDOWN_SECONDS=900
fi
if [[ ! "$RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_COOLDOWN_SECONDS" =~ ^[0-9]+$ ]]; then
  RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_COOLDOWN_SECONDS=900
fi
if [[ ! "$RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_GLOBAL_SECONDS" =~ ^[0-9]+$ ]] || (( RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_GLOBAL_SECONDS <= 0 )); then
  RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_GLOBAL_SECONDS=900
fi
if [[ ! "$RECOVERY_COLDMATH_HARDENING_TRIGGER_COOLDOWN_SECONDS" =~ ^[0-9]+$ ]]; then
  RECOVERY_COLDMATH_HARDENING_TRIGGER_COOLDOWN_SECONDS=900
fi
if [[ ! "$RECOVERY_EFFECTIVENESS_STALE_CRIT_SECONDS" =~ ^[0-9]+$ ]]; then
  RECOVERY_EFFECTIVENESS_STALE_CRIT_SECONDS=21600
fi

parse_extra_args() {
  local raw="${1:-}"
  local -n dest_ref="$2"
  dest_ref=()
  if [[ -n "$raw" ]]; then
    read -r -a dest_ref <<<"$raw"
  fi
}

EXTRA_METAR_ARGS_ARRAY=()
EXTRA_SETTLEMENT_ARGS_ARRAY=()
parse_extra_args "${EXTRA_METAR_ARGS:-}" EXTRA_METAR_ARGS_ARRAY
parse_extra_args "${EXTRA_SETTLEMENT_ARGS:-}" EXTRA_SETTLEMENT_ARGS_ARRAY

log_line() {
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[$ts] $*" >> "$RECOVERY_LOG_FILE"
}

run_as_user() {
  local target_user="$1"
  shift
  if [[ "$(id -un)" == "$target_user" ]]; then
    "$@"
  else
    sudo -u "$target_user" -- "$@"
  fi
}

service_exists() {
  local unit="$1"
  systemctl cat "$unit" >/dev/null 2>&1
}

service_state() {
  local unit="$1"
  local state
  state="$(systemctl is-active "$unit" 2>/dev/null || true)"
  state="$(echo "$state" | tr -d '\r' | tr '\n' ' ' | xargs)"
  [[ -n "$state" ]] || state="unknown"
  echo "$state"
}

service_enabled_state() {
  local unit="$1"
  local state
  state="$(systemctl is-enabled "$unit" 2>/dev/null || true)"
  state="$(echo "$state" | tr -d '\r' | tr '\n' ' ' | xargs)"
  [[ -n "$state" ]] || state="unknown"
  echo "$state"
}

reporting_activating_age_seconds() {
  local state sub_state state_change_mono_usec inactive_exit_mono_usec exec_main_start_mono_usec active_enter_mono_usec start_mono_usec uptime_seconds age_seconds
  state="$(service_state betbot-temperature-reporting.service)"
  sub_state="$(systemctl show -p SubState --value betbot-temperature-reporting.service 2>/dev/null || true)"
  sub_state="$(echo "$sub_state" | tr -d '\r' | tr '\n' ' ' | xargs)"
  if [[ "$state" != "activating" && "$sub_state" != "start" ]]; then
    echo "-1"
    return
  fi

  state_change_mono_usec="$(systemctl show -p StateChangeTimestampMonotonic --value betbot-temperature-reporting.service 2>/dev/null || true)"
  state_change_mono_usec="$(echo "$state_change_mono_usec" | tr -d '\r' | tr '\n' ' ' | xargs)"
  if [[ ! "$state_change_mono_usec" =~ ^[0-9]+$ ]]; then
    state_change_mono_usec=0
  fi

  inactive_exit_mono_usec="$(systemctl show -p InactiveExitTimestampMonotonic --value betbot-temperature-reporting.service 2>/dev/null || true)"
  inactive_exit_mono_usec="$(echo "$inactive_exit_mono_usec" | tr -d '\r' | tr '\n' ' ' | xargs)"
  if [[ ! "$inactive_exit_mono_usec" =~ ^[0-9]+$ ]]; then
    inactive_exit_mono_usec=0
  fi

  exec_main_start_mono_usec="$(systemctl show -p ExecMainStartTimestampMonotonic --value betbot-temperature-reporting.service 2>/dev/null || true)"
  exec_main_start_mono_usec="$(echo "$exec_main_start_mono_usec" | tr -d '\r' | tr '\n' ' ' | xargs)"
  if [[ ! "$exec_main_start_mono_usec" =~ ^[0-9]+$ ]]; then
    exec_main_start_mono_usec=0
  fi

  active_enter_mono_usec="$(systemctl show -p ActiveEnterTimestampMonotonic --value betbot-temperature-reporting.service 2>/dev/null || true)"
  active_enter_mono_usec="$(echo "$active_enter_mono_usec" | tr -d '\r' | tr '\n' ' ' | xargs)"
  if [[ ! "$active_enter_mono_usec" =~ ^[0-9]+$ ]]; then
    active_enter_mono_usec=0
  fi

  start_mono_usec=0
  if (( state_change_mono_usec > 0 )); then
    start_mono_usec="$state_change_mono_usec"
  elif (( inactive_exit_mono_usec > 0 )); then
    start_mono_usec="$inactive_exit_mono_usec"
  elif (( exec_main_start_mono_usec > 0 )); then
    start_mono_usec="$exec_main_start_mono_usec"
  elif (( active_enter_mono_usec > 0 )); then
    start_mono_usec="$active_enter_mono_usec"
  fi

  if (( start_mono_usec <= 0 )); then
    echo "-1"
    return
  fi

  uptime_seconds="$(cut -d. -f1 /proc/uptime 2>/dev/null || echo 0)"
  if [[ ! "$uptime_seconds" =~ ^[0-9]+$ ]]; then
    echo "-1"
    return
  fi

  age_seconds="$(( uptime_seconds - (start_mono_usec / 1000000) ))"
  if (( age_seconds < 0 )); then
    age_seconds=0
  fi
  echo "$age_seconds"
}

now_epoch="$(date +%s)"
health_snapshot="$("$PYTHON_BIN" - "$HEALTH_STATUS_FILE" "$READINESS_DIR" "$RECOVERY_STATE_FILE" "$RECOVERY_READINESS_RUNNER_STATE_FILE" "$now_epoch" <<'PY'
from __future__ import annotations

from pathlib import Path
import glob
import json
import os
import sys
from typing import Any


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default

def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def latest_file(directory: Path, pattern: str) -> Path | None:
    matches = glob.glob(str(directory / pattern))
    if not matches:
        return None
    matches.sort(key=lambda path: os.path.getmtime(path))
    return Path(matches[-1])


def load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


health_file = Path(sys.argv[1])
readiness_dir = Path(sys.argv[2])
state_file = Path(sys.argv[3])
readiness_runner_state_file = Path(sys.argv[4])
now_epoch = parse_int(sys.argv[5], 0)

health = load_json(health_file)
latest_readiness_file = latest_file(readiness_dir, "kalshi_temperature_live_readiness_*.json")
readiness = load_json(latest_readiness_file)
latest_alpha_summary_file = readiness_dir / "health" / "alpha_summary_latest.json"
latest_log_maintenance_file = readiness_dir / "health" / "log_maintenance" / "log_maintenance_latest.json"
log_maintenance = load_json(latest_log_maintenance_file)
state = load_json(state_file)
readiness_runner = load_json(readiness_runner_state_file)

health_status = normalize_text(health.get("status")).lower() or "unknown"
red_reasons = health.get("red_reasons") if isinstance(health.get("red_reasons"), list) else []
yellow_reasons = health.get("yellow_reasons") if isinstance(health.get("yellow_reasons"), list) else []
freshness = health.get("freshness_plan") if isinstance(health.get("freshness_plan"), dict) else {}
scan_budget = health.get("scan_budget") if isinstance(health.get("scan_budget"), dict) else {}
command_execution = health.get("command_execution") if isinstance(health.get("command_execution"), dict) else {}
latest_cycle_metrics = health.get("latest_cycle_metrics") if isinstance(health.get("latest_cycle_metrics"), dict) else {}
health_age = -1
if health_file.exists():
    try:
        health_age = max(0, int(now_epoch - int(health_file.stat().st_mtime)))
    except Exception:
        health_age = -1

executive = readiness.get("executive_summary") if isinstance(readiness.get("executive_summary"), dict) else {}
pipeline_status = normalize_text(executive.get("shortest_horizon_pipeline_status")).lower() or "unknown"
pipeline_reason = normalize_text(executive.get("shortest_horizon_pipeline_reason"))
readiness_age = -1
if latest_readiness_file is not None and latest_readiness_file.exists():
    try:
        readiness_age = max(0, int(now_epoch - int(latest_readiness_file.stat().st_mtime)))
    except Exception:
        readiness_age = -1

alpha_summary_age = -1
if latest_alpha_summary_file.exists():
    try:
        alpha_summary_age = max(0, int(now_epoch - int(latest_alpha_summary_file.stat().st_mtime)))
    except Exception:
        alpha_summary_age = -1

log_maintenance_health_status = normalize_text(log_maintenance.get("health_status")).lower() or "unknown"
log_maintenance_age = -1
if latest_log_maintenance_file.exists():
    try:
        log_maintenance_age = max(0, int(now_epoch - int(latest_log_maintenance_file.stat().st_mtime)))
    except Exception:
        log_maintenance_age = -1
log_maintenance_usage = (
    log_maintenance.get("usage")
    if isinstance(log_maintenance.get("usage"), dict)
    else {}
)
log_maintenance_usage_bytes = parse_int(log_maintenance_usage.get("log_dir_bytes"), 0)
readiness_runner_status = normalize_text(readiness_runner.get("run_status")).lower() or "unknown"
readiness_runner_stage = normalize_text(readiness_runner.get("stage")) or "unknown"
readiness_runner_age = -1
if readiness_runner_state_file.exists():
    try:
        readiness_runner_age = max(0, int(now_epoch - int(readiness_runner_state_file.stat().st_mtime)))
    except Exception:
        readiness_runner_age = -1

print(f"HEALTH_STATUS={health_status}")
print("HEALTH_RED_REASONS=" + ",".join(str(item).strip() for item in red_reasons if normalize_text(item)))
print("HEALTH_YELLOW_REASONS=" + ",".join(str(item).strip() for item in yellow_reasons if normalize_text(item)))
print(f"HEALTH_AGE_SECONDS={health_age}")
print(f"PIPELINE_STATUS={pipeline_status}")
print("PIPELINE_REASON=" + pipeline_reason.replace("\n", " ").replace("|", "/"))
print(f"READINESS_AGE_SECONDS={readiness_age}")
print(f"READINESS_FILE={latest_readiness_file or ''}")
print(f"READINESS_RUNNER_STATUS={readiness_runner_status}")
print("READINESS_RUNNER_STAGE=" + readiness_runner_stage.replace("\n", " ").replace("|", "/"))
print(f"READINESS_RUNNER_AGE_SECONDS={readiness_runner_age}")
print(f"ALPHA_SUMMARY_AGE_SECONDS={alpha_summary_age}")
print(f"ALPHA_SUMMARY_FILE={latest_alpha_summary_file if latest_alpha_summary_file.exists() else ''}")
print(f"LOG_MAINTENANCE_HEALTH_STATUS={log_maintenance_health_status}")
print(f"LOG_MAINTENANCE_AGE_SECONDS={log_maintenance_age}")
print(f"LOG_MAINTENANCE_USAGE_BYTES={log_maintenance_usage_bytes}")
print(f"FRESHNESS_PRESSURE_ACTIVE={1 if bool(freshness.get('pressure_active')) else 0}")
print(f"FRESHNESS_STALE_RATE={parse_float(freshness.get('metar_observation_stale_rate'), 0.0):.6f}")
print(f"FRESHNESS_STALE_COUNT={parse_int(freshness.get('metar_observation_stale_count'), 0)}")
print(f"FRESHNESS_APPROVAL_RATE={parse_float(freshness.get('approval_rate'), 0.0):.6f}")
print(f"FRESHNESS_INTENTS_TOTAL={parse_int(latest_cycle_metrics.get('intents_total'), 0)}")
print(f"COMMAND_METAR_ATTEMPTS={parse_int(command_execution.get('metar_attempts'), 1)}")
print(f"COMMAND_SETTLEMENT_ATTEMPTS={parse_int(command_execution.get('settlement_attempts'), 0)}")
print(f"COMMAND_SHADOW_ATTEMPTS={parse_int(command_execution.get('shadow_attempts'), 1)}")
print(f"SCAN_EFFECTIVE_MAX_MARKETS={parse_int(scan_budget.get('effective_max_markets'), 0)}")
print(f"SCAN_NEXT_MAX_MARKETS={parse_int(scan_budget.get('next_max_markets'), 0)}")
print("SCAN_ADAPTIVE_ACTION=" + normalize_text(scan_budget.get("adaptive_decision_action")))
print("SCAN_ADAPTIVE_REASON=" + normalize_text(scan_budget.get("adaptive_decision_reason")))
print(f"STATE_CONSECUTIVE_RED={parse_int(state.get('consecutive_red'), 0)}")
print(f"STATE_CONSECUTIVE_PIPELINE_RED={parse_int(state.get('consecutive_pipeline_red'), 0)}")
print(f"STATE_CONSECUTIVE_FRESHNESS_PRESSURE={parse_int(state.get('consecutive_freshness_pressure'), 0)}")
print(f"STATE_CONSECUTIVE_RETRY_PRESSURE={parse_int(state.get('consecutive_retry_pressure'), 0)}")
print(f"STATE_LAST_SHADOW_RESTART_EPOCH={parse_int(state.get('last_shadow_restart_epoch'), 0)}")
print(f"STATE_LAST_METAR_REFRESH_EPOCH={parse_int(state.get('last_metar_refresh_epoch'), 0)}")
print(f"STATE_LAST_SETTLEMENT_REFRESH_EPOCH={parse_int(state.get('last_settlement_refresh_epoch'), 0)}")
print(f"STATE_LAST_REPORTING_TRIGGER_EPOCH={parse_int(state.get('last_reporting_trigger_epoch'), 0)}")
print(f"STATE_LAST_REPORTING_RESTART_EPOCH={parse_int(state.get('last_reporting_restart_epoch'), 0)}")
print(f"STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH={parse_int(state.get('last_alpha_summary_trigger_epoch'), 0)}")
print(f"STATE_LAST_ALPHA_WORKER_RESTART_EPOCH={parse_int(state.get('last_alpha_worker_restart_epoch'), 0)}")
print(f"STATE_LAST_BREADTH_WORKER_RESTART_EPOCH={parse_int(state.get('last_breadth_worker_restart_epoch'), 0)}")
print(f"STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH={parse_int(state.get('last_log_maintenance_trigger_epoch'), 0)}")
print(f"STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH={parse_int(state.get('last_log_maintenance_timer_enable_epoch'), 0)}")
print(f"STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH={parse_int(state.get('last_recovery_env_persistence_repair_epoch'), 0)}")
print(f"STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH={parse_int(state.get('last_coldmath_stage_timeout_guardrail_repair_epoch'), 0)}")
print(f"STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH={parse_int(state.get('last_coldmath_hardening_trigger_epoch'), 0)}")
print(f"STATE_LAST_SCAN_BUDGET_TRIM_EPOCH={parse_int(state.get('last_scan_budget_trim_epoch'), 0)}")
PY
)"

HEALTH_STATUS="unknown"
HEALTH_RED_REASONS=""
HEALTH_YELLOW_REASONS=""
HEALTH_AGE_SECONDS="-1"
PIPELINE_STATUS="unknown"
PIPELINE_REASON=""
READINESS_AGE_SECONDS="-1"
READINESS_FILE=""
READINESS_RUNNER_STATUS="unknown"
READINESS_RUNNER_STAGE="unknown"
READINESS_RUNNER_AGE_SECONDS="-1"
ALPHA_SUMMARY_AGE_SECONDS="-1"
ALPHA_SUMMARY_FILE=""
LOG_MAINTENANCE_HEALTH_STATUS="unknown"
LOG_MAINTENANCE_AGE_SECONDS="-1"
LOG_MAINTENANCE_USAGE_BYTES=0
FRESHNESS_PRESSURE_ACTIVE=0
FRESHNESS_STALE_RATE="0"
FRESHNESS_STALE_COUNT=0
FRESHNESS_APPROVAL_RATE="0"
FRESHNESS_INTENTS_TOTAL=0
COMMAND_METAR_ATTEMPTS=1
COMMAND_SETTLEMENT_ATTEMPTS=0
COMMAND_SHADOW_ATTEMPTS=1
SCAN_EFFECTIVE_MAX_MARKETS=0
SCAN_NEXT_MAX_MARKETS=0
SCAN_ADAPTIVE_ACTION=""
SCAN_ADAPTIVE_REASON=""
STATE_CONSECUTIVE_RED=0
STATE_CONSECUTIVE_PIPELINE_RED=0
STATE_CONSECUTIVE_FRESHNESS_PRESSURE=0
STATE_CONSECUTIVE_RETRY_PRESSURE=0
STATE_LAST_SHADOW_RESTART_EPOCH=0
STATE_LAST_METAR_REFRESH_EPOCH=0
STATE_LAST_SETTLEMENT_REFRESH_EPOCH=0
STATE_LAST_REPORTING_TRIGGER_EPOCH=0
STATE_LAST_REPORTING_RESTART_EPOCH=0
STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH=0
STATE_LAST_ALPHA_WORKER_RESTART_EPOCH=0
STATE_LAST_BREADTH_WORKER_RESTART_EPOCH=0
STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH=0
STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH=0
STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH=0
STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH=0
STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH=0
STATE_LAST_SCAN_BUDGET_TRIM_EPOCH=0

while IFS='=' read -r key value; do
  case "$key" in
    HEALTH_STATUS) HEALTH_STATUS="$value" ;;
    HEALTH_RED_REASONS) HEALTH_RED_REASONS="$value" ;;
    HEALTH_YELLOW_REASONS) HEALTH_YELLOW_REASONS="$value" ;;
    HEALTH_AGE_SECONDS) HEALTH_AGE_SECONDS="$value" ;;
    PIPELINE_STATUS) PIPELINE_STATUS="$value" ;;
    PIPELINE_REASON) PIPELINE_REASON="$value" ;;
    READINESS_AGE_SECONDS) READINESS_AGE_SECONDS="$value" ;;
    READINESS_FILE) READINESS_FILE="$value" ;;
    READINESS_RUNNER_STATUS) READINESS_RUNNER_STATUS="$value" ;;
    READINESS_RUNNER_STAGE) READINESS_RUNNER_STAGE="$value" ;;
    READINESS_RUNNER_AGE_SECONDS) READINESS_RUNNER_AGE_SECONDS="$value" ;;
    ALPHA_SUMMARY_AGE_SECONDS) ALPHA_SUMMARY_AGE_SECONDS="$value" ;;
    ALPHA_SUMMARY_FILE) ALPHA_SUMMARY_FILE="$value" ;;
    LOG_MAINTENANCE_HEALTH_STATUS) LOG_MAINTENANCE_HEALTH_STATUS="$value" ;;
    LOG_MAINTENANCE_AGE_SECONDS) LOG_MAINTENANCE_AGE_SECONDS="$value" ;;
    LOG_MAINTENANCE_USAGE_BYTES) LOG_MAINTENANCE_USAGE_BYTES="$value" ;;
    FRESHNESS_PRESSURE_ACTIVE) FRESHNESS_PRESSURE_ACTIVE="$value" ;;
    FRESHNESS_STALE_RATE) FRESHNESS_STALE_RATE="$value" ;;
    FRESHNESS_STALE_COUNT) FRESHNESS_STALE_COUNT="$value" ;;
    FRESHNESS_APPROVAL_RATE) FRESHNESS_APPROVAL_RATE="$value" ;;
    FRESHNESS_INTENTS_TOTAL) FRESHNESS_INTENTS_TOTAL="$value" ;;
    COMMAND_METAR_ATTEMPTS) COMMAND_METAR_ATTEMPTS="$value" ;;
    COMMAND_SETTLEMENT_ATTEMPTS) COMMAND_SETTLEMENT_ATTEMPTS="$value" ;;
    COMMAND_SHADOW_ATTEMPTS) COMMAND_SHADOW_ATTEMPTS="$value" ;;
    SCAN_EFFECTIVE_MAX_MARKETS) SCAN_EFFECTIVE_MAX_MARKETS="$value" ;;
    SCAN_NEXT_MAX_MARKETS) SCAN_NEXT_MAX_MARKETS="$value" ;;
    SCAN_ADAPTIVE_ACTION) SCAN_ADAPTIVE_ACTION="$value" ;;
    SCAN_ADAPTIVE_REASON) SCAN_ADAPTIVE_REASON="$value" ;;
    STATE_CONSECUTIVE_RED) STATE_CONSECUTIVE_RED="$value" ;;
    STATE_CONSECUTIVE_PIPELINE_RED) STATE_CONSECUTIVE_PIPELINE_RED="$value" ;;
    STATE_CONSECUTIVE_FRESHNESS_PRESSURE) STATE_CONSECUTIVE_FRESHNESS_PRESSURE="$value" ;;
    STATE_CONSECUTIVE_RETRY_PRESSURE) STATE_CONSECUTIVE_RETRY_PRESSURE="$value" ;;
    STATE_LAST_SHADOW_RESTART_EPOCH) STATE_LAST_SHADOW_RESTART_EPOCH="$value" ;;
    STATE_LAST_METAR_REFRESH_EPOCH) STATE_LAST_METAR_REFRESH_EPOCH="$value" ;;
    STATE_LAST_SETTLEMENT_REFRESH_EPOCH) STATE_LAST_SETTLEMENT_REFRESH_EPOCH="$value" ;;
    STATE_LAST_REPORTING_TRIGGER_EPOCH) STATE_LAST_REPORTING_TRIGGER_EPOCH="$value" ;;
    STATE_LAST_REPORTING_RESTART_EPOCH) STATE_LAST_REPORTING_RESTART_EPOCH="$value" ;;
    STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH) STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH="$value" ;;
    STATE_LAST_ALPHA_WORKER_RESTART_EPOCH) STATE_LAST_ALPHA_WORKER_RESTART_EPOCH="$value" ;;
    STATE_LAST_BREADTH_WORKER_RESTART_EPOCH) STATE_LAST_BREADTH_WORKER_RESTART_EPOCH="$value" ;;
    STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH) STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH="$value" ;;
    STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH) STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH="$value" ;;
    STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH) STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH="$value" ;;
    STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH) STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH="$value" ;;
    STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH) STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH="$value" ;;
    STATE_LAST_SCAN_BUDGET_TRIM_EPOCH) STATE_LAST_SCAN_BUDGET_TRIM_EPOCH="$value" ;;
  esac
done <<< "$health_snapshot"

if [[ ! "$HEALTH_AGE_SECONDS" =~ ^-?[0-9]+$ ]]; then HEALTH_AGE_SECONDS=-1; fi
if [[ ! "$READINESS_AGE_SECONDS" =~ ^-?[0-9]+$ ]]; then READINESS_AGE_SECONDS=-1; fi
if [[ ! "$READINESS_RUNNER_AGE_SECONDS" =~ ^-?[0-9]+$ ]]; then READINESS_RUNNER_AGE_SECONDS=-1; fi
if [[ ! "$ALPHA_SUMMARY_AGE_SECONDS" =~ ^-?[0-9]+$ ]]; then ALPHA_SUMMARY_AGE_SECONDS=-1; fi
if [[ ! "$LOG_MAINTENANCE_AGE_SECONDS" =~ ^-?[0-9]+$ ]]; then LOG_MAINTENANCE_AGE_SECONDS=-1; fi
if [[ ! "$LOG_MAINTENANCE_USAGE_BYTES" =~ ^[0-9]+$ ]]; then LOG_MAINTENANCE_USAGE_BYTES=0; fi
if [[ ! "$FRESHNESS_PRESSURE_ACTIVE" =~ ^[0-9]+$ ]]; then FRESHNESS_PRESSURE_ACTIVE=0; fi
if [[ ! "$FRESHNESS_STALE_COUNT" =~ ^[0-9]+$ ]]; then FRESHNESS_STALE_COUNT=0; fi
if [[ ! "$FRESHNESS_INTENTS_TOTAL" =~ ^[0-9]+$ ]]; then FRESHNESS_INTENTS_TOTAL=0; fi
if [[ ! "$COMMAND_METAR_ATTEMPTS" =~ ^[0-9]+$ ]]; then COMMAND_METAR_ATTEMPTS=1; fi
if [[ ! "$COMMAND_SETTLEMENT_ATTEMPTS" =~ ^[0-9]+$ ]]; then COMMAND_SETTLEMENT_ATTEMPTS=0; fi
if [[ ! "$COMMAND_SHADOW_ATTEMPTS" =~ ^[0-9]+$ ]]; then COMMAND_SHADOW_ATTEMPTS=1; fi
if [[ ! "$SCAN_EFFECTIVE_MAX_MARKETS" =~ ^[0-9]+$ ]]; then SCAN_EFFECTIVE_MAX_MARKETS=0; fi
if [[ ! "$SCAN_NEXT_MAX_MARKETS" =~ ^[0-9]+$ ]]; then SCAN_NEXT_MAX_MARKETS=0; fi
if [[ ! "$STATE_CONSECUTIVE_RED" =~ ^[0-9]+$ ]]; then STATE_CONSECUTIVE_RED=0; fi
if [[ ! "$STATE_CONSECUTIVE_PIPELINE_RED" =~ ^[0-9]+$ ]]; then STATE_CONSECUTIVE_PIPELINE_RED=0; fi
if [[ ! "$STATE_CONSECUTIVE_FRESHNESS_PRESSURE" =~ ^[0-9]+$ ]]; then STATE_CONSECUTIVE_FRESHNESS_PRESSURE=0; fi
if [[ ! "$STATE_CONSECUTIVE_RETRY_PRESSURE" =~ ^[0-9]+$ ]]; then STATE_CONSECUTIVE_RETRY_PRESSURE=0; fi
if [[ ! "$STATE_LAST_SHADOW_RESTART_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_SHADOW_RESTART_EPOCH=0; fi
if [[ ! "$STATE_LAST_METAR_REFRESH_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_METAR_REFRESH_EPOCH=0; fi
if [[ ! "$STATE_LAST_SETTLEMENT_REFRESH_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_SETTLEMENT_REFRESH_EPOCH=0; fi
if [[ ! "$STATE_LAST_REPORTING_TRIGGER_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_REPORTING_TRIGGER_EPOCH=0; fi
if [[ ! "$STATE_LAST_REPORTING_RESTART_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_REPORTING_RESTART_EPOCH=0; fi
if [[ ! "$STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH=0; fi
if [[ ! "$STATE_LAST_ALPHA_WORKER_RESTART_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_ALPHA_WORKER_RESTART_EPOCH=0; fi
if [[ ! "$STATE_LAST_BREADTH_WORKER_RESTART_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_BREADTH_WORKER_RESTART_EPOCH=0; fi
if [[ ! "$STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH=0; fi
if [[ ! "$STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH=0; fi
if [[ ! "$STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH=0; fi
if [[ ! "$STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH=0; fi
if [[ ! "$STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH=0; fi
if [[ ! "$STATE_LAST_SCAN_BUDGET_TRIM_EPOCH" =~ ^[0-9]+$ ]]; then STATE_LAST_SCAN_BUDGET_TRIM_EPOCH=0; fi
if [[ ! "$FRESHNESS_STALE_RATE" =~ ^[0-9]+([.][0-9]+)?$ ]]; then FRESHNESS_STALE_RATE="0"; fi
if [[ ! "$FRESHNESS_APPROVAL_RATE" =~ ^[0-9]+([.][0-9]+)?$ ]]; then FRESHNESS_APPROVAL_RATE="0"; fi

shadow_service_state="$(service_state betbot-temperature-shadow.service)"
reporting_timer_state="$(service_state betbot-temperature-reporting.timer)"
reporting_service_state="$(service_state betbot-temperature-reporting.service)"
alpha_summary_timer_state="$(service_state betbot-temperature-alpha-summary.timer)"
alpha_worker_service_state="$(service_state betbot-temperature-alpha-workers.service)"
breadth_worker_service_state="$(service_state betbot-temperature-breadth-worker.service)"
log_maintenance_timer_state="$(service_state "$RECOVERY_LOG_MAINTENANCE_TIMER_NAME")"
log_maintenance_timer_enabled_state="$(service_enabled_state "$RECOVERY_LOG_MAINTENANCE_TIMER_NAME")"
log_maintenance_service_state="$(service_state "$RECOVERY_LOG_MAINTENANCE_SERVICE_NAME")"
shadow_service_reason_state="$(normalize_reason_state_suffix "$shadow_service_state")"
reporting_timer_reason_state="$(normalize_reason_state_suffix "$reporting_timer_state")"
alpha_summary_timer_reason_state="$(normalize_reason_state_suffix "$alpha_summary_timer_state")"
alpha_worker_service_reason_state="$(normalize_reason_state_suffix "$alpha_worker_service_state")"
breadth_worker_service_reason_state="$(normalize_reason_state_suffix "$breadth_worker_service_state")"
log_maintenance_timer_reason_state="$(normalize_reason_state_suffix "$log_maintenance_timer_state")"
reporting_service_activating_age_seconds="$(reporting_activating_age_seconds)"
if [[ ! "$reporting_service_activating_age_seconds" =~ ^-?[0-9]+$ ]]; then
  reporting_service_activating_age_seconds=-1
fi
alpha_summary_trigger_age_seconds=999999999
alpha_summary_trigger_grace_active=0
if (( STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH > 0 )); then
  alpha_summary_trigger_age_seconds=$((now_epoch - STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH))
  if (( alpha_summary_trigger_age_seconds < 0 )); then
    alpha_summary_trigger_age_seconds=0
  fi
  if (( alpha_summary_trigger_age_seconds <= RECOVERY_ALPHA_SUMMARY_TRIGGER_GRACE_SECONDS )); then
    alpha_summary_trigger_grace_active=1
  fi
fi

issue_detected=0
need_restart_shadow=0
need_restart_reporting=0
need_reset_failed_reporting=0
need_refresh_metar=0
need_refresh_settlement=0
need_reporting_trigger=0
need_reporting_timer_enable=0
need_alpha_summary_trigger=0
need_alpha_summary_timer_enable=0
need_log_maintenance_timer_enable=0
need_trim_scan_budget=0
need_restart_alpha_worker=0
need_restart_breadth_worker=0
need_trigger_log_maintenance=0
need_repair_recovery_env_persistence_gate=0
need_repair_coldmath_stage_timeout_guardrails=0
need_trigger_coldmath_hardening_on_effectiveness_gap=0

new_consecutive_red=0
new_consecutive_pipeline_red=0
if [[ "$HEALTH_STATUS" == "red" ]]; then
  new_consecutive_red=$((STATE_CONSECUTIVE_RED + 1))
  issue_detected=1
fi
if [[ "$PIPELINE_STATUS" == "red" ]]; then
  new_consecutive_pipeline_red=$((STATE_CONSECUTIVE_PIPELINE_RED + 1))
  issue_detected=1
fi

freshness_rate_milli="$("$PYTHON_BIN" - <<PY
rate = float("${FRESHNESS_STALE_RATE}")
print(int(round(rate * 1000.0)))
PY
)"
freshness_rate_crit_milli="$("$PYTHON_BIN" - <<PY
rate = float("${RECOVERY_FRESHNESS_STALE_RATE_CRIT}")
print(int(round(rate * 1000.0)))
PY
)"

freshness_pressure_now=0
if [[ "$FRESHNESS_PRESSURE_ACTIVE" == "1" ]]; then
  freshness_pressure_now=1
elif (( FRESHNESS_INTENTS_TOTAL >= RECOVERY_FRESHNESS_MIN_INTENTS )) && (( freshness_rate_milli >= freshness_rate_crit_milli )); then
  freshness_pressure_now=1
fi

new_consecutive_freshness_pressure=0
if (( freshness_pressure_now == 1 )); then
  new_consecutive_freshness_pressure=$((STATE_CONSECUTIVE_FRESHNESS_PRESSURE + 1))
fi

retry_pressure_now=0
if (( COMMAND_METAR_ATTEMPTS > 1 )) || (( COMMAND_SHADOW_ATTEMPTS > 1 )); then
  retry_pressure_now=1
fi
new_consecutive_retry_pressure=0
if (( retry_pressure_now == 1 )); then
  new_consecutive_retry_pressure=$((STATE_CONSECUTIVE_RETRY_PRESSURE + 1))
fi

readiness_runner_active=0
if [[ "$READINESS_RUNNER_STATUS" == "starting" || "$READINESS_RUNNER_STATUS" == "running" ]]; then
  if [[ "$READINESS_RUNNER_AGE_SECONDS" -ge 0 && "$READINESS_RUNNER_AGE_SECONDS" -le "$RECOVERY_READINESS_RUNNER_FRESH_SECONDS" ]]; then
    readiness_runner_active=1
  fi
fi

decision_reasons=()
action_records=()

recovery_env_persistence_status="unknown"
recovery_env_persistence_error_present=0
recovery_env_persistence_target_file=""
recovery_env_persistence_parse_error=0
recovery_env_persistence_strict_blocked=0
coldmath_stage_timeout_guardrails_status="unavailable"
coldmath_stage_timeout_guardrails_strict_blocked=0
coldmath_stage_timeout_guardrails_required_keys="none"
coldmath_stage_timeout_guardrails_invalid_keys="none"
coldmath_stage_timeout_guardrails_disabled_keys="none"
coldmath_stage_timeout_required_stages="none"
coldmath_stage_timeout_timeout_stages="none"
coldmath_stage_timeout_missing_stage_telemetry="none"
coldmath_stage_timeout_stage_telemetry_status="unavailable"
recovery_effectiveness_summary_available=0
recovery_effectiveness_summary_source="none"
recovery_effectiveness_summary_file=""
recovery_effectiveness_harmful_actions="none"
recovery_effectiveness_demoted_actions="none"
recovery_effectiveness_summary_text=""
recovery_effectiveness_file_age_seconds=-1
recovery_effectiveness_stale_threshold_seconds="$RECOVERY_EFFECTIVENESS_STALE_CRIT_SECONDS"
recovery_effectiveness_summary_stale=0
recovery_effectiveness_gap_detected=0
recovery_effectiveness_gap_reason="none"
if [[ -f "$COLDMATH_HARDENING_STATUS_FILE" ]]; then
  recovery_env_persistence_snapshot="$("$PYTHON_BIN" - "$COLDMATH_HARDENING_STATUS_FILE" "$COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR" "$COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED" "$COLDMATH_HARDENING_ENABLED" "$COLDMATH_MARKET_INGEST_ENABLED" "$COLDMATH_RECOVERY_ADVISOR_ENABLED" "$COLDMATH_RECOVERY_LOOP_ENABLED" "$COLDMATH_RECOVERY_CAMPAIGN_ENABLED" "$COLDMATH_STAGE_TIMEOUT_SECONDS" "$COLDMATH_SNAPSHOT_TIMEOUT_SECONDS" "$COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS" "$COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS" "$COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS" "$COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import re
import sys


def _normalize(value: object) -> str:
    return str(value or "").strip().lower()


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return _normalize(value) in {"1", "true", "yes", "on"}


path = Path(sys.argv[1])
strict_fail_on_error = _normalize(sys.argv[2]) in {"1", "true", "yes", "on"}
strict_timeout_required = _normalize(sys.argv[3]) in {"1", "true", "yes", "on"}
hardening_enabled = _normalize(sys.argv[4]) in {"1", "true", "yes", "on"}
market_ingest_enabled = _normalize(sys.argv[5]) in {"1", "true", "yes", "on"}
recovery_advisor_enabled = _normalize(sys.argv[6]) in {"1", "true", "yes", "on"}
recovery_loop_enabled = _normalize(sys.argv[7]) in {"1", "true", "yes", "on"}
recovery_campaign_enabled = _normalize(sys.argv[8]) in {"1", "true", "yes", "on"}
global_timeout_raw = str(sys.argv[9] or "").strip()
snapshot_timeout_raw = str(sys.argv[10] or "").strip()
market_ingest_timeout_raw = str(sys.argv[11] or "").strip()
recovery_advisor_timeout_raw = str(sys.argv[12] or "").strip()
recovery_loop_timeout_raw = str(sys.argv[13] or "").strip()
recovery_campaign_timeout_raw = str(sys.argv[14] or "").strip()
parse_error = False
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    parse_error = True
    payload = {}
if not isinstance(payload, dict):
    parse_error = True
    payload = {}

recovery_env_persistence = (
    payload.get("recovery_env_persistence")
    if isinstance(payload.get("recovery_env_persistence"), dict)
    else {}
)
status = _normalize(recovery_env_persistence.get("status")) or "unknown"
target = str(recovery_env_persistence.get("target_file") or "").strip().replace("\n", " ")
error = str(recovery_env_persistence.get("error") or "").strip()
error_present = bool(error)
strict_blocked = bool(
    strict_fail_on_error and status in {"error", "execution_failed"}
)

stage_specs: list[tuple[str, str, str, str]] = []
if hardening_enabled:
    stage_specs.append(
        ("snapshot", "coldmath_snapshot_summary", "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS", snapshot_timeout_raw)
    )
    if market_ingest_enabled:
        stage_specs.append(
            ("market_ingest", "polymarket_market_ingest", "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS", market_ingest_timeout_raw)
        )
    if recovery_advisor_enabled:
        stage_specs.append(
            ("recovery_advisor", "kalshi_temperature_recovery_advisor", "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS", recovery_advisor_timeout_raw)
        )
    if recovery_loop_enabled:
        stage_specs.append(
            ("recovery_loop", "kalshi_temperature_recovery_loop", "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS", recovery_loop_timeout_raw)
        )
    if recovery_campaign_enabled:
        stage_specs.append(
            ("recovery_campaign", "kalshi_temperature_recovery_campaign", "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS", recovery_campaign_timeout_raw)
        )

required_stages = [item[0] for item in stage_specs]
required_keys = [item[2] for item in stage_specs]
stage_status_map: dict[str, str] = {}
stage_telemetry_available = False
stages_payload = payload.get("stages")
if isinstance(stages_payload, list):
    expected_stage_names = {item[1] for item in stage_specs}
    for row in stages_payload:
        if not isinstance(row, dict):
            continue
        stage_name = _normalize(row.get("stage"))
        if not stage_name or stage_name not in expected_stage_names:
            continue
        stage_telemetry_available = True
        stage_status_map[stage_name] = _normalize(row.get("status")) or "unknown"

def _is_uint(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9]+", str(value or "").strip()))

invalid_keys: list[str] = []
disabled_keys: list[str] = []
if not stage_specs:
    guardrail_status = "not_required"
elif not stage_telemetry_available:
    guardrail_status = "unavailable"
else:
    for _, _, timeout_key, timeout_raw in stage_specs:
        effective_timeout = timeout_raw if timeout_raw else global_timeout_raw
        if not _is_uint(effective_timeout):
            invalid_keys.append(timeout_key)
            continue
        if int(effective_timeout) <= 0:
            disabled_keys.append(timeout_key)
    if invalid_keys:
        guardrail_status = "invalid"
    elif disabled_keys:
        guardrail_status = "disabled"
    else:
        guardrail_status = "ok"

timeout_stages: list[str] = []
missing_stage_telemetry: list[str] = []
if not stage_specs:
    stage_telemetry_status = "not_required"
elif not stage_telemetry_available:
    stage_telemetry_status = "unavailable"
else:
    for stage_label, stage_name, _, _ in stage_specs:
        stage_status = _normalize(stage_status_map.get(stage_name))
        if not stage_status:
            missing_stage_telemetry.append(stage_label)
            continue
        if stage_status == "timeout":
            timeout_stages.append(stage_label)
    if timeout_stages:
        stage_telemetry_status = "timeout"
    else:
        stage_telemetry_status = "ok"

timeout_guardrail_strict_blocked = bool(
    strict_timeout_required and guardrail_status in {"invalid", "disabled"}
)

print(f"STATUS={status}")
print(f"TARGET_FILE={target}")
print(f"ERROR_PRESENT={1 if error_present else 0}")
print(f"PARSE_ERROR={1 if parse_error else 0}")
print(f"STRICT_BLOCKED={1 if strict_blocked else 0}")
print(f"TIMEOUT_GUARDRAILS_STATUS={guardrail_status}")
print(f"TIMEOUT_GUARDRAILS_STRICT_BLOCKED={1 if timeout_guardrail_strict_blocked else 0}")
print("TIMEOUT_GUARDRAILS_REQUIRED_KEYS=" + (",".join(required_keys) if required_keys else "none"))
print("TIMEOUT_GUARDRAILS_INVALID_KEYS=" + (",".join(invalid_keys) if invalid_keys else "none"))
print("TIMEOUT_GUARDRAILS_DISABLED_KEYS=" + (",".join(disabled_keys) if disabled_keys else "none"))
print("TIMEOUT_GUARDRAILS_REQUIRED_STAGES=" + (",".join(required_stages) if required_stages else "none"))
print("TIMEOUT_GUARDRAILS_TIMEOUT_STAGES=" + (",".join(timeout_stages) if timeout_stages else "none"))
print("TIMEOUT_GUARDRAILS_MISSING_STAGE_TELEMETRY=" + (",".join(missing_stage_telemetry) if missing_stage_telemetry else "none"))
print(f"TIMEOUT_GUARDRAILS_STAGE_TELEMETRY_STATUS={stage_telemetry_status}")
PY
)"
  while IFS='=' read -r key value; do
    case "$key" in
      STATUS) recovery_env_persistence_status="$value" ;;
      TARGET_FILE) recovery_env_persistence_target_file="$value" ;;
      ERROR_PRESENT) recovery_env_persistence_error_present="$value" ;;
      PARSE_ERROR) recovery_env_persistence_parse_error="$value" ;;
      STRICT_BLOCKED) recovery_env_persistence_strict_blocked="$value" ;;
      TIMEOUT_GUARDRAILS_STATUS) coldmath_stage_timeout_guardrails_status="$value" ;;
      TIMEOUT_GUARDRAILS_STRICT_BLOCKED) coldmath_stage_timeout_guardrails_strict_blocked="$value" ;;
      TIMEOUT_GUARDRAILS_REQUIRED_KEYS) coldmath_stage_timeout_guardrails_required_keys="$value" ;;
      TIMEOUT_GUARDRAILS_INVALID_KEYS) coldmath_stage_timeout_guardrails_invalid_keys="$value" ;;
      TIMEOUT_GUARDRAILS_DISABLED_KEYS) coldmath_stage_timeout_guardrails_disabled_keys="$value" ;;
      TIMEOUT_GUARDRAILS_REQUIRED_STAGES) coldmath_stage_timeout_required_stages="$value" ;;
      TIMEOUT_GUARDRAILS_TIMEOUT_STAGES) coldmath_stage_timeout_timeout_stages="$value" ;;
      TIMEOUT_GUARDRAILS_MISSING_STAGE_TELEMETRY) coldmath_stage_timeout_missing_stage_telemetry="$value" ;;
      TIMEOUT_GUARDRAILS_STAGE_TELEMETRY_STATUS) coldmath_stage_timeout_stage_telemetry_status="$value" ;;
    esac
  done <<< "$recovery_env_persistence_snapshot"
fi

effectiveness_snapshot="$("$PYTHON_BIN" - "$OUTPUT_DIR" "$now_epoch" "$RECOVERY_EFFECTIVENESS_STALE_CRIT_SECONDS" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys


def normalize_text(value: object) -> str:
    return str(value or "").strip()


def as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def parse_int(value: object, default: int = 0) -> int:
    try:
        return int(float(str(value)))
    except Exception:
        return default


def parse_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value))
    except Exception:
        return default


def as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return normalize_text(value).lower() in {"1", "true", "yes", "y", "on"}


def as_action_list(value: object) -> list[str]:
    out: list[str] = []
    if not isinstance(value, list):
        return out
    for item in value:
        key = normalize_text(item)
        if not key:
            continue
        out.append(key)
    return out


def dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = normalize_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def humanize_action_key(value: str) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    return " ".join(text.replace("_", " ").replace("-", " ").replace(":", " ").split())


def load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def infer_harmful_actions_from_loop(payload: dict[str, object]) -> list[str]:
    thresholds = as_dict(payload.get("adaptive_effectiveness_thresholds"))
    min_executions = max(1, parse_int(thresholds.get("min_executions"), 3))
    min_worsening_ratio = max(0.0, min(1.0, parse_float(thresholds.get("min_worsening_ratio"), 0.8)))
    min_average_delta = parse_float(thresholds.get("min_average_negative_share_delta"), 0.0)
    action_effectiveness = payload.get("action_effectiveness")
    if not isinstance(action_effectiveness, dict):
        return []
    harmful: list[str] = []
    for raw_key, raw_row in sorted(action_effectiveness.items(), key=lambda item: normalize_text(item[0])):
        action_key = normalize_text(raw_key)
        row = as_dict(raw_row)
        if not action_key or not row:
            continue
        executed_count = max(0, parse_int(row.get("executed_count"), 0))
        worsening_count = max(0, parse_int(row.get("worsening_count"), 0))
        worsening_ratio = (float(worsening_count) / float(executed_count)) if executed_count > 0 else 0.0
        average_delta = parse_float(row.get("average_negative_share_delta"), 0.0)
        if (
            executed_count >= min_executions
            and worsening_ratio >= min_worsening_ratio
            and average_delta > min_average_delta
        ):
            harmful.append(action_key)
    return dedupe(harmful)


output_dir = Path(sys.argv[1])
now_epoch = parse_int(sys.argv[2], 0)
stale_threshold_seconds = max(0, parse_int(sys.argv[3], 21600))
health_dir = output_dir / "health"
advisor_latest = health_dir / "kalshi_temperature_recovery_advisor_latest.json"
loop_latest = health_dir / "kalshi_temperature_recovery_loop_latest.json"

summary_available = False
summary_source = "none"
summary_file = ""
harmful_actions: list[str] = []
demoted_actions: list[str] = []

advisor_payload = load_json(advisor_latest)
if advisor_payload:
    metrics = as_dict(advisor_payload.get("metrics"))
    recovery_effectiveness = as_dict(metrics.get("recovery_effectiveness"))
    remediation_plan = as_dict(advisor_payload.get("remediation_plan"))
    harmful_actions = dedupe(as_action_list(recovery_effectiveness.get("persistently_harmful_actions")))
    if not harmful_actions:
        scoreboard = recovery_effectiveness.get("scoreboard")
        if isinstance(scoreboard, dict):
            inferred_harmful: list[str] = []
            for raw_key, raw_row in sorted(scoreboard.items(), key=lambda item: normalize_text(item[0])):
                row = as_dict(raw_row)
                if row and as_bool(row.get("persistently_harmful")):
                    inferred_harmful.append(normalize_text(raw_key))
            harmful_actions = dedupe(inferred_harmful)
    demoted_actions = dedupe(as_action_list(remediation_plan.get("demoted_actions_for_effectiveness")))
    summary_available = bool(recovery_effectiveness) or bool(remediation_plan)
    if summary_available or harmful_actions or demoted_actions:
        summary_source = "advisor_latest"
        summary_file = str(advisor_latest)

loop_payload = load_json(loop_latest)
if loop_payload and (not harmful_actions and not demoted_actions):
    harmful_actions = infer_harmful_actions_from_loop(loop_payload)
    demoted_actions = dedupe(as_action_list(loop_payload.get("demoted_actions")))
    summary_available = summary_available or bool(loop_payload)
    if summary_available or harmful_actions or demoted_actions:
        summary_source = "loop_latest"
        summary_file = str(loop_latest)

if not summary_available and not harmful_actions and not demoted_actions:
    summary_source = "none"
    summary_file = ""

if summary_available:
    if harmful_actions or demoted_actions:
        parts = [
            f"effectiveness harmful routes {len(harmful_actions)}",
            f"demoted routes {len(demoted_actions)}",
        ]
        lead_route = ""
        if harmful_actions:
            lead_route = humanize_action_key(harmful_actions[0])
        elif demoted_actions:
            lead_route = humanize_action_key(demoted_actions[0])
        if lead_route:
            parts.append(f"top route {lead_route}")
        summary_text = ", ".join(parts)
    else:
        summary_text = "effectiveness shows no persistently harmful routes"
else:
    summary_text = ""

file_age_seconds = -1
if summary_file:
    summary_path = Path(summary_file)
    if summary_path.exists():
        try:
            file_age_seconds = max(0, int(now_epoch - int(summary_path.stat().st_mtime)))
        except Exception:
            file_age_seconds = -1

stale = bool(file_age_seconds >= 0 and file_age_seconds > stale_threshold_seconds)
gap_detected = False
gap_reason = "none"
if not summary_available:
    gap_detected = True
    gap_reason = "summary_missing"
elif file_age_seconds < 0:
    gap_detected = True
    gap_reason = "summary_missing"
elif stale:
    gap_detected = True
    gap_reason = "summary_stale"

summary_text = summary_text.replace("\n", " ").replace("|", "/").replace("=", "-").strip()

print(f"SUMMARY_AVAILABLE={1 if summary_available else 0}")
print(f"SUMMARY_SOURCE={summary_source}")
print(f"SUMMARY_FILE={summary_file}")
print("HARMFUL_ACTIONS=" + (",".join(harmful_actions) if harmful_actions else "none"))
print("DEMOTED_ACTIONS=" + (",".join(demoted_actions) if demoted_actions else "none"))
print(f"SUMMARY_TEXT={summary_text}")
print(f"SUMMARY_FILE_AGE_SECONDS={file_age_seconds}")
print(f"SUMMARY_STALE_THRESHOLD_SECONDS={stale_threshold_seconds}")
print(f"SUMMARY_STALE={1 if stale else 0}")
print(f"GAP_DETECTED={1 if gap_detected else 0}")
print(f"GAP_REASON={gap_reason}")
PY
)"
while IFS='=' read -r key value; do
  case "$key" in
    SUMMARY_AVAILABLE) recovery_effectiveness_summary_available="$value" ;;
    SUMMARY_SOURCE) recovery_effectiveness_summary_source="$value" ;;
    SUMMARY_FILE) recovery_effectiveness_summary_file="$value" ;;
    HARMFUL_ACTIONS) recovery_effectiveness_harmful_actions="$value" ;;
    DEMOTED_ACTIONS) recovery_effectiveness_demoted_actions="$value" ;;
    SUMMARY_TEXT) recovery_effectiveness_summary_text="$value" ;;
    SUMMARY_FILE_AGE_SECONDS) recovery_effectiveness_file_age_seconds="$value" ;;
    SUMMARY_STALE_THRESHOLD_SECONDS) recovery_effectiveness_stale_threshold_seconds="$value" ;;
    SUMMARY_STALE) recovery_effectiveness_summary_stale="$value" ;;
    GAP_DETECTED) recovery_effectiveness_gap_detected="$value" ;;
    GAP_REASON) recovery_effectiveness_gap_reason="$value" ;;
  esac
done <<< "$effectiveness_snapshot"
if [[ ! "$recovery_effectiveness_summary_available" =~ ^[01]$ ]]; then
  recovery_effectiveness_summary_available=0
fi
if [[ ! "$recovery_effectiveness_file_age_seconds" =~ ^-?[0-9]+$ ]]; then
  recovery_effectiveness_file_age_seconds=-1
fi
if [[ ! "$recovery_effectiveness_stale_threshold_seconds" =~ ^[0-9]+$ ]]; then
  recovery_effectiveness_stale_threshold_seconds="$RECOVERY_EFFECTIVENESS_STALE_CRIT_SECONDS"
fi
if [[ ! "$recovery_effectiveness_summary_stale" =~ ^[01]$ ]]; then
  recovery_effectiveness_summary_stale=0
fi
if [[ ! "$recovery_effectiveness_gap_detected" =~ ^[01]$ ]]; then
  recovery_effectiveness_gap_detected=0
fi
case "$recovery_effectiveness_gap_reason" in
  none|summary_missing|summary_stale)
    ;;
  *)
    recovery_effectiveness_gap_reason="none"
    ;;
esac

if [[ "$recovery_env_persistence_strict_blocked" == "1" ]]; then
  issue_detected=1
  need_repair_recovery_env_persistence_gate=1
  decision_reasons+=("recovery_env_persistence_error")
fi
if [[ "$coldmath_stage_timeout_guardrails_strict_blocked" == "1" ]]; then
  issue_detected=1
  if [[ "$coldmath_stage_timeout_guardrails_invalid_keys" != "none" ]]; then
    decision_reasons+=("coldmath_stage_timeout_guardrails_invalid")
    need_repair_coldmath_stage_timeout_guardrails=1
  fi
  if [[ "$coldmath_stage_timeout_guardrails_disabled_keys" != "none" ]]; then
    decision_reasons+=("coldmath_stage_timeout_guardrails_disabled")
    need_repair_coldmath_stage_timeout_guardrails=1
  fi
fi
if [[ "$coldmath_stage_timeout_timeout_stages" != "none" ]]; then
  issue_detected=1
  decision_reasons+=("coldmath_stage_timeout_stage_timeouts")
  need_repair_coldmath_stage_timeout_guardrails=1
fi
if [[ "$RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY" == "1" && "$recovery_effectiveness_gap_detected" == "1" ]]; then
  issue_detected=1
  need_trigger_coldmath_hardening_on_effectiveness_gap=1
  if [[ "$recovery_effectiveness_gap_reason" == "summary_stale" ]]; then
    decision_reasons+=("recovery_effectiveness_summary_stale")
  else
    decision_reasons+=("recovery_effectiveness_summary_missing")
  fi
fi

if [[ "$shadow_service_state" != "active" ]]; then
  issue_detected=1
  need_restart_shadow=1
  decision_reasons+=("shadow_service_${shadow_service_reason_state}")
fi

if [[ "$reporting_timer_state" != "active" ]]; then
  issue_detected=1
  need_reporting_timer_enable=1
  decision_reasons+=("reporting_timer_${reporting_timer_reason_state}")
fi

if [[ "$RECOVERY_REQUIRE_ALPHA_SUMMARY_TIMER" == "1" ]] && [[ "$alpha_summary_timer_state" != "active" ]]; then
  issue_detected=1
  need_alpha_summary_timer_enable=1
  decision_reasons+=("alpha_summary_timer_${alpha_summary_timer_reason_state}")
fi

if [[ "$RECOVERY_REQUIRE_ALPHA_WORKER" == "1" ]] && [[ "$alpha_worker_service_state" != "active" ]]; then
  issue_detected=1
  need_restart_alpha_worker=1
  decision_reasons+=("alpha_worker_service_${alpha_worker_service_reason_state}")
fi

if [[ "$RECOVERY_REQUIRE_BREADTH_WORKER" == "1" ]] && [[ "$breadth_worker_service_state" != "active" ]]; then
  issue_detected=1
  need_restart_breadth_worker=1
  decision_reasons+=("breadth_worker_service_${breadth_worker_service_reason_state}")
fi

if [[ "$RECOVERY_REQUIRE_LOG_MAINTENANCE_TIMER" == "1" ]] && [[ "$log_maintenance_timer_state" != "active" ]]; then
  issue_detected=1
  need_log_maintenance_timer_enable=1
  decision_reasons+=("log_maintenance_timer_${log_maintenance_timer_reason_state}")
fi

if [[ "$HEALTH_AGE_SECONDS" -lt 0 || "$HEALTH_AGE_SECONDS" -gt "$RECOVERY_HEALTH_STALE_CRIT_SECONDS" ]]; then
  issue_detected=1
  need_restart_shadow=1
  decision_reasons+=("live_status_stale_or_missing")
fi

if [[ "$READINESS_AGE_SECONDS" -lt 0 || "$READINESS_AGE_SECONDS" -gt "$RECOVERY_READINESS_STALE_CRIT_SECONDS" ]]; then
  if (( readiness_runner_active == 1 )); then
    decision_reasons+=("readiness_stale_but_runner_active")
  else
    issue_detected=1
    need_reporting_trigger=1
    decision_reasons+=("readiness_stale_or_missing")
  fi
fi

if [[ "$ALPHA_SUMMARY_AGE_SECONDS" -lt 0 || "$ALPHA_SUMMARY_AGE_SECONDS" -gt "$RECOVERY_ALPHA_SUMMARY_STALE_CRIT_SECONDS" ]]; then
  if (( alpha_summary_trigger_grace_active == 1 )); then
    decision_reasons+=("alpha_summary_stale_trigger_grace")
  else
    issue_detected=1
    need_alpha_summary_trigger=1
    decision_reasons+=("alpha_summary_stale_or_missing")
  fi
fi

if [[ "$LOG_MAINTENANCE_AGE_SECONDS" -lt 0 || "$LOG_MAINTENANCE_AGE_SECONDS" -gt "$RECOVERY_LOG_MAINTENANCE_STALE_CRIT_SECONDS" ]]; then
  issue_detected=1
  need_trigger_log_maintenance=1
  decision_reasons+=("log_maintenance_stale_or_missing")
fi

if [[ "$LOG_MAINTENANCE_HEALTH_STATUS" == "red" ]]; then
  issue_detected=1
  need_trigger_log_maintenance=1
  decision_reasons+=("log_maintenance_health_red")
elif [[ "$LOG_MAINTENANCE_HEALTH_STATUS" == "yellow" && "$RECOVERY_LOG_MAINTENANCE_TRIGGER_ON_YELLOW" == "1" ]]; then
  issue_detected=1
  need_trigger_log_maintenance=1
  decision_reasons+=("log_maintenance_health_yellow")
fi

if [[ "$reporting_service_activating_age_seconds" -ge "$RECOVERY_REPORTING_ACTIVATING_CRIT_SECONDS" ]]; then
  restart_reporting_for_activating=1
  if (( readiness_runner_active == 1 )); then
    restart_reporting_for_activating=0
    decision_reasons+=("reporting_service_activating_long_runner_active")
  elif [[ "$RECOVERY_REPORTING_ACTIVATING_RESTART_REQUIRE_STALE" == "1" ]]; then
    if [[ "$READINESS_AGE_SECONDS" -ge 0 && "$READINESS_AGE_SECONDS" -le "$RECOVERY_READINESS_STALE_CRIT_SECONDS" && "$PIPELINE_STATUS" != "red" ]]; then
      restart_reporting_for_activating=0
      decision_reasons+=("reporting_service_activating_long_deferred")
    fi
  fi
  if [[ "$restart_reporting_for_activating" == "1" ]]; then
    issue_detected=1
    need_restart_reporting=1
    decision_reasons+=("reporting_service_activating_too_long")
  fi
fi

if [[ "$reporting_service_state" == "failed" ]]; then
  issue_detected=1
  need_restart_reporting=1
  need_reset_failed_reporting=1
  decision_reasons+=("reporting_service_failed")
fi

if [[ "$HEALTH_STATUS" == "red" ]]; then
  case ",$HEALTH_RED_REASONS," in
    *",missing_metar_summary,"*|*",metar_summary_stale_critical,"*)
      need_refresh_metar=1
      decision_reasons+=("metar_stale_or_missing")
      ;;
  esac
  case ",$HEALTH_RED_REASONS," in
    *",settlement_summary_stale_critical,"*|*",missing_settlement_summary,"*)
      need_refresh_settlement=1
      decision_reasons+=("settlement_stale_or_missing")
      ;;
  esac
  case ",$HEALTH_RED_REASONS," in
    *",shadow_cycle_failed,"*|*",missing_shadow_summary,"*|*",shadow_summary_stale_critical,"*)
      need_restart_shadow=1
      decision_reasons+=("shadow_failed_or_stale")
      ;;
  esac
fi

if [[ "$new_consecutive_red" -ge "$RECOVERY_RED_CONSECUTIVE_THRESHOLD" ]]; then
  need_restart_shadow=1
  decision_reasons+=("consecutive_red_threshold")
fi

if [[ "$PIPELINE_STATUS" == "red" ]]; then
  need_reporting_trigger=1
  decision_reasons+=("pipeline_red")
fi

if [[ "$new_consecutive_pipeline_red" -ge "$RECOVERY_PIPELINE_RED_CONSECUTIVE_THRESHOLD" ]]; then
  need_reporting_trigger=1
  decision_reasons+=("consecutive_pipeline_red_threshold")
fi

if (( freshness_pressure_now == 1 )); then
  decision_reasons+=("freshness_pressure_detected")
fi
if [[ "$new_consecutive_freshness_pressure" -ge "$RECOVERY_FRESHNESS_PRESSURE_CONSECUTIVE_THRESHOLD" ]]; then
  issue_detected=1
  need_refresh_metar=1
  need_reporting_trigger=1
  decision_reasons+=("freshness_pressure_persistent")
  if (( SCAN_EFFECTIVE_MAX_MARKETS > RECOVERY_SCAN_BUDGET_TRIM_MIN )); then
    need_trim_scan_budget=1
    decision_reasons+=("scan_budget_trim_candidate")
  fi
fi
if [[ "$new_consecutive_freshness_pressure" -ge $(( RECOVERY_FRESHNESS_PRESSURE_CONSECUTIVE_THRESHOLD + 2 )) ]]; then
  issue_detected=1
  need_restart_shadow=1
  decision_reasons+=("freshness_pressure_persistent_high")
fi

if (( retry_pressure_now == 1 )); then
  decision_reasons+=("retry_pressure_detected")
fi
if [[ "$new_consecutive_retry_pressure" -ge "$RECOVERY_RETRY_PRESSURE_CONSECUTIVE_THRESHOLD" ]]; then
  issue_detected=1
  need_restart_shadow=1
  need_refresh_metar=1
  decision_reasons+=("retry_pressure_persistent")
fi

if [[ "$need_reporting_timer_enable" == "1" ]]; then
  if service_exists betbot-temperature-reporting.timer; then
    if systemctl enable --now betbot-temperature-reporting.timer >/dev/null 2>&1; then
      action_records+=("enable_reporting_timer:ok")
      reporting_timer_state="$(service_state betbot-temperature-reporting.timer)"
    else
      action_records+=("enable_reporting_timer:failed")
    fi
  else
    action_records+=("enable_reporting_timer:missing_unit")
  fi
fi

if [[ "$need_alpha_summary_timer_enable" == "1" ]]; then
  if service_exists betbot-temperature-alpha-summary.timer; then
    if systemctl enable --now betbot-temperature-alpha-summary.timer >/dev/null 2>&1; then
      action_records+=("enable_alpha_summary_timer:ok")
      alpha_summary_timer_state="$(service_state betbot-temperature-alpha-summary.timer)"
    else
      action_records+=("enable_alpha_summary_timer:failed")
    fi
  else
    action_records+=("enable_alpha_summary_timer:missing_unit")
  fi
fi

if [[ "$need_log_maintenance_timer_enable" == "1" ]]; then
  force_log_maintenance_timer_enable=0
  if [[ "$log_maintenance_timer_enabled_state" != "enabled" || "$log_maintenance_timer_state" == "failed" ]]; then
    force_log_maintenance_timer_enable=1
  fi
  if [[ "$RECOVERY_ENABLE_LOG_MAINTENANCE_TIMER_ENABLE" != "1" ]]; then
    action_records+=("enable_log_maintenance_timer:disabled")
  elif (( force_log_maintenance_timer_enable == 0 )) && (( now_epoch - STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH < RECOVERY_LOG_MAINTENANCE_TIMER_ENABLE_COOLDOWN_SECONDS )); then
    action_records+=("enable_log_maintenance_timer:cooldown")
  elif service_exists "$RECOVERY_LOG_MAINTENANCE_TIMER_NAME"; then
    if systemctl enable --now "$RECOVERY_LOG_MAINTENANCE_TIMER_NAME" >/dev/null 2>&1; then
      action_records+=("enable_log_maintenance_timer:ok")
      STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH="$now_epoch"
      log_maintenance_timer_state="$(service_state "$RECOVERY_LOG_MAINTENANCE_TIMER_NAME")"
      log_maintenance_timer_enabled_state="$(service_enabled_state "$RECOVERY_LOG_MAINTENANCE_TIMER_NAME")"
      log_maintenance_service_state="$(service_state "$RECOVERY_LOG_MAINTENANCE_SERVICE_NAME")"
    else
      action_records+=("enable_log_maintenance_timer:failed")
    fi
  else
    action_records+=("enable_log_maintenance_timer:missing_unit")
  fi
fi

repair_env_persistence_gate_succeeded=0
if [[ "$need_repair_recovery_env_persistence_gate" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_ENV_PERSISTENCE_REPAIR" != "1" ]]; then
    action_records+=("repair_recovery_env_persistence_gate:disabled")
  elif (( now_epoch - STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH < RECOVERY_ENV_PERSISTENCE_REPAIR_COOLDOWN_SECONDS )); then
    action_records+=("repair_recovery_env_persistence_gate:cooldown")
  elif [[ ! -f "$RECOVERY_ENV_PERSISTENCE_REPAIR_SCRIPT" ]]; then
    action_records+=("repair_recovery_env_persistence_gate:missing_script")
  elif /bin/bash "$RECOVERY_ENV_PERSISTENCE_REPAIR_SCRIPT" --enable "$ENV_FILE" >> "$RECOVERY_LOG_FILE" 2>&1; then
    action_records+=("repair_recovery_env_persistence_gate:ok")
    STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH="$now_epoch"
    repair_env_persistence_gate_succeeded=1
  else
    action_records+=("repair_recovery_env_persistence_gate:failed")
  fi
fi

repair_coldmath_stage_timeout_guardrails_succeeded=0
if [[ "$need_repair_coldmath_stage_timeout_guardrails" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR" != "1" ]]; then
    action_records+=("repair_coldmath_stage_timeout_guardrails:disabled")
  elif (( now_epoch - STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH < RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_COOLDOWN_SECONDS )); then
    action_records+=("repair_coldmath_stage_timeout_guardrails:cooldown")
  elif [[ ! -f "$RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_SCRIPT" ]]; then
    action_records+=("repair_coldmath_stage_timeout_guardrails:missing_script")
  elif /bin/bash "$RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_SCRIPT" --global-seconds "$RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_GLOBAL_SECONDS" "$ENV_FILE" >> "$RECOVERY_LOG_FILE" 2>&1; then
    action_records+=("repair_coldmath_stage_timeout_guardrails:ok")
    STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH="$now_epoch"
    repair_coldmath_stage_timeout_guardrails_succeeded=1
  else
    action_records+=("repair_coldmath_stage_timeout_guardrails:failed")
  fi
fi

if (( repair_env_persistence_gate_succeeded == 1 )); then
  if [[ "$RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_ENV_REPAIR" != "1" ]]; then
    action_records+=("trigger_coldmath_hardening_after_env_repair:disabled")
  elif (( now_epoch - STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH < RECOVERY_COLDMATH_HARDENING_TRIGGER_COOLDOWN_SECONDS )); then
    action_records+=("trigger_coldmath_hardening_after_env_repair:cooldown")
  elif ! service_exists "$RECOVERY_COLDMATH_HARDENING_SERVICE_NAME"; then
    action_records+=("trigger_coldmath_hardening_after_env_repair:missing_unit")
  elif systemctl --no-block start "$RECOVERY_COLDMATH_HARDENING_SERVICE_NAME" >/dev/null 2>&1; then
    action_records+=("trigger_coldmath_hardening_after_env_repair:ok")
    STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH="$now_epoch"
  else
    action_records+=("trigger_coldmath_hardening_after_env_repair:failed")
  fi
fi

if (( repair_coldmath_stage_timeout_guardrails_succeeded == 1 )); then
  # Reuse the shared hardening trigger cooldown so env and timeout repairs do not fan out twice in one loop.
  if [[ "$RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_STAGE_TIMEOUT_REPAIR" != "1" ]]; then
    action_records+=("trigger_coldmath_hardening_after_stage_timeout_repair:disabled")
  elif (( now_epoch - STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH < RECOVERY_COLDMATH_HARDENING_TRIGGER_COOLDOWN_SECONDS )); then
    action_records+=("trigger_coldmath_hardening_after_stage_timeout_repair:cooldown")
  elif ! service_exists "$RECOVERY_COLDMATH_HARDENING_SERVICE_NAME"; then
    action_records+=("trigger_coldmath_hardening_after_stage_timeout_repair:missing_unit")
  elif systemctl --no-block start "$RECOVERY_COLDMATH_HARDENING_SERVICE_NAME" >/dev/null 2>&1; then
    action_records+=("trigger_coldmath_hardening_after_stage_timeout_repair:ok")
    STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH="$now_epoch"
  else
    action_records+=("trigger_coldmath_hardening_after_stage_timeout_repair:failed")
  fi
fi

if [[ "$need_trigger_coldmath_hardening_on_effectiveness_gap" == "1" ]]; then
  # Reuse the shared hardening trigger cooldown so strict-gap remediation paths do not fan out twice in one loop.
  if [[ "$RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_EFFECTIVENESS_GAP" != "1" ]]; then
    action_records+=("trigger_coldmath_hardening_on_effectiveness_gap:disabled")
  elif (( now_epoch - STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH < RECOVERY_COLDMATH_HARDENING_TRIGGER_COOLDOWN_SECONDS )); then
    action_records+=("trigger_coldmath_hardening_on_effectiveness_gap:cooldown")
  elif ! service_exists "$RECOVERY_COLDMATH_HARDENING_SERVICE_NAME"; then
    action_records+=("trigger_coldmath_hardening_on_effectiveness_gap:missing_unit")
  elif systemctl --no-block start "$RECOVERY_COLDMATH_HARDENING_SERVICE_NAME" >/dev/null 2>&1; then
    action_records+=("trigger_coldmath_hardening_on_effectiveness_gap:ok")
    STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH="$now_epoch"
  else
    action_records+=("trigger_coldmath_hardening_on_effectiveness_gap:failed")
  fi
fi

if [[ "$need_restart_shadow" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_SERVICE_RESTARTS" != "1" ]]; then
    action_records+=("restart_shadow:disabled")
  elif (( now_epoch - STATE_LAST_SHADOW_RESTART_EPOCH < RECOVERY_SHADOW_RESTART_COOLDOWN_SECONDS )); then
    action_records+=("restart_shadow:cooldown")
  elif service_exists betbot-temperature-shadow.service; then
    if systemctl restart betbot-temperature-shadow.service >/dev/null 2>&1; then
      action_records+=("restart_shadow:ok")
      STATE_LAST_SHADOW_RESTART_EPOCH="$now_epoch"
      shadow_service_state="$(service_state betbot-temperature-shadow.service)"
    else
      action_records+=("restart_shadow:failed")
    fi
  else
    action_records+=("restart_shadow:missing_unit")
  fi
fi

if [[ "$need_restart_reporting" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_SERVICE_RESTARTS" != "1" ]]; then
    action_records+=("restart_reporting:disabled")
  elif [[ "$need_reset_failed_reporting" != "1" ]] && (( now_epoch - STATE_LAST_REPORTING_RESTART_EPOCH < RECOVERY_REPORTING_RESTART_COOLDOWN_SECONDS )); then
    action_records+=("restart_reporting:cooldown")
  elif service_exists betbot-temperature-reporting.service; then
    if [[ "$need_reset_failed_reporting" == "1" ]]; then
      if systemctl reset-failed betbot-temperature-reporting.service >/dev/null 2>&1; then
        action_records+=("reset_reporting_failed_state:ok")
      else
        action_records+=("reset_reporting_failed_state:failed")
      fi
    fi
    # Keep watchdog latency low; reporting is a long-running oneshot.
    if systemctl --no-block restart betbot-temperature-reporting.service >/dev/null 2>&1; then
      action_records+=("restart_reporting:ok")
      STATE_LAST_REPORTING_RESTART_EPOCH="$now_epoch"
      reporting_service_state="$(service_state betbot-temperature-reporting.service)"
      reporting_service_activating_age_seconds="$(reporting_activating_age_seconds)"
      if [[ ! "$reporting_service_activating_age_seconds" =~ ^-?[0-9]+$ ]]; then
        reporting_service_activating_age_seconds=-1
      fi
    else
      action_records+=("restart_reporting:failed")
    fi
  else
    action_records+=("restart_reporting:missing_unit")
  fi
fi

if [[ "$need_restart_alpha_worker" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_ALPHA_WORKER_RESTARTS" != "1" ]]; then
    action_records+=("restart_alpha_worker:disabled")
  elif (( now_epoch - STATE_LAST_ALPHA_WORKER_RESTART_EPOCH < RECOVERY_ALPHA_WORKER_RESTART_COOLDOWN_SECONDS )); then
    action_records+=("restart_alpha_worker:cooldown")
  elif service_exists betbot-temperature-alpha-workers.service; then
    if systemctl restart betbot-temperature-alpha-workers.service >/dev/null 2>&1; then
      action_records+=("restart_alpha_worker:ok")
      STATE_LAST_ALPHA_WORKER_RESTART_EPOCH="$now_epoch"
      alpha_worker_service_state="$(service_state betbot-temperature-alpha-workers.service)"
    else
      action_records+=("restart_alpha_worker:failed")
    fi
  else
    action_records+=("restart_alpha_worker:missing_unit")
  fi
fi

if [[ "$need_restart_breadth_worker" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_BREADTH_WORKER_RESTARTS" != "1" ]]; then
    action_records+=("restart_breadth_worker:disabled")
  elif (( now_epoch - STATE_LAST_BREADTH_WORKER_RESTART_EPOCH < RECOVERY_BREADTH_WORKER_RESTART_COOLDOWN_SECONDS )); then
    action_records+=("restart_breadth_worker:cooldown")
  elif service_exists betbot-temperature-breadth-worker.service; then
    if systemctl restart betbot-temperature-breadth-worker.service >/dev/null 2>&1; then
      action_records+=("restart_breadth_worker:ok")
      STATE_LAST_BREADTH_WORKER_RESTART_EPOCH="$now_epoch"
      breadth_worker_service_state="$(service_state betbot-temperature-breadth-worker.service)"
    else
      action_records+=("restart_breadth_worker:failed")
    fi
  else
    action_records+=("restart_breadth_worker:missing_unit")
  fi
fi

if [[ "$need_refresh_metar" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_METAR_REFRESH" != "1" ]]; then
    action_records+=("refresh_metar:disabled")
  elif (( now_epoch - STATE_LAST_METAR_REFRESH_EPOCH < RECOVERY_METAR_REFRESH_COOLDOWN_SECONDS )); then
    action_records+=("refresh_metar:cooldown")
  else
    if run_as_user "$RECOVERY_RUN_AS_USER" \
      "$PYTHON_BIN" -m betbot.cli kalshi-temperature-metar-ingest \
      --output-dir "$OUTPUT_DIR" \
      --timeout-seconds 25 \
      "${EXTRA_METAR_ARGS_ARRAY[@]}" >> "$RECOVERY_LOG_FILE" 2>&1; then
      action_records+=("refresh_metar:ok")
      STATE_LAST_METAR_REFRESH_EPOCH="$now_epoch"
    else
      action_records+=("refresh_metar:failed")
    fi
  fi
fi

if [[ "$need_refresh_settlement" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_SETTLEMENT_REFRESH" != "1" ]]; then
    action_records+=("refresh_settlement:disabled")
  elif (( now_epoch - STATE_LAST_SETTLEMENT_REFRESH_EPOCH < RECOVERY_SETTLEMENT_REFRESH_COOLDOWN_SECONDS )); then
    action_records+=("refresh_settlement:cooldown")
  else
    if run_as_user "$RECOVERY_RUN_AS_USER" \
      "$PYTHON_BIN" -m betbot.cli kalshi-temperature-settlement-state \
      --output-dir "$OUTPUT_DIR" \
      --top-n "$SETTLEMENT_TOP_N" \
      --final-report-cache-ttl-minutes "$FINAL_REPORT_CACHE_TTL_MINUTES" \
      --final-report-timeout-seconds "$FINAL_REPORT_TIMEOUT_SECONDS" \
      "${EXTRA_SETTLEMENT_ARGS_ARRAY[@]}" >> "$RECOVERY_LOG_FILE" 2>&1; then
      action_records+=("refresh_settlement:ok")
      STATE_LAST_SETTLEMENT_REFRESH_EPOCH="$now_epoch"
    else
      action_records+=("refresh_settlement:failed")
    fi
  fi
fi

if [[ "$need_trigger_log_maintenance" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER" != "1" ]]; then
    action_records+=("trigger_log_maintenance:disabled")
  elif (( now_epoch - STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH < RECOVERY_LOG_MAINTENANCE_TRIGGER_COOLDOWN_SECONDS )); then
    action_records+=("trigger_log_maintenance:cooldown")
  elif service_exists "$RECOVERY_LOG_MAINTENANCE_SERVICE_NAME"; then
    if systemctl --no-block start "$RECOVERY_LOG_MAINTENANCE_SERVICE_NAME" >/dev/null 2>&1; then
      action_records+=("trigger_log_maintenance:ok")
      STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH="$now_epoch"
      log_maintenance_service_state="$(service_state "$RECOVERY_LOG_MAINTENANCE_SERVICE_NAME")"
    else
      action_records+=("trigger_log_maintenance:failed")
    fi
  else
    action_records+=("trigger_log_maintenance:missing_unit")
  fi
fi

if [[ "$need_trim_scan_budget" == "1" ]]; then
  base_max_markets="$SCAN_EFFECTIVE_MAX_MARKETS"
  if [[ ! "$base_max_markets" =~ ^[0-9]+$ ]] || (( base_max_markets <= 0 )); then
    base_max_markets="$SCAN_NEXT_MAX_MARKETS"
  fi
  if [[ ! "$base_max_markets" =~ ^[0-9]+$ ]] || (( base_max_markets <= 0 )); then
    base_max_markets=0
  fi

  if [[ "$RECOVERY_ENABLE_SCAN_BUDGET_TRIM" != "1" ]]; then
    action_records+=("trim_scan_budget:disabled")
  elif (( base_max_markets <= RECOVERY_SCAN_BUDGET_TRIM_MIN )); then
    action_records+=("trim_scan_budget:below_min")
  elif (( now_epoch - STATE_LAST_SCAN_BUDGET_TRIM_EPOCH < RECOVERY_SCAN_BUDGET_TRIM_COOLDOWN_SECONDS )); then
    action_records+=("trim_scan_budget:cooldown")
  else
    trim_target="$("$PYTHON_BIN" - <<PY
import math
base = max(0, int("${base_max_markets}"))
trim_factor = float("${RECOVERY_SCAN_BUDGET_TRIM_FACTOR}")
minimum = max(0, int("${RECOVERY_SCAN_BUDGET_TRIM_MIN}"))
if trim_factor <= 0:
    trim_factor = 0.85
candidate = int(math.floor(base * trim_factor))
if candidate < minimum:
    candidate = minimum
print(candidate)
PY
)"
    if [[ ! "$trim_target" =~ ^[0-9]+$ ]] || (( trim_target >= base_max_markets )); then
      action_records+=("trim_scan_budget:no_change")
    else
      if run_as_user "$RECOVERY_RUN_AS_USER" \
        "$PYTHON_BIN" - "$ADAPTIVE_MAX_MARKETS_STATE_FILE" "$trim_target" >> "$RECOVERY_LOG_FILE" 2>&1 <<'PY'
from __future__ import annotations

from pathlib import Path
import sys

path = Path(sys.argv[1])
value = int(float(sys.argv[2]))
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(str(value), encoding="utf-8")
PY
      then
        action_records+=("trim_scan_budget:ok:${base_max_markets}->${trim_target}")
        STATE_LAST_SCAN_BUDGET_TRIM_EPOCH="$now_epoch"
      else
        action_records+=("trim_scan_budget:failed")
      fi
    fi
  fi
fi

if [[ "$need_reporting_trigger" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_REPORTING_TRIGGER" != "1" ]]; then
    action_records+=("trigger_reporting:disabled")
  elif (( now_epoch - STATE_LAST_REPORTING_TRIGGER_EPOCH < RECOVERY_REPORTING_TRIGGER_COOLDOWN_SECONDS )); then
    action_records+=("trigger_reporting:cooldown")
  elif service_exists betbot-temperature-reporting.service; then
    if systemctl --no-block start betbot-temperature-reporting.service >/dev/null 2>&1; then
      action_records+=("trigger_reporting:ok")
      STATE_LAST_REPORTING_TRIGGER_EPOCH="$now_epoch"
      reporting_service_state="$(service_state betbot-temperature-reporting.service)"
    else
      action_records+=("trigger_reporting:failed")
    fi
  else
    action_records+=("trigger_reporting:missing_unit")
  fi
fi

if [[ "$need_alpha_summary_trigger" == "1" ]]; then
  if [[ "$RECOVERY_ENABLE_ALPHA_SUMMARY_TRIGGER" != "1" ]]; then
    action_records+=("trigger_alpha_summary:disabled")
  elif (( now_epoch - STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH < RECOVERY_ALPHA_SUMMARY_TRIGGER_COOLDOWN_SECONDS )); then
    action_records+=("trigger_alpha_summary:cooldown")
  elif service_exists betbot-temperature-alpha-summary.service; then
    if systemctl --no-block start betbot-temperature-alpha-summary.service >/dev/null 2>&1; then
      action_records+=("trigger_alpha_summary:ok")
      STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH="$now_epoch"
      alpha_summary_trigger_age_seconds=0
      alpha_summary_trigger_grace_active=1
    else
      action_records+=("trigger_alpha_summary:failed")
    fi
  else
    action_records+=("trigger_alpha_summary:missing_unit")
  fi
fi

issue_remaining=0
if [[ "$shadow_service_state" != "active" ]]; then
  issue_remaining=1
fi
if [[ "$reporting_timer_state" != "active" ]]; then
  issue_remaining=1
fi
if [[ "$RECOVERY_REQUIRE_ALPHA_SUMMARY_TIMER" == "1" && "$alpha_summary_timer_state" != "active" ]]; then
  issue_remaining=1
fi
if [[ "$RECOVERY_REQUIRE_ALPHA_WORKER" == "1" && "$alpha_worker_service_state" != "active" ]]; then
  issue_remaining=1
fi
if [[ "$RECOVERY_REQUIRE_BREADTH_WORKER" == "1" && "$breadth_worker_service_state" != "active" ]]; then
  issue_remaining=1
fi
if [[ "$RECOVERY_REQUIRE_LOG_MAINTENANCE_TIMER" == "1" && "$log_maintenance_timer_state" != "active" ]]; then
  issue_remaining=1
fi
if [[ "$HEALTH_STATUS" == "red" || "$PIPELINE_STATUS" == "red" ]]; then
  issue_remaining=1
fi
if [[ "$HEALTH_AGE_SECONDS" -lt 0 || "$HEALTH_AGE_SECONDS" -gt "$RECOVERY_HEALTH_STALE_CRIT_SECONDS" ]]; then
  issue_remaining=1
fi
if [[ "$READINESS_AGE_SECONDS" -lt 0 || "$READINESS_AGE_SECONDS" -gt "$RECOVERY_READINESS_STALE_CRIT_SECONDS" ]]; then
  if (( readiness_runner_active != 1 )); then
    issue_remaining=1
  fi
fi
if [[ "$ALPHA_SUMMARY_AGE_SECONDS" -lt 0 || "$ALPHA_SUMMARY_AGE_SECONDS" -gt "$RECOVERY_ALPHA_SUMMARY_STALE_CRIT_SECONDS" ]]; then
  if (( alpha_summary_trigger_grace_active != 1 )); then
    issue_remaining=1
  fi
fi
if [[ "$LOG_MAINTENANCE_AGE_SECONDS" -lt 0 || "$LOG_MAINTENANCE_AGE_SECONDS" -gt "$RECOVERY_LOG_MAINTENANCE_STALE_CRIT_SECONDS" ]]; then
  issue_remaining=1
fi
if [[ "$recovery_env_persistence_strict_blocked" == "1" ]]; then
  issue_remaining=1
fi
if [[ "$coldmath_stage_timeout_guardrails_strict_blocked" == "1" ]]; then
  issue_remaining=1
fi
if [[ "$coldmath_stage_timeout_timeout_stages" != "none" ]]; then
  issue_remaining=1
fi
if [[ "$RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY" == "1" && "$recovery_effectiveness_gap_detected" == "1" ]]; then
  issue_remaining=1
fi
for action in "${action_records[@]}"; do
  case "$action" in
    *:failed|*:missing_unit|*:missing_script|*:disabled)
      issue_remaining=1
      ;;
  esac
done

resolved_in_run=0
if [[ "$issue_detected" == "1" && "$issue_remaining" == "0" ]]; then
  resolved_in_run=1
fi

if [[ "$HEALTH_STATUS" != "red" ]]; then
  new_consecutive_red=0
fi
if [[ "$PIPELINE_STATUS" != "red" ]]; then
  new_consecutive_pipeline_red=0
fi
if (( freshness_pressure_now != 1 )); then
  new_consecutive_freshness_pressure=0
fi
if (( retry_pressure_now != 1 )); then
  new_consecutive_retry_pressure=0
fi

decision_csv=""
if (( ${#decision_reasons[@]} > 0 )); then
  decision_csv="$(printf '%s\n' "${decision_reasons[@]}" | awk 'NF' | sort -u | paste -sd',' -)"
fi
actions_csv=""
if (( ${#action_records[@]} > 0 )); then
  actions_csv="$(printf '%s\n' "${action_records[@]}" | awk 'NF' | paste -sd',' -)"
fi

mkdir -p "$RECOVERY_DIR"

report_paths="$("$PYTHON_BIN" - "$RECOVERY_DIR" "$RECOVERY_LATEST_FILE" "$RECOVERY_MAX_EVENT_FILES" "$now_epoch" "$issue_detected" "$issue_remaining" "$resolved_in_run" "$decision_csv" "$actions_csv" "$HEALTH_STATUS" "$HEALTH_RED_REASONS" "$HEALTH_YELLOW_REASONS" "$HEALTH_AGE_SECONDS" "$PIPELINE_STATUS" "$PIPELINE_REASON" "$READINESS_AGE_SECONDS" "$READINESS_FILE" "$shadow_service_state" "$reporting_timer_state" "$reporting_service_state" "$reporting_service_activating_age_seconds" "$alpha_worker_service_state" "$breadth_worker_service_state" "$new_consecutive_red" "$new_consecutive_pipeline_red" "$new_consecutive_freshness_pressure" "$new_consecutive_retry_pressure" "$FRESHNESS_PRESSURE_ACTIVE" "$FRESHNESS_STALE_RATE" "$FRESHNESS_STALE_COUNT" "$FRESHNESS_APPROVAL_RATE" "$FRESHNESS_INTENTS_TOTAL" "$COMMAND_METAR_ATTEMPTS" "$COMMAND_SETTLEMENT_ATTEMPTS" "$COMMAND_SHADOW_ATTEMPTS" "$SCAN_EFFECTIVE_MAX_MARKETS" "$SCAN_NEXT_MAX_MARKETS" "$SCAN_ADAPTIVE_ACTION" "$SCAN_ADAPTIVE_REASON" "$STATE_LAST_ALPHA_WORKER_RESTART_EPOCH" "$STATE_LAST_BREADTH_WORKER_RESTART_EPOCH" "$LOG_MAINTENANCE_HEALTH_STATUS" "$LOG_MAINTENANCE_AGE_SECONDS" "$LOG_MAINTENANCE_USAGE_BYTES" "$log_maintenance_timer_state" "$log_maintenance_timer_enabled_state" "$log_maintenance_service_state" "$STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH" "$STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH" "$STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH" "$STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH" "$STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH" "$recovery_env_persistence_status" "$recovery_env_persistence_error_present" "$recovery_env_persistence_target_file" "$recovery_env_persistence_parse_error" "$recovery_env_persistence_strict_blocked" "$COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED" "$coldmath_stage_timeout_guardrails_status" "$coldmath_stage_timeout_guardrails_strict_blocked" "$coldmath_stage_timeout_guardrails_required_keys" "$coldmath_stage_timeout_guardrails_invalid_keys" "$coldmath_stage_timeout_guardrails_disabled_keys" "$coldmath_stage_timeout_required_stages" "$coldmath_stage_timeout_timeout_stages" "$coldmath_stage_timeout_missing_stage_telemetry" "$coldmath_stage_timeout_stage_telemetry_status" "$recovery_effectiveness_summary_available" "$recovery_effectiveness_summary_source" "$recovery_effectiveness_summary_file" "$recovery_effectiveness_harmful_actions" "$recovery_effectiveness_demoted_actions" "$recovery_effectiveness_summary_text" "$recovery_effectiveness_file_age_seconds" "$recovery_effectiveness_stale_threshold_seconds" "$recovery_effectiveness_summary_stale" "$recovery_effectiveness_gap_detected" "$recovery_effectiveness_gap_reason" "$RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys


def parse_int(value: str, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default

def parse_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def split_csv_tokens(value: str) -> list[str]:
    values = split_csv(value)
    lowered = [item.lower() for item in values]
    if not values:
        return []
    if len(values) == 1 and lowered[0] in {"none", "n/a", "na"}:
        return []
    return values


recovery_dir = Path(sys.argv[1])
latest_file = Path(sys.argv[2])
max_events = max(1, parse_int(sys.argv[3], 240))
now_epoch = parse_int(sys.argv[4], 0)
issue_detected = parse_bool(sys.argv[5])
issue_remaining = parse_bool(sys.argv[6])
resolved_in_run = parse_bool(sys.argv[7])
decision_reasons = split_csv(sys.argv[8])
action_records = split_csv(sys.argv[9])
health_status = str(sys.argv[10] or "unknown").strip().lower() or "unknown"
health_red_reasons = split_csv(sys.argv[11])
health_yellow_reasons = split_csv(sys.argv[12])
health_age_seconds = parse_int(sys.argv[13], -1)
pipeline_status = str(sys.argv[14] or "unknown").strip().lower() or "unknown"
pipeline_reason = str(sys.argv[15] or "").strip()
readiness_age_seconds = parse_int(sys.argv[16], -1)
readiness_file = str(sys.argv[17] or "").strip()
shadow_service_state = str(sys.argv[18] or "unknown").strip()
reporting_timer_state = str(sys.argv[19] or "unknown").strip()
reporting_service_state = str(sys.argv[20] or "unknown").strip()
reporting_service_activating_age_seconds = parse_int(sys.argv[21], -1)
alpha_worker_service_state = str(sys.argv[22] or "unknown").strip()
breadth_worker_service_state = str(sys.argv[23] or "unknown").strip()
consecutive_red = parse_int(sys.argv[24], 0)
consecutive_pipeline_red = parse_int(sys.argv[25], 0)
consecutive_freshness_pressure = parse_int(sys.argv[26], 0)
consecutive_retry_pressure = parse_int(sys.argv[27], 0)
freshness_pressure_active = parse_bool(sys.argv[28])
freshness_stale_rate = parse_float(sys.argv[29], 0.0)
freshness_stale_count = parse_int(sys.argv[30], 0)
freshness_approval_rate = parse_float(sys.argv[31], 0.0)
freshness_intents_total = parse_int(sys.argv[32], 0)
command_metar_attempts = parse_int(sys.argv[33], 1)
command_settlement_attempts = parse_int(sys.argv[34], 0)
command_shadow_attempts = parse_int(sys.argv[35], 1)
scan_effective_max_markets = parse_int(sys.argv[36], 0)
scan_next_max_markets = parse_int(sys.argv[37], 0)
scan_adaptive_action = str(sys.argv[38] or "").strip()
scan_adaptive_reason = str(sys.argv[39] or "").strip()
last_alpha_worker_restart_epoch = parse_int(sys.argv[40], 0)
last_breadth_worker_restart_epoch = parse_int(sys.argv[41], 0)
log_maintenance_health_status = str(sys.argv[42] or "unknown").strip().lower() or "unknown"
log_maintenance_age_seconds = parse_int(sys.argv[43], -1)
log_maintenance_usage_bytes = parse_int(sys.argv[44], 0)
log_maintenance_timer_state = str(sys.argv[45] or "unknown").strip()
log_maintenance_timer_enabled_state = str(sys.argv[46] or "unknown").strip()
log_maintenance_service_state = str(sys.argv[47] or "unknown").strip()
last_log_maintenance_trigger_epoch = parse_int(sys.argv[48], 0)
last_log_maintenance_timer_enable_epoch = parse_int(sys.argv[49], 0)
last_recovery_env_persistence_repair_epoch = parse_int(sys.argv[50], 0)
last_coldmath_stage_timeout_guardrail_repair_epoch = parse_int(sys.argv[51], 0)
last_coldmath_hardening_trigger_epoch = parse_int(sys.argv[52], 0)
recovery_env_persistence_status = str(sys.argv[53] or "unknown").strip().lower() or "unknown"
recovery_env_persistence_error_present = parse_bool(sys.argv[54])
recovery_env_persistence_target_file = str(sys.argv[55] or "").strip()
recovery_env_persistence_parse_error = parse_bool(sys.argv[56])
recovery_env_persistence_strict_blocked = parse_bool(sys.argv[57])
coldmath_stage_timeout_strict_required = parse_bool(sys.argv[58])
coldmath_stage_timeout_guardrails_status = str(sys.argv[59] or "unavailable").strip().lower() or "unavailable"
coldmath_stage_timeout_guardrails_strict_blocked = parse_bool(sys.argv[60])
coldmath_stage_timeout_guardrails_required_keys = split_csv_tokens(sys.argv[61])
coldmath_stage_timeout_guardrails_invalid_keys = split_csv_tokens(sys.argv[62])
coldmath_stage_timeout_guardrails_disabled_keys = split_csv_tokens(sys.argv[63])
coldmath_stage_timeout_required_stages = split_csv_tokens(sys.argv[64])
coldmath_stage_timeout_timeout_stages = split_csv_tokens(sys.argv[65])
coldmath_stage_timeout_missing_stage_telemetry = split_csv_tokens(sys.argv[66])
coldmath_stage_timeout_stage_telemetry_status = str(sys.argv[67] or "unavailable").strip().lower() or "unavailable"
recovery_effectiveness_summary_available = parse_bool(sys.argv[68])
recovery_effectiveness_summary_source = str(sys.argv[69] or "none").strip().lower() or "none"
recovery_effectiveness_summary_file = str(sys.argv[70] or "").strip()
recovery_effectiveness_harmful_actions = split_csv_tokens(sys.argv[71])
recovery_effectiveness_demoted_actions = split_csv_tokens(sys.argv[72])
recovery_effectiveness_summary_text = str(sys.argv[73] or "").strip()
recovery_effectiveness_file_age_seconds = parse_int(sys.argv[74], -1)
recovery_effectiveness_stale_threshold_seconds = max(0, parse_int(sys.argv[75], 21600))
recovery_effectiveness_summary_stale = parse_bool(sys.argv[76])
recovery_effectiveness_gap_detected = parse_bool(sys.argv[77])
recovery_effectiveness_gap_reason = str(sys.argv[78] or "none").strip().lower() or "none"
recovery_effectiveness_strict_required = parse_bool(sys.argv[79])
if recovery_effectiveness_gap_reason not in {"none", "summary_missing", "summary_stale"}:
    recovery_effectiveness_gap_reason = "none"

captured_at = datetime.fromtimestamp(now_epoch, tz=timezone.utc).isoformat()
payload = {
    "status": "ready",
    "event": "pipeline_recovery_check",
    "captured_at": captured_at,
    "issue_detected": issue_detected,
    "issue_remaining": issue_remaining,
    "resolved_in_run": resolved_in_run,
    "decision_reasons": decision_reasons,
    "actions_attempted": action_records,
    "health": {
        "status": health_status,
        "red_reasons": health_red_reasons,
        "yellow_reasons": health_yellow_reasons,
        "age_seconds": health_age_seconds,
    },
    "pipeline": {
        "status": pipeline_status,
        "reason": pipeline_reason,
        "readiness_age_seconds": readiness_age_seconds,
        "readiness_file": readiness_file,
    },
    "freshness": {
        "pressure_active": freshness_pressure_active,
        "stale_rate": round(freshness_stale_rate, 6),
        "stale_count": freshness_stale_count,
        "approval_rate": round(freshness_approval_rate, 6),
        "intents_total": freshness_intents_total,
    },
    "execution_attempts": {
        "metar": command_metar_attempts,
        "settlement": command_settlement_attempts,
        "shadow": command_shadow_attempts,
    },
    "scan_budget": {
        "effective_max_markets": scan_effective_max_markets,
        "next_max_markets": scan_next_max_markets,
        "adaptive_action": scan_adaptive_action,
        "adaptive_reason": scan_adaptive_reason,
    },
    "service_states": {
        "shadow_service": shadow_service_state,
        "reporting_timer": reporting_timer_state,
        "reporting_service": reporting_service_state,
        "reporting_service_activating_age_seconds": reporting_service_activating_age_seconds,
        "alpha_worker_service": alpha_worker_service_state,
        "breadth_worker_service": breadth_worker_service_state,
        "log_maintenance_timer": log_maintenance_timer_state,
        "log_maintenance_timer_enabled": log_maintenance_timer_enabled_state,
        "log_maintenance_service": log_maintenance_service_state,
    },
    "log_maintenance": {
        "health_status": log_maintenance_health_status,
        "age_seconds": log_maintenance_age_seconds,
        "usage_bytes": log_maintenance_usage_bytes,
    },
    "recovery_env_persistence": {
        "status": recovery_env_persistence_status,
        "error_present": recovery_env_persistence_error_present,
        "target_file": recovery_env_persistence_target_file,
        "parse_error": recovery_env_persistence_parse_error,
        "strict_blocked": recovery_env_persistence_strict_blocked,
    },
    "coldmath_stage_timeout_guardrails": {
        "status": coldmath_stage_timeout_guardrails_status,
        "strict_required": coldmath_stage_timeout_strict_required,
        "strict_blocked": coldmath_stage_timeout_guardrails_strict_blocked,
        "required_keys": coldmath_stage_timeout_guardrails_required_keys,
        "invalid_keys": coldmath_stage_timeout_guardrails_invalid_keys,
        "disabled_keys": coldmath_stage_timeout_guardrails_disabled_keys,
        "required_stages": coldmath_stage_timeout_required_stages,
        "timeout_stages": coldmath_stage_timeout_timeout_stages,
        "missing_stage_telemetry": coldmath_stage_timeout_missing_stage_telemetry,
        "stage_telemetry_status": coldmath_stage_timeout_stage_telemetry_status,
    },
    "recovery_effectiveness": {
        "summary_available": recovery_effectiveness_summary_available,
        "summary_source": recovery_effectiveness_summary_source,
        "summary_file_used": recovery_effectiveness_summary_file,
        "persistently_harmful_actions": recovery_effectiveness_harmful_actions,
        "demoted_actions_for_effectiveness": recovery_effectiveness_demoted_actions,
        "harmful_action_count": len(recovery_effectiveness_harmful_actions),
        "demoted_action_count": len(recovery_effectiveness_demoted_actions),
        "summary": recovery_effectiveness_summary_text,
        "file_age_seconds": recovery_effectiveness_file_age_seconds,
        "stale_threshold_seconds": recovery_effectiveness_stale_threshold_seconds,
        "stale": recovery_effectiveness_summary_stale,
        "gap_detected": recovery_effectiveness_gap_detected,
        "gap_reason": recovery_effectiveness_gap_reason,
        "strict_required": recovery_effectiveness_strict_required,
    },
    "state": {
        "consecutive_red": consecutive_red,
        "consecutive_pipeline_red": consecutive_pipeline_red,
        "consecutive_freshness_pressure": consecutive_freshness_pressure,
        "consecutive_retry_pressure": consecutive_retry_pressure,
        "last_alpha_worker_restart_epoch": last_alpha_worker_restart_epoch,
        "last_breadth_worker_restart_epoch": last_breadth_worker_restart_epoch,
        "last_log_maintenance_trigger_epoch": last_log_maintenance_trigger_epoch,
        "last_log_maintenance_timer_enable_epoch": last_log_maintenance_timer_enable_epoch,
        "last_recovery_env_persistence_repair_epoch": last_recovery_env_persistence_repair_epoch,
        "last_coldmath_stage_timeout_guardrail_repair_epoch": last_coldmath_stage_timeout_guardrail_repair_epoch,
        "last_coldmath_hardening_trigger_epoch": last_coldmath_hardening_trigger_epoch,
    },
}

recovery_dir.mkdir(parents=True, exist_ok=True)
latest_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

event_file = ""
if issue_detected or action_records:
    stamp = datetime.fromtimestamp(now_epoch, tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    event_file = str(recovery_dir / f"recovery_event_{stamp}.json")
    Path(event_file).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    old_events = sorted(recovery_dir.glob("recovery_event_*.json"))
    if len(old_events) > max_events:
        for path in old_events[: len(old_events) - max_events]:
            try:
                path.unlink()
            except OSError:
                pass

print(f"latest={latest_file}")
print(f"event={event_file}")
PY
)"

latest_report_path="$(awk -F'=' '/^latest=/{print $2}' <<<"$report_paths" | tail -n 1)"
event_report_path="$(awk -F'=' '/^event=/{print $2}' <<<"$report_paths" | tail -n 1)"

"$PYTHON_BIN" - "$RECOVERY_STATE_FILE" "$now_epoch" "$new_consecutive_red" "$new_consecutive_pipeline_red" "$new_consecutive_freshness_pressure" "$new_consecutive_retry_pressure" "$STATE_LAST_SHADOW_RESTART_EPOCH" "$STATE_LAST_METAR_REFRESH_EPOCH" "$STATE_LAST_SETTLEMENT_REFRESH_EPOCH" "$STATE_LAST_REPORTING_TRIGGER_EPOCH" "$STATE_LAST_REPORTING_RESTART_EPOCH" "$STATE_LAST_ALPHA_SUMMARY_TRIGGER_EPOCH" "$STATE_LAST_ALPHA_WORKER_RESTART_EPOCH" "$STATE_LAST_BREADTH_WORKER_RESTART_EPOCH" "$STATE_LAST_SCAN_BUDGET_TRIM_EPOCH" "$STATE_LAST_LOG_MAINTENANCE_TRIGGER_EPOCH" "$STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH" "$STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH" "$STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH" "$STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys


def parse_int(value: str, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


state_path = Path(sys.argv[1])
payload = {
    "captured_at_epoch": parse_int(sys.argv[2], 0),
    "consecutive_red": parse_int(sys.argv[3], 0),
    "consecutive_pipeline_red": parse_int(sys.argv[4], 0),
    "consecutive_freshness_pressure": parse_int(sys.argv[5], 0),
    "consecutive_retry_pressure": parse_int(sys.argv[6], 0),
    "last_shadow_restart_epoch": parse_int(sys.argv[7], 0),
    "last_metar_refresh_epoch": parse_int(sys.argv[8], 0),
    "last_settlement_refresh_epoch": parse_int(sys.argv[9], 0),
    "last_reporting_trigger_epoch": parse_int(sys.argv[10], 0),
    "last_reporting_restart_epoch": parse_int(sys.argv[11], 0),
    "last_alpha_summary_trigger_epoch": parse_int(sys.argv[12], 0),
    "last_alpha_worker_restart_epoch": parse_int(sys.argv[13], 0),
    "last_breadth_worker_restart_epoch": parse_int(sys.argv[14], 0),
    "last_scan_budget_trim_epoch": parse_int(sys.argv[15], 0),
    "last_log_maintenance_trigger_epoch": parse_int(sys.argv[16], 0),
    "last_log_maintenance_timer_enable_epoch": parse_int(sys.argv[17], 0),
    "last_recovery_env_persistence_repair_epoch": parse_int(sys.argv[18], 0),
    "last_coldmath_stage_timeout_guardrail_repair_epoch": parse_int(sys.argv[19], 0),
    "last_coldmath_hardening_trigger_epoch": parse_int(sys.argv[20], 0),
}
state_path.parent.mkdir(parents=True, exist_ok=True)
state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY

log_line "recovery_check issue=$issue_detected health=$HEALTH_STATUS pipeline=$PIPELINE_STATUS freshness_pressure_now=$freshness_pressure_now consecutive_freshness_pressure=$new_consecutive_freshness_pressure consecutive_retry_pressure=$new_consecutive_retry_pressure stale_rate=$FRESHNESS_STALE_RATE approval_rate=$FRESHNESS_APPROVAL_RATE intents_total=$FRESHNESS_INTENTS_TOTAL attempts_metar=$COMMAND_METAR_ATTEMPTS attempts_settlement=$COMMAND_SETTLEMENT_ATTEMPTS attempts_shadow=$COMMAND_SHADOW_ATTEMPTS scan_effective_max_markets=$SCAN_EFFECTIVE_MAX_MARKETS scan_next_max_markets=$SCAN_NEXT_MAX_MARKETS scan_adaptive_action=${SCAN_ADAPTIVE_ACTION:-n/a} scan_adaptive_reason=${SCAN_ADAPTIVE_REASON:-n/a} alpha_summary_age_seconds=$ALPHA_SUMMARY_AGE_SECONDS alpha_summary_trigger_age_seconds=$alpha_summary_trigger_age_seconds alpha_summary_trigger_grace_active=$alpha_summary_trigger_grace_active alpha_summary_timer_state=$alpha_summary_timer_state alpha_worker_state=$alpha_worker_service_state breadth_worker_state=$breadth_worker_service_state reporting_service_state=$reporting_service_state reporting_activating_age_seconds=$reporting_service_activating_age_seconds log_maintenance_health=$LOG_MAINTENANCE_HEALTH_STATUS log_maintenance_age_seconds=$LOG_MAINTENANCE_AGE_SECONDS log_maintenance_usage_bytes=$LOG_MAINTENANCE_USAGE_BYTES log_maintenance_timer_state=$log_maintenance_timer_state log_maintenance_timer_enabled_state=$log_maintenance_timer_enabled_state log_maintenance_service_state=$log_maintenance_service_state last_log_maintenance_timer_enable_epoch=$STATE_LAST_LOG_MAINTENANCE_TIMER_ENABLE_EPOCH recovery_env_persistence_status=$recovery_env_persistence_status recovery_env_persistence_error_present=$recovery_env_persistence_error_present recovery_env_persistence_target_file=${recovery_env_persistence_target_file:-n/a} coldmath_stage_timeout_guardrails_status=$coldmath_stage_timeout_guardrails_status coldmath_stage_timeout_guardrails_strict_blocked=$coldmath_stage_timeout_guardrails_strict_blocked coldmath_stage_timeout_required_stages=${coldmath_stage_timeout_required_stages:-none} coldmath_stage_timeout_timeout_stages=${coldmath_stage_timeout_timeout_stages:-none} coldmath_stage_timeout_stage_telemetry_status=$coldmath_stage_timeout_stage_telemetry_status recovery_effectiveness_summary_available=$recovery_effectiveness_summary_available recovery_effectiveness_source=${recovery_effectiveness_summary_source:-none} recovery_effectiveness_harmful_actions=${recovery_effectiveness_harmful_actions:-none} recovery_effectiveness_demoted_actions=${recovery_effectiveness_demoted_actions:-none} recovery_effectiveness_file_age_seconds=$recovery_effectiveness_file_age_seconds recovery_effectiveness_stale=$recovery_effectiveness_summary_stale recovery_effectiveness_gap_detected=$recovery_effectiveness_gap_detected recovery_effectiveness_gap_reason=$recovery_effectiveness_gap_reason recovery_effectiveness_strict_required=$RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY last_recovery_env_persistence_repair_epoch=$STATE_LAST_RECOVERY_ENV_PERSISTENCE_REPAIR_EPOCH last_coldmath_stage_timeout_guardrail_repair_epoch=$STATE_LAST_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_EPOCH last_coldmath_hardening_trigger_epoch=$STATE_LAST_COLDMATH_HARDENING_TRIGGER_EPOCH reasons=${decision_csv:-none} actions=${actions_csv:-none} latest=$latest_report_path event=${event_report_path:-none}"

if [[ -n "$RECOVERY_WEBHOOK_TARGET_URL" && ( "$issue_detected" == "1" || -n "$actions_csv" ) ]]; then
payload_json="$("$PYTHON_BIN" - "$issue_detected" "$issue_remaining" "$resolved_in_run" "$HEALTH_STATUS" "$PIPELINE_STATUS" "$decision_csv" "$actions_csv" "$shadow_service_state" "$reporting_timer_state" "$reporting_service_state" "$alpha_worker_service_state" "$breadth_worker_service_state" "$reporting_service_activating_age_seconds" "$event_report_path" "$FRESHNESS_PRESSURE_ACTIVE" "$FRESHNESS_STALE_RATE" "$FRESHNESS_APPROVAL_RATE" "$FRESHNESS_INTENTS_TOTAL" "$COMMAND_METAR_ATTEMPTS" "$COMMAND_SETTLEMENT_ATTEMPTS" "$COMMAND_SHADOW_ATTEMPTS" "$SCAN_EFFECTIVE_MAX_MARKETS" "$SCAN_NEXT_MAX_MARKETS" "$SCAN_ADAPTIVE_ACTION" "$SCAN_ADAPTIVE_REASON" "$RECOVERY_NOTIFY_STATUS_CHANGE_ONLY" "$RECOVERY_ALERT_STATE_FILE" "$RECOVERY_WEBHOOK_MESSAGE_MODE" "$LOG_MAINTENANCE_HEALTH_STATUS" "$LOG_MAINTENANCE_AGE_SECONDS" "$LOG_MAINTENANCE_USAGE_BYTES" "$log_maintenance_timer_state" "$log_maintenance_timer_enabled_state" "$log_maintenance_service_state" "$recovery_effectiveness_summary_available" "$recovery_effectiveness_harmful_actions" "$recovery_effectiveness_demoted_actions" "$recovery_effectiveness_summary_text" "$RECOVERY_WEBHOOK_USERNAME" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys


def humanize(value: str) -> str:
    key = str(value or "").strip()
    if not key:
        return ""
    mapping = {
        "shadow_service_inactive": "shadow service not running",
        "shadow_service_failed": "shadow service failed",
        "shadow_service_unknown": "shadow service state unknown",
        "alpha_worker_service_inactive": "alpha worker service not running",
        "alpha_worker_service_failed": "alpha worker service failed",
        "alpha_worker_service_unknown": "alpha worker state unknown",
        "breadth_worker_service_inactive": "breadth worker service not running",
        "breadth_worker_service_failed": "breadth worker service failed",
        "breadth_worker_service_unknown": "breadth worker state unknown",
        "reporting_timer_inactive": "reporting timer not running",
        "reporting_timer_failed": "reporting timer failed",
        "reporting_timer_unknown": "reporting timer state unknown",
        "alpha_summary_timer_inactive": "alpha summary timer not running",
        "alpha_summary_timer_failed": "alpha summary timer failed",
        "alpha_summary_timer_unknown": "alpha summary timer state unknown",
        "live_status_stale_or_missing": "live status is stale or missing",
        "readiness_stale_or_missing": "readiness report is stale or missing",
        "readiness_stale_but_runner_active": "readiness report stale but refresh runner is active",
        "alpha_summary_stale_or_missing": "alpha summary is stale or missing",
        "alpha_summary_stale_trigger_grace": "alpha summary stale but in post-trigger grace window",
        "reporting_service_activating_too_long": "reporting service stuck starting",
        "reporting_service_activating_long_deferred": "reporting service still starting (restart deferred)",
        "reporting_service_activating_long_runner_active": "reporting service still starting while runner actively refreshing",
        "reporting_service_failed": "reporting service failed",
        "metar_stale_or_missing": "METAR refresh stale/missing",
        "settlement_stale_or_missing": "settlement refresh stale/missing",
        "shadow_failed_or_stale": "shadow output stale or failed",
        "consecutive_red_threshold": "health stayed red across checks",
        "pipeline_red": "pipeline marked red",
        "consecutive_pipeline_red_threshold": "pipeline stayed red across checks",
        "freshness_pressure_detected": "freshness pressure detected",
        "freshness_pressure_persistent": "freshness pressure persisted across checks",
        "freshness_pressure_persistent_high": "freshness pressure remained high",
        "scan_budget_trim_candidate": "scan budget trim candidate detected",
        "recovery_env_persistence_error": "coldmath recovery env persistence has error",
        "recovery_effectiveness_summary_missing": "recovery effectiveness summary missing",
        "recovery_effectiveness_summary_stale": "recovery effectiveness summary stale",
        "coldmath_stage_timeout_guardrails_invalid": "coldmath stage-timeout guardrails invalid",
        "coldmath_stage_timeout_guardrails_disabled": "coldmath stage-timeout guardrails disabled",
        "coldmath_stage_timeout_stage_timeouts": "coldmath hardening stage timeout detected",
        "log_maintenance_timer_inactive": "log-maintenance timer not running",
        "log_maintenance_timer_failed": "log-maintenance timer failed",
        "log_maintenance_timer_unknown": "log-maintenance timer state unknown",
        "log_maintenance_stale_or_missing": "log-maintenance status stale or missing",
        "log_maintenance_health_red": "log-maintenance health is red",
        "log_maintenance_health_yellow": "log-maintenance health is yellow",
        "retry_pressure_detected": "command retries elevated",
        "retry_pressure_persistent": "command retries persisted across checks",
        "enable_reporting_timer:ok": "enabled reporting timer",
        "enable_reporting_timer:failed": "failed to enable reporting timer",
        "enable_reporting_timer:missing_unit": "reporting timer unit missing",
        "enable_alpha_summary_timer:ok": "enabled alpha summary timer",
        "enable_alpha_summary_timer:failed": "failed to enable alpha summary timer",
        "enable_alpha_summary_timer:missing_unit": "alpha summary timer unit missing",
        "restart_shadow:ok": "restarted shadow service",
        "restart_shadow:failed": "failed to restart shadow service",
        "restart_shadow:cooldown": "shadow restart skipped (cooldown)",
        "restart_shadow:disabled": "shadow restart disabled",
        "restart_shadow:missing_unit": "shadow service unit missing",
        "restart_reporting:ok": "restarted reporting service",
        "restart_reporting:failed": "failed to restart reporting service",
        "restart_reporting:cooldown": "reporting restart skipped (cooldown)",
        "restart_reporting:disabled": "reporting restart disabled",
        "restart_reporting:missing_unit": "reporting service unit missing",
        "restart_alpha_worker:ok": "restarted alpha worker service",
        "restart_alpha_worker:failed": "failed to restart alpha worker service",
        "restart_alpha_worker:cooldown": "alpha worker restart skipped (cooldown)",
        "restart_alpha_worker:disabled": "alpha worker restart disabled",
        "restart_alpha_worker:missing_unit": "alpha worker unit missing",
        "restart_breadth_worker:ok": "restarted breadth worker service",
        "restart_breadth_worker:failed": "failed to restart breadth worker service",
        "restart_breadth_worker:cooldown": "breadth worker restart skipped (cooldown)",
        "restart_breadth_worker:disabled": "breadth worker restart disabled",
        "restart_breadth_worker:missing_unit": "breadth worker unit missing",
        "reset_reporting_failed_state:ok": "cleared reporting failed state",
        "reset_reporting_failed_state:failed": "failed to clear reporting failed state",
        "refresh_metar:ok": "ran METAR refresh",
        "refresh_metar:failed": "METAR refresh failed",
        "refresh_metar:cooldown": "METAR refresh skipped (cooldown)",
        "refresh_metar:disabled": "METAR refresh disabled",
        "refresh_settlement:ok": "ran settlement refresh",
        "refresh_settlement:failed": "settlement refresh failed",
        "refresh_settlement:cooldown": "settlement refresh skipped (cooldown)",
        "refresh_settlement:disabled": "settlement refresh disabled",
        "trim_scan_budget:ok": "trimmed adaptive scan budget",
        "trim_scan_budget:failed": "failed to trim adaptive scan budget",
        "trim_scan_budget:disabled": "scan budget trim disabled",
        "trim_scan_budget:cooldown": "scan budget trim skipped (cooldown)",
        "trim_scan_budget:below_min": "scan budget trim skipped (already near minimum)",
        "trim_scan_budget:no_change": "scan budget trim skipped (no change)",
        "trigger_reporting:ok": "triggered reporting run",
        "trigger_reporting:failed": "failed to trigger reporting run",
        "trigger_reporting:cooldown": "reporting trigger skipped (cooldown)",
        "trigger_reporting:disabled": "reporting trigger disabled",
        "trigger_reporting:missing_unit": "reporting service unit missing",
        "trigger_alpha_summary:ok": "triggered alpha summary run",
        "trigger_alpha_summary:failed": "failed to trigger alpha summary run",
        "trigger_alpha_summary:cooldown": "alpha summary trigger skipped (cooldown)",
        "trigger_alpha_summary:disabled": "alpha summary trigger disabled",
        "trigger_alpha_summary:missing_unit": "alpha summary service unit missing",
        "trigger_log_maintenance:ok": "triggered log-maintenance run",
        "trigger_log_maintenance:failed": "failed to trigger log-maintenance run",
        "trigger_log_maintenance:cooldown": "log-maintenance trigger skipped (cooldown)",
        "trigger_log_maintenance:disabled": "log-maintenance trigger disabled",
        "trigger_log_maintenance:missing_unit": "log-maintenance unit missing",
        "enable_log_maintenance_timer:ok": "enabled log-maintenance timer",
        "enable_log_maintenance_timer:failed": "failed to enable log-maintenance timer",
        "enable_log_maintenance_timer:cooldown": "log-maintenance timer enable skipped (cooldown)",
        "enable_log_maintenance_timer:disabled": "log-maintenance timer auto-enable disabled",
        "enable_log_maintenance_timer:missing_unit": "log-maintenance timer unit missing",
        "repair_recovery_env_persistence_gate:ok": "repaired coldmath recovery env persistence strict gate",
        "repair_recovery_env_persistence_gate:failed": "failed to repair coldmath recovery env persistence strict gate",
        "repair_recovery_env_persistence_gate:cooldown": "coldmath recovery env persistence repair skipped (cooldown)",
        "repair_recovery_env_persistence_gate:disabled": "coldmath recovery env persistence repair disabled",
        "repair_recovery_env_persistence_gate:missing_script": "coldmath recovery env persistence repair script missing",
        "repair_coldmath_stage_timeout_guardrails:ok": "repaired coldmath stage-timeout guardrails",
        "repair_coldmath_stage_timeout_guardrails:failed": "failed to repair coldmath stage-timeout guardrails",
        "repair_coldmath_stage_timeout_guardrails:cooldown": "coldmath stage-timeout guardrail repair skipped (cooldown)",
        "repair_coldmath_stage_timeout_guardrails:disabled": "coldmath stage-timeout guardrail repair disabled",
        "repair_coldmath_stage_timeout_guardrails:missing_script": "coldmath stage-timeout guardrail repair script missing",
        "trigger_coldmath_hardening_after_env_repair:ok": "triggered coldmath hardening after env persistence repair",
        "trigger_coldmath_hardening_after_env_repair:failed": "failed to trigger coldmath hardening after env persistence repair",
        "trigger_coldmath_hardening_after_env_repair:cooldown": "coldmath hardening trigger skipped (cooldown)",
        "trigger_coldmath_hardening_after_env_repair:disabled": "coldmath hardening trigger after env repair disabled",
        "trigger_coldmath_hardening_after_env_repair:missing_unit": "coldmath hardening service unit missing",
        "trigger_coldmath_hardening_after_stage_timeout_repair:ok": "triggered coldmath hardening after stage-timeout repair",
        "trigger_coldmath_hardening_after_stage_timeout_repair:failed": "failed to trigger coldmath hardening after stage-timeout repair",
        "trigger_coldmath_hardening_after_stage_timeout_repair:cooldown": "coldmath hardening trigger skipped after stage-timeout repair (cooldown)",
        "trigger_coldmath_hardening_after_stage_timeout_repair:disabled": "coldmath hardening trigger after stage-timeout repair disabled",
        "trigger_coldmath_hardening_after_stage_timeout_repair:missing_unit": "coldmath hardening service unit missing",
        "trigger_coldmath_hardening_on_effectiveness_gap:ok": "triggered coldmath hardening on effectiveness gap",
        "trigger_coldmath_hardening_on_effectiveness_gap:failed": "failed to trigger coldmath hardening on effectiveness gap",
        "trigger_coldmath_hardening_on_effectiveness_gap:cooldown": "coldmath hardening trigger on effectiveness gap skipped (cooldown)",
        "trigger_coldmath_hardening_on_effectiveness_gap:disabled": "coldmath hardening trigger on effectiveness gap disabled",
        "trigger_coldmath_hardening_on_effectiveness_gap:missing_unit": "coldmath hardening service unit missing",
    }
    return mapping.get(key, key.replace("_", " "))


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def split_csv_tokens(value: str) -> list[str]:
    values = split_csv(value)
    lowered = [item.lower() for item in values]
    if not values:
        return []
    if len(values) == 1 and lowered[0] in {"none", "n/a", "na"}:
        return []
    return values


def humanize_route_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return " ".join(text.replace("_", " ").replace("-", " ").replace(":", " ").split())


def build_effectiveness_summary(
    *,
    summary_available: bool,
    harmful_actions: list[str],
    demoted_actions: list[str],
    summary_text: str,
) -> str:
    harmful_human = [humanize_route_key(item) for item in harmful_actions if humanize_route_key(item)]
    demoted_human = [humanize_route_key(item) for item in demoted_actions if humanize_route_key(item)]
    if harmful_human or demoted_human:
        parts = [
            f"effectiveness harmful routes {len(harmful_human)}",
            f"demoted routes {len(demoted_human)}",
        ]
        lead = harmful_human[0] if harmful_human else (demoted_human[0] if demoted_human else "")
        if lead:
            parts.append(f"top route {lead}")
        return ", ".join(parts)
    if summary_available:
        clean_summary = " ".join(str(summary_text or "").split())
        if clean_summary:
            return clean_summary
        return "effectiveness shows no persistently harmful routes"
    return ""


def normalize_fingerprint_action_code(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parts = text.split(":")
    if len(parts) <= 2:
        return text
    return ":".join(parts[:2])


def normalize_fingerprint_decision_code(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parts = text.split(":")
    if len(parts) <= 2:
        return text
    return ":".join(parts[:2])


def severity_sort_codes(
    codes: list[str],
    *,
    kind: str,
) -> list[str]:
    def decision_priority(code: str) -> tuple[int, int, str]:
        key = str(code or "").strip().lower()
        if not key:
            return (999, 999, "")
        explicit = {
            "pipeline_red": 0,
            "consecutive_pipeline_red_threshold": 1,
            "reporting_service_failed": 2,
            "coldmath_stage_timeout_stage_timeouts": 3,
            "recovery_effectiveness_summary_missing": 4,
            "recovery_effectiveness_summary_stale": 5,
            "consecutive_red_threshold": 6,
            "log_maintenance_health_red": 7,
            "shadow_failed_or_stale": 8,
            "metar_stale_or_missing": 9,
            "settlement_stale_or_missing": 10,
            "readiness_stale_or_missing": 11,
            "live_status_stale_or_missing": 12,
            "alpha_summary_stale_or_missing": 13,
            "freshness_pressure_persistent_high": 14,
            "freshness_pressure_persistent": 15,
            "retry_pressure_persistent": 16,
        }
        if key in explicit:
            return (explicit[key], 0, key)
        if "failed" in key:
            return (20, 0, key)
        if "timeout" in key:
            return (25, 0, key)
        if "stale" in key:
            return (30, 0, key)
        if "unknown" in key:
            return (70, 0, key)
        return (60, 0, key)

    def action_priority(code: str) -> tuple[int, int, str]:
        text = str(code or "").strip()
        key = text.lower()
        if not key:
            return (999, 999, "")
        if key == "repair_coldmath_stage_timeout_guardrails:missing_script":
            return (0, 0, key)
        if key == "repair_coldmath_stage_timeout_guardrails:failed":
            return (1, 0, key)
        if key == "trigger_coldmath_hardening_on_effectiveness_gap:failed":
            return (2, 0, key)
        if key == "trigger_coldmath_hardening_on_effectiveness_gap:missing_unit":
            return (3, 0, key)
        parts = key.split(":")
        status = parts[1] if len(parts) > 1 else ""
        status_priority = {
            "failed": 2,
            "missing_script": 3,
            "missing_unit": 4,
            "ok": 6,
            "cooldown": 8,
            "disabled": 9,
            "below_min": 10,
            "no_change": 11,
        }
        priority = status_priority.get(status, 7)
        return (priority, 0, key)

    indexed = [(idx, str(item or "").strip()) for idx, item in enumerate(codes)]
    indexed = [(idx, code) for idx, code in indexed if code]
    if kind == "decision":
        ranked = sorted(indexed, key=lambda row: (*decision_priority(row[1]), row[0]))
    else:
        ranked = sorted(indexed, key=lambda row: (*action_priority(row[1]), row[0]))
    return [code for _, code in ranked]


def summarize(items: list[str], limit: int = 4) -> str:
    if not items:
        return "none"
    if len(items) <= limit:
        return ", ".join(items)
    return ", ".join(items[:limit]) + f" (+{len(items) - limit} more)"


def clip(value: str, limit: int = 180) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    clipped = text[: max(1, limit)].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0].rstrip(" ,;:-")
    return clipped


def humanize_phrase(value: str, default: str = "n/a") -> str:
    text = str(value or "").strip()
    if not text:
        return default
    return " ".join(text.replace("_", " ").split())


def compact_concise(lines: list[str]) -> list[str]:
    cleaned = [clip(line, 170) for line in lines if str(line or "").strip()]
    if not cleaned:
        return cleaned
    header = cleaned[0]
    state = next((line for line in cleaned if line.startswith("State:")), "")
    happened = next((line for line in cleaned if line.startswith("What happened:")), "")
    action_taken = next((line for line in cleaned if line.startswith("Auto actions:")), "")
    issue = next((line for line in cleaned if line.startswith("Issue:")), "")
    pressure = next((line for line in cleaned if line.startswith("Pressure:")), "")
    why = next((line for line in cleaned if line.startswith("Why it matters:")), "")
    action = next((line for line in cleaned if line.startswith("Next step:")), "")
    event = next((line for line in cleaned if line.startswith("Event:")), "")
    out: list[str] = [header]
    for item in (state, happened, action_taken, issue, pressure, why, action, event):
        if item and item not in out:
            out.append(item)
    return out[:7]


issue_detected = str(sys.argv[1]).strip() in {"1", "true", "yes", "y"}
issue_remaining = str(sys.argv[2]).strip() in {"1", "true", "yes", "y"}
resolved_in_run = str(sys.argv[3]).strip() in {"1", "true", "yes", "y"}
health_status = str(sys.argv[4] or "unknown").strip().upper() or "UNKNOWN"
pipeline_status = str(sys.argv[5] or "unknown").strip().upper() or "UNKNOWN"
decision_codes = split_csv(sys.argv[6])
action_codes = split_csv(sys.argv[7])
shadow_service = str(sys.argv[8] or "unknown").strip()
reporting_timer = str(sys.argv[9] or "unknown").strip()
reporting_service = str(sys.argv[10] or "unknown").strip()
alpha_worker_service = str(sys.argv[11] or "unknown").strip()
breadth_worker_service = str(sys.argv[12] or "unknown").strip()
reporting_activating_age = str(sys.argv[13] or "-1").strip()
event_file = str(sys.argv[14] or "").strip()
freshness_pressure_active = str(sys.argv[15]).strip() in {"1", "true", "yes", "y"}
freshness_stale_rate = str(sys.argv[16] or "0").strip()
freshness_approval_rate = str(sys.argv[17] or "0").strip()
freshness_intents_total = str(sys.argv[18] or "0").strip()
command_metar_attempts = str(sys.argv[19] or "1").strip()
command_settlement_attempts = str(sys.argv[20] or "0").strip()
command_shadow_attempts = str(sys.argv[21] or "1").strip()
scan_effective_max_markets = str(sys.argv[22] or "0").strip()
scan_next_max_markets = str(sys.argv[23] or "0").strip()
scan_adaptive_action = str(sys.argv[24] or "").strip()
scan_adaptive_reason = str(sys.argv[25] or "").strip()
status_change_only = str(sys.argv[26]).strip().lower() in {"1", "true", "yes", "y"}
alert_state_path = Path(str(sys.argv[27] or "").strip())
message_mode = (str(sys.argv[28] or "").strip().lower() or "concise")
log_maintenance_health_status = str(sys.argv[29] or "unknown").strip().lower() or "unknown"
log_maintenance_age_seconds = str(sys.argv[30] or "-1").strip()
log_maintenance_usage_bytes = str(sys.argv[31] or "0").strip()
log_maintenance_timer = str(sys.argv[32] or "unknown").strip()
log_maintenance_timer_enabled = str(sys.argv[33] or "unknown").strip()
log_maintenance_service = str(sys.argv[34] or "unknown").strip()
recovery_effectiveness_summary_available = str(sys.argv[35]).strip() in {"1", "true", "yes", "y"}
recovery_effectiveness_harmful_actions = split_csv_tokens(sys.argv[36])
recovery_effectiveness_demoted_actions = split_csv_tokens(sys.argv[37])
recovery_effectiveness_summary_text = str(sys.argv[38] or "").strip()
username = str(sys.argv[39] or "").strip() or "BetBot Recovery"

effectiveness_summary = build_effectiveness_summary(
    summary_available=recovery_effectiveness_summary_available,
    harmful_actions=recovery_effectiveness_harmful_actions,
    demoted_actions=recovery_effectiveness_demoted_actions,
    summary_text=recovery_effectiveness_summary_text,
)

display_decision_codes = severity_sort_codes(decision_codes, kind="decision")
display_action_codes = severity_sort_codes(action_codes, kind="action")
reasons = [humanize(item) for item in display_decision_codes if humanize(item)]
actions = [humanize(item) for item in display_action_codes if humanize(item)]
fingerprint_decision_codes: list[str] = []
for item in decision_codes:
    normalized = normalize_fingerprint_decision_code(item)
    if normalized:
        fingerprint_decision_codes.append(normalized)

fingerprint_action_codes: list[str] = []
for item in action_codes:
    normalized = normalize_fingerprint_action_code(item)
    if normalized:
        fingerprint_action_codes.append(normalized)

# Keep fingerprint stable for status-change notifications.
# Do not include high-churn telemetry fields (rates, attempts, scan budgets),
# otherwise alerts spam even when the incident is unchanged.
fingerprint_payload = {
    "issue_detected": issue_detected,
    "issue_remaining": issue_remaining,
    "resolved_in_run": resolved_in_run,
    "health_status": health_status,
    "pipeline_status": pipeline_status,
    "decision_codes": sorted(fingerprint_decision_codes),
    "action_codes": sorted(fingerprint_action_codes),
    "shadow_service": shadow_service,
    "reporting_timer": reporting_timer,
    "reporting_service": reporting_service,
    "alpha_worker_service": alpha_worker_service,
    "breadth_worker_service": breadth_worker_service,
    "log_maintenance_health_status": log_maintenance_health_status,
    "recovery_effectiveness_summary_available": recovery_effectiveness_summary_available,
    "recovery_effectiveness_harmful_actions": sorted(recovery_effectiveness_harmful_actions),
    "recovery_effectiveness_demoted_actions": sorted(recovery_effectiveness_demoted_actions),
}
fingerprint = json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":"))

last_fingerprint = ""
if alert_state_path:
    try:
        loaded = json.loads(alert_state_path.read_text(encoding="utf-8"))
    except Exception:
        loaded = {}
    if isinstance(loaded, dict):
        last_fingerprint = str(loaded.get("last_fingerprint") or "")

if status_change_only and last_fingerprint == fingerprint:
    print("")
    raise SystemExit(0)

if issue_detected and issue_remaining:
    header = "BetBot Recovery Alert"
elif issue_detected and resolved_in_run:
    header = "BetBot Recovery Auto-Healed"
else:
    header = "BetBot Recovery Action"
event_name = Path(event_file).name if event_file else ""
recovery_state = (
    "Recovery: "
    f"detected={'yes' if issue_detected else 'no'} | "
    f"remaining={'yes' if issue_remaining else 'no'} | "
    f"resolved now={'yes' if resolved_in_run else 'no'}"
)

if message_mode == "concise":
    recovery_state_short = (
        "OPEN"
        if issue_remaining
        else ("AUTO-HEALED" if issue_detected and resolved_in_run else "STABLE")
    )
    lines = [
        header,
        f"State: {recovery_state_short} | health {health_status} | pipeline {pipeline_status}",
        f"What happened: {clip(summarize(reasons, 3), 180)}",
        f"Auto actions: {clip(summarize(actions, 3), 180)}",
    ]
    retries = f"{command_metar_attempts}/{command_settlement_attempts}/{command_shadow_attempts}"
    if issue_remaining and (freshness_pressure_active or retries != "1/1/1"):
        lines.append(
            "Pressure: "
            f"stale={freshness_stale_rate} approval={freshness_approval_rate} intents={freshness_intents_total} retries={retries}"
        )
    why_line = "Why it matters: stale pipeline state can delay or distort decision-quality signals."
    if effectiveness_summary:
      why_line = why_line.rstrip(".") + f"; {effectiveness_summary}."
    lines.append(why_line)
    if event_name:
        lines.append(f"Event: {event_name}")
    if issue_remaining:
        lines.append("Next step: run check_temperature_shadow.sh --strict now.")
    elif issue_detected and resolved_in_run:
        lines.append("Next step: monitor only; auto-heal already applied.")
    else:
        lines.append("Next step: none; continue monitoring.")
    lines = compact_concise(lines)
else:
    lines = [
        header,
        f"Status: health {health_status} | pipeline {pipeline_status}",
        f"What happened: {summarize(reasons)}",
        f"Auto actions: {summarize(actions)}",
        recovery_state,
    ]
if message_mode != "concise":
    why_line = "Why it matters: unresolved pipeline health can block reliable readiness and execution signals."
    if effectiveness_summary:
      why_line = why_line.rstrip(".") + f"; {effectiveness_summary}."
    lines.append(why_line)
    lines.append(
        "Services: "
        f"shadow {humanize_phrase(shadow_service)}, "
        f"reporting timer {humanize_phrase(reporting_timer)}, "
        f"reporting service {humanize_phrase(reporting_service)}, "
        f"alpha worker {humanize_phrase(alpha_worker_service)}, "
        f"breadth worker {humanize_phrase(breadth_worker_service)}, "
        f"reporting start age seconds {reporting_activating_age}, "
        f"log maintenance timer {humanize_phrase(log_maintenance_timer)}, "
        f"log maintenance timer enabled {humanize_phrase(log_maintenance_timer_enabled)}, "
        f"log maintenance service {humanize_phrase(log_maintenance_service)}"
    )
    lines.append(
        "Freshness and scan: "
        f"pressure active {'yes' if freshness_pressure_active else 'no'}, "
        f"stale rate={freshness_stale_rate}, approval rate={freshness_approval_rate}, intents={freshness_intents_total}, "
        f"attempts (metar/settlement/shadow)={command_metar_attempts}/{command_settlement_attempts}/{command_shadow_attempts}, "
        f"scan={scan_effective_max_markets}->{scan_next_max_markets}, "
        f"action={humanize_phrase(scan_adaptive_action)}, reason={humanize_phrase(scan_adaptive_reason)}"
    )
    lines.append(
        "Log maintenance: "
        f"health={humanize_phrase(log_maintenance_health_status.upper(), default='UNKNOWN')}, "
        f"age seconds={log_maintenance_age_seconds}, usage bytes={log_maintenance_usage_bytes}"
    )
    if event_name:
        lines.append(f"Event file: {event_name}")
    lines.append("Next step: run check_temperature_shadow.sh --strict if this repeats.")
text = "\n".join(lines)

if event_file:
    event_path = Path(event_file)
    try:
        event_payload = json.loads(event_path.read_text(encoding="utf-8"))
    except Exception:
        event_payload = {}
    if not isinstance(event_payload, dict):
        event_payload = {}
    event_payload["discord_message_preview"] = text
    event_payload["discord_message_mode"] = message_mode
    event_payload["discord_message_generated_at"] = datetime.now(timezone.utc).isoformat()
    try:
        event_path.write_text(json.dumps(event_payload, indent=2), encoding="utf-8")
    except Exception:
        pass

if alert_state_path:
    try:
        alert_state_path.parent.mkdir(parents=True, exist_ok=True)
        alert_state_path.write_text(
            json.dumps({"last_fingerprint": fingerprint}, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass

print(json.dumps({"text": text, "content": text, "username": username}))
PY
)"
  if [[ -n "$payload_json" ]]; then
    curl --silent --show-error --fail \
      --max-time "$RECOVERY_WEBHOOK_TIMEOUT_SECONDS" \
      --header "Content-Type: application/json" \
      --data-binary "$payload_json" \
      "$RECOVERY_WEBHOOK_TARGET_URL" >/dev/null 2>&1 || true
  fi
fi

exit 0
