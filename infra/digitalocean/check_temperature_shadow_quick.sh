#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
STRICT_MODE=0
ENV_FILE="/etc/betbot/temperature-shadow.env"

usage() {
  cat <<'USAGE'
Usage:
  check_temperature_shadow_quick.sh [--strict] [--env <path>] [env_path]

Options:
  --strict      Enable strict mode (non-zero when quick_result has flags)
  --env <path>  Explicit env file path (default: /etc/betbot/temperature-shadow.env)
  -h, --help    Show this help message
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --strict)
      STRICT_MODE=1
      shift
      ;;
    --env)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --env" >&2
        exit 1
      fi
      ENV_FILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      ENV_FILE="$1"
      shift
      ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"
: "${OUTPUT_DIR:?OUTPUT_DIR not set in $ENV_FILE}"

LIVE_STATUS_FILE="$OUTPUT_DIR/health/live_status_latest.json"
ALPHA_SUMMARY_FILE="$OUTPUT_DIR/health/alpha_summary_latest.json"
if [[ ! -f "$ALPHA_SUMMARY_FILE" && -f "$OUTPUT_DIR/health/alpha_summary/alpha_summary_latest.json" ]]; then
  ALPHA_SUMMARY_FILE="$OUTPUT_DIR/health/alpha_summary/alpha_summary_latest.json"
fi
ROUTE_GUARD_FILE="$OUTPUT_DIR/health/discord_route_guard/discord_route_guard_latest.json"
DISCORD_AUDIT_FILE="$OUTPUT_DIR/health/discord_message_audit/discord_message_audit_latest.json"
DECISION_MATRIX_LANE_ALERT_STATE_FILE="${DECISION_MATRIX_LANE_ALERT_STATE_FILE:-$OUTPUT_DIR/health/.decision_matrix_lane_alert_state.json}"
COLDMATH_HARDENING_STATUS_FILE="${COLDMATH_HARDENING_STATUS_FILE:-$OUTPUT_DIR/health/coldmath_hardening_latest.json}"
RECOVERY_LATEST_FILE="${RECOVERY_LATEST_FILE:-$OUTPUT_DIR/health/recovery/recovery_latest.json}"

LIVE_STATUS_MAX_AGE="${LIVE_STATUS_STRICT_MAX_AGE_SECONDS:-300}"
ALPHA_SUMMARY_MAX_AGE="${ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS:-54000}"
ROUTE_GUARD_MAX_AGE="${DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS:-10800}"
DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD="${DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD:-6}"
DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES="${DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES:-matrix_failed,bootstrap_blocked}"
DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE="${DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE:-0}"
COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR="${COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR:-1}"
COLDMATH_HARDENING_ENABLED="${COLDMATH_HARDENING_ENABLED:-1}"
COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED="${COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED:-${COLDMATH_STAGE_TIMEOUT_GUARDRAILS_STRICT_REQUIRED:-1}}"
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
RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY="${RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY:-0}"

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

if ! [[ "$LIVE_STATUS_MAX_AGE" =~ ^[0-9]+$ ]]; then
  LIVE_STATUS_MAX_AGE=300
fi
if ! [[ "$ALPHA_SUMMARY_MAX_AGE" =~ ^[0-9]+$ ]]; then
  ALPHA_SUMMARY_MAX_AGE=54000
fi
if ! [[ "$ROUTE_GUARD_MAX_AGE" =~ ^[0-9]+$ ]]; then
  ROUTE_GUARD_MAX_AGE=10800
fi
DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE="$(normalize_binary_flag "$DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE" "0")"
COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR="$(normalize_binary_flag "$COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR" "1")"
COLDMATH_HARDENING_ENABLED="$(normalize_binary_flag "$COLDMATH_HARDENING_ENABLED" "1")"
COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED="$(normalize_binary_flag "$COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED" "1")"
COLDMATH_MARKET_INGEST_ENABLED="$(normalize_binary_flag "$COLDMATH_MARKET_INGEST_ENABLED" "1")"
COLDMATH_RECOVERY_ADVISOR_ENABLED="$(normalize_binary_flag "$COLDMATH_RECOVERY_ADVISOR_ENABLED" "1")"
COLDMATH_RECOVERY_LOOP_ENABLED="$(normalize_binary_flag "$COLDMATH_RECOVERY_LOOP_ENABLED" "1")"
COLDMATH_RECOVERY_CAMPAIGN_ENABLED="$(normalize_binary_flag "$COLDMATH_RECOVERY_CAMPAIGN_ENABLED" "1")"
RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY="$(normalize_binary_flag "$RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY" "0")"

SHADOW_SVC="betbot-temperature-shadow.service"
ALPHA_TIMER="betbot-temperature-alpha-summary.timer"
ROUTE_GUARD_TIMER="betbot-temperature-discord-route-guard.timer"

now_epoch="$(date +%s)"

file_age() {
  local p="$1"
  if [[ -f "$p" ]]; then
    local m
    m="$(date -r "$p" +%s 2>/dev/null || echo 0)"
    echo $(( now_epoch - m ))
  else
    echo -1
  fi
}

shadow_state="$(sudo systemctl is-active "$SHADOW_SVC" 2>/dev/null || true)"
alpha_timer_state="$(sudo systemctl is-active "$ALPHA_TIMER" 2>/dev/null || true)"
route_timer_state="$(sudo systemctl is-active "$ROUTE_GUARD_TIMER" 2>/dev/null || true)"
[[ -n "$shadow_state" ]] || shadow_state="unknown"
[[ -n "$alpha_timer_state" ]] || alpha_timer_state="unknown"
[[ -n "$route_timer_state" ]] || route_timer_state="unknown"

live_age="$(file_age "$LIVE_STATUS_FILE")"
alpha_age="$(file_age "$ALPHA_SUMMARY_FILE")"
route_age="$(file_age "$ROUTE_GUARD_FILE")"

echo "BetBot Quick Health — $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
echo "services: shadow=$shadow_state alpha_summary_timer=$alpha_timer_state route_guard_timer=$route_timer_state"

action_flags=()

if [[ -f "$LIVE_STATUS_FILE" ]]; then
  if live_line="$(python3 - "$LIVE_STATUS_FILE" 2>/dev/null <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
flags = p.get("trigger_flags") or {}
fresh = p.get("freshness_plan") or {}
scan = p.get("scan_budget") or {}
latest = p.get("latest_cycle_metrics") or {}
def _as_float(value: object, default: float = 0.0) -> float:
  if isinstance(value, bool):
    return float(int(value))
  try:
    return float(value)
  except Exception:
    return default
def _as_bool(value: object, default: bool = False) -> bool:
  if isinstance(value, bool):
    return value
  if isinstance(value, (int, float)):
    return value != 0
  if isinstance(value, str):
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
      return True
    if lowered in {"0", "false", "no", "off", ""}:
      return False
  return default
guardrail_status = str(fresh.get("approval_rate_guardrail_status") or "unknown")
guardrail_eval = _as_bool(fresh.get("approval_rate_guardrail_evaluated"), False)
cycle_approval_rate = _as_float(fresh.get("approval_rate", 0), 0.0)
cycle_stale_rate = _as_float(fresh.get("metar_observation_stale_rate", 0), 0.0)
guardrail_labels = {
  "within_band": "within band",
  "above_band": "above band",
  "critical_high": "critical high",
  "below_band": "below band",
  "insufficient_sample": "insufficient sample",
}
guardrail_text = guardrail_labels.get(guardrail_status, guardrail_status.replace("_", " "))
if not guardrail_eval:
  guardrail_text = f"{guardrail_text} (sample small)"
print(
  f"live: status={p.get('status','unknown')} approvals_resumed={flags.get('approvals_resumed')} "
  f"planned_orders_resumed={flags.get('planned_orders_resumed')} "
  f"cycle_approval_rate={cycle_approval_rate:.4f} "
  f"cycle_stale_rate={cycle_stale_rate:.4f} "
  f"guardrail={guardrail_text} "
  f"effective_max_markets={scan.get('effective_max_markets','n/a')} "
  f"latest_intents={latest.get('intents_approved',0)}/{latest.get('intents_total',0)} "
  f"latest_planned={latest.get('planned_orders',0)}"
)
PY
  )"; then
    echo "$live_line"
  else
    echo "live: parse_error ($LIVE_STATUS_FILE)"
    action_flags+=("live_status_parse_error")
  fi
else
  echo "live: missing ($LIVE_STATUS_FILE)"
  action_flags+=("live_status_missing")
fi

echo "artifact_age_sec: live_status=$live_age alpha_summary=$alpha_age discord_route_guard=$route_age"
if (( live_age < 0 || live_age > LIVE_STATUS_MAX_AGE )); then
  action_flags+=("live_status_stale")
fi
if (( alpha_age < 0 || alpha_age > ALPHA_SUMMARY_MAX_AGE )); then
  action_flags+=("alpha_summary_stale")
fi
if (( route_age < 0 || route_age > ROUTE_GUARD_MAX_AGE )); then
  action_flags+=("route_guard_stale")
fi

decision_matrix_lane_strict_statuses_normalized="$(printf '%s' "$DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
if [[ -z "$decision_matrix_lane_strict_statuses_normalized" ]]; then
  decision_matrix_lane_strict_statuses_normalized="matrix_failed,bootstrap_blocked"
fi
if [[ -f "$DECISION_MATRIX_LANE_ALERT_STATE_FILE" ]]; then
  lane_age="$(file_age "$DECISION_MATRIX_LANE_ALERT_STATE_FILE")"
  lane_line="$(python3 - "$DECISION_MATRIX_LANE_ALERT_STATE_FILE" "$decision_matrix_lane_strict_statuses_normalized" "$DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys


def _normalize(value: object) -> str:
    return str(value or "").strip().lower()


path = Path(sys.argv[1])
strict_statuses = {
    item.strip().lower()
    for item in str(sys.argv[2] or "").split(",")
    if item.strip()
}
threshold_raw = str(sys.argv[3] or "0").strip()
try:
    strict_threshold = max(0, int(float(threshold_raw)))
except Exception:
    strict_threshold = 0
parse_error = False
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    parse_error = True
    payload = {}
if not isinstance(payload, dict):
    parse_error = True
    payload = {}
status = _normalize(payload.get("last_lane_status")) or "unknown"
notify_reason = _normalize(payload.get("last_notify_reason")) or "none"
try:
    degraded_streak_count = max(0, int(float(payload.get("degraded_streak_count") or 0)))
except Exception:
    degraded_streak_count = 0
try:
    degraded_streak_threshold = max(0, int(float(payload.get("degraded_streak_threshold") or 0)))
except Exception:
    degraded_streak_threshold = 0
try:
    degraded_streak_notify_every = max(0, int(float(payload.get("degraded_streak_notify_every") or 0)))
except Exception:
    degraded_streak_notify_every = 0
status_match = status in strict_statuses if strict_statuses else False
strict_blocked = bool(
    strict_threshold > 0
    and status_match
    and degraded_streak_count >= strict_threshold
)
print(
    "decision_matrix_lane: "
    f"status={status} "
    f"degraded_streak={degraded_streak_count} "
    f"threshold={degraded_streak_threshold} "
    f"every={degraded_streak_notify_every} "
    f"notify_reason={notify_reason} "
    f"strict_statuses={','.join(sorted(strict_statuses)) or 'n/a'} "
    f"strict_threshold={strict_threshold} "
    f"strict_blocked={'true' if strict_blocked else 'false'} "
    f"parse_error={'true' if parse_error else 'false'}"
)
PY
)"
  echo "$lane_line age_sec=$lane_age"
  if [[ "$lane_line" == *"parse_error=true"* ]]; then
    echo "decision_matrix_lane: parse_error ($DECISION_MATRIX_LANE_ALERT_STATE_FILE)"
    if [[ "$DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE" == "1" ]]; then
      action_flags+=("decision_matrix_lane_state_parse_error")
    fi
  fi
  if [[ "$lane_line" == *"strict_blocked=true"* ]]; then
    action_flags+=("decision_matrix_lane_degraded_streak")
  fi
else
  echo "decision_matrix_lane: missing ($DECISION_MATRIX_LANE_ALERT_STATE_FILE)"
  if [[ "$DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE" == "1" ]]; then
    action_flags+=("decision_matrix_lane_state_missing")
  fi
fi

if [[ -f "$COLDMATH_HARDENING_STATUS_FILE" ]]; then
  persistence_line="$(python3 - "$COLDMATH_HARDENING_STATUS_FILE" "$COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys


def _normalize(value: object) -> str:
    return str(value or "").strip().lower()


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return _normalize(value) in {"1", "true", "yes", "on"}


path = Path(sys.argv[1])
strict_fail_on_error = _normalize(sys.argv[2]) in {"1", "true", "yes", "on"}
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
changed = _as_bool(recovery_env_persistence.get("changed"))
target = str(recovery_env_persistence.get("target_file") or "").strip()
error = str(recovery_env_persistence.get("error") or "").strip()
error_present = bool(error)
strict_blocked = bool(
    strict_fail_on_error and status in {"error", "execution_failed"}
)
print(
    "coldmath_recovery_env_persistence: "
    f"status={status} "
    f"changed={'true' if changed else 'false'} "
    f"error_present={'true' if error_present else 'false'} "
    f"target={target or 'n/a'} "
    f"parse_error={'true' if parse_error else 'false'} "
    f"strict_blocked={'true' if strict_blocked else 'false'}"
)
PY
)"
  echo "$persistence_line"
  if [[ "$persistence_line" == *"strict_blocked=true"* ]]; then
    action_flags+=("recovery_env_persistence_error")
  fi
else
  echo "coldmath_recovery_env_persistence: missing ($COLDMATH_HARDENING_STATUS_FILE)"
fi

if [[ -f "$RECOVERY_LATEST_FILE" ]]; then
  recovery_watchdog_line="$(python3 - "$RECOVERY_LATEST_FILE" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys


def _normalize(value: object) -> str:
    return str(value or "").strip().lower()


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return _normalize(value) in {"1", "true", "yes", "on"}


def _as_int(value: object, default: int = -1) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(value))
    except Exception:
        return default


def _latest_action_suffix(actions: list[str], prefix: str) -> str:
    for item in reversed(actions):
        text = str(item or "").strip()
        if text.startswith(prefix):
            return text[len(prefix):] or "unknown"
    return "none"


path = Path(sys.argv[1])
parse_error = False
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    parse_error = True
    payload = {}
if not isinstance(payload, dict):
    parse_error = True
    payload = {}
actions = payload.get("actions_attempted") if isinstance(payload.get("actions_attempted"), list) else []
actions = [str(item).strip() for item in actions if str(item).strip()]
recovery_effectiveness = (
    payload.get("recovery_effectiveness")
    if isinstance(payload.get("recovery_effectiveness"), dict)
    else {}
)
issue_detected = _as_bool(payload.get("issue_detected"))
issue_remaining = _as_bool(payload.get("issue_remaining"))
env_repair_action = _latest_action_suffix(actions, "repair_recovery_env_persistence_gate:")
hardening_trigger_action = _latest_action_suffix(actions, "trigger_coldmath_hardening_after_env_repair:")
stage_timeout_repair_action = _latest_action_suffix(actions, "repair_coldmath_stage_timeout_guardrails:")
stage_timeout_hardening_trigger_action = _latest_action_suffix(actions, "trigger_coldmath_hardening_after_stage_timeout_repair:")
strict_required = _as_bool(recovery_effectiveness.get("strict_required"))
gap_detected = _as_bool(recovery_effectiveness.get("gap_detected"))
gap_reason = _normalize(recovery_effectiveness.get("gap_reason")) or "none"
if gap_reason not in {"none", "summary_missing", "summary_stale"}:
    gap_reason = "none"
stale = _as_bool(recovery_effectiveness.get("stale"))
file_age_seconds = _as_int(recovery_effectiveness.get("file_age_seconds"), -1)
stale_threshold_seconds = _as_int(recovery_effectiveness.get("stale_threshold_seconds"), -1)
summary_available = _as_bool(recovery_effectiveness.get("summary_available"))
print(
    "recovery_watchdog: "
    f"issue_detected={'true' if issue_detected else 'false'} "
    f"issue_remaining={'true' if issue_remaining else 'false'} "
    f"env_repair_action={env_repair_action} "
    f"hardening_trigger_action={hardening_trigger_action} "
    f"stage_timeout_repair_action={stage_timeout_repair_action} "
    f"stage_timeout_hardening_trigger_action={stage_timeout_hardening_trigger_action} "
    f"strict_required={'true' if strict_required else 'false'} "
    f"gap_detected={'true' if gap_detected else 'false'} "
    f"gap_reason={gap_reason} "
    f"stale={'true' if stale else 'false'} "
    f"file_age_seconds={file_age_seconds} "
    f"stale_threshold_seconds={stale_threshold_seconds} "
    f"summary_available={'true' if summary_available else 'false'} "
    f"parse_error={'true' if parse_error else 'false'}"
)
PY
)"
  echo "$recovery_watchdog_line"
  if [[ "$recovery_watchdog_line" == *"parse_error=true"* ]]; then
    action_flags+=("recovery_watchdog_parse_error")
  fi
  if [[ "$recovery_watchdog_line" == *"hardening_trigger_action=failed"* ]]; then
    action_flags+=("recovery_coldmath_hardening_trigger_failed")
  fi
  if [[ "$recovery_watchdog_line" == *"hardening_trigger_action=missing_unit"* ]]; then
    action_flags+=("recovery_coldmath_hardening_trigger_missing_unit")
  fi
  if [[ "$recovery_watchdog_line" == *"stage_timeout_repair_action=failed"* ]]; then
    action_flags+=("recovery_coldmath_stage_timeout_repair_failed")
  fi
  if [[ "$recovery_watchdog_line" == *"stage_timeout_repair_action=missing_script"* ]]; then
    action_flags+=("recovery_coldmath_stage_timeout_repair_missing_script")
  fi
  if [[ "$recovery_watchdog_line" == *"stage_timeout_hardening_trigger_action=failed"* ]]; then
    action_flags+=("recovery_coldmath_stage_timeout_hardening_trigger_failed")
  fi
  if [[ "$recovery_watchdog_line" == *"stage_timeout_hardening_trigger_action=missing_unit"* ]]; then
    action_flags+=("recovery_coldmath_stage_timeout_hardening_trigger_missing_unit")
  fi
  if [[ "$recovery_watchdog_line" == *"strict_required=true"* && "$recovery_watchdog_line" == *"gap_detected=true"* ]]; then
    action_flags+=("recovery_effectiveness_gap_detected")
  fi
else
  echo "recovery_watchdog: missing ($RECOVERY_LATEST_FILE)"
  if [[ "$RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY" == "1" ]]; then
    action_flags+=("recovery_effectiveness_summary_missing")
  fi
fi

coldmath_stage_timeout_line="$(python3 - "$COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED" "$COLDMATH_HARDENING_ENABLED" "$COLDMATH_MARKET_INGEST_ENABLED" "$COLDMATH_RECOVERY_ADVISOR_ENABLED" "$COLDMATH_RECOVERY_LOOP_ENABLED" "$COLDMATH_RECOVERY_CAMPAIGN_ENABLED" "$COLDMATH_STAGE_TIMEOUT_SECONDS" "$COLDMATH_SNAPSHOT_TIMEOUT_SECONDS" "$COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS" "$COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS" "$COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS" "$COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS" <<'PY'
from __future__ import annotations

import re
import sys


def _normalize(value: object) -> str:
    return str(value or "").strip().lower()


def _is_int(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9]+", value))


strict_required = _normalize(sys.argv[1]) in {"1", "true", "yes", "on"}
hardening_enabled = _normalize(sys.argv[2]) in {"1", "true", "yes", "on"}
market_ingest_enabled = _normalize(sys.argv[3]) in {"1", "true", "yes", "on"}
recovery_advisor_enabled = _normalize(sys.argv[4]) in {"1", "true", "yes", "on"}
recovery_loop_enabled = _normalize(sys.argv[5]) in {"1", "true", "yes", "on"}
recovery_campaign_enabled = _normalize(sys.argv[6]) in {"1", "true", "yes", "on"}
global_timeout_raw = str(sys.argv[7] or "").strip()

required_stages: list[tuple[str, str]] = []
if hardening_enabled:
    required_stages.append(("snapshot", str(sys.argv[8] or "").strip()))
    if market_ingest_enabled:
        required_stages.append(("market_ingest", str(sys.argv[9] or "").strip()))
    if recovery_advisor_enabled:
        required_stages.append(("recovery_advisor", str(sys.argv[10] or "").strip()))
    if recovery_loop_enabled:
        required_stages.append(("recovery_loop", str(sys.argv[11] or "").strip()))
    if recovery_campaign_enabled:
        required_stages.append(("recovery_campaign", str(sys.argv[12] or "").strip()))

invalid_or_disabled: list[str] = []
for stage_name, stage_timeout_raw in required_stages:
    effective_raw = stage_timeout_raw if stage_timeout_raw else global_timeout_raw
    if not _is_int(effective_raw):
        invalid_or_disabled.append(stage_name)
        continue
    if int(effective_raw) <= 0:
        invalid_or_disabled.append(stage_name)

strict_blocked = bool(strict_required and invalid_or_disabled)
required_stage_labels = ",".join(stage_name for stage_name, _ in required_stages) or "none"
invalid_or_disabled_labels = ",".join(invalid_or_disabled) if invalid_or_disabled else "none"
print(
    "coldmath_stage_timeouts: "
    f"strict_required={'true' if strict_required else 'false'} "
    f"required_stages={required_stage_labels} "
    f"invalid_or_disabled_stages={invalid_or_disabled_labels} "
    f"strict_blocked={'true' if strict_blocked else 'false'}"
)
PY
)"
echo "$coldmath_stage_timeout_line"
if [[ "$coldmath_stage_timeout_line" == *"strict_blocked=true"* ]]; then
  action_flags+=("coldmath_stage_timeout_guardrails")
fi

if [[ -f "$ALPHA_SUMMARY_FILE" ]]; then
  if alpha_block="$(python3 - "$ALPHA_SUMMARY_FILE" 2>/dev/null <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
h=p.get('headline_metrics') or {}
tv=p.get('trader_view') or {}
def _as_float(value: object, default: float = 0.0) -> float:
  if isinstance(value, bool):
    return float(int(value))
  try:
    return float(value)
  except Exception:
    return default
def _as_int(value: object, default: int = 0) -> int:
  if isinstance(value, bool):
    return int(value)
  try:
    return int(float(value))
  except Exception:
    return default
deploy_conf = _as_float(tv.get('confidence_score'), 0.0)
selection_conf = _as_float(tv.get('selection_confidence_score') or h.get('selection_confidence_score'), 0.0)
projected = _as_float(h.get('projected_pnl_on_reference_bankroll_dollars'), 0.0)
approval_rate_12h = _as_float(h.get('approval_rate'), 0.0)
intents_total_12h = _as_int(h.get('intents_total'), 0)
intents_approved_12h = _as_int(h.get('intents_approved'), 0)
planned_12h = _as_int(h.get('planned_orders'), 0)
top_blocker = str(h.get('top_blocker_reason') or 'n/a').replace("_", " ")
impact_basis = str(h.get('suggestion_impact_pool_basis_label') or 'n/a').replace("_", " ")
print(
  f"alpha: health={h.get('health_status','unknown')} confidence={h.get('confidence_level','unknown')} "
  f"deploy_conf={deploy_conf:.1f}/100 selection_conf={selection_conf:.1f}/100 "
  f"flow12h={intents_approved_12h}/{intents_total_12h} ({approval_rate_12h:.2%}) planned12h={planned_12h} "
  f"top_blocker={top_blocker} "
  f"impact_basis={impact_basis} "
  f"settled_resolved={h.get('settled_unique_market_side_total', h.get('settled_unique_market_side_resolved_predictions',0))} "
  f"projected_pnl=${projected:.2f}"
)
if deploy_conf >= 55 and projected < 0:
    print("alpha_warning: confidence/pnl divergence (deploy_conf high but projected bankroll pnl is negative)")
PY
  )"; then
    if [[ -n "$alpha_block" ]]; then
      echo "$alpha_block"
    fi
    if [[ "$alpha_block" == *"alpha_warning: confidence/pnl divergence"* ]]; then
      action_flags+=("confidence_pnl_divergence")
    fi
  else
    echo "alpha: parse_error ($ALPHA_SUMMARY_FILE)"
    action_flags+=("alpha_summary_parse_error")
  fi
fi

if [[ -f "$ROUTE_GUARD_FILE" ]]; then
  if route_block="$(python3 - "$ROUTE_GUARD_FILE" 2>/dev/null <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
def _as_int(value: object, default: int = 0) -> int:
  if isinstance(value, bool):
    return int(value)
  try:
    return int(float(value))
  except Exception:
    return default
status=str(p.get('guard_status') or 'unknown')
shared=_as_int(p.get('shared_route_group_count'), 0)
keys=[]
for rem in p.get('route_remediations') or []:
  if not isinstance(rem, dict):
    continue
  thread_keys = rem.get('required_thread_env_keys')
  if not isinstance(thread_keys, list):
    continue
  for k in thread_keys:
    if k not in keys:
      keys.append(k)
print(f"discord_route_guard: status={status} shared_route_groups={shared} required_thread_keys={len(keys)}")
if keys:
  print("discord_route_guard_missing_keys_hint=" + ",".join(keys[:8]))
PY
  )"; then
    if [[ -n "$route_block" ]]; then
      echo "$route_block"
    fi
    if route_status="$(python3 - "$ROUTE_GUARD_FILE" 2>/dev/null <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
status = str(p.get('guard_status') or 'unknown')
print(status.replace("\r", "").strip().lower())
PY
    )"; then
      if [[ "$route_status" != "green" ]]; then
        action_flags+=("discord_route_guard_not_green")
      fi
    else
      echo "discord_route_guard: parse_error_status ($ROUTE_GUARD_FILE)"
      action_flags+=("discord_route_guard_parse_error")
    fi
  else
    echo "discord_route_guard: parse_error ($ROUTE_GUARD_FILE)"
    action_flags+=("discord_route_guard_parse_error")
  fi
fi

if [[ -f "$DISCORD_AUDIT_FILE" ]]; then
  if audit_block="$(python3 - "$DISCORD_AUDIT_FILE" 2>/dev/null <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
def _as_float(value: object, default: float = 0.0) -> float:
  if isinstance(value, bool):
    return float(int(value))
  try:
    return float(value)
  except Exception:
    return default
def _as_int(value: object, default: int = 0) -> int:
  if isinstance(value, bool):
    return int(value)
  try:
    return int(float(value))
  except Exception:
    return default
score=_as_float(p.get("overall_score"), 0.0)
streams=p.get("streams") if isinstance(p.get("streams"), list) else []
worst=min((_as_int(row.get("score"), 0) for row in streams if isinstance(row, dict)), default=0)
print(f"discord_message_audit: overall={score:.1f}/100 worst_stream={worst}/100 streams={len(streams)}")
if score < 90 or worst < 85:
    print("discord_message_audit_warning=readability_regression")
PY
  )"; then
    if [[ -n "$audit_block" ]]; then
      echo "$audit_block"
    fi
    if [[ "$audit_block" == *"discord_message_audit_warning=readability_regression"* ]]; then
      action_flags+=("discord_message_readability_regression")
    fi
  else
    echo "discord_message_audit: parse_error ($DISCORD_AUDIT_FILE)"
    action_flags+=("discord_message_audit_parse_error")
  fi
fi

if [[ -x "$SCRIPT_DIR/check_discord_thread_map.sh" ]]; then
  map_json="$(bash "$SCRIPT_DIR/check_discord_thread_map.sh" --env "$ENV_FILE" --json 2>/dev/null || true)"
  if [[ -n "$map_json" ]]; then
    map_line="$(python3 - "$map_json" <<'PY'
import json,sys
raw=(sys.argv[1] if len(sys.argv) > 1 else "").strip()
if not raw:
  print("thread_map: unavailable")
  raise SystemExit(0)
try:
  p=json.loads(raw)
except Exception:
  print("thread_map: parse_error")
  raise SystemExit(0)
def _as_bool(value: object) -> bool:
  if isinstance(value, bool):
    return value
  if isinstance(value, (int, float)):
    return value != 0
  if isinstance(value, str):
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
      return True
    if lowered in {"0", "false", "no", "off", ""}:
      return False
  return bool(value)
def _as_int(value: object, default: int = 0) -> int:
  if isinstance(value, bool):
    return int(value)
  try:
    return int(float(value))
  except Exception:
    return default
def _as_list(value: object) -> list[object]:
  return value if isinstance(value, list) else []

ready=_as_bool(p.get('ready_for_apply', p.get('can_apply', False)))
shared_groups=_as_int(p.get('route_guard_shared_route_group_count'), 0)
missing_map=_as_list(p.get('missing_required_in_map'))
missing_env=_as_list(p.get('missing_required_in_env'))
print(f"thread_map: ready_for_apply={str(ready).lower()} shared_route_groups={shared_groups} missing_map={len(missing_map)} missing_env={len(missing_env)}")
PY
)"
    echo "$map_line"
    thread_map_needs_action="$(python3 - "$map_json" <<'PY'
import json,sys
raw=(sys.argv[1] if len(sys.argv) > 1 else "").strip()
try:
  p=json.loads(raw)
except Exception:
  print("0")
  raise SystemExit(0)
def _as_bool(value: object) -> bool:
  if isinstance(value, bool):
    return value
  if isinstance(value, (int, float)):
    return value != 0
  if isinstance(value, str):
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
      return True
    if lowered in {"0", "false", "no", "off", ""}:
      return False
  return bool(value)
def _as_int(value: object, default: int = 0) -> int:
  if isinstance(value, bool):
    return int(value)
  try:
    return int(float(value))
  except Exception:
    return default

shared=_as_int(p.get("route_guard_shared_route_group_count"), 0)
ready=_as_bool(p.get("ready_for_apply", p.get("can_apply", False)))
print("1" if (shared > 0 and not ready) else "0")
PY
)"
    if [[ "$thread_map_needs_action" == "1" ]]; then
      action_flags+=("thread_map_incomplete")
    fi
  fi
fi

if (( ${#action_flags[@]} == 0 )); then
  echo "quick_result: GREEN"
else
  echo "quick_result: YELLOW flags=$(IFS=,; echo "${action_flags[*]}")"
  if printf '%s\n' "${action_flags[@]}" | grep -q '^thread_map_incomplete$'; then
    echo "next_action: fill /etc/betbot/discord-thread-map.env then run:"
    echo "  sudo bash $SCRIPT_DIR/check_discord_thread_map.sh --env $ENV_FILE --strict --apply"
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^recovery_env_persistence_error$'; then
    echo "next_action: repair coldmath recovery persistence strict gate then re-run checks:"
    echo "  bash $SCRIPT_DIR/set_coldmath_recovery_env_persistence_gate.sh --enable $ENV_FILE"
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^recovery_effectiveness_gap_detected$'; then
    echo "next_action: recovery effectiveness strict gap detected; run pipeline recovery now:"
    echo "  sudo bash $SCRIPT_DIR/run_temperature_pipeline_recovery.sh $ENV_FILE"
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^recovery_effectiveness_summary_missing$'; then
    echo "next_action: recovery effectiveness summary missing; run pipeline recovery and inspect recovery_latest.json:"
    echo "  sudo bash $SCRIPT_DIR/run_temperature_pipeline_recovery.sh $ENV_FILE"
    echo "  cat $RECOVERY_LATEST_FILE"
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^recovery_coldmath_hardening_trigger_failed$'; then
    echo "next_action: coldmath hardening trigger failed; check service logs and retry recovery:"
    echo "  sudo journalctl -u betbot-temperature-coldmath-hardening.service -n 80 --no-pager"
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^recovery_coldmath_hardening_trigger_missing_unit$'; then
    echo "next_action: install/enable coldmath hardening service unit then re-run recovery:"
    echo "  bash $SCRIPT_DIR/install_systemd_temperature_coldmath_hardening.sh"
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^recovery_coldmath_stage_timeout_hardening_trigger_failed$'; then
    echo "next_action: coldmath hardening trigger failed after stage-timeout repair; check service logs and retry recovery:"
    echo "  sudo journalctl -u betbot-temperature-coldmath-hardening.service -n 80 --no-pager"
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^recovery_coldmath_stage_timeout_hardening_trigger_missing_unit$'; then
    echo "next_action: install/enable coldmath hardening service unit for stage-timeout recovery then re-run recovery:"
    echo "  bash $SCRIPT_DIR/install_systemd_temperature_coldmath_hardening.sh"
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^recovery_coldmath_stage_timeout_repair_failed$'; then
    echo "next_action: coldmath stage-timeout guardrail repair failed; check recovery logs and retry recovery:"
    echo "  sudo journalctl -u betbot-temperature-recovery.service -n 80 --no-pager"
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^recovery_coldmath_stage_timeout_repair_missing_script$'; then
    echo "next_action: coldmath stage-timeout guardrail repair script missing; restore script path and retry recovery:"
    echo "  bash $SCRIPT_DIR/set_coldmath_stage_timeout_guardrails.sh --global-seconds 900 --snapshot-seconds 900 --market-ingest-seconds 900 --advisor-seconds 600 --loop-seconds 900 --campaign-seconds 1200 \"$ENV_FILE\""
  fi
  if printf '%s\n' "${action_flags[@]}" | grep -q '^coldmath_stage_timeout_guardrails$'; then
    echo "next_action: set coldmath stage timeout guardrails then re-run quick check:"
    echo "  bash $SCRIPT_DIR/set_coldmath_stage_timeout_guardrails.sh --global-seconds 900 --snapshot-seconds 900 --market-ingest-seconds 900 --advisor-seconds 600 --loop-seconds 900 --campaign-seconds 1200 \"$ENV_FILE\""
  fi
fi

if (( STRICT_MODE == 1 )) && (( ${#action_flags[@]} > 0 )); then
  exit 2
fi
exit 0
