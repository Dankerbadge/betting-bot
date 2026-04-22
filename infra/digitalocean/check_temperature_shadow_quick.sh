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

LIVE_STATUS_MAX_AGE="${LIVE_STATUS_STRICT_MAX_AGE_SECONDS:-300}"
ALPHA_SUMMARY_MAX_AGE="${ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS:-54000}"
ROUTE_GUARD_MAX_AGE="${DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS:-10800}"
DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD="${DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD:-6}"
DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES="${DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES:-matrix_failed,bootstrap_blocked}"
DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE="${DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE:-0}"

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
  live_line="$(python3 - "$LIVE_STATUS_FILE" <<'PY'
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
)"
  echo "$live_line"
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
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    payload = {}
if not isinstance(payload, dict):
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
    f"strict_blocked={'true' if strict_blocked else 'false'}"
)
PY
)"
  echo "$lane_line age_sec=$lane_age"
  if [[ "$lane_line" == *"strict_blocked=true"* ]]; then
    action_flags+=("decision_matrix_lane_degraded_streak")
  fi
else
  echo "decision_matrix_lane: missing ($DECISION_MATRIX_LANE_ALERT_STATE_FILE)"
  if [[ "$DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE" == "1" ]]; then
    action_flags+=("decision_matrix_lane_state_missing")
  fi
fi

if [[ -f "$ALPHA_SUMMARY_FILE" ]]; then
  alpha_block="$(python3 - "$ALPHA_SUMMARY_FILE" <<'PY'
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
)"
  if [[ -n "$alpha_block" ]]; then
    echo "$alpha_block"
  fi
  if [[ "$alpha_block" == *"alpha_warning: confidence/pnl divergence"* ]]; then
    action_flags+=("confidence_pnl_divergence")
  fi
fi

if [[ -f "$ROUTE_GUARD_FILE" ]]; then
  route_block="$(python3 - "$ROUTE_GUARD_FILE" <<'PY'
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
)"
  if [[ -n "$route_block" ]]; then
    echo "$route_block"
  fi
  route_status="$(python3 - "$ROUTE_GUARD_FILE" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]))
status = str(p.get('guard_status') or 'unknown')
print(status.replace("\r", "").strip().lower())
PY
)"
  if [[ "$route_status" != "green" ]]; then
    action_flags+=("discord_route_guard_not_green")
  fi
fi

if [[ -f "$DISCORD_AUDIT_FILE" ]]; then
  audit_block="$(python3 - "$DISCORD_AUDIT_FILE" <<'PY'
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
)"
  if [[ -n "$audit_block" ]]; then
    echo "$audit_block"
  fi
  if [[ "$audit_block" == *"discord_message_audit_warning=readability_regression"* ]]; then
    action_flags+=("discord_message_readability_regression")
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
fi

if (( STRICT_MODE == 1 )) && (( ${#action_flags[@]} > 0 )); then
  exit 2
fi
exit 0
