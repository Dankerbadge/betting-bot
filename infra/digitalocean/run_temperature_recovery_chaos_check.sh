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

RECOVERY_SCRIPT="${RECOVERY_SCRIPT:-$BETBOT_ROOT/infra/digitalocean/run_temperature_pipeline_recovery.sh}"
if [[ ! -x "$RECOVERY_SCRIPT" ]]; then
  echo "missing executable recovery script: $RECOVERY_SCRIPT" >&2
  exit 1
fi

RECOVERY_CHAOS_DIR="${RECOVERY_CHAOS_DIR:-$OUTPUT_DIR/health/recovery}"
RECOVERY_CHAOS_LOG_FILE="${RECOVERY_CHAOS_LOG_FILE:-$OUTPUT_DIR/logs/pipeline_recovery_chaos.log}"
RECOVERY_CHAOS_LATEST_FILE="${RECOVERY_CHAOS_LATEST_FILE:-$RECOVERY_CHAOS_DIR/chaos_check_latest.json}"
RECOVERY_CHAOS_LOCK_FILE="${RECOVERY_CHAOS_LOCK_FILE:-/tmp/betbot-temperature-recovery-chaos.lock}"
RECOVERY_CHAOS_MAX_EVENT_FILES="${RECOVERY_CHAOS_MAX_EVENT_FILES:-120}"
RECOVERY_CHAOS_STOP_SECONDS="${RECOVERY_CHAOS_STOP_SECONDS:-2}"
RECOVERY_CHAOS_WAIT_RECOVER_SECONDS="${RECOVERY_CHAOS_WAIT_RECOVER_SECONDS:-120}"
RECOVERY_CHAOS_WORKER_WAIT_RECOVER_SECONDS="${RECOVERY_CHAOS_WORKER_WAIT_RECOVER_SECONDS:-90}"
RECOVERY_CHAOS_ENABLE_WORKER_DRILLS="${RECOVERY_CHAOS_ENABLE_WORKER_DRILLS:-1}"
RECOVERY_CHAOS_NOTIFY_ON_PASS="${RECOVERY_CHAOS_NOTIFY_ON_PASS:-0}"
RECOVERY_CHAOS_WEBHOOK_URL="${RECOVERY_CHAOS_WEBHOOK_URL:-${RECOVERY_WEBHOOK_URL:-${ALERT_WEBHOOK_URL:-}}}"
RECOVERY_CHAOS_WEBHOOK_THREAD_ID="${RECOVERY_CHAOS_WEBHOOK_THREAD_ID:-${RECOVERY_WEBHOOK_THREAD_ID:-${ALERT_WEBHOOK_THREAD_ID:-}}}"
RECOVERY_CHAOS_WEBHOOK_TIMEOUT_SECONDS="${RECOVERY_CHAOS_WEBHOOK_TIMEOUT_SECONDS:-5}"
RECOVERY_CHAOS_WEBHOOK_MESSAGE_MODE="${RECOVERY_CHAOS_WEBHOOK_MESSAGE_MODE:-concise}"
RECOVERY_CHAOS_WEBHOOK_USERNAME="${RECOVERY_CHAOS_WEBHOOK_USERNAME:-BetBot Recovery Drill}"
ALPHA_WORKER_ENABLED="${ALPHA_WORKER_ENABLED:-0}"
BREADTH_WORKER_ENABLED="${BREADTH_WORKER_ENABLED:-0}"

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

RECOVERY_CHAOS_WEBHOOK_TARGET_URL="$(build_discord_target_url "$RECOVERY_CHAOS_WEBHOOK_URL" "$RECOVERY_CHAOS_WEBHOOK_THREAD_ID")"

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs" "$RECOVERY_CHAOS_DIR"

log_line() {
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[$ts] $*" >> "$RECOVERY_CHAOS_LOG_FILE"
}

service_state() {
  local unit="$1"
  local state
  state="$(systemctl is-active "$unit" 2>/dev/null || true)"
  state="$(echo "$state" | tr -d '\r' | tr '\n' ' ' | xargs)"
  [[ -n "$state" ]] || state="unknown"
  echo "$state"
}

run_forced_recovery() {
  if RECOVERY_SHADOW_RESTART_COOLDOWN_SECONDS=0 \
    RECOVERY_ALPHA_WORKER_RESTART_COOLDOWN_SECONDS=0 \
    RECOVERY_BREADTH_WORKER_RESTART_COOLDOWN_SECONDS=0 \
    RECOVERY_METAR_REFRESH_COOLDOWN_SECONDS=0 \
    RECOVERY_SETTLEMENT_REFRESH_COOLDOWN_SECONDS=0 \
    RECOVERY_REPORTING_TRIGGER_COOLDOWN_SECONDS=0 \
    "$RECOVERY_SCRIPT" "$ENV_FILE" >> "$RECOVERY_CHAOS_LOG_FILE" 2>&1; then
    return 0
  fi
  return $?
}

perform_worker_drill() {
  local unit="$1"
  local label="$2"
  local pre_state post_state recovery_rc wait_seconds emergency_restart skipped pass_flag failure_reason

  pre_state="$(service_state "$unit")"
  post_state="$pre_state"
  recovery_rc=0
  wait_seconds=0
  emergency_restart=0
  skipped=0
  pass_flag=1
  failure_reason=""

  if [[ "$pre_state" != "active" ]]; then
    skipped=1
    pass_flag=0
    failure_reason="baseline_not_active"
  else
    systemctl stop "$unit" >/dev/null 2>&1 || true
    sleep "$RECOVERY_CHAOS_STOP_SECONDS"

    if run_forced_recovery; then
      recovery_rc=0
    else
      recovery_rc=$?
      pass_flag=0
      failure_reason="recovery_script_failed"
    fi

    post_state="$(service_state "$unit")"
    if [[ "$post_state" != "active" ]]; then
      for _ in $(seq 1 "$RECOVERY_CHAOS_WORKER_WAIT_RECOVER_SECONDS"); do
        sleep 1
        wait_seconds=$((wait_seconds + 1))
        post_state="$(service_state "$unit")"
        if [[ "$post_state" == "active" ]]; then
          break
        fi
      done
    fi

    if [[ "$post_state" != "active" ]]; then
      emergency_restart=1
      systemctl restart "$unit" >/dev/null 2>&1 || true
      sleep 2
      post_state="$(service_state "$unit")"
    fi

    if [[ "$post_state" != "active" ]]; then
      pass_flag=0
      if [[ -n "$failure_reason" ]]; then
        failure_reason="${failure_reason}+worker_not_recovered"
      else
        failure_reason="worker_not_recovered"
      fi
    fi
  fi

  echo "${label}:pass=${pass_flag},pre=${pre_state},post=${post_state},recovery_exit=${recovery_rc},wait=${wait_seconds},emergency=${emergency_restart},skipped=${skipped},failure=${failure_reason:-none}"
  if [[ "$pass_flag" == "1" ]]; then
    return 0
  fi
  return 1
}

write_chaos_artifact() {
  local mode="$1"
  local pass_flag="$2"
  local pre_shadow_state="$3"
  local post_shadow_state="$4"
  local recovery_exit_code="$5"
  local disruption_seconds="$6"
  local recovered_wait_seconds="$7"
  local emergency_restart_attempted="$8"
  local failure_reason="$9"
  local decision_notes="${10}"
  local alpha_worker_drill="${11}"
  local breadth_worker_drill="${12}"
  local recovery_effectiveness_strict_required="${13:-0}"
  local recovery_effectiveness_gap_detected="${14:-0}"
  local recovery_effectiveness_gap_reason="${15:-none}"
  local recovery_effectiveness_stale="${16:-0}"
  local recovery_effectiveness_file_age_seconds="${17:--1}"
  local recovery_effectiveness_stale_threshold_seconds="${18:--1}"
  local recovery_effectiveness_summary_available="${19:-0}"
  local recovery_effectiveness_demoted_action_count="${20:-0}"
  local recovery_effectiveness_harmful_action_count="${21:-0}"

  "$PYTHON_BIN" - "$RECOVERY_CHAOS_DIR" "$RECOVERY_CHAOS_LATEST_FILE" "$RECOVERY_CHAOS_MAX_EVENT_FILES" "$mode" "$pass_flag" "$pre_shadow_state" "$post_shadow_state" "$recovery_exit_code" "$disruption_seconds" "$recovered_wait_seconds" "$emergency_restart_attempted" "$failure_reason" "$decision_notes" "$alpha_worker_drill" "$breadth_worker_drill" "$recovery_effectiveness_strict_required" "$recovery_effectiveness_gap_detected" "$recovery_effectiveness_gap_reason" "$recovery_effectiveness_stale" "$recovery_effectiveness_file_age_seconds" "$recovery_effectiveness_stale_threshold_seconds" "$recovery_effectiveness_summary_available" "$recovery_effectiveness_demoted_action_count" "$recovery_effectiveness_harmful_action_count" <<'PY'
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


def parse_bool(value: str, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return default


def split_notes(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split("||") if item.strip()]


out_dir = Path(sys.argv[1])
latest_file = Path(sys.argv[2])
max_events = max(1, parse_int(sys.argv[3], 120))
mode = str(sys.argv[4] or "unknown").strip()
passed = parse_bool(sys.argv[5], False)
pre_shadow_state = str(sys.argv[6] or "unknown").strip()
post_shadow_state = str(sys.argv[7] or "unknown").strip()
recovery_exit_code = parse_int(sys.argv[8], -1)
disruption_seconds = max(0, parse_int(sys.argv[9], 0))
recovered_wait_seconds = max(0, parse_int(sys.argv[10], 0))
emergency_restart_attempted = parse_bool(sys.argv[11], False)
failure_reason = str(sys.argv[12] or "").strip()
notes = split_notes(sys.argv[13])
alpha_worker_drill = str(sys.argv[14] or "").strip()
breadth_worker_drill = str(sys.argv[15] or "").strip()
recovery_effectiveness_strict_required = parse_bool(sys.argv[16], False)
recovery_effectiveness_gap_detected = parse_bool(sys.argv[17], False)
recovery_effectiveness_gap_reason = str(sys.argv[18] or "none").strip().lower() or "none"
if recovery_effectiveness_gap_reason not in {"none", "summary_missing", "summary_stale"}:
    recovery_effectiveness_gap_reason = "none"
recovery_effectiveness_stale = parse_bool(sys.argv[19], False)
recovery_effectiveness_file_age_seconds = parse_int(sys.argv[20], -1)
recovery_effectiveness_stale_threshold_seconds = parse_int(sys.argv[21], -1)
recovery_effectiveness_summary_available = parse_bool(sys.argv[22], False)
recovery_effectiveness_demoted_action_count = max(0, parse_int(sys.argv[23], 0))
recovery_effectiveness_harmful_action_count = max(0, parse_int(sys.argv[24], 0))

captured_at = datetime.now(timezone.utc).isoformat()
payload = {
    "status": "ready",
    "event": "pipeline_recovery_chaos_check",
    "captured_at": captured_at,
    "mode": mode,
    "passed": passed,
    "pre_shadow_state": pre_shadow_state,
    "post_shadow_state": post_shadow_state,
    "recovery_exit_code": recovery_exit_code,
    "disruption_seconds": disruption_seconds,
    "recovered_wait_seconds": recovered_wait_seconds,
    "emergency_restart_attempted": emergency_restart_attempted,
    "failure_reason": failure_reason,
    "notes": notes,
    "alpha_worker_drill": alpha_worker_drill,
    "breadth_worker_drill": breadth_worker_drill,
    "recovery_effectiveness": {
        "strict_required": recovery_effectiveness_strict_required,
        "gap_detected": recovery_effectiveness_gap_detected,
        "gap_reason": recovery_effectiveness_gap_reason,
        "stale": recovery_effectiveness_stale,
        "file_age_seconds": recovery_effectiveness_file_age_seconds,
        "stale_threshold_seconds": recovery_effectiveness_stale_threshold_seconds,
        "summary_available": recovery_effectiveness_summary_available,
        "demoted_action_count": recovery_effectiveness_demoted_action_count,
        "harmful_action_count": recovery_effectiveness_harmful_action_count,
    },
}

out_dir.mkdir(parents=True, exist_ok=True)
latest_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
event_file = out_dir / f"chaos_check_{stamp}.json"
event_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

events = sorted(out_dir.glob("chaos_check_*.json"))
if len(events) > max_events:
    for path in events[: len(events) - max_events]:
        try:
            path.unlink()
        except OSError:
            pass

print(event_file)
PY
}

exec 9>"$RECOVERY_CHAOS_LOCK_FILE"
if ! flock -n 9; then
  notes="lock_contended_skip"
  event_path="$(write_chaos_artifact "skipped_lock_contended" "0" "unknown" "$(service_state betbot-temperature-shadow.service)" "-1" "0" "0" "0" "chaos_lock_contended" "$notes" "" "")"
  log_line "chaos_check skipped reason=lock_contended event=$event_path"
  exit 0
fi

ALLOW_LIVE_ORDERS="${ALLOW_LIVE_ORDERS:-0}"
if [[ "$ALLOW_LIVE_ORDERS" == "1" ]]; then
  notes="skip_live_orders_enabled"
  event_path="$(write_chaos_artifact "skipped_live_enabled" "0" "$(service_state betbot-temperature-shadow.service)" "$(service_state betbot-temperature-shadow.service)" "-1" "0" "0" "0" "live_orders_enabled" "$notes" "" "")"
  log_line "chaos_check skipped reason=live_orders_enabled event=$event_path"
  exit 0
fi

if [[ ! "$RECOVERY_CHAOS_STOP_SECONDS" =~ ^[0-9]+$ ]]; then
  RECOVERY_CHAOS_STOP_SECONDS=2
fi
if [[ ! "$RECOVERY_CHAOS_WAIT_RECOVER_SECONDS" =~ ^[0-9]+$ ]]; then
  RECOVERY_CHAOS_WAIT_RECOVER_SECONDS=120
fi
if [[ ! "$RECOVERY_CHAOS_WORKER_WAIT_RECOVER_SECONDS" =~ ^[0-9]+$ ]]; then
  RECOVERY_CHAOS_WORKER_WAIT_RECOVER_SECONDS=90
fi

pre_shadow_state="$(service_state betbot-temperature-shadow.service)"
notes=()
notes+=("pre_shadow_state=$pre_shadow_state")
mode="disruption_test"

emergency_restart_attempted=0
failure_reason=""
recovery_exit_code=0
recovered_wait_seconds=0
disruption_seconds=0
recovery_effectiveness_strict_required=0
recovery_effectiveness_gap_detected=0
recovery_effectiveness_gap_reason="none"
recovery_effectiveness_stale=0
recovery_effectiveness_file_age_seconds=-1
recovery_effectiveness_stale_threshold_seconds=-1
recovery_effectiveness_summary_available=0
recovery_effectiveness_demoted_action_count=0
recovery_effectiveness_harmful_action_count=0

if [[ "$pre_shadow_state" != "active" ]]; then
  mode="baseline_recovery_only"
  notes+=("baseline_shadow_not_active")
else
  stop_start_epoch="$(date +%s)"
  systemctl stop betbot-temperature-shadow.service >/dev/null 2>&1 || true
  sleep "$RECOVERY_CHAOS_STOP_SECONDS"
  disruption_seconds="$(( $(date +%s) - stop_start_epoch ))"
fi

if run_forced_recovery; then
  recovery_exit_code=0
else
  recovery_exit_code=$?
  failure_reason="recovery_script_failed"
  notes+=("recovery_script_exit=$recovery_exit_code")
fi
notes+=("drill_forced_recovery_cooldown_zero")

recovery_latest_file="$OUTPUT_DIR/health/recovery/recovery_latest.json"
if effectiveness_snapshot="$("$PYTHON_BIN" - "$recovery_latest_file" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys


def parse_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return default


def parse_int(value: object, default: int = 0) -> int:
    try:
        return int(float(str(value)))
    except Exception:
        return default


def as_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    return {}


latest_path = Path(sys.argv[1])
parse_status = "missing"
recovery_effectiveness: dict[str, object] = {}
if latest_path.exists():
    try:
        raw = json.loads(latest_path.read_text(encoding="utf-8"))
        payload = as_dict(raw)
        recovery_effectiveness = as_dict(payload.get("recovery_effectiveness"))
        parse_status = "parsed"
    except Exception:
        parse_status = "malformed"

strict_required = parse_bool(recovery_effectiveness.get("strict_required"), False)
gap_detected = parse_bool(recovery_effectiveness.get("gap_detected"), False)
gap_reason = str(recovery_effectiveness.get("gap_reason") or "none").strip().lower() or "none"
if gap_reason not in {"none", "summary_missing", "summary_stale"}:
    gap_reason = "none"
stale = parse_bool(recovery_effectiveness.get("stale"), False)
file_age_seconds = parse_int(recovery_effectiveness.get("file_age_seconds"), -1)
stale_threshold_seconds = parse_int(recovery_effectiveness.get("stale_threshold_seconds"), -1)
summary_available = parse_bool(recovery_effectiveness.get("summary_available"), False)
demoted_action_count = max(0, parse_int(recovery_effectiveness.get("demoted_action_count"), 0))
harmful_action_count = max(0, parse_int(recovery_effectiveness.get("harmful_action_count"), 0))

print(f"PARSE_STATUS={parse_status}")
print(f"STRICT_REQUIRED={1 if strict_required else 0}")
print(f"GAP_DETECTED={1 if gap_detected else 0}")
print(f"GAP_REASON={gap_reason}")
print(f"STALE={1 if stale else 0}")
print(f"FILE_AGE_SECONDS={file_age_seconds}")
print(f"STALE_THRESHOLD_SECONDS={stale_threshold_seconds}")
print(f"SUMMARY_AVAILABLE={1 if summary_available else 0}")
print(f"DEMOTED_ACTION_COUNT={demoted_action_count}")
print(f"HARMFUL_ACTION_COUNT={harmful_action_count}")
PY
)"; then
  recovery_effectiveness_parse_status="unknown"
  while IFS='=' read -r key value; do
    case "$key" in
      PARSE_STATUS) recovery_effectiveness_parse_status="$value" ;;
      STRICT_REQUIRED) recovery_effectiveness_strict_required="$value" ;;
      GAP_DETECTED) recovery_effectiveness_gap_detected="$value" ;;
      GAP_REASON) recovery_effectiveness_gap_reason="$value" ;;
      STALE) recovery_effectiveness_stale="$value" ;;
      FILE_AGE_SECONDS) recovery_effectiveness_file_age_seconds="$value" ;;
      STALE_THRESHOLD_SECONDS) recovery_effectiveness_stale_threshold_seconds="$value" ;;
      SUMMARY_AVAILABLE) recovery_effectiveness_summary_available="$value" ;;
      DEMOTED_ACTION_COUNT) recovery_effectiveness_demoted_action_count="$value" ;;
      HARMFUL_ACTION_COUNT) recovery_effectiveness_harmful_action_count="$value" ;;
    esac
  done <<< "$effectiveness_snapshot"
  notes+=("recovery_effectiveness_parse_status=$recovery_effectiveness_parse_status")
else
  notes+=("recovery_effectiveness_parse_status=error")
fi
if [[ ! "$recovery_effectiveness_strict_required" =~ ^[01]$ ]]; then
  recovery_effectiveness_strict_required=0
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
if [[ ! "$recovery_effectiveness_stale" =~ ^[01]$ ]]; then
  recovery_effectiveness_stale=0
fi
if [[ ! "$recovery_effectiveness_file_age_seconds" =~ ^-?[0-9]+$ ]]; then
  recovery_effectiveness_file_age_seconds=-1
fi
if [[ ! "$recovery_effectiveness_stale_threshold_seconds" =~ ^-?[0-9]+$ ]]; then
  recovery_effectiveness_stale_threshold_seconds=-1
fi
if [[ ! "$recovery_effectiveness_summary_available" =~ ^[01]$ ]]; then
  recovery_effectiveness_summary_available=0
fi
if [[ ! "$recovery_effectiveness_demoted_action_count" =~ ^[0-9]+$ ]]; then
  recovery_effectiveness_demoted_action_count=0
fi
if [[ ! "$recovery_effectiveness_harmful_action_count" =~ ^[0-9]+$ ]]; then
  recovery_effectiveness_harmful_action_count=0
fi
notes+=("recovery_effectiveness_strict_required=$recovery_effectiveness_strict_required")
notes+=("recovery_effectiveness_gap_detected=$recovery_effectiveness_gap_detected")
notes+=("recovery_effectiveness_gap_reason=$recovery_effectiveness_gap_reason")
notes+=("recovery_effectiveness_stale=$recovery_effectiveness_stale")
notes+=("recovery_effectiveness_file_age_seconds=$recovery_effectiveness_file_age_seconds")
notes+=("recovery_effectiveness_stale_threshold_seconds=$recovery_effectiveness_stale_threshold_seconds")
notes+=("recovery_effectiveness_summary_available=$recovery_effectiveness_summary_available")
notes+=("recovery_effectiveness_demoted_action_count=$recovery_effectiveness_demoted_action_count")
notes+=("recovery_effectiveness_harmful_action_count=$recovery_effectiveness_harmful_action_count")

post_shadow_state="$(service_state betbot-temperature-shadow.service)"
if [[ "$post_shadow_state" != "active" ]]; then
  for _ in $(seq 1 "$RECOVERY_CHAOS_WAIT_RECOVER_SECONDS"); do
    sleep 1
    recovered_wait_seconds=$((recovered_wait_seconds + 1))
    post_shadow_state="$(service_state betbot-temperature-shadow.service)"
    if [[ "$post_shadow_state" == "active" ]]; then
      break
    fi
  done
fi

if [[ "$post_shadow_state" != "active" ]]; then
  emergency_restart_attempted=1
  notes+=("emergency_restart_attempted")
  systemctl restart betbot-temperature-shadow.service >/dev/null 2>&1 || true
  sleep 2
  post_shadow_state="$(service_state betbot-temperature-shadow.service)"
fi

pass_flag=1
if [[ "$recovery_exit_code" -ne 0 ]]; then
  pass_flag=0
  [[ -n "$failure_reason" ]] || failure_reason="recovery_script_failed"
fi
if [[ "$post_shadow_state" != "active" ]]; then
  pass_flag=0
  if [[ -n "$failure_reason" ]]; then
    failure_reason="${failure_reason}+shadow_not_recovered"
  else
    failure_reason="shadow_not_recovered"
  fi
fi
if [[ "$recovery_effectiveness_strict_required" == "1" && "$recovery_effectiveness_gap_detected" == "1" ]]; then
  pass_flag=0
  notes+=("recovery_effectiveness_gap_detected")
  if [[ -n "$failure_reason" ]]; then
    failure_reason="${failure_reason}+recovery_effectiveness_gap_detected"
  else
    failure_reason="recovery_effectiveness_gap_detected"
  fi
fi

alpha_worker_drill_summary=""
breadth_worker_drill_summary=""
if [[ "$RECOVERY_CHAOS_ENABLE_WORKER_DRILLS" == "1" && "$ALPHA_WORKER_ENABLED" == "1" ]]; then
  if alpha_worker_drill_summary="$(perform_worker_drill betbot-temperature-alpha-workers.service alpha_worker)"; then
    notes+=("alpha_worker_drill_pass")
  else
    pass_flag=0
    notes+=("alpha_worker_drill_fail")
    if [[ -n "$failure_reason" ]]; then
      failure_reason="${failure_reason}+alpha_worker_drill_failed"
    else
      failure_reason="alpha_worker_drill_failed"
    fi
  fi
fi
if [[ "$RECOVERY_CHAOS_ENABLE_WORKER_DRILLS" == "1" && "$BREADTH_WORKER_ENABLED" == "1" ]]; then
  if breadth_worker_drill_summary="$(perform_worker_drill betbot-temperature-breadth-worker.service breadth_worker)"; then
    notes+=("breadth_worker_drill_pass")
  else
    pass_flag=0
    notes+=("breadth_worker_drill_fail")
    if [[ -n "$failure_reason" ]]; then
      failure_reason="${failure_reason}+breadth_worker_drill_failed"
    else
      failure_reason="breadth_worker_drill_failed"
    fi
  fi
fi

notes+=("post_shadow_state=$post_shadow_state")
notes+=("mode=$mode")
notes+=("disruption_seconds=$disruption_seconds")
notes+=("recovered_wait_seconds=$recovered_wait_seconds")
notes_joined="$(printf '%s||' "${notes[@]}" | sed 's/||$//')"

event_path="$(write_chaos_artifact "$mode" "$pass_flag" "$pre_shadow_state" "$post_shadow_state" "$recovery_exit_code" "$disruption_seconds" "$recovered_wait_seconds" "$emergency_restart_attempted" "$failure_reason" "$notes_joined" "$alpha_worker_drill_summary" "$breadth_worker_drill_summary" "$recovery_effectiveness_strict_required" "$recovery_effectiveness_gap_detected" "$recovery_effectiveness_gap_reason" "$recovery_effectiveness_stale" "$recovery_effectiveness_file_age_seconds" "$recovery_effectiveness_stale_threshold_seconds" "$recovery_effectiveness_summary_available" "$recovery_effectiveness_demoted_action_count" "$recovery_effectiveness_harmful_action_count")"
log_line "chaos_check mode=$mode pass=$pass_flag pre=$pre_shadow_state post=$post_shadow_state recovery_exit=$recovery_exit_code disruption_seconds=$disruption_seconds recovered_wait_seconds=$recovered_wait_seconds emergency_restart_attempted=$emergency_restart_attempted alpha_worker_drill=${alpha_worker_drill_summary:-none} breadth_worker_drill=${breadth_worker_drill_summary:-none} recovery_effectiveness_strict_required=$recovery_effectiveness_strict_required recovery_effectiveness_gap_detected=$recovery_effectiveness_gap_detected recovery_effectiveness_gap_reason=$recovery_effectiveness_gap_reason recovery_effectiveness_stale=$recovery_effectiveness_stale recovery_effectiveness_file_age_seconds=$recovery_effectiveness_file_age_seconds recovery_effectiveness_stale_threshold_seconds=$recovery_effectiveness_stale_threshold_seconds recovery_effectiveness_summary_available=$recovery_effectiveness_summary_available recovery_effectiveness_demoted_action_count=$recovery_effectiveness_demoted_action_count recovery_effectiveness_harmful_action_count=$recovery_effectiveness_harmful_action_count failure_reason=${failure_reason:-none} event=$event_path"

if [[ -n "$RECOVERY_CHAOS_WEBHOOK_TARGET_URL" ]]; then
  should_notify=0
  if [[ "$pass_flag" == "0" ]]; then
    should_notify=1
  elif [[ "$RECOVERY_CHAOS_NOTIFY_ON_PASS" == "1" ]]; then
    should_notify=1
  fi
  if [[ "$should_notify" == "1" ]]; then
    payload_json="$("$PYTHON_BIN" - "$mode" "$pass_flag" "$pre_shadow_state" "$post_shadow_state" "$recovery_exit_code" "$disruption_seconds" "$recovered_wait_seconds" "$failure_reason" "$event_path" "$alpha_worker_drill_summary" "$breadth_worker_drill_summary" "$RECOVERY_CHAOS_WEBHOOK_MESSAGE_MODE" "$RECOVERY_CHAOS_WEBHOOK_USERNAME" "$recovery_effectiveness_strict_required" "$recovery_effectiveness_gap_detected" "$recovery_effectiveness_gap_reason" "$recovery_effectiveness_stale" "$recovery_effectiveness_summary_available" "$recovery_effectiveness_harmful_action_count" "$recovery_effectiveness_demoted_action_count" "$recovery_effectiveness_file_age_seconds" "$recovery_effectiveness_stale_threshold_seconds" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys

mode = str(sys.argv[1] or "unknown")
passed = str(sys.argv[2] or "0").strip() == "1"
pre_shadow_state = str(sys.argv[3] or "unknown")
post_shadow_state = str(sys.argv[4] or "unknown")
recovery_exit_code = str(sys.argv[5] or "0")
disruption_seconds = str(sys.argv[6] or "0")
recovered_wait_seconds = str(sys.argv[7] or "0")
failure_reason = (str(sys.argv[8] or "").strip() or "none").replace("_", " ")
event_file = str(sys.argv[9] or "")
alpha_worker_drill = str(sys.argv[10] or "").strip()
breadth_worker_drill = str(sys.argv[11] or "").strip()
message_mode = (str(sys.argv[12] or "").strip().lower() or "concise")
username = (str(sys.argv[13] or "").strip() or "BetBot Recovery Drill")
recovery_effectiveness_strict_required = str(sys.argv[14] or "0").strip() in {"1", "true", "yes", "y"}
recovery_effectiveness_gap_detected = str(sys.argv[15] or "0").strip() in {"1", "true", "yes", "y"}
recovery_effectiveness_gap_reason = str(sys.argv[16] or "none").strip().lower() or "none"
if recovery_effectiveness_gap_reason not in {"none", "summary_missing", "summary_stale"}:
    recovery_effectiveness_gap_reason = "none"
recovery_effectiveness_stale = str(sys.argv[17] or "0").strip() in {"1", "true", "yes", "y"}
recovery_effectiveness_summary_available = str(sys.argv[18] or "0").strip() in {"1", "true", "yes", "y"}
try:
    recovery_effectiveness_harmful_action_count = max(0, int(float(str(sys.argv[19] or "0"))))
except Exception:
    recovery_effectiveness_harmful_action_count = 0
try:
    recovery_effectiveness_demoted_action_count = max(0, int(float(str(sys.argv[20] or "0"))))
except Exception:
    recovery_effectiveness_demoted_action_count = 0
try:
    recovery_effectiveness_file_age_seconds = int(float(str(sys.argv[21] or "-1")))
except Exception:
    recovery_effectiveness_file_age_seconds = -1
try:
    recovery_effectiveness_stale_threshold_seconds = int(float(str(sys.argv[22] or "-1")))
except Exception:
    recovery_effectiveness_stale_threshold_seconds = -1


def clip(value: str, limit: int = 140) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 3)].rstrip() + "..."

title = "BetBot Recovery Chaos Check"
result = "PASS" if passed else "FAIL"
event_name = Path(event_file).name if event_file else "n/a"
strict_label = "on" if recovery_effectiveness_strict_required else "off"
gap_label = "yes" if recovery_effectiveness_gap_detected else "no"
stale_label = "yes" if recovery_effectiveness_stale else "no"
summary_label = "yes" if recovery_effectiveness_summary_available else "no"
gap_reason_text = recovery_effectiveness_gap_reason.replace("_", " ")
if recovery_effectiveness_file_age_seconds >= 0 and recovery_effectiveness_stale_threshold_seconds >= 0:
    age_detail = f"{recovery_effectiveness_file_age_seconds}s/{recovery_effectiveness_stale_threshold_seconds}s"
else:
    age_detail = "n/a"
if message_mode == "detailed":
    lines = [
        title,
        f"Status: {result} (mode={mode})",
        f"What happened: shadow service {pre_shadow_state} -> {post_shadow_state}, recovery exit={recovery_exit_code}.",
        f"Timing: disruption {disruption_seconds}s, recover wait {recovered_wait_seconds}s",
        (
            "Effectiveness: strict required={strict}, gap={gap} ({reason}), stale={stale}, "
            "summary={summary}, harmful={harmful}, demoted={demoted}, age={age}"
        ).format(
            strict=strict_label,
            gap=gap_label,
            reason=gap_reason_text,
            stale=stale_label,
            summary=summary_label,
            harmful=recovery_effectiveness_harmful_action_count,
            demoted=recovery_effectiveness_demoted_action_count,
            age=age_detail,
        ),
        f"Alpha worker drill: {alpha_worker_drill or 'n/a'}",
        f"Breadth worker drill: {breadth_worker_drill or 'n/a'}",
        f"Failure reason: {failure_reason}",
        f"Event file: {event_name}",
        "Next step: if failed, run one manual recovery cycle and inspect the event artifact.",
    ]
else:
    lines = [
        title,
        f"Status: {result} ({mode})",
        f"What happened: shadow {pre_shadow_state}->{post_shadow_state}, recovery rc={recovery_exit_code}",
        f"Timing: disruption {disruption_seconds}s | recover wait {recovered_wait_seconds}s",
        (
            "Effectiveness: strict={strict} | gap={gap} ({reason}) | summary={summary} | "
            "harmful={harmful} | demoted={demoted}"
        ).format(
            strict=strict_label,
            gap=gap_label,
            reason=gap_reason_text,
            summary=summary_label,
            harmful=recovery_effectiveness_harmful_action_count,
            demoted=recovery_effectiveness_demoted_action_count,
        ),
        f"Workers: alpha={alpha_worker_drill or 'n/a'} | breadth={breadth_worker_drill or 'n/a'}",
        f"Artifact: {event_name}",
    ]
    if not passed:
        lines.append(f"Failure detail: {clip(failure_reason.replace('_', ' '), 180)}")
        lines.append("Next step: run one manual recovery drill and inspect the event artifact.")
text = "\n".join(lines)
print(json.dumps({"text": text, "content": text, "username": username}))
PY
)"
    curl --silent --show-error --fail \
      --max-time "$RECOVERY_CHAOS_WEBHOOK_TIMEOUT_SECONDS" \
      --header "Content-Type: application/json" \
      --data-binary "$payload_json" \
      "$RECOVERY_CHAOS_WEBHOOK_TARGET_URL" >/dev/null 2>&1 || true
  fi
fi

if [[ "$pass_flag" == "1" ]]; then
  exit 0
fi
exit 2
