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

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs"
CHECKPOINTS_DIR="$OUTPUT_DIR/checkpoints"
mkdir -p "$CHECKPOINTS_DIR"
MILESTONES_DIR="$CHECKPOINTS_DIR/milestones"
mkdir -p "$MILESTONES_DIR"
HEALTH_DIR="$OUTPUT_DIR/health"
mkdir -p "$HEALTH_DIR"
FIRST_RESOLVED_SENTINEL="$CHECKPOINTS_DIR/.shadow_resolved_first_seen"
LOG_FILE="$OUTPUT_DIR/logs/readiness_reports.log"
READINESS_REPORTS_LOCK_FILE="${READINESS_REPORTS_LOCK_FILE:-$OUTPUT_DIR/.readiness_reports.lock}"
READINESS_RUNNER_STATE_FILE="${READINESS_RUNNER_STATE_FILE:-$HEALTH_DIR/readiness_runner_latest.json}"
READINESS_RUNNER_HEARTBEAT_SECONDS="${READINESS_RUNNER_HEARTBEAT_SECONDS:-60}"
READINESS_STAGE_WARN_SECONDS="${READINESS_STAGE_WARN_SECONDS:-900}"
READINESS_STAGE_TIMEOUT_SECONDS="${READINESS_STAGE_TIMEOUT_SECONDS:-1800}"
READINESS_LOG_COMMAND_OUTPUT="${READINESS_LOG_COMMAND_OUTPUT:-0}"

REFERENCE_BANKROLL_DOLLARS="${REFERENCE_BANKROLL_DOLLARS:-1000}"
REPORTING_HOURS="${REPORTING_HOURS:-336}"
TOP_N="${REPORTING_TOP_N:-10}"
SLIPPAGE_BPS_LIST="${REPORTING_SLIPPAGE_BPS_LIST:-0,5,10}"
HORIZONS="${REPORTING_HORIZONS:-1d,7d,14d,21d,28d,3mo,6mo,1yr}"
WINDOW_HOURS_LIST="${REPORTING_WINDOW_HOURS_LIST:-4,14}"
WINDOW_SUMMARIZE_SCRIPT="${WINDOW_SUMMARIZE_SCRIPT:-$BETBOT_ROOT/infra/digitalocean/summarize_window.py}"
READINESS_FAIL_ON_PIPELINE_RED="${READINESS_FAIL_ON_PIPELINE_RED:-0}"
READINESS_STRICT_LIVE_READINESS_ARTIFACT="${READINESS_STRICT_LIVE_READINESS_ARTIFACT:-0}"
PIPELINE_ALERT_WEBHOOK_URL="${PIPELINE_ALERT_WEBHOOK_URL:-${RECOVERY_WEBHOOK_URL:-${ALERT_WEBHOOK_URL:-}}}"
PIPELINE_ALERT_WEBHOOK_THREAD_ID="${PIPELINE_ALERT_WEBHOOK_THREAD_ID:-${RECOVERY_WEBHOOK_THREAD_ID:-${ALERT_WEBHOOK_THREAD_ID:-}}}"
PIPELINE_ALERT_WEBHOOK_TIMEOUT_SECONDS="${PIPELINE_ALERT_WEBHOOK_TIMEOUT_SECONDS:-5}"
PIPELINE_ALERT_NOTIFY_STATUS_CHANGE_ONLY="${PIPELINE_ALERT_NOTIFY_STATUS_CHANGE_ONLY:-1}"
PIPELINE_ALERT_MESSAGE_MODE="${PIPELINE_ALERT_MESSAGE_MODE:-concise}"
PIPELINE_ALERT_WEBHOOK_USERNAME="${PIPELINE_ALERT_WEBHOOK_USERNAME:-BetBot Pipeline}"
PIPELINE_ALERT_STATE_FILE="${PIPELINE_ALERT_STATE_FILE:-$CHECKPOINTS_DIR/.pipeline_alert_state.json}"
BLOCKER_AUDIT_ENABLED="${BLOCKER_AUDIT_ENABLED:-1}"
BLOCKER_AUDIT_RUN_SCRIPT="${BLOCKER_AUDIT_RUN_SCRIPT:-$BETBOT_ROOT/infra/digitalocean/run_temperature_blocker_audit.sh}"
EXECUTION_COST_TAPE_ENABLED="${EXECUTION_COST_TAPE_ENABLED:-1}"
EXECUTION_COST_TAPE_WINDOW_HOURS="${EXECUTION_COST_TAPE_WINDOW_HOURS:-168.0}"
EXECUTION_COST_TAPE_MIN_CANDIDATE_SAMPLES="${EXECUTION_COST_TAPE_MIN_CANDIDATE_SAMPLES:-200}"
EXECUTION_COST_TAPE_MIN_QUOTE_COVERAGE_RATIO="${EXECUTION_COST_TAPE_MIN_QUOTE_COVERAGE_RATIO:-0.60}"
EXECUTION_COST_TAPE_MAX_TICKERS="${EXECUTION_COST_TAPE_MAX_TICKERS:-25}"
EXECUTION_COST_TAPE_JOURNAL_DB_PATH="${EXECUTION_COST_TAPE_JOURNAL_DB_PATH:-}"
EXECUTION_COST_TAPE_MIN_GLOBAL_EXPECTED_EDGE_SHARE_FOR_EXCLUSION="${EXECUTION_COST_TAPE_MIN_GLOBAL_EXPECTED_EDGE_SHARE_FOR_EXCLUSION:-0.45}"
EXECUTION_COST_TAPE_MIN_TICKER_ROWS_FOR_EXCLUSION="${EXECUTION_COST_TAPE_MIN_TICKER_ROWS_FOR_EXCLUSION:-200}"
EXECUTION_COST_TAPE_EXCLUSION_MAX_QUOTE_COVERAGE_RATIO="${EXECUTION_COST_TAPE_EXCLUSION_MAX_QUOTE_COVERAGE_RATIO:-0.20}"
EXECUTION_COST_TAPE_MAX_TICKER_MEAN_SPREAD_FOR_EXCLUSION="${EXECUTION_COST_TAPE_MAX_TICKER_MEAN_SPREAD_FOR_EXCLUSION:-0.10}"
EXECUTION_COST_TAPE_MAX_EXCLUDED_TICKERS="${EXECUTION_COST_TAPE_MAX_EXCLUDED_TICKERS:-12}"
DECISION_MATRIX_HARDENING_ENABLED="${DECISION_MATRIX_HARDENING_ENABLED:-1}"
DECISION_MATRIX_WINDOW_HOURS="${DECISION_MATRIX_WINDOW_HOURS:-168.0}"
DECISION_MATRIX_MIN_SETTLED_OUTCOMES="${DECISION_MATRIX_MIN_SETTLED_OUTCOMES:-25}"
DECISION_MATRIX_MAX_TOP_BLOCKER_SHARE="${DECISION_MATRIX_MAX_TOP_BLOCKER_SHARE:-0.55}"
DECISION_MATRIX_MIN_APPROVAL_RATE="${DECISION_MATRIX_MIN_APPROVAL_RATE:-0.03}"
DECISION_MATRIX_MIN_INTENTS_SAMPLE="${DECISION_MATRIX_MIN_INTENTS_SAMPLE:-1000}"
DECISION_MATRIX_MAX_SPARSE_EDGE_BLOCK_SHARE="${DECISION_MATRIX_MAX_SPARSE_EDGE_BLOCK_SHARE:-0.80}"

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

PIPELINE_ALERT_WEBHOOK_TARGET_URL="$(build_discord_target_url "$PIPELINE_ALERT_WEBHOOK_URL" "$PIPELINE_ALERT_WEBHOOK_THREAD_ID")"

EXTRA_READINESS_ARGS_ARRAY=()
if [[ -n "${EXTRA_READINESS_ARGS:-}" ]]; then
  read -r -a EXTRA_READINESS_ARGS_ARRAY <<<"${EXTRA_READINESS_ARGS}"
fi

NONFATAL_STAGE_FAILURES=()
RUN_ID="$(date -u +"%Y%m%d_%H%M%S")_$$"
RUN_START_EPOCH="$(date +%s)"
RUNNER_STAGE="bootstrap"
RUNNER_MESSAGE="initializing"
RUNNER_STATUS="starting"
RUNNER_FINAL_STATUS="failed"
LOCK_HELD=0
WRITE_RUNNER_STATE=0

write_runner_state() {
  if [[ "$WRITE_RUNNER_STATE" != "1" ]]; then
    return 0
  fi
  "$PYTHON_BIN" - "$READINESS_RUNNER_STATE_FILE" "$RUN_ID" "$RUNNER_STATUS" "$RUNNER_STAGE" "$RUNNER_MESSAGE" "$RUN_START_EPOCH" "$$" "${NONFATAL_STAGE_FAILURES[*]:-}" <<'PY'
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


path = Path(sys.argv[1])
run_id = str(sys.argv[2] or "").strip()
run_status = str(sys.argv[3] or "unknown").strip()
stage = str(sys.argv[4] or "").strip()
message = str(sys.argv[5] or "").strip()
started_epoch = parse_int(sys.argv[6], 0)
pid_value = parse_int(sys.argv[7], 0)
nonfatal_raw = str(sys.argv[8] or "").strip()
nonfatal_failures = [item.strip() for item in nonfatal_raw.split(" ") if item.strip()]

captured_now = datetime.now(timezone.utc)
payload = {
    "status": "ready",
    "event": "readiness_runner_state",
    "run_id": run_id,
    "run_status": run_status,
    "stage": stage,
    "message": message,
    "pid": pid_value,
    "started_at_epoch": started_epoch,
    "started_at": datetime.fromtimestamp(started_epoch, tz=timezone.utc).isoformat() if started_epoch > 0 else "",
    "updated_at": captured_now.isoformat(),
    "nonfatal_stage_failures": nonfatal_failures,
}
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY
}

set_runner_stage() {
  local status="$1"
  local stage="$2"
  local message="$3"
  RUNNER_STATUS="$status"
  RUNNER_STAGE="$stage"
  RUNNER_MESSAGE="$message"
  write_runner_state
}

run_stage() {
  local required_mode="$1"
  local stage_name="$2"
  shift 2

  local stage_start_epoch
  stage_start_epoch="$(date +%s)"
  set_runner_stage "running" "$stage_name" "started"
  local stage_timeout_seconds="$READINESS_STAGE_TIMEOUT_SECONDS"
  local log_command_output=0
  if [[ "$READINESS_LOG_COMMAND_OUTPUT" == "1" ]]; then
    log_command_output=1
  fi
  if [[ ! "$stage_timeout_seconds" =~ ^[0-9]+$ ]]; then
    stage_timeout_seconds=0
  fi
  local stage_log_file="$OUTPUT_DIR/logs/readiness_stage_${stage_name}_${RUN_ID}.log"
  : > "$stage_log_file"
  if (( log_command_output == 1 )); then
    if (( stage_timeout_seconds > 0 )) && command -v timeout >/dev/null 2>&1; then
      timeout --signal=TERM --kill-after=30 "$stage_timeout_seconds" "$@" >> "$LOG_FILE" 2>&1 &
    else
      "$@" >> "$LOG_FILE" 2>&1 &
    fi
  else
    if (( stage_timeout_seconds > 0 )) && command -v timeout >/dev/null 2>&1; then
      timeout --signal=TERM --kill-after=30 "$stage_timeout_seconds" "$@" > "$stage_log_file" 2>&1 &
    else
      "$@" > "$stage_log_file" 2>&1 &
    fi
  fi
  local stage_pid=$!
  local heartbeat_pid=""
  if [[ "$READINESS_RUNNER_HEARTBEAT_SECONDS" =~ ^[0-9]+$ ]] && (( READINESS_RUNNER_HEARTBEAT_SECONDS > 0 )); then
    (
      while kill -0 "$stage_pid" 2>/dev/null; do
        sleep "$READINESS_RUNNER_HEARTBEAT_SECONDS"
        if kill -0 "$stage_pid" 2>/dev/null; then
          RUNNER_STATUS="running"
          RUNNER_STAGE="$stage_name"
          RUNNER_MESSAGE="in_progress"
          write_runner_state
        fi
      done
    ) &
    heartbeat_pid=$!
  fi

  local rc=0
  if ! wait "$stage_pid"; then
    rc=$?
  fi
  local stage_end_epoch
  stage_end_epoch="$(date +%s)"
  local stage_duration_seconds=$(( stage_end_epoch - stage_start_epoch ))
  if [[ -n "$heartbeat_pid" ]]; then
    kill "$heartbeat_pid" >/dev/null 2>&1 || true
    wait "$heartbeat_pid" >/dev/null 2>&1 || true
  fi

  if [[ "$rc" -eq 0 ]]; then
    if (( log_command_output != 1 )); then
      rm -f "$stage_log_file" >/dev/null 2>&1 || true
    fi
    echo "stage_complete stage=$stage_name required=$required_mode duration_seconds=$stage_duration_seconds exit_code=0" >> "$LOG_FILE"
    if [[ "$READINESS_STAGE_WARN_SECONDS" =~ ^[0-9]+$ ]] && (( READINESS_STAGE_WARN_SECONDS > 0 )) && (( stage_duration_seconds >= READINESS_STAGE_WARN_SECONDS )); then
      NONFATAL_STAGE_FAILURES+=("${stage_name}:slow_${stage_duration_seconds}s")
      set_runner_stage "running" "$stage_name" "ok_slow_${stage_duration_seconds}s"
    else
      set_runner_stage "running" "$stage_name" "ok"
    fi
    return 0
  fi

  local failure_tag="${stage_name}:exit_${rc}"
  if (( rc == 124 || rc == 137 )); then
    failure_tag="${stage_name}:timeout_exit_${rc}"
  fi
  if (( log_command_output != 1 )); then
    if [[ -s "$stage_log_file" ]]; then
      echo "stage_failure_output stage=$stage_name log_file=$stage_log_file tail_lines=80" >> "$LOG_FILE"
      tail -n 80 "$stage_log_file" >> "$LOG_FILE" || true
    else
      echo "stage_failure_output stage=$stage_name log_file=$stage_log_file tail_lines=80 empty=true" >> "$LOG_FILE"
    fi
  fi
  echo "stage_failed stage=$stage_name required=$required_mode duration_seconds=$stage_duration_seconds exit_code=$rc" >> "$LOG_FILE"
  NONFATAL_STAGE_FAILURES+=("$failure_tag")
  set_runner_stage "running" "$stage_name" "failed_exit_${rc}"
  if [[ "$required_mode" == "required" ]]; then
    return "$rc"
  fi
  return 0
}

refresh_latest_json_copy() {
  local search_dir="$1"
  local stem="$2"
  local latest_path="$3"
  local newest=""
  newest="$(ls -1t "$search_dir"/"${stem}"_*.json 2>/dev/null | grep -Ev '_latest\.json$' | head -n 1 || true)"
  if [[ -z "$newest" ]]; then
    return 0
  fi
  cp -f "$newest" "$latest_path" 2>/dev/null || true
}

refresh_latest_report_aliases() {
  refresh_latest_json_copy "$CHECKPOINTS_DIR" "station_tuning_window_1h" "$CHECKPOINTS_DIR/station_tuning_window_1h_latest.json"
  refresh_latest_json_copy "$CHECKPOINTS_DIR" "station_tuning_window_4h" "$CHECKPOINTS_DIR/station_tuning_window_4h_latest.json"
  refresh_latest_json_copy "$CHECKPOINTS_DIR" "station_tuning_window_14h" "$CHECKPOINTS_DIR/station_tuning_window_14h_latest.json"
  refresh_latest_json_copy "$CHECKPOINTS_DIR" "profitability_1h" "$CHECKPOINTS_DIR/profitability_1h_latest.json"
  refresh_latest_json_copy "$CHECKPOINTS_DIR" "profitability_4h" "$CHECKPOINTS_DIR/profitability_4h_latest.json"
  refresh_latest_json_copy "$CHECKPOINTS_DIR" "profitability_14h" "$CHECKPOINTS_DIR/profitability_14h_latest.json"
  refresh_latest_json_copy "$CHECKPOINTS_DIR" "blocker_audit_168h" "$CHECKPOINTS_DIR/blocker_audit_168h_latest.json"
  refresh_latest_json_copy "$HEALTH_DIR" "execution_cost_tape" "$HEALTH_DIR/execution_cost_tape_latest.json"
  refresh_latest_json_copy "$HEALTH_DIR" "decision_matrix_hardening" "$HEALTH_DIR/decision_matrix_hardening_latest.json"

  refresh_latest_json_copy "$OUTPUT_DIR" "kalshi_temperature_live_readiness" "$OUTPUT_DIR/kalshi_temperature_live_readiness_latest.json"
  refresh_latest_json_copy "$OUTPUT_DIR" "kalshi_temperature_go_live_gate" "$OUTPUT_DIR/kalshi_temperature_go_live_gate_latest.json"
  refresh_latest_json_copy "$OUTPUT_DIR" "kalshi_temperature_bankroll_validation" "$OUTPUT_DIR/kalshi_temperature_bankroll_validation_latest.json"
  refresh_latest_json_copy "$OUTPUT_DIR" "kalshi_temperature_alpha_gap_report" "$OUTPUT_DIR/kalshi_temperature_alpha_gap_report_latest.json"
}

finalize_runner() {
  local rc=$?
  if [[ "$RUNNER_FINAL_STATUS" == "failed" && "$rc" -eq 0 ]]; then
    RUNNER_FINAL_STATUS="completed"
  fi
  if [[ "$RUNNER_FINAL_STATUS" == "completed" && "${#NONFATAL_STAGE_FAILURES[@]}" -gt 0 ]]; then
    RUNNER_FINAL_STATUS="completed_with_warnings"
  fi
  if [[ "$WRITE_RUNNER_STATE" == "1" ]]; then
    set_runner_stage "$RUNNER_FINAL_STATUS" "$RUNNER_STAGE" "$RUNNER_MESSAGE"
  fi
}
trap finalize_runner EXIT

cd "$BETBOT_ROOT"
ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "=== $ts readiness report cycle start run_id=$RUN_ID ===" >> "$LOG_FILE"

exec 9>"$READINESS_REPORTS_LOCK_FILE"
if ! flock -n 9; then
  echo "readiness_report skipped reason=lock_contended lock=$READINESS_REPORTS_LOCK_FILE" >> "$LOG_FILE"
  exit 0
fi
LOCK_HELD=1
WRITE_RUNNER_STATE=1
set_runner_stage "running" "lock" "acquired"

if [[ ! -f "$WINDOW_SUMMARIZE_SCRIPT" ]]; then
  echo "missing window summarize script: $WINDOW_SUMMARIZE_SCRIPT" >> "$LOG_FILE"
  exit 1
fi

window_end_epoch="$(date +%s)"
window_ts="$(date -u +"%Y%m%d_%H%M%S")"
IFS=',' read -r -a WINDOW_HOURS_ARRAY <<<"$WINDOW_HOURS_LIST"
for raw_hours in "${WINDOW_HOURS_ARRAY[@]}"; do
  hours="$(echo "$raw_hours" | tr -d '[:space:]')"
  if [[ -z "$hours" ]]; then
    continue
  fi
  if ! [[ "$hours" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "invalid window hours value: $hours" >> "$LOG_FILE"
    continue
  fi
  window_label="${hours}h"
  window_start_epoch="$("$PYTHON_BIN" -c "print(max(0, int($window_end_epoch - float($hours) * 3600)))")"
  window_output="$CHECKPOINTS_DIR/station_tuning_window_${window_label}_${window_ts}.json"
  run_stage optional "window_summary_${window_label}" \
    "$PYTHON_BIN" "$WINDOW_SUMMARIZE_SCRIPT" \
      --out-dir "$OUTPUT_DIR" \
      --start-epoch "$window_start_epoch" \
      --end-epoch "$window_end_epoch" \
      --label "$window_label" \
      --output "$window_output"
done

live_readiness_cmd=(
  "$PYTHON_BIN" -m betbot.cli kalshi-temperature-live-readiness
  --output-dir "$OUTPUT_DIR"
  --horizons "$HORIZONS"
  --reference-bankroll-dollars "$REFERENCE_BANKROLL_DOLLARS"
  --slippage-bps-list "$SLIPPAGE_BPS_LIST"
  --top-n "$TOP_N"
)
if (( ${#EXTRA_READINESS_ARGS_ARRAY[@]} > 0 )); then
  live_readiness_cmd+=("${EXTRA_READINESS_ARGS_ARRAY[@]}")
fi
run_stage required "live_readiness" "${live_readiness_cmd[@]}"

latest_live_readiness="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_live_readiness_*.json 2>/dev/null | head -n 1 || true)"
if [[ -z "$latest_live_readiness" ]]; then
  if [[ "$READINESS_STRICT_LIVE_READINESS_ARTIFACT" == "1" ]]; then
    echo "live_readiness_artifact_missing strict=1 pattern=$OUTPUT_DIR/kalshi_temperature_live_readiness_*.json" >> "$LOG_FILE"
    exit 1
  fi
fi

if [[ -n "$latest_live_readiness" ]]; then
  pipeline_snapshot="$("$PYTHON_BIN" - "$latest_live_readiness" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("unknown|parse_error|")
    raise SystemExit(0)
executive = payload.get("executive_summary")
if not isinstance(executive, dict):
    print("unknown|missing_executive_summary|")
    raise SystemExit(0)
status = str(executive.get("shortest_horizon_pipeline_status") or "unknown").strip().lower() or "unknown"
reason = str(executive.get("shortest_horizon_pipeline_reason") or "").strip()
captured_at = str(payload.get("captured_at") or "").strip()
print(f"{status}|{reason}|{captured_at}")
PY
)"
  pipeline_status="$(awk -F'|' '{print $1}' <<<"$pipeline_snapshot")"
  pipeline_reason="$(awk -F'|' '{print $2}' <<<"$pipeline_snapshot")"
  pipeline_captured_at="$(awk -F'|' '{print $3}' <<<"$pipeline_snapshot")"
  if [[ "$READINESS_STRICT_LIVE_READINESS_ARTIFACT" == "1" && "$pipeline_status" == "unknown" ]]; then
    reason_text="${pipeline_reason:-unknown}"
    echo "live_readiness_artifact_invalid strict=1 reason=$reason_text source=$latest_live_readiness" >> "$LOG_FILE"
    exit 1
  fi
  if [[ "$pipeline_status" == "red" ]]; then
    alert_path="$CHECKPOINTS_DIR/pipeline_health_alert_${window_ts}.json"
    "$PYTHON_BIN" - "$alert_path" "$latest_live_readiness" "$pipeline_status" "$pipeline_reason" "$pipeline_captured_at" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys

alert_path = Path(sys.argv[1])
live_readiness_file = sys.argv[2]
pipeline_status = sys.argv[3]
pipeline_reason = sys.argv[4]
pipeline_captured_at = sys.argv[5]
payload = {
    "status": "ready",
    "event": "pipeline_health_alert",
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "pipeline_status": pipeline_status,
    "pipeline_reason": pipeline_reason,
    "source_live_readiness_file": live_readiness_file,
    "source_live_readiness_captured_at": pipeline_captured_at,
}
alert_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY
    echo "pipeline_alert status=$pipeline_status reason=$pipeline_reason source=$latest_live_readiness alert=$alert_path" >> "$LOG_FILE"
    if [[ -n "$PIPELINE_ALERT_WEBHOOK_TARGET_URL" ]]; then
      alert_payload="$("$PYTHON_BIN" - "$pipeline_status" "$pipeline_reason" "$latest_live_readiness" "$alert_path" "$PIPELINE_ALERT_NOTIFY_STATUS_CHANGE_ONLY" "$PIPELINE_ALERT_STATE_FILE" "$PIPELINE_ALERT_MESSAGE_MODE" "$PIPELINE_ALERT_WEBHOOK_USERNAME" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys

status = str(sys.argv[1] or "unknown")
reason = str(sys.argv[2] or "").strip() or "unknown_reason"
source = str(sys.argv[3] or "")
alert_path = str(sys.argv[4] or "")
status_change_only = str(sys.argv[5]).strip().lower() in {"1", "true", "yes", "y"}
alert_state_path = Path(str(sys.argv[6] or "").strip())
message_mode = (str(sys.argv[7] or "").strip().lower() or "concise")
username = (str(sys.argv[8] or "").strip() or "BetBot Pipeline")

reason_map = {
    "pipeline_status_red": "pipeline health is red",
    "missing_live_status_file": "live status file missing",
    "invalid_live_status_payload": "live status payload invalid",
    "status_red": "live status is red",
    "missing_metar_summary": "METAR summary missing",
    "metar_summary_stale_critical": "METAR summary stale (critical)",
    "missing_shadow_summary": "shadow summary missing",
    "shadow_summary_stale_critical": "shadow summary stale (critical)",
    "missing_settlement_summary": "settlement summary missing",
    "settlement_summary_stale_critical": "settlement summary stale (critical)",
    "settlement_state_loaded_false": "settlement state not loaded",
}
readable_reason = reason_map.get(reason, reason.replace("_", " "))

source_name = Path(source).name if source else "n/a"
alert_name = Path(alert_path).name if alert_path else "n/a"
fingerprint = json.dumps(
    {
        # Keep only stable signal fields so rotating artifact filenames do not
        # defeat dedupe for identical ongoing incidents.
        "event": "pipeline_health_alert",
        "status": status.strip().upper(),
        "reason": reason,
    },
    sort_keys=True,
    separators=(",", ":"),
)

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

if message_mode == "detailed":
    lines = [
        "Pipeline Health Alert",
        f"Status: {status.upper()}",
        f"What happened: {readable_reason}.",
        "Why it matters: readiness and go-live reports can drift until this is fixed.",
        f"Source report: {source_name}",
        f"Alert artifact: {alert_name}",
        "Next step: run check_temperature_shadow.sh --strict and inspect the red reason list.",
    ]
else:
    lines = [
        "Pipeline Health Alert",
        f"Status: {status.upper()}",
        f"What happened: {readable_reason}.",
        f"Source: {source_name}",
        "Next step: run check_temperature_shadow.sh --strict if this repeats.",
    ]
text = "\n".join(lines)

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
      if [[ -n "$alert_payload" ]]; then
        curl --silent --show-error --fail \
          --max-time "$PIPELINE_ALERT_WEBHOOK_TIMEOUT_SECONDS" \
          --header "Content-Type: application/json" \
          --data-binary "$alert_payload" \
          "$PIPELINE_ALERT_WEBHOOK_TARGET_URL" >/dev/null 2>&1 || true
      fi
    fi
    if [[ "$READINESS_FAIL_ON_PIPELINE_RED" == "1" ]]; then
      echo "pipeline_alert fail_on_red enabled; exiting non-zero" >> "$LOG_FILE"
      exit 2
    fi
  else
    # Reset alert fingerprint once pipeline is healthy so future red incidents
    # with the same reason can notify again.
    if [[ -n "$PIPELINE_ALERT_STATE_FILE" && -f "$PIPELINE_ALERT_STATE_FILE" ]]; then
      rm -f "$PIPELINE_ALERT_STATE_FILE" || true
    fi
  fi
fi

go_live_gate_cmd=(
  "$PYTHON_BIN" -m betbot.cli kalshi-temperature-go-live-gate
  --output-dir "$OUTPUT_DIR"
  --horizons "$HORIZONS"
  --reference-bankroll-dollars "$REFERENCE_BANKROLL_DOLLARS"
  --slippage-bps-list "$SLIPPAGE_BPS_LIST"
  --top-n "$TOP_N"
)
if [[ -n "$latest_live_readiness" ]]; then
  go_live_gate_cmd+=(--source-live-readiness-file "$latest_live_readiness")
fi
if (( ${#EXTRA_READINESS_ARGS_ARRAY[@]} > 0 )); then
  go_live_gate_cmd+=("${EXTRA_READINESS_ARGS_ARRAY[@]}")
fi
run_stage optional "go_live_gate" "${go_live_gate_cmd[@]}"

bankroll_cmd=(
  "$PYTHON_BIN" -m betbot.cli kalshi-temperature-bankroll-validation
  --output-dir "$OUTPUT_DIR"
  --hours "$REPORTING_HOURS"
  --reference-bankroll-dollars "$REFERENCE_BANKROLL_DOLLARS"
  --slippage-bps-list "$SLIPPAGE_BPS_LIST"
  --top-n "$TOP_N"
)
if (( ${#EXTRA_READINESS_ARGS_ARRAY[@]} > 0 )); then
  bankroll_cmd+=("${EXTRA_READINESS_ARGS_ARRAY[@]}")
fi
run_stage optional "bankroll_validation" "${bankroll_cmd[@]}"

latest_bankroll_validation="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_bankroll_validation_*.json 2>/dev/null | head -n 1 || true)"
alpha_gap_cmd=(
  "$PYTHON_BIN" -m betbot.cli kalshi-temperature-alpha-gap-report
  --output-dir "$OUTPUT_DIR"
  --hours "$REPORTING_HOURS"
  --reference-bankroll-dollars "$REFERENCE_BANKROLL_DOLLARS"
  --slippage-bps-list "$SLIPPAGE_BPS_LIST"
  --top-n "$TOP_N"
)
if [[ -n "$latest_bankroll_validation" ]]; then
  alpha_gap_cmd+=(--source-bankroll-validation-file "$latest_bankroll_validation")
fi
if (( ${#EXTRA_READINESS_ARGS_ARRAY[@]} > 0 )); then
  alpha_gap_cmd+=("${EXTRA_READINESS_ARGS_ARRAY[@]}")
fi
run_stage optional "alpha_gap_report" "${alpha_gap_cmd[@]}"

if [[ "$BLOCKER_AUDIT_ENABLED" == "1" && -x "$BLOCKER_AUDIT_RUN_SCRIPT" ]]; then
  run_stage optional "blocker_audit" "$BLOCKER_AUDIT_RUN_SCRIPT" "$ENV_FILE"
fi

if [[ "$EXECUTION_COST_TAPE_ENABLED" == "1" ]]; then
  execution_cost_tape_cmd=(
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-execution-cost-tape
    --output-dir "$OUTPUT_DIR"
    --window-hours "$EXECUTION_COST_TAPE_WINDOW_HOURS"
    --min-candidate-samples "$EXECUTION_COST_TAPE_MIN_CANDIDATE_SAMPLES"
    --min-quote-coverage-ratio "$EXECUTION_COST_TAPE_MIN_QUOTE_COVERAGE_RATIO"
    --max-tickers "$EXECUTION_COST_TAPE_MAX_TICKERS"
    --min-global-expected-edge-share-for-exclusion "$EXECUTION_COST_TAPE_MIN_GLOBAL_EXPECTED_EDGE_SHARE_FOR_EXCLUSION"
    --min-ticker-rows-for-exclusion "$EXECUTION_COST_TAPE_MIN_TICKER_ROWS_FOR_EXCLUSION"
    --exclusion-max-quote-coverage-ratio "$EXECUTION_COST_TAPE_EXCLUSION_MAX_QUOTE_COVERAGE_RATIO"
    --max-ticker-mean-spread-for-exclusion "$EXECUTION_COST_TAPE_MAX_TICKER_MEAN_SPREAD_FOR_EXCLUSION"
    --max-excluded-tickers "$EXECUTION_COST_TAPE_MAX_EXCLUDED_TICKERS"
  )
  if [[ -n "$EXECUTION_COST_TAPE_JOURNAL_DB_PATH" ]]; then
    execution_cost_tape_cmd+=(--journal-db-path "$EXECUTION_COST_TAPE_JOURNAL_DB_PATH")
  fi
  run_stage optional "execution_cost_tape" "${execution_cost_tape_cmd[@]}"
fi

if [[ "$DECISION_MATRIX_HARDENING_ENABLED" == "1" ]]; then
  decision_matrix_cmd=(
    "$PYTHON_BIN" -m betbot.cli decision-matrix-hardening
    --output-dir "$OUTPUT_DIR"
    --window-hours "$DECISION_MATRIX_WINDOW_HOURS"
    --min-settled-outcomes "$DECISION_MATRIX_MIN_SETTLED_OUTCOMES"
    --max-top-blocker-share "$DECISION_MATRIX_MAX_TOP_BLOCKER_SHARE"
    --min-approval-rate "$DECISION_MATRIX_MIN_APPROVAL_RATE"
    --min-intents-sample "$DECISION_MATRIX_MIN_INTENTS_SAMPLE"
    --max-sparse-edge-block-share "$DECISION_MATRIX_MAX_SPARSE_EDGE_BLOCK_SHARE"
    --min-execution-cost-candidate-samples "$EXECUTION_COST_TAPE_MIN_CANDIDATE_SAMPLES"
    --min-execution-cost-quote-coverage-ratio "$EXECUTION_COST_TAPE_MIN_QUOTE_COVERAGE_RATIO"
  )
  run_stage optional "decision_matrix_hardening" "${decision_matrix_cmd[@]}"
fi

latest_profitability_14h="$(ls -1t "$CHECKPOINTS_DIR"/profitability_14h_*.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_profitability_14h" ]]; then
  resolved_metrics="$("$PYTHON_BIN" - "$latest_profitability_14h" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("0 0 0")
    raise SystemExit(0)
shadow_settled = payload.get("shadow_settled_reference")
if not isinstance(shadow_settled, dict):
    print("0 0 0")
    raise SystemExit(0)
resolved_unique_market_sides = shadow_settled.get("resolved_unique_market_sides")
resolved_unique_shadow_orders = shadow_settled.get("resolved_unique_shadow_orders")
resolved_planned_rows = (
    shadow_settled.get("resolved_planned_rows")
    or shadow_settled.get("resolved_planned_orders")
)
try:
    unique_market_sides = int(float(resolved_unique_market_sides or 0))
except Exception:
    unique_market_sides = 0
try:
    unique_shadow_orders = int(float(resolved_unique_shadow_orders or 0))
except Exception:
    unique_shadow_orders = 0
try:
    planned_rows = int(float(resolved_planned_rows or 0))
except Exception:
    planned_rows = 0
print(f"{unique_market_sides} {unique_shadow_orders} {planned_rows}")
PY
)"
  resolved_unique_market_sides="$(awk '{print $1}' <<<"$resolved_metrics")"
  resolved_unique_shadow_orders="$(awk '{print $2}' <<<"$resolved_metrics")"
  resolved_planned_rows="$(awk '{print $3}' <<<"$resolved_metrics")"
  if [[ "$resolved_unique_market_sides" =~ ^[0-9]+$ ]] && (( resolved_unique_market_sides > 0 )); then
    milestone_basis="resolved_unique_market_sides"
    milestone_value="$resolved_unique_market_sides"
  elif [[ "$resolved_unique_shadow_orders" =~ ^[0-9]+$ ]] && (( resolved_unique_shadow_orders > 0 )); then
    milestone_basis="resolved_unique_shadow_orders"
    milestone_value="$resolved_unique_shadow_orders"
  elif [[ "$resolved_planned_rows" =~ ^[0-9]+$ ]] && (( resolved_planned_rows > 0 )); then
    milestone_basis="resolved_planned_rows"
    milestone_value="$resolved_planned_rows"
  else
    milestone_basis=""
    milestone_value="0"
  fi
  if (( milestone_value > 0 )); then
    if [[ ! -f "$FIRST_RESOLVED_SENTINEL" ]]; then
      milestone_path="$MILESTONES_DIR/shadow_resolved_first_${window_ts}.json"
      "$PYTHON_BIN" - "$milestone_path" "$latest_profitability_14h" "$milestone_basis" "$milestone_value" "$resolved_unique_market_sides" "$resolved_unique_shadow_orders" "$resolved_planned_rows" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys

milestone_path = Path(sys.argv[1])
profitability_file = sys.argv[2]
basis = str(sys.argv[3] or "")
basis_value = int(float(sys.argv[4] or 0))
resolved_unique_market_sides = int(float(sys.argv[5] or 0))
resolved_unique_shadow_orders = int(float(sys.argv[6] or 0))
resolved_planned_rows = int(float(sys.argv[7] or 0))
payload = {
    "status": "ready",
    "event": "shadow_resolved_first",
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "milestone_basis": basis,
    "milestone_basis_value": basis_value,
    "resolved_unique_market_sides": resolved_unique_market_sides,
    "resolved_unique_shadow_orders": resolved_unique_shadow_orders,
    "resolved_planned_rows": resolved_planned_rows,
    "profitability_file": profitability_file,
}
milestone_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY
      echo "$window_ts" > "$FIRST_RESOLVED_SENTINEL"
      echo "milestone_emitted shadow_resolved_first basis=$milestone_basis value=$milestone_value unique_market_sides=$resolved_unique_market_sides unique_shadow_orders=$resolved_unique_shadow_orders resolved_rows=$resolved_planned_rows file=$milestone_path" >> "$LOG_FILE"
    fi
  fi
fi

if (( ${#NONFATAL_STAGE_FAILURES[@]} > 0 )); then
  echo "readiness_report completed_with_warnings nonfatal_failures=${NONFATAL_STAGE_FAILURES[*]}" >> "$LOG_FILE"
fi
refresh_latest_report_aliases
RUNNER_STAGE="finalize"
RUNNER_MESSAGE="cycle_complete"
RUNNER_FINAL_STATUS="completed"
echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") readiness report cycle end run_id=$RUN_ID ===" >> "$LOG_FILE"
