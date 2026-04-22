#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="betbot-temperature-shadow"
ALPHA_SERVICE_NAME="betbot-temperature-alpha-workers"
BREADTH_SERVICE_NAME="betbot-temperature-breadth-worker"
ALPHA_SUMMARY_TIMER_NAME="betbot-temperature-alpha-summary.timer"
RECOVERY_SERVICE_NAME="betbot-temperature-recovery"
RECOVERY_TIMER_NAME="${RECOVERY_SERVICE_NAME}.timer"
RECOVERY_CHAOS_SERVICE_NAME="betbot-temperature-recovery-chaos"
RECOVERY_CHAOS_TIMER_NAME="${RECOVERY_CHAOS_SERVICE_NAME}.timer"
LOG_MAINTENANCE_SERVICE_NAME="betbot-temperature-log-maintenance"
LOG_MAINTENANCE_TIMER_NAME="${LOG_MAINTENANCE_SERVICE_NAME}.timer"
DISCORD_ROUTE_GUARD_SERVICE_NAME="betbot-temperature-discord-route-guard"
DISCORD_ROUTE_GUARD_TIMER_NAME="${DISCORD_ROUTE_GUARD_SERVICE_NAME}.timer"
STALE_METRICS_DRILL_SERVICE_NAME="betbot-temperature-stale-metrics-drill"
STALE_METRICS_DRILL_TIMER_NAME="${STALE_METRICS_DRILL_SERVICE_NAME}.timer"
DISCORD_ROUTE_GUARD_TIMER_INSTALLED=0
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DISCORD_THREAD_MAP_PATH="${DISCORD_THREAD_MAP_PATH:-/etc/betbot/discord-thread-map.env}"
STRICT_MODE=0
ENV_FILE="/etc/betbot/temperature-shadow.env"

usage() {
  cat <<'USAGE'
Usage:
  check_temperature_shadow.sh [--strict] [--env <path>] [env_path]

Options:
  --strict      Enable strict gate checks (non-zero on hard failures)
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

DISCORD_THREAD_MAP_APPLY_CMD="sudo bash $SCRIPT_DIR/apply_discord_thread_map.sh --env $ENV_FILE"
DISCORD_THREAD_MAP_CHECK_CMD="sudo bash $SCRIPT_DIR/check_discord_thread_map.sh --env $ENV_FILE --map $DISCORD_THREAD_MAP_PATH --strict"
DISCORD_THREAD_MAP_PRECHECK_APPLY_CMD="sudo bash $SCRIPT_DIR/check_discord_thread_map.sh --env $ENV_FILE --map $DISCORD_THREAD_MAP_PATH --strict --apply"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1091
source "$ENV_FILE"

: "${OUTPUT_DIR:?OUTPUT_DIR not set in $ENV_FILE}"
CHECKPOINTS_DIR="$OUTPUT_DIR/checkpoints"
HEALTH_STATUS_FILE="$OUTPUT_DIR/health/live_status_latest.json"
LIVE_STATUS_STRICT_MAX_AGE_SECONDS="${LIVE_STATUS_STRICT_MAX_AGE_SECONDS:-300}"
ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS="${ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS:-54000}"
AUTO_PROFILE_STRICT_MAX_AGE_SECONDS="${AUTO_PROFILE_STRICT_MAX_AGE_SECONDS:-86400}"
DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS="${DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS:-10800}"
DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD="${DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD:-6}"
DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES="${DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES:-matrix_failed,bootstrap_blocked}"
DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE="${DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE:-0}"
DECISION_MATRIX_LANE_ALERT_STATE_FILE="${DECISION_MATRIX_LANE_ALERT_STATE_FILE:-$OUTPUT_DIR/health/.decision_matrix_lane_alert_state.json}"
REPLAN_COOLDOWN_STATE_FILE="${REPLAN_COOLDOWN_STATE_FILE:-$OUTPUT_DIR/.adaptive_replan_cooldown_minutes}"
REPLAN_BACKSTOP_STATE_FILE="${REPLAN_BACKSTOP_STATE_FILE:-$OUTPUT_DIR/.adaptive_replan_backstop}"
AUTO_PROFILE_PATH="${APPROVAL_GATE_PROFILE_AUTO_PATH:-$OUTPUT_DIR/runtime/approval_gate_profile_auto.json}"

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

normalize_status_token() {
  local raw="${1:-}"
  printf '%s' "${raw:-unknown}" \
    | tr '[:upper:]' '[:lower:]' \
    | tr -d '\r' \
    | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

normalize_boolean_token() {
  local raw="${1:-}"
  local default_value="${2:-false}"
  local default_binary normalized_binary
  default_binary="$(normalize_binary_flag "$default_value" "0")"
  normalized_binary="$(normalize_binary_flag "$raw" "$default_binary")"
  if [[ "$normalized_binary" == "1" ]]; then
    echo "true"
  else
    echo "false"
  fi
}

if ! [[ "$LIVE_STATUS_STRICT_MAX_AGE_SECONDS" =~ ^[0-9]+$ ]]; then
  LIVE_STATUS_STRICT_MAX_AGE_SECONDS=300
fi
if ! [[ "$ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS" =~ ^[0-9]+$ ]]; then
  ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS=54000
fi
if ! [[ "$AUTO_PROFILE_STRICT_MAX_AGE_SECONDS" =~ ^[0-9]+$ ]]; then
  AUTO_PROFILE_STRICT_MAX_AGE_SECONDS=86400
fi
if ! [[ "$DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS" =~ ^[0-9]+$ ]]; then
  DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS=10800
fi
DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE="$(normalize_binary_flag "$DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE" "0")"
APPROVAL_GATE_PROFILE_AUTO_ENABLED_NORMALIZED="$(normalize_binary_flag "${APPROVAL_GATE_PROFILE_AUTO_ENABLED:-0}" "0")"
ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_ENABLED_NORMALIZED="$(normalize_binary_flag "${ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_ENABLED:-0}" "0")"

AUTO_PROFILE_EXPECTED="0"
if [[ "$APPROVAL_GATE_PROFILE_AUTO_ENABLED_NORMALIZED" == "1" || "$ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_ENABLED_NORMALIZED" == "1" ]]; then
  AUTO_PROFILE_EXPECTED="1"
fi
overall_status="unknown"
alpha_summary_health_status="unknown"
alpha_summary_health_reason=""
alpha_summary_payload_consistent="true"
alpha_summary_message_quality_pass="true"
alpha_summary_quality_gate_source="unknown"
alpha_summary_quality_gate_auto_applied="false"
alpha_summary_quality_gate_min_probability_confidence=""
alpha_summary_quality_gate_min_expected_edge_net=""
alpha_summary_auto_apply_enabled="false"
alpha_summary_auto_apply_should_apply="false"
alpha_summary_auto_apply_applied_in_this_run="false"
alpha_summary_auto_apply_released_in_this_run="false"
alpha_summary_auto_apply_apply_reason="unknown"
alpha_summary_auto_apply_profile_path=""
alpha_summary_trader_payload_consistent="unknown"
alpha_summary_trader_view_present="false"
alpha_summary_trader_consistent="unknown"
alpha_summary_available="0"
live_status_age_seconds="-1"
alpha_summary_age_seconds="-1"
auto_profile_age_seconds="-1"
discord_route_guard_age_seconds="-1"
discord_route_guard_status="unknown"
discord_route_guard_shared_route_group_count="0"
discord_route_guard_route_hint=""
discord_route_guard_required_thread_keys=""
decision_matrix_lane_state_available="0"
decision_matrix_lane_state_age_seconds="-1"
decision_matrix_lane_status="unknown"
decision_matrix_lane_degraded_streak_count="0"
decision_matrix_lane_degraded_streak_threshold="0"
decision_matrix_lane_degraded_streak_notify_every="0"
decision_matrix_lane_last_notify_reason="none"
decision_matrix_lane_strict_status_match="false"
HAS_JQ=1
if ! command -v jq >/dev/null 2>&1; then
  HAS_JQ=0
fi

echo "=== systemd ==="
if sudo systemctl cat "$SERVICE_NAME" >/dev/null 2>&1; then
  sudo systemctl --no-pager --full status "$SERVICE_NAME" | sed -n '1,40p' || true
else
  echo "service not installed yet: $SERVICE_NAME"
fi
echo
svc_enabled="$(sudo systemctl is-enabled "$SERVICE_NAME" 2>/dev/null || true)"
svc_active="$(sudo systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
[[ -n "$svc_enabled" ]] || svc_enabled="unknown"
[[ -n "$svc_active" ]] || svc_active="unknown"
echo "enabled=$svc_enabled active=$svc_active"
if sudo systemctl cat "$RECOVERY_TIMER_NAME" >/dev/null 2>&1; then
  recovery_timer_enabled="$(sudo systemctl is-enabled "$RECOVERY_TIMER_NAME" 2>/dev/null || true)"
  recovery_timer_active="$(sudo systemctl is-active "$RECOVERY_TIMER_NAME" 2>/dev/null || true)"
  [[ -n "$recovery_timer_enabled" ]] || recovery_timer_enabled="unknown"
  [[ -n "$recovery_timer_active" ]] || recovery_timer_active="unknown"
  echo "recovery_timer_enabled=$recovery_timer_enabled recovery_timer_active=$recovery_timer_active"
fi
if sudo systemctl cat "$RECOVERY_SERVICE_NAME" >/dev/null 2>&1; then
  recovery_service_active="$(sudo systemctl is-active "$RECOVERY_SERVICE_NAME" 2>/dev/null || true)"
  [[ -n "$recovery_service_active" ]] || recovery_service_active="unknown"
  echo "recovery_service_active=$recovery_service_active"
fi
if sudo systemctl cat "$RECOVERY_CHAOS_TIMER_NAME" >/dev/null 2>&1; then
  recovery_chaos_timer_enabled="$(sudo systemctl is-enabled "$RECOVERY_CHAOS_TIMER_NAME" 2>/dev/null || true)"
  recovery_chaos_timer_active="$(sudo systemctl is-active "$RECOVERY_CHAOS_TIMER_NAME" 2>/dev/null || true)"
  [[ -n "$recovery_chaos_timer_enabled" ]] || recovery_chaos_timer_enabled="unknown"
  [[ -n "$recovery_chaos_timer_active" ]] || recovery_chaos_timer_active="unknown"
  echo "recovery_chaos_timer_enabled=$recovery_chaos_timer_enabled recovery_chaos_timer_active=$recovery_chaos_timer_active"
fi
if sudo systemctl cat "$RECOVERY_CHAOS_SERVICE_NAME" >/dev/null 2>&1; then
  recovery_chaos_service_active="$(sudo systemctl is-active "$RECOVERY_CHAOS_SERVICE_NAME" 2>/dev/null || true)"
  [[ -n "$recovery_chaos_service_active" ]] || recovery_chaos_service_active="unknown"
  echo "recovery_chaos_service_active=$recovery_chaos_service_active"
fi
if sudo systemctl cat "$LOG_MAINTENANCE_TIMER_NAME" >/dev/null 2>&1; then
  log_maintenance_timer_enabled="$(sudo systemctl is-enabled "$LOG_MAINTENANCE_TIMER_NAME" 2>/dev/null || true)"
  log_maintenance_timer_active="$(sudo systemctl is-active "$LOG_MAINTENANCE_TIMER_NAME" 2>/dev/null || true)"
  [[ -n "$log_maintenance_timer_enabled" ]] || log_maintenance_timer_enabled="unknown"
  [[ -n "$log_maintenance_timer_active" ]] || log_maintenance_timer_active="unknown"
  echo "log_maintenance_timer_enabled=$log_maintenance_timer_enabled log_maintenance_timer_active=$log_maintenance_timer_active"
fi
if sudo systemctl cat "$DISCORD_ROUTE_GUARD_TIMER_NAME" >/dev/null 2>&1; then
  DISCORD_ROUTE_GUARD_TIMER_INSTALLED=1
  discord_route_guard_timer_enabled="$(sudo systemctl is-enabled "$DISCORD_ROUTE_GUARD_TIMER_NAME" 2>/dev/null || true)"
  discord_route_guard_timer_active="$(sudo systemctl is-active "$DISCORD_ROUTE_GUARD_TIMER_NAME" 2>/dev/null || true)"
  [[ -n "$discord_route_guard_timer_enabled" ]] || discord_route_guard_timer_enabled="unknown"
  [[ -n "$discord_route_guard_timer_active" ]] || discord_route_guard_timer_active="unknown"
  echo "discord_route_guard_timer_enabled=$discord_route_guard_timer_enabled discord_route_guard_timer_active=$discord_route_guard_timer_active"
fi
if sudo systemctl cat "$DISCORD_ROUTE_GUARD_SERVICE_NAME" >/dev/null 2>&1; then
  discord_route_guard_service_active="$(sudo systemctl is-active "$DISCORD_ROUTE_GUARD_SERVICE_NAME" 2>/dev/null || true)"
  [[ -n "$discord_route_guard_service_active" ]] || discord_route_guard_service_active="unknown"
  echo "discord_route_guard_service_active=$discord_route_guard_service_active"
fi
if sudo systemctl cat "$STALE_METRICS_DRILL_TIMER_NAME" >/dev/null 2>&1; then
  stale_metrics_drill_timer_enabled="$(sudo systemctl is-enabled "$STALE_METRICS_DRILL_TIMER_NAME" 2>/dev/null || true)"
  stale_metrics_drill_timer_active="$(sudo systemctl is-active "$STALE_METRICS_DRILL_TIMER_NAME" 2>/dev/null || true)"
  [[ -n "$stale_metrics_drill_timer_enabled" ]] || stale_metrics_drill_timer_enabled="unknown"
  [[ -n "$stale_metrics_drill_timer_active" ]] || stale_metrics_drill_timer_active="unknown"
  echo "stale_metrics_drill_timer_enabled=$stale_metrics_drill_timer_enabled stale_metrics_drill_timer_active=$stale_metrics_drill_timer_active"
fi
if sudo systemctl cat "$STALE_METRICS_DRILL_SERVICE_NAME" >/dev/null 2>&1; then
  stale_metrics_drill_service_active="$(sudo systemctl is-active "$STALE_METRICS_DRILL_SERVICE_NAME" 2>/dev/null || true)"
  [[ -n "$stale_metrics_drill_service_active" ]] || stale_metrics_drill_service_active="unknown"
  echo "stale_metrics_drill_service_active=$stale_metrics_drill_service_active"
fi
if sudo systemctl cat "$ALPHA_SERVICE_NAME" >/dev/null 2>&1; then
  alpha_enabled="$(sudo systemctl is-enabled "$ALPHA_SERVICE_NAME" 2>/dev/null || true)"
  alpha_active="$(sudo systemctl is-active "$ALPHA_SERVICE_NAME" 2>/dev/null || true)"
  [[ -n "$alpha_enabled" ]] || alpha_enabled="unknown"
  [[ -n "$alpha_active" ]] || alpha_active="unknown"
  echo "alpha_enabled=$alpha_enabled alpha_active=$alpha_active"
fi
if sudo systemctl cat "$BREADTH_SERVICE_NAME" >/dev/null 2>&1; then
  breadth_enabled="$(sudo systemctl is-enabled "$BREADTH_SERVICE_NAME" 2>/dev/null || true)"
  breadth_active="$(sudo systemctl is-active "$BREADTH_SERVICE_NAME" 2>/dev/null || true)"
  [[ -n "$breadth_enabled" ]] || breadth_enabled="unknown"
  [[ -n "$breadth_active" ]] || breadth_active="unknown"
  echo "breadth_enabled=$breadth_enabled breadth_active=$breadth_active"
fi
if sudo systemctl cat "$ALPHA_SUMMARY_TIMER_NAME" >/dev/null 2>&1; then
  alpha_summary_timer_enabled="$(sudo systemctl is-enabled "$ALPHA_SUMMARY_TIMER_NAME" 2>/dev/null || true)"
  alpha_summary_timer_active="$(sudo systemctl is-active "$ALPHA_SUMMARY_TIMER_NAME" 2>/dev/null || true)"
  [[ -n "$alpha_summary_timer_enabled" ]] || alpha_summary_timer_enabled="unknown"
  [[ -n "$alpha_summary_timer_active" ]] || alpha_summary_timer_active="unknown"
  echo "alpha_summary_timer_enabled=$alpha_summary_timer_enabled alpha_summary_timer_active=$alpha_summary_timer_active"
fi

echo
echo "=== latest summaries ==="
for pat in \
  "kalshi_temperature_metar_summary_*.json" \
  "kalshi_temperature_settlement_state_*.json" \
  "kalshi_temperature_shadow_watch_summary_*.json" \
  "kalshi_temperature_live_readiness_*.json" \
  "kalshi_temperature_go_live_gate_*.json" \
  "kalshi_temperature_bankroll_validation_*.json" \
  "kalshi_temperature_alpha_gap_report_*.json"; do
  latest="$(ls -1t "$OUTPUT_DIR"/$pat 2>/dev/null | head -n 1 || true)"
  if [[ -z "$latest" ]]; then
    echo "$pat -> MISSING"
  else
    age="$(( $(date +%s) - $(date -r "$latest" +%s) ))"
    echo "$pat -> $(basename "$latest") age_sec=$age"
  fi
done
if [[ -f "$REPLAN_COOLDOWN_STATE_FILE" ]]; then
  replan_cooldown_value="$(python3 - "$REPLAN_COOLDOWN_STATE_FILE" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys


path = Path(sys.argv[1])
text = ""
try:
    text = path.read_text(encoding="utf-8").strip()
except Exception:
    text = ""
if not text:
    print("n/a")
    raise SystemExit(0)
try:
    direct_value = float(text)
except Exception:
    direct_value = None
if isinstance(direct_value, float):
    formatted = f"{direct_value:.3f}".rstrip("0").rstrip(".")
    print(formatted or "0")
    raise SystemExit(0)
try:
    payload = json.loads(text)
except Exception:
    print(text)
    raise SystemExit(0)
if not isinstance(payload, dict):
    print("n/a")
    raise SystemExit(0)
raw = payload.get("cooldown_minutes")
try:
    minutes = float(raw)
except Exception:
    print("n/a")
    raise SystemExit(0)
up_streak = payload.get("up_streak")
down_streak = payload.get("down_streak")
action = str(payload.get("adaptive_action") or "").strip() or "n/a"
reason = str(payload.get("adaptive_reason") or "").strip() or "n/a"
try:
    up_text = str(max(0, int(float(up_streak))))
except Exception:
    up_text = "0"
try:
    down_text = str(max(0, int(float(down_streak))))
except Exception:
    down_text = "0"
minutes_text = f"{minutes:.3f}".rstrip("0").rstrip(".")
print(f"{minutes_text or '0'} up_streak={up_text} down_streak={down_text} action={action} reason={reason}")
PY
)"
  replan_cooldown_age="$(( $(date +%s) - $(date -r "$REPLAN_COOLDOWN_STATE_FILE" +%s) ))"
  echo "adaptive_replan_cooldown_minutes -> ${replan_cooldown_value:-n/a} age_sec=$replan_cooldown_age"
else
  echo "adaptive_replan_cooldown_minutes -> MISSING"
fi
if [[ -f "$REPLAN_BACKSTOP_STATE_FILE" ]]; then
  replan_backstop_value="$(python3 - "$REPLAN_BACKSTOP_STATE_FILE" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = ""
try:
    text = path.read_text(encoding="utf-8").strip()
except Exception:
    text = ""
if not text:
    print("n/a")
    raise SystemExit(0)
try:
    payload = json.loads(text)
except Exception:
    print(text)
    raise SystemExit(0)
if not isinstance(payload, dict):
    print("n/a")
    raise SystemExit(0)
value = payload.get("min_orders_backstop")
last_unique_market_sides = payload.get("last_unique_market_sides")
stagnation_cycles = payload.get("stagnation_cycles")
adaptive_reason = payload.get("adaptive_reason")
try:
    value_text = str(int(float(value)))
except Exception:
    value_text = "n/a"
try:
    sides_text = str(int(float(last_unique_market_sides)))
except Exception:
    sides_text = "n/a"
try:
    stagnation_text = str(max(0, int(float(stagnation_cycles))))
except Exception:
    stagnation_text = "0"
reason_text = str(adaptive_reason or "").strip() or "n/a"
print(
    f"{value_text} last_unique_market_sides={sides_text} "
    f"stagnation_cycles={stagnation_text} reason={reason_text}"
)
PY
)"
  replan_backstop_age="$(( $(date +%s) - $(date -r "$REPLAN_BACKSTOP_STATE_FILE" +%s) ))"
  echo "adaptive_replan_backstop_min_orders -> ${replan_backstop_value:-n/a} age_sec=$replan_backstop_age"
else
  echo "adaptive_replan_backstop_min_orders -> MISSING"
fi
alpha_summary_latest_health="$OUTPUT_DIR/health/alpha_summary_latest.json"
if [[ -f "$alpha_summary_latest_health" ]]; then
  age="$(( $(date +%s) - $(date -r "$alpha_summary_latest_health" +%s) ))"
  echo "alpha_summary_latest.json -> $(basename "$alpha_summary_latest_health") age_sec=$age"
else
  echo "alpha_summary_latest.json -> MISSING"
fi

echo
echo "=== checkpoint windows ==="
for pat in \
  "station_tuning_window_4h_*.json" \
  "profitability_4h_*.json" \
  "station_tuning_window_14h_*.json" \
  "profitability_14h_*.json" \
  "alpha_summary_12h_*.json"; do
  latest="$(ls -1t "$CHECKPOINTS_DIR"/$pat 2>/dev/null | head -n 1 || true)"
  if [[ -z "$latest" ]]; then
    echo "$pat -> MISSING"
  else
    age="$(( $(date +%s) - $(date -r "$latest" +%s) ))"
    echo "$pat -> $(basename "$latest") age_sec=$age"
  fi
done

echo
echo "=== quick gates ==="
if [[ -f "$HEALTH_STATUS_FILE" ]]; then
  if live_status_mtime="$(date -r "$HEALTH_STATUS_FILE" +%s 2>/dev/null)"; then
    now_epoch="$(date +%s)"
    if [[ "$live_status_mtime" =~ ^[0-9]+$ && "$now_epoch" =~ ^[0-9]+$ ]] && (( now_epoch >= live_status_mtime )); then
      live_status_age_seconds="$(( now_epoch - live_status_mtime ))"
    fi
  fi
  if (( HAS_JQ == 1 )); then
    if jq -e . "$HEALTH_STATUS_FILE" >/dev/null 2>&1; then
      overall_status="$(jq -r '.status // "unknown"' "$HEALTH_STATUS_FILE")"
      jq -r '
      "live_status status=\(.status // "unknown") approvals_resumed=\(.trigger_flags.approvals_resumed // false) planned_orders_resumed=\(.trigger_flags.planned_orders_resumed // false) shadow_resolved_first=\(.trigger_flags.shadow_resolved_first // false) shadow_basis=\(.trigger_flags.resolved_shadow_basis // "none") shadow_basis_value_14h=\(.trigger_flags.resolved_shadow_basis_value_14h // 0) effective_max_markets=\(.scan_budget.effective_max_markets // "n/a") next_max_markets=\(.scan_budget.next_max_markets // "n/a") adaptive_action=\(.scan_budget.adaptive_decision_action // "hold") adaptive_reason=\(.scan_budget.adaptive_decision_reason // "n/a") scan_cap_bound_with_headroom=\(.scan_budget.scan_cap_bound_with_headroom // false) load_per_vcpu=\(.scan_budget.load_per_vcpu // "n/a") intents_total_hint=\(.scan_budget.intents_total_hint // "n/a") settlement_pressure_active=\(.settlement_refresh_plan.pressure_active // false) settlement_blocked_rows_rolling=\(.settlement_refresh_plan.settlement_finalization_blocked_count // 0) settlement_blocked_underlyings=\(.settlement_refresh_plan.settlement_finalization_blocked_underlyings // 0) settlement_pending_count=\(.settlement_refresh_plan.pending_final_report_count // 0) settlement_unresolved_now=\(((.settlement_refresh_plan.settlement_finalization_blocked_underlyings // 0) + (.settlement_refresh_plan.pending_final_report_count // 0))) settlement_blocked_rate_rolling=\(.settlement_refresh_plan.settlement_finalization_blocked_rate // 0) settlement_blocked_rate_actionable=\(if (.settlement_refresh_plan.pressure_active // false) then (.settlement_refresh_plan.settlement_finalization_blocked_rate // 0) else 0 end) settlement_refresh_seconds=\(.settlement_refresh_plan.refresh_seconds_effective // 0) settlement_top_n=\(.settlement_refresh_plan.top_n_effective // 0) freshness_pressure_active=\(.freshness_plan.pressure_active // false) stale_count=\(.freshness_plan.metar_observation_stale_count // 0) stale_rate=\(.freshness_plan.metar_observation_stale_rate // 0) approval_rate=\(.freshness_plan.approval_rate // 0) range_possible_count=\(.freshness_plan.yes_range_still_possible_count // 0) range_possible_rate=\(.freshness_plan.yes_range_still_possible_rate // 0) range_possible_rate_effective=\(.freshness_plan.yes_range_still_possible_rate_effective // 0) metar_timeout=\(.freshness_plan.metar_timeout_seconds_effective // 0) metar_retries=\(.freshness_plan.metar_retry_attempts_effective // 0) interval_gap_effective=\(.interval_gap_control.effective_max_yes_possible_gap_for_yes_side // 0) interval_gap_next=\(.interval_gap_control.next_max_yes_possible_gap_for_yes_side // 0) interval_gap_action=\(.interval_gap_control.adaptive_action // "hold") interval_gap_reason=\(.interval_gap_control.adaptive_reason // "n/a") interval_gap_range_rate_effective=\(.interval_gap_control.range_possible_rate_effective // 0) replan_cooldown_effective=\(.replan_cooldown.effective_minutes // 0) replan_cooldown_next=\(.replan_cooldown.next_minutes // 0) replan_min_backstop=\(.replan_cooldown.min_orders_backstop_effective // 0) replan_next_min_backstop=\(.replan_cooldown.next_min_orders_backstop // 0) replan_stagnation_cycles=\(.replan_cooldown.stagnation_cycles // 0) replan_action=\(.replan_cooldown.adaptive_action // "hold") replan_reason=\(.replan_cooldown.adaptive_reason // "n/a") replan_blocked=\(.replan_cooldown.blocked_count // 0)/\(.replan_cooldown.input_count // 0) replan_blocked_ratio=\(.replan_cooldown.blocked_ratio // 0) replan_override_ratio=\(.replan_cooldown.override_ratio // 0) replan_backstop=\(.replan_cooldown.backstop_released_count // 0) replan_unique_market_sides=\(.replan_cooldown.unique_market_sides // 0) replan_unique_underlyings=\(.replan_cooldown.unique_underlyings // 0) attempts_metar=\(.command_execution.metar_attempts // 0) attempts_settlement=\(.command_execution.settlement_attempts // 0) attempts_shadow=\(.command_execution.shadow_attempts // 0)"
      ' "$HEALTH_STATUS_FILE"
      jq -r '
        "live_status reasons red=\((.red_reasons // []) | join(",")) yellow=\((.yellow_reasons // []) | join(","))"
      ' "$HEALTH_STATUS_FILE"
    else
      overall_status="unknown"
      echo "live_status -> PARSE_ERROR ($HEALTH_STATUS_FILE)"
    fi
  else
    echo "live_status -> present (install jq for parsed output): $HEALTH_STATUS_FILE"
  fi
else
  echo "live_status -> MISSING ($HEALTH_STATUS_FILE)"
fi
overall_status="$(normalize_status_token "${overall_status:-unknown}")"

latest_readiness="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_live_readiness_*.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_readiness" && "$HAS_JQ" == "1" ]]; then
  if jq -e . "$latest_readiness" >/dev/null 2>&1; then
    jq -r '
      "live_readiness recommendation=\(.overall_live_readiness.recommendation // "unknown") small=\(.overall_live_readiness.ready_for_small_live_pilot // false) scaled=\(.overall_live_readiness.ready_for_scaled_live // false)"
    ' "$latest_readiness"
  else
    echo "live_readiness_latest -> PARSE_ERROR ($latest_readiness)"
  fi
fi

readiness_runner_state="$OUTPUT_DIR/health/readiness_runner_latest.json"
if [[ -f "$readiness_runner_state" && "$HAS_JQ" == "1" ]]; then
  runner_age="$(( $(date +%s) - $(date -r "$readiness_runner_state" +%s) ))"
  if jq -e . "$readiness_runner_state" >/dev/null 2>&1; then
    jq -r --arg age "$runner_age" '
      "readiness_runner status=\(.run_status // "unknown") stage=\(.stage // "unknown") message=\(.message // "") nonfatal=\((.nonfatal_stage_failures // []) | join(",")) age_sec=" + $age
    ' "$readiness_runner_state"
  else
    echo "readiness_runner_latest -> PARSE_ERROR ($readiness_runner_state) age_sec=$runner_age"
  fi
fi

latest_gate="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_go_live_gate_*.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_gate" && "$HAS_JQ" == "1" ]]; then
  if jq -e . "$latest_gate" >/dev/null 2>&1; then
    jq -r '
      "go_live_gate status=\(.gate_status // "unknown") recommendation=\(.recommendation // "unknown") earliest=\(.earliest_passing_horizon // "") failed_horizons=\(.failed_horizon_count // 0)"
    ' "$latest_gate"
  else
    echo "go_live_gate_latest -> PARSE_ERROR ($latest_gate)"
  fi
fi

latest_alpha_dashboard="$(ls -1t "$OUTPUT_DIR"/alpha_workers/alpha_worker_dashboard_latest.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_alpha_dashboard" && "$HAS_JQ" == "1" ]]; then
  jq -r '
    "alpha_dashboard profiles=\(.explorer_profiles.rankings.profile_count // 0) approved_delta=\(.explorer_profiles.delta_relaxed_minus_default.approved_delta_relaxed_minus_default // 0) resolved_unique_market_sides=\(.conservative_headline.metrics.resolved_unique_market_sides // 0) roi_ref=\(.conservative_headline.metrics.roi_on_reference_bankroll // 0) live_ready=\(.latest_reports.live_readiness.ready_for_small_live_pilot // false) gate=\(.latest_reports.go_live_gate.gate_status // "unknown") main_limit=\(.conservative_headline.metrics.main_limiting_factor // "unknown") next_layer=\(.conservative_headline.metrics.next_missing_alpha_layer // "")"
  ' "$latest_alpha_dashboard"
fi

latest_breadth_dashboard="$(ls -1t "$OUTPUT_DIR"/breadth_worker/breadth_worker_dashboard_latest.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_breadth_dashboard" && "$HAS_JQ" == "1" ]]; then
  jq -r '
    "breadth_dashboard profiles=\(.profile_count // 0) union_market_sides_approved=\(.union_metrics.unique_market_sides_approved_rows // 0) union_market_tickers=\(.union_metrics.unique_market_tickers_all_rows // 0) consensus_candidates=\(.consensus.consensus_candidate_count // 0) parallelism=\(.adaptive_parallelism.current_parallelism // 0) p_load_milli=\(.adaptive_parallelism.load_per_vcpu_milli // 0) p_action=\(.adaptive_guidance.parallelism.action // "hold") max_markets=\(.adaptive_max_markets.current_max_markets // 0) m_load_milli=\(.adaptive_max_markets.load_per_vcpu_milli // 0) m_action=\(.adaptive_guidance.max_markets.action // "hold") target_pressure_level=\(.adaptive_max_markets.target_pressure.level // 0) target_pressure_reason=\(.adaptive_max_markets.target_pressure.reason // "targets_met") scan_secs=\(.adaptive_max_markets.last_constraint_scan_duration_seconds // 0)"
  ' "$latest_breadth_dashboard"
fi

latest_settlement="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_settlement_state_*.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_settlement" && "$HAS_JQ" == "1" ]]; then
  jq -r '
    "settlement_state lookups attempted=\(.final_report_lookup_attempted // 0) ready=\(.final_report_ready_count // 0) pending=\(.final_report_pending_count // 0) errors=\(.final_report_error_count // 0)"
  ' "$latest_settlement"
fi

latest_bankroll="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_bankroll_validation_*.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_bankroll" && "$HAS_JQ" == "1" ]]; then
  jq -r '
    "bankroll_validation meaningful_deploy=\(.viability_summary.could_reference_bankroll_have_been_deployed_meaningfully // false) roi_ref=\(.viability_summary.what_return_would_have_been_produced_on_bankroll // 0) main_limit=\(.viability_summary.main_limiting_factor // "unknown")"
  ' "$latest_bankroll"
  jq -r '
    "bankroll_breadth resolved_market_sides=\(.opportunity_breadth.resolved_unique_market_sides // 0) deployed_market_sides=\(.viability_summary.deployed_unique_market_side_calls // 0) resolved_families=\(.opportunity_breadth.resolved_unique_underlying_families // 0) repeated_entry_multiplier=\(.opportunity_breadth.repeated_entry_multiplier // 0)"
  ' "$latest_bankroll"
  jq -r '
    "bankroll_hysa beats_hysa=\(.viability_summary.would_plausibly_beat_hysa_after_slippage_fees // false) excess_window=\(.viability_summary.excess_return_over_hysa_for_window // 0) hysa_window=\(.viability_summary.equivalent_window_hysa_return_on_reference_bankroll // 0)"
  ' "$latest_bankroll"
  jq -r '
    "bankroll_concentration warning=\(.concentration_checks.concentration_warning // false) top_market_side_share=\(.concentration_checks.concentration_metrics.top_market_side_share // 0) duplicate_count=\(.concentration_checks.duplicate_count // 0) duplicate_warnings_truncated=\(.concentration_checks.duplicate_warning_stats.warnings_truncated // 0)"
  ' "$latest_bankroll"
fi

latest_tuning_14h="$(ls -1t "$CHECKPOINTS_DIR"/station_tuning_window_14h_*.json 2>/dev/null | head -n 1 || true)"
latest_profitability_14h="$(ls -1t "$CHECKPOINTS_DIR"/profitability_14h_*.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_tuning_14h" && -n "$latest_profitability_14h" ]]; then
  echo "alpha_focus_14h source_tuning=$(basename "$latest_tuning_14h") source_profitability=$(basename "$latest_profitability_14h")"
  python3 - "$latest_tuning_14h" "$latest_profitability_14h" "${latest_bankroll:-}" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path


def _load(path: str) -> dict:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _fmt_pct(value: float | int | None) -> str:
    try:
        return f"{float(value) * 100.0:.2f}%"
    except Exception:
        return "n/a"


tuning = _load(sys.argv[1])
profit = _load(sys.argv[2])
bankroll = _load(sys.argv[3]) if len(sys.argv) > 3 else {}

rates = tuning.get("rates") if isinstance(tuning.get("rates"), dict) else {}
approval_rate = rates.get("approval_rate")
stale_rate = rates.get("stale_block_rate")
print(f"alpha_focus_14h rates approval={_fmt_pct(approval_rate)} stale={_fmt_pct(stale_rate)}")

reason_counts = tuning.get("policy_reason_counts") if isinstance(tuning.get("policy_reason_counts"), dict) else {}
top_blockers = sorted(
    ((str(k), int(v)) for k, v in reason_counts.items() if str(k) != "approved"),
    key=lambda item: item[1],
    reverse=True,
)[:5]
settlement_backlog = (
    bankroll.get("data_quality", {}).get("settlement_backlog_now")
    if isinstance(bankroll.get("data_quality"), dict)
    else {}
)
current_settlement_unresolved = int(
    settlement_backlog.get("current_settlement_unresolved") or 0
)
if current_settlement_unresolved <= 0 and top_blockers:
    filtered = [item for item in top_blockers if item[0] != "settlement_finalization_blocked"]
    if filtered:
        top_blockers = filtered
if top_blockers:
    formatted = ", ".join(f"{reason}:{count}" for reason, count in top_blockers)
    print(f"alpha_focus_14h top_blockers {formatted}")
else:
    print("alpha_focus_14h top_blockers n/a")

attr = profit.get("attribution") if isinstance(profit.get("attribution"), dict) else {}
shadow = (
    profit.get("shadow_settled_reference")
    if isinstance(profit.get("shadow_settled_reference"), dict)
    else {}
)
headline = shadow.get("headline") if isinstance(shadow.get("headline"), dict) else {}
resolved_unique_market_sides = int(headline.get("resolved_predictions") or 0)
unique_market_side_win_rate = headline.get("win_rate")
resolved_rows = int(shadow.get("resolved_planned_rows") or 0)
row_win_rate = shadow.get("selection_win_rate_resolved_rows")
try:
    unique_market_side_win_rate_fmt = _fmt_pct(unique_market_side_win_rate)
except Exception:
    unique_market_side_win_rate_fmt = "n/a"
try:
    row_win_rate_fmt = _fmt_pct(row_win_rate)
except Exception:
    row_win_rate_fmt = "n/a"
print(
    "alpha_focus_14h settled_quality "
    f"basis=unique_market_side resolved={resolved_unique_market_sides} "
    f"win_rate={unique_market_side_win_rate_fmt} "
    f"rows_audit_only={resolved_rows} rows_win_rate={row_win_rate_fmt}"
)

by_station = attr.get("by_station") if isinstance(attr.get("by_station"), dict) else {}
station_rows: list[tuple[str, float, int]] = []
for station, row in by_station.items():
    if not isinstance(row, dict):
        continue
    intents_total = int(row.get("intents_total") or 0)
    if intents_total < 500:
        continue
    approval = float(row.get("approval_rate") or 0.0)
    station_rows.append((str(station), approval, intents_total))
station_rows.sort(key=lambda item: (item[1], -item[2], item[0]))
if station_rows:
    formatted = "; ".join(f"{s}:ar={a*100:.2f}% intents={n}" for s, a, n in station_rows[:5])
    print(f"alpha_focus_14h weakest_stations {formatted}")
else:
    print("alpha_focus_14h weakest_stations n/a")

by_hour = attr.get("by_local_hour") if isinstance(attr.get("by_local_hour"), dict) else {}
hour_rows: list[tuple[str, float, int]] = []
for hour, row in by_hour.items():
    if not isinstance(row, dict):
        continue
    intents_total = int(row.get("intents_total") or 0)
    if intents_total < 500:
        continue
    approval = float(row.get("approval_rate") or 0.0)
    hour_rows.append((str(hour), approval, intents_total))
hour_rows.sort(key=lambda item: (item[1], -item[2], item[0]))
if hour_rows:
    formatted = "; ".join(f"h{h}:ar={a*100:.2f}% intents={n}" for h, a, n in hour_rows[:5])
    print(f"alpha_focus_14h weakest_hours {formatted}")
else:
    print("alpha_focus_14h weakest_hours n/a")
PY
fi

latest_blocker_audit="$(ls -1t "$CHECKPOINTS_DIR"/blocker_audit_*_latest.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_blocker_audit" && "$HAS_JQ" == "1" ]]; then
  jq -r '
    "blocker_audit_latest file=\(.source_files.window_summary_file // "unknown") top=\(.headline.largest_blocker_reason // "none") count=\(.headline.largest_blocker_count // 0) blocked_total=\(.headline.blocked_total // 0)"
  ' "$latest_blocker_audit"
  jq -r '
    (.top_blockers[0] // {}) as $top
    | "blocker_audit_action " + ($top.reason_human // "none") + ": " + ($top.recommended_action // "n/a")
  ' "$latest_blocker_audit"
fi

if [[ -f "$alpha_summary_latest_health" && "$HAS_JQ" == "1" ]]; then
  if jq -e . "$alpha_summary_latest_health" >/dev/null 2>&1; then
    alpha_summary_available="1"
    if alpha_summary_mtime="$(date -r "$alpha_summary_latest_health" +%s 2>/dev/null)"; then
      now_epoch="$(date +%s)"
      if [[ "$alpha_summary_mtime" =~ ^[0-9]+$ && "$now_epoch" =~ ^[0-9]+$ ]] && (( now_epoch >= alpha_summary_mtime )); then
        alpha_summary_age_seconds="$(( now_epoch - alpha_summary_mtime ))"
      fi
    fi
    alpha_summary_health_status="$(jq -r '.health.status // .headline_metrics.health_status // "unknown"' "$alpha_summary_latest_health")"
    alpha_summary_health_reason="$(jq -r '.health.reason_text // .headline_metrics.health_reason_text // ""' "$alpha_summary_latest_health")"
    alpha_summary_payload_consistent="$(jq -r '
      if ((.headline_metrics | type) == "object") and (.headline_metrics | has("approval_auto_apply_payload_consistent")) then
        .headline_metrics.approval_auto_apply_payload_consistent
      else
        true
      end
    ' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_message_quality_pass="$(jq -r '
      if ((.headline_metrics | type) == "object") and (.headline_metrics | has("message_quality_overall_pass")) then
        .headline_metrics.message_quality_overall_pass
      else
        true
      end
    ' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_trader_payload_consistent="$(jq -r '
      if ((.headline_metrics | type) == "object") and (.headline_metrics | has("trader_view_payload_consistent")) then
        .headline_metrics.trader_view_payload_consistent
      else
        true
      end
    ' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_quality_gate_source="$(jq -r '.headline_metrics.quality_gate_source // "unknown"' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_quality_gate_auto_applied="$(jq -r '.headline_metrics.quality_gate_auto_applied // false' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_quality_gate_min_probability_confidence="$(jq -r '.headline_metrics.quality_gate_min_probability_confidence // ""' "$alpha_summary_latest_health")"
    alpha_summary_quality_gate_min_expected_edge_net="$(jq -r '.headline_metrics.quality_gate_min_expected_edge_net // ""' "$alpha_summary_latest_health")"
    alpha_summary_auto_apply_enabled="$(jq -r '.approval_auto_apply.enabled // false' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_auto_apply_should_apply="$(jq -r '.approval_auto_apply.should_apply // false' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_auto_apply_applied_in_this_run="$(jq -r '.approval_auto_apply.applied_in_this_run // false' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_auto_apply_released_in_this_run="$(jq -r '.approval_auto_apply.released_in_this_run // false' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_auto_apply_apply_reason="$(jq -r '.approval_auto_apply.apply_reason // "unknown"' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_auto_apply_profile_path="$(jq -r '.approval_auto_apply.profile_path // ""' "$alpha_summary_latest_health")"
    jq -r '
      "alpha_summary_latest health=\(.health.status // .headline_metrics.health_status // "unknown") confidence=\(.headline_metrics.confidence_level // "unknown") approvals=\(.headline_metrics.intents_approved // 0)/\(.headline_metrics.intents_total // 0) planned=\(.headline_metrics.planned_orders // 0) breadth=\(.headline_metrics.resolved_unique_market_sides // 0) settled_wins=\(.headline_metrics.settled_unique_market_side_wins // 0) settled_losses=\(.headline_metrics.settled_unique_market_side_losses // 0) settled_total=\(.headline_metrics.settled_unique_market_side_total // .headline_metrics.settled_unique_market_side_resolved_predictions // 0) settled_pnl_if_live=\(.headline_metrics.settled_unique_market_side_counterfactual_pnl_dollars_if_live // 0) last_settled_ticker=\(.headline_metrics.last_resolved_unique_market_side.market_ticker // "n/a") last_settled_side=\(.headline_metrics.last_resolved_unique_market_side.side // "n/a") last_settled_win=\(.headline_metrics.last_resolved_unique_market_side.win // "n/a") last_settled_pnl=\(.headline_metrics.last_resolved_unique_market_side.counterfactual_pnl_dollars_if_live // "n/a") trial_balance=\(.headline_metrics.trial_balance_current_dollars // .trial_balance_current_dollars // .trial_balance_current // "n/a") trial_balance_delta=\(.headline_metrics.trial_balance_growth_dollars // .headline_metrics.trial_balance_delta_dollars // .trial_balance_delta_total // "n/a") expected_edge_estimate=\(.headline_metrics.expected_shadow_edge_total // .expected_shadow_edge_total // "n/a") impact_basis_label=\(.trader_view.suggestion_impact_pool_basis_label // .headline_metrics.suggestion_impact_pool_basis_label // "n/a") projection_basis=\(if ((.headline_metrics.settled_unique_market_side_resolved_predictions // 0) > 0) then "settled_counterfactual" else "provisional_no_settled" end) trader_view_present=\((.trader_view | type) == "object") discord_structured=\((.discord_summary_structured | type) == "object") dup_rows_since_reset=\(.headline_metrics.trial_duplicate_rows_since_reset // 0) dup_ratio_since_reset=\(.headline_metrics.trial_duplicate_rows_ratio_since_reset // 0) replan_blocked=\(.headline_metrics.replan_cooldown_blocked_count // 0)/\(.headline_metrics.replan_cooldown_input_count // 0) replan_blocked_ratio=\(.headline_metrics.replan_cooldown_blocked_ratio // 0) replan_override=\(.headline_metrics.replan_cooldown_override_count // 0) replan_backstop=\(.headline_metrics.replan_cooldown_backstop_released_count // 0) replan_uniques=\(.headline_metrics.replan_cooldown_unique_market_sides // 0)/\(.headline_metrics.replan_cooldown_unique_underlyings // 0) projected_pnl=\(.headline_metrics.projected_pnl_on_reference_bankroll_dollars // 0) beat_hysa=\(.headline_metrics.beat_hysa // false) limiting_factor=\(.headline_metrics.display_limiting_factor // .headline_metrics.limiting_factor // "unknown") quality_risk=\(.headline_metrics.approval_quality_risk_alert_active // false) quality_risk_level=\(.headline_metrics.approval_quality_risk_alert_level // "none") quality_risk_streak=\(.headline_metrics.approval_quality_risk_streak // 0) auto_mode=\(.headline_metrics.approval_auto_apply_recommendation_mode // "n/a") auto_payload_consistent=\(.headline_metrics.approval_auto_apply_payload_consistent // true) trader_payload_consistent=\(.headline_metrics.trader_view_payload_consistent // true) msg_quality_pass=\(.headline_metrics.message_quality_overall_pass // true) msg_quality_fail_count=\(.headline_metrics.message_quality_failed_check_count // 0) auto_quality_risk_projected_streak=\(.headline_metrics.approval_auto_apply_quality_risk_projected_streak_for_auto // 0) auto_quality_risk_persistent=\(.headline_metrics.approval_auto_apply_quality_risk_persistent_for_auto // false) auto_strict_guardrail_max=\(.headline_metrics.approval_auto_apply_strict_guardrail_max // 0) edge_gate_share=\(.headline_metrics.edge_gate_blocked_share_of_blocked // 0) actions_improving=\(.suggestion_tracking_summary.counts.improving // 0) actions_stalled=\(.suggestion_tracking_summary.counts.stalled // 0) actions_regressing=\(.suggestion_tracking_summary.counts.regressing // 0) actions_closed=\(.suggestion_tracking_summary.counts.closed // 0) actions_new=\(.suggestion_tracking_summary.counts.new // 0) actions_escalated=\(.suggestion_tracking_summary.escalated_count // 0) checklist_count=\(.action_checklist_summary.count // 0) checklist_escalated=\(.action_checklist_summary.escalated_count // 0) checklist_next_due=\(.action_checklist_summary.next_due_at_utc // "n/a") checklist_owners=\((.action_checklist_summary.owners // []) | join(",")) top_action_key=\(.action_checklist_top[0].key // "n/a") top_action_owner=\(.action_checklist_top[0].owner // "n/a") top_action_escalated=\(.action_checklist_top[0].escalated // false) top_action_due=\(.action_checklist_top[0].due_at_utc // "n/a")"
    ' "$alpha_summary_latest_health"
    jq -r '
      "alpha_summary_trader_view decision=\(.trader_view.decision_now // "n/a") mode=\(.trader_view.mode // "n/a") confidence=\(.trader_view.confidence_score // "n/a") band=\(.trader_view.confidence_band // "n/a") recommendation=\(.trader_view.live_recommendation // "n/a") approval_rate=\(.trader_view.approval_rate // "n/a") plan_conversion=\(.trader_view.plan_conversion_rate // "n/a") top_blocker=\(.trader_view.top_blocker_reason // "n/a") blocker_share=\(.trader_view.top_blocker_share_of_blocked // "n/a") freshness=\(.trader_view.freshness_block_rate // "n/a") breadth=\(.trader_view.resolved_unique_market_sides // 0)/\(.trader_view.unresolved_unique_market_sides // 0) projected_pnl=\(.trader_view.projected_pnl_reference_bankroll_dollars // "n/a") checkin_1d=\(.trader_view.checkin_pnl_1d_dollars // "n/a") checkin_7d=\(.trader_view.checkin_pnl_7d_dollars // "n/a") checkin_\(.trader_view.checkin_month_window_label // "30d")=\(.trader_view.checkin_month_pnl_dollars // "n/a") settled_available=\(.trader_view.settled_predictions_available // false) settled_win_rate=\(.trader_view.settled_prediction_win_rate // "n/a")"
    ' "$alpha_summary_latest_health"
    jq -r '
      "alpha_summary_trader_actions "
      + "1=\((.trader_view.next_actions_top3[0] // "n/a")) ; "
      + "2=\((.trader_view.next_actions_top3[1] // "n/a")) ; "
      + "3=\((.trader_view.next_actions_top3[2] // "n/a"))"
    ' "$alpha_summary_latest_health"
    alpha_summary_trader_view_present="$(jq -r '(.trader_view | type) == "object"' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    alpha_summary_trader_consistent="$(jq -r '
      def asnum: if type == "number" then . else null end;
      (.trader_view // {}) as $tv
      | (.headline_metrics // {}) as $hm
      | ($tv.approval_rate | asnum) as $tv_ar
      | ($hm.approval_rate | asnum) as $hm_ar
      | ($tv.confidence_score | asnum) as $tv_cs
      | ($hm.deployment_confidence_score | asnum) as $hm_cs
      | (
          (($tv | type) == "object")
          and (($tv.mode // "") | type == "string")
          and (($tv.mode // "") | test("^(shadow_only|live)$"))
          and (($tv.decision_now // "") | type == "string")
          and (($tv.decision_now // "") | length > 0)
          and (($tv.live_recommendation // "") == ($hm.live_recommendation // ""))
          and ($tv_ar != null and $hm_ar != null and (($tv_ar - $hm_ar) | abs) <= 0.000001)
          and ($tv_cs != null and $hm_cs != null and (($tv_cs - $hm_cs) | abs) <= 0.001)
        )
    ' "$alpha_summary_latest_health" | tr '[:upper:]' '[:lower:]')"
    echo "alpha_summary_trader_consistency present=$alpha_summary_trader_view_present consistent=$alpha_summary_trader_consistent"
    jq -r '
      "alpha_summary_live_readiness recommendation=\(.headline_metrics.live_recommendation // "n/a") small_live=\(.headline_metrics.ready_for_small_live_pilot // false) scaled_live=\(.headline_metrics.ready_for_scaled_live // false) earliest=\(.headline_metrics.earliest_passing_horizon // "n/a") confidence_score=\(.headline_metrics.deployment_confidence_score // "n/a") uncapped_score=\(.headline_metrics.deployment_confidence_score_uncapped // "n/a") band=\(.headline_metrics.deployment_confidence_band // "n/a") pilot_gap=\(.headline_metrics.pilot_gap_effective_points // "n/a") pilot_checks=\(.headline_metrics.pilot_checks_passed // "n/a")/\(.headline_metrics.pilot_checks_total // "n/a") pilot_open=\(.headline_metrics.pilot_checks_open // "n/a") pilot_flips=\(.headline_metrics.pilot_minimum_flips_needed // "n/a") pilot_top_open=\(.headline_metrics.pilot_top_open_reason // "n/a") cap_reason=\(.headline_metrics.deployment_confidence_cap_reason // "none") top_blocker=\(.headline_metrics.top_live_readiness_blocker_reason // "n/a") 1d=\(.live_readiness_horizons."1d".readiness_status // "n/a")/\(.live_readiness_horizons."1d".gates.gate_score // "n/a") 14d=\(.live_readiness_horizons."14d".readiness_status // "n/a")/\(.live_readiness_horizons."14d".gates.gate_score // "n/a") 28d=\(.live_readiness_horizons."28d".readiness_status // "n/a")/\(.live_readiness_horizons."28d".gates.gate_score // "n/a") 1yr=\(.live_readiness_horizons."1yr".readiness_status // "n/a")/\(.live_readiness_horizons."1yr".gates.gate_score // "n/a")"
    ' "$alpha_summary_latest_health"
  else
    alpha_summary_available="0"
    alpha_summary_health_status="unknown"
    alpha_summary_health_reason="parse_error"
    echo "alpha_summary_latest.json -> PARSE_ERROR ($alpha_summary_latest_health)"
  fi
fi
alpha_summary_health_status="$(normalize_status_token "${alpha_summary_health_status:-unknown}")"
alpha_summary_health_reason="$(printf '%s' "${alpha_summary_health_reason:-}" | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g' | sed 's/^ //; s/ $//')"
alpha_summary_payload_consistent="$(normalize_boolean_token "${alpha_summary_payload_consistent:-unknown}" "false")"
alpha_summary_message_quality_pass="$(normalize_boolean_token "${alpha_summary_message_quality_pass:-unknown}" "false")"
alpha_summary_quality_gate_source="$(normalize_status_token "${alpha_summary_quality_gate_source:-unknown}")"
alpha_summary_quality_gate_auto_applied="$(normalize_boolean_token "${alpha_summary_quality_gate_auto_applied:-false}" "false")"
alpha_summary_auto_apply_enabled="$(normalize_boolean_token "${alpha_summary_auto_apply_enabled:-false}" "false")"
alpha_summary_auto_apply_should_apply="$(normalize_boolean_token "${alpha_summary_auto_apply_should_apply:-false}" "false")"
alpha_summary_auto_apply_applied_in_this_run="$(normalize_boolean_token "${alpha_summary_auto_apply_applied_in_this_run:-false}" "false")"
alpha_summary_auto_apply_released_in_this_run="$(normalize_boolean_token "${alpha_summary_auto_apply_released_in_this_run:-false}" "false")"
alpha_summary_auto_apply_apply_reason="$(normalize_status_token "${alpha_summary_auto_apply_apply_reason:-unknown}")"
alpha_summary_trader_payload_consistent="$(normalize_boolean_token "${alpha_summary_trader_payload_consistent:-unknown}" "false")"
if [[ -n "$alpha_summary_auto_apply_profile_path" && "$alpha_summary_auto_apply_profile_path" != "null" ]]; then
  AUTO_PROFILE_PATH="$alpha_summary_auto_apply_profile_path"
fi
if [[ -f "$AUTO_PROFILE_PATH" ]]; then
  if auto_profile_mtime="$(date -r "$AUTO_PROFILE_PATH" +%s 2>/dev/null)"; then
    now_epoch="$(date +%s)"
    if [[ "$auto_profile_mtime" =~ ^[0-9]+$ && "$now_epoch" =~ ^[0-9]+$ ]] && (( now_epoch >= auto_profile_mtime )); then
      auto_profile_age_seconds="$(( now_epoch - auto_profile_mtime ))"
    fi
  fi
fi

latest_recovery="$(ls -1t "$OUTPUT_DIR"/health/recovery/recovery_latest.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_recovery" && "$HAS_JQ" == "1" ]]; then
  jq -r '
    "recovery_latest issue_detected=\(.issue_detected // false) issue_remaining=\(.issue_remaining // false) resolved_in_run=\(.resolved_in_run // false) health_status=\(.health.status // "unknown") pipeline_status=\(.pipeline.status // "unknown") freshness_pressure_active=\(.freshness.pressure_active // false) freshness_stale_rate=\(.freshness.stale_rate // 0) freshness_approval_rate=\(.freshness.approval_rate // 0) consecutive_freshness_pressure=\(.state.consecutive_freshness_pressure // 0) consecutive_retry_pressure=\(.state.consecutive_retry_pressure // 0) attempts_metar=\(.execution_attempts.metar // 0) attempts_shadow=\(.execution_attempts.shadow // 0) scan_effective_max_markets=\(.scan_budget.effective_max_markets // 0) scan_next_max_markets=\(.scan_budget.next_max_markets // 0) scan_action=\(.scan_budget.adaptive_action // "") alpha_worker_state=\(.service_states.alpha_worker_service // "unknown") breadth_worker_state=\(.service_states.breadth_worker_service // "unknown") reporting_activating_age_seconds=\(.service_states.reporting_service_activating_age_seconds // -1) log_maintenance_health=\(.log_maintenance.health_status // "unknown") log_maintenance_age_seconds=\(.log_maintenance.age_seconds // -1) log_maintenance_timer_state=\(.service_states.log_maintenance_timer // "unknown") log_maintenance_timer_enabled=\(.service_states.log_maintenance_timer_enabled // "unknown") last_log_maintenance_timer_enable_epoch=\(.state.last_log_maintenance_timer_enable_epoch // 0) reasons=\((.decision_reasons // []) | join(",")) actions=\((.actions_attempted // []) | join(","))"
  ' "$latest_recovery"
fi

latest_chaos="$(ls -1t "$OUTPUT_DIR"/health/recovery/chaos_check_latest.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_chaos" && "$HAS_JQ" == "1" ]]; then
  jq -r '
    "recovery_chaos_latest mode=\(.mode // "unknown") passed=\(.passed // false) pre_shadow=\(.pre_shadow_state // "unknown") post_shadow=\(.post_shadow_state // "unknown") failure_reason=\(.failure_reason // "")"
  ' "$latest_chaos"
fi

latest_stale_metrics_drill="$(ls -1t "$OUTPUT_DIR"/recovery_chaos/stale_metrics_drill/stale_metrics_drill_latest.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_stale_metrics_drill" && "$HAS_JQ" == "1" ]]; then
  stale_metrics_drill_age="$(( $(date +%s) - $(date -r "$latest_stale_metrics_drill" +%s) ))"
  jq -r --arg age "$stale_metrics_drill_age" '
    "stale_metrics_drill_latest status=\(.status // "unknown") age_sec=\($age) cycle_count=\(.metrics.cycle_count // 0) blocker_stale_cycles=\(.metrics.blocker_metrics_stale_cycle_count // 0) disallowed_max_markets_hits=\(.metrics.disallowed_max_markets_reason_hits // 0) check_no_stale_data_driven_max_markets=\(.checks.no_stale_data_driven_max_markets_reasons // false)"
  ' "$latest_stale_metrics_drill"
fi

latest_log_maintenance="$(ls -1t "$OUTPUT_DIR"/health/log_maintenance/log_maintenance_latest.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_log_maintenance" && "$HAS_JQ" == "1" ]]; then
  if jq -e . "$latest_log_maintenance" >/dev/null 2>&1; then
    jq -r '
      "log_maintenance_latest health=\(.health_status // "unknown") usage_gib=\(.usage.log_dir_gib // 0) compressed=\(.compressed_count // 0) pruned=\(.pruned_count // 0) logrotate_rule_present=\(.policy.logrotate_rule_present // false)"
    ' "$latest_log_maintenance"
  else
    echo "log_maintenance_latest -> PARSE_ERROR ($latest_log_maintenance)"
  fi
fi

latest_discord_route_guard="$(ls -1t "$OUTPUT_DIR"/health/discord_route_guard/discord_route_guard_latest.json 2>/dev/null | head -n 1 || true)"
if [[ -n "$latest_discord_route_guard" && "$HAS_JQ" == "1" ]]; then
  discord_route_guard_age_seconds="$(( $(date +%s) - $(date -r "$latest_discord_route_guard" +%s) ))"
  if jq -e . "$latest_discord_route_guard" >/dev/null 2>&1; then
    discord_route_guard_status="$(jq -r '.guard_status // "unknown"' "$latest_discord_route_guard" 2>/dev/null || echo "unknown")"
    discord_route_guard_status="$(normalize_status_token "${discord_route_guard_status:-unknown}")"
    discord_route_guard_shared_route_group_count="$(jq -r '.shared_route_group_count // 0' "$latest_discord_route_guard" 2>/dev/null || echo "0")"
    discord_route_guard_route_hint="$(jq -r '.route_remediations[0].route_hint // .shared_route_groups[0].route_hint // ""' "$latest_discord_route_guard" 2>/dev/null || true)"
    discord_route_guard_required_thread_keys="$(jq -r '[.route_remediations[]?.required_thread_env_keys[]?] | unique | join(",")' "$latest_discord_route_guard" 2>/dev/null || true)"
    jq -r --arg age "$discord_route_guard_age_seconds" '
      "discord_route_guard_latest status=\(.guard_status // "unknown") age_sec=\($age) strict=\(.strict_mode // false) shared_route_groups=\(.shared_route_group_count // 0)"
    ' "$latest_discord_route_guard"
    if [[ -n "$discord_route_guard_route_hint" || -n "$discord_route_guard_required_thread_keys" ]]; then
      echo "discord_route_guard_remediation route_hint=${discord_route_guard_route_hint:-n/a} required_thread_env_keys=${discord_route_guard_required_thread_keys:-n/a}"
    fi
  else
    discord_route_guard_status="unknown"
    discord_route_guard_shared_route_group_count="0"
    discord_route_guard_route_hint=""
    discord_route_guard_required_thread_keys=""
    echo "discord_route_guard_latest -> PARSE_ERROR ($latest_discord_route_guard) age_sec=$discord_route_guard_age_seconds"
  fi
fi

log_maintenance_alert_state="$OUTPUT_DIR/health/log_maintenance/log_maintenance_alert_state.json"
if [[ -f "$log_maintenance_alert_state" && "$HAS_JQ" == "1" ]]; then
  if jq -e . "$log_maintenance_alert_state" >/dev/null 2>&1; then
    last_alert_epoch="$(jq -r '.last_alert_epoch // 0' "$log_maintenance_alert_state" 2>/dev/null || echo "0")"
    now_epoch="$(date +%s)"
    alert_age_seconds="n/a"
    if [[ "$last_alert_epoch" =~ ^[0-9]+$ ]] && (( last_alert_epoch > 0 )) && (( now_epoch >= last_alert_epoch )); then
      alert_age_seconds="$((now_epoch - last_alert_epoch))"
    fi
    jq -r --arg alert_age_seconds "$alert_age_seconds" '
      "log_maintenance_alert_state last_status=\(.last_status // "unknown") last_alert_age_seconds=\($alert_age_seconds) dedupe_fingerprint_set=\(((.last_fingerprint // "") | length) > 0)"
    ' "$log_maintenance_alert_state"
  else
    echo "log_maintenance_alert_state -> PARSE_ERROR ($log_maintenance_alert_state)"
  fi
fi

decision_matrix_lane_strict_statuses_normalized="$(printf '%s' "$DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
if [[ -z "$decision_matrix_lane_strict_statuses_normalized" ]]; then
  decision_matrix_lane_strict_statuses_normalized="matrix_failed,bootstrap_blocked"
fi
if [[ -f "$DECISION_MATRIX_LANE_ALERT_STATE_FILE" ]]; then
  decision_matrix_lane_state_available="1"
  if lane_state_mtime="$(date -r "$DECISION_MATRIX_LANE_ALERT_STATE_FILE" +%s 2>/dev/null)"; then
    now_epoch="$(date +%s)"
    if [[ "$lane_state_mtime" =~ ^[0-9]+$ && "$now_epoch" =~ ^[0-9]+$ ]] && (( now_epoch >= lane_state_mtime )); then
      decision_matrix_lane_state_age_seconds="$(( now_epoch - lane_state_mtime ))"
    fi
  fi
  lane_state_vars="$(python3 - "$DECISION_MATRIX_LANE_ALERT_STATE_FILE" "$decision_matrix_lane_strict_statuses_normalized" <<'PY'
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
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    payload = {}
if not isinstance(payload, dict):
    payload = {}
status = _normalize(payload.get("last_lane_status")) or "unknown"
degraded_streak_count = payload.get("degraded_streak_count")
degraded_streak_threshold = payload.get("degraded_streak_threshold")
degraded_streak_notify_every = payload.get("degraded_streak_notify_every")
last_notify_reason = _normalize(payload.get("last_notify_reason")) or "none"
try:
    streak_count = max(0, int(float(degraded_streak_count)))
except Exception:
    streak_count = 0
try:
    streak_threshold = max(0, int(float(degraded_streak_threshold)))
except Exception:
    streak_threshold = 0
try:
    streak_notify_every = max(0, int(float(degraded_streak_notify_every)))
except Exception:
    streak_notify_every = 0
strict_status_match = status in strict_statuses if strict_statuses else False
print(f"status={status}")
print(f"degraded_streak_count={streak_count}")
print(f"degraded_streak_threshold={streak_threshold}")
print(f"degraded_streak_notify_every={streak_notify_every}")
print(f"last_notify_reason={last_notify_reason}")
print(f"strict_status_match={'true' if strict_status_match else 'false'}")
PY
)"
  while IFS='=' read -r key value; do
    case "$key" in
      status) decision_matrix_lane_status="$value" ;;
      degraded_streak_count) decision_matrix_lane_degraded_streak_count="$value" ;;
      degraded_streak_threshold) decision_matrix_lane_degraded_streak_threshold="$value" ;;
      degraded_streak_notify_every) decision_matrix_lane_degraded_streak_notify_every="$value" ;;
      last_notify_reason) decision_matrix_lane_last_notify_reason="$value" ;;
      strict_status_match) decision_matrix_lane_strict_status_match="$value" ;;
    esac
  done <<< "$lane_state_vars"
  echo "decision_matrix_lane_alert_state status=${decision_matrix_lane_status:-unknown} degraded_streak=${decision_matrix_lane_degraded_streak_count:-0} threshold=${decision_matrix_lane_degraded_streak_threshold:-0} every=${decision_matrix_lane_degraded_streak_notify_every:-0} strict_statuses=${decision_matrix_lane_strict_statuses_normalized:-n/a} strict_match=${decision_matrix_lane_strict_status_match:-false} notify_reason=${decision_matrix_lane_last_notify_reason:-none} age_sec=${decision_matrix_lane_state_age_seconds:-n/a}"
else
  echo "decision_matrix_lane_alert_state -> MISSING ($DECISION_MATRIX_LANE_ALERT_STATE_FILE)"
fi

echo
echo "=== tail logs ==="
tail -n 20 "$OUTPUT_DIR/logs/contract_specs_loop.log" 2>/dev/null || true
tail -n 20 "$OUTPUT_DIR/logs/metar_loop.log" 2>/dev/null || true
tail -n 20 "$OUTPUT_DIR/logs/settlement_loop.log" 2>/dev/null || true
tail -n 20 "$OUTPUT_DIR/logs/shadow_loop.log" 2>/dev/null || true

echo
echo "=== journal tail ==="
sudo journalctl -u "$SERVICE_NAME" -n 30 --no-pager || true

if (( STRICT_MODE == 1 )); then
  alpha_worker_expected="$(normalize_binary_flag "${ALPHA_WORKER_ENABLED:-0}" "0")"
  breadth_worker_expected="$(normalize_binary_flag "${BREADTH_WORKER_ENABLED:-0}" "0")"
  stale_metrics_drill_expected="$(normalize_binary_flag "${STALE_METRICS_DRILL_TIMER_EXPECTED:-0}" "0")"
  if (( DISCORD_ROUTE_GUARD_TIMER_INSTALLED == 1 )); then
    discord_route_guard_expected_default="1"
  else
    discord_route_guard_expected_default="0"
  fi
  if [[ -n "${DISCORD_ROUTE_GUARD_TIMER_EXPECTED+x}" ]]; then
    discord_route_guard_expected="$(normalize_binary_flag "${DISCORD_ROUTE_GUARD_TIMER_EXPECTED}" "$discord_route_guard_expected_default")"
  else
    discord_route_guard_expected="$discord_route_guard_expected_default"
  fi
  discord_route_guard_fail_on_collision_default="0"
  if [[ "$discord_route_guard_expected" == "1" ]]; then
    discord_route_guard_fail_on_collision_default="1"
  fi
  if [[ -n "${DISCORD_ROUTE_GUARD_FAIL_ON_COLLISION+x}" ]]; then
    discord_route_guard_fail_on_collision_legacy="$(normalize_binary_flag "${DISCORD_ROUTE_GUARD_FAIL_ON_COLLISION}" "$discord_route_guard_fail_on_collision_default")"
  else
    discord_route_guard_fail_on_collision_legacy="$discord_route_guard_fail_on_collision_default"
  fi
  if [[ -n "${DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION+x}" ]]; then
    discord_route_guard_fail_on_collision="$(normalize_binary_flag "${DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION}" "$discord_route_guard_fail_on_collision_legacy")"
  elif [[ -n "${DISCORD_ROUTE_GUARD_FAIL_ON_COLLISION+x}" ]]; then
    discord_route_guard_fail_on_collision="$discord_route_guard_fail_on_collision_legacy"
  else
    discord_route_guard_fail_on_collision="$discord_route_guard_fail_on_collision_default"
  fi
  alpha_worker_active="$(sudo systemctl is-active "$ALPHA_SERVICE_NAME" 2>/dev/null || true)"
  breadth_worker_active="$(sudo systemctl is-active "$BREADTH_SERVICE_NAME" 2>/dev/null || true)"
  stale_metrics_drill_timer_active="$(sudo systemctl is-active "$STALE_METRICS_DRILL_TIMER_NAME" 2>/dev/null || true)"
  stale_metrics_drill_timer_enabled="$(sudo systemctl is-enabled "$STALE_METRICS_DRILL_TIMER_NAME" 2>/dev/null || true)"
  discord_route_guard_timer_active="$(sudo systemctl is-active "$DISCORD_ROUTE_GUARD_TIMER_NAME" 2>/dev/null || true)"
  discord_route_guard_timer_enabled="$(sudo systemctl is-enabled "$DISCORD_ROUTE_GUARD_TIMER_NAME" 2>/dev/null || true)"
  [[ -n "$alpha_worker_active" ]] || alpha_worker_active="unknown"
  [[ -n "$breadth_worker_active" ]] || breadth_worker_active="unknown"
  [[ -n "$stale_metrics_drill_timer_active" ]] || stale_metrics_drill_timer_active="unknown"
  [[ -n "$stale_metrics_drill_timer_enabled" ]] || stale_metrics_drill_timer_enabled="unknown"
  [[ -n "$discord_route_guard_timer_active" ]] || discord_route_guard_timer_active="unknown"
  [[ -n "$discord_route_guard_timer_enabled" ]] || discord_route_guard_timer_enabled="unknown"

  if [[ "$overall_status" == "red" ]]; then
    echo
    echo "STRICT CHECK FAILED: live_status is red" >&2
    exit 2
  fi
  if [[ "$overall_status" == "yellow" ]]; then
    echo
    echo "STRICT CHECK WARNING: live_status is yellow" >&2
    exit 1
  fi
  if [[ "$overall_status" != "green" ]]; then
    echo
    echo "STRICT CHECK FAILED: live_status is unknown/non-green (status=$overall_status)" >&2
    exit 2
  fi
  if [[ "$live_status_age_seconds" =~ ^[0-9]+$ ]] && [[ "$LIVE_STATUS_STRICT_MAX_AGE_SECONDS" =~ ^[0-9]+$ ]] && (( live_status_age_seconds > LIVE_STATUS_STRICT_MAX_AGE_SECONDS )); then
    echo
    echo "STRICT CHECK FAILED: live_status artifact stale (age=${live_status_age_seconds}s > ${LIVE_STATUS_STRICT_MAX_AGE_SECONDS}s)" >&2
    exit 2
  fi
  if [[ "$alpha_worker_expected" == "1" && "$alpha_worker_active" != "active" ]]; then
    echo
    echo "STRICT CHECK FAILED: alpha worker service expected but not active (state=$alpha_worker_active)" >&2
    exit 2
  fi
  if [[ "$breadth_worker_expected" == "1" && "$breadth_worker_active" != "active" ]]; then
    echo
    echo "STRICT CHECK FAILED: breadth worker service expected but not active (state=$breadth_worker_active)" >&2
    exit 2
  fi
  if [[ "$stale_metrics_drill_expected" == "1" && "$stale_metrics_drill_timer_active" != "active" ]]; then
    echo
    echo "STRICT CHECK FAILED: stale-metrics drill timer expected but not active (state=$stale_metrics_drill_timer_active)" >&2
    exit 2
  fi
  if [[ "$stale_metrics_drill_expected" == "1" && "$stale_metrics_drill_timer_enabled" != "enabled" ]]; then
    echo
    echo "STRICT CHECK FAILED: stale-metrics drill timer expected but not enabled (state=$stale_metrics_drill_timer_enabled)" >&2
    exit 2
  fi
  if [[ "$discord_route_guard_expected" == "1" && "$discord_route_guard_timer_active" != "active" ]]; then
    echo
    echo "STRICT CHECK FAILED: discord-route-guard timer expected but not active (state=$discord_route_guard_timer_active)" >&2
    exit 2
  fi
  if [[ "$discord_route_guard_expected" == "1" && "$discord_route_guard_timer_enabled" != "enabled" ]]; then
    echo
    echo "STRICT CHECK FAILED: discord-route-guard timer expected but not enabled (state=$discord_route_guard_timer_enabled)" >&2
    exit 2
  fi
  if [[ "$discord_route_guard_expected" == "1" && "$discord_route_guard_age_seconds" =~ ^[0-9]+$ ]] && [[ "$DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS" =~ ^[0-9]+$ ]] && (( discord_route_guard_age_seconds > DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS )); then
    echo
    echo "STRICT CHECK FAILED: discord-route-guard artifact stale (age=${discord_route_guard_age_seconds}s > ${DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS}s)" >&2
    exit 2
  fi
  if [[ "$discord_route_guard_expected" == "1" && "$discord_route_guard_fail_on_collision" == "1" && "$discord_route_guard_status" != "green" ]]; then
    echo
    echo "STRICT CHECK FAILED: discord-route-guard indicates non-green route separation (status=$discord_route_guard_status, shared_route_groups=$discord_route_guard_shared_route_group_count)" >&2
    if [[ -n "$discord_route_guard_route_hint" || -n "$discord_route_guard_required_thread_keys" ]]; then
      echo "STRICT CHECK REMEDIATION: route_hint=${discord_route_guard_route_hint:-n/a} required_thread_env_keys=${discord_route_guard_required_thread_keys:-n/a}" >&2
    fi
    echo "STRICT CHECK REMEDIATION: edit $DISCORD_THREAD_MAP_PATH and set the required *_WEBHOOK_THREAD_ID values." >&2
    echo "STRICT CHECK REMEDIATION: preflight map with $DISCORD_THREAD_MAP_CHECK_CMD" >&2
    echo "STRICT CHECK REMEDIATION: preflight+apply with $DISCORD_THREAD_MAP_PRECHECK_APPLY_CMD" >&2
    echo "STRICT CHECK REMEDIATION: run $DISCORD_THREAD_MAP_APPLY_CMD" >&2
    exit 2
  fi
  if [[ "$alpha_summary_available" != "1" ]]; then
    echo
    echo "STRICT CHECK FAILED: alpha summary artifact unavailable (missing file or jq unavailable)" >&2
    exit 2
  fi
  if [[ "$alpha_summary_age_seconds" =~ ^[0-9]+$ ]] && [[ "$ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS" =~ ^[0-9]+$ ]] && (( alpha_summary_age_seconds > ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS )); then
    echo
    echo "STRICT CHECK FAILED: alpha summary artifact stale (age=${alpha_summary_age_seconds}s > ${ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS}s)" >&2
    exit 2
  fi
  if [[ "$alpha_summary_health_status" == "unknown" || -z "$alpha_summary_health_status" ]]; then
    echo
    echo "STRICT CHECK FAILED: alpha summary health status unknown" >&2
    exit 2
  fi
  if [[ "$alpha_summary_health_status" == "red" ]]; then
    echo
    echo "STRICT CHECK FAILED: alpha summary health is red" >&2
    exit 2
  fi
  if [[ "$alpha_summary_health_status" == "yellow" ]]; then
    echo
    if [[ -n "$alpha_summary_health_reason" ]]; then
      echo "STRICT CHECK WARNING: alpha summary health is yellow (reason: $alpha_summary_health_reason)" >&2
    else
      echo "STRICT CHECK WARNING: alpha summary health is yellow" >&2
    fi
    exit 1
  fi
  if [[ "$alpha_summary_payload_consistent" != "true" ]]; then
    echo
    echo "STRICT CHECK FAILED: alpha summary payload consistency failed (approval_auto_apply payload mismatch)" >&2
    exit 2
  fi
  if [[ "$alpha_summary_message_quality_pass" != "true" ]]; then
    echo
    echo "STRICT CHECK FAILED: alpha summary message quality checks failed" >&2
    exit 2
  fi
  if [[ "$alpha_summary_trader_payload_consistent" != "true" ]]; then
    echo
    echo "STRICT CHECK FAILED: alpha summary trader_view payload consistency failed" >&2
    exit 2
  fi
  if [[ "$alpha_summary_trader_view_present" != "true" ]]; then
    echo
    echo "STRICT CHECK FAILED: alpha summary trader_view block missing" >&2
    exit 2
  fi
  if [[ "$alpha_summary_trader_consistent" != "true" ]]; then
    echo
    echo "STRICT CHECK FAILED: alpha summary trader_view consistency check failed" >&2
    exit 2
  fi
  strict_lane_threshold="${DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD:-0}"
  if [[ ! "$strict_lane_threshold" =~ ^[0-9]+$ ]]; then
    strict_lane_threshold=0
  fi
  if [[ "$DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE" == "1" && "$decision_matrix_lane_state_available" != "1" ]]; then
    echo
    echo "STRICT CHECK FAILED: decision-matrix lane state file required but missing ($DECISION_MATRIX_LANE_ALERT_STATE_FILE)" >&2
    exit 2
  fi
  if [[ "$strict_lane_threshold" =~ ^[0-9]+$ ]] && (( strict_lane_threshold > 0 )); then
    if [[ "$decision_matrix_lane_strict_status_match" == "true" && "$decision_matrix_lane_degraded_streak_count" =~ ^[0-9]+$ ]] && (( decision_matrix_lane_degraded_streak_count >= strict_lane_threshold )); then
      echo
      echo "STRICT CHECK FAILED: decision-matrix lane degraded streak active (status=$decision_matrix_lane_status streak=${decision_matrix_lane_degraded_streak_count} threshold=${strict_lane_threshold} statuses=${decision_matrix_lane_strict_statuses_normalized})" >&2
      exit 2
    fi
  fi
  if [[ "$AUTO_PROFILE_EXPECTED" == "1" ]]; then
    auto_profile_required_now="0"
    if [[ "$alpha_summary_auto_apply_released_in_this_run" == "true" || "$alpha_summary_auto_apply_apply_reason" == released_profile* ]]; then
      auto_profile_required_now="0"
    elif [[ "$alpha_summary_auto_apply_enabled" == "true" && ( "$alpha_summary_auto_apply_should_apply" == "true" || "$alpha_summary_auto_apply_applied_in_this_run" == "true" ) ]]; then
      auto_profile_required_now="1"
    elif [[ ( "$alpha_summary_quality_gate_source" == "auto_profile" || "$alpha_summary_quality_gate_source" == auto_profile* ) && "$alpha_summary_quality_gate_auto_applied" == "true" ]]; then
      auto_profile_required_now="1"
    fi
    if [[ "$auto_profile_required_now" == "1" && ! -f "$AUTO_PROFILE_PATH" ]]; then
      echo
      echo "STRICT CHECK FAILED: auto profile required but missing ($AUTO_PROFILE_PATH)" >&2
      exit 2
    fi
    if [[ "$auto_profile_required_now" == "1" && "$auto_profile_age_seconds" =~ ^[0-9]+$ ]] && [[ "$AUTO_PROFILE_STRICT_MAX_AGE_SECONDS" =~ ^[0-9]+$ ]] && (( auto_profile_age_seconds > AUTO_PROFILE_STRICT_MAX_AGE_SECONDS )); then
      echo
      echo "STRICT CHECK FAILED: auto profile stale (age=${auto_profile_age_seconds}s > ${AUTO_PROFILE_STRICT_MAX_AGE_SECONDS}s)" >&2
      exit 2
    fi
    if [[ "$auto_profile_required_now" == "1" && "$alpha_summary_quality_gate_source" != "auto_profile" && "$alpha_summary_quality_gate_source" != auto_profile* ]]; then
      echo
      echo "STRICT CHECK FAILED: auto profile required but alpha summary source is '$alpha_summary_quality_gate_source'" >&2
      exit 2
    fi
    if [[ "$auto_profile_required_now" == "1" && "$alpha_summary_quality_gate_auto_applied" != "true" ]]; then
      echo
      echo "STRICT CHECK FAILED: auto profile required but alpha summary reports auto_applied=$alpha_summary_quality_gate_auto_applied" >&2
      exit 2
    fi
    if [[ "$auto_profile_required_now" == "1" && ! "$alpha_summary_quality_gate_min_probability_confidence" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
      echo
      echo "STRICT CHECK FAILED: auto profile required but min probability confidence missing/non-numeric" >&2
      exit 2
    fi
    if [[ "$auto_profile_required_now" == "1" && ! "$alpha_summary_quality_gate_min_expected_edge_net" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
      echo
      echo "STRICT CHECK FAILED: auto profile required but min expected edge missing/non-numeric" >&2
      exit 2
    fi
  fi
fi
