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
: "${BETBOT_ENV_FILE:?BETBOT_ENV_FILE is required}"

PYTHON_BIN="$BETBOT_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing python venv executable: $PYTHON_BIN" >&2
  exit 1
fi
if [[ ! -f "$BETBOT_ENV_FILE" ]]; then
  echo "missing betbot credentials env file: $BETBOT_ENV_FILE" >&2
  exit 1
fi

ALPHA_WORKER_ENABLED="${ALPHA_WORKER_ENABLED:-1}"
if [[ "$ALPHA_WORKER_ENABLED" != "1" ]]; then
  echo "alpha workers disabled (ALPHA_WORKER_ENABLED=$ALPHA_WORKER_ENABLED)"
  exit 0
fi

ALPHA_WORKER_LOOP_SECONDS="${ALPHA_WORKER_LOOP_SECONDS:-60}"
ALPHA_WORKER_DIR="${ALPHA_WORKER_DIR:-$OUTPUT_DIR/alpha_workers}"
AUTO_METAR_POLICY_PATH="${AUTO_METAR_POLICY_PATH:-$OUTPUT_DIR/runtime/metar_age_policy_auto.json}"

ALPHA_POLICY_REFRESH_SECONDS="${ALPHA_POLICY_REFRESH_SECONDS:-300}"
ALPHA_POLICY_LOOKBACK_HOURS="${ALPHA_POLICY_LOOKBACK_HOURS:-14}"
ALPHA_POLICY_BASE_MAX_AGE_MINUTES="${ALPHA_POLICY_BASE_MAX_AGE_MINUTES:-${MAX_METAR_AGE_MINUTES:-22.5}}"
ALPHA_POLICY_HARD_CAP_MINUTES="${ALPHA_POLICY_HARD_CAP_MINUTES:-35.0}"
ALPHA_POLICY_MIN_STALE_COUNT="${ALPHA_POLICY_MIN_STALE_COUNT:-20}"
ALPHA_POLICY_MIN_TOTAL_COUNT="${ALPHA_POLICY_MIN_TOTAL_COUNT:-30}"
ALPHA_POLICY_MIN_STALE_RATE="${ALPHA_POLICY_MIN_STALE_RATE:-0.30}"
ALPHA_POLICY_MIN_INCREMENT_MINUTES="${ALPHA_POLICY_MIN_INCREMENT_MINUTES:-1.0}"
ALPHA_POLICY_LIFT_MINUTES="${ALPHA_POLICY_LIFT_MINUTES:-1.0}"
ALPHA_POLICY_APPROVED_SLACK_MINUTES="${ALPHA_POLICY_APPROVED_SLACK_MINUTES:-1.0}"
ALPHA_POLICY_TOP_N="${ALPHA_POLICY_TOP_N:-25}"
ALPHA_POLICY_FOCUS_STATIONS="${ALPHA_POLICY_FOCUS_STATIONS:-KNYC,KMDW,KLAS}"
ALPHA_POLICY_FOCUS_STATION_MIN_TOTAL_COUNT="${ALPHA_POLICY_FOCUS_STATION_MIN_TOTAL_COUNT:-20}"
ALPHA_POLICY_FOCUS_STATION_MIN_STALE_COUNT="${ALPHA_POLICY_FOCUS_STATION_MIN_STALE_COUNT:-10}"
ALPHA_POLICY_FOCUS_STATION_MIN_STALE_RATE="${ALPHA_POLICY_FOCUS_STATION_MIN_STALE_RATE:-0.20}"
ALPHA_POLICY_FOCUS_STATION_EXTRA_LIFT_MINUTES="${ALPHA_POLICY_FOCUS_STATION_EXTRA_LIFT_MINUTES:-1.0}"
ALPHA_POLICY_TAF_READY_BONUS_LIFT_MINUTES="${ALPHA_POLICY_TAF_READY_BONUS_LIFT_MINUTES:-0.75}"
ALPHA_POLICY_TAF_READY_MIN_RATE="${ALPHA_POLICY_TAF_READY_MIN_RATE:-0.50}"
ALPHA_POLICY_FORECAST_READY_BONUS_LIFT_MINUTES="${ALPHA_POLICY_FORECAST_READY_BONUS_LIFT_MINUTES:-0.50}"
ALPHA_POLICY_FORECAST_READY_MIN_RATE="${ALPHA_POLICY_FORECAST_READY_MIN_RATE:-0.50}"
ALPHA_POLICY_REQUIRE_TAF_OR_FORECAST_FOR_BONUS="${ALPHA_POLICY_REQUIRE_TAF_OR_FORECAST_FOR_BONUS:-1}"

ALPHA_EXPLORER_REFRESH_SECONDS="${ALPHA_EXPLORER_REFRESH_SECONDS:-180}"
ALPHA_EXPLORER_MAX_MARKETS="${ALPHA_EXPLORER_MAX_MARKETS:-1200}"
ALPHA_EXPLORER_MAX_ORDERS="${ALPHA_EXPLORER_MAX_ORDERS:-60}"
ALPHA_EXPLORER_MAX_INTENTS_PER_UNDERLYING="${ALPHA_EXPLORER_MAX_INTENTS_PER_UNDERLYING:-18}"
ALPHA_EXPLORER_MIN_SETTLEMENT_CONFIDENCE="${ALPHA_EXPLORER_MIN_SETTLEMENT_CONFIDENCE:-${MIN_SETTLEMENT_CONFIDENCE:-0.6}}"
ALPHA_EXPLORER_MIN_ALPHA_STRENGTH="${ALPHA_EXPLORER_MIN_ALPHA_STRENGTH:-0.0}"
ALPHA_EXPLORER_MIN_PROBABILITY_CONFIDENCE="${ALPHA_EXPLORER_MIN_PROBABILITY_CONFIDENCE:-${MIN_PROBABILITY_CONFIDENCE:-}}"
ALPHA_EXPLORER_MIN_EXPECTED_EDGE_NET="${ALPHA_EXPLORER_MIN_EXPECTED_EDGE_NET:-${MIN_EXPECTED_EDGE_NET:-}}"
ALPHA_EXPLORER_MIN_EDGE_TO_RISK_RATIO="${ALPHA_EXPLORER_MIN_EDGE_TO_RISK_RATIO:-${MIN_EDGE_TO_RISK_RATIO:-}}"
ALPHA_EXPLORER_ENFORCE_PROBABILITY_EDGE_THRESHOLDS="${ALPHA_EXPLORER_ENFORCE_PROBABILITY_EDGE_THRESHOLDS:-${ENFORCE_PROBABILITY_EDGE_THRESHOLDS:-1}}"
ALPHA_EXPLORER_ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR="${ALPHA_EXPLORER_ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR:-${ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR:-1}}"
ALPHA_EXPLORER_FALLBACK_MIN_PROBABILITY_CONFIDENCE="${ALPHA_EXPLORER_FALLBACK_MIN_PROBABILITY_CONFIDENCE:-${FALLBACK_MIN_PROBABILITY_CONFIDENCE:-}}"
ALPHA_EXPLORER_FALLBACK_MIN_EXPECTED_EDGE_NET="${ALPHA_EXPLORER_FALLBACK_MIN_EXPECTED_EDGE_NET:-${FALLBACK_MIN_EXPECTED_EDGE_NET:-0.005}}"
ALPHA_EXPLORER_FALLBACK_MIN_EDGE_TO_RISK_RATIO="${ALPHA_EXPLORER_FALLBACK_MIN_EDGE_TO_RISK_RATIO:-${FALLBACK_MIN_EDGE_TO_RISK_RATIO:-0.02}}"
ALPHA_EXPLORER_RELAXED_MAX_YES_GAP="${ALPHA_EXPLORER_RELAXED_MAX_YES_GAP:-3.0}"
ALPHA_EXPLORER_RELAXED_MAX_YES_GAP_WIDE="${ALPHA_EXPLORER_RELAXED_MAX_YES_GAP_WIDE:-6.0}"
ALPHA_EXPLORER_RELAXED_MAX_YES_GAP_ULTRA="${ALPHA_EXPLORER_RELAXED_MAX_YES_GAP_ULTRA:-8.0}"
ALPHA_EXPLORER_RELAXED_AGE_MINUTES="${ALPHA_EXPLORER_RELAXED_AGE_MINUTES:-30.0}"
ALPHA_EXPLORER_RELAXED_AGE_WIDE_MINUTES="${ALPHA_EXPLORER_RELAXED_AGE_WIDE_MINUTES:-34.0}"
ALPHA_EXPLORER_RELAXED_AGE_ULTRA_MINUTES="${ALPHA_EXPLORER_RELAXED_AGE_ULTRA_MINUTES:-36.0}"
ALPHA_EXPLORER_ENABLE_RELAXED_AGE="${ALPHA_EXPLORER_ENABLE_RELAXED_AGE:-1}"
ALPHA_EXPLORER_ENABLE_WIDE_PROFILE="${ALPHA_EXPLORER_ENABLE_WIDE_PROFILE:-1}"
ALPHA_EXPLORER_ENABLE_ULTRA_PROFILE="${ALPHA_EXPLORER_ENABLE_ULTRA_PROFILE:-1}"
ALPHA_EXPLORER_ENABLE_ULTRA_AGE_ONLY_PROFILE="${ALPHA_EXPLORER_ENABLE_ULTRA_AGE_ONLY_PROFILE:-1}"
ALPHA_EXPLORER_TAF_STALE_GRACE_MINUTES="${ALPHA_EXPLORER_TAF_STALE_GRACE_MINUTES:-${TAF_STALE_GRACE_MINUTES:-2.5}}"
ALPHA_EXPLORER_TAF_STALE_GRACE_MAX_VOLATILITY_SCORE="${ALPHA_EXPLORER_TAF_STALE_GRACE_MAX_VOLATILITY_SCORE:-${TAF_STALE_GRACE_MAX_VOLATILITY_SCORE:-1.0}}"
ALPHA_EXPLORER_TAF_STALE_GRACE_MAX_RANGE_WIDTH="${ALPHA_EXPLORER_TAF_STALE_GRACE_MAX_RANGE_WIDTH:-${TAF_STALE_GRACE_MAX_RANGE_WIDTH:-10.0}}"
ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO="${ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO:-${METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO:-0.92}}"
ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN="${ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN:-${METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN:-0.03}}"
ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN="${ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN:-${METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN:-0.005}}"
ALPHA_EXPLORER_PROFILE_PARALLEL="${ALPHA_EXPLORER_PROFILE_PARALLEL:-1}"
ALPHA_EXPLORER_PROFILE_PARALLELISM="${ALPHA_EXPLORER_PROFILE_PARALLELISM:-0}"
ALPHA_EXPLORER_TIMEOUT_SECONDS="${ALPHA_EXPLORER_TIMEOUT_SECONDS:-12}"
ALPHA_EXPLORER_TOP_N="${ALPHA_EXPLORER_TOP_N:-40}"
ALPHA_EXPLORER_MAX_MARKETS_ADAPTIVE_ENABLED="${ALPHA_EXPLORER_MAX_MARKETS_ADAPTIVE_ENABLED:-1}"
ALPHA_EXPLORER_MAX_MARKETS_MIN="${ALPHA_EXPLORER_MAX_MARKETS_MIN:-1200}"
ALPHA_EXPLORER_MAX_MARKETS_MAX="${ALPHA_EXPLORER_MAX_MARKETS_MAX:-$ALPHA_EXPLORER_MAX_MARKETS}"
ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_SECONDS="${ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_SECONDS:-18}"
ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_SLACK_SECONDS="${ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_SLACK_SECONDS:-5}"
ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_HARD_SLACK_SECONDS="${ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_HARD_SLACK_SECONDS:-10}"
ALPHA_EXPLORER_MAX_MARKETS_STEP_UP="${ALPHA_EXPLORER_MAX_MARKETS_STEP_UP:-220}"
ALPHA_EXPLORER_MAX_MARKETS_STEP_UP_FAST="${ALPHA_EXPLORER_MAX_MARKETS_STEP_UP_FAST:-420}"
ALPHA_EXPLORER_MAX_MARKETS_STEP_DOWN="${ALPHA_EXPLORER_MAX_MARKETS_STEP_DOWN:-180}"
ALPHA_EXPLORER_MAX_MARKETS_STEP_DOWN_FAST="${ALPHA_EXPLORER_MAX_MARKETS_STEP_DOWN_FAST:-360}"
ALPHA_EXPLORER_ADAPTIVE_LOAD_LOW_MILLI="${ALPHA_EXPLORER_ADAPTIVE_LOAD_LOW_MILLI:-360}"
ALPHA_EXPLORER_ADAPTIVE_LOAD_HIGH_MILLI="${ALPHA_EXPLORER_ADAPTIVE_LOAD_HIGH_MILLI:-860}"
ALPHA_EXPLORER_ADAPTIVE_LOAD_VERY_HIGH_MILLI="${ALPHA_EXPLORER_ADAPTIVE_LOAD_VERY_HIGH_MILLI:-960}"
ALPHA_EXPLORER_ADAPTIVE_MAX_MARKETS_STATE_FILE="${ALPHA_EXPLORER_ADAPTIVE_MAX_MARKETS_STATE_FILE:-$ALPHA_WORKER_DIR/.adaptive_explorer_max_markets.json}"

ALPHA_WS_COLLECT_ENABLED="${ALPHA_WS_COLLECT_ENABLED:-1}"
ALPHA_WS_COLLECT_REFRESH_SECONDS="${ALPHA_WS_COLLECT_REFRESH_SECONDS:-45}"
ALPHA_WS_COLLECT_RUN_SECONDS="${ALPHA_WS_COLLECT_RUN_SECONDS:-12}"
ALPHA_WS_COLLECT_MAX_EVENTS="${ALPHA_WS_COLLECT_MAX_EVENTS:-0}"
ALPHA_WS_COLLECT_CHANNELS="${ALPHA_WS_COLLECT_CHANNELS:-orderbook_snapshot,orderbook_delta,user_orders,user_fills,market_positions}"
ALPHA_WS_COLLECT_MARKET_TICKERS="${ALPHA_WS_COLLECT_MARKET_TICKERS:-}"
ALPHA_WS_COLLECT_CONNECT_TIMEOUT_SECONDS="${ALPHA_WS_COLLECT_CONNECT_TIMEOUT_SECONDS:-10}"
ALPHA_WS_COLLECT_READ_TIMEOUT_SECONDS="${ALPHA_WS_COLLECT_READ_TIMEOUT_SECONDS:-1}"
ALPHA_WS_COLLECT_PING_INTERVAL_SECONDS="${ALPHA_WS_COLLECT_PING_INTERVAL_SECONDS:-15}"
ALPHA_WS_COLLECT_FLUSH_STATE_EVERY_SECONDS="${ALPHA_WS_COLLECT_FLUSH_STATE_EVERY_SECONDS:-2}"
ALPHA_WS_COLLECT_RECONNECT_MAX_ATTEMPTS="${ALPHA_WS_COLLECT_RECONNECT_MAX_ATTEMPTS:-8}"
ALPHA_WS_COLLECT_RECONNECT_BACKOFF_SECONDS="${ALPHA_WS_COLLECT_RECONNECT_BACKOFF_SECONDS:-1}"
ALPHA_WS_STATE_MAX_AGE_SECONDS="${ALPHA_WS_STATE_MAX_AGE_SECONDS:-${WS_STATE_MAX_AGE_SECONDS:-30}}"

ALPHA_VALIDATION_REFRESH_SECONDS="${ALPHA_VALIDATION_REFRESH_SECONDS:-1800}"
ALPHA_VALIDATION_HOURS="${ALPHA_VALIDATION_HOURS:-14}"
ALPHA_VALIDATION_TOP_N="${ALPHA_VALIDATION_TOP_N:-10}"
ALPHA_VALIDATION_SLIPPAGE_BPS_LIST="${ALPHA_VALIDATION_SLIPPAGE_BPS_LIST:-0,5,10}"
ALPHA_VALIDATION_ROLLUPS_ENABLED="${ALPHA_VALIDATION_ROLLUPS_ENABLED:-0}"
ALPHA_READINESS_HORIZONS="${ALPHA_READINESS_HORIZONS:-1d,7d,14d,21d,28d,3mo,6mo,1yr}"

ALPHA_PREWARM_ENABLED="${ALPHA_PREWARM_ENABLED:-0}"
ALPHA_PREWARM_REFRESH_SECONDS="${ALPHA_PREWARM_REFRESH_SECONDS:-3600}"
ALPHA_PREWARM_HISTORY_CSV="${ALPHA_PREWARM_HISTORY_CSV:-$BETBOT_ROOT/outputs/kalshi_nonsports_history.csv}"
ALPHA_PREWARM_MAX_STATION_DAY_KEYS="${ALPHA_PREWARM_MAX_STATION_DAY_KEYS:-200}"
ALPHA_PREWARM_TIMEOUT_SECONDS="${ALPHA_PREWARM_TIMEOUT_SECONDS:-12}"

ALPHA_DASHBOARD_REFRESH_SECONDS="${ALPHA_DASHBOARD_REFRESH_SECONDS:-60}"
ALPHA_DASHBOARD_PATH="${ALPHA_DASHBOARD_PATH:-$ALPHA_WORKER_DIR/alpha_worker_dashboard_latest.json}"

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs" "$ALPHA_WORKER_DIR" "$(dirname "$AUTO_METAR_POLICY_PATH")"

LOG_FILE="$OUTPUT_DIR/logs/alpha_workers.log"
POLICY_SUMMARY_PATH="$ALPHA_WORKER_DIR/metar_policy_autotune_latest.json"
EXPLORER_INPUT_DIR="$ALPHA_WORKER_DIR/explorer_inputs"

cd "$BETBOT_ROOT"

run_cmd() {
  local label="$1"
  shift
  local start_ts end_ts rc start_epoch end_epoch duration_seconds
  start_ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  start_epoch="$(date +%s)"
  echo "[$start_ts] $label start" >> "$LOG_FILE"
  if "$@" >> "$LOG_FILE" 2>&1; then
    rc=0
  else
    rc=$?
  fi
  end_ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  end_epoch="$(date +%s)"
  duration_seconds="$(( end_epoch - start_epoch ))"
  if (( duration_seconds < 0 )); then
    duration_seconds=0
  fi
  echo "[$end_ts] $label end rc=$rc duration=$duration_seconds" >> "$LOG_FILE"
  if [[ "$label" == "explorer_constraint_scan" ]]; then
    LAST_EXPLORER_CONSTRAINT_SCAN_DURATION_SECONDS="$duration_seconds"
  fi
  return "$rc"
}

latest_file() {
  local pattern="$1"
  "$PYTHON_BIN" - "$pattern" <<'PY'
import glob
import os
import sys

pattern = sys.argv[1]
try:
    matches = glob.glob(pattern)
except Exception:
    matches = []
if not matches:
    raise SystemExit(0)
try:
    matches.sort(key=lambda path: os.path.getmtime(path), reverse=True)
except Exception:
    matches.sort(reverse=True)
print(matches[0])
PY
}

normalize_min_expected_edge_net() {
  local requested="$1"
  local fallback="$2"
  local enforce="$3"
  "$PYTHON_BIN" - "$requested" "$fallback" "$enforce" <<'PY'
from __future__ import annotations
import math
import sys
from typing import Any

def parse_float(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = float(text)
    except Exception:
        return None
    if not math.isfinite(parsed):
        return None
    return float(parsed)

def parse_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}

requested = parse_float(sys.argv[1])
fallback = parse_float(sys.argv[2])
enforce = parse_bool(sys.argv[3])

if enforce and isinstance(fallback, float) and fallback > 0.0:
    if requested is None or requested <= 0.0:
        requested = fallback

if isinstance(requested, float) and requested >= 0.0:
    print(f"{requested:.6f}")
PY
}

load_per_vcpu_milli() {
  local load_1m vcpus
  load_1m="$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo 0)"
  vcpus="$(nproc 2>/dev/null || echo 1)"
  "$PYTHON_BIN" - "$load_1m" "$vcpus" <<'PY'
import sys
try:
    load = float(sys.argv[1])
except Exception:
    load = 0.0
try:
    vcpus = int(sys.argv[2])
except Exception:
    vcpus = 1
if vcpus <= 0:
    vcpus = 1
milli = int(round((load / float(vcpus)) * 1000.0))
print(max(0, milli))
PY
}

compute_effective_explorer_max_markets() {
  local configured current next min_markets max_markets load_milli scan_duration
  local target_scan_seconds scan_slack_seconds scan_hard_slack_seconds
  local scan_low_bound scan_low_hard_bound scan_high_bound scan_high_hard_bound
  local step_up step_up_fast step_down step_down_fast
  configured="$ALPHA_EXPLORER_MAX_MARKETS"
  min_markets="$ALPHA_EXPLORER_MAX_MARKETS_MIN"
  max_markets="$ALPHA_EXPLORER_MAX_MARKETS_MAX"
  target_scan_seconds="$ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_SECONDS"
  scan_slack_seconds="$ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_SLACK_SECONDS"
  scan_hard_slack_seconds="$ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_HARD_SLACK_SECONDS"
  step_up="$ALPHA_EXPLORER_MAX_MARKETS_STEP_UP"
  step_up_fast="$ALPHA_EXPLORER_MAX_MARKETS_STEP_UP_FAST"
  step_down="$ALPHA_EXPLORER_MAX_MARKETS_STEP_DOWN"
  step_down_fast="$ALPHA_EXPLORER_MAX_MARKETS_STEP_DOWN_FAST"

  if (( configured < 1 )); then
    configured=1
  fi
  if (( min_markets < 1 )); then
    min_markets=1
  fi
  if (( max_markets < min_markets )); then
    max_markets="$min_markets"
  fi
  if (( configured < min_markets )); then
    configured="$min_markets"
  fi
  if (( configured > max_markets )); then
    configured="$max_markets"
  fi
  if (( target_scan_seconds < 1 )); then
    target_scan_seconds=1
  fi
  if (( scan_slack_seconds < 1 )); then
    scan_slack_seconds=1
  fi
  if (( scan_hard_slack_seconds < scan_slack_seconds )); then
    scan_hard_slack_seconds="$scan_slack_seconds"
  fi
  if (( step_up < 1 )); then
    step_up=1
  fi
  if (( step_up_fast < step_up )); then
    step_up_fast="$step_up"
  fi
  if (( step_down < 1 )); then
    step_down=1
  fi
  if (( step_down_fast < step_down )); then
    step_down_fast="$step_down"
  fi

  scan_low_bound="$(( target_scan_seconds - scan_slack_seconds ))"
  scan_low_hard_bound="$(( target_scan_seconds - scan_hard_slack_seconds ))"
  scan_high_bound="$(( target_scan_seconds + scan_slack_seconds ))"
  scan_high_hard_bound="$(( target_scan_seconds + scan_hard_slack_seconds ))"
  if (( scan_low_bound < 1 )); then
    scan_low_bound=1
  fi
  if (( scan_low_hard_bound < 1 )); then
    scan_low_hard_bound=1
  fi

  current="$configured"
  scan_duration="${LAST_EXPLORER_CONSTRAINT_SCAN_DURATION_SECONDS:-0}"
  if [[ -f "$ALPHA_EXPLORER_ADAPTIVE_MAX_MARKETS_STATE_FILE" ]]; then
    local saved_current saved_duration
    saved_current="$("$PYTHON_BIN" - "$ALPHA_EXPLORER_ADAPTIVE_MAX_MARKETS_STATE_FILE" <<'PY'
from pathlib import Path
import json
import sys
path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("")
    raise SystemExit(0)
value = payload.get("current_max_markets")
if isinstance(value, int):
    print(value)
elif isinstance(value, float):
    print(int(value))
else:
    print("")
PY
)"
    if [[ "$saved_current" =~ ^[0-9]+$ ]]; then
      current="$saved_current"
    fi
    if [[ -z "${LAST_EXPLORER_CONSTRAINT_SCAN_DURATION_SECONDS:-}" || "${LAST_EXPLORER_CONSTRAINT_SCAN_DURATION_SECONDS:-0}" == "0" ]]; then
      saved_duration="$("$PYTHON_BIN" - "$ALPHA_EXPLORER_ADAPTIVE_MAX_MARKETS_STATE_FILE" <<'PY'
from pathlib import Path
import json
import sys
path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("0")
    raise SystemExit(0)
value = payload.get("last_constraint_scan_duration_seconds")
try:
    print(int(float(value)))
except Exception:
    print("0")
PY
)"
      if [[ "$saved_duration" =~ ^[0-9]+$ ]]; then
        scan_duration="$saved_duration"
      fi
    fi
  fi
  if (( current < min_markets )); then
    current="$min_markets"
  fi
  if (( current > max_markets )); then
    current="$max_markets"
  fi

  load_milli="$(load_per_vcpu_milli)"
  next="$current"
  if [[ "$ALPHA_EXPLORER_MAX_MARKETS_ADAPTIVE_ENABLED" == "1" ]]; then
    if (( load_milli >= ALPHA_EXPLORER_ADAPTIVE_LOAD_VERY_HIGH_MILLI || scan_duration > scan_high_hard_bound )); then
      next="$(( current - step_down_fast ))"
    elif (( load_milli >= ALPHA_EXPLORER_ADAPTIVE_LOAD_HIGH_MILLI || scan_duration > scan_high_bound )); then
      next="$(( current - step_down ))"
    elif (( load_milli <= ALPHA_EXPLORER_ADAPTIVE_LOAD_LOW_MILLI && scan_duration > 0 && scan_duration < scan_low_hard_bound )); then
      next="$(( current + step_up_fast ))"
    elif (( load_milli <= ALPHA_EXPLORER_ADAPTIVE_LOAD_LOW_MILLI && scan_duration > 0 && scan_duration < scan_low_bound )); then
      next="$(( current + step_up ))"
    fi
  fi

  if (( next < min_markets )); then
    next="$min_markets"
  fi
  if (( next > max_markets )); then
    next="$max_markets"
  fi

  "$PYTHON_BIN" - "$ALPHA_EXPLORER_ADAPTIVE_MAX_MARKETS_STATE_FILE" "$configured" "$next" "$load_milli" "$min_markets" "$max_markets" "$scan_duration" "$target_scan_seconds" "$scan_slack_seconds" "$scan_hard_slack_seconds" "$step_up" "$step_up_fast" "$step_down" "$step_down_fast" "$ALPHA_EXPLORER_ADAPTIVE_LOAD_LOW_MILLI" "$ALPHA_EXPLORER_ADAPTIVE_LOAD_HIGH_MILLI" "$ALPHA_EXPLORER_ADAPTIVE_LOAD_VERY_HIGH_MILLI" "$ALPHA_EXPLORER_MAX_MARKETS_ADAPTIVE_ENABLED" <<'PY'
from pathlib import Path
import json
import sys
from datetime import datetime, timezone

path = Path(sys.argv[1])
payload = {
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "configured_max_markets": int(sys.argv[2]),
    "current_max_markets": int(sys.argv[3]),
    "load_per_vcpu_milli": int(sys.argv[4]),
    "min_max_markets": int(sys.argv[5]),
    "max_max_markets": int(sys.argv[6]),
    "last_constraint_scan_duration_seconds": int(sys.argv[7]),
    "target_constraint_scan_seconds": int(sys.argv[8]),
    "target_scan_slack_seconds": int(sys.argv[9]),
    "target_scan_hard_slack_seconds": int(sys.argv[10]),
    "step_up": int(sys.argv[11]),
    "step_up_fast": int(sys.argv[12]),
    "step_down": int(sys.argv[13]),
    "step_down_fast": int(sys.argv[14]),
    "load_low_milli": int(sys.argv[15]),
    "load_high_milli": int(sys.argv[16]),
    "load_very_high_milli": int(sys.argv[17]),
    "adaptive_enabled": str(sys.argv[18]).strip() == "1",
}
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY

  echo "$next"
}

build_policy() {
  local -a focus_station_args=()
  if [[ -n "$ALPHA_POLICY_FOCUS_STATIONS" ]]; then
    local -a _focus_station_tokens=()
    IFS=',' read -r -a _focus_station_tokens <<<"$ALPHA_POLICY_FOCUS_STATIONS"
    local _token
    for _token in "${_focus_station_tokens[@]}"; do
      _token="$(echo "$_token" | tr -d '[:space:]')"
      if [[ -n "$_token" ]]; then
        focus_station_args+=(--focus-station "$_token")
      fi
    done
  fi
  local -a taf_bonus_args=()
  if [[ "$ALPHA_POLICY_REQUIRE_TAF_OR_FORECAST_FOR_BONUS" == "1" ]]; then
    taf_bonus_args+=(--require-taf-or-forecast-for-bonus)
  fi
  run_cmd "metar_policy_autotune" \
    "$PYTHON_BIN" "$BETBOT_ROOT/infra/digitalocean/build_metar_age_policy.py" \
      --out-dir "$OUTPUT_DIR" \
      --hours "$ALPHA_POLICY_LOOKBACK_HOURS" \
      --base-max-age-minutes "$ALPHA_POLICY_BASE_MAX_AGE_MINUTES" \
      --hard-cap-minutes "$ALPHA_POLICY_HARD_CAP_MINUTES" \
      --min-stale-count "$ALPHA_POLICY_MIN_STALE_COUNT" \
      --min-total-count "$ALPHA_POLICY_MIN_TOTAL_COUNT" \
      --min-stale-rate "$ALPHA_POLICY_MIN_STALE_RATE" \
      --min-increment-minutes "$ALPHA_POLICY_MIN_INCREMENT_MINUTES" \
      --lift-minutes "$ALPHA_POLICY_LIFT_MINUTES" \
      --approved-slack-minutes "$ALPHA_POLICY_APPROVED_SLACK_MINUTES" \
      --focus-station-min-total-count "$ALPHA_POLICY_FOCUS_STATION_MIN_TOTAL_COUNT" \
      --focus-station-min-stale-count "$ALPHA_POLICY_FOCUS_STATION_MIN_STALE_COUNT" \
      --focus-station-min-stale-rate "$ALPHA_POLICY_FOCUS_STATION_MIN_STALE_RATE" \
      --focus-station-extra-lift-minutes "$ALPHA_POLICY_FOCUS_STATION_EXTRA_LIFT_MINUTES" \
      --taf-ready-bonus-lift-minutes "$ALPHA_POLICY_TAF_READY_BONUS_LIFT_MINUTES" \
      --taf-ready-min-rate "$ALPHA_POLICY_TAF_READY_MIN_RATE" \
      --forecast-ready-bonus-lift-minutes "$ALPHA_POLICY_FORECAST_READY_BONUS_LIFT_MINUTES" \
      --forecast-ready-min-rate "$ALPHA_POLICY_FORECAST_READY_MIN_RATE" \
      "${focus_station_args[@]}" \
      "${taf_bonus_args[@]}" \
      --top-n "$ALPHA_POLICY_TOP_N" \
      --output "$AUTO_METAR_POLICY_PATH"
  cp "$AUTO_METAR_POLICY_PATH" "$POLICY_SUMMARY_PATH" >/dev/null 2>&1 || true
}

refresh_explorer_inputs() {
  mkdir -p "$EXPLORER_INPUT_DIR"
  local -a speci_args=()
  if [[ -n "${SPECI_CALIBRATION_JSON:-}" && -f "${SPECI_CALIBRATION_JSON:-}" ]]; then
    speci_args=(--speci-calibration-json "$SPECI_CALIBRATION_JSON")
  fi
  run_cmd "explorer_contract_specs" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-contract-specs \
      --env-file "$BETBOT_ENV_FILE" \
      --top-n "$ALPHA_EXPLORER_TOP_N" \
      --output-dir "$EXPLORER_INPUT_DIR"
  local specs_csv
  specs_csv="$(latest_file "$EXPLORER_INPUT_DIR/kalshi_temperature_contract_specs_*.csv")"
  if [[ -z "$specs_csv" ]]; then
    return 1
  fi
  run_cmd "explorer_metar_ingest" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-metar-ingest \
      --specs-csv "$specs_csv" \
      --timeout-seconds "$ALPHA_EXPLORER_TIMEOUT_SECONDS" \
      --output-dir "$EXPLORER_INPUT_DIR"
  run_cmd "explorer_constraint_scan" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-constraint-scan \
      --specs-csv "$specs_csv" \
      --max-markets "${EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS:-$ALPHA_EXPLORER_MAX_MARKETS}" \
      --timeout-seconds "$ALPHA_EXPLORER_TIMEOUT_SECONDS" \
      "${speci_args[@]}" \
      --output-dir "$EXPLORER_INPUT_DIR"
  local constraint_csv
  constraint_csv="$(latest_file "$EXPLORER_INPUT_DIR/kalshi_temperature_constraint_scan_*.csv")"
  if [[ -z "$constraint_csv" ]]; then
    return 1
  fi
  run_cmd "explorer_settlement_state" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-settlement-state \
      --specs-csv "$specs_csv" \
      --constraint-csv "$constraint_csv" \
      --top-n "$ALPHA_EXPLORER_TOP_N" \
      --output-dir "$EXPLORER_INPUT_DIR"
  return 0
}

run_explorer_profile() {
  local label="$1"
  local disable_interval="$2"
  local max_yes_gap="$3"
  local max_age="$4"
  local profile_dir="$ALPHA_WORKER_DIR/explore_${label}"
  mkdir -p "$profile_dir"

  local -a policy_args=()
  local -a speci_args=()
  local -a settlement_args=()
  local -a metar_args=()
  local -a interval_args=()
  local -a quality_args=()

  if [[ -f "$AUTO_METAR_POLICY_PATH" ]]; then
    policy_args=(--metar-age-policy-json "$AUTO_METAR_POLICY_PATH")
  elif [[ -n "${METAR_AGE_POLICY_JSON:-}" && -f "${METAR_AGE_POLICY_JSON:-}" ]]; then
    policy_args=(--metar-age-policy-json "$METAR_AGE_POLICY_JSON")
  fi
  if [[ -n "${SPECI_CALIBRATION_JSON:-}" && -f "${SPECI_CALIBRATION_JSON:-}" ]]; then
    speci_args=(--speci-calibration-json "$SPECI_CALIBRATION_JSON")
  fi

  local specs_csv constraint_csv settlement_state_json metar_summary_json
  specs_csv="$(latest_file "$EXPLORER_INPUT_DIR/kalshi_temperature_contract_specs_*.csv")"
  constraint_csv="$(latest_file "$EXPLORER_INPUT_DIR/kalshi_temperature_constraint_scan_*.csv")"
  settlement_state_json="$(latest_file "$EXPLORER_INPUT_DIR/kalshi_temperature_settlement_state_*.json")"
  metar_summary_json="$(latest_file "$EXPLORER_INPUT_DIR/kalshi_temperature_metar_summary_*.json")"
  if [[ -z "$specs_csv" || -z "$constraint_csv" ]]; then
    return 1
  fi
  if [[ -n "$settlement_state_json" ]]; then
    settlement_args=(--settlement-state-json "$settlement_state_json")
  fi
  if [[ -n "$metar_summary_json" ]]; then
    metar_args=(--metar-summary-json "$metar_summary_json")
  fi
  if [[ "$disable_interval" == "1" ]]; then
    interval_args=(--disable-interval-consistency-gate --max-yes-possible-gap-for-yes-side "$max_yes_gap")
  fi
  if [[ "$ALPHA_EXPLORER_ENFORCE_PROBABILITY_EDGE_THRESHOLDS" != "1" ]]; then
    quality_args+=(--disable-enforce-probability-edge-thresholds)
  fi
  if [[ "$ALPHA_EXPLORER_ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR" == "1" ]]; then
    quality_args+=(--enforce-entry-price-probability-floor)
  fi
  local effective_min_expected_edge_net
  effective_min_expected_edge_net="$(normalize_min_expected_edge_net \
    "$ALPHA_EXPLORER_MIN_EXPECTED_EDGE_NET" \
    "$ALPHA_EXPLORER_FALLBACK_MIN_EXPECTED_EDGE_NET" \
    "$ALPHA_EXPLORER_ENFORCE_PROBABILITY_EDGE_THRESHOLDS")"
  local effective_fallback_probability="$ALPHA_EXPLORER_FALLBACK_MIN_PROBABILITY_CONFIDENCE"
  if [[ -z "$effective_fallback_probability" ]]; then
    effective_fallback_probability="$ALPHA_EXPLORER_MIN_SETTLEMENT_CONFIDENCE"
  fi
  if [[ "$effective_fallback_probability" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--fallback-min-probability-confidence "$effective_fallback_probability")
  fi
  if [[ "$ALPHA_EXPLORER_FALLBACK_MIN_EXPECTED_EDGE_NET" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--fallback-min-expected-edge-net "$ALPHA_EXPLORER_FALLBACK_MIN_EXPECTED_EDGE_NET")
  fi
  if [[ "$ALPHA_EXPLORER_FALLBACK_MIN_EDGE_TO_RISK_RATIO" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--fallback-min-edge-to-risk-ratio "$ALPHA_EXPLORER_FALLBACK_MIN_EDGE_TO_RISK_RATIO")
  fi
  if [[ "$ALPHA_EXPLORER_MIN_PROBABILITY_CONFIDENCE" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--min-probability-confidence "$ALPHA_EXPLORER_MIN_PROBABILITY_CONFIDENCE")
  fi
  if [[ "$effective_min_expected_edge_net" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--min-expected-edge-net "$effective_min_expected_edge_net")
  fi
  if [[ "$ALPHA_EXPLORER_MIN_EDGE_TO_RISK_RATIO" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--min-edge-to-risk-ratio "$ALPHA_EXPLORER_MIN_EDGE_TO_RISK_RATIO")
  fi
  if [[ "$ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--metar-freshness-quality-boundary-ratio "$ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO")
  fi
  if [[ "$ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--metar-freshness-quality-probability-margin "$ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN")
  fi
  if [[ "$ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--metar-freshness-quality-expected-edge-margin "$ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN")
  fi

  run_cmd "explorer_${label}" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-trader \
      --env-file "$BETBOT_ENV_FILE" \
      --intents-only \
      --disable-require-market-snapshot-seq \
      --specs-csv "$specs_csv" \
      --constraint-csv "$constraint_csv" \
      "${settlement_args[@]}" \
      "${metar_args[@]}" \
      --max-markets "${EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS:-$ALPHA_EXPLORER_MAX_MARKETS}" \
      --max-orders "$ALPHA_EXPLORER_MAX_ORDERS" \
      --max-intents-per-underlying "$ALPHA_EXPLORER_MAX_INTENTS_PER_UNDERLYING" \
      --planning-bankroll "${PLANNING_BANKROLL_DOLLARS:-1000}" \
      --daily-risk-cap "${DAILY_RISK_CAP_DOLLARS:-100}" \
      --max-live-submissions-per-day "${MAX_LIVE_SUBMISSIONS_PER_DAY:-20}" \
      --max-live-cost-per-day-dollars "${MAX_LIVE_COST_PER_DAY_DOLLARS:-100}" \
      --max-metar-age-minutes "$max_age" \
      --min-settlement-confidence "$ALPHA_EXPLORER_MIN_SETTLEMENT_CONFIDENCE" \
      --taf-stale-grace-minutes "$ALPHA_EXPLORER_TAF_STALE_GRACE_MINUTES" \
      --taf-stale-grace-max-volatility-score "$ALPHA_EXPLORER_TAF_STALE_GRACE_MAX_VOLATILITY_SCORE" \
      --taf-stale-grace-max-range-width "$ALPHA_EXPLORER_TAF_STALE_GRACE_MAX_RANGE_WIDTH" \
      --min-alpha-strength "$ALPHA_EXPLORER_MIN_ALPHA_STRENGTH" \
      --timeout-seconds "$ALPHA_EXPLORER_TIMEOUT_SECONDS" \
      "${policy_args[@]}" \
      "${speci_args[@]}" \
      "${quality_args[@]}" \
      "${interval_args[@]}" \
      --output-dir "$profile_dir"
}

explorer_profile_default() {
  run_explorer_profile "default" "0" "0.0" "${MAX_METAR_AGE_MINUTES:-22.5}"
}

explorer_profile_relaxed() {
  run_explorer_profile "relaxed_interval" "1" "$ALPHA_EXPLORER_RELAXED_MAX_YES_GAP" "${MAX_METAR_AGE_MINUTES:-22.5}"
}

explorer_profile_relaxed_age() {
  run_explorer_profile "relaxed_age" "1" "$ALPHA_EXPLORER_RELAXED_MAX_YES_GAP" "$ALPHA_EXPLORER_RELAXED_AGE_MINUTES"
}

explorer_profile_wide_gap_age() {
  run_explorer_profile "wide_gap_age" "1" "$ALPHA_EXPLORER_RELAXED_MAX_YES_GAP_WIDE" "$ALPHA_EXPLORER_RELAXED_AGE_WIDE_MINUTES"
}

explorer_profile_ultra_gap_age() {
  run_explorer_profile "ultra_gap_age" "1" "$ALPHA_EXPLORER_RELAXED_MAX_YES_GAP_ULTRA" "$ALPHA_EXPLORER_RELAXED_AGE_ULTRA_MINUTES"
}

explorer_profile_ultra_age_only() {
  run_explorer_profile "ultra_age_only" "1" "$ALPHA_EXPLORER_RELAXED_MAX_YES_GAP" "$ALPHA_EXPLORER_RELAXED_AGE_ULTRA_MINUTES"
}

run_explorer_profiles() {
  local -a fn_names=("explorer_profile_default" "explorer_profile_relaxed")
  if [[ "$ALPHA_EXPLORER_ENABLE_RELAXED_AGE" == "1" ]]; then
    fn_names+=("explorer_profile_relaxed_age")
  fi
  if [[ "$ALPHA_EXPLORER_ENABLE_WIDE_PROFILE" == "1" ]]; then
    fn_names+=("explorer_profile_wide_gap_age")
  fi
  if [[ "$ALPHA_EXPLORER_ENABLE_ULTRA_PROFILE" == "1" ]]; then
    fn_names+=("explorer_profile_ultra_gap_age")
  fi
  if [[ "$ALPHA_EXPLORER_ENABLE_ULTRA_AGE_ONLY_PROFILE" == "1" ]]; then
    fn_names+=("explorer_profile_ultra_age_only")
  fi

  local batch_size
  batch_size="$ALPHA_EXPLORER_PROFILE_PARALLELISM"
  if ! [[ "$batch_size" =~ ^[0-9]+$ ]] || (( batch_size <= 0 )); then
    if [[ "$ALPHA_EXPLORER_PROFILE_PARALLEL" == "1" ]]; then
      batch_size="${#fn_names[@]}"
    else
      batch_size=1
    fi
  fi
  if (( batch_size < 1 )); then
    batch_size=1
  fi

  local total="${#fn_names[@]}"
  local idx=0
  local had_failures=0
  while (( idx < total )); do
    local end="$(( idx + batch_size ))"
    if (( end > total )); then
      end="$total"
    fi
    local -a pids=()
    local i
    for (( i = idx; i < end; i++ )); do
      "${fn_names[$i]}" & pids+=("$!")
    done
    local pid
    for pid in "${pids[@]}"; do
      if ! wait "$pid"; then
        had_failures=1
      fi
    done
    idx="$end"
  done
  if (( had_failures != 0 )); then
    return 1
  fi
  return 0
}

run_ws_collect() {
  if [[ "$ALPHA_WS_COLLECT_ENABLED" != "1" ]]; then
    return 0
  fi
  local -a ticker_args=()
  if [[ -n "$ALPHA_WS_COLLECT_MARKET_TICKERS" ]]; then
    ticker_args=(--market-tickers "$ALPHA_WS_COLLECT_MARKET_TICKERS")
  fi
  run_cmd "ws_state_collect" \
    "$PYTHON_BIN" -m betbot.cli kalshi-ws-state-collect \
      --env-file "$BETBOT_ENV_FILE" \
      --channels "$ALPHA_WS_COLLECT_CHANNELS" \
      "${ticker_args[@]}" \
      --run-seconds "$ALPHA_WS_COLLECT_RUN_SECONDS" \
      --max-events "$ALPHA_WS_COLLECT_MAX_EVENTS" \
      --connect-timeout-seconds "$ALPHA_WS_COLLECT_CONNECT_TIMEOUT_SECONDS" \
      --read-timeout-seconds "$ALPHA_WS_COLLECT_READ_TIMEOUT_SECONDS" \
      --ping-interval-seconds "$ALPHA_WS_COLLECT_PING_INTERVAL_SECONDS" \
      --flush-state-every-seconds "$ALPHA_WS_COLLECT_FLUSH_STATE_EVERY_SECONDS" \
      --reconnect-max-attempts "$ALPHA_WS_COLLECT_RECONNECT_MAX_ATTEMPTS" \
      --reconnect-backoff-seconds "$ALPHA_WS_COLLECT_RECONNECT_BACKOFF_SECONDS" \
      --ws-state-max-age-seconds "$ALPHA_WS_STATE_MAX_AGE_SECONDS" \
      --output-dir "$OUTPUT_DIR"
}

run_validation_rollups() {
  local latest_live_readiness=""
  local latest_bankroll_validation=""
  run_cmd "live_readiness_fast" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-live-readiness \
      --output-dir "$OUTPUT_DIR" \
      --horizons "$ALPHA_READINESS_HORIZONS" \
      --reference-bankroll-dollars "${REFERENCE_BANKROLL_DOLLARS:-1000}" \
      --slippage-bps-list "$ALPHA_VALIDATION_SLIPPAGE_BPS_LIST" \
      --top-n "$ALPHA_VALIDATION_TOP_N"
  latest_live_readiness="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_live_readiness_*.json 2>/dev/null | head -n 1 || true)"
  local -a go_live_gate_cmd=(
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-go-live-gate
    --output-dir "$OUTPUT_DIR"
    --horizons "$ALPHA_READINESS_HORIZONS"
    --reference-bankroll-dollars "${REFERENCE_BANKROLL_DOLLARS:-1000}"
    --slippage-bps-list "$ALPHA_VALIDATION_SLIPPAGE_BPS_LIST"
    --top-n "$ALPHA_VALIDATION_TOP_N"
  )
  if [[ -n "$latest_live_readiness" ]]; then
    go_live_gate_cmd+=(--source-live-readiness-file "$latest_live_readiness")
  fi
  run_cmd "go_live_gate_fast" \
    "${go_live_gate_cmd[@]}"
  run_cmd "bankroll_validation_fast" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-bankroll-validation \
      --output-dir "$OUTPUT_DIR" \
      --hours "$ALPHA_VALIDATION_HOURS" \
      --top-n "$ALPHA_VALIDATION_TOP_N" \
      --reference-bankroll-dollars "${REFERENCE_BANKROLL_DOLLARS:-1000}" \
      --slippage-bps-list "$ALPHA_VALIDATION_SLIPPAGE_BPS_LIST"
  latest_bankroll_validation="$(ls -1t "$OUTPUT_DIR"/kalshi_temperature_bankroll_validation_*.json 2>/dev/null | head -n 1 || true)"
  local -a alpha_gap_cmd=(
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-alpha-gap-report
    --output-dir "$OUTPUT_DIR"
    --hours "$ALPHA_VALIDATION_HOURS"
    --top-n "$ALPHA_VALIDATION_TOP_N"
  )
  if [[ -n "$latest_bankroll_validation" ]]; then
    alpha_gap_cmd+=(--source-bankroll-validation-file "$latest_bankroll_validation")
  fi
  run_cmd "alpha_gap_report_fast" \
    "${alpha_gap_cmd[@]}"
}

run_prewarm_if_enabled() {
  if [[ "$ALPHA_PREWARM_ENABLED" != "1" ]]; then
    return 0
  fi
  if [[ ! -f "$ALPHA_PREWARM_HISTORY_CSV" ]]; then
    local ts
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "[$ts] weather_prewarm skipped missing_history_csv=$ALPHA_PREWARM_HISTORY_CSV" >> "$LOG_FILE"
    return 0
  fi
  run_cmd "weather_prewarm" \
    "$PYTHON_BIN" -m betbot.cli kalshi-weather-prewarm \
      --history-csv "$ALPHA_PREWARM_HISTORY_CSV" \
      --max-station-day-keys "$ALPHA_PREWARM_MAX_STATION_DAY_KEYS" \
      --timeout-seconds "$ALPHA_PREWARM_TIMEOUT_SECONDS" \
      --output-dir "$OUTPUT_DIR"
}

write_dashboard() {
  run_cmd "alpha_worker_dashboard" \
    "$PYTHON_BIN" "$BETBOT_ROOT/infra/digitalocean/build_alpha_worker_dashboard.py" \
      --out-dir "$OUTPUT_DIR" \
      --alpha-worker-dir "$ALPHA_WORKER_DIR" \
      --policy-json "$AUTO_METAR_POLICY_PATH" \
      --output "$ALPHA_DASHBOARD_PATH"
}

last_policy_epoch=0
last_ws_collect_epoch=0
last_explorer_epoch=0
last_validation_epoch=0
last_prewarm_epoch=0
last_dashboard_epoch=0
LAST_EXPLORER_CONSTRAINT_SCAN_DURATION_SECONDS=0
EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS="$ALPHA_EXPLORER_MAX_MARKETS"
LAST_EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS=""

echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") alpha workers start ===" >> "$LOG_FILE"
echo "alpha_worker_dir=$ALPHA_WORKER_DIR policy_path=$AUTO_METAR_POLICY_PATH loop=${ALPHA_WORKER_LOOP_SECONDS}s" >> "$LOG_FILE"
if [[ -z "${METAR_AGE_POLICY_JSON:-}" ]]; then
  echo "warning: METAR_AGE_POLICY_JSON is empty in env; shadow loop will not consume auto policy until set." >> "$LOG_FILE"
fi

while true; do
  now_epoch="$(date +%s)"
  EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS="$(compute_effective_explorer_max_markets)"
  if [[ "$EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS" != "$LAST_EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS" ]]; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] alpha_explorer_max_markets effective=$EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS adaptive=$ALPHA_EXPLORER_MAX_MARKETS_ADAPTIVE_ENABLED" >> "$LOG_FILE"
    LAST_EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS="$EFFECTIVE_ALPHA_EXPLORER_MAX_MARKETS"
  fi

  if (( now_epoch - last_policy_epoch >= ALPHA_POLICY_REFRESH_SECONDS )); then
    if build_policy; then
      last_policy_epoch="$now_epoch"
    fi
  fi

  ws_due=0
  ws_pid=""
  if (( now_epoch - last_ws_collect_epoch >= ALPHA_WS_COLLECT_REFRESH_SECONDS )); then
    run_ws_collect &
    ws_pid="$!"
    ws_due=1
  fi

  if (( now_epoch - last_explorer_epoch >= ALPHA_EXPLORER_REFRESH_SECONDS )); then
    if refresh_explorer_inputs; then
      if run_explorer_profiles; then
        last_explorer_epoch="$now_epoch"
      else
        echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] explorer_profiles_failed_after_refresh" >> "$LOG_FILE"
      fi
    else
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] explorer_input_refresh_failed" >> "$LOG_FILE"
    fi
  fi

  if [[ "$ALPHA_VALIDATION_ROLLUPS_ENABLED" == "1" ]]; then
    if (( now_epoch - last_validation_epoch >= ALPHA_VALIDATION_REFRESH_SECONDS )); then
      if run_validation_rollups; then
        last_validation_epoch="$now_epoch"
      else
        echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] validation_rollups_failed" >> "$LOG_FILE"
      fi
    fi
  fi

  if (( now_epoch - last_prewarm_epoch >= ALPHA_PREWARM_REFRESH_SECONDS )); then
    if run_prewarm_if_enabled; then
      last_prewarm_epoch="$now_epoch"
    else
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] prewarm_failed" >> "$LOG_FILE"
    fi
  fi

  if (( now_epoch - last_dashboard_epoch >= ALPHA_DASHBOARD_REFRESH_SECONDS )); then
    if write_dashboard; then
      last_dashboard_epoch="$now_epoch"
    else
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] alpha_dashboard_refresh_failed" >> "$LOG_FILE"
    fi
  fi

  if (( ws_due == 1 )); then
    if wait "$ws_pid"; then
      last_ws_collect_epoch="$now_epoch"
    else
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] ws_state_collect_failed" >> "$LOG_FILE"
    fi
  fi

  sleep "$ALPHA_WORKER_LOOP_SECONDS"
done
