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

BREADTH_WORKER_ENABLED="${BREADTH_WORKER_ENABLED:-0}"
if [[ "$BREADTH_WORKER_ENABLED" != "1" ]]; then
  echo "breadth worker disabled (BREADTH_WORKER_ENABLED=$BREADTH_WORKER_ENABLED)"
  exit 0
fi

BREADTH_WORKER_LOOP_SECONDS="${BREADTH_WORKER_LOOP_SECONDS:-20}"
BREADTH_INPUT_REFRESH_SECONDS="${BREADTH_INPUT_REFRESH_SECONDS:-60}"
BREADTH_PROFILE_PARALLELISM="${BREADTH_PROFILE_PARALLELISM:-3}"
BREADTH_WORKER_DIR="${BREADTH_WORKER_DIR:-$OUTPUT_DIR/breadth_worker}"
BREADTH_INPUT_DIR="${BREADTH_INPUT_DIR:-$BREADTH_WORKER_DIR/inputs}"
BREADTH_DASHBOARD_REFRESH_SECONDS="${BREADTH_DASHBOARD_REFRESH_SECONDS:-30}"
BREADTH_DASHBOARD_PATH="${BREADTH_DASHBOARD_PATH:-$BREADTH_WORKER_DIR/breadth_worker_dashboard_latest.json}"
BREADTH_CONSENSUS_OUTPUT_PATH="${BREADTH_CONSENSUS_OUTPUT_PATH:-$BREADTH_WORKER_DIR/breadth_worker_consensus_latest.json}"
BREADTH_CONSENSUS_TOP_N="${BREADTH_CONSENSUS_TOP_N:-250}"
BREADTH_CONSENSUS_MIN_PROFILE_SUPPORT="${BREADTH_CONSENSUS_MIN_PROFILE_SUPPORT:-1}"
BREADTH_CONSENSUS_MIN_SUPPORT_RATIO="${BREADTH_CONSENSUS_MIN_SUPPORT_RATIO:-0.0}"
BREADTH_TARGET_UNIQUE_MARKET_SIDES_APPROVED="${BREADTH_TARGET_UNIQUE_MARKET_SIDES_APPROVED:-10}"
BREADTH_TARGET_UNIQUE_UNDERLYINGS="${BREADTH_TARGET_UNIQUE_UNDERLYINGS:-4}"
BREADTH_TARGET_CONSENSUS_CANDIDATES="${BREADTH_TARGET_CONSENSUS_CANDIDATES:-25}"
BREADTH_TARGET_STEP_UP="${BREADTH_TARGET_STEP_UP:-180}"
BREADTH_TARGET_STEP_UP_FAST="${BREADTH_TARGET_STEP_UP_FAST:-360}"
BREADTH_REPLAN_PRESSURE_ENABLED="${BREADTH_REPLAN_PRESSURE_ENABLED:-1}"
BREADTH_REPLAN_PRESSURE_MIN_INPUT_COUNT="${BREADTH_REPLAN_PRESSURE_MIN_INPUT_COUNT:-10}"
BREADTH_REPLAN_PRESSURE_BLOCKED_RATIO_MIN="${BREADTH_REPLAN_PRESSURE_BLOCKED_RATIO_MIN:-0.70}"
BREADTH_REPLAN_PRESSURE_MAX_UNIQUE_MARKET_SIDES="${BREADTH_REPLAN_PRESSURE_MAX_UNIQUE_MARKET_SIDES:-8}"
BREADTH_REPLAN_PRESSURE_MAX_UNIQUE_UNDERLYINGS="${BREADTH_REPLAN_PRESSURE_MAX_UNIQUE_UNDERLYINGS:-6}"
BREADTH_REPLAN_PRESSURE_LEVEL="${BREADTH_REPLAN_PRESSURE_LEVEL:-1}"
BREADTH_REPLAN_PRESSURE_REQUIRE_TARGET_DEFICIT="${BREADTH_REPLAN_PRESSURE_REQUIRE_TARGET_DEFICIT:-1}"
BREADTH_OVERLAP_PRESSURE_ENABLED="${BREADTH_OVERLAP_PRESSURE_ENABLED:-1}"
BREADTH_OVERLAP_PRESSURE_MIN_INTENTS="${BREADTH_OVERLAP_PRESSURE_MIN_INTENTS:-100}"
BREADTH_OVERLAP_PRESSURE_RATIO_MIN="${BREADTH_OVERLAP_PRESSURE_RATIO_MIN:-0.08}"
BREADTH_OVERLAP_PRESSURE_RATIO_HIGH="${BREADTH_OVERLAP_PRESSURE_RATIO_HIGH:-0.15}"
BREADTH_OVERLAP_PRESSURE_REQUIRE_TARGET_DEFICIT="${BREADTH_OVERLAP_PRESSURE_REQUIRE_TARGET_DEFICIT:-0}"
BREADTH_OVERLAP_PRESSURE_REQUIRE_LOW_STALE="${BREADTH_OVERLAP_PRESSURE_REQUIRE_LOW_STALE:-1}"
BREADTH_OVERLAP_PRESSURE_MAX_STALE_RATE="${BREADTH_OVERLAP_PRESSURE_MAX_STALE_RATE:-0.15}"
BREADTH_OVERLAP_PRESSURE_ROLLING_WINDOW_TAG="${BREADTH_OVERLAP_PRESSURE_ROLLING_WINDOW_TAG:-14h}"
BREADTH_OVERLAP_PRESSURE_ROLLING_MAX_AGE_SECONDS="${BREADTH_OVERLAP_PRESSURE_ROLLING_MAX_AGE_SECONDS:-21600}"
BREADTH_OVERLAP_PRESSURE_ROLLING_MIN_INTENTS="${BREADTH_OVERLAP_PRESSURE_ROLLING_MIN_INTENTS:-100}"
BREADTH_OVERLAP_PRESSURE_MAX_SETTLEMENT_UNRESOLVED="${BREADTH_OVERLAP_PRESSURE_MAX_SETTLEMENT_UNRESOLVED:-20}"
BREADTH_OVERLAP_PRESSURE_BLOCKED_MIN="${BREADTH_OVERLAP_PRESSURE_BLOCKED_MIN:-300}"
BREADTH_OVERLAP_PRESSURE_BLOCKED_SHARE_MIN="${BREADTH_OVERLAP_PRESSURE_BLOCKED_SHARE_MIN:-0.35}"
BREADTH_OVERLAP_PRESSURE_BLOCKED_SHARE_HIGH="${BREADTH_OVERLAP_PRESSURE_BLOCKED_SHARE_HIGH:-0.55}"
BREADTH_HEADROOM_EXPLORATION_ENABLED="${BREADTH_HEADROOM_EXPLORATION_ENABLED:-1}"
BREADTH_HEADROOM_EXPLORATION_MAX_LOAD_PER_VCPU="${BREADTH_HEADROOM_EXPLORATION_MAX_LOAD_PER_VCPU:-0.70}"
BREADTH_HEADROOM_EXPLORATION_MIN_INTENTS="${BREADTH_HEADROOM_EXPLORATION_MIN_INTENTS:-80}"
BREADTH_HEADROOM_EXPLORATION_MIN_APPROVAL_RATE="${BREADTH_HEADROOM_EXPLORATION_MIN_APPROVAL_RATE:-0.35}"
BREADTH_HEADROOM_EXPLORATION_MAX_STALE_RATE="${BREADTH_HEADROOM_EXPLORATION_MAX_STALE_RATE:-0.15}"
BREADTH_HEADROOM_EXPLORATION_LEVEL="${BREADTH_HEADROOM_EXPLORATION_LEVEL:-1}"
BREADTH_HEADROOM_EXPLORATION_REQUIRE_TARGET_DEFICIT="${BREADTH_HEADROOM_EXPLORATION_REQUIRE_TARGET_DEFICIT:-0}"

BREADTH_MAX_MARKETS="${BREADTH_MAX_MARKETS:-2000}"
BREADTH_MAX_ORDERS="${BREADTH_MAX_ORDERS:-120}"
BREADTH_MAX_INTENTS_PER_UNDERLYING="${BREADTH_MAX_INTENTS_PER_UNDERLYING:-36}"
BREADTH_TOP_N="${BREADTH_TOP_N:-60}"
BREADTH_TIMEOUT_SECONDS="${BREADTH_TIMEOUT_SECONDS:-12}"
BREADTH_MIN_SETTLEMENT_CONFIDENCE="${BREADTH_MIN_SETTLEMENT_CONFIDENCE:-${MIN_SETTLEMENT_CONFIDENCE:-0.6}}"
BREADTH_MIN_ALPHA_STRENGTH="${BREADTH_MIN_ALPHA_STRENGTH:-0.0}"
BREADTH_MIN_PROBABILITY_CONFIDENCE="${BREADTH_MIN_PROBABILITY_CONFIDENCE:-${MIN_PROBABILITY_CONFIDENCE:-}}"
BREADTH_MIN_EXPECTED_EDGE_NET="${BREADTH_MIN_EXPECTED_EDGE_NET:-${MIN_EXPECTED_EDGE_NET:-}}"
BREADTH_MIN_EDGE_TO_RISK_RATIO="${BREADTH_MIN_EDGE_TO_RISK_RATIO:-${MIN_EDGE_TO_RISK_RATIO:-}}"
BREADTH_ENFORCE_PROBABILITY_EDGE_THRESHOLDS="${BREADTH_ENFORCE_PROBABILITY_EDGE_THRESHOLDS:-${ENFORCE_PROBABILITY_EDGE_THRESHOLDS:-1}}"
BREADTH_ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR="${BREADTH_ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR:-${ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR:-1}}"
BREADTH_FALLBACK_MIN_PROBABILITY_CONFIDENCE="${BREADTH_FALLBACK_MIN_PROBABILITY_CONFIDENCE:-${FALLBACK_MIN_PROBABILITY_CONFIDENCE:-}}"
BREADTH_FALLBACK_MIN_EXPECTED_EDGE_NET="${BREADTH_FALLBACK_MIN_EXPECTED_EDGE_NET:-${FALLBACK_MIN_EXPECTED_EDGE_NET:-0.005}}"
BREADTH_FALLBACK_MIN_EDGE_TO_RISK_RATIO="${BREADTH_FALLBACK_MIN_EDGE_TO_RISK_RATIO:-${FALLBACK_MIN_EDGE_TO_RISK_RATIO:-0.02}}"

BREADTH_RELAXED_GAP_PRIMARY="${BREADTH_RELAXED_GAP_PRIMARY:-3.0}"
BREADTH_RELAXED_GAP_WIDE="${BREADTH_RELAXED_GAP_WIDE:-6.0}"
BREADTH_RELAXED_GAP_ULTRA="${BREADTH_RELAXED_GAP_ULTRA:-8.0}"
BREADTH_RELAXED_AGE_PRIMARY="${BREADTH_RELAXED_AGE_PRIMARY:-26.0}"
BREADTH_RELAXED_AGE_WIDE="${BREADTH_RELAXED_AGE_WIDE:-30.0}"
BREADTH_RELAXED_AGE_ULTRA="${BREADTH_RELAXED_AGE_ULTRA:-34.0}"
BREADTH_ENABLE_WIDE_GAP_PROFILE="${BREADTH_ENABLE_WIDE_GAP_PROFILE:-1}"
BREADTH_ENABLE_ULTRA_GAP_PROFILE="${BREADTH_ENABLE_ULTRA_GAP_PROFILE:-1}"
BREADTH_ENABLE_ULTRA_AGE_ONLY_PROFILE="${BREADTH_ENABLE_ULTRA_AGE_ONLY_PROFILE:-1}"
BREADTH_TAF_STALE_GRACE_MINUTES="${BREADTH_TAF_STALE_GRACE_MINUTES:-${TAF_STALE_GRACE_MINUTES:-2.5}}"
BREADTH_TAF_STALE_GRACE_MAX_VOLATILITY_SCORE="${BREADTH_TAF_STALE_GRACE_MAX_VOLATILITY_SCORE:-${TAF_STALE_GRACE_MAX_VOLATILITY_SCORE:-1.0}}"
BREADTH_TAF_STALE_GRACE_MAX_RANGE_WIDTH="${BREADTH_TAF_STALE_GRACE_MAX_RANGE_WIDTH:-${TAF_STALE_GRACE_MAX_RANGE_WIDTH:-10.0}}"
BREADTH_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO="${BREADTH_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO:-${METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO:-0.92}}"
BREADTH_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN="${BREADTH_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN:-${METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN:-0.03}}"
BREADTH_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN="${BREADTH_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN:-${METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN:-0.005}}"

BREADTH_PROFILE_PARALLELISM_ADAPTIVE_ENABLED="${BREADTH_PROFILE_PARALLELISM_ADAPTIVE_ENABLED:-1}"
BREADTH_PROFILE_PARALLELISM_MIN="${BREADTH_PROFILE_PARALLELISM_MIN:-2}"
BREADTH_PROFILE_PARALLELISM_MAX="${BREADTH_PROFILE_PARALLELISM_MAX:-6}"
BREADTH_ADAPTIVE_LOAD_VERY_LOW_MILLI="${BREADTH_ADAPTIVE_LOAD_VERY_LOW_MILLI:-140}"
BREADTH_ADAPTIVE_LOAD_LOW_MILLI="${BREADTH_ADAPTIVE_LOAD_LOW_MILLI:-220}"
BREADTH_ADAPTIVE_LOAD_HIGH_MILLI="${BREADTH_ADAPTIVE_LOAD_HIGH_MILLI:-700}"
BREADTH_ADAPTIVE_LOAD_VERY_HIGH_MILLI="${BREADTH_ADAPTIVE_LOAD_VERY_HIGH_MILLI:-900}"
BREADTH_ADAPTIVE_PARALLELISM_STEP_UP="${BREADTH_ADAPTIVE_PARALLELISM_STEP_UP:-1}"
BREADTH_ADAPTIVE_PARALLELISM_STEP_UP_FAST="${BREADTH_ADAPTIVE_PARALLELISM_STEP_UP_FAST:-2}"
BREADTH_ADAPTIVE_PARALLELISM_STEP_DOWN="${BREADTH_ADAPTIVE_PARALLELISM_STEP_DOWN:-1}"
BREADTH_ADAPTIVE_PARALLELISM_STEP_DOWN_FAST="${BREADTH_ADAPTIVE_PARALLELISM_STEP_DOWN_FAST:-2}"
BREADTH_ADAPTIVE_PARALLELISM_STATE_FILE="${BREADTH_ADAPTIVE_PARALLELISM_STATE_FILE:-$BREADTH_WORKER_DIR/.adaptive_parallelism.json}"
BREADTH_LOOP_SLEEP_ADAPTIVE_ENABLED="${BREADTH_LOOP_SLEEP_ADAPTIVE_ENABLED:-1}"
BREADTH_WORKER_LOOP_SECONDS_MIN="${BREADTH_WORKER_LOOP_SECONDS_MIN:-2}"
BREADTH_WORKER_LOOP_SECONDS_MAX="${BREADTH_WORKER_LOOP_SECONDS_MAX:-$BREADTH_WORKER_LOOP_SECONDS}"
BREADTH_ADAPTIVE_LOOP_SLEEP_STATE_FILE="${BREADTH_ADAPTIVE_LOOP_SLEEP_STATE_FILE:-$BREADTH_WORKER_DIR/.adaptive_loop_sleep.json}"

BREADTH_MAX_MARKETS_ADAPTIVE_ENABLED="${BREADTH_MAX_MARKETS_ADAPTIVE_ENABLED:-1}"
BREADTH_MAX_MARKETS_MIN="${BREADTH_MAX_MARKETS_MIN:-1200}"
BREADTH_MAX_MARKETS_MAX="${BREADTH_MAX_MARKETS_MAX:-$BREADTH_MAX_MARKETS}"
BREADTH_MAX_MARKETS_TARGET_SCAN_SECONDS="${BREADTH_MAX_MARKETS_TARGET_SCAN_SECONDS:-30}"
BREADTH_MAX_MARKETS_TARGET_SCAN_SLACK_SECONDS="${BREADTH_MAX_MARKETS_TARGET_SCAN_SLACK_SECONDS:-6}"
BREADTH_MAX_MARKETS_TARGET_SCAN_HARD_SLACK_SECONDS="${BREADTH_MAX_MARKETS_TARGET_SCAN_HARD_SLACK_SECONDS:-12}"
BREADTH_MAX_MARKETS_STEP_UP="${BREADTH_MAX_MARKETS_STEP_UP:-120}"
BREADTH_MAX_MARKETS_STEP_DOWN="${BREADTH_MAX_MARKETS_STEP_DOWN:-180}"
BREADTH_MAX_MARKETS_STEP_UP_FAST="${BREADTH_MAX_MARKETS_STEP_UP_FAST:-240}"
BREADTH_MAX_MARKETS_STEP_DOWN_FAST="${BREADTH_MAX_MARKETS_STEP_DOWN_FAST:-360}"
BREADTH_TARGET_PRESSURE_CAP_MULTIPLIER="${BREADTH_TARGET_PRESSURE_CAP_MULTIPLIER:-1.5}"
BREADTH_ADAPTIVE_MAX_MARKETS_STATE_FILE="${BREADTH_ADAPTIVE_MAX_MARKETS_STATE_FILE:-$BREADTH_WORKER_DIR/.adaptive_max_markets.json}"

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs" "$BREADTH_WORKER_DIR" "$BREADTH_INPUT_DIR"

LOG_FILE="$OUTPUT_DIR/logs/breadth_worker.log"
PROFILES_DIR="$BREADTH_WORKER_DIR/profiles"
mkdir -p "$PROFILES_DIR"

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
  RUN_CMD_LAST_LABEL="$label"
  RUN_CMD_LAST_DURATION_SECONDS="$duration_seconds"
  RUN_CMD_LAST_RC="$rc"
  if [[ "$label" == "breadth_constraint_scan" ]]; then
    LAST_CONSTRAINT_SCAN_DURATION_SECONDS="$duration_seconds"
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

refresh_inputs() {
  local -a speci_args=()
  if [[ -n "${SPECI_CALIBRATION_JSON:-}" && -f "${SPECI_CALIBRATION_JSON:-}" ]]; then
    speci_args=(--speci-calibration-json "$SPECI_CALIBRATION_JSON")
  fi
  run_cmd "breadth_contract_specs" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-contract-specs \
      --env-file "$BETBOT_ENV_FILE" \
      --top-n "$BREADTH_TOP_N" \
      --output-dir "$BREADTH_INPUT_DIR"
  local specs_csv
  specs_csv="$(latest_file "$BREADTH_INPUT_DIR/kalshi_temperature_contract_specs_*.csv")"
  if [[ -z "$specs_csv" ]]; then
    return 1
  fi
  run_cmd "breadth_metar_ingest" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-metar-ingest \
      --specs-csv "$specs_csv" \
      --timeout-seconds "$BREADTH_TIMEOUT_SECONDS" \
      --output-dir "$BREADTH_INPUT_DIR"
  run_cmd "breadth_constraint_scan" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-constraint-scan \
      --specs-csv "$specs_csv" \
      --max-markets "${EFFECTIVE_MAX_MARKETS:-$BREADTH_MAX_MARKETS}" \
      --timeout-seconds "$BREADTH_TIMEOUT_SECONDS" \
      "${speci_args[@]}" \
      --output-dir "$BREADTH_INPUT_DIR"
  local constraint_csv
  constraint_csv="$(latest_file "$BREADTH_INPUT_DIR/kalshi_temperature_constraint_scan_*.csv")"
  if [[ -z "$constraint_csv" ]]; then
    return 1
  fi
  run_cmd "breadth_settlement_state" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-settlement-state \
      --specs-csv "$specs_csv" \
      --constraint-csv "$constraint_csv" \
      --top-n "$BREADTH_TOP_N" \
      --output-dir "$BREADTH_INPUT_DIR"
  return 0
}

run_profile() {
  local label="$1"
  local disable_interval="$2"
  local max_gap="$3"
  local max_age="$4"
  local profile_dir="$PROFILES_DIR/$label"
  mkdir -p "$profile_dir"

  local specs_csv constraint_csv settlement_state_json metar_summary_json
  specs_csv="$(latest_file "$BREADTH_INPUT_DIR/kalshi_temperature_contract_specs_*.csv")"
  constraint_csv="$(latest_file "$BREADTH_INPUT_DIR/kalshi_temperature_constraint_scan_*.csv")"
  settlement_state_json="$(latest_file "$BREADTH_INPUT_DIR/kalshi_temperature_settlement_state_*.json")"
  metar_summary_json="$(latest_file "$BREADTH_INPUT_DIR/kalshi_temperature_metar_summary_*.json")"
  if [[ -z "$specs_csv" || -z "$constraint_csv" ]]; then
    return 1
  fi

  local -a policy_args=()
  local -a speci_args=()
  local -a settlement_args=()
  local -a metar_args=()
  local -a interval_args=()
  local -a quality_args=()
  if [[ -f "${AUTO_METAR_POLICY_PATH:-}" ]]; then
    policy_args=(--metar-age-policy-json "$AUTO_METAR_POLICY_PATH")
  elif [[ -n "${METAR_AGE_POLICY_JSON:-}" && -f "${METAR_AGE_POLICY_JSON:-}" ]]; then
    policy_args=(--metar-age-policy-json "$METAR_AGE_POLICY_JSON")
  fi
  if [[ -n "${SPECI_CALIBRATION_JSON:-}" && -f "${SPECI_CALIBRATION_JSON:-}" ]]; then
    speci_args=(--speci-calibration-json "$SPECI_CALIBRATION_JSON")
  fi
  if [[ -n "$settlement_state_json" ]]; then
    settlement_args=(--settlement-state-json "$settlement_state_json")
  fi
  if [[ -n "$metar_summary_json" ]]; then
    metar_args=(--metar-summary-json "$metar_summary_json")
  fi
  if [[ "$disable_interval" == "1" ]]; then
    interval_args=(--disable-interval-consistency-gate --max-yes-possible-gap-for-yes-side "$max_gap")
  fi
  if [[ "$BREADTH_ENFORCE_PROBABILITY_EDGE_THRESHOLDS" != "1" ]]; then
    quality_args+=(--disable-enforce-probability-edge-thresholds)
  fi
  if [[ "$BREADTH_ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR" == "1" ]]; then
    quality_args+=(--enforce-entry-price-probability-floor)
  fi
  local effective_min_expected_edge_net
  effective_min_expected_edge_net="$(normalize_min_expected_edge_net \
    "$BREADTH_MIN_EXPECTED_EDGE_NET" \
    "$BREADTH_FALLBACK_MIN_EXPECTED_EDGE_NET" \
    "$BREADTH_ENFORCE_PROBABILITY_EDGE_THRESHOLDS")"
  local effective_fallback_probability="$BREADTH_FALLBACK_MIN_PROBABILITY_CONFIDENCE"
  if [[ -z "$effective_fallback_probability" ]]; then
    effective_fallback_probability="$BREADTH_MIN_SETTLEMENT_CONFIDENCE"
  fi
  if [[ "$effective_fallback_probability" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--fallback-min-probability-confidence "$effective_fallback_probability")
  fi
  if [[ "$BREADTH_FALLBACK_MIN_EXPECTED_EDGE_NET" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--fallback-min-expected-edge-net "$BREADTH_FALLBACK_MIN_EXPECTED_EDGE_NET")
  fi
  if [[ "$BREADTH_FALLBACK_MIN_EDGE_TO_RISK_RATIO" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--fallback-min-edge-to-risk-ratio "$BREADTH_FALLBACK_MIN_EDGE_TO_RISK_RATIO")
  fi
  if [[ "$BREADTH_MIN_PROBABILITY_CONFIDENCE" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--min-probability-confidence "$BREADTH_MIN_PROBABILITY_CONFIDENCE")
  fi
  if [[ "$effective_min_expected_edge_net" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--min-expected-edge-net "$effective_min_expected_edge_net")
  fi
  if [[ "$BREADTH_MIN_EDGE_TO_RISK_RATIO" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--min-edge-to-risk-ratio "$BREADTH_MIN_EDGE_TO_RISK_RATIO")
  fi
  if [[ "$BREADTH_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--metar-freshness-quality-boundary-ratio "$BREADTH_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO")
  fi
  if [[ "$BREADTH_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--metar-freshness-quality-probability-margin "$BREADTH_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN")
  fi
  if [[ "$BREADTH_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    quality_args+=(--metar-freshness-quality-expected-edge-margin "$BREADTH_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN")
  fi

  run_cmd "breadth_profile_${label}" \
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-trader \
      --env-file "$BETBOT_ENV_FILE" \
      --intents-only \
      --disable-require-market-snapshot-seq \
      --specs-csv "$specs_csv" \
      --constraint-csv "$constraint_csv" \
      "${settlement_args[@]}" \
      "${metar_args[@]}" \
      --max-markets "${EFFECTIVE_MAX_MARKETS:-$BREADTH_MAX_MARKETS}" \
      --max-orders "$BREADTH_MAX_ORDERS" \
      --max-intents-per-underlying "$BREADTH_MAX_INTENTS_PER_UNDERLYING" \
      --planning-bankroll "${PLANNING_BANKROLL_DOLLARS:-1000}" \
      --daily-risk-cap "${DAILY_RISK_CAP_DOLLARS:-100}" \
      --max-live-submissions-per-day "${MAX_LIVE_SUBMISSIONS_PER_DAY:-20}" \
      --max-live-cost-per-day-dollars "${MAX_LIVE_COST_PER_DAY_DOLLARS:-100}" \
      --max-metar-age-minutes "$max_age" \
      --min-settlement-confidence "$BREADTH_MIN_SETTLEMENT_CONFIDENCE" \
      --taf-stale-grace-minutes "$BREADTH_TAF_STALE_GRACE_MINUTES" \
      --taf-stale-grace-max-volatility-score "$BREADTH_TAF_STALE_GRACE_MAX_VOLATILITY_SCORE" \
      --taf-stale-grace-max-range-width "$BREADTH_TAF_STALE_GRACE_MAX_RANGE_WIDTH" \
      --min-alpha-strength "$BREADTH_MIN_ALPHA_STRENGTH" \
      --timeout-seconds "$BREADTH_TIMEOUT_SECONDS" \
      "${policy_args[@]}" \
      "${speci_args[@]}" \
      "${quality_args[@]}" \
      "${interval_args[@]}" \
      --output-dir "$profile_dir"
}

run_profiles_batched() {
  local -a labels=("strict_baseline" "relaxed_interval" "relaxed_age")
  local -a disable_interval=("0" "1" "1")
  local -a gaps=("0.0" "$BREADTH_RELAXED_GAP_PRIMARY" "$BREADTH_RELAXED_GAP_PRIMARY")
  local -a ages=("${MAX_METAR_AGE_MINUTES:-22.5}" "${MAX_METAR_AGE_MINUTES:-22.5}" "$BREADTH_RELAXED_AGE_PRIMARY")
  if [[ "$BREADTH_ENABLE_WIDE_GAP_PROFILE" == "1" ]]; then
    labels+=("wide_gap_age")
    disable_interval+=("1")
    gaps+=("$BREADTH_RELAXED_GAP_WIDE")
    ages+=("$BREADTH_RELAXED_AGE_WIDE")
  fi
  if [[ "$BREADTH_ENABLE_ULTRA_GAP_PROFILE" == "1" ]]; then
    labels+=("ultra_gap_age")
    disable_interval+=("1")
    gaps+=("$BREADTH_RELAXED_GAP_ULTRA")
    ages+=("$BREADTH_RELAXED_AGE_ULTRA")
  fi
  if [[ "$BREADTH_ENABLE_ULTRA_AGE_ONLY_PROFILE" == "1" ]]; then
    labels+=("ultra_age_only")
    disable_interval+=("1")
    gaps+=("$BREADTH_RELAXED_GAP_PRIMARY")
    ages+=("$BREADTH_RELAXED_AGE_ULTRA")
  fi

  local total="${#labels[@]}"
  local idx=0
  local had_failures=0
  local batch_size="${EFFECTIVE_PROFILE_PARALLELISM:-$BREADTH_PROFILE_PARALLELISM}"
  if (( batch_size < 1 )); then
    batch_size=1
  fi
  while (( idx < total )); do
    local end="$(( idx + batch_size ))"
    if (( end > total )); then
      end="$total"
    fi
    local -a pids=()
    local i
    for (( i = idx; i < end; i++ )); do
      run_profile "${labels[$i]}" "${disable_interval[$i]}" "${gaps[$i]}" "${ages[$i]}" &
      pids+=("$!")
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

read_target_pressure_snapshot() {
  "$PYTHON_BIN" - "$BREADTH_DASHBOARD_PATH" "$BREADTH_TARGET_UNIQUE_MARKET_SIDES_APPROVED" "$BREADTH_TARGET_UNIQUE_UNDERLYINGS" "$BREADTH_TARGET_CONSENSUS_CANDIDATES" "$OUTPUT_DIR/health/live_status_latest.json" "$OUTPUT_DIR/checkpoints" "$BREADTH_OVERLAP_PRESSURE_ROLLING_WINDOW_TAG" "$BREADTH_OVERLAP_PRESSURE_ROLLING_MAX_AGE_SECONDS" "$BREADTH_OVERLAP_PRESSURE_ROLLING_MIN_INTENTS" "$BREADTH_OVERLAP_PRESSURE_MAX_SETTLEMENT_UNRESOLVED" "$BREADTH_OVERLAP_PRESSURE_BLOCKED_MIN" "$BREADTH_OVERLAP_PRESSURE_BLOCKED_SHARE_MIN" "$BREADTH_OVERLAP_PRESSURE_BLOCKED_SHARE_HIGH" "$BREADTH_REPLAN_PRESSURE_ENABLED" "$BREADTH_REPLAN_PRESSURE_MIN_INPUT_COUNT" "$BREADTH_REPLAN_PRESSURE_BLOCKED_RATIO_MIN" "$BREADTH_REPLAN_PRESSURE_MAX_UNIQUE_MARKET_SIDES" "$BREADTH_REPLAN_PRESSURE_MAX_UNIQUE_UNDERLYINGS" "$BREADTH_REPLAN_PRESSURE_LEVEL" "$BREADTH_REPLAN_PRESSURE_REQUIRE_TARGET_DEFICIT" "$BREADTH_OVERLAP_PRESSURE_ENABLED" "$BREADTH_OVERLAP_PRESSURE_MIN_INTENTS" "$BREADTH_OVERLAP_PRESSURE_RATIO_MIN" "$BREADTH_OVERLAP_PRESSURE_RATIO_HIGH" "$BREADTH_OVERLAP_PRESSURE_REQUIRE_TARGET_DEFICIT" "$BREADTH_OVERLAP_PRESSURE_REQUIRE_LOW_STALE" "$BREADTH_OVERLAP_PRESSURE_MAX_STALE_RATE" "$BREADTH_HEADROOM_EXPLORATION_ENABLED" "$BREADTH_HEADROOM_EXPLORATION_MAX_LOAD_PER_VCPU" "$BREADTH_HEADROOM_EXPLORATION_MIN_INTENTS" "$BREADTH_HEADROOM_EXPLORATION_MIN_APPROVAL_RATE" "$BREADTH_HEADROOM_EXPLORATION_MAX_STALE_RATE" "$BREADTH_HEADROOM_EXPLORATION_LEVEL" "$BREADTH_HEADROOM_EXPLORATION_REQUIRE_TARGET_DEFICIT" <<'PY'
from pathlib import Path
import json
import sys
import time


def parse_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return int(default)


def parse_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


dashboard_path = Path(sys.argv[1])
target_market_sides = max(0, parse_int(sys.argv[2], 0))
target_underlyings = max(0, parse_int(sys.argv[3], 0))
target_consensus = max(0, parse_int(sys.argv[4], 0))
live_status_path = Path(sys.argv[5])
checkpoints_dir = Path(sys.argv[6])
rolling_window_tag = str(sys.argv[7] or "").strip() or "14h"
rolling_max_age_seconds = max(0.0, parse_float(sys.argv[8], 21600.0))
rolling_min_intents = max(0, parse_int(sys.argv[9], 100))
overlap_max_settlement_unresolved = max(0, parse_int(sys.argv[10], 20))
overlap_blocked_min = max(0, parse_int(sys.argv[11], 300))
overlap_blocked_share_min = max(0.0, parse_float(sys.argv[12], 0.35))
overlap_blocked_share_high = max(overlap_blocked_share_min, parse_float(sys.argv[13], overlap_blocked_share_min))
replan_pressure_enabled = str(sys.argv[14]).strip().lower() in {"1", "true", "yes", "y"}
replan_min_input = max(0, parse_int(sys.argv[15], 0))
replan_blocked_ratio_min = max(0.0, parse_float(sys.argv[16], 0.0))
replan_max_market_sides = max(0, parse_int(sys.argv[17], 0))
replan_max_underlyings = max(0, parse_int(sys.argv[18], 0))
replan_target_level = max(0, min(2, parse_int(sys.argv[19], 1)))
replan_require_target_deficit = str(sys.argv[20]).strip().lower() in {"1", "true", "yes", "y"}
overlap_pressure_enabled = str(sys.argv[21]).strip().lower() in {"1", "true", "yes", "y"}
overlap_min_intents = max(0, parse_int(sys.argv[22], 0))
overlap_ratio_min = max(0.0, parse_float(sys.argv[23], 0.0))
overlap_ratio_high = max(overlap_ratio_min, parse_float(sys.argv[24], overlap_ratio_min))
overlap_require_target_deficit = str(sys.argv[25]).strip().lower() in {"1", "true", "yes", "y"}
overlap_require_low_stale = str(sys.argv[26]).strip().lower() in {"1", "true", "yes", "y"}
overlap_max_stale_rate = max(0.0, parse_float(sys.argv[27], 0.0))
headroom_exploration_enabled = str(sys.argv[28]).strip().lower() in {"1", "true", "yes", "y"}
headroom_exploration_max_load = max(0.0, parse_float(sys.argv[29], 0.70))
headroom_exploration_min_intents = max(0, parse_int(sys.argv[30], 0))
headroom_exploration_min_approval_rate = max(0.0, parse_float(sys.argv[31], 0.0))
headroom_exploration_max_stale_rate = max(0.0, parse_float(sys.argv[32], 1.0))
headroom_exploration_level = max(0, min(2, parse_int(sys.argv[33], 1)))
headroom_exploration_require_target_deficit = str(sys.argv[34]).strip().lower() in {"1", "true", "yes", "y"}

market_sides = 0
underlyings = 0
consensus = 0
reason = "targets_met"
level = 0

rolling_overlap_count = 0
rolling_stale_count = 0
rolling_intents_total = 0
rolling_blocked_total = 0
rolling_overlap_ratio = 0.0
rolling_overlap_blocked_share = 0.0
rolling_stale_rate = 0.0
rolling_metrics_fresh = False

if checkpoints_dir.exists():
    try:
        window_candidates = sorted(
            checkpoints_dir.glob(f"station_tuning_window_{rolling_window_tag}_*.json"),
            key=lambda p: p.stat().st_mtime,
        )
    except Exception:
        window_candidates = []
    if window_candidates:
        latest_window = window_candidates[-1]
        try:
            window_payload = json.loads(latest_window.read_text(encoding="utf-8"))
        except Exception:
            window_payload = {}
        age_seconds = -1.0
        try:
            age_seconds = max(0.0, time.time() - latest_window.stat().st_mtime)
        except Exception:
            age_seconds = -1.0
        if isinstance(window_payload, dict) and (
            rolling_max_age_seconds <= 0.0 or age_seconds < 0.0 or age_seconds <= rolling_max_age_seconds
        ):
            totals = window_payload.get("totals") if isinstance(window_payload.get("totals"), dict) else {}
            reason_counts = (
                window_payload.get("policy_reason_counts")
                if isinstance(window_payload.get("policy_reason_counts"), dict)
                else {}
            )
            rolling_intents_total = max(0, parse_int(totals.get("intents_total"), 0))
            rolling_overlap_count = max(
                0,
                parse_int(reason_counts.get("yes_range_still_possible"), 0)
                + parse_int(reason_counts.get("no_side_interval_overlap_still_possible"), 0),
            )
            rolling_stale_count = max(0, parse_int(reason_counts.get("metar_observation_stale"), 0))
            rolling_approved = max(
                0,
                parse_int(totals.get("approved"), parse_int(reason_counts.get("approved"), 0)),
            )
            if rolling_approved > rolling_intents_total:
                rolling_approved = rolling_intents_total
            rolling_blocked_total = max(0, rolling_intents_total - rolling_approved)
            if rolling_intents_total > 0:
                rolling_overlap_ratio = max(0.0, min(1.0, rolling_overlap_count / float(rolling_intents_total)))
                rolling_stale_rate = max(0.0, min(1.0, rolling_stale_count / float(rolling_intents_total)))
            if rolling_blocked_total > 0:
                rolling_overlap_blocked_share = max(
                    0.0,
                    min(1.0, rolling_overlap_count / float(rolling_blocked_total)),
                )
            rolling_metrics_fresh = True

if dashboard_path.exists():
    try:
        payload = json.loads(dashboard_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
else:
    payload = {}

union_metrics = payload.get("union_metrics") if isinstance(payload.get("union_metrics"), dict) else {}
consensus_block = payload.get("consensus") if isinstance(payload.get("consensus"), dict) else {}

market_sides = max(0, parse_int(union_metrics.get("unique_market_sides_approved_rows"), 0))
underlyings = max(0, parse_int(union_metrics.get("unique_underlyings_all_rows"), 0))
consensus = max(0, parse_int(consensus_block.get("consensus_candidate_count"), 0))

market_ratio = 1.0 if target_market_sides <= 0 else min(1.0, market_sides / float(target_market_sides))
underlying_ratio = 1.0 if target_underlyings <= 0 else min(1.0, underlyings / float(target_underlyings))
consensus_ratio = 1.0 if target_consensus <= 0 else min(1.0, consensus / float(target_consensus))

avg_ratio = (market_ratio + underlying_ratio + consensus_ratio) / 3.0

if target_market_sides > 0 and market_sides < target_market_sides:
    reason = "below_target_unique_market_sides"
if target_underlyings > 0 and underlyings < target_underlyings:
    reason = "below_target_unique_underlyings"
if target_consensus > 0 and consensus < target_consensus:
    reason = "below_target_consensus_candidates"
if reason != "targets_met":
    deficit_depth = 1.0 - avg_ratio
    if (
        deficit_depth >= 0.55
        or (target_market_sides > 0 and market_sides <= max(0, int(target_market_sides * 0.35)))
        or (target_underlyings > 0 and underlyings <= max(0, int(target_underlyings * 0.35)))
    ):
        level = 2
    else:
        level = 1

if replan_pressure_enabled and live_status_path.exists():
    try:
        live_payload = json.loads(live_status_path.read_text(encoding="utf-8"))
    except Exception:
        live_payload = {}
    if isinstance(live_payload, dict):
        replan_block = (
            live_payload.get("replan_cooldown")
            if isinstance(live_payload.get("replan_cooldown"), dict)
            else {}
        )
        replan_input = max(0, parse_int(replan_block.get("input_count"), 0))
        replan_blocked_ratio = max(0.0, parse_float(replan_block.get("blocked_ratio"), 0.0))
        replan_unique_market_sides = max(0, parse_int(replan_block.get("unique_market_sides"), 0))
        replan_unique_underlyings = max(0, parse_int(replan_block.get("unique_underlyings"), 0))
        replan_market_sides_bottleneck = (
            replan_max_market_sides > 0 and replan_unique_market_sides <= replan_max_market_sides
        )
        replan_underlyings_bottleneck = (
            replan_max_underlyings > 0 and replan_unique_underlyings <= replan_max_underlyings
        )
        if (
            replan_input >= replan_min_input
            and replan_blocked_ratio >= replan_blocked_ratio_min
            and (replan_market_sides_bottleneck or replan_underlyings_bottleneck)
            and (
                not replan_require_target_deficit
                or market_ratio < 0.999
                or underlying_ratio < 0.999
                or consensus_ratio < 0.999
            )
        ):
            promoted_level = max(level, replan_target_level)
            severe_ratio = max(replan_blocked_ratio_min + 0.15, 0.85)
            if (
                replan_blocked_ratio >= severe_ratio
                and (
                    (replan_max_market_sides > 0 and replan_unique_market_sides <= max(1, int(replan_max_market_sides * 0.6)))
                    or (replan_max_underlyings > 0 and replan_unique_underlyings <= max(1, int(replan_max_underlyings * 0.6)))
                )
            ):
                promoted_level = max(promoted_level, 2)
            if promoted_level > level:
                level = promoted_level
                if reason == "targets_met":
                    reason = "replan_breadth_bottleneck"
                elif "replan_breadth_bottleneck" not in reason:
                    reason = f"{reason}+replan_breadth_bottleneck"

if overlap_pressure_enabled and live_status_path.exists():
    try:
        live_payload = json.loads(live_status_path.read_text(encoding="utf-8"))
    except Exception:
        live_payload = {}
    if isinstance(live_payload, dict):
        freshness = (
            live_payload.get("freshness_plan")
            if isinstance(live_payload.get("freshness_plan"), dict)
            else {}
        )
        scan_budget = (
            live_payload.get("scan_budget")
            if isinstance(live_payload.get("scan_budget"), dict)
            else {}
        )
        overlap_count = max(0, parse_int(freshness.get("yes_range_still_possible_count"), 0))
        intents_hint = max(0, parse_int(scan_budget.get("intents_total_hint"), 0))
        stale_rate = max(0.0, parse_float(freshness.get("metar_observation_stale_rate"), 0.0))
        settlement_refresh = (
            live_payload.get("settlement_refresh_plan")
            if isinstance(live_payload.get("settlement_refresh_plan"), dict)
            else {}
        )
        settlement_blocked_underlyings = max(
            0,
            parse_int(settlement_refresh.get("settlement_finalization_blocked_underlyings"), 0),
        )
        settlement_pending = max(
            0,
            parse_int(settlement_refresh.get("pending_final_report_count"), 0),
        )
        settlement_unresolved = settlement_blocked_underlyings + settlement_pending
        overlap_ratio = 0.0
        if intents_hint > 0:
            overlap_ratio = max(0.0, min(1.0, overlap_count / float(intents_hint)))
        effective_overlap_ratio = overlap_ratio
        if rolling_metrics_fresh and rolling_intents_total >= rolling_min_intents:
            effective_overlap_ratio = max(effective_overlap_ratio, rolling_overlap_ratio)
        effective_stale_rate = stale_rate
        if rolling_metrics_fresh and rolling_intents_total >= rolling_min_intents:
            effective_stale_rate = max(effective_stale_rate, rolling_stale_rate)
        overlap_blocked_share_trigger = (
            rolling_metrics_fresh
            and rolling_blocked_total >= overlap_blocked_min
            and rolling_overlap_blocked_share >= overlap_blocked_share_min
        )
        meets_stale_guard = (not overlap_require_low_stale) or (effective_stale_rate <= overlap_max_stale_rate)
        settlement_backlog_guard = settlement_unresolved <= overlap_max_settlement_unresolved
        if (
            intents_hint >= overlap_min_intents
            and (effective_overlap_ratio >= overlap_ratio_min or overlap_blocked_share_trigger)
            and meets_stale_guard
            and settlement_backlog_guard
            and (
                not overlap_require_target_deficit
                or market_ratio < 0.999
                or underlying_ratio < 0.999
                or consensus_ratio < 0.999
            )
        ):
            promoted_level = max(level, 1)
            if (
                effective_overlap_ratio >= overlap_ratio_high
                or (
                    overlap_blocked_share_trigger
                    and rolling_overlap_blocked_share >= overlap_blocked_share_high
                )
            ):
                promoted_level = max(promoted_level, 2)
            if promoted_level > level:
                level = promoted_level
                if reason == "targets_met":
                    reason = "range_overlap_pressure"
                elif "range_overlap_pressure" not in reason:
                    reason = f"{reason}+range_overlap_pressure"
                if overlap_blocked_share_trigger and "blockedshare" not in reason:
                    reason = f"{reason}+blockedshare"
                if (
                    rolling_metrics_fresh
                    and rolling_intents_total >= rolling_min_intents
                    and rolling_overlap_ratio > overlap_ratio
                    and "rolling" not in reason
                ):
                    reason = f"{reason}+rolling"

if headroom_exploration_enabled and live_status_path.exists():
    try:
        live_payload = json.loads(live_status_path.read_text(encoding="utf-8"))
    except Exception:
        live_payload = {}
    if isinstance(live_payload, dict):
        freshness = (
            live_payload.get("freshness_plan")
            if isinstance(live_payload.get("freshness_plan"), dict)
            else {}
        )
        scan_budget = (
            live_payload.get("scan_budget")
            if isinstance(live_payload.get("scan_budget"), dict)
            else {}
        )
        load_per_vcpu = max(0.0, parse_float(scan_budget.get("load_per_vcpu"), 0.0))
        if load_per_vcpu <= 0.0:
            load_per_vcpu = max(0.0, parse_float(scan_budget.get("load_per_vcpu_milli"), 0.0) / 1000.0)
        intents_hint = max(0, parse_int(scan_budget.get("intents_total_hint"), 0))
        approval_rate = max(0.0, parse_float(freshness.get("approval_rate"), 0.0))
        stale_rate = max(0.0, parse_float(freshness.get("metar_observation_stale_rate"), 0.0))
        if (
            load_per_vcpu <= headroom_exploration_max_load
            and intents_hint >= headroom_exploration_min_intents
            and approval_rate >= headroom_exploration_min_approval_rate
            and stale_rate <= headroom_exploration_max_stale_rate
            and (
                not headroom_exploration_require_target_deficit
                or market_ratio < 0.999
                or underlying_ratio < 0.999
                or consensus_ratio < 0.999
            )
        ):
            promoted_level = max(level, headroom_exploration_level)
            if promoted_level > level:
                level = promoted_level
                if reason == "targets_met":
                    reason = "headroom_exploration"
                elif "headroom_exploration" not in reason:
                    reason = f"{reason}+headroom_exploration"

print(
    "|".join(
        [
            str(level),
            str(reason),
            str(market_sides),
            str(underlyings),
            str(consensus),
            str(target_market_sides),
            str(target_underlyings),
            str(target_consensus),
            f"{market_ratio:.6f}",
            f"{underlying_ratio:.6f}",
            f"{consensus_ratio:.6f}",
        ]
    )
)
PY
}

compute_effective_profile_parallelism() {
  local configured current next min_parallel max_parallel load_milli
  local step_up step_up_fast step_down step_down_fast
  configured="$BREADTH_PROFILE_PARALLELISM"
  min_parallel="$BREADTH_PROFILE_PARALLELISM_MIN"
  max_parallel="$BREADTH_PROFILE_PARALLELISM_MAX"
  step_up="$BREADTH_ADAPTIVE_PARALLELISM_STEP_UP"
  step_up_fast="$BREADTH_ADAPTIVE_PARALLELISM_STEP_UP_FAST"
  step_down="$BREADTH_ADAPTIVE_PARALLELISM_STEP_DOWN"
  step_down_fast="$BREADTH_ADAPTIVE_PARALLELISM_STEP_DOWN_FAST"

  if (( configured < 1 )); then
    configured=1
  fi
  if (( min_parallel < 1 )); then
    min_parallel=1
  fi
  if (( max_parallel < min_parallel )); then
    max_parallel="$min_parallel"
  fi
  if (( configured < min_parallel )); then
    configured="$min_parallel"
  fi
  if (( configured > max_parallel )); then
    configured="$max_parallel"
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

  current="$configured"
  if [[ -f "$BREADTH_ADAPTIVE_PARALLELISM_STATE_FILE" ]]; then
    local saved
    saved="$("$PYTHON_BIN" - "$BREADTH_ADAPTIVE_PARALLELISM_STATE_FILE" <<'PY'
from pathlib import Path
import json
import sys
path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("")
    raise SystemExit(0)
value = payload.get("current_parallelism")
if isinstance(value, int):
    print(value)
elif isinstance(value, float):
    print(int(value))
else:
    print("")
PY
)"
    if [[ "$saved" =~ ^[0-9]+$ ]]; then
      current="$saved"
    fi
  fi
  if (( current < min_parallel )); then
    current="$min_parallel"
  fi
  if (( current > max_parallel )); then
    current="$max_parallel"
  fi

  load_milli="$(load_per_vcpu_milli)"
  next="$current"
  if [[ "$BREADTH_PROFILE_PARALLELISM_ADAPTIVE_ENABLED" == "1" ]]; then
    if (( load_milli <= BREADTH_ADAPTIVE_LOAD_VERY_LOW_MILLI )); then
      next="$(( current + step_up_fast ))"
    elif (( load_milli <= BREADTH_ADAPTIVE_LOAD_LOW_MILLI )); then
      next="$(( current + step_up ))"
    elif (( current < configured )) && (( load_milli < BREADTH_ADAPTIVE_LOAD_HIGH_MILLI )); then
      # Avoid getting stuck under configured baseline after temporary downshifts.
      next="$(( current + step_up ))"
    elif (( current > configured )) && (( load_milli > BREADTH_ADAPTIVE_LOAD_LOW_MILLI )); then
      # Drift back toward configured when load no longer justifies elevated parallelism.
      next="$(( current - step_down ))"
    elif (( load_milli >= BREADTH_ADAPTIVE_LOAD_VERY_HIGH_MILLI )); then
      next="$(( current - step_down_fast ))"
    elif (( load_milli >= BREADTH_ADAPTIVE_LOAD_HIGH_MILLI )); then
      next="$(( current - step_down ))"
    fi
  else
    next="$configured"
  fi

  if (( next < min_parallel )); then
    next="$min_parallel"
  fi
  if (( next > max_parallel )); then
    next="$max_parallel"
  fi

  "$PYTHON_BIN" - "$BREADTH_ADAPTIVE_PARALLELISM_STATE_FILE" "$configured" "$next" "$load_milli" "$min_parallel" "$max_parallel" "$BREADTH_ADAPTIVE_LOAD_VERY_LOW_MILLI" "$BREADTH_ADAPTIVE_LOAD_LOW_MILLI" "$BREADTH_ADAPTIVE_LOAD_HIGH_MILLI" "$BREADTH_ADAPTIVE_LOAD_VERY_HIGH_MILLI" "$step_up" "$step_up_fast" "$step_down" "$step_down_fast" "$BREADTH_PROFILE_PARALLELISM_ADAPTIVE_ENABLED" <<'PY'
from pathlib import Path
import json
import sys
from datetime import datetime, timezone

path = Path(sys.argv[1])
payload = {
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "configured_parallelism": int(sys.argv[2]),
    "current_parallelism": int(sys.argv[3]),
    "load_per_vcpu_milli": int(sys.argv[4]),
    "min_parallelism": int(sys.argv[5]),
    "max_parallelism": int(sys.argv[6]),
    "load_very_low_milli": int(sys.argv[7]),
    "load_low_milli": int(sys.argv[8]),
    "load_high_milli": int(sys.argv[9]),
    "load_very_high_milli": int(sys.argv[10]),
    "step_up": int(sys.argv[11]),
    "step_up_fast": int(sys.argv[12]),
    "step_down": int(sys.argv[13]),
    "step_down_fast": int(sys.argv[14]),
    "adaptive_enabled": str(sys.argv[15]).strip() == "1",
}
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY

  echo "$next"
}

compute_effective_max_markets() {
  local configured current next min_markets max_markets load_milli scan_duration
  local step_up step_up_fast step_down step_down_fast target_scan_seconds
  local scan_slack_seconds scan_hard_slack_seconds scan_low_bound scan_low_hard_bound
  local scan_high_bound scan_high_hard_bound
  local pressure_cap_markets
  local target_step_up target_step_up_fast
  local target_snapshot target_level target_reason target_market_sides target_underlyings
  local target_consensus_candidates target_market_target target_underlying_target target_consensus_target
  local target_market_ratio target_underlying_ratio target_consensus_ratio target_pressure_applied
  configured="$BREADTH_MAX_MARKETS"
  min_markets="$BREADTH_MAX_MARKETS_MIN"
  max_markets="$BREADTH_MAX_MARKETS_MAX"
  step_up="$BREADTH_MAX_MARKETS_STEP_UP"
  step_up_fast="$BREADTH_MAX_MARKETS_STEP_UP_FAST"
  step_down="$BREADTH_MAX_MARKETS_STEP_DOWN"
  step_down_fast="$BREADTH_MAX_MARKETS_STEP_DOWN_FAST"
  target_scan_seconds="$BREADTH_MAX_MARKETS_TARGET_SCAN_SECONDS"
  scan_slack_seconds="$BREADTH_MAX_MARKETS_TARGET_SCAN_SLACK_SECONDS"
  scan_hard_slack_seconds="$BREADTH_MAX_MARKETS_TARGET_SCAN_HARD_SLACK_SECONDS"
  target_step_up="$BREADTH_TARGET_STEP_UP"
  target_step_up_fast="$BREADTH_TARGET_STEP_UP_FAST"
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
  if (( target_step_up < 1 )); then
    target_step_up=1
  fi
  if (( target_step_up_fast < target_step_up )); then
    target_step_up_fast="$target_step_up"
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
  pressure_cap_markets="$max_markets"
  local computed_pressure_cap
  computed_pressure_cap="$("$PYTHON_BIN" - "$configured" "$min_markets" "$max_markets" "$BREADTH_TARGET_PRESSURE_CAP_MULTIPLIER" <<'PY'
import math
import sys
try:
    configured = int(float(sys.argv[1]))
except Exception:
    configured = 1
try:
    minimum = int(float(sys.argv[2]))
except Exception:
    minimum = 1
try:
    maximum = int(float(sys.argv[3]))
except Exception:
    maximum = max(1, minimum)
try:
    multiplier = float(sys.argv[4])
except Exception:
    multiplier = 1.0
if minimum < 1:
    minimum = 1
if maximum < minimum:
    maximum = minimum
if configured < minimum:
    configured = minimum
if configured > maximum:
    configured = maximum
if not math.isfinite(multiplier):
    multiplier = 1.0
if multiplier < 1.0:
    multiplier = 1.0
cap = int(round(configured * multiplier))
if cap < minimum:
    cap = minimum
if cap > maximum:
    cap = maximum
print(cap)
PY
)"
  if [[ "$computed_pressure_cap" =~ ^[0-9]+$ ]]; then
    pressure_cap_markets="$computed_pressure_cap"
  fi
  if (( pressure_cap_markets < min_markets )); then
    pressure_cap_markets="$min_markets"
  fi
  if (( pressure_cap_markets > max_markets )); then
    pressure_cap_markets="$max_markets"
  fi

  current="$configured"
  scan_duration="${LAST_CONSTRAINT_SCAN_DURATION_SECONDS:-0}"
  if [[ -f "$BREADTH_ADAPTIVE_MAX_MARKETS_STATE_FILE" ]]; then
    local saved_current saved_duration
    saved_current="$("$PYTHON_BIN" - "$BREADTH_ADAPTIVE_MAX_MARKETS_STATE_FILE" <<'PY'
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
    if [[ -z "${LAST_CONSTRAINT_SCAN_DURATION_SECONDS:-}" || "${LAST_CONSTRAINT_SCAN_DURATION_SECONDS:-0}" == "0" ]]; then
      saved_duration="$("$PYTHON_BIN" - "$BREADTH_ADAPTIVE_MAX_MARKETS_STATE_FILE" <<'PY'
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
  target_level=0
  target_reason="targets_met"
  target_market_sides=0
  target_underlyings=0
  target_consensus_candidates=0
  target_market_target=0
  target_underlying_target=0
  target_consensus_target=0
  target_market_ratio="1.0"
  target_underlying_ratio="1.0"
  target_consensus_ratio="1.0"
  target_pressure_applied="none"
  target_snapshot="$(read_target_pressure_snapshot)"
  if [[ -n "$target_snapshot" ]]; then
    IFS='|' read -r target_level target_reason target_market_sides target_underlyings target_consensus_candidates target_market_target target_underlying_target target_consensus_target target_market_ratio target_underlying_ratio target_consensus_ratio <<<"$target_snapshot"
  fi
  if ! [[ "$target_level" =~ ^[0-9]+$ ]]; then
    target_level=0
  fi
  if ! [[ "$target_market_sides" =~ ^[0-9]+$ ]]; then
    target_market_sides=0
  fi
  if ! [[ "$target_underlyings" =~ ^[0-9]+$ ]]; then
    target_underlyings=0
  fi
  if ! [[ "$target_consensus_candidates" =~ ^[0-9]+$ ]]; then
    target_consensus_candidates=0
  fi
  if ! [[ "$target_market_target" =~ ^[0-9]+$ ]]; then
    target_market_target=0
  fi
  if ! [[ "$target_underlying_target" =~ ^[0-9]+$ ]]; then
    target_underlying_target=0
  fi
  if ! [[ "$target_consensus_target" =~ ^[0-9]+$ ]]; then
    target_consensus_target=0
  fi

  if [[ "$BREADTH_MAX_MARKETS_ADAPTIVE_ENABLED" == "1" ]]; then
    if (( load_milli >= BREADTH_ADAPTIVE_LOAD_VERY_HIGH_MILLI || scan_duration > scan_high_hard_bound )); then
      next="$(( current - step_down_fast ))"
    elif (( load_milli >= BREADTH_ADAPTIVE_LOAD_HIGH_MILLI || scan_duration > scan_high_bound )); then
      next="$(( current - step_down ))"
    elif (( load_milli <= BREADTH_ADAPTIVE_LOAD_VERY_LOW_MILLI && scan_duration > 0 && scan_duration < scan_low_hard_bound )); then
      next="$(( current + step_up_fast ))"
    elif (( load_milli <= BREADTH_ADAPTIVE_LOAD_LOW_MILLI && scan_duration > 0 && scan_duration < scan_low_bound )); then
      next="$(( current + step_up ))"
    fi
    if (( target_level >= 2 )) && (( load_milli <= BREADTH_ADAPTIVE_LOAD_HIGH_MILLI )) && (( scan_duration == 0 || scan_duration <= scan_high_hard_bound )); then
      next="$(( next + target_step_up_fast ))"
      target_pressure_applied="fast"
    elif (( target_level >= 1 )) && (( load_milli <= BREADTH_ADAPTIVE_LOAD_HIGH_MILLI )) && (( scan_duration == 0 || scan_duration <= scan_high_bound )); then
      next="$(( next + target_step_up ))"
      target_pressure_applied="normal"
    fi
    if [[ "$target_pressure_applied" != "none" ]] && (( next > pressure_cap_markets )); then
      next="$pressure_cap_markets"
      target_pressure_applied="${target_pressure_applied}_capped"
    fi
    if (( target_level == 0 )) && (( current > configured )) && (( scan_duration == 0 || scan_duration <= target_scan_seconds )); then
      local decay_step
      decay_step="$step_down"
      if (( load_milli >= BREADTH_ADAPTIVE_LOAD_HIGH_MILLI )); then
        decay_step="$step_down_fast"
      fi
      next="$(( current - decay_step ))"
      if (( next < configured )); then
        next="$configured"
      fi
      if [[ "$target_pressure_applied" == "none" ]]; then
        target_pressure_applied="decay_to_configured"
      else
        target_pressure_applied="${target_pressure_applied}+decay_to_configured"
      fi
    fi
  else
    next="$configured"
  fi

  if (( next < min_markets )); then
    next="$min_markets"
  fi
  if (( next > max_markets )); then
    next="$max_markets"
  fi

  "$PYTHON_BIN" - "$BREADTH_ADAPTIVE_MAX_MARKETS_STATE_FILE" "$configured" "$next" "$load_milli" "$min_markets" "$max_markets" "$scan_duration" "$target_scan_seconds" "$scan_slack_seconds" "$scan_hard_slack_seconds" "$step_up" "$step_up_fast" "$step_down" "$step_down_fast" "$BREADTH_ADAPTIVE_LOAD_VERY_LOW_MILLI" "$BREADTH_ADAPTIVE_LOAD_LOW_MILLI" "$BREADTH_ADAPTIVE_LOAD_HIGH_MILLI" "$BREADTH_ADAPTIVE_LOAD_VERY_HIGH_MILLI" "$BREADTH_MAX_MARKETS_ADAPTIVE_ENABLED" "$target_level" "$target_reason" "$target_pressure_applied" "$target_market_sides" "$target_underlyings" "$target_consensus_candidates" "$target_market_ratio" "$target_underlying_ratio" "$target_consensus_ratio" "$target_market_target" "$target_underlying_target" "$target_consensus_target" "$pressure_cap_markets" "$BREADTH_TARGET_PRESSURE_CAP_MULTIPLIER" "$BREADTH_REPLAN_PRESSURE_REQUIRE_TARGET_DEFICIT" <<'PY'
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
    "load_very_low_milli": int(sys.argv[15]),
    "load_low_milli": int(sys.argv[16]),
    "load_high_milli": int(sys.argv[17]),
    "load_very_high_milli": int(sys.argv[18]),
    "adaptive_enabled": str(sys.argv[19]).strip() == "1",
    "target_pressure": {
        "level": int(sys.argv[20]),
        "reason": str(sys.argv[21] or "").strip(),
        "applied_step_mode": str(sys.argv[22] or "").strip(),
        "current_metrics": {
            "unique_market_sides_approved_rows": int(sys.argv[23]),
            "unique_underlyings_all_rows": int(sys.argv[24]),
            "consensus_candidate_count": int(sys.argv[25]),
            "unique_market_sides_ratio": float(sys.argv[26]),
            "unique_underlyings_ratio": float(sys.argv[27]),
            "consensus_candidate_ratio": float(sys.argv[28]),
        },
        "targets": {
            "unique_market_sides_approved_rows": int(sys.argv[29]),
            "unique_underlyings_all_rows": int(sys.argv[30]),
            "consensus_candidate_count": int(sys.argv[31]),
        },
        "pressure_cap_max_markets": int(sys.argv[32]),
        "pressure_cap_multiplier": float(sys.argv[33]),
        "require_target_deficit_for_replan_pressure": str(sys.argv[34]).strip() == "1",
    },
}
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY

  echo "$next"
}

compute_effective_loop_sleep() {
  local configured min_sleep max_sleep next load_milli
  configured="$BREADTH_WORKER_LOOP_SECONDS"
  min_sleep="$BREADTH_WORKER_LOOP_SECONDS_MIN"
  max_sleep="$BREADTH_WORKER_LOOP_SECONDS_MAX"
  if (( configured < 1 )); then
    configured=1
  fi
  if (( min_sleep < 1 )); then
    min_sleep=1
  fi
  if (( max_sleep < min_sleep )); then
    max_sleep="$min_sleep"
  fi
  if (( configured < min_sleep )); then
    configured="$min_sleep"
  fi
  if (( configured > max_sleep )); then
    configured="$max_sleep"
  fi

  load_milli="$(load_per_vcpu_milli)"
  next="$configured"
  if [[ "$BREADTH_LOOP_SLEEP_ADAPTIVE_ENABLED" == "1" ]]; then
    if (( load_milli <= BREADTH_ADAPTIVE_LOAD_VERY_LOW_MILLI )); then
      next="$min_sleep"
    elif (( load_milli <= BREADTH_ADAPTIVE_LOAD_LOW_MILLI )); then
      next="$(( (configured + min_sleep) / 2 ))"
    elif (( load_milli >= BREADTH_ADAPTIVE_LOAD_VERY_HIGH_MILLI )); then
      next="$max_sleep"
    elif (( load_milli >= BREADTH_ADAPTIVE_LOAD_HIGH_MILLI )); then
      next="$(( (configured + max_sleep + 1) / 2 ))"
    fi
  fi

  if (( next < min_sleep )); then
    next="$min_sleep"
  fi
  if (( next > max_sleep )); then
    next="$max_sleep"
  fi

  "$PYTHON_BIN" - "$BREADTH_ADAPTIVE_LOOP_SLEEP_STATE_FILE" "$configured" "$next" "$load_milli" "$min_sleep" "$max_sleep" "$BREADTH_ADAPTIVE_LOAD_VERY_LOW_MILLI" "$BREADTH_ADAPTIVE_LOAD_LOW_MILLI" "$BREADTH_ADAPTIVE_LOAD_HIGH_MILLI" "$BREADTH_ADAPTIVE_LOAD_VERY_HIGH_MILLI" "$BREADTH_LOOP_SLEEP_ADAPTIVE_ENABLED" <<'PY'
from pathlib import Path
import json
import sys
from datetime import datetime, timezone

path = Path(sys.argv[1])
payload = {
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "configured_loop_sleep_seconds": int(sys.argv[2]),
    "current_loop_sleep_seconds": int(sys.argv[3]),
    "load_per_vcpu_milli": int(sys.argv[4]),
    "min_loop_sleep_seconds": int(sys.argv[5]),
    "max_loop_sleep_seconds": int(sys.argv[6]),
    "load_very_low_milli": int(sys.argv[7]),
    "load_low_milli": int(sys.argv[8]),
    "load_high_milli": int(sys.argv[9]),
    "load_very_high_milli": int(sys.argv[10]),
    "adaptive_enabled": str(sys.argv[11]).strip() == "1",
}
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY

  echo "$next"
}

write_dashboard() {
  run_cmd "breadth_worker_dashboard" \
    "$PYTHON_BIN" "$BETBOT_ROOT/infra/digitalocean/build_breadth_worker_dashboard.py" \
      --out-dir "$OUTPUT_DIR" \
      --breadth-worker-dir "$BREADTH_WORKER_DIR" \
      --consensus-output "$BREADTH_CONSENSUS_OUTPUT_PATH" \
      --consensus-top-n "$BREADTH_CONSENSUS_TOP_N" \
      --consensus-min-profile-support "$BREADTH_CONSENSUS_MIN_PROFILE_SUPPORT" \
      --consensus-min-support-ratio "$BREADTH_CONSENSUS_MIN_SUPPORT_RATIO" \
      --output "$BREADTH_DASHBOARD_PATH"
}

last_input_epoch=0
last_dashboard_epoch=0
LAST_CONSTRAINT_SCAN_DURATION_SECONDS=0
EFFECTIVE_MAX_MARKETS="$BREADTH_MAX_MARKETS"
LAST_EFFECTIVE_MAX_MARKETS=""
EFFECTIVE_PROFILE_PARALLELISM="$BREADTH_PROFILE_PARALLELISM"
LAST_EFFECTIVE_PROFILE_PARALLELISM=""
EFFECTIVE_LOOP_SLEEP_SECONDS="$BREADTH_WORKER_LOOP_SECONDS"
LAST_EFFECTIVE_LOOP_SLEEP_SECONDS=""
BREADTH_INPUTS_READY=0

echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") breadth worker start ===" >> "$LOG_FILE"
echo "breadth_worker_dir=$BREADTH_WORKER_DIR input_dir=$BREADTH_INPUT_DIR loop=${BREADTH_WORKER_LOOP_SECONDS}s" >> "$LOG_FILE"

while true; do
  now_epoch="$(date +%s)"
  EFFECTIVE_MAX_MARKETS="$(compute_effective_max_markets)"
  if [[ "$EFFECTIVE_MAX_MARKETS" != "$LAST_EFFECTIVE_MAX_MARKETS" ]]; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] breadth_max_markets effective=$EFFECTIVE_MAX_MARKETS adaptive=$BREADTH_MAX_MARKETS_ADAPTIVE_ENABLED" >> "$LOG_FILE"
    LAST_EFFECTIVE_MAX_MARKETS="$EFFECTIVE_MAX_MARKETS"
  fi

  EFFECTIVE_PROFILE_PARALLELISM="$(compute_effective_profile_parallelism)"
  if [[ "$EFFECTIVE_PROFILE_PARALLELISM" != "$LAST_EFFECTIVE_PROFILE_PARALLELISM" ]]; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] breadth_parallelism effective=$EFFECTIVE_PROFILE_PARALLELISM adaptive=$BREADTH_PROFILE_PARALLELISM_ADAPTIVE_ENABLED" >> "$LOG_FILE"
    LAST_EFFECTIVE_PROFILE_PARALLELISM="$EFFECTIVE_PROFILE_PARALLELISM"
  fi

  EFFECTIVE_LOOP_SLEEP_SECONDS="$(compute_effective_loop_sleep)"
  if [[ "$EFFECTIVE_LOOP_SLEEP_SECONDS" != "$LAST_EFFECTIVE_LOOP_SLEEP_SECONDS" ]]; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] breadth_loop_sleep effective=${EFFECTIVE_LOOP_SLEEP_SECONDS}s adaptive=$BREADTH_LOOP_SLEEP_ADAPTIVE_ENABLED" >> "$LOG_FILE"
    LAST_EFFECTIVE_LOOP_SLEEP_SECONDS="$EFFECTIVE_LOOP_SLEEP_SECONDS"
  fi

  if (( now_epoch - last_input_epoch >= BREADTH_INPUT_REFRESH_SECONDS )); then
    if refresh_inputs; then
      last_input_epoch="$now_epoch"
      BREADTH_INPUTS_READY=1
    else
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] breadth_input_refresh_failed" >> "$LOG_FILE"
    fi
  fi

  if (( BREADTH_INPUTS_READY == 1 )); then
    if ! run_profiles_batched; then
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] breadth_profiles_failed" >> "$LOG_FILE"
    fi
  else
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] breadth_profiles_skipped_inputs_not_ready" >> "$LOG_FILE"
  fi

  if (( now_epoch - last_dashboard_epoch >= BREADTH_DASHBOARD_REFRESH_SECONDS )); then
    if write_dashboard; then
      last_dashboard_epoch="$now_epoch"
    else
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] breadth_dashboard_refresh_failed" >> "$LOG_FILE"
    fi
  fi

  sleep "$EFFECTIVE_LOOP_SLEEP_SECONDS"
done
