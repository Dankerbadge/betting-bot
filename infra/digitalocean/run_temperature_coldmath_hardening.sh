#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-/etc/betbot/temperature-shadow.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing runtime env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -r "$ENV_FILE" ]]; then
  echo "runtime env file is not readable: $ENV_FILE" >&2
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

COLDMATH_HARDENING_ENABLED="${COLDMATH_HARDENING_ENABLED:-1}"
COLDMATH_OUTPUT_DIR="${COLDMATH_OUTPUT_DIR:-$OUTPUT_DIR}"
COLDMATH_SNAPSHOT_DIR="${COLDMATH_SNAPSHOT_DIR:-$BETBOT_ROOT/tmp/coldmath_snapshot}"
COLDMATH_WALLET_ADDRESS="${COLDMATH_WALLET_ADDRESS:-}"

mkdir -p "$COLDMATH_OUTPUT_DIR" "$COLDMATH_OUTPUT_DIR/logs" "$COLDMATH_OUTPUT_DIR/health"
LOG_FILE="${COLDMATH_HARDENING_LOG_FILE:-$COLDMATH_OUTPUT_DIR/logs/coldmath_hardening.log}"
LOCK_FILE="${COLDMATH_HARDENING_LOCK_FILE:-$COLDMATH_OUTPUT_DIR/.coldmath_hardening.lock}"

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

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

if [[ "$COLDMATH_HARDENING_ENABLED" != "1" ]]; then
  echo "[$(timestamp_utc)] coldmath hardening skipped: disabled" >> "$LOG_FILE"
  exit 0
fi

if [[ -z "$COLDMATH_WALLET_ADDRESS" ]]; then
  echo "[$(timestamp_utc)] coldmath hardening skipped: COLDMATH_WALLET_ADDRESS is empty" >> "$LOG_FILE"
  exit 0
fi

if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "[$(timestamp_utc)] coldmath hardening skipped: lock busy" >> "$LOG_FILE"
    exit 0
  fi
else
  echo "[$(timestamp_utc)] coldmath hardening warning: flock unavailable; continuing without lock" >> "$LOG_FILE"
fi

COLDMATH_STALE_HOURS="${COLDMATH_STALE_HOURS:-48.0}"
COLDMATH_DATA_API_BASE_URL="${COLDMATH_DATA_API_BASE_URL:-https://data-api.polymarket.com}"
COLDMATH_GAMMA_BASE_URL="${COLDMATH_GAMMA_BASE_URL:-https://gamma-api.polymarket.com}"
COLDMATH_API_TIMEOUT_SECONDS="${COLDMATH_API_TIMEOUT_SECONDS:-20.0}"
COLDMATH_POSITIONS_PAGE_SIZE="${COLDMATH_POSITIONS_PAGE_SIZE:-500}"
COLDMATH_POSITIONS_MAX_PAGES="${COLDMATH_POSITIONS_MAX_PAGES:-20}"
COLDMATH_REFRESH_CLOSED_POSITIONS="${COLDMATH_REFRESH_CLOSED_POSITIONS:-1}"
COLDMATH_CLOSED_POSITIONS_PAGE_SIZE="${COLDMATH_CLOSED_POSITIONS_PAGE_SIZE:-50}"
COLDMATH_CLOSED_POSITIONS_MAX_PAGES="${COLDMATH_CLOSED_POSITIONS_MAX_PAGES:-20}"
COLDMATH_REFRESH_TRADES="${COLDMATH_REFRESH_TRADES:-1}"
COLDMATH_REFRESH_ACTIVITY="${COLDMATH_REFRESH_ACTIVITY:-1}"
COLDMATH_INCLUDE_TAKER_ONLY_TRADES="${COLDMATH_INCLUDE_TAKER_ONLY_TRADES:-1}"
COLDMATH_INCLUDE_ALL_TRADE_ROLES="${COLDMATH_INCLUDE_ALL_TRADE_ROLES:-1}"
COLDMATH_TRADES_PAGE_SIZE="${COLDMATH_TRADES_PAGE_SIZE:-500}"
COLDMATH_TRADES_MAX_PAGES="${COLDMATH_TRADES_MAX_PAGES:-20}"
COLDMATH_ACTIVITY_PAGE_SIZE="${COLDMATH_ACTIVITY_PAGE_SIZE:-500}"
COLDMATH_ACTIVITY_MAX_PAGES="${COLDMATH_ACTIVITY_MAX_PAGES:-20}"

COLDMATH_MARKET_INGEST_ENABLED="${COLDMATH_MARKET_INGEST_ENABLED:-1}"
COLDMATH_MARKET_INGEST_REFRESH_SNAPSHOT="${COLDMATH_MARKET_INGEST_REFRESH_SNAPSHOT:-0}"
COLDMATH_MARKET_MAX_MARKETS="${COLDMATH_MARKET_MAX_MARKETS:-500}"
COLDMATH_MARKET_PAGE_SIZE="${COLDMATH_MARKET_PAGE_SIZE:-200}"
COLDMATH_MARKET_MAX_PAGES="${COLDMATH_MARKET_MAX_PAGES:-10}"
COLDMATH_MARKET_TIMEOUT_SECONDS="${COLDMATH_MARKET_TIMEOUT_SECONDS:-15.0}"
COLDMATH_MARKET_INCLUDE_INACTIVE="${COLDMATH_MARKET_INCLUDE_INACTIVE:-0}"

COLDMATH_REPLICATION_ENABLED="${COLDMATH_REPLICATION_ENABLED:-1}"
COLDMATH_REPLICATION_TOP_N="${COLDMATH_REPLICATION_TOP_N:-12}"
COLDMATH_REPLICATION_MARKET_TICKERS="${COLDMATH_REPLICATION_MARKET_TICKERS:-}"
COLDMATH_REPLICATION_REQUIRE_LIQUIDITY_FILTER="${COLDMATH_REPLICATION_REQUIRE_LIQUIDITY_FILTER:-1}"
COLDMATH_REPLICATION_REQUIRE_TWO_SIDED_QUOTES="${COLDMATH_REPLICATION_REQUIRE_TWO_SIDED_QUOTES:-1}"
COLDMATH_REPLICATION_MAX_SPREAD_DOLLARS="${COLDMATH_REPLICATION_MAX_SPREAD_DOLLARS:-0.18}"
COLDMATH_REPLICATION_MIN_LIQUIDITY_SCORE="${COLDMATH_REPLICATION_MIN_LIQUIDITY_SCORE:-0.45}"
COLDMATH_REPLICATION_MAX_FAMILY_CANDIDATES="${COLDMATH_REPLICATION_MAX_FAMILY_CANDIDATES:-3}"
COLDMATH_REPLICATION_MAX_FAMILY_SHARE="${COLDMATH_REPLICATION_MAX_FAMILY_SHARE:-0.60}"
COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS="${COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS:-}"
COLDMATH_REPLICATION_USE_EXECUTION_COST_EXCLUSIONS="${COLDMATH_REPLICATION_USE_EXECUTION_COST_EXCLUSIONS:-1}"
COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS_FILE="${COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS_FILE:-$COLDMATH_OUTPUT_DIR/health/execution_cost_tape_latest.json}"
COLDMATH_REPLICATION_REPAIR_DISABLE_LIQUIDITY_FILTER="${COLDMATH_REPLICATION_REPAIR_DISABLE_LIQUIDITY_FILTER:-1}"
COLDMATH_REPLICATION_REPAIR_DISABLE_REQUIRE_TWO_SIDED_QUOTES="${COLDMATH_REPLICATION_REPAIR_DISABLE_REQUIRE_TWO_SIDED_QUOTES:-1}"
COLDMATH_EXECUTION_COST_TAPE_ENABLED="${COLDMATH_EXECUTION_COST_TAPE_ENABLED:-1}"
COLDMATH_EXECUTION_COST_TAPE_WINDOW_HOURS="${COLDMATH_EXECUTION_COST_TAPE_WINDOW_HOURS:-168.0}"
COLDMATH_EXECUTION_COST_TAPE_MIN_CANDIDATE_SAMPLES="${COLDMATH_EXECUTION_COST_TAPE_MIN_CANDIDATE_SAMPLES:-200}"
COLDMATH_EXECUTION_COST_TAPE_MIN_QUOTE_COVERAGE_RATIO="${COLDMATH_EXECUTION_COST_TAPE_MIN_QUOTE_COVERAGE_RATIO:-0.60}"
COLDMATH_EXECUTION_COST_TAPE_MAX_TICKERS="${COLDMATH_EXECUTION_COST_TAPE_MAX_TICKERS:-25}"
COLDMATH_EXECUTION_COST_TAPE_JOURNAL_DB_PATH="${COLDMATH_EXECUTION_COST_TAPE_JOURNAL_DB_PATH:-}"
COLDMATH_EXECUTION_COST_TAPE_MIN_GLOBAL_EXPECTED_EDGE_SHARE_FOR_EXCLUSION="${COLDMATH_EXECUTION_COST_TAPE_MIN_GLOBAL_EXPECTED_EDGE_SHARE_FOR_EXCLUSION:-0.45}"
COLDMATH_EXECUTION_COST_TAPE_MIN_TICKER_ROWS_FOR_EXCLUSION="${COLDMATH_EXECUTION_COST_TAPE_MIN_TICKER_ROWS_FOR_EXCLUSION:-200}"
COLDMATH_EXECUTION_COST_TAPE_EXCLUSION_MAX_QUOTE_COVERAGE_RATIO="${COLDMATH_EXECUTION_COST_TAPE_EXCLUSION_MAX_QUOTE_COVERAGE_RATIO:-0.20}"
COLDMATH_EXECUTION_COST_TAPE_MAX_TICKER_MEAN_SPREAD_FOR_EXCLUSION="${COLDMATH_EXECUTION_COST_TAPE_MAX_TICKER_MEAN_SPREAD_FOR_EXCLUSION:-0.10}"
COLDMATH_EXECUTION_COST_TAPE_MAX_EXCLUDED_TICKERS="${COLDMATH_EXECUTION_COST_TAPE_MAX_EXCLUDED_TICKERS:-12}"
COLDMATH_DECISION_MATRIX_ENABLED="${COLDMATH_DECISION_MATRIX_ENABLED:-1}"
COLDMATH_DECISION_MATRIX_WINDOW_HOURS="${COLDMATH_DECISION_MATRIX_WINDOW_HOURS:-168.0}"
COLDMATH_DECISION_MATRIX_MIN_SETTLED_OUTCOMES="${COLDMATH_DECISION_MATRIX_MIN_SETTLED_OUTCOMES:-25}"
COLDMATH_DECISION_MATRIX_MAX_TOP_BLOCKER_SHARE="${COLDMATH_DECISION_MATRIX_MAX_TOP_BLOCKER_SHARE:-0.55}"
COLDMATH_DECISION_MATRIX_MIN_APPROVAL_RATE="${COLDMATH_DECISION_MATRIX_MIN_APPROVAL_RATE:-0.03}"
COLDMATH_DECISION_MATRIX_MIN_INTENTS_SAMPLE="${COLDMATH_DECISION_MATRIX_MIN_INTENTS_SAMPLE:-1000}"
COLDMATH_DECISION_MATRIX_MAX_SPARSE_EDGE_BLOCK_SHARE="${COLDMATH_DECISION_MATRIX_MAX_SPARSE_EDGE_BLOCK_SHARE:-0.80}"
COLDMATH_HARDENING_FAIL_ON_NOISE="${COLDMATH_HARDENING_FAIL_ON_NOISE:-0}"
COLDMATH_ACTIONABLE_MIN_POSITIONS_ROWS="${COLDMATH_ACTIONABLE_MIN_POSITIONS_ROWS:-100}"
COLDMATH_ACTIONABLE_MIN_CANDIDATES="${COLDMATH_ACTIONABLE_MIN_CANDIDATES:-4}"
COLDMATH_ACTIONABLE_MIN_MATCHED_RATIO="${COLDMATH_ACTIONABLE_MIN_MATCHED_RATIO:-0.20}"
COLDMATH_ACTIONABLE_REQUIRE_INGEST="${COLDMATH_ACTIONABLE_REQUIRE_INGEST:-$COLDMATH_MARKET_INGEST_ENABLED}"
COLDMATH_ACTIONABLE_REQUIRE_REPLICATION="${COLDMATH_ACTIONABLE_REQUIRE_REPLICATION:-$COLDMATH_REPLICATION_ENABLED}"
COLDMATH_ACTIONABLE_REQUIRE_DECISION_MATRIX="${COLDMATH_ACTIONABLE_REQUIRE_DECISION_MATRIX:-$COLDMATH_DECISION_MATRIX_ENABLED}"
COLDMATH_ACTIONABLE_MIN_MATRIX_SCORE="${COLDMATH_ACTIONABLE_MIN_MATRIX_SCORE:-75}"
COLDMATH_ACTIONABLE_ALLOWED_MATRIX_STATUSES="${COLDMATH_ACTIONABLE_ALLOWED_MATRIX_STATUSES:-green,yellow}"
COLDMATH_ACTIONABLE_REQUIRE_MATRIX_SUPPORTS="${COLDMATH_ACTIONABLE_REQUIRE_MATRIX_SUPPORTS:-1}"
COLDMATH_ACTIONABLE_ALLOW_MATRIX_BOOTSTRAP="${COLDMATH_ACTIONABLE_ALLOW_MATRIX_BOOTSTRAP:-1}"
COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_MAX_HOURS="${COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_MAX_HOURS:-336}"
COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_DISABLE_AT_SETTLED_OUTCOMES="${COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_DISABLE_AT_SETTLED_OUTCOMES:-$COLDMATH_DECISION_MATRIX_MIN_SETTLED_OUTCOMES}"
COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_STATE_FILE="${COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_STATE_FILE:-$COLDMATH_OUTPUT_DIR/health/decision_matrix_bootstrap_state.json}"
COLDMATH_LANE_ALERT_ENABLED="${COLDMATH_LANE_ALERT_ENABLED:-1}"
COLDMATH_LANE_ALERT_NOTIFY_STATUS_CHANGE_ONLY="${COLDMATH_LANE_ALERT_NOTIFY_STATUS_CHANGE_ONLY:-1}"
COLDMATH_LANE_ALERT_WEBHOOK_URL="${COLDMATH_LANE_ALERT_WEBHOOK_URL:-${PIPELINE_ALERT_WEBHOOK_URL:-${RECOVERY_WEBHOOK_URL:-${ALERT_WEBHOOK_URL:-}}}}"
COLDMATH_LANE_ALERT_WEBHOOK_THREAD_ID="${COLDMATH_LANE_ALERT_WEBHOOK_THREAD_ID:-${PIPELINE_ALERT_WEBHOOK_THREAD_ID:-${RECOVERY_WEBHOOK_THREAD_ID:-${ALERT_WEBHOOK_THREAD_ID:-}}}}"
COLDMATH_LANE_ALERT_WEBHOOK_TIMEOUT_SECONDS="${COLDMATH_LANE_ALERT_WEBHOOK_TIMEOUT_SECONDS:-5}"
COLDMATH_LANE_ALERT_WEBHOOK_USERNAME="${COLDMATH_LANE_ALERT_WEBHOOK_USERNAME:-BetBot Matrix Lane}"
COLDMATH_LANE_ALERT_MESSAGE_MODE="${COLDMATH_LANE_ALERT_MESSAGE_MODE:-concise}"
COLDMATH_LANE_ALERT_STATE_FILE="${COLDMATH_LANE_ALERT_STATE_FILE:-$COLDMATH_OUTPUT_DIR/health/.decision_matrix_lane_alert_state.json}"
COLDMATH_LANE_ALERT_DEGRADED_STATUSES="${COLDMATH_LANE_ALERT_DEGRADED_STATUSES:-matrix_failed,bootstrap_blocked}"
COLDMATH_LANE_ALERT_DEGRADED_STREAK_THRESHOLD="${COLDMATH_LANE_ALERT_DEGRADED_STREAK_THRESHOLD:-3}"
COLDMATH_LANE_ALERT_DEGRADED_STREAK_NOTIFY_EVERY="${COLDMATH_LANE_ALERT_DEGRADED_STREAK_NOTIFY_EVERY:-3}"
COLDMATH_LANE_ALERT_TARGET_URL="$(build_discord_target_url "$COLDMATH_LANE_ALERT_WEBHOOK_URL" "$COLDMATH_LANE_ALERT_WEBHOOK_THREAD_ID")"

RUN_ID="$(date -u +"%Y%m%d_%H%M%S")_$$"
RUN_START_EPOCH="$(date +%s)"

STAGE_ROWS=()
OVERALL_STATUS="ready"

run_stage() {
  local required="$1"
  local stage_name="$2"
  shift 2
  local stage_start stage_end stage_duration rc status
  stage_start="$(date +%s)"
  rc=0
  if "$@" >> "$LOG_FILE" 2>&1; then
    status="ok"
  else
    rc=$?
    status="error"
  fi
  stage_end="$(date +%s)"
  stage_duration=$(( stage_end - stage_start ))
  STAGE_ROWS+=("${stage_name}|${status}|${rc}|${stage_duration}|${required}")
  if [[ "$status" != "ok" ]]; then
    OVERALL_STATUS="partial"
    if [[ "$required" == "required" ]]; then
      OVERALL_STATUS="error"
      return "$rc"
    fi
  fi
  return 0
}

echo "=== $(timestamp_utc) coldmath hardening start run_id=$RUN_ID ===" >> "$LOG_FILE"

snapshot_cmd=(
  "$PYTHON_BIN" -m betbot.cli coldmath-snapshot-summary
  --snapshot-dir "$COLDMATH_SNAPSHOT_DIR"
  --wallet-address "$COLDMATH_WALLET_ADDRESS"
  --stale-hours "$COLDMATH_STALE_HOURS"
  --refresh-from-api
  --data-api-base-url "$COLDMATH_DATA_API_BASE_URL"
  --api-timeout-seconds "$COLDMATH_API_TIMEOUT_SECONDS"
  --positions-page-size "$COLDMATH_POSITIONS_PAGE_SIZE"
  --positions-max-pages "$COLDMATH_POSITIONS_MAX_PAGES"
  --closed-positions-page-size "$COLDMATH_CLOSED_POSITIONS_PAGE_SIZE"
  --closed-positions-max-pages "$COLDMATH_CLOSED_POSITIONS_MAX_PAGES"
  --trades-page-size "$COLDMATH_TRADES_PAGE_SIZE"
  --trades-max-pages "$COLDMATH_TRADES_MAX_PAGES"
  --activity-page-size "$COLDMATH_ACTIVITY_PAGE_SIZE"
  --activity-max-pages "$COLDMATH_ACTIVITY_MAX_PAGES"
  --output-dir "$COLDMATH_OUTPUT_DIR"
)
if [[ "$COLDMATH_REFRESH_CLOSED_POSITIONS" != "1" ]]; then
  snapshot_cmd+=(--disable-closed-positions-refresh)
fi
if [[ "$COLDMATH_REFRESH_TRADES" != "1" ]]; then
  snapshot_cmd+=(--disable-trades-refresh)
fi
if [[ "$COLDMATH_REFRESH_ACTIVITY" != "1" ]]; then
  snapshot_cmd+=(--disable-activity-refresh)
fi
if [[ "$COLDMATH_INCLUDE_TAKER_ONLY_TRADES" != "1" ]]; then
  snapshot_cmd+=(--disable-taker-only-trades)
fi
if [[ "$COLDMATH_INCLUDE_ALL_TRADE_ROLES" != "1" ]]; then
  snapshot_cmd+=(--disable-all-trade-roles)
fi
run_stage required "coldmath_snapshot_summary" "${snapshot_cmd[@]}"

if [[ "$COLDMATH_MARKET_INGEST_ENABLED" == "1" ]]; then
  ingest_cmd=(
    "$PYTHON_BIN" -m betbot.cli polymarket-market-ingest
    --output-dir "$COLDMATH_OUTPUT_DIR"
    --max-markets "$COLDMATH_MARKET_MAX_MARKETS"
    --page-size "$COLDMATH_MARKET_PAGE_SIZE"
    --max-pages "$COLDMATH_MARKET_MAX_PAGES"
    --gamma-base-url "$COLDMATH_GAMMA_BASE_URL"
    --timeout-seconds "$COLDMATH_MARKET_TIMEOUT_SECONDS"
    --coldmath-snapshot-dir "$COLDMATH_SNAPSHOT_DIR"
    --coldmath-wallet-address "$COLDMATH_WALLET_ADDRESS"
    --coldmath-stale-hours "$COLDMATH_STALE_HOURS"
    --coldmath-data-api-base-url "$COLDMATH_DATA_API_BASE_URL"
    --coldmath-api-timeout-seconds "$COLDMATH_API_TIMEOUT_SECONDS"
    --coldmath-positions-page-size "$COLDMATH_POSITIONS_PAGE_SIZE"
    --coldmath-positions-max-pages "$COLDMATH_POSITIONS_MAX_PAGES"
    --coldmath-closed-positions-page-size "$COLDMATH_CLOSED_POSITIONS_PAGE_SIZE"
    --coldmath-closed-positions-max-pages "$COLDMATH_CLOSED_POSITIONS_MAX_PAGES"
    --coldmath-trades-page-size "$COLDMATH_TRADES_PAGE_SIZE"
    --coldmath-trades-max-pages "$COLDMATH_TRADES_MAX_PAGES"
    --coldmath-activity-page-size "$COLDMATH_ACTIVITY_PAGE_SIZE"
    --coldmath-activity-max-pages "$COLDMATH_ACTIVITY_MAX_PAGES"
  )
  if [[ "$COLDMATH_MARKET_INCLUDE_INACTIVE" == "1" ]]; then
    ingest_cmd+=(--include-inactive)
  fi
  if [[ "$COLDMATH_MARKET_INGEST_REFRESH_SNAPSHOT" == "1" ]]; then
    ingest_cmd+=(--coldmath-refresh-from-api)
  fi
  if [[ "$COLDMATH_REFRESH_CLOSED_POSITIONS" != "1" ]]; then
    ingest_cmd+=(--coldmath-disable-closed-positions-refresh)
  fi
  if [[ "$COLDMATH_REFRESH_TRADES" != "1" ]]; then
    ingest_cmd+=(--coldmath-disable-trades-refresh)
  fi
  if [[ "$COLDMATH_REFRESH_ACTIVITY" != "1" ]]; then
    ingest_cmd+=(--coldmath-disable-activity-refresh)
  fi
  if [[ "$COLDMATH_INCLUDE_TAKER_ONLY_TRADES" != "1" ]]; then
    ingest_cmd+=(--coldmath-disable-taker-only-trades)
  fi
  if [[ "$COLDMATH_INCLUDE_ALL_TRADE_ROLES" != "1" ]]; then
    ingest_cmd+=(--coldmath-disable-all-trade-roles)
  fi
  run_stage optional "polymarket_market_ingest" "${ingest_cmd[@]}"
else
  STAGE_ROWS+=("polymarket_market_ingest|skipped|0|0|optional")
fi

if [[ "$COLDMATH_REPLICATION_ENABLED" == "1" ]]; then
  replication_cmd=(
    "$PYTHON_BIN" -m betbot.cli coldmath-replication-plan
    --output-dir "$COLDMATH_OUTPUT_DIR"
    --top-n "$COLDMATH_REPLICATION_TOP_N"
    --max-spread-dollars "$COLDMATH_REPLICATION_MAX_SPREAD_DOLLARS"
    --min-liquidity-score "$COLDMATH_REPLICATION_MIN_LIQUIDITY_SCORE"
    --max-family-candidates "$COLDMATH_REPLICATION_MAX_FAMILY_CANDIDATES"
    --max-family-share "$COLDMATH_REPLICATION_MAX_FAMILY_SHARE"
  )
  if [[ -n "$COLDMATH_REPLICATION_MARKET_TICKERS" ]]; then
    replication_cmd+=(--market-tickers "$COLDMATH_REPLICATION_MARKET_TICKERS")
  fi
  if [[ -n "$COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS" ]]; then
    replication_cmd+=(--excluded-market-tickers "$COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS")
  fi
  if [[ "$COLDMATH_REPLICATION_REQUIRE_LIQUIDITY_FILTER" != "1" ]]; then
    replication_cmd+=(--disable-liquidity-filter)
  fi
  if [[ "$COLDMATH_REPLICATION_REQUIRE_TWO_SIDED_QUOTES" != "1" ]]; then
    replication_cmd+=(--disable-require-two-sided-quotes)
  fi
  run_stage optional "coldmath_replication_plan" "${replication_cmd[@]}"
else
  STAGE_ROWS+=("coldmath_replication_plan|skipped|0|0|optional")
fi

if [[ "$COLDMATH_EXECUTION_COST_TAPE_ENABLED" == "1" ]]; then
  execution_cost_tape_cmd=(
    "$PYTHON_BIN" -m betbot.cli kalshi-temperature-execution-cost-tape
    --output-dir "$COLDMATH_OUTPUT_DIR"
    --window-hours "$COLDMATH_EXECUTION_COST_TAPE_WINDOW_HOURS"
    --min-candidate-samples "$COLDMATH_EXECUTION_COST_TAPE_MIN_CANDIDATE_SAMPLES"
    --min-quote-coverage-ratio "$COLDMATH_EXECUTION_COST_TAPE_MIN_QUOTE_COVERAGE_RATIO"
    --max-tickers "$COLDMATH_EXECUTION_COST_TAPE_MAX_TICKERS"
    --min-global-expected-edge-share-for-exclusion "$COLDMATH_EXECUTION_COST_TAPE_MIN_GLOBAL_EXPECTED_EDGE_SHARE_FOR_EXCLUSION"
    --min-ticker-rows-for-exclusion "$COLDMATH_EXECUTION_COST_TAPE_MIN_TICKER_ROWS_FOR_EXCLUSION"
    --exclusion-max-quote-coverage-ratio "$COLDMATH_EXECUTION_COST_TAPE_EXCLUSION_MAX_QUOTE_COVERAGE_RATIO"
    --max-ticker-mean-spread-for-exclusion "$COLDMATH_EXECUTION_COST_TAPE_MAX_TICKER_MEAN_SPREAD_FOR_EXCLUSION"
    --max-excluded-tickers "$COLDMATH_EXECUTION_COST_TAPE_MAX_EXCLUDED_TICKERS"
  )
  if [[ -n "$COLDMATH_EXECUTION_COST_TAPE_JOURNAL_DB_PATH" ]]; then
    execution_cost_tape_cmd+=(--journal-db-path "$COLDMATH_EXECUTION_COST_TAPE_JOURNAL_DB_PATH")
  fi
  run_stage optional "execution_cost_tape" "${execution_cost_tape_cmd[@]}"
else
  STAGE_ROWS+=("execution_cost_tape|skipped|0|0|optional")
fi

if [[ "$COLDMATH_REPLICATION_ENABLED" == "1" && "$COLDMATH_REPLICATION_USE_EXECUTION_COST_EXCLUSIONS" == "1" ]]; then
  replication_repair_cmd=(
    "$PYTHON_BIN" -m betbot.cli coldmath-replication-plan
    --output-dir "$COLDMATH_OUTPUT_DIR"
    --top-n "$COLDMATH_REPLICATION_TOP_N"
    --max-spread-dollars "$COLDMATH_REPLICATION_MAX_SPREAD_DOLLARS"
    --min-liquidity-score "$COLDMATH_REPLICATION_MIN_LIQUIDITY_SCORE"
    --max-family-candidates "$COLDMATH_REPLICATION_MAX_FAMILY_CANDIDATES"
    --max-family-share "$COLDMATH_REPLICATION_MAX_FAMILY_SHARE"
  )
  if [[ -n "$COLDMATH_REPLICATION_MARKET_TICKERS" ]]; then
    replication_repair_cmd+=(--market-tickers "$COLDMATH_REPLICATION_MARKET_TICKERS")
  fi
  if [[ -n "$COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS" ]]; then
    replication_repair_cmd+=(--excluded-market-tickers "$COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS")
  fi
  if [[ -n "$COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS_FILE" ]]; then
    replication_repair_cmd+=(--excluded-market-tickers-file "$COLDMATH_REPLICATION_EXCLUDED_MARKET_TICKERS_FILE")
  fi
  if [[ "$COLDMATH_REPLICATION_REQUIRE_LIQUIDITY_FILTER" != "1" || "$COLDMATH_REPLICATION_REPAIR_DISABLE_LIQUIDITY_FILTER" == "1" ]]; then
    replication_repair_cmd+=(--disable-liquidity-filter)
  fi
  if [[ "$COLDMATH_REPLICATION_REQUIRE_TWO_SIDED_QUOTES" != "1" || "$COLDMATH_REPLICATION_REPAIR_DISABLE_REQUIRE_TWO_SIDED_QUOTES" == "1" ]]; then
    replication_repair_cmd+=(--disable-require-two-sided-quotes)
  fi
  run_stage optional "coldmath_replication_plan_repair" "${replication_repair_cmd[@]}"
else
  STAGE_ROWS+=("coldmath_replication_plan_repair|skipped|0|0|optional")
fi

if [[ "$COLDMATH_DECISION_MATRIX_ENABLED" == "1" ]]; then
  decision_matrix_cmd=(
    "$PYTHON_BIN" -m betbot.cli decision-matrix-hardening
    --output-dir "$COLDMATH_OUTPUT_DIR"
    --window-hours "$COLDMATH_DECISION_MATRIX_WINDOW_HOURS"
    --min-settled-outcomes "$COLDMATH_DECISION_MATRIX_MIN_SETTLED_OUTCOMES"
    --max-top-blocker-share "$COLDMATH_DECISION_MATRIX_MAX_TOP_BLOCKER_SHARE"
    --min-approval-rate "$COLDMATH_DECISION_MATRIX_MIN_APPROVAL_RATE"
    --min-intents-sample "$COLDMATH_DECISION_MATRIX_MIN_INTENTS_SAMPLE"
    --max-sparse-edge-block-share "$COLDMATH_DECISION_MATRIX_MAX_SPARSE_EDGE_BLOCK_SHARE"
    --min-execution-cost-candidate-samples "$COLDMATH_EXECUTION_COST_TAPE_MIN_CANDIDATE_SAMPLES"
    --min-execution-cost-quote-coverage-ratio "$COLDMATH_EXECUTION_COST_TAPE_MIN_QUOTE_COVERAGE_RATIO"
  )
  run_stage optional "decision_matrix_hardening" "${decision_matrix_cmd[@]}"
else
  STAGE_ROWS+=("decision_matrix_hardening|skipped|0|0|optional")
fi

latest_snapshot_file="$(ls -1t "$COLDMATH_OUTPUT_DIR"/coldmath_snapshot_summary_*.json 2>/dev/null | head -n 1 || true)"
latest_ingest_file="$(ls -1t "$COLDMATH_OUTPUT_DIR"/polymarket_temperature_markets_summary_*.json 2>/dev/null | head -n 1 || true)"
latest_replication_file="$(ls -1t "$COLDMATH_OUTPUT_DIR"/coldmath_replication_plan_*.json 2>/dev/null | head -n 1 || true)"
latest_decision_matrix_file="$(ls -1t "$COLDMATH_OUTPUT_DIR"/health/decision_matrix_hardening_*.json 2>/dev/null | grep -Ev 'decision_matrix_hardening_latest\.json$' | head -n 1 || true)"

if [[ "$OVERALL_STATUS" == "error" ]]; then
  EXIT_CODE=1
else
  EXIT_CODE=0
fi

RUN_END_EPOCH="$(date +%s)"
RUN_DURATION_SECONDS=$(( RUN_END_EPOCH - RUN_START_EPOCH ))
STAGE_ROWS_JOINED="$(printf '%s;;' "${STAGE_ROWS[@]}")"
HEALTH_DIR="$COLDMATH_OUTPUT_DIR/health"

health_emit_result="$("$PYTHON_BIN" - "$HEALTH_DIR" "$RUN_ID" "$OVERALL_STATUS" "$RUN_START_EPOCH" "$RUN_END_EPOCH" "$RUN_DURATION_SECONDS" "$COLDMATH_WALLET_ADDRESS" "$COLDMATH_OUTPUT_DIR" "$COLDMATH_SNAPSHOT_DIR" "$STAGE_ROWS_JOINED" "$latest_snapshot_file" "$latest_ingest_file" "$latest_replication_file" "$latest_decision_matrix_file" "$COLDMATH_ACTIONABLE_MIN_POSITIONS_ROWS" "$COLDMATH_ACTIONABLE_MIN_CANDIDATES" "$COLDMATH_ACTIONABLE_MIN_MATCHED_RATIO" "$COLDMATH_ACTIONABLE_REQUIRE_INGEST" "$COLDMATH_ACTIONABLE_REQUIRE_REPLICATION" "$COLDMATH_ACTIONABLE_REQUIRE_DECISION_MATRIX" "$COLDMATH_ACTIONABLE_MIN_MATRIX_SCORE" "$COLDMATH_ACTIONABLE_ALLOWED_MATRIX_STATUSES" "$COLDMATH_ACTIONABLE_REQUIRE_MATRIX_SUPPORTS" "$COLDMATH_ACTIONABLE_ALLOW_MATRIX_BOOTSTRAP" "$COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_MAX_HOURS" "$COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_DISABLE_AT_SETTLED_OUTCOMES" "$COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_STATE_FILE" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys


def _to_int(value: str, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_status(path_text: str) -> tuple[str, dict[str, object] | None]:
    path = Path(path_text)
    if not path_text or not path.exists():
        return ("missing", None)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ("invalid_json", None)
    if isinstance(payload, dict):
        status = str(payload.get("status") or "").strip().lower() or "unknown"
        return (status, payload)
    return ("invalid_payload", None)


def _load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, object]) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        return False
    return True


def _safe_unlink(path: Path) -> bool:
    try:
        path.unlink()
    except FileNotFoundError:
        return True
    except Exception:
        return False
    return True


health_dir = Path(sys.argv[1])
run_id = str(sys.argv[2] or "")
overall_status = str(sys.argv[3] or "unknown")
start_epoch = _to_int(sys.argv[4], 0)
end_epoch = _to_int(sys.argv[5], 0)
run_duration_seconds = _to_int(sys.argv[6], 0)
wallet_address = str(sys.argv[7] or "").strip().lower()
output_dir = str(sys.argv[8] or "")
snapshot_dir = str(sys.argv[9] or "")
stage_rows_raw = str(sys.argv[10] or "")
snapshot_file = str(sys.argv[11] or "")
ingest_file = str(sys.argv[12] or "")
replication_file = str(sys.argv[13] or "")
decision_matrix_file = str(sys.argv[14] or "")
actionable_min_positions_rows = _to_int(sys.argv[15], 0)
actionable_min_candidates = _to_int(sys.argv[16], 0)
actionable_min_matched_ratio = _to_float(sys.argv[17], 0.0)
actionable_require_ingest = _to_bool(sys.argv[18])
actionable_require_replication = _to_bool(sys.argv[19])
actionable_require_decision_matrix = _to_bool(sys.argv[20])
actionable_min_matrix_score = _to_int(sys.argv[21], 0)
allowed_matrix_statuses = {
    item.strip().lower()
    for item in str(sys.argv[22] or "").split(",")
    if item.strip()
}
if not allowed_matrix_statuses:
    allowed_matrix_statuses = {"green", "yellow"}
actionable_require_matrix_supports = _to_bool(sys.argv[23])
actionable_allow_matrix_bootstrap = _to_bool(sys.argv[24])
actionable_matrix_bootstrap_max_hours = _to_float(sys.argv[25], 0.0)
actionable_matrix_bootstrap_disable_at_settled_outcomes = _to_int(sys.argv[26], 0)
actionable_matrix_bootstrap_state_file = str(sys.argv[27] or "").strip()

stage_rows: list[dict[str, object]] = []
for row in [item for item in stage_rows_raw.split(";;") if item]:
    name, status, exit_code, duration, required = (row.split("|", 4) + ["", "", "", "", ""])[:5]
    stage_rows.append(
        {
            "stage": name,
            "status": status,
            "exit_code": _to_int(exit_code, 0),
            "duration_seconds": _to_int(duration, 0),
            "required": required,
        }
    )

snapshot_status, snapshot_payload = _load_status(snapshot_file)
ingest_status, ingest_payload = _load_status(ingest_file)
replication_status, replication_payload = _load_status(replication_file)
decision_matrix_status, decision_matrix_payload = _load_status(decision_matrix_file)

captured_at = datetime.now(timezone.utc)
payload: dict[str, object] = {
    "status": overall_status,
    "event": "coldmath_hardening_cycle",
    "captured_at": captured_at.isoformat(),
    "run_id": run_id,
    "started_at_epoch": start_epoch,
    "finished_at_epoch": end_epoch,
    "started_at": (
        datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat() if start_epoch > 0 else ""
    ),
    "finished_at": (
        datetime.fromtimestamp(end_epoch, tz=timezone.utc).isoformat() if end_epoch > 0 else ""
    ),
    "duration_seconds": run_duration_seconds,
    "wallet_address": wallet_address,
    "output_dir": output_dir,
    "snapshot_dir": snapshot_dir,
    "stages": stage_rows,
    "artifacts": {
        "coldmath_snapshot_summary": {"file": snapshot_file, "status": snapshot_status},
        "polymarket_temperature_markets_summary": {"file": ingest_file, "status": ingest_status},
        "coldmath_replication_plan": {"file": replication_file, "status": replication_status},
        "decision_matrix_hardening": {"file": decision_matrix_file, "status": decision_matrix_status},
    },
}

if isinstance(snapshot_payload, dict):
    payload["snapshot_headline"] = {
        "positions_rows": snapshot_payload.get("positions_rows"),
        "closed_positions_rows": snapshot_payload.get("closed_positions_rows"),
        "valuation_timestamp": snapshot_payload.get("valuation_timestamp"),
        "snapshot_age_hours": snapshot_payload.get("snapshot_age_hours"),
        "is_stale": snapshot_payload.get("is_stale"),
    }
if isinstance(ingest_payload, dict):
    payload["market_ingest_headline"] = {
        "markets_count": ingest_payload.get("markets_count"),
        "request_count": ingest_payload.get("request_count"),
        "coldmath_alignment_status": (
            ingest_payload.get("coldmath_temperature_alignment", {}).get("status")
            if isinstance(ingest_payload.get("coldmath_temperature_alignment"), dict)
            else ""
        ),
        "coldmath_matched_positions": (
            ingest_payload.get("coldmath_temperature_alignment", {}).get("matched_positions")
            if isinstance(ingest_payload.get("coldmath_temperature_alignment"), dict)
            else 0
        ),
    }
if isinstance(replication_payload, dict):
    payload["replication_headline"] = {
        "theme": replication_payload.get("theme"),
        "candidate_count": replication_payload.get("candidate_count"),
        "preferred_side": replication_payload.get("preferred_side"),
        "matched_ratio": replication_payload.get("matched_ratio"),
    }
if isinstance(decision_matrix_payload, dict):
    payload["decision_matrix_headline"] = {
        "matrix_health_status": decision_matrix_payload.get("matrix_health_status"),
        "matrix_score": decision_matrix_payload.get("matrix_score"),
        "supports_consistency_and_profitability": decision_matrix_payload.get("supports_consistency_and_profitability"),
        "critical_blockers_count": decision_matrix_payload.get("critical_blockers_count"),
    }

snapshot_positions_rows = 0
snapshot_is_stale = False
if isinstance(snapshot_payload, dict):
    snapshot_positions_rows = _to_int(str(snapshot_payload.get("positions_rows") or 0), 0)
    snapshot_is_stale = bool(snapshot_payload.get("is_stale") is True)

alignment_status = ""
alignment_matched_ratio = 0.0
alignment_matched_positions = 0
if isinstance(ingest_payload, dict):
    alignment = ingest_payload.get("coldmath_temperature_alignment")
    if isinstance(alignment, dict):
        alignment_status = str(alignment.get("status") or "").strip().lower()
        alignment_matched_ratio = _to_float(str(alignment.get("matched_ratio") or 0.0), 0.0)
        alignment_matched_positions = _to_int(str(alignment.get("matched_positions") or 0), 0)

replication_candidate_count = 0
if isinstance(replication_payload, dict):
    replication_candidate_count = _to_int(str(replication_payload.get("candidate_count") or 0), 0)

decision_matrix_health_status = ""
decision_matrix_score = 0
decision_matrix_supports_consistency = False
decision_matrix_supports_bootstrap = False
critical_blockers_count = 0
decision_matrix_settled_outcomes = 0
if isinstance(decision_matrix_payload, dict):
    decision_matrix_health_status = str(
        decision_matrix_payload.get("matrix_health_status") or ""
    ).strip().lower()
    decision_matrix_score = _to_int(str(decision_matrix_payload.get("matrix_score") or 0), 0)
    decision_matrix_supports_consistency = bool(
        decision_matrix_payload.get("supports_consistency_and_profitability") is True
    )
    decision_matrix_supports_bootstrap = bool(
        decision_matrix_payload.get("supports_bootstrap_progression") is True
    )
    critical_blockers_count = _to_int(
        str(decision_matrix_payload.get("critical_blockers_count") or 0),
        0,
    )
    observed_metrics = decision_matrix_payload.get("observed_metrics")
    if isinstance(observed_metrics, dict):
        decision_matrix_settled_outcomes = _to_int(
            str(observed_metrics.get("settled_outcomes") or 0),
            0,
        )

bootstrap_state_path = (
    Path(actionable_matrix_bootstrap_state_file).expanduser()
    if actionable_matrix_bootstrap_state_file
    else None
)

check_snapshot = bool(
    snapshot_status in {"ready", "partial"} and snapshot_positions_rows >= actionable_min_positions_rows and not snapshot_is_stale
)
check_ingest = True
check_replication = True
check_decision_matrix = True
strict_decision_matrix = True
bootstrap_decision_matrix = False
bootstrap_decision_matrix_raw = False
bootstrap_guard_status = "not_applicable"
bootstrap_guard_reasons: list[str] = []
bootstrap_guard_warnings: list[str] = []
bootstrap_guard_elapsed_hours = None
bootstrap_guard_first_seen_epoch = 0
bootstrap_guard_last_seen_epoch = 0
bootstrap_guard_hours_to_expiry = None
if actionable_require_ingest:
    check_ingest = bool(
        ingest_status in {"ready", "partial", "ready_partial"}
        and alignment_status in {"ready", "partial", "ready_partial", "empty_positions"}
        and alignment_matched_ratio >= actionable_min_matched_ratio
        and alignment_matched_positions > 0
    )
if actionable_require_replication:
    check_replication = bool(
        replication_status == "ready" and replication_candidate_count >= actionable_min_candidates
    )
if actionable_require_decision_matrix:
    strict_decision_matrix = bool(
        decision_matrix_status in {"ready", "partial"}
        and decision_matrix_health_status in allowed_matrix_statuses
        and decision_matrix_score >= actionable_min_matrix_score
        and (
            not actionable_require_matrix_supports
            or decision_matrix_supports_consistency
        )
    )
    bootstrap_decision_matrix_raw = bool(
        actionable_allow_matrix_bootstrap
        and decision_matrix_status in {"ready", "partial"}
        and decision_matrix_supports_bootstrap
    )
    bootstrap_decision_matrix = bool(bootstrap_decision_matrix_raw)
    now_epoch = end_epoch if end_epoch > 0 else int(captured_at.timestamp())
    if bootstrap_decision_matrix_raw:
        bootstrap_guard_status = "active"
        state_payload: dict[str, object] = {}
        if isinstance(bootstrap_state_path, Path) and bootstrap_state_path.exists():
            state_payload = _load_json(bootstrap_state_path)
        bootstrap_guard_first_seen_epoch = _to_int(str(state_payload.get("first_seen_epoch") or 0), 0)
        bootstrap_guard_last_seen_epoch = _to_int(str(state_payload.get("last_seen_epoch") or 0), 0)
        if bootstrap_guard_first_seen_epoch <= 0:
            bootstrap_guard_first_seen_epoch = int(now_epoch)
        if bootstrap_guard_last_seen_epoch <= 0:
            bootstrap_guard_last_seen_epoch = int(now_epoch)
        bootstrap_guard_elapsed_hours = max(
            0.0,
            float(now_epoch - bootstrap_guard_first_seen_epoch) / 3600.0,
        )

        safe_bootstrap_disable_settled = max(0, actionable_matrix_bootstrap_disable_at_settled_outcomes)
        if (
            safe_bootstrap_disable_settled > 0
            and decision_matrix_settled_outcomes >= safe_bootstrap_disable_settled
        ):
            bootstrap_guard_reasons.append("settled_outcomes_reached_disable_threshold")

        safe_bootstrap_max_hours = max(0.0, float(actionable_matrix_bootstrap_max_hours))
        if (
            safe_bootstrap_max_hours > 0.0
            and isinstance(bootstrap_guard_elapsed_hours, float)
            and bootstrap_guard_elapsed_hours >= safe_bootstrap_max_hours
        ):
            bootstrap_guard_reasons.append("bootstrap_window_expired")

        if bootstrap_guard_reasons:
            bootstrap_decision_matrix = False
            bootstrap_guard_status = "blocked"
        if isinstance(bootstrap_state_path, Path):
            state_update = {
                "status": "blocked" if bootstrap_guard_reasons else "active",
                "first_seen_epoch": int(bootstrap_guard_first_seen_epoch),
                "last_seen_epoch": int(now_epoch),
                "first_seen_at": datetime.fromtimestamp(
                    bootstrap_guard_first_seen_epoch, tz=timezone.utc
                ).isoformat(),
                "last_seen_at": datetime.fromtimestamp(now_epoch, tz=timezone.utc).isoformat(),
                "run_id": run_id,
                "decision_matrix_settled_outcomes": int(decision_matrix_settled_outcomes),
                "bootstrap_guard_elapsed_hours": (
                    round(float(bootstrap_guard_elapsed_hours), 6)
                    if isinstance(bootstrap_guard_elapsed_hours, float)
                    else None
                ),
                "bootstrap_guard_reasons": list(bootstrap_guard_reasons),
            }
            if not _write_json(bootstrap_state_path, state_update):
                bootstrap_guard_warnings.append("failed_to_persist_bootstrap_state")
    elif strict_decision_matrix:
        bootstrap_guard_status = "strict_signal_ready"
        if isinstance(bootstrap_state_path, Path) and bootstrap_state_path.exists():
            if not _safe_unlink(bootstrap_state_path):
                bootstrap_guard_warnings.append("failed_to_reset_bootstrap_state")
    else:
        bootstrap_guard_status = "inactive"
    safe_bootstrap_max_hours = max(0.0, float(actionable_matrix_bootstrap_max_hours))
    if (
        isinstance(bootstrap_guard_elapsed_hours, float)
        and safe_bootstrap_max_hours > 0.0
    ):
        bootstrap_guard_hours_to_expiry = max(
            0.0,
            float(safe_bootstrap_max_hours - bootstrap_guard_elapsed_hours),
        )
    check_decision_matrix = bool(strict_decision_matrix or bootstrap_decision_matrix)

decision_matrix_lane_status = "not_required"
decision_matrix_lane_summary = "Decision matrix lane not required."
if actionable_require_decision_matrix:
    if strict_decision_matrix:
        decision_matrix_lane_status = "strict"
        decision_matrix_lane_summary = "Decision matrix lane: strict pass (bootstrap not used)."
    elif bootstrap_decision_matrix:
        decision_matrix_lane_status = "bootstrap"
        if isinstance(bootstrap_guard_hours_to_expiry, float):
            decision_matrix_lane_summary = (
                "Decision matrix lane: bootstrap pass "
                f"({bootstrap_guard_elapsed_hours:.1f}h elapsed, "
                f"~{bootstrap_guard_hours_to_expiry:.1f}h to expiry)."
            )
        elif isinstance(bootstrap_guard_elapsed_hours, float):
            decision_matrix_lane_summary = (
                "Decision matrix lane: bootstrap pass "
                f"({bootstrap_guard_elapsed_hours:.1f}h elapsed)."
            )
        else:
            decision_matrix_lane_summary = "Decision matrix lane: bootstrap pass."
    elif bootstrap_decision_matrix_raw and not bootstrap_decision_matrix:
        decision_matrix_lane_status = "bootstrap_blocked"
        blocked_reason = (
            ", ".join(bootstrap_guard_reasons[:2])
            if bootstrap_guard_reasons
            else (bootstrap_guard_status or "guard blocked")
        )
        decision_matrix_lane_summary = f"Decision matrix lane: bootstrap blocked ({blocked_reason})."
    else:
        decision_matrix_lane_status = "matrix_failed"
        decision_matrix_lane_summary = "Decision matrix lane: failed (strict and bootstrap both off)."

supports_targeted_trading = bool(
    check_snapshot and check_ingest and check_replication and check_decision_matrix
)
failed_checks: list[str] = []
if not check_snapshot:
    failed_checks.append("snapshot_signal_too_weak")
if actionable_require_ingest and not check_ingest:
    failed_checks.append("ingest_alignment_signal_too_weak")
if actionable_require_replication and not check_replication:
    failed_checks.append("replication_candidates_too_weak")
if actionable_require_decision_matrix and not check_decision_matrix:
    failed_checks.append("decision_matrix_signal_too_weak")

payload["targeted_trading_support"] = {
    "supports_targeted_trading": supports_targeted_trading,
    "failed_checks": failed_checks,
    "decision_matrix_lane": {
        "status": decision_matrix_lane_status,
        "summary_line": decision_matrix_lane_summary,
        "decision_matrix_signal": check_decision_matrix if actionable_require_decision_matrix else None,
        "decision_matrix_strict_signal": strict_decision_matrix if actionable_require_decision_matrix else None,
        "decision_matrix_bootstrap_signal_raw": (
            bootstrap_decision_matrix_raw if actionable_require_decision_matrix else None
        ),
        "decision_matrix_bootstrap_signal": bootstrap_decision_matrix if actionable_require_decision_matrix else None,
        "decision_matrix_bootstrap_guard_status": bootstrap_guard_status if actionable_require_decision_matrix else None,
        "decision_matrix_bootstrap_guard_reasons": bootstrap_guard_reasons if actionable_require_decision_matrix else [],
        "decision_matrix_bootstrap_guard_elapsed_hours": (
            round(float(bootstrap_guard_elapsed_hours), 6)
            if actionable_require_decision_matrix and isinstance(bootstrap_guard_elapsed_hours, float)
            else None
        ),
        "decision_matrix_bootstrap_guard_hours_to_expiry": (
            round(float(bootstrap_guard_hours_to_expiry), 6)
            if actionable_require_decision_matrix and isinstance(bootstrap_guard_hours_to_expiry, float)
            else None
        ),
    },
    "checks": {
        "snapshot_signal": check_snapshot,
        "ingest_alignment_signal": check_ingest if actionable_require_ingest else None,
        "replication_signal": check_replication if actionable_require_replication else None,
        "decision_matrix_signal": check_decision_matrix if actionable_require_decision_matrix else None,
        "decision_matrix_strict_signal": strict_decision_matrix if actionable_require_decision_matrix else None,
        "decision_matrix_bootstrap_signal_raw": (
            bootstrap_decision_matrix_raw if actionable_require_decision_matrix else None
        ),
        "decision_matrix_bootstrap_signal": bootstrap_decision_matrix if actionable_require_decision_matrix else None,
    },
    "thresholds": {
        "min_positions_rows": actionable_min_positions_rows,
        "min_candidates": actionable_min_candidates,
        "min_matched_ratio": actionable_min_matched_ratio,
        "require_ingest": actionable_require_ingest,
        "require_replication": actionable_require_replication,
        "require_decision_matrix": actionable_require_decision_matrix,
        "min_matrix_score": actionable_min_matrix_score,
        "allowed_matrix_statuses": sorted(allowed_matrix_statuses),
        "require_matrix_supports_consistency": actionable_require_matrix_supports,
        "allow_matrix_bootstrap": actionable_allow_matrix_bootstrap,
        "matrix_bootstrap_max_hours": round(float(max(0.0, actionable_matrix_bootstrap_max_hours)), 6),
        "matrix_bootstrap_disable_at_settled_outcomes": max(0, actionable_matrix_bootstrap_disable_at_settled_outcomes),
        "matrix_bootstrap_state_file": str(bootstrap_state_path) if isinstance(bootstrap_state_path, Path) else "",
    },
    "observed": {
        "snapshot_status": snapshot_status,
        "snapshot_positions_rows": snapshot_positions_rows,
        "snapshot_is_stale": snapshot_is_stale,
        "ingest_status": ingest_status,
        "alignment_status": alignment_status,
        "alignment_matched_ratio": round(float(alignment_matched_ratio), 6),
        "alignment_matched_positions": alignment_matched_positions,
        "replication_status": replication_status,
        "replication_candidate_count": replication_candidate_count,
        "decision_matrix_status": decision_matrix_status,
        "decision_matrix_health_status": decision_matrix_health_status,
        "decision_matrix_score": decision_matrix_score,
        "decision_matrix_supports_consistency_and_profitability": decision_matrix_supports_consistency,
        "decision_matrix_supports_bootstrap_progression": decision_matrix_supports_bootstrap,
        "decision_matrix_critical_blockers_count": critical_blockers_count,
        "decision_matrix_settled_outcomes": decision_matrix_settled_outcomes,
        "decision_matrix_bootstrap_guard_status": bootstrap_guard_status,
        "decision_matrix_bootstrap_guard_reasons": bootstrap_guard_reasons,
        "decision_matrix_bootstrap_guard_warnings": bootstrap_guard_warnings,
        "decision_matrix_bootstrap_guard_elapsed_hours": (
            round(float(bootstrap_guard_elapsed_hours), 6)
            if isinstance(bootstrap_guard_elapsed_hours, float)
            else None
        ),
        "decision_matrix_bootstrap_first_seen_epoch": (
            int(bootstrap_guard_first_seen_epoch) if bootstrap_guard_first_seen_epoch > 0 else 0
        ),
        "decision_matrix_bootstrap_last_seen_epoch": (
            int(bootstrap_guard_last_seen_epoch) if bootstrap_guard_last_seen_epoch > 0 else 0
        ),
    },
}

health_dir.mkdir(parents=True, exist_ok=True)
stamp = captured_at.strftime("%Y%m%d_%H%M%S")
event_path = health_dir / f"coldmath_hardening_{stamp}.json"
latest_path = health_dir / "coldmath_hardening_latest.json"
encoded = json.dumps(payload, indent=2, sort_keys=True)
event_path.write_text(encoded, encoding="utf-8")
latest_path.write_text(encoded, encoding="utf-8")
print(f"{event_path}|{1 if supports_targeted_trading else 0}")
PY
)"

event_path="${health_emit_result%%|*}"
supports_targeted_trading="0"
if [[ "$health_emit_result" == *"|"* ]]; then
  supports_targeted_trading="${health_emit_result##*|}"
fi

lane_alert_result=""
if [[ "$COLDMATH_LANE_ALERT_ENABLED" == "1" && -n "$event_path" ]]; then
  lane_alert_result="$("$PYTHON_BIN" - "$event_path" "$COLDMATH_LANE_ALERT_STATE_FILE" "$COLDMATH_LANE_ALERT_NOTIFY_STATUS_CHANGE_ONLY" "$COLDMATH_LANE_ALERT_MESSAGE_MODE" "$COLDMATH_LANE_ALERT_WEBHOOK_USERNAME" "$COLDMATH_LANE_ALERT_TARGET_URL" "$COLDMATH_LANE_ALERT_WEBHOOK_TIMEOUT_SECONDS" "$COLDMATH_LANE_ALERT_DEGRADED_STREAK_THRESHOLD" "$COLDMATH_LANE_ALERT_DEGRADED_STREAK_NOTIFY_EVERY" "$COLDMATH_LANE_ALERT_DEGRADED_STATUSES" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import urllib.error
import urllib.request


def _normalize(value: object) -> str:
    return str(value or "").strip()


def _to_bool(value: object, default: bool = False) -> bool:
    text = _normalize(value).lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "y", "on"}


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _parse_csv_set(value: object, default: set[str]) -> set[str]:
    raw = _normalize(value).lower()
    if not raw:
        return set(default)
    values = {item.strip() for item in raw.split(",") if item and item.strip()}
    return values or set(default)


def _load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_json(path: Path, payload: dict[str, object]) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        return False
    return True


event_path = Path(sys.argv[1])
state_file_text = _normalize(sys.argv[2])
notify_status_change_only = _to_bool(sys.argv[3], True)
message_mode = _normalize(sys.argv[4]).lower() or "concise"
username = _normalize(sys.argv[5]) or "BetBot Matrix Lane"
target_url = _normalize(sys.argv[6])
timeout_seconds = max(1.0, _to_float(sys.argv[7], 5.0))
degraded_streak_threshold = max(0, _to_int(sys.argv[8], 3))
degraded_streak_notify_every = max(0, _to_int(sys.argv[9], 0))
degraded_statuses = _parse_csv_set(sys.argv[10], {"matrix_failed", "bootstrap_blocked"})
effective_streak_notify_every = (
    degraded_streak_notify_every if degraded_streak_notify_every > 0 else max(1, degraded_streak_threshold)
)

if message_mode not in {"concise", "detailed"}:
    message_mode = "concise"

event_payload = _load_json(event_path)
targeted = (
    event_payload.get("targeted_trading_support")
    if isinstance(event_payload.get("targeted_trading_support"), dict)
    else {}
)
lane = (
    targeted.get("decision_matrix_lane")
    if isinstance(targeted.get("decision_matrix_lane"), dict)
    else {}
)
lane_status = _normalize(lane.get("status")).lower()
lane_summary = _normalize(lane.get("summary_line"))

if not lane_status:
    checks = targeted.get("checks") if isinstance(targeted.get("checks"), dict) else {}
    strict_signal = bool(checks.get("decision_matrix_strict_signal") is True)
    bootstrap_signal = bool(checks.get("decision_matrix_bootstrap_signal") is True)
    bootstrap_raw = bool(checks.get("decision_matrix_bootstrap_signal_raw") is True)
    if strict_signal:
        lane_status = "strict"
        lane_summary = "Decision matrix lane: strict pass (bootstrap not used)."
    elif bootstrap_signal:
        lane_status = "bootstrap"
        lane_summary = "Decision matrix lane: bootstrap pass."
    elif bootstrap_raw and not bootstrap_signal:
        lane_status = "bootstrap_blocked"
        lane_summary = "Decision matrix lane: bootstrap blocked."
    elif checks:
        lane_status = "matrix_failed"
        lane_summary = "Decision matrix lane: failed (strict and bootstrap both off)."
    else:
        lane_status = "not_required"
        lane_summary = "Decision matrix lane not required."

supports_targeted_trading = bool(targeted.get("supports_targeted_trading") is True)
run_id = _normalize(event_payload.get("run_id"))
captured_at = _normalize(event_payload.get("captured_at"))

state_path = Path(state_file_text).expanduser() if state_file_text else None
prev_state = _load_json(state_path) if isinstance(state_path, Path) and state_path.exists() else {}
has_prev_state = bool(prev_state and _normalize(prev_state.get("last_lane_status")))
prev_lane_status = _normalize(prev_state.get("last_lane_status")).lower()
prev_support = bool(prev_state.get("last_supports_targeted_trading") is True)
lane_changed = bool(has_prev_state and prev_lane_status != lane_status)
support_changed = bool(has_prev_state and prev_support != supports_targeted_trading)
prev_degraded_streak_count = max(0, _to_int(prev_state.get("degraded_streak_count"), 0))
lane_is_degraded = lane_status in degraded_statuses
prev_lane_is_degraded = prev_lane_status in degraded_statuses
if lane_is_degraded:
    if has_prev_state and prev_lane_is_degraded:
        degraded_streak_count = prev_degraded_streak_count + 1
    else:
        degraded_streak_count = 1
else:
    degraded_streak_count = 0

degraded_streak_triggered = False
if (
    lane_is_degraded
    and degraded_streak_threshold > 0
    and degraded_streak_count >= degraded_streak_threshold
):
    if degraded_streak_count == degraded_streak_threshold:
        degraded_streak_triggered = True
    elif effective_streak_notify_every > 0 and (
        (degraded_streak_count - degraded_streak_threshold) % effective_streak_notify_every == 0
    ):
        degraded_streak_triggered = True

should_notify = False
notify_reasons: list[str] = []
if target_url:
    if notify_status_change_only:
        should_notify = bool(lane_changed or support_changed)
        if should_notify:
            notify_reasons.append("status_change")
    else:
        should_notify = lane_status not in {"", "not_required"}
        if should_notify:
            notify_reasons.append("status")
    if degraded_streak_triggered:
        should_notify = True
        notify_reasons.append("degraded_streak")
notify_reason = "+".join(notify_reasons) if notify_reasons else "none"

status_label = lane_status.upper() if lane_status else "UNKNOWN"
if has_prev_state:
    transition = f"{prev_lane_status.upper() or 'UNKNOWN'} -> {status_label}"
else:
    transition = f"(initial) -> {status_label}"
support_label = "ON" if supports_targeted_trading else "OFF"
prev_support_label = "ON" if prev_support else "OFF"
streak_line = (
    f"Degraded streak: {degraded_streak_count} (threshold {degraded_streak_threshold}, every {effective_streak_notify_every})."
    if lane_is_degraded and degraded_streak_threshold > 0
    else "Degraded streak: n/a."
)

if message_mode == "detailed":
    lines = [
        "Decision Matrix Lane Alert",
        f"Transition: {transition}",
        f"Targeted trading support: {support_label} (previous {prev_support_label})",
        f"Detail: {lane_summary or 'n/a'}",
        streak_line,
        f"Notify reason: {notify_reason}",
        f"Run id: {run_id or 'n/a'}",
        f"Captured at: {captured_at or 'n/a'}",
        f"Source artifact: {event_path.name}",
    ]
else:
    lines = [
        "Decision Matrix Lane Alert",
        f"Transition: {transition}",
        f"Support: {support_label} (previous {prev_support_label})",
        f"Detail: {lane_summary or 'n/a'}",
        streak_line,
    ]
message_text = "\n".join(lines)
if len(message_text) > 1900:
    message_text = message_text[:1897].rstrip() + "..."

notified = False
notify_error = ""
if should_notify and target_url:
    body = json.dumps({"text": message_text, "content": message_text, "username": username}).encode("utf-8")
    request = urllib.request.Request(
        target_url,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            notified = 200 <= int(getattr(response, "status", 0) or 0) < 300
            if not notified:
                notify_error = f"http_status_{int(getattr(response, 'status', 0) or 0)}"
    except urllib.error.HTTPError as exc:  # noqa: PERF203
        notify_error = f"http_error_{exc.code}"
    except Exception as exc:  # noqa: BLE001
        notify_error = _normalize(exc.__class__.__name__) or "notify_exception"

now_iso = datetime.now(timezone.utc).isoformat()
state_payload: dict[str, object] = {
    "last_lane_status": lane_status,
    "last_supports_targeted_trading": supports_targeted_trading,
    "last_seen_run_id": run_id,
    "last_seen_at": now_iso,
    "last_captured_at": captured_at,
    "last_lane_summary": lane_summary,
    "last_notify_target_present": bool(target_url),
    "last_notify_attempted": bool(should_notify and bool(target_url)),
    "last_notified": bool(notified),
    "last_notify_error": notify_error,
    "last_notify_reason": notify_reason,
    "degraded_statuses": sorted(degraded_statuses),
    "degraded_streak_count": degraded_streak_count,
    "degraded_streak_threshold": degraded_streak_threshold,
    "degraded_streak_notify_every": effective_streak_notify_every,
    "last_lane_is_degraded": lane_is_degraded,
    "last_degraded_streak_triggered": degraded_streak_triggered,
}
if notified:
    state_payload["last_notified_at"] = now_iso
    state_payload["last_notified_lane_status"] = lane_status
    state_payload["last_notified_run_id"] = run_id
if degraded_streak_triggered:
    state_payload["last_degraded_streak_triggered_at"] = now_iso
    state_payload["last_degraded_streak_triggered_count"] = degraded_streak_count
if notified and degraded_streak_triggered:
    state_payload["last_degraded_streak_notified_at"] = now_iso
    state_payload["last_degraded_streak_notified_count"] = degraded_streak_count

state_saved = True
if isinstance(state_path, Path):
    state_saved = _save_json(state_path, state_payload)

print(
    "lane_alert "
    f"status={lane_status or 'unknown'} "
    f"prev={prev_lane_status or 'none'} "
    f"lane_changed={1 if lane_changed else 0} "
    f"support_changed={1 if support_changed else 0} "
    f"degraded={1 if lane_is_degraded else 0} "
    f"streak={degraded_streak_count} "
    f"streak_triggered={1 if degraded_streak_triggered else 0} "
    f"notify_reason={notify_reason} "
    f"target={1 if bool(target_url) else 0} "
    f"notified={1 if notified else 0} "
    f"state_saved={1 if state_saved else 0} "
    f"error={notify_error or 'none'}"
)
PY
)"
  if [[ -n "$lane_alert_result" ]]; then
    echo "$lane_alert_result" >> "$LOG_FILE"
  fi
fi

if [[ "$COLDMATH_HARDENING_FAIL_ON_NOISE" == "1" && "$supports_targeted_trading" != "1" && "$OVERALL_STATUS" != "error" ]]; then
  EXIT_CODE=2
fi

echo "coldmath_hardening status=$OVERALL_STATUS actionable_support=$supports_targeted_trading duration_seconds=$RUN_DURATION_SECONDS snapshot=$latest_snapshot_file ingest=$latest_ingest_file replication=$latest_replication_file decision_matrix=$latest_decision_matrix_file event=$event_path" >> "$LOG_FILE"
echo "=== $(timestamp_utc) coldmath hardening end run_id=$RUN_ID status=$OVERALL_STATUS ===" >> "$LOG_FILE"

exit "$EXIT_CODE"
