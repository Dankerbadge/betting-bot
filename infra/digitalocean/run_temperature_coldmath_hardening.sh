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
COLDMATH_HARDENING_FAIL_ON_NOISE="${COLDMATH_HARDENING_FAIL_ON_NOISE:-0}"
COLDMATH_ACTIONABLE_MIN_POSITIONS_ROWS="${COLDMATH_ACTIONABLE_MIN_POSITIONS_ROWS:-100}"
COLDMATH_ACTIONABLE_MIN_CANDIDATES="${COLDMATH_ACTIONABLE_MIN_CANDIDATES:-4}"
COLDMATH_ACTIONABLE_MIN_MATCHED_RATIO="${COLDMATH_ACTIONABLE_MIN_MATCHED_RATIO:-0.20}"
COLDMATH_ACTIONABLE_REQUIRE_INGEST="${COLDMATH_ACTIONABLE_REQUIRE_INGEST:-$COLDMATH_MARKET_INGEST_ENABLED}"
COLDMATH_ACTIONABLE_REQUIRE_REPLICATION="${COLDMATH_ACTIONABLE_REQUIRE_REPLICATION:-$COLDMATH_REPLICATION_ENABLED}"

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

latest_snapshot_file="$(ls -1t "$COLDMATH_OUTPUT_DIR"/coldmath_snapshot_summary_*.json 2>/dev/null | head -n 1 || true)"
latest_ingest_file="$(ls -1t "$COLDMATH_OUTPUT_DIR"/polymarket_temperature_markets_summary_*.json 2>/dev/null | head -n 1 || true)"
latest_replication_file="$(ls -1t "$COLDMATH_OUTPUT_DIR"/coldmath_replication_plan_*.json 2>/dev/null | head -n 1 || true)"

if [[ "$OVERALL_STATUS" == "error" ]]; then
  EXIT_CODE=1
else
  EXIT_CODE=0
fi

RUN_END_EPOCH="$(date +%s)"
RUN_DURATION_SECONDS=$(( RUN_END_EPOCH - RUN_START_EPOCH ))
STAGE_ROWS_JOINED="$(printf '%s;;' "${STAGE_ROWS[@]}")"
HEALTH_DIR="$COLDMATH_OUTPUT_DIR/health"

health_emit_result="$("$PYTHON_BIN" - "$HEALTH_DIR" "$RUN_ID" "$OVERALL_STATUS" "$RUN_START_EPOCH" "$RUN_END_EPOCH" "$RUN_DURATION_SECONDS" "$COLDMATH_WALLET_ADDRESS" "$COLDMATH_OUTPUT_DIR" "$COLDMATH_SNAPSHOT_DIR" "$STAGE_ROWS_JOINED" "$latest_snapshot_file" "$latest_ingest_file" "$latest_replication_file" "$COLDMATH_ACTIONABLE_MIN_POSITIONS_ROWS" "$COLDMATH_ACTIONABLE_MIN_CANDIDATES" "$COLDMATH_ACTIONABLE_MIN_MATCHED_RATIO" "$COLDMATH_ACTIONABLE_REQUIRE_INGEST" "$COLDMATH_ACTIONABLE_REQUIRE_REPLICATION" <<'PY'
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
actionable_min_positions_rows = _to_int(sys.argv[14], 0)
actionable_min_candidates = _to_int(sys.argv[15], 0)
actionable_min_matched_ratio = _to_float(sys.argv[16], 0.0)
actionable_require_ingest = _to_bool(sys.argv[17])
actionable_require_replication = _to_bool(sys.argv[18])

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

check_snapshot = bool(
    snapshot_status in {"ready", "partial"} and snapshot_positions_rows >= actionable_min_positions_rows and not snapshot_is_stale
)
check_ingest = True
check_replication = True
if actionable_require_ingest:
    check_ingest = bool(
        ingest_status in {"ready", "partial"}
        and alignment_status in {"ready", "partial", "ready_partial", "empty_positions"}
        and alignment_matched_ratio >= actionable_min_matched_ratio
        and alignment_matched_positions > 0
    )
if actionable_require_replication:
    check_replication = bool(
        replication_status == "ready" and replication_candidate_count >= actionable_min_candidates
    )

supports_targeted_trading = bool(check_snapshot and check_ingest and check_replication)
failed_checks: list[str] = []
if not check_snapshot:
    failed_checks.append("snapshot_signal_too_weak")
if actionable_require_ingest and not check_ingest:
    failed_checks.append("ingest_alignment_signal_too_weak")
if actionable_require_replication and not check_replication:
    failed_checks.append("replication_candidates_too_weak")

payload["targeted_trading_support"] = {
    "supports_targeted_trading": supports_targeted_trading,
    "failed_checks": failed_checks,
    "checks": {
        "snapshot_signal": check_snapshot,
        "ingest_alignment_signal": check_ingest if actionable_require_ingest else None,
        "replication_signal": check_replication if actionable_require_replication else None,
    },
    "thresholds": {
        "min_positions_rows": actionable_min_positions_rows,
        "min_candidates": actionable_min_candidates,
        "min_matched_ratio": actionable_min_matched_ratio,
        "require_ingest": actionable_require_ingest,
        "require_replication": actionable_require_replication,
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

if [[ "$COLDMATH_HARDENING_FAIL_ON_NOISE" == "1" && "$supports_targeted_trading" != "1" && "$OVERALL_STATUS" != "error" ]]; then
  EXIT_CODE=2
fi

echo "coldmath_hardening status=$OVERALL_STATUS actionable_support=$supports_targeted_trading duration_seconds=$RUN_DURATION_SECONDS snapshot=$latest_snapshot_file ingest=$latest_ingest_file replication=$latest_replication_file event=$event_path" >> "$LOG_FILE"
echo "=== $(timestamp_utc) coldmath hardening end run_id=$RUN_ID status=$OVERALL_STATUS ===" >> "$LOG_FILE"

exit "$EXIT_CODE"
