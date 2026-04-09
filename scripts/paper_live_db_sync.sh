#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUTS_DIR="${PAPER_LIVE_OUTPUTS_DIR:-$ROOT_DIR/outputs}"
LATEST_OUTPUT_JSON="${PAPER_LIVE_LATEST_OUTPUT_JSON:-$OUTPUTS_DIR/overnight_alpha_latest.json}"
SECRETS_FILE="${PAPER_LIVE_SUPABASE_ENV_FILE:-$HOME/.codex/secrets/betting-bot-supabase.env}"
INGEST_SCRIPT="${PAPER_LIVE_INGEST_SCRIPT:-$ROOT_DIR/separate_projects/supabase-bot-state/scripts/ingest_outputs_to_supabase.py}"
RUN_HOURLY="${PAPER_LIVE_RUN_HOURLY:-0}"
MAX_ATTEMPTS="${PAPER_LIVE_INGEST_MAX_ATTEMPTS:-12}"
BACKOFF_BASE_SECONDS="${PAPER_LIVE_INGEST_BACKOFF_BASE_SECONDS:-5}"
BACKOFF_MAX_SECONDS="${PAPER_LIVE_INGEST_BACKOFF_MAX_SECONDS:-120}"
INNER_NETWORK_RETRIES="${PAPER_LIVE_INNER_NETWORK_RETRIES:-8}"
INNER_RETRY_BACKOFF_SECONDS="${PAPER_LIVE_INNER_RETRY_BACKOFF_SECONDS:-2}"
INNER_MAX_RETRY_BACKOFF_SECONDS="${PAPER_LIVE_INNER_MAX_RETRY_BACKOFF_SECONDS:-30}"
LOCK_DIR="${PAPER_LIVE_SYNC_LOCK_DIR:-/tmp/paper_live_chain_db_sync.lock}"
LOCK_PID_FILE="$LOCK_DIR/pid"
LOCK_TS_FILE="$LOCK_DIR/started_at_utc"
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${PAPER_LIVE_SYNC_RUN_DIR:-/tmp/paper_live_chain_db_sync_${RUN_TS}}"
SUMMARY_JSON="${PAPER_LIVE_SYNC_SUMMARY_PATH:-$RUN_DIR/summary.json}"
LATEST_SUMMARY_JSON="${PAPER_LIVE_SYNC_LATEST_SUMMARY_PATH:-/tmp/paper_live_chain_db_sync_latest.json}"
SUPABASE_PREFLIGHT_JSON="$RUN_DIR/supabase_preflight.json"
SUPABASE_PREFLIGHT_STDOUT="$RUN_DIR/supabase_preflight.stdout.log"
SUPABASE_PREFLIGHT_STDERR="$RUN_DIR/supabase_preflight.stderr.log"
PREFLIGHT_MAX_ATTEMPTS="${PAPER_LIVE_PREFLIGHT_MAX_ATTEMPTS:-3}"
PREFLIGHT_BACKOFF_SECONDS="${PAPER_LIVE_PREFLIGHT_BACKOFF_SECONDS:-3}"
PREFLIGHT_DNS_SOFT_FAIL="${PAPER_LIVE_PREFLIGHT_DNS_SOFT_FAIL:-1}"
REQUIRED_TOOLS=("python3" "jq")
REQUIRED_ENV_VARS=("OPSBOT_SUPABASE_URL" "OPSBOT_SUPABASE_SERVICE_ROLE_KEY" "OPSBOT_SUPABASE_PROJECT_REF")

mkdir -p "$RUN_DIR"

for tool_name in "${REQUIRED_TOOLS[@]}"; do
  if ! command -v "$tool_name" >/dev/null 2>&1; then
    python3 - <<PY
import json
from pathlib import Path
summary = {
  "run_ts_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "run_dir": "$RUN_DIR",
  "status": "failed",
  "reason": "missing_tool",
  "missing_tool": "$tool_name",
}
Path("$SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
Path("$LATEST_SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(summary, indent=2, sort_keys=True))
PY
    exit 1
  fi
done

try_acquire_lock() {
  mkdir "$LOCK_DIR" 2>/dev/null || return 1
  printf '%s\n' "$$" > "$LOCK_PID_FILE"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$LOCK_TS_FILE"
  return 0
}

evict_stale_lock_if_safe() {
  if [[ ! -d "$LOCK_DIR" ]]; then
    return 0
  fi
  local holder_pid=""
  if [[ -f "$LOCK_PID_FILE" ]]; then
    holder_pid="$(tr -cd '0-9' < "$LOCK_PID_FILE" || true)"
  fi
  if [[ -n "$holder_pid" ]] && kill -0 "$holder_pid" 2>/dev/null; then
    return 1
  fi
  rm -f "$LOCK_PID_FILE" "$LOCK_TS_FILE"
  rmdir "$LOCK_DIR" 2>/dev/null || true
  return 0
}

lock_acquired="false"
if try_acquire_lock; then
  lock_acquired="true"
else
  evict_stale_lock_if_safe || true
  if try_acquire_lock; then
    lock_acquired="true"
  fi
fi

if [[ "$lock_acquired" != "true" ]]; then
  python3 - <<PY
import json
from pathlib import Path
summary = {
  "run_ts_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "run_dir": "$RUN_DIR",
  "status": "skipped",
  "reason": "lock_held",
}
Path("$SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
Path("$LATEST_SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(summary, indent=2, sort_keys=True))
PY
  exit 0
fi

cleanup() {
  rm -f "$LOCK_PID_FILE" "$LOCK_TS_FILE" 2>/dev/null || true
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

if [[ ! -f "$SECRETS_FILE" ]]; then
  echo "missing secrets file: $SECRETS_FILE" > "$RUN_DIR/first_actionable_blocker.txt"
  echo "degraded" > "$RUN_DIR/replication_state.txt"
  echo "" > "$RUN_DIR/missing_env_vars.txt"
  echo "1" > "$RUN_DIR/hourly_rc.txt"
  echo "false" > "$RUN_DIR/ingest1_success.txt"
  echo "false" > "$RUN_DIR/ingest2_success.txt"
  echo "{}" > "$RUN_DIR/run2_deltas.json"
  echo "false" > "$RUN_DIR/run2_delta_all_zero.txt"
  python3 - <<PY
import json
from pathlib import Path
run_dir = Path("$RUN_DIR")
preflight_payload = {}
preflight_status = None
preflight_errors = []
try:
    preflight_payload = json.loads(Path("$SUPABASE_PREFLIGHT_JSON").read_text(encoding="utf-8"))
except Exception:
    preflight_payload = {}
if isinstance(preflight_payload, dict):
    preflight_status = preflight_payload.get("status")
    maybe_errors = preflight_payload.get("errors")
    if isinstance(maybe_errors, list):
        preflight_errors = [str(item or "") for item in maybe_errors]
summary = {
  "run_ts_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "run_dir": str(run_dir),
  "hourly_rc": 1,
  "ingest1_rc": None,
  "ingest2_rc": None,
  "ingest1_codes": "",
  "ingest2_codes": "",
  "ingest1_success": False,
  "ingest2_success": False,
  "run2_deltas": {},
  "run2_delta_all_zero": False,
  "replication_state": "degraded",
  "first_actionable_blocker": f"missing secrets file: $SECRETS_FILE",
  "local_snapshot": {},
}
Path("$SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
Path("$LATEST_SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(summary, indent=2, sort_keys=True))
PY
  exit 1
fi

preflight_is_dns_retryable() {
  local preflight_json_path="$1"
  local stderr_path="$2"
  python3 - "$preflight_json_path" "$stderr_path" <<'PY'
import json
import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
stderr_path = Path(sys.argv[2])

if summary_path.exists():
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        normalized = [str(item or "") for item in errors]
        dns_only = all(text.startswith("dns_unresolved_hosts:") for text in normalized)
        print("1" if dns_only else "0")
        raise SystemExit(0)

stderr_text = ""
try:
    stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace").lower()
except OSError:
    pass

markers = (
    "nodename nor servname",
    "name or service not known",
    "temporary failure in name resolution",
    "no address associated with hostname",
    "dns_unresolved_hosts",
)
print("1" if any(marker in stderr_text for marker in markers) else "0")
PY
}

max_preflight_attempts="$PREFLIGHT_MAX_ATTEMPTS"
if ! [[ "$max_preflight_attempts" =~ ^[0-9]+$ ]] || [[ "$max_preflight_attempts" -lt 1 ]]; then
  max_preflight_attempts=1
fi

: > "$SUPABASE_PREFLIGHT_STDOUT"
: > "$SUPABASE_PREFLIGHT_STDERR"
echo "false" > "$RUN_DIR/supabase_preflight_dns_soft_fail.txt"
supabase_preflight_rc=1
supabase_preflight_attempts=0
while [[ "$supabase_preflight_attempts" -lt "$max_preflight_attempts" ]]; do
  supabase_preflight_attempts=$((supabase_preflight_attempts + 1))
  attempt_stdout="$RUN_DIR/supabase_preflight.stdout.attempt${supabase_preflight_attempts}.log"
  attempt_stderr="$RUN_DIR/supabase_preflight.stderr.attempt${supabase_preflight_attempts}.log"

  set +e
  python3 "$ROOT_DIR/scripts/automation_preflight.py" \
    --profile supabase_sync \
    --repo-root "$ROOT_DIR" \
    --secrets-file "$SECRETS_FILE" \
    --output-json "$SUPABASE_PREFLIGHT_JSON" \
    > "$attempt_stdout" \
    2> "$attempt_stderr"
  supabase_preflight_rc=$?
  set -e

  {
    printf '=== attempt %s rc=%s at %s ===\n' "$supabase_preflight_attempts" "$supabase_preflight_rc" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    cat "$attempt_stdout" 2>/dev/null || true
    echo
  } >> "$SUPABASE_PREFLIGHT_STDOUT"
  {
    printf '=== attempt %s rc=%s at %s ===\n' "$supabase_preflight_attempts" "$supabase_preflight_rc" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    cat "$attempt_stderr" 2>/dev/null || true
    echo
  } >> "$SUPABASE_PREFLIGHT_STDERR"

  if [[ "$supabase_preflight_rc" -eq 0 ]]; then
    break
  fi
  if [[ "$supabase_preflight_attempts" -ge "$max_preflight_attempts" ]]; then
    break
  fi
  retryable_dns="$(preflight_is_dns_retryable "$SUPABASE_PREFLIGHT_JSON" "$attempt_stderr")"
  if [[ "$retryable_dns" != "1" ]]; then
    break
  fi
  sleep "$PREFLIGHT_BACKOFF_SECONDS"
done
echo "$supabase_preflight_attempts" > "$RUN_DIR/supabase_preflight_attempts.txt"

if [[ "$supabase_preflight_rc" -ne 0 ]]; then
  retryable_dns="$(preflight_is_dns_retryable "$SUPABASE_PREFLIGHT_JSON" "$SUPABASE_PREFLIGHT_STDERR")"
  soft_fail_enabled="1"
  case "${PREFLIGHT_DNS_SOFT_FAIL:-1}" in
    0|false|FALSE|False|no|NO)
      soft_fail_enabled="0"
      ;;
  esac

  if [[ "$retryable_dns" == "1" && "$soft_fail_enabled" == "1" ]]; then
    echo "true" > "$RUN_DIR/supabase_preflight_dns_soft_fail.txt"
    printf 'warning: Supabase preflight DNS-only failure; continuing with ingest due to PAPER_LIVE_PREFLIGHT_DNS_SOFT_FAIL=%s\n' "${PREFLIGHT_DNS_SOFT_FAIL}" >&2
  else
  preflight_blocker="$(
    python3 - "$SUPABASE_PREFLIGHT_JSON" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print("supabase preflight failed without summary output")
    raise SystemExit(0)
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("supabase preflight failed and summary JSON was unreadable")
    raise SystemExit(0)
errors = payload.get("errors")
if isinstance(errors, list) and errors:
    print(f"supabase preflight blocked: {errors[0]}")
else:
    status = payload.get("status")
    print(f"supabase preflight blocked: status={status}")
PY
  )"
  echo "$preflight_blocker" > "$RUN_DIR/first_actionable_blocker.txt"
  echo "degraded" > "$RUN_DIR/replication_state.txt"
  echo "" > "$RUN_DIR/missing_env_vars.txt"
  echo "-1" > "$RUN_DIR/hourly_rc.txt"
  echo "false" > "$RUN_DIR/ingest1_success.txt"
  echo "false" > "$RUN_DIR/ingest2_success.txt"
  echo "{}" > "$RUN_DIR/run2_deltas.json"
  echo "false" > "$RUN_DIR/run2_delta_all_zero.txt"
  python3 - <<PY
import json
from pathlib import Path
run_dir = Path("$RUN_DIR")
preflight_payload = {}
preflight_status = None
preflight_errors = []
try:
    preflight_payload = json.loads(Path("$SUPABASE_PREFLIGHT_JSON").read_text(encoding="utf-8"))
except Exception:
    preflight_payload = {}
if isinstance(preflight_payload, dict):
    preflight_status = preflight_payload.get("status")
    maybe_errors = preflight_payload.get("errors")
    if isinstance(maybe_errors, list):
        preflight_errors = [str(item or "") for item in maybe_errors]
summary = {
  "run_ts_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "run_dir": str(run_dir),
  "hourly_rc": -1,
  "ingest1_rc": None,
  "ingest2_rc": None,
  "ingest1_codes": "",
  "ingest2_codes": "",
  "ingest1_success": False,
  "ingest2_success": False,
  "run2_deltas": {},
  "run2_delta_all_zero": False,
  "replication_state": "degraded",
  "first_actionable_blocker": "$preflight_blocker",
  "local_snapshot": {},
  "supabase_preflight_json": "$SUPABASE_PREFLIGHT_JSON",
  "supabase_preflight_attempts": $supabase_preflight_attempts,
  "supabase_preflight_status": preflight_status,
  "supabase_preflight_errors": preflight_errors,
  "supabase_preflight_dns_soft_fail": False,
}
Path("$SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
Path("$LATEST_SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(summary, indent=2, sort_keys=True))
PY
    exit 1
  fi
fi

set -a
source "$SECRETS_FILE"
set +a

missing_env_vars=""
present_env_vars=""
for required_var in "${REQUIRED_ENV_VARS[@]}"; do
  value="${!required_var:-}"
  if [[ -z "$value" ]]; then
    if [[ -z "$missing_env_vars" ]]; then
      missing_env_vars="$required_var"
    else
      missing_env_vars="$missing_env_vars,$required_var"
    fi
  else
    if [[ -z "$present_env_vars" ]]; then
      present_env_vars="$required_var"
    else
      present_env_vars="$present_env_vars,$required_var"
    fi
  fi
done
echo "$missing_env_vars" > "$RUN_DIR/missing_env_vars.txt"
echo "$present_env_vars" > "$RUN_DIR/present_env_vars.txt"

extract_first_blocker() {
  local log_path="$1"
  python3 - "$log_path" <<'PY'
import re
import sys

path = sys.argv[1]
patterns = [
    r"socket\.gaierror.*",
    r"urllib\.error\.URLError.*",
    r"Supabase HTTP error.*",
    r"Supabase network error.*",
    r"Missing Supabase credentials.*",
    r"Missing outputs directory.*",
    r"RuntimeError:.*",
    r"HTTP Error [0-9]{3}.*",
]
try:
    lines = open(path, "r", encoding="utf-8", errors="replace").read().splitlines()
except OSError:
    print("")
    raise SystemExit(0)

for line in lines:
    stripped = line.strip()
    if not stripped:
        continue
    for pattern in patterns:
        if re.search(pattern, stripped):
            print(stripped)
            raise SystemExit(0)

for line in lines:
    stripped = line.strip()
    if stripped:
        print(stripped[:300])
        raise SystemExit(0)

print("")
PY
}

hourly_rc=-1
hourly_log="$RUN_DIR/hourly.log"
if [[ "$RUN_HOURLY" == "1" ]]; then
  set +e
  BETBOT_MIN_SECONDS_BETWEEN_RUNS=0 "$ROOT_DIR/scripts/hourly_alpha_overnight.sh" --force > "$hourly_log" 2>&1
  hourly_rc=$?
  set -e
else
  echo '{"status":"skipped","reason":"run_hourly_disabled"}' > "$hourly_log"
fi
echo "$hourly_rc" > "$RUN_DIR/hourly_rc.txt"

run_ingest_with_retry() {
  local label="$1"
  local attempt=1
  local backoff="$BACKOFF_BASE_SECONDS"
  local first_blocker=""
  local success="false"
  local last_rc=1
  : > "$RUN_DIR/${label}_codes.txt"

  while [[ "$attempt" -le "$MAX_ATTEMPTS" ]]; do
    local log_path="$RUN_DIR/${label}_attempt${attempt}.log"
    set +e
    OPSBOT_SUPABASE_NETWORK_RETRIES="$INNER_NETWORK_RETRIES" \
    OPSBOT_SUPABASE_RETRY_BACKOFF_SECONDS="$INNER_RETRY_BACKOFF_SECONDS" \
    OPSBOT_SUPABASE_MAX_RETRY_BACKOFF_SECONDS="$INNER_MAX_RETRY_BACKOFF_SECONDS" \
      python3 "$INGEST_SCRIPT" --outputs-dir "$OUTPUTS_DIR" > "$log_path" 2>&1
    local rc=$?
    set -e
    echo "$rc" >> "$RUN_DIR/${label}_codes.txt"
    last_rc="$rc"
    if [[ "$rc" -eq 0 ]]; then
      success="true"
      break
    fi
    if [[ -z "$first_blocker" ]]; then
      first_blocker="$(extract_first_blocker "$log_path")"
    fi
    if [[ "$attempt" -lt "$MAX_ATTEMPTS" ]]; then
      sleep "$backoff"
      local doubled=$(( backoff * 2 ))
      if [[ "$doubled" -gt "$BACKOFF_MAX_SECONDS" ]]; then
        backoff="$BACKOFF_MAX_SECONDS"
      else
        backoff="$doubled"
      fi
    fi
    attempt=$(( attempt + 1 ))
  done

  echo "$success" > "$RUN_DIR/${label}_success.txt"
  echo "$last_rc" > "$RUN_DIR/${label}_last_rc.txt"
  printf '%s' "$first_blocker" > "$RUN_DIR/${label}_first_blocker.txt"
}

query_table_counts() {
  local out_json="$1"
  python3 - "$out_json" <<'PY'
import json
import os
import urllib.request

base = os.environ["OPSBOT_SUPABASE_URL"].rstrip("/")
key = os.environ["OPSBOT_SUPABASE_SERVICE_ROLE_KEY"]
tables = [
    "execution_journal",
    "execution_frontier_reports",
    "execution_frontier_report_buckets",
    "climate_availability_events",
    "overnight_runs",
    "pilot_scorecards",
]
counts = {}
for table in tables:
    req = urllib.request.Request(
        f"{base}/rest/v1/{table}?select=*&limit=1",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
            "Accept-Profile": "bot_ops",
            "Prefer": "count=exact",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        _ = response.read()
        content_range = response.headers.get("Content-Range", "")
    if "/" not in content_range:
        raise RuntimeError(f"missing Content-Range count for {table}: {content_range!r}")
    counts[table] = int(content_range.split("/")[-1])

with open(os.sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(counts, handle, sort_keys=True)
PY
}

ingest1_success="false"
ingest2_success="false"
ingest1_rc=-1
ingest2_rc=-1
run2_delta_all_zero="false"
echo "{}" > "$RUN_DIR/run2_deltas.json"

if [[ -z "$missing_env_vars" ]]; then
  run_ingest_with_retry "ingest1"
  ingest1_success="$(cat "$RUN_DIR/ingest1_success.txt")"
  ingest1_rc="$(cat "$RUN_DIR/ingest1_last_rc.txt")"

  if [[ "$ingest1_success" == "true" ]]; then
    query_table_counts "$RUN_DIR/counts_before_run2.json"
    run_ingest_with_retry "ingest2"
    ingest2_success="$(cat "$RUN_DIR/ingest2_success.txt")"
    ingest2_rc="$(cat "$RUN_DIR/ingest2_last_rc.txt")"
    if [[ "$ingest2_success" == "true" ]]; then
      query_table_counts "$RUN_DIR/counts_after_run2.json"
      python3 - "$RUN_DIR/counts_before_run2.json" "$RUN_DIR/counts_after_run2.json" "$RUN_DIR/run2_deltas.json" "$RUN_DIR/run2_delta_all_zero.txt" <<'PY'
import json
import sys

before = json.load(open(sys.argv[1], encoding="utf-8"))
after = json.load(open(sys.argv[2], encoding="utf-8"))
keys = sorted(set(before) | set(after))
deltas = {key: int(after.get(key, 0)) - int(before.get(key, 0)) for key in keys}
all_zero = all(value == 0 for value in deltas.values())
json.dump(deltas, open(sys.argv[3], "w", encoding="utf-8"), sort_keys=True)
open(sys.argv[4], "w", encoding="utf-8").write("true" if all_zero else "false")
PY
      run2_delta_all_zero="$(cat "$RUN_DIR/run2_delta_all_zero.txt")"
    fi
  fi
fi

first_actionable_blocker=""
if [[ -n "$missing_env_vars" ]]; then
  first_actionable_blocker="missing required env vars: $missing_env_vars"
elif [[ "$ingest1_success" != "true" ]]; then
  first_actionable_blocker="$(cat "$RUN_DIR/ingest1_first_blocker.txt")"
elif [[ "$ingest2_success" != "true" ]]; then
  first_actionable_blocker="$(cat "$RUN_DIR/ingest2_first_blocker.txt")"
elif [[ "$run2_delta_all_zero" != "true" ]]; then
  first_actionable_blocker="run-2 delta verification found non-zero table deltas"
fi

replication_state="degraded"
if [[ "$ingest1_success" == "true" && "$ingest2_success" == "true" && "$run2_delta_all_zero" == "true" ]]; then
  replication_state="healthy"
fi

echo "$replication_state" > "$RUN_DIR/replication_state.txt"
printf '%s' "$first_actionable_blocker" > "$RUN_DIR/first_actionable_blocker.txt"

if [[ -f "$LATEST_OUTPUT_JSON" ]]; then
  jq '{
    run_id,
    paper_live_status,
    paper_live_order_attempts,
    paper_live_orders_filled,
    paper_live_orders_canceled,
    paper_live_orders_resting,
    paper_live_open_risk_dollars,
    paper_live_open_risk_cap_dollars,
    paper_live_open_risk_remaining_dollars,
    paper_live_execution_state,
    paper_live_family_execution_state,
    paper_live_ticker_execution_state,
    paper_live_markout_10s_dollars,
    paper_live_markout_60s_dollars,
    paper_live_markout_300s_dollars,
    paper_live_family_markout_300s_per_contract,
    paper_live_family_markout_300s_per_risk_pct,
    paper_live_ticker_markout_300s_per_contract,
    paper_live_ticker_markout_300s_per_risk_pct
  }' "$LATEST_OUTPUT_JSON" > "$RUN_DIR/local_snapshot.json"
else
  echo '{}' > "$RUN_DIR/local_snapshot.json"
fi

python3 - <<PY
import json
from pathlib import Path

run_dir = Path("$RUN_DIR")

def _read(path, default=""):
    p = run_dir / path
    if not p.exists():
        return default
    return p.read_text(encoding="utf-8").strip()

preflight_payload = {}
preflight_status = None
preflight_errors: list[str] = []
try:
    preflight_payload = json.loads(Path("$SUPABASE_PREFLIGHT_JSON").read_text(encoding="utf-8"))
except Exception:
    preflight_payload = {}
if isinstance(preflight_payload, dict):
    preflight_status = preflight_payload.get("status")
    maybe_errors = preflight_payload.get("errors")
    if isinstance(maybe_errors, list):
        preflight_errors = [str(item or "") for item in maybe_errors]

summary = {
    "run_ts_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "run_dir": str(run_dir),
    "run_hourly_enabled": "$RUN_HOURLY" == "1",
    "hourly_rc": int(_read("hourly_rc.txt", "-1")),
    "missing_env_vars": _read("missing_env_vars.txt", ""),
    "present_env_vars": _read("present_env_vars.txt", ""),
    "ingest1_rc": int(_read("ingest1_last_rc.txt", "-1")) if (run_dir / "ingest1_last_rc.txt").exists() else None,
    "ingest1_codes": ",".join((run_dir / "ingest1_codes.txt").read_text(encoding="utf-8").split()) if (run_dir / "ingest1_codes.txt").exists() else "",
    "ingest1_success": _read("ingest1_success.txt", "false") == "true",
    "ingest2_rc": int(_read("ingest2_last_rc.txt", "-1")) if (run_dir / "ingest2_last_rc.txt").exists() else None,
    "ingest2_codes": ",".join((run_dir / "ingest2_codes.txt").read_text(encoding="utf-8").split()) if (run_dir / "ingest2_codes.txt").exists() else "",
    "ingest2_success": _read("ingest2_success.txt", "false") == "true",
    "run2_deltas": json.loads(_read("run2_deltas.json", "{}")),
    "run2_delta_all_zero": _read("run2_delta_all_zero.txt", "false") == "true",
    "replication_state": _read("replication_state.txt", "degraded"),
    "first_actionable_blocker": _read("first_actionable_blocker.txt", ""),
    "local_snapshot": json.loads((run_dir / "local_snapshot.json").read_text(encoding="utf-8")),
    "supabase_preflight_json": "$SUPABASE_PREFLIGHT_JSON",
    "supabase_preflight_attempts": int(_read("supabase_preflight_attempts.txt", "0") or "0"),
    "supabase_preflight_status": preflight_status,
    "supabase_preflight_errors": preflight_errors,
    "supabase_preflight_dns_soft_fail": _read("supabase_preflight_dns_soft_fail.txt", "false") == "true",
}

Path("$SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
Path("$LATEST_SUMMARY_JSON").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(summary, indent=2, sort_keys=True))
PY

if [[ "$replication_state" == "healthy" ]]; then
  exit 0
fi
exit 1
