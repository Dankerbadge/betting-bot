#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-/etc/betbot/temperature-shadow.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -r "$ENV_FILE" ]]; then
  echo "env file is not readable (check owner/group/perms): $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

: "${BETBOT_ROOT:?BETBOT_ROOT is required in env file}"
: "${OUTPUT_DIR:?OUTPUT_DIR is required in env file}"

PYTHON_BIN="$BETBOT_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing python binary: $PYTHON_BIN" >&2
  exit 1
fi

LOOP_SCRIPT="$BETBOT_ROOT/infra/digitalocean/run_temperature_shadow_loop.sh"
if [[ ! -x "$LOOP_SCRIPT" ]]; then
  echo "missing loop script: $LOOP_SCRIPT" >&2
  exit 1
fi

DRILL_TIMEOUT_SECONDS="${DRILL_TIMEOUT_SECONDS:-80}"
DRILL_OUTPUT_BASE="${DRILL_OUTPUT_BASE:-/tmp}"
DRILL_OUTPUT_DIR="$DRILL_OUTPUT_BASE/betbot_stale_metrics_drill_$(date +%Y%m%d_%H%M%S)"
DRILL_ENV_FILE="$DRILL_OUTPUT_BASE/temperature-shadow-stale-drill.env"
DRILL_STDOUT_LOG="$DRILL_OUTPUT_BASE/stale_metrics_drill_stdout.log"
STALE_METRICS_DRILL_ALERT_ENABLED="${STALE_METRICS_DRILL_ALERT_ENABLED:-1}"
STALE_METRICS_DRILL_ALERT_WEBHOOK_URL="${STALE_METRICS_DRILL_ALERT_WEBHOOK_URL:-${RECOVERY_CHAOS_WEBHOOK_URL:-${RECOVERY_WEBHOOK_URL:-${ALERT_WEBHOOK_URL:-}}}}"
STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID="${STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID:-${RECOVERY_CHAOS_WEBHOOK_THREAD_ID:-${RECOVERY_WEBHOOK_THREAD_ID:-${ALERT_WEBHOOK_THREAD_ID:-}}}}"
STALE_METRICS_DRILL_ALERT_WEBHOOK_TIMEOUT_SECONDS="${STALE_METRICS_DRILL_ALERT_WEBHOOK_TIMEOUT_SECONDS:-5}"
STALE_METRICS_DRILL_ALERT_NOTIFY_ON_PASS="${STALE_METRICS_DRILL_ALERT_NOTIFY_ON_PASS:-0}"
STALE_METRICS_DRILL_ALERT_NOTIFY_STATUS_CHANGE_ONLY="${STALE_METRICS_DRILL_ALERT_NOTIFY_STATUS_CHANGE_ONLY:-1}"
STALE_METRICS_DRILL_ALERT_MESSAGE_MODE="${STALE_METRICS_DRILL_ALERT_MESSAGE_MODE:-concise}"
STALE_METRICS_DRILL_ALERT_WEBHOOK_USERNAME="${STALE_METRICS_DRILL_ALERT_WEBHOOK_USERNAME:-BetBot Stale Drill}"
STALE_METRICS_DRILL_ALERT_STATE_FILE="${STALE_METRICS_DRILL_ALERT_STATE_FILE:-$OUTPUT_DIR/recovery_chaos/stale_metrics_drill/.alert_state.json}"
STALE_METRICS_DRILL_FAIL_ON_DRILL_FAILURE="${STALE_METRICS_DRILL_FAIL_ON_DRILL_FAILURE:-1}"

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

STALE_METRICS_DRILL_ALERT_WEBHOOK_TARGET_URL="$(build_discord_target_url "$STALE_METRICS_DRILL_ALERT_WEBHOOK_URL" "$STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID")"

mkdir -p "$DRILL_OUTPUT_DIR"
cp "$ENV_FILE" "$DRILL_ENV_FILE"

"$PYTHON_BIN" - "$DRILL_ENV_FILE" "$DRILL_OUTPUT_DIR" <<'PY'
from __future__ import annotations

from pathlib import Path
import re
import sys

env_path = Path(sys.argv[1])
drill_output_dir = sys.argv[2]
text = env_path.read_text(encoding="utf-8")

replacements = {
    "OUTPUT_DIR": drill_output_dir,
    "ADAPTIVE_BLOCKER_METRICS_MAX_AGE_SECONDS": "1",
    "ADAPTIVE_SETTLEMENT_METRICS_MAX_AGE_SECONDS": "1",
    "ADAPTIVE_REPLAN_METRICS_MAX_AGE_SECONDS": "1",
    "LOOP_SLEEP_SECONDS": "5",
    "MAX_MARKETS": "400",
    "SETTLEMENT_TOP_N": "40",
}

for key, value in replacements.items():
    pattern = rf"^{re.escape(key)}=.*$"
    if re.search(pattern, text, flags=re.M):
        text = re.sub(pattern, f"{key}={value}", text, flags=re.M)
    else:
        text += f"\n{key}={value}\n"

env_path.write_text(text, encoding="utf-8")
PY

set +e
timeout "${DRILL_TIMEOUT_SECONDS}s" "$LOOP_SCRIPT" "$DRILL_ENV_FILE" >"$DRILL_STDOUT_LOG" 2>&1
DRILL_EXIT_CODE="$?"
set -e

SHADOW_LOG="$DRILL_OUTPUT_DIR/logs/shadow_loop.log"
if [[ ! -f "$SHADOW_LOG" ]]; then
  echo "drill failed: shadow loop log missing ($SHADOW_LOG)" >&2
  exit 1
fi

ARTIFACT_DIR="$OUTPUT_DIR/recovery_chaos/stale_metrics_drill"
mkdir -p "$ARTIFACT_DIR"
ARTIFACT_PATH="$ARTIFACT_DIR/stale_metrics_drill_$(date -u +%Y%m%d_%H%M%S).json"
LATEST_PATH="$ARTIFACT_DIR/stale_metrics_drill_latest.json"

"$PYTHON_BIN" - "$SHADOW_LOG" "$DRILL_STDOUT_LOG" "$DRILL_OUTPUT_DIR" "$DRILL_ENV_FILE" "$ARTIFACT_PATH" "$LATEST_PATH" "$DRILL_EXIT_CODE" "$DRILL_TIMEOUT_SECONDS" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys

shadow_log = Path(sys.argv[1])
stdout_log = Path(sys.argv[2])
drill_output_dir = Path(sys.argv[3])
drill_env_file = Path(sys.argv[4])
artifact_path = Path(sys.argv[5])
latest_path = Path(sys.argv[6])
drill_exit_code = int(float(sys.argv[7]))
drill_timeout_seconds = int(float(sys.argv[8]))

shadow_text = shadow_log.read_text(encoding="utf-8", errors="replace")
stdout_text = stdout_log.read_text(encoding="utf-8", errors="replace")
cycle_lines = [line.strip() for line in stdout_text.splitlines() if "cycle end (" in line]
replan_lines = [line.strip() for line in shadow_text.splitlines() if "adaptive_replan_backstop" in line or "adaptive_replan_cooldown" in line]

blocker_stale_cycles = [line for line in cycle_lines if "blocker_metrics_fresh=0" in line]
settlement_stale_cycles = [line for line in cycle_lines if "settlement_metrics_fresh=0" in line]
replan_stale_cycles = [line for line in cycle_lines if "replan_metrics_fresh=0" in line]
replan_metrics_stale_reason_hits = [line for line in replan_lines if "reason=metrics_stale" in line]

disallowed_max_markets_hits = [
    line
    for line in cycle_lines
    if "blocker_metrics_fresh=0" in line
    and (
        "adaptive_reason=healthy_approvals_low_stale" in line
        or "adaptive_reason=stale_dominates_and_approvals_low" in line
        or "adaptive_reason=low_intent_throughput_with_headroom" in line
    )
]

pass_checks = {
    "saw_cycle_end": len(cycle_lines) > 0,
    "saw_blocker_metrics_stale_cycle": len(blocker_stale_cycles) > 0,
    "no_stale_data_driven_max_markets_reasons": len(disallowed_max_markets_hits) == 0,
    "saw_replan_metrics_stale_or_gate_not_needed": len(replan_stale_cycles) == 0 or len(replan_metrics_stale_reason_hits) > 0,
}
passed = all(pass_checks.values())

payload = {
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "status": "pass" if passed else "fail",
    "drill": {
        "timeout_seconds": drill_timeout_seconds,
        "exit_code": drill_exit_code,
        "expected_timeout_exit_code": 124,
        "output_dir": str(drill_output_dir),
        "env_file": str(drill_env_file),
        "shadow_log": str(shadow_log),
        "stdout_log": str(stdout_log),
    },
    "metrics": {
        "cycle_count": len(cycle_lines),
        "blocker_metrics_stale_cycle_count": len(blocker_stale_cycles),
        "settlement_metrics_stale_cycle_count": len(settlement_stale_cycles),
        "replan_metrics_stale_cycle_count": len(replan_stale_cycles),
        "replan_metrics_stale_reason_hits": len(replan_metrics_stale_reason_hits),
        "disallowed_max_markets_reason_hits": len(disallowed_max_markets_hits),
    },
    "checks": pass_checks,
    "samples": {
        "cycle_end_last": cycle_lines[-1] if cycle_lines else "",
        "cycle_end_blocker_stale_first": blocker_stale_cycles[0] if blocker_stale_cycles else "",
        "replan_metrics_stale_reason_first": replan_metrics_stale_reason_hits[0] if replan_metrics_stale_reason_hits else "",
        "disallowed_max_markets_reason_first": disallowed_max_markets_hits[0] if disallowed_max_markets_hits else "",
    },
}

artifact_path.parent.mkdir(parents=True, exist_ok=True)
artifact_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(str(artifact_path))
print(json.dumps({"status": payload["status"], "checks": pass_checks}, indent=2))
PY

echo "stale_metrics_drill complete -> $ARTIFACT_PATH"

if [[ "$STALE_METRICS_DRILL_ALERT_ENABLED" == "1" && -n "$STALE_METRICS_DRILL_ALERT_WEBHOOK_TARGET_URL" ]]; then
  webhook_payload="$("$PYTHON_BIN" - "$LATEST_PATH" "$STALE_METRICS_DRILL_ALERT_STATE_FILE" "$STALE_METRICS_DRILL_ALERT_NOTIFY_ON_PASS" "$STALE_METRICS_DRILL_ALERT_NOTIFY_STATUS_CHANGE_ONLY" "$STALE_METRICS_DRILL_ALERT_MESSAGE_MODE" "$STALE_METRICS_DRILL_ALERT_WEBHOOK_USERNAME" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys


def parse_bool(value: str, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return default


def parse_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def clip_word(value: str, limit: int = 180) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    clipped = text[:limit].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0].rstrip(" ,;:-")
    return clipped


artifact_path = Path(sys.argv[1])
state_path = Path(sys.argv[2])
notify_on_pass = parse_bool(sys.argv[3], False)
status_change_only = parse_bool(sys.argv[4], True)
message_mode = str(sys.argv[5] or "").strip().lower() or "concise"
username = str(sys.argv[6] or "").strip() or "BetBot Stale Drill"

try:
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(0)
if not isinstance(payload, dict):
    raise SystemExit(0)

status = str(payload.get("status") or "unknown").strip().lower()
checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
samples = payload.get("samples") if isinstance(payload.get("samples"), dict) else {}
drill = payload.get("drill") if isinstance(payload.get("drill"), dict) else {}

failed_checks = [name for name, value in checks.items() if value is False]
fingerprint = f"{status}|{'/'.join(sorted(failed_checks))}"

state = {}
if state_path.exists():
    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        loaded = {}
    if isinstance(loaded, dict):
        state = loaded

last_status = str(state.get("last_status") or "").strip().lower()
last_fingerprint = str(state.get("last_fingerprint") or "").strip()
same_incident = (status == last_status and fingerprint == last_fingerprint)

should_alert = status == "fail" or (status == "pass" and notify_on_pass)
if should_alert and status_change_only and same_incident:
    should_alert = False

state_update = {
    "last_checked_at": datetime.now(timezone.utc).isoformat(),
    "last_status": status,
    "last_fingerprint": fingerprint,
    "last_artifact_path": str(artifact_path),
}

if should_alert:
    state_update["last_alert_at"] = datetime.now(timezone.utc).isoformat()

state_path.parent.mkdir(parents=True, exist_ok=True)
state_path.write_text(json.dumps(state_update, indent=2), encoding="utf-8")

if not should_alert:
    raise SystemExit(0)

title = "Stale Data Drill Alert" if status == "fail" else "Stale Data Drill Check"
cycle_count = parse_int(metrics.get("cycle_count"), 0)
blocker_stale_count = parse_int(metrics.get("blocker_metrics_stale_cycle_count"), 0)
disallowed_hits = parse_int(metrics.get("disallowed_max_markets_reason_hits"), 0)
timeout_seconds = parse_int(drill.get("timeout_seconds"), 0)
exit_code = parse_int(drill.get("exit_code"), -1)

check_name_map = {
    "blocker_metrics_stale_exercised": "blocker freshness fallback",
    "settlement_metrics_stale_exercised": "settlement freshness fallback",
    "replan_metrics_stale_exercised": "replan freshness fallback",
    "replan_metrics_stale_reason_observed": "replan metrics-stale reason",
    "disallowed_max_markets_reason_observed": "disallowed scan-budget reason",
}


def humanize_check(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    return check_name_map.get(text, text.replace("_", " "))

lines = [title, f"Status: {status.upper()}"]
if message_mode == "concise":
    lines.append(
        f"What happened: cycles {cycle_count} | stale-cycle hits {blocker_stale_count} | disallowed-scan hits {disallowed_hits}"
    )
else:
    lines.append(
        f"What happened: cycle coverage {cycle_count} | blocker-stale cycles {blocker_stale_count} | disallowed max-markets hits {disallowed_hits}"
    )
lines.append(f"Run details: timeout={timeout_seconds}s | exit={exit_code}")

if failed_checks:
    humanized = [humanize_check(name) for name in failed_checks if humanize_check(name)]
    lines.append("Failed checks: " + ", ".join(humanized or failed_checks))
if status == "fail":
    sample_line = str(samples.get("disallowed_max_markets_reason_first") or samples.get("cycle_end_blocker_stale_first") or "").strip()
    if sample_line:
        if message_mode == "concise":
            lines.append("Example: " + clip_word(sample_line, 180))
        else:
            lines.append("Example:")
            lines.append(clip_word(sample_line, 1200))
    lines.append("Next step: run check_temperature_shadow.sh --strict and review stale-drill artifacts.")
else:
    if message_mode == "concise":
        lines.append("Next step: none (drill checks passed).")

message = "\n".join(lines)
payload["discord_message_preview"] = message
payload["discord_message_mode"] = message_mode
payload["discord_message_generated_at"] = datetime.now(timezone.utc).isoformat()
try:
    artifact_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
except Exception:
    pass
print(json.dumps({"text": message, "content": message, "username": username}))
PY
)"
  if [[ -n "$webhook_payload" ]]; then
    curl --silent --show-error --fail --max-time "$STALE_METRICS_DRILL_ALERT_WEBHOOK_TIMEOUT_SECONDS" \
      --header "Content-Type: application/json" \
      --user-agent "betbot-stale-metrics-drill/1.0" \
      --data-binary "$webhook_payload" \
      "$STALE_METRICS_DRILL_ALERT_WEBHOOK_TARGET_URL" >/dev/null 2>&1 || true
  fi
fi

drill_status="$("$PYTHON_BIN" - "$LATEST_PATH" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("unknown")
    raise SystemExit(0)
if not isinstance(payload, dict):
    print("unknown")
    raise SystemExit(0)
print(str(payload.get("status") or "unknown").strip().lower() or "unknown")
PY
)"

echo "stale_metrics_drill status=$drill_status"
if [[ "$drill_status" != "pass" && "$STALE_METRICS_DRILL_FAIL_ON_DRILL_FAILURE" == "1" ]]; then
  echo "stale_metrics_drill failed (status=$drill_status)" >&2
  exit 2
fi
