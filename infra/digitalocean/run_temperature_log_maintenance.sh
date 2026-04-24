#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-/etc/betbot/temperature-shadow.env}"
ROTATE_FILE="/etc/logrotate.d/betbot-temperature"

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

LOG_DIR="${BETBOT_LOG_DIR:-$OUTPUT_DIR/logs}"
MAINT_ENABLED="${LOG_MAINTENANCE_ENABLED:-1}"
RUN_LOGROTATE="${LOG_MAINTENANCE_RUN_LOGROTATE:-1}"
AUTO_INSTALL_LOGROTATE="${LOG_MAINTENANCE_AUTO_INSTALL_LOGROTATE:-1}"
MAX_COMPRESS_PER_RUN="${LOG_MAINTENANCE_MAX_COMPRESS_PER_RUN:-2}"
MIN_ROLLOVER_AGE_MINUTES="${LOG_MAINTENANCE_MIN_ROLLOVER_AGE_MINUTES:-10}"
PRUNE_DAYS="${LOG_MAINTENANCE_PRUNE_DAYS:-21}"
GZIP_LEVEL="${LOG_MAINTENANCE_GZIP_LEVEL:-6}"
MAX_BYTES_WARN="${LOG_MAINTENANCE_MAX_BYTES_WARN:-21474836480}"
MAX_BYTES_CRIT="${LOG_MAINTENANCE_MAX_BYTES_CRIT:-42949672960}"
INSTALLER_SCRIPT="${LOG_MAINTENANCE_INSTALLER_SCRIPT:-$BETBOT_ROOT/infra/digitalocean/install_logrotate_temperature_logs.sh}"
LOG_MAINT_ALERT_ENABLED="${LOG_MAINT_ALERT_ENABLED:-1}"
LOG_MAINT_ALERT_WEBHOOK_URL="${LOG_MAINT_ALERT_WEBHOOK_URL:-${ALERT_WEBHOOK_URL:-}}"
LOG_MAINT_ALERT_WEBHOOK_THREAD_ID="${LOG_MAINT_ALERT_WEBHOOK_THREAD_ID:-${ALERT_WEBHOOK_THREAD_ID:-}}"
LOG_MAINT_ALERT_WEBHOOK_TIMEOUT_SECONDS="${LOG_MAINT_ALERT_WEBHOOK_TIMEOUT_SECONDS:-5}"
LOG_MAINT_ALERT_WEBHOOK_USERNAME="${LOG_MAINT_ALERT_WEBHOOK_USERNAME:-BetBot Ops}"
LOG_MAINT_ALERT_NOTIFY_STATUS_CHANGE_ONLY="${LOG_MAINT_ALERT_NOTIFY_STATUS_CHANGE_ONLY:-1}"
LOG_MAINT_ALERT_MESSAGE_MODE="${LOG_MAINT_ALERT_MESSAGE_MODE:-concise}"
LOG_MAINT_ALERT_GROWTH_BYTES_THRESHOLD="${LOG_MAINT_ALERT_GROWTH_BYTES_THRESHOLD:-1073741824}"
LOG_MAINT_ALERT_MIN_INTERVAL_SECONDS="${LOG_MAINT_ALERT_MIN_INTERVAL_SECONDS:-10800}"
LOG_MAINT_ALERT_STATE_FILE="${LOG_MAINT_ALERT_STATE_FILE:-$OUTPUT_DIR/health/log_maintenance/log_maintenance_alert_state.json}"

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

LOG_MAINT_ALERT_WEBHOOK_TARGET_URL="$(build_discord_target_url "$LOG_MAINT_ALERT_WEBHOOK_URL" "$LOG_MAINT_ALERT_WEBHOOK_THREAD_ID")"

mkdir -p "$LOG_DIR" "$OUTPUT_DIR/health/log_maintenance"
LOG_FILE="$LOG_DIR/log_maintenance.log"
LOCK_FILE="$OUTPUT_DIR/.log_maintenance.lock"
LATEST_FILE="$OUTPUT_DIR/health/log_maintenance/log_maintenance_latest.json"
EVENT_FILE="$OUTPUT_DIR/health/log_maintenance/log_maintenance_$(date -u +"%Y%m%d_%H%M%S").json"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] log maintenance skipped: lock busy" >> "$LOG_FILE"
  exit 0
fi

compressed_tmp="$(mktemp)"
pruned_tmp="$(mktemp)"
actions_tmp="$(mktemp)"
largest_tmp="$(mktemp)"
trap 'rm -f "$compressed_tmp" "$pruned_tmp" "$actions_tmp" "$largest_tmp"' EXIT

append_action() {
  local action="${1:-}"
  [[ -n "$action" ]] || return 0
  printf '%s\n' "$action" >> "$actions_tmp"
}

collect_usage_bytes() {
  if command -v du >/dev/null 2>&1; then
    du -sb "$LOG_DIR" 2>/dev/null | awk '{print $1}'
  else
    echo "0"
  fi
}

echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") log maintenance cycle start ===" >> "$LOG_FILE"
cycle_started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

if [[ "$MAINT_ENABLED" != "1" ]]; then
  append_action "maintenance_disabled"
  usage_bytes="$(collect_usage_bytes)"
  export LM_HEALTH_STATUS="disabled"
  export LM_RUN_ENABLED="$MAINT_ENABLED"
  export LM_LOGROTATE_ENABLED="$RUN_LOGROTATE"
  export LM_LOGROTATE_PRESENT="0"
  export LM_AUTO_INSTALL_LOGROTATE="$AUTO_INSTALL_LOGROTATE"
  export LM_MAX_COMPRESS_PER_RUN="$MAX_COMPRESS_PER_RUN"
  export LM_MIN_ROLLOVER_AGE_MINUTES="$MIN_ROLLOVER_AGE_MINUTES"
  export LM_PRUNE_DAYS="$PRUNE_DAYS"
  export LM_GZIP_LEVEL="$GZIP_LEVEL"
  export LM_WARN_BYTES="$MAX_BYTES_WARN"
  export LM_CRIT_BYTES="$MAX_BYTES_CRIT"
  export LM_USAGE_BYTES="$usage_bytes"
  export LM_COMPRESSED_COUNT="0"
  export LM_PRUNED_COUNT="0"
  export LM_CYCLE_EPOCH="$(date +%s)"
  export LM_STARTED_AT="$cycle_started_at"
  export LM_FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  python3 - "$LATEST_FILE" "$EVENT_FILE" "$compressed_tmp" "$pruned_tmp" "$actions_tmp" "$largest_tmp" "$LOG_DIR" "$ROTATE_FILE" <<'PY'
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


def _read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    out: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        item = line.strip()
        if item:
            out.append(item)
    return out


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


latest_path = Path(sys.argv[1])
event_path = Path(sys.argv[2])
compressed_path = Path(sys.argv[3])
pruned_path = Path(sys.argv[4])
actions_path = Path(sys.argv[5])
largest_path = Path(sys.argv[6])
log_dir = Path(sys.argv[7])
rotate_file = Path(sys.argv[8])

largest_rows: list[dict[str, Any]] = []
for row in _read_lines(largest_path):
    parts = row.split("\t", 1)
    if len(parts) != 2:
        continue
    size_raw, name = parts
    largest_rows.append(
        {
            "name": name,
            "size_bytes": _to_int(size_raw),
        }
    )

payload = {
    "status": "ready",
    "captured_at": os.environ.get("LM_FINISHED_AT") or "",
    "health_status": os.environ.get("LM_HEALTH_STATUS") or "unknown",
    "log_dir": str(log_dir),
    "rotate_file": str(rotate_file),
    "policy": {
        "maintenance_enabled": os.environ.get("LM_RUN_ENABLED") == "1",
        "run_logrotate": os.environ.get("LM_LOGROTATE_ENABLED") == "1",
        "logrotate_rule_present": os.environ.get("LM_LOGROTATE_PRESENT") == "1",
        "auto_install_logrotate": os.environ.get("LM_AUTO_INSTALL_LOGROTATE") == "1",
        "max_compress_per_run": _to_int(os.environ.get("LM_MAX_COMPRESS_PER_RUN")),
        "min_rollover_age_minutes": _to_int(os.environ.get("LM_MIN_ROLLOVER_AGE_MINUTES")),
        "prune_days": _to_int(os.environ.get("LM_PRUNE_DAYS")),
        "gzip_level": _to_int(os.environ.get("LM_GZIP_LEVEL")),
        "max_bytes_warn": _to_int(os.environ.get("LM_WARN_BYTES")),
        "max_bytes_crit": _to_int(os.environ.get("LM_CRIT_BYTES")),
    },
    "usage": {
        "log_dir_bytes": _to_int(os.environ.get("LM_USAGE_BYTES")),
        "log_dir_gib": round(_to_float(os.environ.get("LM_USAGE_BYTES")) / (1024.0**3), 4),
    },
    "actions_attempted": _read_lines(actions_path),
    "compressed_count": _to_int(os.environ.get("LM_COMPRESSED_COUNT")),
    "compressed_files": _read_lines(compressed_path),
    "pruned_count": _to_int(os.environ.get("LM_PRUNED_COUNT")),
    "pruned_files": _read_lines(pruned_path),
    "largest_files": largest_rows,
    "runtime": {
        "started_at": os.environ.get("LM_STARTED_AT") or "",
        "finished_at": os.environ.get("LM_FINISHED_AT") or "",
        "cycle_epoch": _to_int(os.environ.get("LM_CYCLE_EPOCH")),
    },
}

latest_path.parent.mkdir(parents=True, exist_ok=True)
event_path.parent.mkdir(parents=True, exist_ok=True)
latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
event_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY
  echo "log maintenance disabled by policy (LOG_MAINTENANCE_ENABLED=0)" >> "$LOG_FILE"
  echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") log maintenance cycle end ===" >> "$LOG_FILE"
  exit 0
fi

logrotate_present=0
if [[ -f "$ROTATE_FILE" ]]; then
  logrotate_present=1
fi

if [[ "$logrotate_present" == "0" && "$AUTO_INSTALL_LOGROTATE" == "1" && -x "$INSTALLER_SCRIPT" ]]; then
  BETBOT_LOGROTATE_FORCE_NOW=0 "$INSTALLER_SCRIPT" "$ENV_FILE" >> "$LOG_FILE" 2>&1 || true
  append_action "install_logrotate_rule"
  if [[ -f "$ROTATE_FILE" ]]; then
    logrotate_present=1
  fi
fi

if [[ "$RUN_LOGROTATE" == "1" ]]; then
  if [[ "$logrotate_present" == "1" ]]; then
    if command -v ionice >/dev/null 2>&1; then
      ionice -c3 nice -n 19 logrotate "$ROTATE_FILE" >> "$LOG_FILE" 2>&1 || true
    else
      nice -n 19 logrotate "$ROTATE_FILE" >> "$LOG_FILE" 2>&1 || true
    fi
    append_action "run_logrotate"
  else
    append_action "skip_logrotate_rule_missing"
  fi
fi

compressed_count=0
if [[ "$MAX_COMPRESS_PER_RUN" =~ ^[0-9]+$ ]] && (( MAX_COMPRESS_PER_RUN > 0 )); then
  mapfile -t compress_rows < <(find "$LOG_DIR" -maxdepth 1 -type f -name '*.log-*' ! -name '*.gz' -mmin "+$MIN_ROLLOVER_AGE_MINUTES" -printf '%s\t%p\n' 2>/dev/null | sort -nr || true)
  for row in "${compress_rows[@]}"; do
    (( compressed_count >= MAX_COMPRESS_PER_RUN )) && break
    file_path="${row#*$'\t'}"
    if [[ -z "$file_path" || ! -f "$file_path" ]]; then
      continue
    fi
    if command -v ionice >/dev/null 2>&1; then
      ionice -c3 nice -n 19 gzip -"${GZIP_LEVEL}" -f "$file_path" >> "$LOG_FILE" 2>&1 || true
    else
      nice -n 19 gzip -"${GZIP_LEVEL}" -f "$file_path" >> "$LOG_FILE" 2>&1 || true
    fi
    if [[ -f "${file_path}.gz" ]]; then
      ((compressed_count+=1))
      printf '%s\n' "$(basename "${file_path}.gz")" >> "$compressed_tmp"
    fi
  done
fi
append_action "compress_backlog"

pruned_count=0
if [[ "$PRUNE_DAYS" =~ ^[0-9]+$ ]] && (( PRUNE_DAYS > 0 )); then
  while IFS= read -r gz_file; do
    [[ -n "$gz_file" ]] || continue
    if [[ -f "$gz_file" ]]; then
      rm -f -- "$gz_file"
      ((pruned_count+=1))
      printf '%s\n' "$(basename "$gz_file")" >> "$pruned_tmp"
    fi
  done < <(find "$LOG_DIR" -maxdepth 1 -type f -name '*.log-*.gz' -mtime "+$PRUNE_DAYS" -print 2>/dev/null || true)
fi
append_action "prune_old_compressed"

usage_bytes="$(collect_usage_bytes)"
health_status="green"
if [[ "$usage_bytes" =~ ^[0-9]+$ ]]; then
  if (( usage_bytes >= MAX_BYTES_CRIT )); then
    health_status="red"
  elif (( usage_bytes >= MAX_BYTES_WARN )); then
    health_status="yellow"
  fi
fi

find "$LOG_DIR" -maxdepth 1 -type f -name '*.log*' -printf '%s\t%f\n' 2>/dev/null | sort -nr | head -n 12 > "$largest_tmp" || true

cycle_finished_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

export LM_HEALTH_STATUS="$health_status"
export LM_RUN_ENABLED="$MAINT_ENABLED"
export LM_LOGROTATE_ENABLED="$RUN_LOGROTATE"
export LM_LOGROTATE_PRESENT="$logrotate_present"
export LM_AUTO_INSTALL_LOGROTATE="$AUTO_INSTALL_LOGROTATE"
export LM_MAX_COMPRESS_PER_RUN="$MAX_COMPRESS_PER_RUN"
export LM_MIN_ROLLOVER_AGE_MINUTES="$MIN_ROLLOVER_AGE_MINUTES"
export LM_PRUNE_DAYS="$PRUNE_DAYS"
export LM_GZIP_LEVEL="$GZIP_LEVEL"
export LM_WARN_BYTES="$MAX_BYTES_WARN"
export LM_CRIT_BYTES="$MAX_BYTES_CRIT"
export LM_USAGE_BYTES="$usage_bytes"
export LM_COMPRESSED_COUNT="$compressed_count"
export LM_PRUNED_COUNT="$pruned_count"
export LM_CYCLE_EPOCH="$(date +%s)"
export LM_STARTED_AT="$cycle_started_at"
export LM_FINISHED_AT="$cycle_finished_at"

python3 - "$LATEST_FILE" "$EVENT_FILE" "$compressed_tmp" "$pruned_tmp" "$actions_tmp" "$largest_tmp" "$LOG_DIR" "$ROTATE_FILE" <<'PY'
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


def _read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    out: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        item = line.strip()
        if item:
            out.append(item)
    return out


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


latest_path = Path(sys.argv[1])
event_path = Path(sys.argv[2])
compressed_path = Path(sys.argv[3])
pruned_path = Path(sys.argv[4])
actions_path = Path(sys.argv[5])
largest_path = Path(sys.argv[6])
log_dir = Path(sys.argv[7])
rotate_file = Path(sys.argv[8])

largest_rows: list[dict[str, Any]] = []
for row in _read_lines(largest_path):
    parts = row.split("\t", 1)
    if len(parts) != 2:
        continue
    size_raw, name = parts
    largest_rows.append(
        {
            "name": name,
            "size_bytes": _to_int(size_raw),
        }
    )

payload = {
    "status": "ready",
    "captured_at": os.environ.get("LM_FINISHED_AT") or "",
    "health_status": os.environ.get("LM_HEALTH_STATUS") or "unknown",
    "log_dir": str(log_dir),
    "rotate_file": str(rotate_file),
    "policy": {
        "maintenance_enabled": os.environ.get("LM_RUN_ENABLED") == "1",
        "run_logrotate": os.environ.get("LM_LOGROTATE_ENABLED") == "1",
        "logrotate_rule_present": os.environ.get("LM_LOGROTATE_PRESENT") == "1",
        "auto_install_logrotate": os.environ.get("LM_AUTO_INSTALL_LOGROTATE") == "1",
        "max_compress_per_run": _to_int(os.environ.get("LM_MAX_COMPRESS_PER_RUN")),
        "min_rollover_age_minutes": _to_int(os.environ.get("LM_MIN_ROLLOVER_AGE_MINUTES")),
        "prune_days": _to_int(os.environ.get("LM_PRUNE_DAYS")),
        "gzip_level": _to_int(os.environ.get("LM_GZIP_LEVEL")),
        "max_bytes_warn": _to_int(os.environ.get("LM_WARN_BYTES")),
        "max_bytes_crit": _to_int(os.environ.get("LM_CRIT_BYTES")),
    },
    "usage": {
        "log_dir_bytes": _to_int(os.environ.get("LM_USAGE_BYTES")),
        "log_dir_gib": round(_to_float(os.environ.get("LM_USAGE_BYTES")) / (1024.0**3), 4),
    },
    "actions_attempted": _read_lines(actions_path),
    "compressed_count": _to_int(os.environ.get("LM_COMPRESSED_COUNT")),
    "compressed_files": _read_lines(compressed_path),
    "pruned_count": _to_int(os.environ.get("LM_PRUNED_COUNT")),
    "pruned_files": _read_lines(pruned_path),
    "largest_files": largest_rows,
    "runtime": {
        "started_at": os.environ.get("LM_STARTED_AT") or "",
        "finished_at": os.environ.get("LM_FINISHED_AT") or "",
        "cycle_epoch": _to_int(os.environ.get("LM_CYCLE_EPOCH")),
    },
}

latest_path.parent.mkdir(parents=True, exist_ok=True)
event_path.parent.mkdir(parents=True, exist_ok=True)
latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
event_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY

if [[ "$LOG_MAINT_ALERT_ENABLED" == "1" && -n "$LOG_MAINT_ALERT_WEBHOOK_TARGET_URL" ]]; then
  alert_payload="$(
    python3 - "$LATEST_FILE" "$LOG_MAINT_ALERT_STATE_FILE" "$LOG_MAINT_ALERT_NOTIFY_STATUS_CHANGE_ONLY" "$LOG_MAINT_ALERT_MESSAGE_MODE" "$LOG_MAINT_ALERT_GROWTH_BYTES_THRESHOLD" "$LOG_MAINT_ALERT_MIN_INTERVAL_SECONDS" "$LOG_MAINT_ALERT_WEBHOOK_USERNAME" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any


def _normalize(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _parse_epoch(text: str) -> int:
    raw = _normalize(text)
    if not raw:
        return int(time.time())
    try:
        return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp())
    except Exception:
        return int(time.time())


def _clip(text: str, limit: int = 90) -> str:
    raw = _normalize(text)
    if len(raw) <= limit:
        return raw
    return raw[: max(1, limit - 3)].rstrip() + "..."


latest_path = Path(sys.argv[1])
state_path = Path(sys.argv[2])
notify_status_change_only = _normalize(sys.argv[3]).lower() in {"1", "true", "yes", "y"}
message_mode = (_normalize(sys.argv[4]).lower() or "concise")
growth_threshold_bytes = max(0, _to_int(sys.argv[5], 0))
min_interval_seconds = max(0, _to_int(sys.argv[6], 0))
username = _normalize(sys.argv[7]) or "BetBot Ops"

try:
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
except Exception:
    payload = {}

try:
    state = json.loads(state_path.read_text(encoding="utf-8"))
except Exception:
    state = {}
if not isinstance(state, dict):
    state = {}

status = _normalize(payload.get("health_status")).lower() or "unknown"
captured_at = _normalize(payload.get("captured_at")) or datetime.now(timezone.utc).isoformat()
captured_epoch = _parse_epoch(captured_at)
usage = _to_int(((payload.get("usage") or {}) if isinstance(payload.get("usage"), dict) else {}).get("log_dir_bytes"), 0)
usage_gib = _to_float(((payload.get("usage") or {}) if isinstance(payload.get("usage"), dict) else {}).get("log_dir_gib"), usage / (1024.0**3))
compressed_count = _to_int(payload.get("compressed_count"), 0)
pruned_count = _to_int(payload.get("pruned_count"), 0)

prev_usage = _to_int(state.get("last_usage_bytes"), usage)
prev_captured_at = _normalize(state.get("last_captured_at"))
prev_epoch = _parse_epoch(prev_captured_at) if prev_captured_at else captured_epoch
prev_status = _normalize(state.get("last_status")).lower()
growth_bytes = max(0, usage - prev_usage)
elapsed_seconds = max(0, captured_epoch - prev_epoch)

reasons: list[str] = []
if status in {"yellow", "red"}:
    reasons.append(f"log_health_{status}")
if growth_threshold_bytes > 0 and growth_bytes >= growth_threshold_bytes:
    reasons.append("log_growth_spike")

should_alert = len(reasons) > 0
fingerprint = json.dumps(
    {"status": status, "reasons": sorted(reasons)},
    sort_keys=True,
    separators=(",", ":"),
)
last_fingerprint = _normalize(state.get("last_fingerprint"))
last_alert_epoch = _to_int(state.get("last_alert_epoch"), 0)
escalated_to_red = (status == "red" and prev_status != "red")

if should_alert and notify_status_change_only and not escalated_to_red:
    same_incident = last_fingerprint == fingerprint and bool(last_fingerprint)
    if same_incident and (captured_epoch - last_alert_epoch) < min_interval_seconds:
        should_alert = False

largest_files = payload.get("largest_files")
largest_rows = largest_files if isinstance(largest_files, list) else []
top_lines: list[str] = []
for row in largest_rows[:3]:
    if not isinstance(row, dict):
        continue
    name = _clip(_normalize(row.get("name")), 60)
    size_gib = _to_float(row.get("size_bytes"), 0.0) / (1024.0**3)
    if name:
        top_lines.append(f"{name} ({size_gib:.2f} GiB)")

if should_alert:
    reason_text_map = {
        "log_health_yellow": "log usage warning threshold reached",
        "log_health_red": "log usage critical threshold reached",
        "log_growth_spike": "rapid log growth detected",
    }
    readable_reasons = ", ".join(reason_text_map.get(item, item.replace("_", " ")) for item in reasons)
    growth_gib = growth_bytes / (1024.0**3)
    elapsed_m = elapsed_seconds / 60.0
    if message_mode == "detailed":
        lines = [
            "Log Health Alert",
            f"Status: {status.upper()}",
            f"What happened: {readable_reasons}.",
            f"Current usage: {usage_gib:.2f} GiB",
            f"Growth since last check: +{growth_gib:.2f} GiB over {elapsed_m:.1f}m",
            f"Auto cleanup: compressed={compressed_count}, pruned={pruned_count}",
        ]
        if top_lines:
            lines.append("Largest files right now:")
            lines.extend(f"- {line}" for line in top_lines)
        lines.append("Next step: confirm hot logs are rotating and retention settings are still safe.")
    else:
        top_logs_short = ", ".join(top_lines[:2]) if top_lines else "n/a"
        lines = [
            "Log Health Alert",
            f"Status: {status.upper()}",
            f"What happened: {readable_reasons}.",
            f"Usage: {usage_gib:.2f} GiB | Growth: +{growth_gib:.2f} GiB/{elapsed_m:.1f}m",
            f"Auto cleanup: compressed={compressed_count}, pruned={pruned_count}",
            f"Largest files: {_clip(top_logs_short, 140)}",
        ]
        lines.append("Next step: check log maintenance and the noisiest workers.")
    text = "\n".join(lines)
else:
    text = ""

state_out = {
    "last_usage_bytes": usage,
    "last_captured_at": captured_at,
    "last_status": status,
    "last_fingerprint": fingerprint if should_alert else ("" if not reasons else last_fingerprint),
    "last_alert_epoch": captured_epoch if should_alert else last_alert_epoch,
}
try:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state_out, indent=2), encoding="utf-8")
except Exception:
    pass

if should_alert and text:
    print(json.dumps({"text": text, "content": text, "username": username}))
PY
  )"
  if [[ -n "$alert_payload" ]]; then
    curl --silent --show-error --fail \
      --max-time "$LOG_MAINT_ALERT_WEBHOOK_TIMEOUT_SECONDS" \
      --header "Content-Type: application/json" \
      --data-binary "$alert_payload" \
      "$LOG_MAINT_ALERT_WEBHOOK_TARGET_URL" >/dev/null 2>&1 || true
    echo "log maintenance alert sent health=$health_status usage_bytes=$usage_bytes" >> "$LOG_FILE"
  fi
fi

echo "log maintenance health=$health_status usage_bytes=$usage_bytes compressed=$compressed_count pruned=$pruned_count" >> "$LOG_FILE"
echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") log maintenance cycle end ===" >> "$LOG_FILE"
