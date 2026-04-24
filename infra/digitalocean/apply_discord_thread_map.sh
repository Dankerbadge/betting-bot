#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
SETTER_SCRIPT="$SCRIPT_DIR/set_discord_thread_ids.sh"

ENV_FILE="/etc/betbot/temperature-shadow.env"
MAP_FILE="$SCRIPT_DIR/discord_thread_map.template.env"
LATEST_FILE=""
RESTART=1
AUDIT=1

if [[ -f "/etc/betbot/discord-thread-map.env" ]]; then
  MAP_FILE="/etc/betbot/discord-thread-map.env"
fi

usage() {
  cat <<'EOF'
usage:
  apply_discord_thread_map.sh [--env <path>] [--map <path>] [--latest <path>] [--no-restart] [--no-audit]

defaults:
  --env /etc/betbot/temperature-shadow.env
  --map /etc/betbot/discord-thread-map.env (preferred if present),
        else infra/digitalocean/discord_thread_map.template.env
  --latest <OUTPUT_DIR>/health/discord_route_guard/discord_route_guard_latest.json (auto-derived from env)
  restart enabled
  strict audit enabled

The map file should contain KEY=value lines for required thread-ID env keys.
By default this script auto-detects required keys from the latest route-guard
artifact, and falls back to this baseline set:
  SHADOW_ALERT_WEBHOOK_THREAD_ID
  ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID
  BLOCKER_AUDIT_WEBHOOK_THREAD_ID
  RECOVERY_WEBHOOK_THREAD_ID
  PIPELINE_ALERT_WEBHOOK_THREAD_ID
  RECOVERY_CHAOS_WEBHOOK_THREAD_ID
  STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID
  LOG_MAINT_ALERT_WEBHOOK_THREAD_ID

Values may be:
  - raw thread id
  - <#thread_id>
  - full Discord thread URL
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      [[ $# -ge 2 ]] || { echo "missing value for --env" >&2; exit 1; }
      ENV_FILE="$2"
      shift 2
      ;;
    --map)
      [[ $# -ge 2 ]] || { echo "missing value for --map" >&2; exit 1; }
      MAP_FILE="$2"
      shift 2
      ;;
    --latest)
      [[ $# -ge 2 ]] || { echo "missing value for --latest" >&2; exit 1; }
      LATEST_FILE="$2"
      shift 2
      ;;
    --no-restart)
      RESTART=0
      shift
      ;;
    --no-audit)
      AUDIT=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! -x "$SETTER_SCRIPT" ]]; then
  echo "missing setter script: $SETTER_SCRIPT" >&2
  exit 1
fi
if [[ ! -f "$MAP_FILE" ]]; then
  echo "missing map file: $MAP_FILE" >&2
  exit 1
fi

default_map_keys=(
  SHADOW_ALERT_WEBHOOK_THREAD_ID
  ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID
  BLOCKER_AUDIT_WEBHOOK_THREAD_ID
  RECOVERY_WEBHOOK_THREAD_ID
  PIPELINE_ALERT_WEBHOOK_THREAD_ID
  RECOVERY_CHAOS_WEBHOOK_THREAD_ID
  STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID
  LOG_MAINT_ALERT_WEBHOOK_THREAD_ID
)
map_keys=("${default_map_keys[@]}")
key_source="default"

if [[ -z "$LATEST_FILE" ]]; then
  output_dir="$(awk -F= '$1=="OUTPUT_DIR"{print substr($0, index($0, "=")+1)}' "$ENV_FILE" | tail -n 1 | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | sed 's/^["'"'"']//; s/["'"'"']$//')"
  if [[ -n "$output_dir" ]]; then
    candidate_latest="$output_dir/health/discord_route_guard/discord_route_guard_latest.json"
    if [[ -f "$candidate_latest" ]]; then
      LATEST_FILE="$candidate_latest"
    fi
  fi
fi

if [[ -n "$LATEST_FILE" && -f "$LATEST_FILE" ]]; then
  mapfile -t dynamic_keys < <(python3 - "$LATEST_FILE" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

latest = Path(sys.argv[1])
try:
    payload = json.loads(latest.read_text(encoding="utf-8"))
except Exception:
    payload = {}
if not isinstance(payload, dict):
    payload = {}

keys: list[str] = []
for remediation in payload.get("route_remediations", []) if isinstance(payload.get("route_remediations"), list) else []:
    if not isinstance(remediation, dict):
        continue
    required = remediation.get("required_thread_env_keys")
    if isinstance(required, list):
        for key in required:
            key_text = str(key or "").strip()
            if key_text:
                keys.append(key_text)

for key in sorted(set(keys)):
    print(key)
PY
)
  if (( ${#dynamic_keys[@]} > 0 )); then
    map_keys=("${dynamic_keys[@]}")
    key_source="route_guard_latest:$LATEST_FILE"
  fi
fi

declare -a assignments=()
for key in "${map_keys[@]}"; do
  value="$(awk -F= -v k="$key" '$1==k{print substr($0, index($0, "=")+1)}' "$MAP_FILE" | tail -n 1 | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
  [[ -n "$value" ]] || continue
  assignments+=("${key}=${value}")
done

if (( ${#assignments[@]} == 0 )); then
  echo "no non-empty thread IDs found in map for required keys (source=$key_source): $MAP_FILE" >&2
  exit 1
fi

declare -a cmd=("$SETTER_SCRIPT")
if (( RESTART == 1 )); then
  cmd+=(--restart)
fi
if (( AUDIT == 1 )); then
  cmd+=(--audit)
fi
cmd+=("$ENV_FILE")
cmd+=("${assignments[@]}")

echo "applying ${#assignments[@]} thread routing values from: $MAP_FILE"
echo "key source: $key_source"
for item in "${assignments[@]}"; do
  key="${item%%=*}"
  echo "  - $key"
done
echo ""

"${cmd[@]}"
