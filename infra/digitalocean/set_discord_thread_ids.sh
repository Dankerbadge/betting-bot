#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
AUDIT_SCRIPT="$SCRIPT_DIR/audit_discord_webhook_routing.sh"
ENV_FILE="/etc/betbot/temperature-shadow.env"
RESTART_SERVICES=0
RUN_STRICT_AUDIT=0

print_usage() {
  cat <<'EOF'
usage:
  set_discord_thread_ids.sh [--restart] [--audit] [/etc/betbot/temperature-shadow.env] KEY=THREAD_ID_OR_URL [KEY=THREAD_ID_OR_URL ...]

allowed keys:
  ALERT_WEBHOOK_THREAD_ID
  SHADOW_ALERT_WEBHOOK_THREAD_ID
  ALPHA_SUMMARY_WEBHOOK_THREAD_ID
  ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID
  ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID
  BLOCKER_AUDIT_WEBHOOK_THREAD_ID
  RECOVERY_WEBHOOK_THREAD_ID
  PIPELINE_ALERT_WEBHOOK_THREAD_ID
  RECOVERY_CHAOS_WEBHOOK_THREAD_ID
  STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID
  LOG_MAINT_ALERT_WEBHOOK_THREAD_ID

accepted values:
  - raw Discord thread id (e.g. 1499999999999999999)
  - Discord thread URL (e.g. https://discord.com/channels/<guild>/<channel>/<thread>)
  - Discord webhook URL with ?thread_id=<id>
  - Discord mention format <#1499999999999999999>

flags:
  --restart   restart impacted betbot services/timers after update
  --audit     run strict route audit after update
  -h, --help  show this help

example:
  set_discord_thread_ids.sh --restart --audit /etc/betbot/temperature-shadow.env \
    SHADOW_ALERT_WEBHOOK_THREAD_ID=https://discord.com/channels/123/456/1499999999999999999 \
    ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID=<#1499999999999999998>
EOF
}

run_with_privilege() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
    return
  fi
  if command -v sudo >/dev/null 2>&1; then
    sudo "$@"
    return
  fi
  "$@"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --restart)
      RESTART_SERVICES=1
      shift
      ;;
    --audit)
      RUN_STRICT_AUDIT=1
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done

if [[ $# -gt 0 && "$1" != *=* ]]; then
  ENV_FILE="$1"
  shift
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -r "$ENV_FILE" || ! -w "$ENV_FILE" ]]; then
  echo "env file must be readable and writable: $ENV_FILE" >&2
  exit 1
fi

if (( $# == 0 )); then
  print_usage
  exit 1
fi

python3 - "$ENV_FILE" "$@" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import re
import shutil
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
assignments = sys.argv[2:]

allowed_keys = {
    "ALERT_WEBHOOK_THREAD_ID",
    "SHADOW_ALERT_WEBHOOK_THREAD_ID",
    "ALPHA_SUMMARY_WEBHOOK_THREAD_ID",
    "ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID",
    "ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID",
    "BLOCKER_AUDIT_WEBHOOK_THREAD_ID",
    "RECOVERY_WEBHOOK_THREAD_ID",
    "PIPELINE_ALERT_WEBHOOK_THREAD_ID",
    "RECOVERY_CHAOS_WEBHOOK_THREAD_ID",
    "STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID",
    "LOG_MAINT_ALERT_WEBHOOK_THREAD_ID",
}


def extract_thread_id(raw_value: str) -> str | None:
    value = raw_value.strip()
    if not value:
        return None
    if re.fullmatch(r"[0-9]{5,32}", value):
        return value
    mention = re.fullmatch(r"<#([0-9]{5,32})>", value)
    if mention:
        return mention.group(1)
    query = re.search(r"[?&]thread_id=([0-9]{5,32})", value)
    if query:
        return query.group(1)
    if "discord.com/channels/" in value:
        try:
            after = value.split("discord.com/channels/", 1)[1]
        except Exception:
            after = ""
        path = after.split("?", 1)[0].split("#", 1)[0]
        parts = [part for part in path.split("/") if part]
        if len(parts) >= 3 and re.fullmatch(r"[0-9]{5,32}", parts[2]):
            return parts[2]
        return None
    trailing = re.search(r"([0-9]{5,32})(?:[/?#].*)?$", value)
    if trailing:
        return trailing.group(1)
    return None


updates: dict[str, str] = {}
for raw in assignments:
    if "=" not in raw:
        raise SystemExit(f"invalid assignment (missing '='): {raw}")
    key, value = raw.split("=", 1)
    key = key.strip()
    value = value.strip()
    if key not in allowed_keys:
        raise SystemExit(f"unsupported key: {key}")
    thread_id = extract_thread_id(value)
    if not thread_id:
        raise SystemExit(f"invalid thread id/url for {key}: {value!r}")
    updates[key] = thread_id

original = env_path.read_text(encoding="utf-8")
lines = original.splitlines()

def assign_line(text: str, key: str, value: str) -> str | None:
    stripped = text.strip()
    if not stripped:
        return None
    if stripped.startswith("#"):
        return None
    if "=" not in text:
        return None
    lhs = text.split("=", 1)[0].strip()
    if lhs != key:
        return None
    return f"{key}={value}"

remaining = dict(updates)
out_lines: list[str] = []
for line in lines:
    replaced = False
    for key, value in list(remaining.items()):
        new_line = assign_line(line, key, value)
        if new_line is not None:
            out_lines.append(new_line)
            remaining.pop(key, None)
            replaced = True
            break
    if not replaced:
        out_lines.append(line)

if remaining:
    if out_lines and out_lines[-1].strip():
        out_lines.append("")
    out_lines.append("# Discord thread route overrides")
    for key in sorted(remaining):
        out_lines.append(f"{key}={remaining[key]}")

new_text = "\n".join(out_lines)
if original.endswith("\n"):
    new_text += "\n"

timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
backup_path = env_path.with_name(env_path.name + f".bak_{timestamp}")
shutil.copy2(env_path, backup_path)
env_path.write_text(new_text, encoding="utf-8")

print(f"updated: {env_path}")
print(f"backup: {backup_path}")
for key in sorted(updates):
    print(f"{key}={updates[key]}")
PY

echo ""
if [[ "$RESTART_SERVICES" == "1" ]]; then
  services=(
    "betbot-temperature-shadow.service"
    "betbot-temperature-reporting.timer"
    "betbot-temperature-recovery.timer"
    "betbot-temperature-recovery-chaos.timer"
    "betbot-temperature-blocker-audit.timer"
    "betbot-temperature-log-maintenance.timer"
  )
  for unit in "${services[@]}"; do
    run_with_privilege systemctl restart "$unit"
  done
  echo "services restarted"
  run_with_privilege systemctl is-active "${services[@]}" | paste -sd',' - | awk '{print "service_states: "$0}'
else
  echo "next:"
  echo "  sudo systemctl restart betbot-temperature-shadow.service"
  echo "  sudo systemctl restart betbot-temperature-reporting.timer"
  echo "  sudo systemctl restart betbot-temperature-recovery.timer"
  echo "  sudo systemctl restart betbot-temperature-recovery-chaos.timer"
  echo "  sudo systemctl restart betbot-temperature-blocker-audit.timer"
  echo "  sudo systemctl restart betbot-temperature-log-maintenance.timer"
fi

if [[ "$RUN_STRICT_AUDIT" == "1" ]]; then
  echo ""
  if [[ -x "$AUDIT_SCRIPT" ]]; then
    run_with_privilege "$AUDIT_SCRIPT" "$ENV_FILE" strict
  else
    echo "missing audit script: $AUDIT_SCRIPT" >&2
    exit 2
  fi
else
  echo ""
  echo "next:"
  echo "  bash \"$AUDIT_SCRIPT\" \"$ENV_FILE\" strict"
fi
