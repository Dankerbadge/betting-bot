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

PYTHON_BIN="$BETBOT_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing python venv executable: $PYTHON_BIN" >&2
  exit 1
fi

AUDIT_SCRIPT="${DISCORD_ROUTE_GUARD_AUDIT_SCRIPT:-$BETBOT_ROOT/infra/digitalocean/audit_discord_webhook_routing.sh}"
if [[ ! -x "$AUDIT_SCRIPT" ]]; then
  echo "missing audit script: $AUDIT_SCRIPT" >&2
  exit 1
fi

DISCORD_ROUTE_GUARD_ENABLED="${DISCORD_ROUTE_GUARD_ENABLED:-1}"
DISCORD_ROUTE_GUARD_STRICT="${DISCORD_ROUTE_GUARD_STRICT:-1}"
DISCORD_ROUTE_GUARD_SERVICE_FAIL_ON_COLLISION="${DISCORD_ROUTE_GUARD_SERVICE_FAIL_ON_COLLISION:-${DISCORD_ROUTE_GUARD_FAIL_ON_COLLISION:-0}}"
DISCORD_ROUTE_GUARD_WEBHOOK_URL="${DISCORD_ROUTE_GUARD_WEBHOOK_URL:-${LOG_MAINT_ALERT_WEBHOOK_URL:-${RECOVERY_WEBHOOK_URL:-${ALERT_WEBHOOK_URL:-}}}}"
DISCORD_ROUTE_GUARD_WEBHOOK_THREAD_ID="${DISCORD_ROUTE_GUARD_WEBHOOK_THREAD_ID:-${LOG_MAINT_ALERT_WEBHOOK_THREAD_ID:-${RECOVERY_WEBHOOK_THREAD_ID:-${ALERT_WEBHOOK_THREAD_ID:-}}}}"
DISCORD_ROUTE_GUARD_WEBHOOK_TIMEOUT_SECONDS="${DISCORD_ROUTE_GUARD_WEBHOOK_TIMEOUT_SECONDS:-5}"
DISCORD_ROUTE_GUARD_WEBHOOK_USERNAME="${DISCORD_ROUTE_GUARD_WEBHOOK_USERNAME:-BetBot Route Guard}"
DISCORD_ROUTE_GUARD_NOTIFY_STATUS_CHANGE_ONLY="${DISCORD_ROUTE_GUARD_NOTIFY_STATUS_CHANGE_ONLY:-1}"
DISCORD_ROUTE_GUARD_MESSAGE_MODE="${DISCORD_ROUTE_GUARD_MESSAGE_MODE:-concise}"
DISCORD_ROUTE_GUARD_DIR="${DISCORD_ROUTE_GUARD_DIR:-$OUTPUT_DIR/health/discord_route_guard}"
DISCORD_ROUTE_GUARD_STATE_FILE="${DISCORD_ROUTE_GUARD_STATE_FILE:-$DISCORD_ROUTE_GUARD_DIR/.alert_state.json}"
DISCORD_ROUTE_GUARD_APPLY_CMD="${DISCORD_ROUTE_GUARD_APPLY_CMD:-sudo bash $BETBOT_ROOT/infra/digitalocean/check_discord_thread_map.sh --env $ENV_FILE --map /etc/betbot/discord-thread-map.env --strict --apply}"

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

DISCORD_ROUTE_GUARD_WEBHOOK_TARGET_URL="$(build_discord_target_url "$DISCORD_ROUTE_GUARD_WEBHOOK_URL" "$DISCORD_ROUTE_GUARD_WEBHOOK_THREAD_ID")"

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs" "$DISCORD_ROUTE_GUARD_DIR"
LOG_FILE="$OUTPUT_DIR/logs/discord_route_guard.log"
LOCK_FILE="$OUTPUT_DIR/.discord_route_guard.lock"
LATEST_FILE="$DISCORD_ROUTE_GUARD_DIR/discord_route_guard_latest.json"
EVENT_FILE="$DISCORD_ROUTE_GUARD_DIR/discord_route_guard_$(date -u +"%Y%m%d_%H%M%S").json"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] discord route guard skipped: lock busy" >> "$LOG_FILE"
  exit 0
fi

echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") discord route guard cycle start ===" >> "$LOG_FILE"

if [[ "$DISCORD_ROUTE_GUARD_ENABLED" != "1" ]]; then
  "$PYTHON_BIN" - "$LATEST_FILE" "$EVENT_FILE" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys

latest_path = Path(sys.argv[1])
event_path = Path(sys.argv[2])
payload = {
    "status": "ready",
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "guard_status": "disabled",
    "strict_mode": False,
    "shared_route_group_count": 0,
    "shared_route_groups": [],
    "route_remediations": [],
}
latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
event_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(str(event_path))
PY
  echo "discord route guard disabled by policy (DISCORD_ROUTE_GUARD_ENABLED=0)" >> "$LOG_FILE"
  echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") discord route guard cycle end ===" >> "$LOG_FILE"
  exit 0
fi

audit_mode="warn"
if [[ "$DISCORD_ROUTE_GUARD_STRICT" == "1" ]]; then
  audit_mode="strict"
fi

audit_output_file="$(mktemp)"
trap 'rm -f "$audit_output_file"' EXIT
set +e
"$AUDIT_SCRIPT" "$ENV_FILE" "$audit_mode" >"$audit_output_file" 2>&1
audit_rc=$?
set -e

summary_json="$("$PYTHON_BIN" - "$audit_output_file" "$LATEST_FILE" "$EVENT_FILE" "$audit_mode" "$audit_rc" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

audit_path = Path(sys.argv[1])
latest_path = Path(sys.argv[2])
event_path = Path(sys.argv[3])
audit_mode = str(sys.argv[4] or "warn").strip().lower() or "warn"
audit_rc = int(float(sys.argv[5]))

raw = audit_path.read_text(encoding="utf-8", errors="replace")
payload: dict[str, Any] = {}
for idx in [i for i, ch in enumerate(raw) if ch == "{"][::-1]:
    candidate = raw[idx:].strip()
    try:
        parsed = json.loads(candidate)
    except Exception:
        continue
    if isinstance(parsed, dict) and "bot_count" in parsed:
        payload = parsed
        break

shared_route_group_count = int(payload.get("shared_route_group_count") or 0)
shared_route_groups = payload.get("shared_route_groups") if isinstance(payload.get("shared_route_groups"), list) else []
route_remediations = payload.get("route_remediations") if isinstance(payload.get("route_remediations"), list) else []
strict_mode = bool(payload.get("strict_mode")) if payload else (audit_mode == "strict")

if shared_route_group_count > 0:
    guard_status = "red" if strict_mode else "yellow"
else:
    guard_status = "green"

captured = datetime.now(timezone.utc).isoformat()
event_payload = {
    "status": "ready",
    "captured_at": captured,
    "guard_status": guard_status,
    "strict_mode": strict_mode,
    "audit_mode": audit_mode,
    "audit_exit_code": int(audit_rc),
    "shared_route_group_count": shared_route_group_count,
    "shared_route_groups": shared_route_groups,
    "route_remediations": route_remediations,
    "raw_audit_excerpt": "\n".join(raw.splitlines()[:120]),
}
latest_path.parent.mkdir(parents=True, exist_ok=True)
event_path.parent.mkdir(parents=True, exist_ok=True)
latest_path.write_text(json.dumps(event_payload, indent=2), encoding="utf-8")
event_path.write_text(json.dumps(event_payload, indent=2), encoding="utf-8")

print(
    json.dumps(
        {
            "guard_status": guard_status,
            "strict_mode": strict_mode,
            "shared_route_group_count": shared_route_group_count,
            "event_file": str(event_path),
        }
    )
)
PY
)"

guard_status="$("$PYTHON_BIN" - "$summary_json" <<'PY'
from __future__ import annotations
import json
import sys
payload = json.loads(sys.argv[1])
print(str(payload.get("guard_status") or "unknown"))
PY
)"

if [[ -n "$DISCORD_ROUTE_GUARD_WEBHOOK_TARGET_URL" ]]; then
  webhook_payload="$("$PYTHON_BIN" - "$LATEST_FILE" "$DISCORD_ROUTE_GUARD_STATE_FILE" "$DISCORD_ROUTE_GUARD_NOTIFY_STATUS_CHANGE_ONLY" "$DISCORD_ROUTE_GUARD_MESSAGE_MODE" "$DISCORD_ROUTE_GUARD_WEBHOOK_USERNAME" "$DISCORD_ROUTE_GUARD_APPLY_CMD" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys

def clip_word(text: str, limit: int = 120) -> str:
    raw = str(text or "").strip()
    if len(raw) <= limit:
        return raw
    clipped = raw[:limit].rstrip()
    if " " in clipped:
        clipped = clipped.rsplit(" ", 1)[0].rstrip(" ,;:-")
    return clipped

def humanize_bot(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    mapping = {
        "shadow_loop_health": "shadow loop health",
        "alpha_summary_12h_ops": "alpha summary ops",
        "blocker_audit_168h": "blocker audit",
        "pipeline_recovery": "pipeline recovery",
        "readiness_pipeline_alert": "readiness alert",
        "log_maintenance_alert": "log maintenance",
        "route_guard_health": "route guard",
        "stale_metrics_drill": "stale drill",
    }
    if text in mapping:
        return mapping[text]
    return text.replace("_", " ")

latest_path = Path(sys.argv[1])
state_path = Path(sys.argv[2])
status_change_only = str(sys.argv[3]).strip().lower() in {"1", "true", "yes", "y"}
message_mode = (str(sys.argv[4] or "").strip().lower() or "concise")
username = (str(sys.argv[5] or "").strip() or "BetBot Route Guard")
apply_cmd = str(sys.argv[6] or "").strip()

try:
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
except Exception:
    payload = {}
if not isinstance(payload, dict):
    payload = {}

status = str(payload.get("guard_status") or "unknown").strip().lower()
shared = int(payload.get("shared_route_group_count") or 0)
strict_mode = bool(payload.get("strict_mode"))
remediations = payload.get("route_remediations") if isinstance(payload.get("route_remediations"), list) else []
groups = payload.get("shared_route_groups") if isinstance(payload.get("shared_route_groups"), list) else []

state = {}
if state_path.exists():
    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        loaded = {}
    if isinstance(loaded, dict):
        state = loaded

fingerprint = json.dumps(
    {
        "status": status,
        "shared_route_group_count": shared,
        "groups": groups,
    },
    sort_keys=True,
    separators=(",", ":"),
)
last_fingerprint = str(state.get("last_fingerprint") or "")
last_status = str(state.get("last_status") or "")
changed = (fingerprint != last_fingerprint) or (status != last_status)

notify = False
if status in {"red", "yellow"}:
    if (not status_change_only) or changed:
        notify = True
elif status == "green":
    if changed and last_status in {"red", "yellow"}:
        notify = True

state_out = {
    "last_checked_at": datetime.now(timezone.utc).isoformat(),
    "last_status": status,
    "last_fingerprint": fingerprint,
}
state_path.parent.mkdir(parents=True, exist_ok=True)
state_path.write_text(json.dumps(state_out, indent=2), encoding="utf-8")

header = "Discord Route Separation Check"
if status == "green":
    health = "GREEN"
    why = "route separation healthy"
elif status == "yellow":
    health = "YELLOW"
    why = "shared routes detected"
else:
    health = "RED"
    why = "shared routes violate strict separation"

first_group = groups[0] if groups else {}
first_route = str(first_group.get("route_hint") or "n/a")
first_bots = first_group.get("bots") if isinstance(first_group.get("bots"), list) else []
if first_bots:
    visible = [humanize_bot(str(item)) for item in first_bots[:3]]
    remaining = max(0, len(first_bots) - len(visible))
    if remaining > 0:
        visible.append(f"+{remaining} more")
    first_bots_text = ", ".join(visible)
else:
    first_bots_text = "n/a"
missing_keys: list[str] = []
for rem in remediations:
    if not isinstance(rem, dict):
        continue
    for key in rem.get("required_thread_env_keys") or []:
        text = str(key or "").strip()
        if text and text not in missing_keys:
            missing_keys.append(text)
missing_keys_count = len(missing_keys)

if message_mode == "detailed":
    lines = [
        header,
        f"Status: {health}",
        f"What happened: {why}.",
        f"Strict mode: {'on' if strict_mode else 'off'}",
        f"Collision groups: {shared}",
        f"Top shared route: {first_route}",
        f"Streams on top collision: {first_bots_text}",
        "Next step: assign one unique thread ID per bot stream and rerun the route audit.",
    ]
    if missing_keys:
        lines.append("Missing thread keys: " + ", ".join(missing_keys[:8]))
    if status in {"red", "yellow"} and apply_cmd:
        lines.append("Run this command: " + apply_cmd)
else:
    strict_text = "on" if strict_mode else "off"
    lines = [header]
    if status == "green":
        lines.append(f"Status: GREEN | strict={strict_text} | collisions=0")
        lines.append("What happened: route separation is healthy.")
        lines.append("Next step: none.")
    else:
        lines.append(f"Status: {health} | strict={strict_text} | collisions={shared}")
        lines.append(f"What happened: top collision on {clip_word(first_route, 112)}")
        lines.append(f"Streams impacted: {clip_word(first_bots_text, 112)}")
        lines.append(f"Missing thread IDs: {missing_keys_count}")
        lines.append("Next step: assign one unique Discord thread ID per bot stream.")
    if status in {"red", "yellow"}:
        if apply_cmd:
            run_hint = apply_cmd.replace(" --map /etc/betbot/discord-thread-map.env", "")
            run_hint = run_hint.replace("/home/betbot/betting-bot/", "")
            run_hint = " ".join(run_hint.split())
            lines.append("Run this command: " + clip_word(run_hint, 156))

text = "\n".join(lines)

try:
    latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
except Exception:
    latest_payload = {}
if not isinstance(latest_payload, dict):
    latest_payload = {}
latest_payload["discord_message_preview"] = text
latest_payload["discord_message_mode"] = message_mode
latest_payload["discord_message_generated_at"] = datetime.now(timezone.utc).isoformat()
try:
    latest_path.write_text(json.dumps(latest_payload, indent=2), encoding="utf-8")
except Exception:
    pass

if not notify:
    print("")
    raise SystemExit(0)

print(json.dumps({"text": text, "content": text, "username": username}))
PY
)"
  if [[ -n "$webhook_payload" ]]; then
    curl --silent --show-error --fail \
      --max-time "$DISCORD_ROUTE_GUARD_WEBHOOK_TIMEOUT_SECONDS" \
      --header "Content-Type: application/json" \
      --data-binary "$webhook_payload" \
      "$DISCORD_ROUTE_GUARD_WEBHOOK_TARGET_URL" >/dev/null 2>&1 || true
  fi
fi

echo "discord_route_guard status=$guard_status strict=$DISCORD_ROUTE_GUARD_STRICT audit_rc=$audit_rc latest=$LATEST_FILE event=$EVENT_FILE" >> "$LOG_FILE"
echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") discord route guard cycle end ===" >> "$LOG_FILE"

if [[ "$DISCORD_ROUTE_GUARD_SERVICE_FAIL_ON_COLLISION" == "1" && "$guard_status" != "green" ]]; then
  exit 2
fi
exit 0
