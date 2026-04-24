#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="/etc/betbot/temperature-shadow.env"
SEND_MODE=0
UNIQUE_ONLY=1
TIMEOUT_SECONDS=5
MESSAGE_PREFIX="BetBot route test"

usage() {
  cat <<'EOF'
usage:
  test_discord_routes.sh [--env <path>] [--send] [--all] [--timeout <sec>] [--prefix <text>]

defaults:
  --env /etc/betbot/temperature-shadow.env
  dry-run mode (no posts)
  --all disabled (send one test per unique route only)
  --timeout 5

flags:
  --send           actually send probe messages
  --all            when --send, send one probe per bot (not deduped by route)
  --timeout <sec>  webhook timeout seconds
  --prefix <text>  probe message title
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      [[ $# -ge 2 ]] || { echo "missing value for --env" >&2; exit 1; }
      ENV_FILE="$2"
      shift 2
      ;;
    --send)
      SEND_MODE=1
      shift
      ;;
    --all)
      UNIQUE_ONLY=0
      shift
      ;;
    --timeout)
      [[ $# -ge 2 ]] || { echo "missing value for --timeout" >&2; exit 1; }
      TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --prefix)
      [[ $# -ge 2 ]] || { echo "missing value for --prefix" >&2; exit 1; }
      MESSAGE_PREFIX="$2"
      shift 2
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

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -r "$ENV_FILE" ]]; then
  echo "env file not readable: $ENV_FILE" >&2
  exit 1
fi
if [[ ! "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "--timeout must be integer seconds" >&2
  exit 1
fi

python3 - "$ENV_FILE" "$SEND_MODE" "$UNIQUE_ONLY" "$TIMEOUT_SECONDS" "$MESSAGE_PREFIX" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import subprocess
import sys
from pathlib import Path


def parse_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        env[key] = value
    return env


def text(env: dict[str, str], key: str) -> str:
    return str(env.get(key, "") or "").strip()


def flag(env: dict[str, str], key: str, default: bool = True) -> bool:
    value = text(env, key).lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


def resolve(env: dict[str, str], key: str, *fallback_keys: str) -> str:
    value = text(env, key)
    if value:
        return value
    for fallback in fallback_keys:
        value = text(env, fallback)
        if value:
            return value
    return ""


def resolve_thread(env: dict[str, str], key: str, *fallback_keys: str) -> str:
    value = text(env, key)
    if value:
        return value
    for fallback in fallback_keys:
        value = text(env, fallback)
        if value:
            return value
    return ""


def webhook_hint(url: str) -> str:
    if not url:
        return "<unset>"
    m = re.match(r"^https://discord\.com/api/webhooks/([^/]+)/(.+)$", url)
    if not m:
        return "<non-discord-url>"
    webhook_id, token = m.groups()
    return f"id={webhook_id} token=...{token[-6:]}"


def build_target_url(url: str, thread_id: str) -> str:
    if not url or not thread_id:
        return url
    if "thread_id=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}thread_id={thread_id}"


def route_hint(url: str, thread_id: str) -> str:
    tid = thread_id.strip()
    return f"{webhook_hint(url)} thread={tid or '<default>'}"


def send_probe(url: str, username: str, content: str, timeout: int) -> tuple[bool, str]:
    payload = json.dumps(
        {"content": content, "text": content, "username": username, "allowed_mentions": {"parse": []}}
    )
    cmd = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--max-time",
        str(max(1, timeout)),
        "--header",
        "Content-Type: application/json",
        "--user-agent",
        "betbot-route-test/1.0",
        "--data-binary",
        payload,
        url,
    ]
    try:
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True, "ok"
    except subprocess.CalledProcessError as exc:
        return False, f"curl_exit_{exc.returncode}"
    except Exception as exc:
        return False, f"error:{type(exc).__name__}"


env_path = Path(sys.argv[1])
send_mode = str(sys.argv[2]).strip() == "1"
unique_only = str(sys.argv[3]).strip() == "1"
timeout_seconds = int(float(sys.argv[4]))
prefix = str(sys.argv[5] or "").strip() or "BetBot route test"

env = parse_env(env_path)

routing = [
    (
        "shadow_loop_health",
        resolve(env, "SHADOW_ALERT_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "SHADOW_ALERT_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        flag(env, "ALERT_NOTIFY_YELLOW", True) or flag(env, "ALERT_NOTIFY_RED", True) or flag(env, "ALERT_NOTIFY_MILESTONE", True),
        text(env, "SHADOW_ALERT_WEBHOOK_USERNAME") or "BetBot Shadow Health",
    ),
    (
        "alpha_summary_12h_selected",
        resolve(env, "ALPHA_SUMMARY_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "ALPHA_SUMMARY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        flag(env, "ALPHA_SUMMARY_SEND_WEBHOOK", True),
        "BetBot",
    ),
    (
        "alpha_summary_12h_alpha",
        resolve(env, "ALPHA_SUMMARY_WEBHOOK_ALPHA_URL", "ALPHA_SUMMARY_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID", "ALPHA_SUMMARY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        flag(env, "ALPHA_SUMMARY_SEND_ALPHA_WEBHOOK", False),
        "BetBot Alpha",
    ),
    (
        "alpha_summary_12h_ops",
        resolve(env, "ALPHA_SUMMARY_WEBHOOK_OPS_URL", "ALPHA_SUMMARY_WEBHOOK_ALPHA_URL", "ALPHA_SUMMARY_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID", "ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID", "ALPHA_SUMMARY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        flag(env, "ALPHA_SUMMARY_SEND_OPS_WEBHOOK", False),
        "BetBot Ops",
    ),
    (
        "blocker_audit_168h",
        resolve(env, "BLOCKER_AUDIT_WEBHOOK_URL", "ALPHA_SUMMARY_WEBHOOK_ALPHA_URL", "ALPHA_SUMMARY_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "BLOCKER_AUDIT_WEBHOOK_THREAD_ID", "ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID", "ALPHA_SUMMARY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        flag(env, "BLOCKER_AUDIT_SEND_WEBHOOK", True),
        text(env, "BLOCKER_AUDIT_WEBHOOK_USERNAME") or "BetBot Ops",
    ),
    (
        "pipeline_recovery",
        resolve(env, "RECOVERY_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "RECOVERY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        True,
        text(env, "RECOVERY_WEBHOOK_USERNAME") or "BetBot Recovery",
    ),
    (
        "readiness_pipeline_alert",
        resolve(env, "PIPELINE_ALERT_WEBHOOK_URL", "RECOVERY_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "PIPELINE_ALERT_WEBHOOK_THREAD_ID", "RECOVERY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        True,
        text(env, "PIPELINE_ALERT_WEBHOOK_USERNAME") or "BetBot Pipeline",
    ),
    (
        "recovery_chaos_check",
        resolve(env, "RECOVERY_CHAOS_WEBHOOK_URL", "RECOVERY_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "RECOVERY_CHAOS_WEBHOOK_THREAD_ID", "RECOVERY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        True,
        text(env, "RECOVERY_CHAOS_WEBHOOK_USERNAME") or "BetBot Recovery Drill",
    ),
    (
        "stale_metrics_drill",
        resolve(env, "STALE_METRICS_DRILL_ALERT_WEBHOOK_URL", "RECOVERY_CHAOS_WEBHOOK_URL", "RECOVERY_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID", "RECOVERY_CHAOS_WEBHOOK_THREAD_ID", "RECOVERY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        flag(env, "STALE_METRICS_DRILL_ALERT_ENABLED", True),
        text(env, "STALE_METRICS_DRILL_ALERT_WEBHOOK_USERNAME") or "BetBot Stale Drill",
    ),
    (
        "log_maintenance_alert",
        resolve(env, "LOG_MAINT_ALERT_WEBHOOK_URL", "ALERT_WEBHOOK_URL"),
        resolve_thread(env, "LOG_MAINT_ALERT_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID"),
        flag(env, "LOG_MAINT_ALERT_ENABLED", True),
        text(env, "LOG_MAINT_ALERT_WEBHOOK_USERNAME") or "BetBot Ops",
    ),
]

enabled_routes: list[dict[str, str]] = []
print(f"Discord Route Test Plan")
print(f"env_file: {env_path}")
print(f"mode: {'send' if send_mode else 'dry-run'} (unique_only={unique_only})")
print("")
for bot, base_url, thread_id, enabled, username in routing:
    target_url = build_target_url(base_url, thread_id)
    status = "enabled" if enabled else "disabled"
    route = route_hint(base_url, thread_id) if enabled else "<disabled>"
    print(f"- {bot}: {route} ({status})")
    if enabled and target_url:
        enabled_routes.append(
            {
                "bot": bot,
                "base_url": base_url,
                "target_url": target_url,
                "thread_id": thread_id,
                "username": username or "BetBot",
            }
        )

if not send_mode:
    raise SystemExit(0)

print("")
to_send: list[dict[str, str]] = []
if unique_only:
    dedupe: dict[str, dict[str, str]] = {}
    for row in enabled_routes:
        route_key = f"{row['base_url']}||{row['thread_id']}"
        dedupe.setdefault(route_key, row)
    to_send = list(dedupe.values())
else:
    to_send = enabled_routes

if not to_send:
    print("no enabled routes to send")
    raise SystemExit(0)

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
failures = 0
print(f"sending probes: {len(to_send)}")
for row in to_send:
    content = (
        f"{prefix}\n"
        f"bot: {row['bot']}\n"
        f"route: {route_hint(row['base_url'], row['thread_id'])}\n"
        f"time: {now}"
    )
    ok, detail = send_probe(row["target_url"], row["username"], content, timeout_seconds)
    print(f"- {row['bot']}: {'ok' if ok else 'fail'} ({detail})")
    if not ok:
        failures += 1

if failures:
    raise SystemExit(2)
PY
