#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-/etc/betbot/temperature-shadow.env}"
MODE="${2:-warn}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 1
fi

python3 - "$ENV_FILE" "$MODE" <<'PY'
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any


def parse_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        env[key] = value
    return env


def text(env: dict[str, str], key: str) -> str:
    return str(env.get(key, "") or "").strip()


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


def flag(env: dict[str, str], key: str, default: bool = True) -> bool:
    value = text(env, key).lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


def webhook_hint(url: str) -> str:
    if not url:
        return "<unset>"
    m = re.match(r"^https://discord\.com/api/webhooks/([^/]+)/(.+)$", url)
    if not m:
        return "<non-discord-url>"
    webhook_id, _token = m.groups()
    # Never print webhook token fragments in operator output/artifacts.
    return f"id={webhook_id}"


def route_hint(url: str, thread_id: str) -> str:
    base = webhook_hint(url)
    tid = text({"thread_id": thread_id}, "thread_id")
    if not tid:
        return f"{base} thread=<default>"
    return f"{base} thread={tid}"


env_file = Path(sys.argv[1])
mode = (sys.argv[2] if len(sys.argv) > 2 else "warn").strip().lower() or "warn"
strict_mode = mode == "strict" or text(parse_env(env_file), "DISCORD_WEBHOOK_SEPARATION_STRICT") == "1"

env = parse_env(env_file)
alert = resolve(env, "ALERT_WEBHOOK_URL")
alert_thread = resolve_thread(env, "ALERT_WEBHOOK_THREAD_ID")
alpha_selected = resolve(env, "ALPHA_SUMMARY_WEBHOOK_URL", "ALERT_WEBHOOK_URL")
alpha_selected_thread = resolve_thread(env, "ALPHA_SUMMARY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID")
alpha_dedicated = resolve(env, "ALPHA_SUMMARY_WEBHOOK_ALPHA_URL", "ALPHA_SUMMARY_WEBHOOK_URL", "ALERT_WEBHOOK_URL")
alpha_dedicated_thread = resolve_thread(
    env,
    "ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID",
    "ALPHA_SUMMARY_WEBHOOK_THREAD_ID",
    "ALERT_WEBHOOK_THREAD_ID",
)
alpha_ops = resolve(env, "ALPHA_SUMMARY_WEBHOOK_OPS_URL", "ALPHA_SUMMARY_WEBHOOK_ALPHA_URL", "ALPHA_SUMMARY_WEBHOOK_URL", "ALERT_WEBHOOK_URL")
alpha_ops_thread = resolve_thread(
    env,
    "ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID",
    "ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID",
    "ALPHA_SUMMARY_WEBHOOK_THREAD_ID",
    "ALERT_WEBHOOK_THREAD_ID",
)
blocker = resolve(
    env,
    "BLOCKER_AUDIT_WEBHOOK_URL",
    "ALPHA_SUMMARY_WEBHOOK_ALPHA_URL",
    "ALPHA_SUMMARY_WEBHOOK_URL",
    "ALERT_WEBHOOK_URL",
)
blocker_thread = resolve_thread(
    env,
    "BLOCKER_AUDIT_WEBHOOK_THREAD_ID",
    "ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID",
    "ALPHA_SUMMARY_WEBHOOK_THREAD_ID",
    "ALERT_WEBHOOK_THREAD_ID",
)
shadow = resolve(env, "SHADOW_ALERT_WEBHOOK_URL", "ALERT_WEBHOOK_URL")
shadow_thread = resolve_thread(env, "SHADOW_ALERT_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID")
recovery = resolve(env, "RECOVERY_WEBHOOK_URL", "ALERT_WEBHOOK_URL")
recovery_thread = resolve_thread(env, "RECOVERY_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID")
pipeline = resolve(env, "PIPELINE_ALERT_WEBHOOK_URL", "RECOVERY_WEBHOOK_URL", "ALERT_WEBHOOK_URL")
pipeline_thread = resolve_thread(
    env,
    "PIPELINE_ALERT_WEBHOOK_THREAD_ID",
    "RECOVERY_WEBHOOK_THREAD_ID",
    "ALERT_WEBHOOK_THREAD_ID",
)
chaos = resolve(env, "RECOVERY_CHAOS_WEBHOOK_URL", "RECOVERY_WEBHOOK_URL", "ALERT_WEBHOOK_URL")
chaos_thread = resolve_thread(
    env,
    "RECOVERY_CHAOS_WEBHOOK_THREAD_ID",
    "RECOVERY_WEBHOOK_THREAD_ID",
    "ALERT_WEBHOOK_THREAD_ID",
)
stale = resolve(
    env,
    "STALE_METRICS_DRILL_ALERT_WEBHOOK_URL",
    "RECOVERY_CHAOS_WEBHOOK_URL",
    "RECOVERY_WEBHOOK_URL",
    "ALERT_WEBHOOK_URL",
)
stale_thread = resolve_thread(
    env,
    "STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID",
    "RECOVERY_CHAOS_WEBHOOK_THREAD_ID",
    "RECOVERY_WEBHOOK_THREAD_ID",
    "ALERT_WEBHOOK_THREAD_ID",
)
log_maint = resolve(env, "LOG_MAINT_ALERT_WEBHOOK_URL", "ALERT_WEBHOOK_URL")
log_maint_thread = resolve_thread(env, "LOG_MAINT_ALERT_WEBHOOK_THREAD_ID", "ALERT_WEBHOOK_THREAD_ID")

shadow_username = text(env, "SHADOW_ALERT_WEBHOOK_USERNAME") or "BetBot Shadow Health"
alpha_selected_username = "BetBot"
alpha_dedicated_username = "BetBot Alpha"
alpha_ops_username = "BetBot Ops"
blocker_username = text(env, "BLOCKER_AUDIT_WEBHOOK_USERNAME") or "BetBot Ops"
recovery_username = text(env, "RECOVERY_WEBHOOK_USERNAME") or "BetBot Recovery"
pipeline_username = text(env, "PIPELINE_ALERT_WEBHOOK_USERNAME") or "BetBot Pipeline"
chaos_username = text(env, "RECOVERY_CHAOS_WEBHOOK_USERNAME") or "BetBot Recovery Drill"
stale_username = text(env, "STALE_METRICS_DRILL_ALERT_WEBHOOK_USERNAME") or "BetBot Stale Drill"
log_maint_username = text(env, "LOG_MAINT_ALERT_WEBHOOK_USERNAME") or "BetBot Ops"

alpha_selected_enabled = flag(env, "ALPHA_SUMMARY_SEND_WEBHOOK", True)
alpha_dedicated_enabled = flag(env, "ALPHA_SUMMARY_SEND_ALPHA_WEBHOOK", False)
alpha_ops_enabled = flag(env, "ALPHA_SUMMARY_SEND_OPS_WEBHOOK", False)
blocker_enabled = flag(env, "BLOCKER_AUDIT_SEND_WEBHOOK", True)
shadow_enabled = True
recovery_enabled = True
pipeline_enabled = True
chaos_enabled = True
stale_enabled = flag(env, "STALE_METRICS_DRILL_ALERT_ENABLED", True)
log_maint_enabled = flag(env, "LOG_MAINT_ALERT_ENABLED", True)

routing = [
    (
        "shadow_loop_health",
        shadow,
        shadow_thread,
        "SHADOW_ALERT_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "SHADOW_ALERT_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        shadow_enabled,
        shadow_username,
    ),
    (
        "alpha_summary_12h_selected",
        alpha_selected,
        alpha_selected_thread,
        "ALPHA_SUMMARY_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "ALPHA_SUMMARY_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        alpha_selected_enabled,
        alpha_selected_username,
    ),
    (
        "alpha_summary_12h_alpha",
        alpha_dedicated,
        alpha_dedicated_thread,
        "ALPHA_SUMMARY_WEBHOOK_ALPHA_URL -> ALPHA_SUMMARY_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID -> ALPHA_SUMMARY_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        alpha_dedicated_enabled,
        alpha_dedicated_username,
    ),
    (
        "alpha_summary_12h_ops",
        alpha_ops,
        alpha_ops_thread,
        "ALPHA_SUMMARY_WEBHOOK_OPS_URL -> ALPHA_SUMMARY_WEBHOOK_ALPHA_URL -> ALPHA_SUMMARY_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID -> ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID -> ALPHA_SUMMARY_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        alpha_ops_enabled,
        alpha_ops_username,
    ),
    (
        "blocker_audit_168h",
        blocker,
        blocker_thread,
        "BLOCKER_AUDIT_WEBHOOK_URL -> ALPHA_SUMMARY_WEBHOOK_ALPHA_URL -> ALPHA_SUMMARY_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "BLOCKER_AUDIT_WEBHOOK_THREAD_ID -> ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID -> ALPHA_SUMMARY_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        blocker_enabled,
        blocker_username,
    ),
    (
        "pipeline_recovery",
        recovery,
        recovery_thread,
        "RECOVERY_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "RECOVERY_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        recovery_enabled,
        recovery_username,
    ),
    (
        "readiness_pipeline_alert",
        pipeline,
        pipeline_thread,
        "PIPELINE_ALERT_WEBHOOK_URL -> RECOVERY_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "PIPELINE_ALERT_WEBHOOK_THREAD_ID -> RECOVERY_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        pipeline_enabled,
        pipeline_username,
    ),
    (
        "recovery_chaos_check",
        chaos,
        chaos_thread,
        "RECOVERY_CHAOS_WEBHOOK_URL -> RECOVERY_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "RECOVERY_CHAOS_WEBHOOK_THREAD_ID -> RECOVERY_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        chaos_enabled,
        chaos_username,
    ),
    (
        "stale_metrics_drill",
        stale,
        stale_thread,
        "STALE_METRICS_DRILL_ALERT_WEBHOOK_URL -> RECOVERY_CHAOS_WEBHOOK_URL -> RECOVERY_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID -> RECOVERY_CHAOS_WEBHOOK_THREAD_ID -> RECOVERY_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        stale_enabled,
        stale_username,
    ),
    (
        "log_maintenance_alert",
        log_maint,
        log_maint_thread,
        "LOG_MAINT_ALERT_WEBHOOK_URL -> ALERT_WEBHOOK_URL",
        "LOG_MAINT_ALERT_WEBHOOK_THREAD_ID -> ALERT_WEBHOOK_THREAD_ID",
        log_maint_enabled,
        log_maint_username,
    ),
]

print("Discord Webhook Routing Audit")
print(f"env_file: {env_file}")
print(f"strict_mode: {strict_mode}")
print("")
for bot_name, url, thread_id, fallback_chain, thread_fallback_chain, enabled, username in routing:
    state = "enabled" if enabled else "disabled"
    target_hint = route_hint(url, thread_id) if enabled else "<disabled>"
    print(f"- {bot_name}: {target_hint} ({state})")
    print(f"  username: {username or '<unset>'}")
    print(f"  source_chain: {fallback_chain}")
    print(f"  thread_chain: {thread_fallback_chain}")

route_groups: dict[str, dict[str, Any]] = {}
webhook_groups: dict[str, list[str]] = {}
for bot_name, url, thread_id, _, _, enabled, _ in routing:
    if not enabled or not url:
        continue
    route_key = f"{url}||{thread_id}"
    route_groups.setdefault(route_key, {"url": url, "thread_id": thread_id, "bots": []})["bots"].append(bot_name)
    webhook_groups.setdefault(url, []).append(bot_name)

route_collisions = [
    {"route_hint": route_hint(group["url"], group["thread_id"]), "bots": group["bots"]}
    for group in route_groups.values()
    if len(group["bots"]) > 1
]

thread_var_by_bot = {
    "shadow_loop_health": "SHADOW_ALERT_WEBHOOK_THREAD_ID",
    "alpha_summary_12h_selected": "ALPHA_SUMMARY_WEBHOOK_THREAD_ID",
    "alpha_summary_12h_alpha": "ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID",
    "alpha_summary_12h_ops": "ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID",
    "blocker_audit_168h": "BLOCKER_AUDIT_WEBHOOK_THREAD_ID",
    "pipeline_recovery": "RECOVERY_WEBHOOK_THREAD_ID",
    "readiness_pipeline_alert": "PIPELINE_ALERT_WEBHOOK_THREAD_ID",
    "recovery_chaos_check": "RECOVERY_CHAOS_WEBHOOK_THREAD_ID",
    "stale_metrics_drill": "STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID",
    "log_maintenance_alert": "LOG_MAINT_ALERT_WEBHOOK_THREAD_ID",
}

thread_route_remediations: list[dict[str, Any]] = []
for row in route_collisions:
    bots = [str(item) for item in row.get("bots", [])]
    if len(bots) <= 1:
        continue
    missing_keys = [thread_var_by_bot.get(bot, "") for bot in bots]
    missing_keys = [key for key in missing_keys if key]
    thread_route_remediations.append(
        {
            "route_hint": row.get("route_hint", ""),
            "bots": bots,
            "required_thread_env_keys": missing_keys,
        }
    )

webhook_collisions = [
    {"webhook_hint": webhook_hint(url), "bots": bots}
    for url, bots in webhook_groups.items()
    if len(bots) > 1
]

print("")
if route_collisions:
    print("shared_route_groups:")
    for row in route_collisions:
        bots = ", ".join(row["bots"])
        print(f"- {row['route_hint']}: {bots}")
else:
    print("shared_route_groups: none")

if thread_route_remediations:
    print("")
    print("route_remediation:")
    print("- create one Discord thread per bot stream in your channel")
    print("- set the bot-specific *_WEBHOOK_THREAD_ID env vars below")
    for row in thread_route_remediations:
        route_text = str(row.get("route_hint") or "")
        keys = row.get("required_thread_env_keys") or []
        key_text = ", ".join(str(item) for item in keys if str(item))
        print(f"- {route_text}")
        if key_text:
            print(f"  set: {key_text}")

print("")
if webhook_collisions:
    print("shared_webhook_groups (informational):")
    for row in webhook_collisions:
        bots = ", ".join(row["bots"])
        print(f"- {row['webhook_hint']}: {bots}")
else:
    print("shared_webhook_groups (informational): none")

result = {
    "strict_mode": strict_mode,
    "bot_count": len(routing),
    "configured_count": sum(1 for _, url, _, _, _, enabled, _ in routing if enabled and bool(url)),
    "enabled_count": sum(1 for _, _, _, _, _, enabled, _ in routing if bool(enabled)),
    "shared_route_group_count": len(route_collisions),
    "shared_route_groups": route_collisions,
    "route_remediations": thread_route_remediations,
    "shared_webhook_group_count": len(webhook_collisions),
    "shared_webhook_groups": webhook_collisions,
}
print("")
print(json.dumps(result, indent=2))

if strict_mode and route_collisions:
    sys.exit(2)
PY
