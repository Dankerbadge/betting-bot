#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_REPO="${BETBOT_SOURCE_REPO:-$(cd "$SCRIPT_DIR/.." && pwd)}"
RUNTIME_ROOT="${BETBOT_RUNTIME_ROOT:-$HOME/betting-bot-runtime}"
SOURCE_ENV_FILE="${BETBOT_SOURCE_ENV_FILE:-$SOURCE_REPO/data/research/account_onboarding.local.env}"
RUNTIME_ENV_FILE="${BETBOT_RUNTIME_ENV_FILE:-$RUNTIME_ROOT/data/research/account_onboarding.local.env}"
LAUNCHD_PLIST_PATH="${BETBOT_LAUNCHD_PLIST_PATH:-$HOME/Library/LaunchAgents/com.openai.codex.betbot.hourly.plist}"
LAUNCHD_STDOUT_PATH="${BETBOT_LAUNCHD_STDOUT_PATH:-$RUNTIME_ROOT/outputs/hourly_alpha_launchd_stdout.log}"
LAUNCHD_STDERR_PATH="${BETBOT_LAUNCHD_STDERR_PATH:-$RUNTIME_ROOT/outputs/hourly_alpha_launchd_stderr.log}"

if [[ ! -d "$SOURCE_REPO" ]]; then
  echo "missing source repo: $SOURCE_REPO" >&2
  exit 1
fi
if [[ ! -f "$SOURCE_ENV_FILE" ]]; then
  echo "missing source env file: $SOURCE_ENV_FILE" >&2
  exit 1
fi

mkdir -p "$RUNTIME_ROOT" "$RUNTIME_ROOT/outputs" "$(dirname "$LAUNCHD_PLIST_PATH")"

rsync -a --delete \
  --exclude ".git/" \
  --exclude ".venv/" \
  --exclude ".monitoring-venv/" \
  --exclude ".mypy_cache/" \
  --exclude ".pytest_cache/" \
  --exclude ".ruff_cache/" \
  --exclude "outputs/" \
  --exclude "exports/" \
  --exclude "__pycache__/" \
  "$SOURCE_REPO/" "$RUNTIME_ROOT/"

mkdir -p "$RUNTIME_ROOT/outputs" "$RUNTIME_ROOT/.secrets" "$(dirname "$RUNTIME_ENV_FILE")"
cp "$SOURCE_ENV_FILE" "$RUNTIME_ENV_FILE"
chmod 600 "$RUNTIME_ENV_FILE"

copy_output_seed_if_exists() {
  local relative_path="$1"
  local src="$SOURCE_REPO/outputs/$relative_path"
  local dst="$RUNTIME_ROOT/outputs/$relative_path"
  if [[ ! -f "$src" ]]; then
    return 0
  fi
  mkdir -p "$(dirname "$dst")"
  cp "$src" "$dst"
}

copy_output_seed_if_exists "kalshi_nonsports_history.csv"
copy_output_seed_if_exists "kalshi_execution_journal.sqlite3"
copy_output_seed_if_exists "kalshi_ws_state_latest.json"
copy_output_seed_if_exists "overnight_alpha/paper_live_account_state.json"
copy_output_seed_if_exists "overnight_alpha/shadow_bankroll_state.json"

if [[ -f "$SOURCE_REPO/.secrets/kalshi_private_key.pem" ]]; then
  cp "$SOURCE_REPO/.secrets/kalshi_private_key.pem" "$RUNTIME_ROOT/.secrets/kalshi_private_key.pem"
  chmod 600 "$RUNTIME_ROOT/.secrets/kalshi_private_key.pem"
fi
if [[ -f "$SOURCE_REPO/.secrets/noaa_cdo_token.txt" ]]; then
  cp "$SOURCE_REPO/.secrets/noaa_cdo_token.txt" "$RUNTIME_ROOT/.secrets/noaa_cdo_token.txt"
  chmod 600 "$RUNTIME_ROOT/.secrets/noaa_cdo_token.txt"
fi

python3 - "$RUNTIME_ENV_FILE" "$SOURCE_REPO" "$RUNTIME_ROOT" <<'PY'
from __future__ import annotations

from pathlib import Path
import sys

env_path = Path(sys.argv[1])
source_repo = Path(sys.argv[2]).as_posix()
runtime_root = Path(sys.argv[3]).as_posix()

raw_lines = env_path.read_text(encoding="utf-8").splitlines()
updated_lines: list[str] = []
seen: set[str] = set()

for line in raw_lines:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in line:
        updated_lines.append(line)
        continue
    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip().replace(source_repo, runtime_root)
    if key == "KALSHI_PRIVATE_KEY_PATH":
        value = f"{runtime_root}/.secrets/kalshi_private_key.pem"
    if key == "BETBOT_WEATHER_CDO_TOKEN_FILE":
        value = f"{runtime_root}/.secrets/noaa_cdo_token.txt"
    seen.add(key)
    updated_lines.append(f"{key}={value}")

if "KALSHI_PRIVATE_KEY_PATH" not in seen:
    updated_lines.append(f"KALSHI_PRIVATE_KEY_PATH={runtime_root}/.secrets/kalshi_private_key.pem")
if "BETBOT_WEATHER_CDO_TOKEN_FILE" not in seen:
    updated_lines.append(f"BETBOT_WEATHER_CDO_TOKEN_FILE={runtime_root}/.secrets/noaa_cdo_token.txt")

env_path.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")
PY

cat > "$LAUNCHD_PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.openai.codex.betbot.hourly</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>-lc</string>
    <string>BETBOT_ENV_FILE='$RUNTIME_ENV_FILE' '$RUNTIME_ROOT/scripts/hourly_alpha_overnight.sh'</string>
  </array>

  <key>WorkingDirectory</key>
  <string>$RUNTIME_ROOT</string>

  <key>StartInterval</key>
  <integer>3600</integer>

  <key>RunAtLoad</key>
  <true/>

  <key>StandardOutPath</key>
  <string>$LAUNCHD_STDOUT_PATH</string>

  <key>StandardErrorPath</key>
  <string>$LAUNCHD_STDERR_PATH</string>
</dict>
</plist>
PLIST

touch "$LAUNCHD_STDOUT_PATH" "$LAUNCHD_STDERR_PATH"
plutil -lint "$LAUNCHD_PLIST_PATH" >/dev/null

uid="$(id -u)"
launchctl bootout "gui/$uid/com.openai.codex.betbot.hourly" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$uid" "$LAUNCHD_PLIST_PATH"
launchctl enable "gui/$uid/com.openai.codex.betbot.hourly"
launchctl kickstart -k "gui/$uid/com.openai.codex.betbot.hourly"

echo "Installed launchd hourly runtime."
echo "source_repo=$SOURCE_REPO"
echo "runtime_root=$RUNTIME_ROOT"
echo "runtime_env_file=$RUNTIME_ENV_FILE"
echo "launchd_plist=$LAUNCHD_PLIST_PATH"
launchctl print "gui/$uid/com.openai.codex.betbot.hourly" | sed -n '1,120p'
