#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICE_NAME="betbot-temperature-coldmath-hardening"
TIMER_NAME="${SERVICE_NAME}.timer"
ENV_FILE="/etc/betbot/temperature-shadow.env"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
TIMER_FILE="/etc/systemd/system/${TIMER_NAME}"
RUN_SCRIPT="$REPO_DIR/infra/digitalocean/run_temperature_coldmath_hardening.sh"
DEPLOY_USER="${BETBOT_DEPLOY_USER:-${SUDO_USER:-${USER:-}}}"
INTERVAL="${1:-2h}"
STRICT_FAIL_KEY="COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR"
REQUIRE_SUMMARY_KEY="RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY"
REMEDIATION_SCRIPT="$REPO_DIR/infra/digitalocean/set_coldmath_recovery_env_persistence_gate.sh"

read_env_key_value() {
  local key="$1"
  local line

  line="$(grep -E -m1 "^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=" "$ENV_FILE" || true)"
  if [[ -z "$line" ]]; then
    return 1
  fi

  printf '%s' "${line#*=}"
}

normalize_env_flag() {
  local raw_value="${1:-}"
  local trimmed
  local lower

  trimmed="$(printf '%s' "$raw_value" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  if [[ "$trimmed" == \"*\" && "$trimmed" == *\" ]]; then
    trimmed="${trimmed:1:${#trimmed}-2}"
  elif [[ "$trimmed" == \'*\' && "$trimmed" == *\' ]]; then
    trimmed="${trimmed:1:${#trimmed}-2}"
  fi

  lower="$(printf '%s' "$trimmed" | tr '[:upper:]' '[:lower:]')"
  case "$lower" in
    1|true|yes|on)
      printf 'enabled'
      ;;
    0|false|no|off)
      printf 'disabled'
      ;;
    *)
      printf 'invalid_or_missing'
      ;;
  esac
}

if [[ -z "$DEPLOY_USER" || "$DEPLOY_USER" == "root" ]]; then
  if id betbot >/dev/null 2>&1; then
    DEPLOY_USER="betbot"
  fi
fi

if ! id "$DEPLOY_USER" >/dev/null 2>&1; then
  echo "Unable to resolve deploy user: $DEPLOY_USER" >&2
  echo "Tip: set BETBOT_DEPLOY_USER=<user> when running this installer." >&2
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE" >&2
  echo "Create it from $REPO_DIR/infra/digitalocean/temperature-shadow.env.example" >&2
  exit 1
fi

strict_fail_raw=""
require_summary_raw=""
strict_fail_state="invalid_or_missing"
require_summary_state="invalid_or_missing"
strict_fail_found=0
require_summary_found=0

if strict_fail_raw="$(read_env_key_value "$STRICT_FAIL_KEY")"; then
  strict_fail_found=1
  strict_fail_state="$(normalize_env_flag "$strict_fail_raw")"
fi

if require_summary_raw="$(read_env_key_value "$REQUIRE_SUMMARY_KEY")"; then
  require_summary_found=1
  require_summary_state="$(normalize_env_flag "$require_summary_raw")"
fi

if [[ "$strict_fail_state" == "enabled" && "$require_summary_state" == "enabled" ]]; then
  echo "Strict recovery gates enabled: $STRICT_FAIL_KEY and $REQUIRE_SUMMARY_KEY are enabled."
else
  echo "WARNING: Strict recovery gates are not fully enabled." >&2
  if [[ "$strict_fail_state" != "enabled" ]]; then
    if [[ "$strict_fail_found" -eq 0 ]]; then
      echo "WARNING: $STRICT_FAIL_KEY is missing in $ENV_FILE (treated as disabled)." >&2
    elif [[ "$strict_fail_state" == "disabled" ]]; then
      echo "WARNING: $STRICT_FAIL_KEY is disabled in $ENV_FILE (value='$strict_fail_raw')." >&2
    else
      echo "WARNING: $STRICT_FAIL_KEY has invalid value '$strict_fail_raw' in $ENV_FILE (treated as disabled)." >&2
    fi
  fi
  if [[ "$require_summary_state" != "enabled" ]]; then
    if [[ "$require_summary_found" -eq 0 ]]; then
      echo "WARNING: $REQUIRE_SUMMARY_KEY is missing in $ENV_FILE (treated as disabled)." >&2
    elif [[ "$require_summary_state" == "disabled" ]]; then
      echo "WARNING: $REQUIRE_SUMMARY_KEY is disabled in $ENV_FILE (value='$require_summary_raw')." >&2
    else
      echo "WARNING: $REQUIRE_SUMMARY_KEY has invalid value '$require_summary_raw' in $ENV_FILE (treated as disabled)." >&2
    fi
  fi
  echo "Remediation: bash $REMEDIATION_SCRIPT --enable $ENV_FILE" >&2
fi

if [[ ! -x "$RUN_SCRIPT" ]]; then
  echo "Missing executable run script: $RUN_SCRIPT" >&2
  echo "Run: chmod +x $RUN_SCRIPT" >&2
  exit 1
fi

sudo tee "$SERVICE_FILE" >/dev/null <<UNIT
[Unit]
Description=BetBot ColdMath hardening cycle
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=$ENV_FILE
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/bash $RUN_SCRIPT $ENV_FILE
TimeoutStartSec=60min
TimeoutStopSec=2min
User=$DEPLOY_USER
WorkingDirectory=$REPO_DIR
StandardOutput=journal
StandardError=journal
UNIT

sudo tee "$TIMER_FILE" >/dev/null <<UNIT
[Unit]
Description=Run BetBot ColdMath hardening cycle every $INTERVAL

[Timer]
OnBootSec=9m
OnUnitActiveSec=$INTERVAL
Unit=$SERVICE_NAME.service
Persistent=true

[Install]
WantedBy=timers.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now "$TIMER_NAME"
sudo systemctl start "$SERVICE_NAME"

echo
echo "Timer installed: $TIMER_NAME"
echo "Status:"
sudo systemctl --no-pager --full status "$TIMER_NAME" | sed -n '1,50p'
echo
echo "Recent runs:"
sudo journalctl -u "$SERVICE_NAME" -n 40 --no-pager || true
