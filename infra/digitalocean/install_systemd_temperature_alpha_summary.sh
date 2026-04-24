#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICE_NAME="betbot-temperature-alpha-summary"
TIMER_NAME="${SERVICE_NAME}.timer"
ENV_FILE="/etc/betbot/temperature-shadow.env"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
TIMER_FILE="/etc/systemd/system/${TIMER_NAME}"
RUN_SCRIPT="$REPO_DIR/infra/digitalocean/run_temperature_alpha_summary.sh"
DEPLOY_USER="${BETBOT_DEPLOY_USER:-${SUDO_USER:-${USER:-}}}"
INTERVAL="${1:-12h}"

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

if [[ ! -f "$RUN_SCRIPT" ]]; then
  echo "Missing run script: $RUN_SCRIPT" >&2
  exit 1
fi

sudo tee "$SERVICE_FILE" >/dev/null <<UNIT
[Unit]
Description=BetBot 12h alpha summary (Discord + artifact)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=$ENV_FILE
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/bash $RUN_SCRIPT $ENV_FILE
TimeoutStartSec=45min
TimeoutStopSec=2min
User=$DEPLOY_USER
WorkingDirectory=$REPO_DIR
StandardOutput=journal
StandardError=journal
UNIT

sudo tee "$TIMER_FILE" >/dev/null <<UNIT
[Unit]
Description=Run BetBot alpha summary every $INTERVAL

[Timer]
OnBootSec=12m
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
