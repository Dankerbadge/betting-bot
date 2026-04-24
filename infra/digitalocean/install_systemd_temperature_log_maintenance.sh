#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICE_NAME="betbot-temperature-log-maintenance"
TIMER_NAME="${SERVICE_NAME}.timer"
ENV_FILE="/etc/betbot/temperature-shadow.env"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
TIMER_FILE="/etc/systemd/system/${TIMER_NAME}"
RUN_SCRIPT="$REPO_DIR/infra/digitalocean/run_temperature_log_maintenance.sh"
INTERVAL="${1:-30m}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE" >&2
  echo "Create it from $REPO_DIR/infra/digitalocean/temperature-shadow.env.example" >&2
  exit 1
fi

if [[ ! -f "$RUN_SCRIPT" ]]; then
  echo "Missing run script: $RUN_SCRIPT" >&2
  echo "Run: chmod +x $RUN_SCRIPT" >&2
  exit 1
fi

sudo tee "$SERVICE_FILE" >/dev/null <<UNIT
[Unit]
Description=BetBot log maintenance (logrotate + bounded compression/prune)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=$ENV_FILE
ExecStart=/usr/bin/bash $RUN_SCRIPT $ENV_FILE
TimeoutStartSec=25min
TimeoutStopSec=2min
WorkingDirectory=$REPO_DIR
StandardOutput=journal
StandardError=journal
Nice=19
IOSchedulingClass=idle
UNIT

sudo tee "$TIMER_FILE" >/dev/null <<UNIT
[Unit]
Description=Run BetBot log maintenance every $INTERVAL

[Timer]
OnBootSec=10m
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
