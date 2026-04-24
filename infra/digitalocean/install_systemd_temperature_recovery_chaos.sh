#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICE_NAME="betbot-temperature-recovery-chaos"
TIMER_NAME="${SERVICE_NAME}.timer"
ENV_FILE="/etc/betbot/temperature-shadow.env"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
TIMER_FILE="/etc/systemd/system/${TIMER_NAME}"
RUN_SCRIPT="$REPO_DIR/infra/digitalocean/run_temperature_recovery_chaos_check.sh"
ON_CALENDAR="${1:-*-*-* 06:40:00 UTC}"
RANDOMIZED_DELAY_SEC="${2:-15m}"

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
Description=BetBot nightly recovery chaos check
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=$ENV_FILE
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/bash $RUN_SCRIPT $ENV_FILE
WorkingDirectory=$REPO_DIR
User=root
Group=root
StandardOutput=journal
StandardError=journal
UNIT

sudo tee "$TIMER_FILE" >/dev/null <<UNIT
[Unit]
Description=Run BetBot recovery chaos check nightly ($ON_CALENDAR)

[Timer]
OnCalendar=$ON_CALENDAR
RandomizedDelaySec=$RANDOMIZED_DELAY_SEC
AccuracySec=1m
Unit=$SERVICE_NAME.service
Persistent=true

[Install]
WantedBy=timers.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now "$TIMER_NAME"

echo
echo "Recovery chaos timer installed: $TIMER_NAME"
echo "Status:"
sudo systemctl --no-pager --full status "$TIMER_NAME" | sed -n '1,50p'
echo
echo "Manual run command:"
echo "  sudo systemctl start $SERVICE_NAME"
echo
echo "Recent runs:"
sudo journalctl -u "$SERVICE_NAME" -n 40 --no-pager || true
