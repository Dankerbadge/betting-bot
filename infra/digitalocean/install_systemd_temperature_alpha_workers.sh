#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICE_NAME="betbot-temperature-alpha-workers"
ENV_FILE="/etc/betbot/temperature-shadow.env"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
RUN_SCRIPT="$REPO_DIR/infra/digitalocean/run_temperature_alpha_workers.sh"
DEPLOY_USER="${BETBOT_DEPLOY_USER:-${SUDO_USER:-${USER:-}}}"

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
Description=BetBot temperature alpha expansion workers
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=300
StartLimitBurst=5

[Service]
Type=simple
EnvironmentFile=$ENV_FILE
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/bash $RUN_SCRIPT $ENV_FILE
Restart=always
RestartSec=10
TimeoutStartSec=45min
TimeoutStopSec=2min
User=$DEPLOY_USER
WorkingDirectory=$REPO_DIR
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now "$SERVICE_NAME"

sleep 1
sudo systemctl --no-pager --full status "$SERVICE_NAME" | sed -n '1,60p'

echo
echo "Service installed: $SERVICE_NAME"
echo "Logs:"
echo "  sudo journalctl -u $SERVICE_NAME -f"
