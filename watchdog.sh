#!/usr/bin/env bash
set -euo pipefail
LOCKFILE="/tmp/ib_gateway_watchdog.lock"
exec 9>"$LOCKFILE" || exit 1
flock -n 9 || exit 0

# =========================
# CONFIG
# =========================
PORT=4002
TMUX_SESSION="ibgw"
DISPLAY_NUM=1
LOG="$HOME/supreme-options-bot-prekafka/logs/watchdog.log"

IBC_DIR="/home/ubuntu/IBC"
GATEWAY_DIR="/home/ubuntu/Jts"
INI="/home/ubuntu/IBC/config.ini"
IBC_START="/home/ubuntu/IBC/target/IBCLinux/scripts/ibcstart.sh"

PYTHON_BIN="/home/ubuntu/optionsenv/bin/python"

BOOT_SLEEP=10
RETRIES=18
SLEEP_BETWEEN=10

# Path where send_text() lives
ALERT_PYTHON_PATH="/home/ubuntu/supreme-options-bot-prekafka"

# =========================
# HELPERS
# =========================
ts() { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }

mkdir -p "$(dirname "$LOG")"

# -------------------------
# Check if IB API is healthy
# -------------------------
api_ok() {
  "$PYTHON_BIN" - <<PY >/dev/null 2>&1
from ib_insync import IB
ib = IB()
try:
    ok = ib.connect("127.0.0.1", ${PORT}, clientId=997, timeout=3)
    if not ok:
        raise SystemExit(2)
    ib.reqCurrentTime()
    raise SystemExit(0)
except Exception:
    raise SystemExit(2)
finally:
    try:
        ib.disconnect()
    except Exception:
        pass
PY
}

# -------------------------
# Ensure Xvfb
# -------------------------
ensure_xvfb() {
  if ! pgrep -f "Xvfb :${DISPLAY_NUM}" >/dev/null 2>&1; then
    log "Starting Xvfb :${DISPLAY_NUM}"
    nohup Xvfb ":${DISPLAY_NUM}" -screen 0 1920x1080x24 >/dev/null 2>&1 &
    sleep 1
  fi
}

# -------------------------
# Restart Gateway
# -------------------------
restart_gateway() {
  log "Restarting IB Gateway via IBC"

  pkill -f -i "ibgateway" || true
  pkill -f -i "IBGateway" || true
  pkill -f -i "tws" || true
  sleep 3

  ensure_xvfb

  if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
    tmux kill-session -t "$TMUX_SESSION"
  fi

  tmux new-session -d -s "$TMUX_SESSION" \
    "export DISPLAY=:${DISPLAY_NUM}; bash -lc '$IBC_START --gateway --tws-path=\"$GATEWAY_DIR\" --ibc-path=\"$IBC_DIR\" --ini=\"$INI\"'"

  sleep "$BOOT_SLEEP"
}

# -------------------------
# Send 2FA alert
# -------------------------
send_2fa_alert() {
  log "Sending 2FA alert"

  "$PYTHON_BIN" - <<PY
import sys
sys.path.append("${ALERT_PYTHON_PATH}")
from message import send_text

send_text(
    "⚠️ IB Gateway needs manual login / 2FA.\n\n"
    "Watchdog restarted Gateway but API did not recover.\n"
    "Please approve login in IB app or VNC."
)
PY
}

# =========================
# MAIN
# =========================
if api_ok; then
  log "OK — IB API healthy"
  exit 0
fi

log "IB API unhealthy — restarting"
restart_gateway

for ((i=1; i<=RETRIES; i++)); do
  if api_ok; then
    log "RECOVERED — IB API healthy after restart ($i/$RETRIES)"
    exit 0
  fi
  log "Waiting ($i/$RETRIES)"
  sleep "$SLEEP_BETWEEN"
done

log "FAILED — likely 2FA required"
send_2fa_alert
exit 1
