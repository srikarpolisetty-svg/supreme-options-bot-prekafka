#!/usr/bin/env bash
set -euo pipefail

LOCKFILE="/tmp/ib_gateway_watchdog.lock"
exec 9>"$LOCKFILE" || exit 1
flock -n 9 || exit 0

PORT=4002
TMUX_SESSION="ib"
DISPLAY_NUM=2
LOG="$HOME/supreme-options-bot-prekafka/logs/watchdog.log"

IBC_DIR="/home/ubuntu/IBC"
GATEWAY_DIR="/home/ubuntu/Jts"
INI="/home/ubuntu/IBC/config.ini"
IBC_START="/home/ubuntu/IBC/target/IBCLinux/scripts/ibcstart.sh"

# MUST be provided for your ibcstart.sh
TWS_VERSION="1043"

PYTHON_BIN="/home/ubuntu/optionsenv/bin/python"

BOOT_SLEEP=10
RETRIES=18
SLEEP_BETWEEN=10

ALERT_PYTHON_PATH="/home/ubuntu/supreme-options-bot-prekafka"
TMUX_KEEPALIVE_SECONDS=600

ts()  { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }
mkdir -p "$(dirname "$LOG")"

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

ensure_xvfb() {
  if ! command -v Xvfb >/dev/null 2>&1; then
    log "ERROR: Xvfb not found in PATH"
    return 1
  fi

  # Never collide with VNC/X servers
  if pgrep -af "Xtigervnc :${DISPLAY_NUM}\b|Xvnc :${DISPLAY_NUM}\b|vncserver :${DISPLAY_NUM}\b|Xorg :${DISPLAY_NUM}\b" >/dev/null 2>&1; then
    log "ERROR: Display :${DISPLAY_NUM} is owned by VNC/X server. Pick another DISPLAY_NUM."
    return 1
  fi

  if pgrep -af "Xvfb :${DISPLAY_NUM}\b" >/dev/null 2>&1; then
    log "Xvfb already running on :${DISPLAY_NUM}"
    return 0
  fi

  log "Starting Xvfb :${DISPLAY_NUM}"
  local xvfb_log="$HOME/supreme-options-bot-prekafka/logs/xvfb_${DISPLAY_NUM}.log"
  nohup Xvfb ":${DISPLAY_NUM}" -screen 0 1920x1080x24 >>"$xvfb_log" 2>&1 &
  sleep 1

  if ! pgrep -af "Xvfb :${DISPLAY_NUM}\b" >/dev/null 2>&1; then
    log "ERROR: Failed to start Xvfb :${DISPLAY_NUM}"
    tail -n 120 "$xvfb_log" | tee -a "$LOG" || true
    return 1
  fi
}

start_tmux_ibc() {
  log "Starting tmux session '$TMUX_SESSION' with IBC"

  local start_log="$HOME/supreme-options-bot-prekafka/logs/tmux_start_${TMUX_SESSION}.log"
  local pane_log="$HOME/supreme-options-bot-prekafka/logs/tmux_pane_${TMUX_SESSION}.log"
  : >"$start_log" || true
  : >"$pane_log" || true

  if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
    tmux kill-session -t "$TMUX_SESSION" >>"$start_log" 2>&1 || true
  fi

  set +e
  tmux new-session -d -s "$TMUX_SESSION" bash -lc "
    set +e
    export DISPLAY=:${DISPLAY_NUM}

    echo '[watchdog] DISPLAY='\"\$DISPLAY\"
    echo '[watchdog] launching IBC...'
    echo '[watchdog] cmd: ${IBC_START} ${TWS_VERSION} --gateway --tws-path=${GATEWAY_DIR} --ibc-path=${IBC_DIR} --ibc-ini=${INI}'

    \"${IBC_START}\" \"${TWS_VERSION}\" --gateway \
      --tws-path=\"${GATEWAY_DIR}\" \
      --ibc-path=\"${IBC_DIR}\" \
      --ibc-ini=\"${INI}\"

    ec=\$?
    echo \"[watchdog] ibcstart exited ec=\$ec\"
    echo \"[watchdog] keeping tmux alive for ${TMUX_KEEPALIVE_SECONDS}s...\"
    sleep ${TMUX_KEEPALIVE_SECONDS}
  " >>"$start_log" 2>&1
  tmux_ec=$?
  set -e

  log "tmux new-session exit_code=$tmux_ec"
  tail -n 120 "$start_log" | tee -a "$LOG" || true

  if [[ $tmux_ec -ne 0 ]]; then
    log "ERROR: tmux failed to create the session"
    return 1
  fi

  if ! tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
    log "ERROR: tmux reported success but session '$TMUX_SESSION' not found"
    tmux ls 2>&1 | tee -a "$LOG" || true
    return 1
  fi

  tmux pipe-pane -t "${TMUX_SESSION}:0.0" -o "cat >> \"$pane_log\"" >>"$start_log" 2>&1 || true
  sleep 2
  log "tmux pane output (startup):"
  tmux capture-pane -t "${TMUX_SESSION}:0.0" -p 2>&1 | tail -n 200 | tee -a "$LOG" || true
  log "tail pane log:"
  tail -n 120 "$pane_log" | tee -a "$LOG" || true

  return 0
}

restart_gateway() {
  log "Restarting IB Gateway via IBC"

  pkill -f -i "ibgateway" || true
  pkill -f -i "IBGateway" || true
  pkill -f -i "tws" || true
  sleep 3

  ensure_xvfb
  start_tmux_ibc
  sleep "$BOOT_SLEEP"
}

send_2fa_alert() {
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

if api_ok; then
  log "OK — IB API healthy"
  exit 0
fi

log "IB API unhealthy — restarting"
if ! restart_gateway; then
  log "ERROR: restart_gateway failed — aborting"
  exit 1
fi

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
