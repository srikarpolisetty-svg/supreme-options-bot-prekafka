#!/usr/bin/env bash
set -euo pipefail

# =========================
# DEBUG / SAFETY
# =========================
LOCKFILE="/tmp/ib_gateway_watchdog.lock"
exec 9>"$LOCKFILE" || exit 1
flock -n 9 || exit 0

# =========================
# CONFIG
# =========================
PORT=4002
TMUX_SESSION="ib"
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

ALERT_PYTHON_PATH="/home/ubuntu/supreme-options-bot-prekafka"

# =========================
# HELPERS
# =========================
ts()  { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }

mkdir -p "$(dirname "$LOG")"

# Print a mini-debug snapshot (super useful when stuff fails)
debug_dump() {
  log "----- DEBUG DUMP BEGIN -----"

  log "User: $(whoami)"
  log "Host: $(hostname)"
  log "PWD: $(pwd)"
  log "DISPLAY will be :${DISPLAY_NUM}"
  log "Paths:"
  log "  IBC_START=$IBC_START"
  log "  IBC_DIR=$IBC_DIR"
  log "  GATEWAY_DIR=$GATEWAY_DIR"
  log "  INI=$INI"
  log "  PYTHON_BIN=$PYTHON_BIN"

  # Existence + perms
  ls -l "$IBC_START" "$INI" "$PYTHON_BIN" 2>&1 | tee -a "$LOG" || true
  ls -ld "$GATEWAY_DIR" "$IBC_DIR" 2>&1 | tee -a "$LOG" || true

  # Xvfb
  if pgrep -af "Xvfb :${DISPLAY_NUM}" >/dev/null 2>&1; then
    log "Xvfb running:"
    pgrep -af "Xvfb :${DISPLAY_NUM}" 2>&1 | tee -a "$LOG" || true
  else
    log "Xvfb NOT running"
  fi

  # Port
  log "Port listeners for ${PORT}:"
  ss -ltnp 2>&1 | grep ":${PORT}\b" | tee -a "$LOG" || log "  (none listening)"

  # Processes
  log "Relevant processes:"
  ps aux 2>&1 | egrep -i "ibgateway|IBC|tws|java|Xvfb|tmux" | grep -v egrep | tee -a "$LOG" || true

  # tmux list
  log "tmux ls:"
  tmux ls 2>&1 | tee -a "$LOG" || log "  (no tmux server / no sessions)"

  log "----- DEBUG DUMP END -----"
}

# Trap errors so you *always* see why it died
on_err() {
  local ec=$?
  log "ERROR: watchdog crashed (exit_code=$ec) at line ${BASH_LINENO[0]}: ${BASH_COMMAND}"
  debug_dump
  exit "$ec"
}
trap on_err ERR

# Also log every command if you want ultra-verbose debugging:
# set -x

# =========================
# Check if IB API is healthy
# =========================
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

# =========================
# Ensure Xvfb
# =========================
ensure_xvfb() {
  if ! command -v Xvfb >/dev/null 2>&1; then
    log "ERROR: Xvfb not found in PATH"
    return 1
  fi

  if ! pgrep -f "Xvfb :${DISPLAY_NUM}" >/dev/null 2>&1; then
    log "Starting Xvfb :${DISPLAY_NUM}"
    nohup Xvfb ":${DISPLAY_NUM}" -screen 0 1920x1080x24 >>"$LOG" 2>&1 &
    sleep 1
  fi

  if ! pgrep -f "Xvfb :${DISPLAY_NUM}" >/dev/null 2>&1; then
    log "ERROR: Failed to start Xvfb :${DISPLAY_NUM}"
    return 1
  fi
}

# =========================
# Restart Gateway
# =========================
restart_gateway() {
  log "Restarting IB Gateway via IBC"
  debug_dump

  # Validate required paths early (fail with clear log)
  for p in "$IBC_START" "$INI" "$PYTHON_BIN" "$IBC_DIR" "$GATEWAY_DIR"; do
    if [[ ! -e "$p" ]]; then
      log "ERROR: Missing path: $p"
      return 1
    fi
  done

  if [[ ! -x "$IBC_START" ]]; then
    log "ERROR: ibcstart.sh is not executable: $IBC_START"
    return 1
  fi

  # Kill anything old
  pkill -f -i "ibgateway" || true
  pkill -f -i "IBGateway" || true
  pkill -f -i "tws" || true
  sleep 3

  ensure_xvfb

  # Kill previous tmux session if it exists
  if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
    log "Killing existing tmux session: $TMUX_SESSION"
    tmux kill-session -t "$TMUX_SESSION"
  fi

  # Start tmux and immediately capture any startup errors
  log "Starting tmux session '$TMUX_SESSION' with IBC"
  tmux new-session -d -s "$TMUX_SESSION" bash -lc "
    export DISPLAY=:${DISPLAY_NUM}
    echo '[watchdog] DISPLAY='\"\$DISPLAY\"
    echo '[watchdog] running:' \"$IBC_START\" --gateway --tws-path \"$GATEWAY_DIR\" --ibc-path \"$IBC_DIR\" --ini \"$INI\"
    \"$IBC_START\" --gateway \
      --tws-path \"$GATEWAY_DIR\" \
      --ibc-path \"$IBC_DIR\" \
      --ini \"$INI\"
  " 2>>"$LOG"

  # Confirm tmux session exists
  if ! tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
    log "ERROR: tmux session '$TMUX_SESSION' was not created"
    tmux ls 2>&1 | tee -a "$LOG" || true
    return 1
  fi

  # Give it a moment and then dump pane output into the log
  sleep 2
  log "tmux pane output (startup):"
  tmux capture-pane -t "$TMUX_SESSION" -p 2>&1 | tail -n 200 | tee -a "$LOG" || true

  sleep "$BOOT_SLEEP"
}

# =========================
# Send 2FA alert
# =========================
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

  # Every few retries, dump useful diagnostics
  if (( i == 1 || i == 3 || i == 6 || i == 12 || i == RETRIES )); then
    log "Health still failing — checkpoint debug"
    debug_dump
    log "tmux pane output (checkpoint):"
    tmux capture-pane -t "$TMUX_SESSION" -p 2>&1 | tail -n 200 | tee -a "$LOG" || true
  fi

  sleep "$SLEEP_BETWEEN"
done

log "FAILED — likely 2FA required"
debug_dump
send_2fa_alert
exit 1
