#!/usr/bin/env bash
set -euo pipefail

# =========================
# CONFIG
# =========================
LOCKFILE="/tmp/ib_watchdog.lock"
LOG="$HOME/ib_watchdog/watchdog.log"
mkdir -p "$(dirname "$LOG")"

PYTHON_BIN="/home/ubuntu/optionsenv/bin/python"

# --- health check ---
HOST="127.0.0.1"
PORT=4002
CLIENT_ID=997
TIMEOUT=3

# --- tmux / IBC launcher ---
TMUX_SESSION="ib"
DISPLAY_NUM=1
IBC_START="/home/ubuntu/IBC/target/IBCLinux/scripts/ibcstart.sh"
INI="/home/ubuntu/IBC/config.ini"
TWS_VERSION="1043"
TMUX_KEEPALIVE_SECONDS=600

# --- retries ---
BOOT_SLEEP=10
RETRIES=12
SLEEP_BETWEEN=5

# --- alert ---
ALERT_PYTHON_PATH="/home/ubuntu/supreme-options-bot-prekafka"

# --- Xvfb (headless display) ---
XVFB_BIN="/usr/bin/Xvfb"
XVFB_SCREEN="0"
XVFB_RES="1920x1080x24"
XVFB_PIDFILE="/tmp/xvfb_${DISPLAY_NUM}.pid"
XVFB_START_WAIT=1

# =========================
# HELPERS
# =========================
ts()  { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }

api_ok() {
  "$PYTHON_BIN" - <<PY >/dev/null 2>&1
from ib_insync import IB
import sys

HOST = "${HOST}"
PORT = ${PORT}
CLIENT_ID = ${CLIENT_ID}
TIMEOUT = ${TIMEOUT}

ib = IB()
try:
    ok = ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=TIMEOUT)
    if not ok:
        sys.exit(2)
    ib.reqCurrentTime()
    sys.exit(0)
except Exception:
    sys.exit(2)
finally:
    try:
        ib.disconnect()
    except Exception:
        pass
PY
}

send_fail_alert() {
  log "Sending failure alert"
  "$PYTHON_BIN" - <<PY
import sys
sys.path.append("${ALERT_PYTHON_PATH}")
from message import send_text

send_text(
    "⚠️ IB Gateway startup failed.\\n\\n"
    "Watchdog tried to start Xvfb + IBC in tmux, but the IB API did not become healthy.\\n\\n"
    "What to check:\\n"
    f"• tmux session: ${TMUX_SESSION}\\n"
    f"• DISPLAY: :${DISPLAY_NUM} (Xvfb)\\n"
    "• IB Gateway login / 2FA screen (may require manual step)\\n"
    "• IBC config.ini values\\n\\n"
    f"Action: tmux attach -t ${TMUX_SESSION}"
)
PY
}

# -------------------------
# Ensure DISPLAY exists using Xvfb (start if missing)
# -------------------------
ensure_display() {
  local xvfb_log="$HOME/ib_watchdog/xvfb_${DISPLAY_NUM}.log"
  mkdir -p "$(dirname "$xvfb_log")"

  if pgrep -af "Xvfb :${DISPLAY_NUM}\b" >/dev/null 2>&1; then
    log "Display :${DISPLAY_NUM} already present (Xvfb)."
    return 0
  fi

  # stale lock/socket cleanup if no process is running
  if [[ -e "/tmp/.X${DISPLAY_NUM}-lock" || -e "/tmp/.X11-unix/X${DISPLAY_NUM}" ]]; then
    log "Found X lock/socket for :${DISPLAY_NUM} but no Xvfb process. Cleaning stale files."
    rm -f "/tmp/.X${DISPLAY_NUM}-lock" "/tmp/.X11-unix/X${DISPLAY_NUM}" >>"$LOG" 2>&1 || true
  fi

  if [[ ! -x "$XVFB_BIN" ]]; then
    log "ERROR: Xvfb not found at $XVFB_BIN (try: sudo apt-get install -y xvfb)"
    return 1
  fi

  log "Display :${DISPLAY_NUM} missing — starting Xvfb (:${DISPLAY_NUM})"
  : >"$xvfb_log" || true

  set +e
  "$XVFB_BIN" ":${DISPLAY_NUM}" \
    -screen 0 "$XVFB_RES" \
    -ac +extension RANDR -nolisten tcp \
    >>"$xvfb_log" 2>&1 &
  local xvfb_pid=$!
  set -e

  echo "$xvfb_pid" >"$XVFB_PIDFILE" || true
  sleep "$XVFB_START_WAIT"

  if kill -0 "$xvfb_pid" 2>/dev/null && pgrep -af "Xvfb :${DISPLAY_NUM}\b" >/dev/null 2>&1; then
    log "Xvfb started successfully on :${DISPLAY_NUM} (pid=$xvfb_pid)"
    return 0
  fi

  log "ERROR: Xvfb failed to start on :${DISPLAY_NUM}. Last 80 lines of $xvfb_log:"
  tail -n 80 "$xvfb_log" | tee -a "$LOG" || true
  return 1
}



start_ibc_in_tmux() {
  log "Starting IBC in new tmux session '$TMUX_SESSION' (DISPLAY=:${DISPLAY_NUM})"

  if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
    log "Killing old tmux session '$TMUX_SESSION'"
    tmux kill-session -t "$TMUX_SESSION" || true
  fi

  tmux new-session -d -s "$TMUX_SESSION" bash -lc "
    set +e
    export DISPLAY=:${DISPLAY_NUM}

    echo '[tmux] DISPLAY='\"\$DISPLAY\"
    echo '[tmux] Starting IB Gateway via IBC'
    echo \"[tmux] cmd: ${IBC_START} ${TWS_VERSION} --gateway --ibc-ini=${INI}\"

    \"${IBC_START}\" \"${TWS_VERSION}\" --gateway --ibc-ini=\"${INI}\"
    ec=\$?

    echo \"[tmux] ibcstart exited ec=\$ec\"
    echo \"[tmux] keeping tmux alive for ${TMUX_KEEPALIVE_SECONDS}s\"
    sleep ${TMUX_KEEPALIVE_SECONDS}
  "
}

# =========================
# MAIN (with lock)
# =========================
(
  flock -n 9 || exit 0

  log "Watchdog start"

  # 1) healthy? done
  if api_ok; then
    log "OK — IB API healthy"
    exit 0
  fi

  # 2) ensure headless display exists
  if ! ensure_display; then
    log "FAILED — could not ensure Xvfb display"
    send_fail_alert
    exit 1
  fi

  # 3) unhealthy → start IBC
  log "IB API unhealthy — launching IBC"
  start_ibc_in_tmux

  sleep "$BOOT_SLEEP"

  # 4) retry health
  for ((i=1; i<=RETRIES; i++)); do
    if api_ok; then
      log "RECOVERED — IB API healthy ($i/$RETRIES)"
      exit 0
    fi
    log "Waiting for API... ($i/$RETRIES)"
    sleep "$SLEEP_BETWEEN"
  done

  # 5) failed → alert
  log "FAILED — API still unhealthy after retries"
  send_fail_alert
  exit 1

) 9>"$LOCKFILE"
