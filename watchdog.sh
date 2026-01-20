#!/usr/bin/env bash
set -euo pipefail

# =========================
# CONFIG
# =========================
LOCKFILE="/tmp/ib_gateway_watchdog.lock"

PORT=4002
TMUX_SESSION="ib"
DISPLAY_NUM=1

ROOT="$HOME/supreme-options-bot-prekafka"
LOG="$ROOT/logs/watchdog.log"

IBC_START="/home/ubuntu/IBC/target/IBCLinux/scripts/ibcstart.sh"
INI="/home/ubuntu/IBC/config.ini"
TWS_VERSION="1043"

PYTHON_BIN="/home/ubuntu/optionsenv/bin/python"
ALERT_PYTHON_PATH="$ROOT"

BOOT_SLEEP=12
RETRIES=12
SLEEP_BETWEEN=5
TMUX_KEEPALIVE_SECONDS=600

# --- NEW: VNC auto-start ---
VNC_CMD="vncserver"
VNC_START_WAIT=5

# =========================
# HELPERS
# =========================
ts()  { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }
mkdir -p "$(dirname "$LOG")"

# -------------------------
# IB API health check
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
    try: ib.disconnect()
    except Exception: pass
PY
}

# -------------------------
# Send failure alert
# -------------------------
send_fail_alert() {
  log "Sending failure alert"
  "$PYTHON_BIN" - <<PY
import sys
sys.path.append("${ALERT_PYTHON_PATH}")
from message import send_text
send_text(
    "⚠️ IB Gateway watchdog failed.\n\n"
    "Tried starting IBC in tmux but API did not come up on port ${PORT}.\n"
    "Please check VNC (:${DISPLAY_NUM}) and tmux session '${TMUX_SESSION}'."
)
PY
}

# -------------------------
# Ensure TigerVNC display exists
# If missing, attempt to start it.
# -------------------------
ensure_display() {
  if pgrep -af "Xtigervnc :${DISPLAY_NUM}\b|Xvnc :${DISPLAY_NUM}\b|vncserver :${DISPLAY_NUM}\b" >/dev/null 2>&1; then
    log "Display :${DISPLAY_NUM} present (TigerVNC)."
    return 0
  fi

  log "Display :${DISPLAY_NUM} not found — attempting to start TigerVNC"
  set +e
  ${VNC_CMD} :${DISPLAY_NUM} >>"$LOG" 2>&1
  local ec=$?
  set -e

  if [[ $ec -ne 0 ]]; then
    log "ERROR: Failed to start vncserver :${DISPLAY_NUM} (exit=$ec)"
    return 1
  fi

  sleep "${VNC_START_WAIT}"

  if pgrep -af "Xtigervnc :${DISPLAY_NUM}\b|Xvnc :${DISPLAY_NUM}\b|vncserver :${DISPLAY_NUM}\b" >/dev/null 2>&1; then
    log "TigerVNC started successfully on :${DISPLAY_NUM}"
    return 0
  fi

  log "ERROR: vncserver started but display :${DISPLAY_NUM} not detected"
  return 1
}

# -------------------------
# Is tmux session alive?
# -------------------------
tmux_ok() {
  tmux has-session -t "$TMUX_SESSION" 2>/dev/null
}

# -------------------------
# Start (or restart) IBC inside tmux
# -------------------------
start_ibc_in_tmux() {
  log "Starting IBC in tmux session '$TMUX_SESSION' (DISPLAY=:${DISPLAY_NUM})"

  local start_log="$ROOT/logs/tmux_start_${TMUX_SESSION}.log"
  local pane_log="$ROOT/logs/tmux_pane_${TMUX_SESSION}.log"
  : >"$start_log" || true
  : >"$pane_log" || true

  # Kill old session if it exists
  if tmux_ok; then
    log "Killing existing tmux session '$TMUX_SESSION'"
    tmux kill-session -t "$TMUX_SESSION" >>"$start_log" 2>&1 || true
  fi

  # Start tmux running IBC. Keep session alive for debugging.
  set +e
  tmux new-session -d -s "$TMUX_SESSION" bash -lc "
    set +e
    export DISPLAY=:${DISPLAY_NUM}
    export XAUTHORITY=\$HOME/.Xauthority

    echo '[watchdog] DISPLAY='\"\$DISPLAY\"
    echo "[watchdog] cmd: ${IBC_START} ${TWS_VERSION} --gateway --ibc-ini=${INI}"
    ec=\$?
    echo \"[watchdog] ibcstart exited ec=\$ec\"
    echo \"[watchdog] keeping tmux alive for ${TMUX_KEEPALIVE_SECONDS}s...\"
    sleep ${TMUX_KEEPALIVE_SECONDS}
  " >>"$start_log" 2>&1
  local tmux_ec=$?
  set -e

  log "tmux new-session exit_code=$tmux_ec"
  tail -n 80 "$start_log" | tee -a "$LOG" || true

  if [[ $tmux_ec -ne 0 ]]; then
    log "ERROR: tmux failed to start session"
    return 1
  fi

  if ! tmux_ok; then
    log "ERROR: tmux session not found after start"
    tmux ls 2>&1 | tee -a "$LOG" || true
    return 1
  fi

  # Pipe pane output to a file + show a snippet
  tmux pipe-pane -t "${TMUX_SESSION}:0.0" -o "cat >> \"$pane_log\"" >>"$start_log" 2>&1 || true
  sleep 2
  log "tmux pane output (startup tail):"
  tmux capture-pane -t "${TMUX_SESSION}:0.0" -p 2>&1 | tail -n 120 | tee -a "$LOG" || true

  return 0
}

# =========================
# MAIN (lock scoped so tmux cannot inherit it)
# =========================
(
  flock -n 9 || exit 0

  log "Watchdog start"

  # 1) If API already healthy, done
  if api_ok; then
    log "OK — IB API healthy"
    exit 0
  fi

  # 2) Ensure VNC display exists (auto-start if missing)
  if ! ensure_display; then
    send_fail_alert
    exit 1
  fi

  # 3) Check tmux + start IBC
  log "IB API unhealthy — starting IBC"
  if ! start_ibc_in_tmux; then
    send_fail_alert
    exit 1
  fi

  sleep "$BOOT_SLEEP"

  # 4) Retry health a few times
  for ((i=1; i<=RETRIES; i++)); do
    if api_ok; then
      log "RECOVERED — IB API healthy ($i/$RETRIES)"
      exit 0
    fi
    log "Waiting for API... ($i/$RETRIES)"
    sleep "$SLEEP_BETWEEN"
  done

  # 5) Failed
  log "FAILED — API still unhealthy after retries"
  log "tmux pane output (failure tail):"
  tmux capture-pane -t "${TMUX_SESSION}:0.0" -p 2>&1 | tail -n 200 | tee -a "$LOG" || true
  send_fail_alert
  exit 1

) 9>"$LOCKFILE"
