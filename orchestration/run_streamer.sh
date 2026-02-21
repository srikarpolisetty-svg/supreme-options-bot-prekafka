#!/usr/bin/env bash
set -euo pipefail

LOCK="/tmp/live_streamer.lock"
LOG="logs/live_streamer.log"
mkdir -p logs

# Only one instance allowed.
exec flock -n "$LOCK" bash -c "
  echo \"[$(date -u)] starting streamer\" >> \"$LOG\"
  python -u keepstreaming.py streamer >> \"$LOG\" 2>&1
"

