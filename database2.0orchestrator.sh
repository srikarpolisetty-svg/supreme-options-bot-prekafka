#!/bin/bash

# move into the folder where this .sh file lives
cd "$(dirname "$0")"

# ---- CONFIG ----
RUN_ID="r001"
LOG_DIR="logs"
SCRIPT="databentodatabasebackfillworkingversion.py"
# ----------------

mkdir -p $LOG_DIR

echo "Starting options pipeline | run_id=$RUN_ID"

# single normal run, still logging
python $SCRIPT \
  > $LOG_DIR/run_$RUN_ID.log 2>&1

echo "Run finished."