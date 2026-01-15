#!/bin/bash
set -e

cd /home/ubuntu/supreme-options-bot-prekafka || exit 1

LOGDIR="/home/ubuntu/supreme-options-bot-prekafka/logs"
mkdir -p "$LOGDIR"

# 🔴 WRAPPER LOG: capture EVERYTHING (cron-safe)
exec >> "$LOGDIR/cronwrapper.log" 2>&1
set -x
echo "===== CRON START $(date) ====="
whoami
pwd

RUN_ID=$(date +"%Y-%m-%d_%H-%M-%S")
START_TS=$(date +%s)
echo "RUN_ID=$RUN_ID"

# Record code version (read-only, safe)
git rev-parse --short HEAD || true

for SHARD in {0..7}; do
  CLIENT_ID=$((1000 + SHARD))

  /home/ubuntu/optionsenv/bin/python -u masterfile.py \
    --shard $SHARD \
    --shards 8 \
    --run_id "$RUN_ID" \
    --client_id $CLIENT_ID \
    >> "$LOGDIR/data_${SHARD}.log" 2>&1 &
done

wait

/home/ubuntu/optionsenv/bin/python -u masteringest.py \
  --run_id "$RUN_ID" >> "$LOGDIR/masteringest.log" 2>&1

END_TS=$(date +%s)
TOTAL_RUNTIME_SECONDS=$((END_TS - START_TS))
echo "TOTAL_RUNTIME_SECONDS=$TOTAL_RUNTIME_SECONDS"

echo "===== CRON END $(date) ====="
