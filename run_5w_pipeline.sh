#!/bin/bash
set -e

cd /home/ubuntu/supreme-options-bot-prekafka || exit 1

LOGDIR="/home/ubuntu/supreme-options-bot-prekafka/logs"
mkdir -p "$LOGDIR"

# 🔴 WRAPPER LOG: capture EVERYTHING (cron-safe)
exec >> "$LOGDIR/cron_5w_wrapper.log" 2>&1
set -x
echo "===== CRON START $(date) ====="
whoami
pwd

RUN_ID=$(date +"%Y-%m-%d_%H-%M-%S")
echo "RUN_ID=$RUN_ID"

# Record code version (read-only, safe)
git rev-parse --short HEAD || true

for SHARD in {0..3}; do
  CLIENT_ID=$((1000 + SHARD))

  /home/ubuntu/optionsenv/bin/python -u fiveweekdatabase_masterfile.py \
    --shard $SHARD \
    --shards 4 \
    --run_id "$RUN_ID" \
    --client_id $CLIENT_ID \
    >> "$LOGDIR/5week_${SHARD}.log" 2>&1 &
done


wait

/home/ubuntu/optionsenv/bin/python -u fiveweekdatabase_masteringest.py \
  --run_id "$RUN_ID" >> "$LOGDIR/5week_master.log" 2>&1

echo "===== CRON END $(date) ====="
