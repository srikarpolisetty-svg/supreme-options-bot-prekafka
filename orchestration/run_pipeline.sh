#!/bin/bash
set -e

cd /home/ubuntu/supreme-options-bot-prekafka || exit 1

LOGDIR="/home/ubuntu/supreme-options-bot-prekafka/logs"
mkdir -p "$LOGDIR"

# 🔴 WRAPPER LOG: capture EVERYTHING (cron-safe) — do this BEFORE gates
exec >> "$LOGDIR/cronwrapper.log" 2>&1
set -x
echo "===== CRON START $(date) ====="
whoami
pwd

# -------------------------
# Skip THIRD-FRIDAY WEEK (monthly options expiry)
# -------------------------
TODAY=$(date +%Y-%m-%d)
YEAR=$(date +%Y)
MONTH=$(date +%m)

# First day of month
FIRST_DAY="${YEAR}-${MONTH}-01"

# Day of week of first day (1=Mon .. 7=Sun)
FIRST_DOW=$(date -d "$FIRST_DAY" +%u)

# Days to first Friday
DAYS_TO_FRIDAY=$(( (5 - FIRST_DOW + 7) % 7 ))

# First Friday
FIRST_FRIDAY=$(date -d "$FIRST_DAY +${DAYS_TO_FRIDAY} days" +%Y-%m-%d)

# Third Friday
THIRD_FRIDAY=$(date -d "$FIRST_FRIDAY +14 days" +%Y-%m-%d)

# Monday of third-Friday week
TF_WEEK_START=$(date -d "$THIRD_FRIDAY -$(date -d "$THIRD_FRIDAY" +%u) days +1 day" +%Y-%m-%d)

# Sunday of that week
TF_WEEK_END=$(date -d "$TF_WEEK_START +6 days" +%Y-%m-%d)

if [[ "$TODAY" >="$TF_WEEK_START" && "$TODAY" <="$TF_WEEK_END" ]]; then
  echo "[CRON][SKIP] Third-Friday week detected"
  echo "today=$TODAY third_friday=$THIRD_FRIDAY week=$TF_WEEK_START..$TF_WEEK_END"
  echo "===== CRON END (SKIPPED) $(date) ====="
  exit 0
fi

RUN_ID=$(date +"%Y-%m-%d_%H-%M-%S")
START_TS=$(date +%s)
echo "RUN_ID=$RUN_ID"

# Record code version (read-only, safe)
git rev-parse --short HEAD || true

for SHARD in {0..11}; do
  CLIENT_ID=$((1000 + SHARD))

  /home/ubuntu/optionsenv/bin/python -u masterfile.py \
    --shard $SHARD \
    --shards 12 \
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
