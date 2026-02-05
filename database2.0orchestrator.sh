#!/bin/bash

# move into the folder where this .sh file lives
cd "$(dirname "$0")"

# ---- CONFIG ----
RUN_ID="r001"
N_SHARDS=4
LOG_DIR="logs"
# ----------------

mkdir -p $LOG_DIR

echo "Starting options pipeline | run_id=$RUN_ID | shards=$N_SHARDS"

for (( i=0; i<$N_SHARDS; i++ ))
do
  echo "Launching shard $i"
  python databentodatabasetwo.py \
    --run-id $RUN_ID \
    --shard-id $i \
    --n-shards $N_SHARDS \
    > $LOG_DIR/shard_$i.log 2>&1 &
done

echo "All shards launched. Waiting for completion..."
wait

echo "All shards finished."
