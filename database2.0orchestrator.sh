#!/bin/bash

# ---- CONFIG ----
RUN_ID="r001"
N_SHARDS=8
# ----------------

echo "Starting options pipeline | run_id=$RUN_ID | shards=$N_SHARDS"

for (( i=0; i<$N_SHARDS; i++ ))
do
  echo "Launching shard $i"
  python databentodatabasetwo.py --run-id $RUN_ID --shard-id $i --n-shards $N_SHARDS &
done

echo "All shards launched. Waiting for completion..."
wait

echo "All shards finished."
