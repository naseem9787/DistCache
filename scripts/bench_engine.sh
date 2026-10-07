#!/bin/bash
# In-process locking benchmark: threads hit the Engine directly (no network).
# Usage: scripts/bench_engine.sh [outfile.csv]   (needs build/bench_engine; Release build)
# Each configuration runs REPS times; the CSV keeps every run so the spread is visible.
set -e
cd "$(dirname "$0")/.."
BIN=./build/bench_engine
OUT=${1:-docs/benchmarks/engine.csv}
SECS=${SECS:-3}
REPS=${REPS:-3}
mkdir -p "$(dirname "$OUT")"
echo "engine,threads,shards,seconds,total_ops,ops_per_sec,read_pct,rep" > "$OUT"

run() {  # engine threads read_pct shards
  for rep in $(seq 1 $REPS); do
    line=$($BIN "$1" "$2" "$SECS" 100000 "$3" "$4")
    echo "$line,$3,$rep" >> "$OUT"
  done
}

for rp in 90 50; do                       # read-heavy cache vs write-heavy
  run none 1 $rp 16
  for t in 1 2 4 8 12; do
    run global  $t $rp 16
    run sharded $t $rp 16
  done
done
# Does the shard count matter? 8 threads, 90% reads.
for s in 1 2 4 8 16 64 256; do run sharded 8 90 $s; done
echo "wrote $OUT ($(($(wc -l < "$OUT") - 1)) runs)"
