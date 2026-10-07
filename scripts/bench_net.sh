#!/bin/bash
# End-to-end benchmark: real server + redis-benchmark over TCP (loopback).
# Usage: scripts/bench_net.sh [outfile.csv]     (needs build/distcache and redis-benchmark)
#
# For each server configuration it runs SET and GET with random keys from a 100k keyspace,
# 64-byte values, at several client counts, with and without pipelining. Every run is
# kept in the CSV. Client and server share this machine and compete for CPU: see README.
#
# Request counts are chosen so every run lasts several seconds. redis-benchmark reports
# time in coarse steps, so runs under ~2 s give quantized, untrustworthy throughput.
#
# Environment overrides (used to re-run a subset with longer runs):
#   COMBOS="50 16"   only these "clients pipeline" combos (comma separated list)
#   NREQ=12000000    requests per run for clients>=10 on every config
#   APPEND=1         append to the CSV instead of replacing it
set -e
cd "$(dirname "$0")/.."
OUT=${1:-docs/benchmarks/network.csv}
REPS=${REPS:-3}
PORT=7400
COMBOS=${COMBOS:-"1 1,10 1,50 1,200 1,50 16"}
mkdir -p "$(dirname "$OUT")"
if [ "${APPEND:-0}" != "1" ]; then
  echo "config,clients,pipeline,test,requests,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms,rep" > "$OUT"
fi

# name | server arguments | requests for c>=10 (slow single-thread server needs fewer)
CONFIGS=(
  "single|--mode single --port $PORT|400000"
  "global-4w|--mode global --threads 4 --port $PORT|3000000"
  "sharded-4w|--mode sharded --shards 64 --threads 4 --port $PORT|3000000"
)

for cfg in "${CONFIGS[@]}"; do
  IFS='|' read -r name args nfast <<< "$cfg"
  [ -n "$NREQ" ] && nfast=$NREQ
  ./build/distcache $args > /dev/null 2>&1 &
  srv=$!
  sleep 0.5
  IFS=',' read -ra combo_list <<< "$COMBOS"
  for combo in "${combo_list[@]}"; do
    set -- $combo; c=$1; p=$2
    n=$nfast; [ "$c" = "1" ] && n=100000
    for rep in $(seq 1 $REPS); do
      redis-benchmark -p $PORT -t set,get -n $n -c $c -P $p -d 64 -r 100000 --threads 4 --csv 2>/dev/null \
        | grep -v '^"test"' | tr -d '"' \
        | awk -F, -v n="$name" -v c=$c -v p=$p -v q=$n -v r=$rep '{printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", n,c,p,$1,q,$2,$3,$4,$5,$6,$7,$8,r}' >> "$OUT"
    done
  done
  kill $srv; wait $srv 2>/dev/null || true
  echo "finished $name"
done
echo "wrote $OUT ($(($(wc -l < "$OUT") - 1)) rows)"
