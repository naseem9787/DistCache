#!/bin/bash
# Cross-check distcache_loadgen against redis-benchmark on the same server and workload.
# They are independent implementations, so agreement (within run-to-run noise) is evidence that the
# load generator is not itself distorting the measurement. Differences in latency are expected to
# be small but not zero: the two tools define per-request latency a little differently (see
# bench/loadgen.cpp for ours).
#
# Usage: scripts/validate_loadgen.sh [reps]
set -e
cd "$(dirname "$0")/.."
REPS=${1:-3}
PORT=7710
SCPU=${SERVER_CPUS:-0-5}
LCPU=${LOADGEN_CPUS:-6-11}
echo "server: sharded, 4 workers, pinned to CPUs $SCPU; load tools pinned to CPUs $LCPU"
printf "%-26s %-10s %12s %10s %10s %10s\n" "workload" "tool" "requests/s" "p50 us" "p99 us" "tool cpu"
for P in 1 16; do
  for rep in $(seq 1 $REPS); do
    taskset -c $SCPU ./build/distcache --mode sharded --threads 4 --port $PORT > /dev/null 2>&1 &
    srv=$!
    sleep 0.5
    # --- our tool: 50 clients, SET only, 8 s measured
    out=$(taskset -c $LCPU ./build/distcache_loadgen --port $PORT --clients 50 --threads 4 --pipeline $P \
          --duration 8 --warmup 1 --get-ratio 0 --keyspace 100000 --value-size 64 --label v)
    rps=$(echo "$out" | grep -o '"rps":[0-9.]*' | cut -d: -f2)
    p50=$(echo "$out" | grep -o '"p50":[0-9.]*' | cut -d: -f2)
    p99=$(echo "$out" | grep -o '"p99":[0-9.]*' | cut -d: -f2)
    printf "%-26s %-10s %12.0f %10.0f %10.0f %10s\n" "SET only, 50 cl, P$P" "loadgen" "$rps" "$p50" "$p99" "-"
    # --- redis-benchmark: request count sized for roughly 8-10 s
    n=2400000; [ $P = 16 ] && n=14000000
    csv=$(taskset -c $LCPU redis-benchmark -p $PORT -t set -n $n -c 50 -P $P -d 64 -r 100000 --threads 4 --csv 2>/dev/null | grep -v '^"test"' | tr -d '"' | head -1)
    rps2=$(echo "$csv" | cut -d, -f2); p50b=$(echo "$csv" | cut -d, -f5); p99b=$(echo "$csv" | cut -d, -f7)
    printf "%-26s %-10s %12.0f %10.0f %10.0f %10s\n" "SET only, 50 cl, P$P" "redis-bm" "$rps2" "$(echo "$p50b*1000" | bc)" "$(echo "$p99b*1000" | bc)" "-"
    kill $srv; wait $srv 2>/dev/null || true
  done
done
