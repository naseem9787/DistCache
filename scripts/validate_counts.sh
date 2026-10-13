#!/bin/bash
# Does the load generator's request count match what the SERVER says it processed?
# With logging enabled every SET produces exactly one log record, so the server's own
# wal_records counter is an independent count of the SETs it executed.
# loadgen runs with --warmup 0, so everything it sends is in the measured window, except the
# requests still in flight when it stops (at most clients x pipeline).
set -e
cd "$(dirname "$0")/.."
DATA_ROOT=${DATA_ROOT:-$HOME/.distcache-data}
mkdir -p "$DATA_ROOT"
PORT=7720
for P in 1 8; do
  dir=$(mktemp -d "$DATA_ROOT/validate-XXXXXX")
  taskset -c ${SERVER_CPUS:-0-5} ./build/distcache --mode sharded --threads 4 --port $PORT --dir "$dir" --fsync no --snapshot-mb 0 > /dev/null 2>&1 &
  srv=$!
  sleep 0.7
  out=$(taskset -c ${LOADGEN_CPUS:-6-11} ./build/distcache_loadgen --port $PORT --clients 50 --pipeline $P --duration 5 --warmup 0 --get-ratio 0 --label v)
  sets=$(echo "$out" | grep -o '"sets":[0-9]*' | cut -d: -f2)
  recs=$(redis-cli -p $PORT INFO | tr -d '\r' | awk -F: '/^wal_records/{print $2}')
  echo "pipeline $P: loadgen counted $sets SETs; server logged $recs records; difference $((recs - sets)) (in-flight at stop: <= $((50 * P)))"
  kill $srv; wait $srv 2>/dev/null || true
  rm -rf "$dir"
done
