#!/bin/bash
# How long does startup recovery take? Log replay vs snapshot load, for the same data.
# Usage: scripts/bench_recovery.sh [num_requests]   (needs build/distcache and redis-benchmark)
set -e
cd "$(dirname "$0")/.."
N=${1:-1000000}
DATA_ROOT=${DATA_ROOT:-$HOME/.distcache-data}
mkdir -p "$DATA_ROOT"
fs_type=$(stat -f -c %T "$DATA_ROOT")
if [ "$fs_type" = "tmpfs" ]; then echo "refusing to run on tmpfs" >&2; exit 2; fi
dir=$(mktemp -d "$DATA_ROOT/recovery-XXXXXX")
PORT=7600
trap 'pkill -f "distcache --mode sharded --threads 4 --port $PORT" 2>/dev/null; rm -rf "$dir"' EXIT

elapsed=0
start_and_time() {  # sets $srv and $elapsed = seconds from process start until it answers PING
  # (not run in a subshell: $srv must reach the caller so the server can be stopped later)
  local t0 t1
  t0=$(date +%s.%N)
  ./build/distcache --mode sharded --threads 4 --port $PORT --dir "$dir" --fsync everysec --snapshot-mb 0 > /tmp/recovery-start.log 2>&1 &
  srv=$!
  until redis-cli -p $PORT PING 2>/dev/null | grep -q PONG; do sleep 0.01; done
  t1=$(date +%s.%N)
  elapsed=$(echo "$t1 - $t0" | bc)
}
stop_clean() { kill -TERM $srv; wait $srv 2>/dev/null || true; }

# 1. load data (random keys from a keyspace the size of N, so many are overwrites)
./build/distcache --mode sharded --threads 4 --port $PORT --dir "$dir" --fsync everysec --snapshot-mb 0 > /dev/null 2>&1 &
srv=$!
sleep 0.7
redis-benchmark -p $PORT -t set -n $N -c 50 -P 16 -d 64 -r $N -q > /dev/null 2>&1
keys=$(redis-cli -p $PORT DBSIZE)
stop_clean
log_bytes=$(du -cb "$dir"/wal-*.log | tail -1 | cut -f1)
echo "dataset: $N SET commands -> $keys distinct keys, log = $((log_bytes / 1048576)) MiB"

# 2. recovery from the log alone
start_and_time; t=$elapsed
echo "recovery from log only      : ${t}s   ($(grep -o 'recovered:.*' /tmp/recovery-start.log | head -1))"
got=$(redis-cli -p $PORT DBSIZE)
[ "$got" = "$keys" ] && echo "  key count after recovery matches: $got" || echo "  MISMATCH: $got vs $keys"

# 3. snapshot, then recovery from the snapshot
redis-cli -p $PORT SAVE > /dev/null
snap_bytes=$(du -cb "$dir"/snapshot-*.dcs | tail -1 | cut -f1)
stop_clean
echo "snapshot file               : $((snap_bytes / 1048576)) MiB"
start_and_time; t=$elapsed
echo "recovery from snapshot      : ${t}s   ($(grep -o 'recovered:.*' /tmp/recovery-start.log | head -1))"
got=$(redis-cli -p $PORT DBSIZE)
[ "$got" = "$keys" ] && echo "  key count after recovery matches: $got" || echo "  MISMATCH: $got vs $keys"
stop_clean
rm -f /tmp/recovery-start.log
