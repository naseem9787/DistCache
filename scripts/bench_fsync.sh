#!/bin/bash
# What does durability cost? SET throughput/latency for each fsync policy vs no persistence.
# Usage: scripts/bench_fsync.sh [outfile.csv]   (needs build/distcache and redis-benchmark)
#
# Same server (sharded, 4 workers) every time; only the persistence setting changes.
# Every run lasts several seconds (request counts are per config because "always" is far
# slower). The data directory is on the local filesystem of this machine: on WSL2/virtual
# disks an fsync may be acknowledged by the hypervisor before it reaches physical media, so
# treat the absolute "always" numbers as indicative only.
set -e
cd "$(dirname "$0")/.."
OUT=${1:-docs/benchmarks/fsync.csv}
# MUST be a real disk: /tmp is often a RAM-backed tmpfs where fsync is a no-op and a big
# log can fill it (then the server, by design, aborts instead of acknowledging lost writes).
DATA_ROOT=${DATA_ROOT:-$HOME/.distcache-data}
mkdir -p "$DATA_ROOT"
fs_type=$(stat -f -c %T "$DATA_ROOT")
if [ "$fs_type" = "tmpfs" ] || [ "$fs_type" = "ramfs" ]; then echo "refusing to benchmark fsync on $fs_type: $DATA_ROOT" >&2; exit 2; fi
echo "data directory: $DATA_ROOT ($fs_type)"
cleanup() { pkill -f "distcache --mode sharded --threads 4 --port 7500" 2>/dev/null || true; rm -rf "$DATA_ROOT"/bench-*; }
trap cleanup EXIT
REPS=${REPS:-3}
PORT=7500
mkdir -p "$(dirname "$OUT")"
echo "config,clients,pipeline,requests,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms,rep,wal_fsyncs,wal_records" > "$OUT"

# name | extra server args | requests at pipeline 1 | requests at pipeline 16
CONFIGS=(
  "no-persistence||3000000|12000000"
  "fsync-no|--fsync no|3000000|12000000"
  "fsync-everysec|--fsync everysec|3000000|12000000"
  "fsync-always|--fsync always|${ALWAYS_N1:-200000}|${ALWAYS_N16:-3000000}"
)

for cfg in "${CONFIGS[@]}"; do
  IFS='|' read -r name extra n1 n16 <<< "$cfg"
  for combo in "50 1" "50 16"; do
    set -- $combo; c=$1; p=$2
    n=$n1; [ "$p" = "16" ] && n=$n16
    # fresh server + empty data directory per combination, so the INFO counters are per combination
    dir=""; args="$extra"
    if [ -n "$extra" ]; then
      dir=$(mktemp -d "$DATA_ROOT/bench-XXXXXX")
      args="$extra --dir $dir --snapshot-mb 0"
    fi
    ./build/distcache --mode sharded --threads 4 --port $PORT $args > /dev/null 2>&1 &
    srv=$!
    sleep 0.7
    for rep in $(seq 1 $REPS); do
      line=$(redis-benchmark -p $PORT -t set -n $n -c $c -P $p -d 64 -r 100000 --threads 4 --csv 2>/dev/null | grep -v '^"test"' | tr -d '"' | head -1)
      stats=$(redis-cli -p $PORT INFO | tr -d '\r' | awk -F: '/^wal_fsyncs/{f=$2} /^wal_records/{r=$2} END{print f","r}')
      [ -z "$stats" ] && stats=","
      echo "$line" | awk -F, -v n="$name" -v c=$c -v p=$p -v q=$n -v r=$rep -v s="$stats" '{printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", n,c,p,q,$2,$3,$4,$5,$6,$7,$8,r,s}' >> "$OUT"
    done
    kill $srv; wait $srv 2>/dev/null || true
    if [ -n "$dir" ]; then rm -rf "$dir"; fi
  done
  echo "finished $name"
done
echo "wrote $OUT ($(($(wc -l < "$OUT") - 1)) rows)"
