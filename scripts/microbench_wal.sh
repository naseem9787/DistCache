#!/bin/bash
# WAL component microbenchmarks: one component at a time, 3 repetitions each, raw lines kept.
# Usage: scripts/microbench_wal.sh [outfile]      (needs build/bench_wal, Release build)
#
# ext4 = a directory on the real disk, tmpfs = /tmp (RAM). Comparing the two isolates what the
# disk costs. The tests use fixed record counts so the in-memory log buffer stays bounded.
set -e
cd "$(dirname "$0")/.."
OUT=${1:-docs/benchmarks/phase6/wal-microbench.txt}
BIN=./build/bench_wal
EXT4=${DATA_ROOT:-$HOME/.distcache-data}
TMPFS=/tmp
REPS=${REPS:-3}
mkdir -p "$(dirname "$OUT")" "$EXT4"
fs_type=$(stat -f -c %T "$EXT4")
[ "$fs_type" = "tmpfs" ] && { echo "DATA_ROOT must not be tmpfs" >&2; exit 2; }
: > "$OUT"
run() { for r in $(seq 1 $REPS); do echo "rep=$r,$($BIN "$@")" >> "$OUT"; done; }

echo "# environment: $(uname -r), $(nproc) CPUs, ext4 dir=$EXT4 ($fs_type), tmpfs dir=$TMPFS" >> "$OUT"

# 1. pure CPU pieces
run crc 110 20000000
run crc 4096 2000000
run encode 64 5000000
run encode 1024 2000000

# 2. appending to the log buffer, contention vs threads (policy "no": no fsync in the way)
for t in 1 2 4 8; do
  run append $t 500000 $EXT4 no
  run append $t 500000 $TMPFS no
done

# 3. the disk itself: sequential write throughput
for dir in $EXT4 $TMPFS; do
  for chunk in 64 1024; do
    run write $dir $chunk 512 0
    run write $dir $chunk 512 1
  done
done

# 4. the whole engine + log path, no network
for cfg in "single 1" "global 4" "sharded 1" "sharded 4" "sharded 8"; do
  set -- $cfg
  run engine $1 $2 500000 none $EXT4
  run engine $1 $2 500000 no $EXT4
  run engine $1 $2 500000 no $TMPFS
  run engine $1 $2 500000 everysec $EXT4
done
echo "wrote $OUT ($(grep -vc '^#' "$OUT") measurements)"
