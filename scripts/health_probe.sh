#!/bin/bash
# Is the machine in a representative state right now? Run before (and after) any benchmark session.
#
# The reference machine is a WSL2 VM whose memory and disk performance changed by an order of
# magnitude between sessions hours apart (page-cache writes: 4 GB/s one hour, 190 MB/s the next).
# Persistent-configuration benchmarks are sensitive to that, so every session records this probe.
# The thresholds were taken from the healthy state observed at the start of the project.
#
# Usage: scripts/health_probe.sh        (needs build/bench_wal)   exit code 0 = healthy, 3 = degraded
set -e
cd "$(dirname "$0")/.."
BIN=./build/bench_wal
DATA_ROOT=${DATA_ROOT:-$HOME/.distcache-data}
mkdir -p "$DATA_ROOT"

best_of() {  # run a command 3 times, print the best MB_per_s_write_only
  local best=0
  for i in 1 2 3; do
    v=$("$@" | grep -o 'MB_per_s_write_only=[0-9]*' | cut -d= -f2)
    [ "${v:-0}" -gt "$best" ] && best=$v
  done
  echo $best
}

tmpfs_mb=$(best_of $BIN write /tmp 1024 256 0)
disk_mb=$(best_of $BIN write "$DATA_ROOT" 1024 256 0)
crc_ns=$($BIN crc 110 5000000 | grep -o 'ns_per_call=[0-9.]*' | cut -d= -f2)
mem_free=$(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo)
dirty_mb=$(awk '/^Dirty:/{print int($2/1024)}' /proc/meminfo)

status=0
flag() { echo "  $1"; status=3; }
echo "health probe $(date -Is)"
printf "  tmpfs write (page cache, 1 MiB chunks) : %6s MB/s   (healthy >= 1500)\n" "$tmpfs_mb"
printf "  ext4 write  (page cache, 1 MiB chunks) : %6s MB/s   (healthy >= 1000)\n" "$disk_mb"
printf "  crc32 of a 110-byte record             : %6s ns     (healthy <= 80)\n" "$crc_ns"
printf "  MemAvailable / Dirty                   : %6s MiB / %s MiB\n" "$mem_free" "$dirty_mb"
[ "$tmpfs_mb" -lt 1500 ] && flag "DEGRADED: memory/page-cache writes are slow; persistent benchmarks will be disk/memory bound"
[ "$disk_mb" -lt 1000 ] && flag "DEGRADED: disk page-cache writes are slow"
awk -v c="$crc_ns" 'BEGIN{exit !(c > 80)}' && flag "DEGRADED: CPU looks throttled (crc32 slow)"
[ "$dirty_mb" -gt 200 ] && flag "NOTE: $dirty_mb MiB dirty pages still queued; run 'sync' and wait"
[ $status -eq 0 ] && echo "  => healthy"
exit $status
