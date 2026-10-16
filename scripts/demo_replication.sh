#!/bin/bash
# A scripted tour of replication and automatic failover: one shard = 1 primary + 2 replicas behind the router.
# Usage: scripts/demo_replication.sh      (needs build/distcache, build/distcache_router, redis-cli)
set -e
cd "$(dirname "$0")/.."
BASE=${BASE_PORT:-7800}
P=$((BASE + 1)); R1=$((BASE + 2)); R2=$((BASE + 3))        # client ports; replication ports are +10
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
run() { echo "\$ $*"; "$@"; }

./build/distcache --port $P  --repl-port $((P + 10))  --repl-require-lease --repl-sync 1 > /dev/null 2>&1 & pids+=($!)
sleep 0.3
for r in $R1 $R2; do
  ./build/distcache --port $r --repl-port $((r + 10)) --repl-require-lease --repl-sync 1 --replicaof 127.0.0.1:$((P + 10)) > /dev/null 2>&1 & pids+=($!)
done
sleep 0.5
./build/distcache_router --port $BASE --node a=127.0.0.1:$P --replica a=127.0.0.1:$R1 --replica a=127.0.0.1:$R2 \
    --lease-ms 500 --probe-ms 100 --probe-timeout-ms 200 --guard-ms 100 --fail-after 3 --sync-replicas 1 > /dev/null 2>&1 & pids+=($!)
until redis-cli -p $BASE SHARD STATUS 2>/dev/null | grep -q "state=healthy"; do sleep 0.2; done

R="redis-cli -p $BASE"
echo "# one shard: a primary and two replicas; the router holds the primary's write lease"
run $R SHARD STATUS
echo
echo "# writes go through the router to the primary; with sync 1 an OK means a replica holds the write too"
run $R SET greeting hello
run $R SET session:1 alive PX 600000
sleep 0.2
echo "# replicas serve reads and refuse writes:"
run redis-cli -p $R1 GET greeting
run redis-cli -p $R1 SET x y
echo
echo "# the primary's view of its replicas:"
redis-cli -p $P INFO | grep -E "^(role|repl_offset|in_sync_replicas|replica[0-9])"
echo
echo "# kill -9 the primary"
kill -9 "${pids[0]}"; t0=$(date +%s.%N)
until $R SET after-failover works 2>/dev/null | grep -q OK; do sleep 0.05; done
echo "writes possible again after $(echo "$(date +%s.%N) - $t0" | bc) s"
run $R GET greeting
run $R TTL session:1
echo
run $R SHARD STATUS
run $R SHARD EVENTS
echo
echo "# the old primary comes back: it must NOT accept writes; the router demotes it and it resynchronises"
./build/distcache --port $P --repl-port $((P + 10)) --repl-require-lease --repl-sync 1 > /dev/null 2>&1 & pids+=($!)
sleep 1
run redis-cli -p $P ROLE
run redis-cli -p $P SET stale write
run redis-cli -p $P GET after-failover
