#!/bin/bash
# End-to-end demo of everything: routing, replication, rate limiting, failover, rejoin, live rebalancing.
#   scripts/demo_full.sh            needs build/ (cmake -S . -B build -G Ninja && cmake --build build) and redis-cli
# It starts the local cluster of scripts/cluster.sh (2 shards x 3 nodes + router, sync replication, WAL),
# narrates each step, and shuts everything down at the end. Takes about 40 seconds.
set -u
cd "$(dirname "$0")/.."
export DC_HOME=${DC_HOME:-/tmp/distcache-demo} BASE_PORT=${BASE_PORT:-7000}
R="redis-cli -p $BASE_PORT"
CL=scripts/cluster.sh
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
run()  { printf '$ %s\n' "$*"; "$@"; }
trap '$CL down >/dev/null 2>&1' EXIT

$CL clean >/dev/null 2>&1
step "1. Start the cluster: 2 shards (a, b), each 1 primary + 2 replicas, sync replication, WAL, one router"
$CL up
run $CL status

step "2. ROUTING: keys are spread over the shards by consistent hashing"
for i in $(seq 1 300); do echo "SET user:$i value-$i"; done | $R > /dev/null
echo "each shard's primary holds its own share (the router's DBSIZE sums both):"
echo -n "  shard a primary: "; redis-cli -p $((BASE_PORT+101)) DBSIZE
echo -n "  shard b primary: "; redis-cli -p $((BASE_PORT+201)) DBSIZE
echo -n "  router DBSIZE:   "; $R DBSIZE
run $R RING LOOKUP user:42

step "3. REPLICATION: every write is on a replica before the client hears OK (--repl-sync 1)"
echo "replica a2 serves reads but refuses writes:"
KEY_A=$(for i in $(seq 1 500); do [ "$($R RING LOOKUP "k$i")" = a ] && echo "k$i" && break; done)
run $R SET "$KEY_A" "stored-on-shard-a"
sleep 0.3
run redis-cli -p $((BASE_PORT+102)) GET "$KEY_A"
run redis-cli -p $((BASE_PORT+102)) SET "$KEY_A" "x"
echo "the primary's view of its replicas:"
redis-cli -p $((BASE_PORT+101)) INFO | grep -E '^(role|repl_offset|in_sync_replicas|replica[0-9])'

step "4. RATE LIMITING: a token bucket of 5 tokens (no meaningful refill) stored as a key on shard a"
# pick a bucket name that lives on shard a, so the failover below hits it
BUCKET=$(for i in $(seq 1 500); do [ "$($R RING LOOKUP "rl:tenant$i")" = a ] && echo "rl:tenant$i" && break; done)
echo "bucket $BUCKET lives on shard: $($R RING LOOKUP "$BUCKET")"
echo "RL.ACQUIRE $BUCKET 5 0.001   ->   [allowed, remaining, retry_after_ms, reset_after_ms]"
for i in 1 2 3 4 5 6 7; do printf 'call %d: ' $i; $R RL.ACQUIRE "$BUCKET" 5 0.001 | tr '\n' ' '; echo; done
sleep 0.3
echo "the bucket is replicated: RL.PEEK on replica a2 agrees (advisory read, nothing consumed):"
run redis-cli -p $((BASE_PORT+102)) RL.PEEK "$BUCKET" 5 0.001
run $R SET session:ttl alive EX 600

step "5. FAILOVER: kill -9 the primary of shard a while writing"
OLD_PRIMARY=a1
run $CL kill $OLD_PRIMARY
START=$(date +%s.%N)
until [ "$($R SET "$KEY_A" "written-after-the-crash" 2>&1)" = OK ]; do sleep 0.05; done
echo "writes to shard a work again after $(echo "$(date +%s.%N) - $START" | bc) s"
run $R SHARD STATUS
run $R SHARD EVENTS
echo "nothing acknowledged was lost, TTLs and the rate limiter's state survived:"
run $R GET user:42
run $R TTL session:ttl
echo "the bucket was empty before the crash and still is (a forgotten token would show as 'allowed'):"
run $R RL.ACQUIRE "$BUCKET" 5 0.001

step "6. REJOIN: the old primary comes back; it must not accept writes and must resynchronise"
run $CL start $OLD_PRIMARY
sleep 3
echo "a1 is now a replica of the new primary and refuses writes:"
run redis-cli -p $((BASE_PORT+101)) SET stale write
run redis-cli -p $((BASE_PORT+101)) GET "$KEY_A"
run $CL status

step "7. LIVE REBALANCING (Phase 7): add a third shard with MIGRATE while data is live"
./build/distcache --port $((BASE_PORT+900)) --threads 2 > "$DC_HOME/logs/c.log" 2>&1 &
CPID=$!
sleep 0.5
run $R RING ADD c 127.0.0.1 $((BASE_PORT+900)) MIGRATE
echo -n "keys now on the new shard c: "; redis-cli -p $((BASE_PORT+900)) DBSIZE
echo -n "all 300 users still readable: "; for i in $(seq 1 300); do [ "$($R GET user:$i)" = "value-$i" ] && echo x; done | wc -l
echo "the rate-limit bucket is still empty (state moved with the key if it moved):"
run $R RL.ACQUIRE "$BUCKET" 5 0.001
run $R RING REMOVE c MIGRATE
kill $CPID 2>/dev/null

step "Done. Shutting the cluster down cleanly."
