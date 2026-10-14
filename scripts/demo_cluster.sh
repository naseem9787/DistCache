#!/bin/bash
# A scripted tour of the cluster: three nodes, one router, live rebalancing, all through redis-cli.
# Usage: scripts/demo_cluster.sh            (needs build/distcache, build/distcache_router, redis-cli)
set -e
cd "$(dirname "$0")/.."
BASE=${BASE_PORT:-7700}
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

for i in 1 2 3 4; do
  ./build/distcache --port $((BASE + i)) --threads 1 > /dev/null 2>&1 &
  pids+=($!)
done
sleep 0.5
./build/distcache_router --port $BASE \
    --node a=127.0.0.1:$((BASE + 1)) --node b=127.0.0.1:$((BASE + 2)) --node c=127.0.0.1:$((BASE + 3)) > /dev/null &
pids+=($!)
sleep 0.5

R="redis-cli -p $BASE"
run() { echo "\$ $*"; "$@"; }

echo "# 3 nodes behind the router. Load 3000 keys through the router:"
for i in $(seq 1 3000); do echo "SET key:$i value-$i"; done | $R > /dev/null
run $R DBSIZE
echo
echo "# where do keys live? (each node's own DBSIZE, bypassing the router)"
for i in 1 2 3; do echo -n "node $i: "; redis-cli -p $((BASE + i)) DBSIZE; done
echo
run $R RING LOOKUP key:42
run $R GET key:42
echo
echo "# hash tags keep related keys together:"
run $R RING LOOKUP "{user:7}:profile"
run $R RING LOOKUP "{user:7}:cart"
echo
echo "# add a 4th node WITHOUT migration: about a quarter of the keys now miss"
run $R RING ADD d 127.0.0.1 $((BASE + 4))
miss=0; for i in $(seq 1 3000); do [ "$($R GET key:$i)" = "" ] && miss=$((miss + 1)); done
echo "keys that now miss: $miss of 3000"
echo
echo "# remove it again, then add it WITH migration: every key stays reachable"
run $R RING REMOVE d
run $R RING ADD d 127.0.0.1 $((BASE + 4)) MIGRATE
miss=0; for i in $(seq 1 3000); do [ "$($R GET key:$i)" = "" ] && miss=$((miss + 1)); done
echo "keys that miss: $miss of 3000"
for i in 1 2 3 4; do echo -n "node $i: "; redis-cli -p $((BASE + i)) DBSIZE; done
echo
run $R RING INFO
