#!/bin/bash
# Is the replication-log mutex a bottleneck? Times the log mutex (wait / hold / encode) and the shard locks with the
# diagnostics build, on a pipelined SET-only load, for: no replication, replication log only, log + 1 async replica.
#   scripts/profile_repl_log.sh [repeats] > docs/benchmarks/final/repl-log-profile.txt
set -u
cd "$(dirname "$0")/.."
REPS=${1:-3}; PORT=7950
cmake --build build --target distcache_diag distcache_loadgen > /dev/null || exit 1
run_one() {  # $1 = label, $2 = extra flags for the primary, $3 = 1 to attach a replica
  taskset -c 0-3 ./build/distcache_diag --port $PORT --threads 4 $2 > /dev/null 2>&1 & local P=$!
  sleep 0.5
  local R=""
  if [ "$3" = 1 ]; then
    taskset -c 4-5 ./build/distcache_diag --port $((PORT+1)) --repl-port $((PORT+11)) --threads 2 --replicaof 127.0.0.1:$((PORT+10)) > /dev/null 2>&1 & R=$!
    sleep 1.0
  fi
  local out; out=$(taskset -c 6-11 ./build/distcache_loadgen --port $PORT --clients 50 --threads 4 --pipeline 16 --duration 8 --warmup 1.5 --keyspace 100000 --get-ratio 0 --label x)
  local rps; rps=$(echo "$out" | sed -n 's/.*"rps":\([0-9.]*\).*/\1/p')
  local info; info=$(redis-cli -p $PORT INFO)
  g() { echo "$info" | tr -d '\r' | sed -n "s/^$1:\(.*\)/\1/p"; }
  local ops; ops=$(g diag_shard_hold_calls)
  printf '%-28s rps=%9.0f ops=%9s | shard_wait avg=%6s ns  shard_hold avg=%6s ns | repl_encode avg=%5s  repl_wait avg=%6s ns  repl_locked avg=%5s ns (calls %s)\n' \
     "$1" "$rps" "$ops" "$(g diag_shard_wait_avg_ns)" "$(g diag_shard_hold_avg_ns)" "$(g diag_repl_encode_avg_ns)" "$(g diag_repl_wait_avg_ns)" "$(g diag_repl_locked_avg_ns)" "$(g diag_repl_locked_calls)"
  [ -n "$R" ] && kill $R 2>/dev/null; kill $P 2>/dev/null; wait 2>/dev/null; sleep 1
}
echo "# diagnostics build, 4 server threads on CPUs 0-3, 50 clients, pipeline 16, SET only, $REPS rounds (interleaved)"
for i in $(seq 1 $REPS); do
  run_one "round $i: no replication" "" 0
  run_one "round $i: log only" "--repl-port $((PORT+10))" 0
  run_one "round $i: log + 1 replica" "--repl-port $((PORT+10))" 1
done
