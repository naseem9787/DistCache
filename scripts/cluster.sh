#!/bin/bash
# A reproducible local DistCache cluster, without Docker:
#     2 shards (a, b) x (1 primary + 2 replicas), persistence on, sync replication (--repl-sync 1),
#     one router that routes, watches and fails over.
#
#   scripts/cluster.sh up            build check + start everything, wait until the router reports healthy shards
#   scripts/cluster.sh status        router view (SHARD STATUS) + every node's role
#   scripts/cluster.sh health        exit 0 only if the router answers and every shard is healthy (for scripts/CI)
#   scripts/cluster.sh kill NODE     kill -9 one node (a1 a2 a3 b1 b2 b3 router)
#   scripts/cluster.sh start NODE    start a node again (an old primary rejoins as a replica)
#   scripts/cluster.sh logs NODE     tail a node's log
#   scripts/cluster.sh down          stop everything (SIGTERM, waits for a clean exit)
#   scripts/cluster.sh clean         down + delete data
#
# Environment: DC_HOME (default /tmp/distcache-cluster; use a disk path if you want data to survive reboots),
#              BASE_PORT (default 7000: router; nodes 71xx/72xx; replication ports +1000), DC_BIN (default ./build).
# Ports: router 7000; a1 a2 a3 = 7101 7102 7103; b1 b2 b3 = 7201 7202 7203; replication ports 8101... 8203.
set -u
cd "$(dirname "$0")/.."
DC_HOME=${DC_HOME:-/tmp/distcache-cluster}
BASE=${BASE_PORT:-7000}
BIN=${DC_BIN:-./build}
NODES="a1 a2 a3 b1 b2 b3"
mkdir -p "$DC_HOME"/{pids,logs,data}

port_of() { case $1 in a1) echo $((BASE+101));; a2) echo $((BASE+102));; a3) echo $((BASE+103));;
                      b1) echo $((BASE+201));; b2) echo $((BASE+202));; b3) echo $((BASE+203));; router) echo $BASE;; esac; }
repl_of() { echo $(($(port_of "$1") + 1000)); }
alive() { [ -f "$DC_HOME/pids/$1.pid" ] && kill -0 "$(cat "$DC_HOME/pids/$1.pid")" 2>/dev/null; }

start_node() {
  local n=$1
  if alive "$n"; then echo "$n already running"; return 0; fi
  # EVERY node is started the same way, with no --replicaof: roles belong to the router. At first start all
  # nodes are primaries at epoch 0 without a lease (so they refuse writes) and the router keeps the designated
  # one (the first --node of the shard) and demotes the others. A restarted node comes back as a primary at the
  # epoch it was last promoted at (0 if never) and is re-adopted or demoted according to the shard's epoch.
  # (Starting restarted replicas with --replicaof <initial primary> is a trap: after a failover that node may
  # itself be a replica, and the two would follow each other forever.)
  local args=(--port "$(port_of "$n")" --repl-port "$(repl_of "$n")" --bind 127.0.0.1 --threads 2
              --dir "$DC_HOME/data/$n" --fsync everysec --repl-sync 1 --repl-sync-timeout-ms 2000 --repl-require-lease)
  "$BIN/distcache" "${args[@]}" >> "$DC_HOME/logs/$n.log" 2>&1 &
  echo $! > "$DC_HOME/pids/$n.pid"
}

start_router() {
  if alive router; then echo "router already running"; return 0; fi
  "$BIN/distcache_router" --port "$BASE" --bind 127.0.0.1 \
      --node "a=127.0.0.1:$(port_of a1)" --replica "a=127.0.0.1:$(port_of a2)" --replica "a=127.0.0.1:$(port_of a3)" \
      --node "b=127.0.0.1:$(port_of b1)" --replica "b=127.0.0.1:$(port_of b2)" --replica "b=127.0.0.1:$(port_of b3)" \
      --sync-replicas 1 --lease-ms 1000 --probe-ms 250 --probe-timeout-ms 300 --guard-ms 250 --fail-after 3 \
      --state-file "$DC_HOME/router.state" >> "$DC_HOME/logs/router.log" 2>&1 &
  echo $! > "$DC_HOME/pids/router.pid"
}

wait_port() { for _ in $(seq 1 100); do redis-cli -p "$1" PING 2>/dev/null | grep -q PONG && return 0; sleep 0.1; done; return 1; }

healthy() {
  local s; s=$(redis-cli -p "$BASE" SHARD STATUS 2>/dev/null) || return 1
  [ "$(echo "$s" | grep -c '^shard:')" -eq 2 ] && [ "$(echo "$s" | grep '^shard:' | grep -vc 'state=healthy')" -eq 0 ]
}

stop_one() {
  local n=$1
  if alive "$n"; then
    kill "$(cat "$DC_HOME/pids/$n.pid")" 2>/dev/null
    for _ in $(seq 1 100); do alive "$n" || break; sleep 0.1; done
    alive "$n" && { echo "$n did not stop, killing"; kill -9 "$(cat "$DC_HOME/pids/$n.pid")"; }
  fi
  rm -f "$DC_HOME/pids/$n.pid"
}

cmd=${1:-help}
case $cmd in
  up)
    [ -x "$BIN/distcache" ] && [ -x "$BIN/distcache_router" ] || { echo "build first: cmake -S . -B build -G Ninja && cmake --build build"; exit 1; }
    command -v redis-cli >/dev/null || { echo "redis-cli is needed (apt install redis-tools)"; exit 1; }
    for n in a1 b1; do start_node $n; done
    for n in a1 b1; do wait_port "$(port_of $n)" || { echo "$n did not start; see $DC_HOME/logs/$n.log"; exit 1; }; done
    for n in a2 a3 b2 b3; do start_node $n; done
    for n in a2 a3 b2 b3; do wait_port "$(port_of $n)" || { echo "$n did not start; see $DC_HOME/logs/$n.log"; exit 1; }; done
    start_router
    wait_port "$BASE" || { echo "router did not start; see $DC_HOME/logs/router.log"; exit 1; }
    for _ in $(seq 1 150); do healthy && break; sleep 0.2; done
    healthy || { echo "cluster did not become healthy"; redis-cli -p "$BASE" SHARD STATUS; exit 1; }
    echo "cluster is up: redis-cli -p $BASE   (logs and data in $DC_HOME)"
    ;;
  status)
    redis-cli -p "$BASE" SHARD STATUS
    for n in $NODES; do
      printf '%s :%s  ' "$n" "$(port_of $n)"
      if alive "$n"; then redis-cli -p "$(port_of $n)" ROLE 2>/dev/null | sed -n '1,2p;4p' | tr '\n' ' ' | sed 's/^/role,epoch,offset= /'; echo; else echo "(not running)"; fi
    done
    ;;
  health) healthy ;;
  kill)
    n=${2:?node}; alive "$n" || { echo "$n is not running"; exit 1; }
    kill -9 "$(cat "$DC_HOME/pids/$n.pid")"; rm -f "$DC_HOME/pids/$n.pid"; echo "killed $n"
    ;;
  start)
    n=${2:?node}
    if [ "$n" = router ]; then start_router; wait_port "$BASE"; else start_node "$n"; wait_port "$(port_of "$n")"; fi
    echo "started $n"
    ;;
  logs) tail -n 40 "$DC_HOME/logs/${2:?node}.log" ;;
  down)
    stop_one router
    for n in $NODES; do stop_one "$n"; done
    echo "cluster stopped"
    ;;
  clean) "$0" down; rm -rf "$DC_HOME"; echo "removed $DC_HOME" ;;
  *) sed -n '2,17p' "$0" ;;
esac
