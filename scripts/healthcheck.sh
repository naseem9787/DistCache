#!/bin/bash
# Liveness/readiness probe for one DistCache process (used by the Docker HEALTHCHECK).
#   healthcheck.sh PORT [HOST]
# exit 0  = answers PING and, if it is a replica, its link to the primary is up and synchronised
# exit 1  = not ready (down, still syncing, or a replica whose link is down)
# A router answers PING and is ready when SHARD STATUS (if it has replicated shards) shows no unhealthy shard.
PORT=${1:?port}
HOST=${2:-127.0.0.1}
rc() { redis-cli -h "$HOST" -p "$PORT" "$@" 2>/dev/null; }
[ "$(rc PING)" = "PONG" ] || exit 1
info=$(rc INFO)
if echo "$info" | grep -q '^role:slave'; then
  echo "$info" | grep -q '^replica_state:online' && echo "$info" | grep -q '^master_link_status:up' || exit 1
fi
status=$(rc SHARD STATUS)
case $status in
  shard:*) echo "$status" | grep '^shard:' | grep -qv 'state=healthy' && exit 1 ;;
esac
exit 0
