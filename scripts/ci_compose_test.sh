#!/bin/bash
# End-to-end check of docker-compose.yml (used by CI; runs anywhere with Docker Compose v2):
#   build the image, start 2 shards x (1 primary + 2 replicas) + the router, then
#   routing -> rate limiter -> kill -9 the primary of one shard -> writes work again -> data and rate-limit state
#   intact -> the old primary comes back as a synchronised replica.
# Exit 0 only if every step holds; on failure the container logs are printed.
set -u
cd "$(dirname "$0")/.."
DC="docker compose"

R() { $DC exec -T router redis-cli -p 6390 "$@" 2>&1; }          # a command through the router
N() { local n=$1; shift; $DC exec -T "$n" redis-cli -p 6380 "$@" 2>&1; }   # a command straight at a node
cleanup() { $DC down -v >/dev/null 2>&1; }
fail() { echo "FAIL: $*"; $DC ps -a; $DC logs --tail 60 2>&1 | tail -250; cleanup; trap - EXIT; exit 1; }
trap cleanup EXIT
step() { echo; echo "== $*"; }

step "build and start (waits for every container's health check)"
$DC up -d --build --wait --wait-timeout 420 || fail "docker compose up"
$DC ps

step "the router answers and both shards are healthy"
for i in $(seq 1 60); do
  s=$(R SHARD STATUS)
  [ "$(echo "$s" | grep -c '^shard:')" -eq 2 ] && [ "$(echo "$s" | grep '^shard:' | grep -vc 'state=healthy')" -eq 0 ] && break
  sleep 1
done
echo "$s"
[ "$(echo "$s" | grep -c 'state=healthy')" -eq 2 ] || fail "shards never became healthy"

step "routing, replication and TTL"
[ "$(R SET hello world | tr -d '\r')" = "OK" ] || fail "SET through the router"
[ "$(R GET hello | tr -d '\r')" = "world" ] || fail "GET through the router"
KEY=$(for i in $(seq 1 300); do [ "$(R RING LOOKUP "ci:k$i" | tr -d '\r')" = "a" ] && echo "ci:k$i" && break; done)
[ -n "$KEY" ] || fail "no key found on shard a"
[ "$(R SET "$KEY" before-crash EX 600 | tr -d '\r')" = "OK" ] || fail "SET on shard a"
echo "key $KEY lives on shard a"

step "rate limiter: a 3-token bucket on shard a"
BUCKET=$(for i in $(seq 1 300); do [ "$(R RING LOOKUP "rl:ci$i" | tr -d '\r')" = "a" ] && echo "rl:ci$i" && break; done)
[ -n "$BUCKET" ] || fail "no bucket name found on shard a"
for i in 1 2 3; do
  [ "$(R RL.ACQUIRE "$BUCKET" 3 0.001 | head -1 | tr -d '\r')" = "1" ] || fail "token $i of the bucket was refused"
done
[ "$(R RL.ACQUIRE "$BUCKET" 3 0.001 | head -1 | tr -d '\r')" = "0" ] || fail "a 4th token was granted from a 3-token bucket"

step "kill -9 the primary of shard a"
P=$(R SHARD STATUS | sed -n 's/^shard:a .*primary=a#\([0-9]\).*/\1/p')
PRIMARY="a$((P + 1))"
echo "primary of shard a is $PRIMARY"
$DC kill -s SIGKILL "$PRIMARY" || fail "could not kill $PRIMARY"
ok=""
for i in $(seq 1 90); do
  if [ "$(R SET "$KEY" after-crash | tr -d '\r')" = "OK" ]; then ok=1; break; fi
  sleep 0.5
done
[ -n "$ok" ] || fail "writes to shard a never worked again after the primary was killed"
echo "writes to shard a work again after about $((i / 2)) s"
R SHARD EVENTS

step "nothing acknowledged was lost; the rate-limit bucket is still empty"
[ "$(R GET hello | tr -d '\r')" = "world" ] || fail "GET hello after failover"
[ "$(R GET "$KEY" | tr -d '\r')" = "after-crash" ] || fail "GET $KEY after failover"
[ "$(R RL.ACQUIRE "$BUCKET" 3 0.001 | head -1 | tr -d '\r')" = "0" ] || fail "the bucket was refilled by the failover"

step "the old primary comes back and rejoins as a synchronised replica"
$DC up -d "$PRIMARY" || fail "restart $PRIMARY"
ok=""
for i in $(seq 1 90); do
  if [ "$(N "$PRIMARY" ROLE | head -1 | tr -d '\r')" = "slave" ] && R SHARD STATUS | grep "$PRIMARY\|a#$P" | grep -q 'synced=yes'; then
    s=$(R SHARD STATUS); [ "$(echo "$s" | grep '^shard:' | grep -vc 'state=healthy')" -eq 0 ] && { ok=1; break; }
  fi
  sleep 1
done
[ -n "$ok" ] || fail "$PRIMARY did not rejoin as a synchronised replica"
R SHARD STATUS
[ "$(N "$PRIMARY" SET stale write | tr -d '\r' | cut -c1-8)" = "READONLY" ] || fail "the old primary accepted a write"
[ "$(R SET after rejoin | tr -d '\r')" = "OK" ] || fail "write after the rejoin"

echo; echo "PASS: docker compose cluster routes, replicates, fails over and recovers"
