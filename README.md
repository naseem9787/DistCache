# DistCache

A Redis-compatible, distributed in-memory cache and rate limiter written from scratch in C++17: RESP over
TCP, TTL and LRU, a multi-threaded engine, a write-ahead log with snapshots, consistent-hash sharding behind
a router, primary/replica replication with automatic lease-fenced failover, and a token-bucket rate limiter
that rides on all of it. Everything is measured on one machine and every guarantee comes with the test or
benchmark behind it, and the cases it **cannot** handle are written down
([`docs/design/failure-model.md`](docs/design/failure-model.md)).

It is not consensus (no Raft/Paxos), it does not claim zero data loss, and it does not claim production-grade
split-brain prevention beyond what is proven under stated timing assumptions.

```bash
cmake -S . -B build -G Ninja && cmake --build build          # needs a C++17 compiler (developed with g++ 15.2), CMake, Ninja, GoogleTest; redis-cli for the demos
./build/distcache --port 6380                                 # one node (listens on 127.0.0.1 only; see --bind)
redis-cli -p 6380 SET session:1 abc EX 60
redis-cli -p 6380 RL.ACQUIRE api:user:42 100 10               # token bucket: burst 100, 10 tokens/s -> [allowed, remaining, retry_after_ms, reset_after_ms]
scripts/demo_full.sh                                          # the whole system in 40 seconds: routing, replication, rate limiting, failover, rejoin, live rebalancing
```

## Contents

1. [What is in it](#what-is-in-it) 2. [Architecture](#architecture) 3. [Run a cluster](#run-a-cluster) 4. [Commands](#commands)
5. [Guarantees](#guarantees) 6. [Failure scenarios](#failure-scenarios) 7. [Benchmarks](#benchmarks)
8. [Design trade-offs](#design-trade-offs) 9. [Limitations](#limitations) 10. [Testing](#testing) 11. [Repository layout](#repository-layout)

## What is in it

| phase | what | deep dive |
|---|---|---|
| 1-3 | TCP + RESP server; `SET GET DEL`; TTL (lazy + active expiry); O(1) LRU eviction | `src/store.hpp`, `src/resp.*` |
| 4 | concurrency: worker threads, one global lock vs sharded locks, ThreadSanitizer | [phase 4](docs/phases/phase4.md) |
| 5 | durability: write-ahead log, snapshots, crash recovery, fsync policies | [phase 5](docs/phases/phase5.md) |
| 6 | benchmark harness; what durability really costs; deferred-ack group commit | [phase 6](docs/phases/phase6.md) |
| 7 | consistent hash ring, router, live rebalancing (`RING ADD/REMOVE ... MIGRATE`) | [phase 7](docs/phases/phase7.md) |
| 8 | replication (async and `sync N`), partial/full resync, lease-fenced automatic failover | [phase 8](docs/phases/phase8.md), [design + proofs](docs/design/replication.md) |
| 9 | distributed rate limiter (`RL.ACQUIRE`/`RL.PEEK`) | [phase 9](docs/phases/phase9.md), [design](docs/design/rate-limiter.md) |
| 10 | integration, chaos testing, two correctness fixes, resumable migration, hardening, packaging | [phase 10](docs/phases/phase10.md) |

## Architecture

```
                      clients (redis-cli, any RESP client, or a smart client using the same ring)
                                         |
                                         v
        +--------------------------------------------------------------------------------+
        |  distcache_router                                                              |
        |   ring:  key --hash tag/murmur--> consistent-hash ring --> SHARD NAME          |
        |   route: shard name --> current primary address  (changes on failover only)    |
        |   coordinator thread, one per replicated shard:                                |
        |     probe ROLE every probe-ms | renew the primary's write LEASE | detect failure|
        |     wait until the old lease cannot be alive | PROMOTE the best replica        |
        |     REPLICAOF the others | demote a returning old primary | SHARD STATUS/EVENTS |
        +-----------------+---------------------------------------+----------------------+
                          | shard "a"                             | shard "b"      ...
              +-----------v------------+              +-----------v------------+
              | PRIMARY  (epoch e)     |              | PRIMARY                |
              |  worker threads        |              |  ...                   |
              |   epoll, RESP parse    |              +------------------------+
              |   execute()            |
              |    |  command gates:   |   a write is: lease valid? role? -> apply under the shard lock
              |    v  READONLY/LOADING/|        |
              |  Engine (64 shards,    |        +--> ObserverChain, inside the same critical section:
              |   LRU, TTL index,      |              1. WAL buffer   (group commit, fsync policy)
              |   RL.* via update())   |              2. replication log (offset k, byte-budgeted)
              |    |                   |
              |  reply is HELD until:  |   WAL durable  AND  (sync N) N replicas acked offset k  AND  lease still valid
              +-----+-------------+----+
                    | repl port   | repl port        records:  R <offset> <RESP command>   |   replicas answer ACK <offset>
              +-----v----+   +----v-----+            full resync: fuzzy snapshot + replay from offset S+1
              | REPLICA  |   | REPLICA  |            every record is idempotent (absolute TTL deadlines, resulting state)
              | read-only|   | read-only|
              +----------+   +----------+
```

Data flow of one write: `client -> router (ring lookup, check shard available) -> primary worker thread ->
execute() -> engine update under the key's shard lock -> WAL record + replication record appended in that same
critical section -> reply held until durable/acknowledged -> OK`.

Control plane: the router grants each primary a **short write lease** (default 2 s, renewed every 500 ms). A
primary refuses writes without a valid lease, and the coordinator promotes a replica only after
`now >= t_ack + L(1+eps) + guard`, i.e. when the old lease cannot still be believed. Every promotion bumps an
**epoch**; a node that sees a higher epoch fences itself or refuses the stale peer. With `--repl-sync N` and
R replicas, promotion needs R-N+1 eligible replicas, which makes the promoted node hold every acknowledged
write (pigeonhole proof in [`docs/design/replication.md`](docs/design/replication.md)).

## Run a cluster

**Without Docker (tested).** `scripts/cluster.sh` starts 2 shards x (1 primary + 2 replicas) with persistence,
`--repl-sync 1` and a router:

```bash
scripts/cluster.sh up                 # ~1 s; ports: router 7000, shard a 7101-7103, shard b 7201-7203
redis-cli -p 7000 SET hello world
scripts/cluster.sh status             # SHARD STATUS + every node's role
scripts/cluster.sh kill a1            # kill -9 a primary ... (find the primary with: status)
redis-cli -p 7000 SHARD EVENTS        # ... and watch the failover
scripts/cluster.sh start a1           # it comes back, is demoted, resynchronises
scripts/cluster.sh health             # exit 0 only when every shard is healthy
scripts/cluster.sh down
scripts/demo_full.sh                  # the narrated end-to-end demo (output saved in docs/demo-output.txt)
```

**Docker Compose (written, NOT run: the development machine has no Docker).** `Dockerfile` and
`docker-compose.yml` define the same topology with health checks (`scripts/healthcheck.sh`); treat them as a
starting point. Nodes are started **without** `--replicaof`: roles belong to the router (see `scripts/cluster.sh`
for the trap this avoids).

**Configuration** is flags only, all validated: [`docs/configuration.md`](docs/configuration.md) (every flag,
defaults, health checks, what `INFO` shows). Default bind is `127.0.0.1`; there is **no authentication**.

## Commands

| command | |
|---|---|
| `PING ECHO` | |
| `SET key value [EX s \| PX ms]`, `GET`, `DEL key [key ...]` | |
| `EXPIRE PEXPIRE TTL PTTL PERSIST` | absolute deadlines, so TTLs survive WAL recovery, replication and failover |
| `DBSIZE KEYS pattern INFO SAVE` | `INFO` sections: Server, Concurrency, Stats, Persistence, Replication |
| `RL.ACQUIRE key capacity rate [cost]` | take tokens: reply `[allowed 1\|0, remaining, retry_after_ms, reset_after_ms]` |
| `RL.PEEK key capacity rate [cost]` | same answer, nothing consumed (a read; stale on replicas) |
| router: `RING INFO`, `RING LOOKUP key`, `RING ADD name host port [weight] [MIGRATE]`, `RING REMOVE name [MIGRATE]`, `RING RESUME`, `RING ABORT`, `SHARD STATUS`, `SHARD EVENTS` | live topology changes; a migration with errors stays pending until `RESUME` |
| node control (used by the router): `ROLE`, `LEASE epoch ms`, `PROMOTE epoch`, `REPLICAOF host port epoch` | |

```bash
redis-cli -p 7000 RL.ACQUIRE api:user:42 5 0.001      # five tokens, no refill that matters
# 1) 1 2) 4 3) -1 4) 1000000      allowed, 4 left, no wait, full again in ~1000 s
```

## Guarantees

| | async (default) | `--repl-sync N` |
|---|---|---|
| a write is acknowledged when | the primary applied it (and its WAL is durable per `--fsync`) | ... and N replicas applied it |
| primary `kill -9` then failover | the unreplicated tail is **lost** | **no acknowledged write is lost**, provided the coordinator can reach >= R-N+1 eligible replicas; otherwise it does not promote |
| durability of an acknowledged write | WAL policy of the primary | N replicas' **memory** as well (not their disk) |
| a timed-out ack | n/a | connection closed; the outcome is unknown |

Other guarantees, each with its evidence in [`failure-model.md`](docs/design/failure-model.md): a returning old
primary is demoted and fully resynchronised (never overwrites); TTLs are absolute; an unavailable or
ambiguous shard refuses reads and writes (fail closed); a frozen primary that wakes up cannot acknowledge
writes. **Rate limiter:** exact per bucket on the primary (race-free under the shard lock; checked by a property
test and 16-thread tests); with `--repl-sync` no consumed token is forgotten by a failover; with async the
over-admission after a failover equals the unreplicated tail (tested exactly); an evicted bucket is a full
bucket. Details: [`docs/design/rate-limiter.md`](docs/design/rate-limiter.md).

## Failure scenarios

Tested (unit, in-process cluster, real processes, and the chaos harness): replica disconnect/reconnect/restart,
duplicated and dropped replication records, a too-slow replica, primary crash (async and sync), a frozen
primary, restart from the WAL, router restart, a failover during a live migration, TTL/delete/rate-limit state
across failover and WAL restart, clock skew after failover (rate limiter), simultaneous random faults under load
([`bench/harness/chaos.py`](bench/harness/chaos.py): kills, SIGSTOP, router restarts, 100k+ acknowledged keys verified
per run, one primary per shard, no resurrection, no limit exceeded).

**Not handled** (full list in [`failure-model.md`](docs/design/failure-model.md)): the router is the only coordinator
(it is not Raft; if it is down no failover happens); split-brain prevention rests on bounded clock-rate drift,
message delay and process pause (a pause longer than the guard at the wrong instant can allow one stale
acknowledged write under async); async loses the tail; sync-N is memory not disk; more simultaneous failures than
the policy tolerates make a shard unavailable until a node returns; clock jumps affect absolute TTLs and
rate-limiter refill; network partitions beyond "node unreachable" are untested; no authentication or TLS.

## Benchmarks

Everything is one machine (i5-11260H, 6C/12T, WSL2, 3.7 GB, g++ 15.2 -O3), loopback, closed-loop clients, repeats in
shuffled order, a health probe before and after, and raw results kept next to every report. Sessions on this machine
drift, so numbers are compared **within a session only**; noisy cells are kept and labelled. Facts and hypotheses
are separated in each document.

| area | headline (details, methodology, raw data) |
|---|---|
| engine, Phase 6 | ~440k req/s unpipelined and ~2.0M at pipeline 16 on 4 workers; sharded locks 3.6x one global mutex; TTL costs 36 % on `SET PX`; WAL costs 29-35 % on pipelined SETs; `fsync always` 8-30x below `everysec` but safe; recovery 1M keys 1.2 s (log) vs 0.64 s (snapshot) - [phase 6](docs/phases/phase6.md), `docs/benchmarks/phase6/` |
| sharding, Phase 7 | 28.6 % of keys move when a 4th node joins (ideal 25 %, `% N` would move 75 %); smart client scales 1.7x/3.0x with 2/3 nodes unpipelined, **the router does not** (one thread per connection) - [phase 7](docs/phases/phase7.md), `docs/benchmarks/phase7/` |
| replication, failover (final session) | final session on the finished code: the replication log alone costs **52 %** of pipelined writes (1.01M vs 2.12M req/s); async +1 replica **-72 %** (594k) and `sync 1` **-72 %** (587k) at pipeline 16, **-47 % / -71 %** at pipeline 1 (p50 336 us vs 88 us); failover after `kill -9` takes **2.6 s** with the default lease (0.65 s with a 500 ms lease), **0 of 25,542** acknowledged writes missing in 20 kills; full resync ~1.3-1.5M keys/s (1M keys in 0.79 s); a replica that also serves heavy reads while the primary is pipelined is cut off and resynchronised in 5 of 5 runs - [phase 8](docs/phases/phase8.md), [`docs/benchmarks/final/replication/`](docs/benchmarks/final/replication/report.md) |
| rate limiter, Phase 9 | hot bucket accurate to 0.9997-1.0000 of the fluid limit, safety bound held in 60/60 runs; 3-node smart client 3.1x one node on many keys (1.9x at pipeline 16), a single hot key does not scale; allowed calls are writes so replication costs what writes cost; across 5 `kill -9` failovers per policy the capacity was never exceeded with `sync 1` and by at most 1 token with async - [phase 9](docs/phases/phase9.md), `docs/benchmarks/phase9/` |
| replication-log cost | one thread appends in 173 ns; 4 threads together reach only ~2.5-2.9M records/s (mutex + shared cache lines) and the log alone halves pipelined write throughput; a lock-free ring is the real fix and was **not** attempted (it re-opens the Phase 8 proofs) - [phase 10](docs/phases/phase10.md), `docs/benchmarks/final/repl-log-profile-*.txt` |
| chaos | 11 runs of 90 s (seeds 1-8 and three repeats of seed 5): 129 injected faults (39 primary kills, 49 replica kills, 17 SIGSTOP freezes, 24 router restarts), about **1.26M live acknowledged keys and 126k acknowledged deletes verified**, no loss, no resurrection, no bucket over its allowance, one primary per shard, replicas converged; one run is a suspended host (see `docs/benchmarks/final/chaos-notes.md`) |

## Design trade-offs

* **Sharded locks over one lock**: 3.6x throughput; LRU becomes approximate (per shard).
* **Log inside the engine critical section** so log order = apply order per key, at the price of a shared mutex on every write.
* **Idempotent records** (absolute deadlines, resulting state) so duplicates, fuzzy snapshots and retries are harmless.
* **Router-granted leases instead of a consensus protocol**: simple and fast failover, but a single coordinator and timing assumptions.
* **Fail closed**: ambiguous shard -> refuse, never guess; the cost is availability.
* **Sync-N proof over quorum reads**: promotion rule R-N+1 gives no-loss without a log-comparison protocol.
* **The rate limiter is a normal key**: replication, WAL, failover and migration come for free; the cost is that async failover can re-admit the unreplicated tail and eviction resets buckets.

## Limitations

See [`docs/design/failure-model.md`](docs/design/failure-model.md) section 3 for the complete, numbered list. In short: single coordinator,
timing-based fencing, async tail loss, sync-N is memory-only, no authentication/TLS, router throughput ceiling,
untested partitions other than node-unreachable, Docker files unrun, and benchmark numbers are single-machine loopback.

## Testing

```bash
./build/distcache_tests                                   # the full suite: 302 tests: 301 pass, 1 skipped by design (a recovery test that needs a particular eviction history),
./build/distcache_tests --gtest_filter='RateLimiter*:TokenBucket*'          # one area
python3 bench/harness/chaos.py --seconds 90 --seed 1      # chaos: real processes, random kills/freezes/router restarts, model-checked
# sanitizers (a 4 GB machine needs -j1):
cmake -S . -B build-tsan -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer" \
      -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread" && cmake --build build-tsan -j1 --target distcache_tests && ./build-tsan/distcache_tests
```

ASan+UBSan and TSan each run the same 302 tests on the final code: 301 pass, 1 skipped by design, **0 sanitizer reports** (`docs/benchmarks/final/sanitizers.txt`). The sanitized `distcache` is built next to the test binary, so the six real-process failover tests run too, with sanitized node processes and report files enabled (none were written). The three real-process failover tests skip themselves when `distcache` is not next to the test binary
(the sanitizer builds), which is why those runs report skips. Every phase's tests were also validated by deliberate
breakages of the code in a scratch copy (Phase 8: 17 mutations, Phase 9: 10, Phase 10: the lost-write fix, the epoch store and the migration repair), and the chaos
harness found and fixed a real wedge ([phase 10](docs/phases/phase10.md)).

## Repository layout

```
src/            engine (store, synced_store), server, RESP, WAL/persistence, hash ring, router, coordinator,
                replication (repl_log, replication, node_replication, record_apply), rate_limiter, main binaries
tests/          gtest suites per area (unit, in-process cluster, real-process e2e, integration)
bench/          loadgen (closed-loop, smart client, rate-limit mode), microbenchmarks, bench/harness/*.py (experiments + chaos)
scripts/        cluster.sh, demo_full.sh, demo_cluster.sh, demo_replication.sh, healthcheck.sh, health_probe.sh, bench scripts
docs/phases/    one document per phase (4-10); docs/design/ (replication, rate-limiter, failure-model); docs/configuration.md
docs/benchmarks/  raw data and generated reports per phase and the final session; docs/demo-output.txt
Dockerfile, docker-compose.yml   (untested)
```
