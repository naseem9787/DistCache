# Phase 9: a distributed rate limiter

Design and proofs: [`docs/design/rate-limiter.md`](../design/rate-limiter.md).
Raw data and the generated report: [`docs/benchmarks/phase9/`](../benchmarks/phase9/).
Code: `src/rate_limiter.*`, `Engine::update` (`src/engine.hpp`, `src/store.hpp`, `src/synced_store.hpp`),
the `RL.*` cases in `src/commands.cpp` and `src/router.cpp`.
Tests: `tests/test_rate_limiter.cpp` (25), `tests/test_rate_limiter_cluster.cpp` (14).

## 1. What it is

A **token bucket whose state is an ordinary DistCache key**. Two commands:

```
RL.ACQUIRE key capacity rate [cost]    take tokens if available (a write)
RL.PEEK    key capacity rate [cost]    the same answer without consuming (a read; use on replicas for dashboards only)

reply: [ allowed (1|0), remaining, retry_after_ms (-1 if allowed), reset_after_ms ]
```

```bash
redis-cli -p 6380 RL.ACQUIRE api:user:42 100 10        # burst of 100, refill 10 tokens/second
# 1) 1  2) 99  3) -1  4) 100
redis-cli -p 6380 RL.ACQUIRE api:user:42 100 10 5      # a request that costs 5 tokens
```

* `capacity` 1..1e9 tokens, `rate` 0.001..1e6 tokens/s (3 decimals), `cost` 1..capacity.
* Integer arithmetic only (micro-tokens): refill is exact, no drift (an hour polled every millisecond at
  1 token/s grants exactly 3,601 tokens).
* A denial writes nothing; an idle bucket expires by itself when it would be full again.
* **Race-free:** the read-modify-write runs under the key's shard lock (`Engine::update`). 16 threads on one
  key with a frozen clock are granted exactly the capacity, on both locking engines.
* **Distributed for free:** the bucket is logged and replicated like a `SET` (as the resulting state, with an
  absolute deadline), so the WAL, async and sync replication, full resync, failover and live migration carry it.
  A bucket lives on one shard (a hash tag co-locates several); there is no cross-shard limit.

## 2. Guarantees (details and the failure table are in the design doc)

| situation | result |
|---|---|
| normal operation | linearizable per bucket; exact |
| `RL.PEEK` on a replica | stale/advisory; `RL.ACQUIRE` on a replica is refused (`-READONLY`) |
| failover with `--repl-sync N` | no consumed token is forgotten |
| failover with async replication | up to the **unreplicated tail** can be granted twice (tested: over-admission == the tail, exactly) |
| clock behind after a failover | stricter (no refill for the skew); clock ahead: at most one extra capacity |
| no safe primary / partition | the shard refuses (`-ERR shard unavailable`); the caller chooses fail-open or fail-closed |
| lost reply under sync N | the token was consumed, the caller was not told (over-restrictive) |
| `--max-keys` eviction | an evicted bucket is a full bucket: evictions reset limits |

## 3. Tests (39 new; whole suite 294 at the end of this step)

Deterministic, with an injected clock: exact refill at the millisecond boundary, fractional rates, capacity
limit, cost, peek, TTL equals time-to-full, no drift, parameter changes, argument validation, a randomised
property (granted <= capacity + rate x time, exactly, over 40 random schedules), clock behaviour (backwards,
slightly ahead, far ahead, forward jump, jitter), concurrency (16 threads, frozen and running clocks, 64 keys),
replication (byte-identical state, replicas refuse to consume, duplicated delivery, replica lag, sync-N holds
the reply back), WAL recovery (refill continues across downtime), failover (sync: nothing forgotten; async:
over-admission == tail; clock behind/ahead; unavailable shard; fenced primary), routing (300 buckets over 3
shards, pipelined order), and live migration (buckets survive; buckets first touched *during* a migration are
not orphaned). **Ten deliberate breakages** of the implementation (non-atomic update, off-by-one refill,
backwards `last_ms`, denial that writes, uncapped refill, unstored clock repair, free tokens, replica serving
writes, TTL, router migration branch) are each caught.

One real bug was found by these tests while writing them: a clock-fault repair that was not *stored* on a
denial left a bucket with a far-future timestamp frozen forever (every denied call recomputed the repair from
scratch). The repair is now stored even when the call is denied.

## 4. Benchmarks

`bench/harness/bench_ratelimit.py --suite all --repeats 5` (about 70 minutes; 340 runs, none failed).
Same method as Phases 6-8: fresh processes per run, pinned CPUs (each node one physical core = 2 hyper-threads,
router and load generators on their own cores), repeats in shuffled order, health probe before and after (both
healthy). **Single machine, loopback, closed-loop clients**, so network cost is absent and latency percentiles
understate queueing a real client would see. Full tables, min..max and CV for every cell:
[`report.md`](../benchmarks/phase9/report.md).

Median req/s, 50 clients, pipeline 16 (pipeline 1 in the report):

| workload | single (2 thr, 1 core) | single (4 thr, 2 cores) | 3-node, smart client | 3-node, router | async +1 replica | sync 1, +1 replica |
|---|---:|---:|---:|---:|---:|---:|
| **allow** (100k keys, every call writes) | 475.9k | 828.9k | 924.1k | 333.8k | 226.7k | 259.5k |
| **deny** (100k keys, >=96 % refused) | 838.7k | 1,724.1k | 1,825.9k | 328.0k | 697.3k | 638.8k |
| **hot** (one key, 1000 tokens/s) | 1,046.6k | 1,182.3k | 1,005.8k | 702.0k | 1,001.8k | 898.0k |

Reading it (facts first, then hypotheses):

* **Accuracy and safety (measured).** On the hot key, tokens granted divided by the fluid limit
  `capacity + rate x time` was **0.9997-1.0000 in every topology** (0.995-0.999 with the smart client), and
  the safety bound held in **all 60 hot-key runs**, direct, through the router, and with replicas. The limiter
  is exact, not merely conservative.
* **Many keys scale with nodes; one key does not (measured).** At pipeline 1 the 3-node cluster with a smart
  client does 408k req/s against 132k for one node (3.1x on 3x the cores; 1.4x the 2-core single node); at
  pipeline 16, 924k vs 476k (1.9x; 1.1x the 2-core single node). The hot key gets **no** speed-up from more nodes
  (1.01M vs 1.05M): a bucket is one key on one shard behind one lock. *Hypothesis for the sub-linear pipeline-16
  scaling:* each pipelined batch is split across nodes into smaller batches (the Phase 7 finding); not re-measured here.
* **The router is the bottleneck (measured, consistent with Phase 7).** Through the router the 3-node cluster
  does 49k (pipeline 1) and 334k (pipeline 16) on allow: 63 % and 30 % *below one node*, because it is one
  thread per connection relaying synchronously. Use a smart client (or several routers) for throughput.
* **Replication costs what writes cost (measured).** Allowed calls are writes: async +1 replica is 36 % (P1) and
  52 % (P16) below a single node, `sync 1` 48 % and 45 % below; the replication log costs on its own (Phase 8,
  52 % at pipeline 16). Denied calls are reads: at >=96 % denied the cost is 17 % (async) and 24 % (sync);
  on the saturated hot key (mostly denied) 2-4 % (async) and 14-21 % (sync). *Hypothesis for the residual cost
  of the mostly-denied rows:* the first five calls per key in each run are writes that must replicate.
* **Read this table with care.** The "deny" workload at pipeline 1 is *not* mostly denied (76 % for the single
  node, 19 % through the router: with fewer requests per key more of them fall within the first five), so
  topologies are not comparable on that row; the report's `denied` column shows the share for every cell.
  Several cells are noisy and kept as measured: deny/P16 on the 4-thread single node ranged 682k..1,753k (CV 27 %),
  async-replica allow/P1 CV 12 %. The load generators (4 threads on 2 cores) may limit the fastest rows.

Failover (kill -9 of the primary after 40 % of a 200,000-token bucket was granted; 8 clients through the
router; shard = 1 primary + 2 replicas; 5 kills per policy):

| policy | tokens told "allowed" in total, per run | over-admission (told allowed - capacity) |
|---|---|---|
| async | 200,001 / 200,000 / 199,999 / 200,000 / 200,000 | +1, 0, -1, 0, 0 |
| sync 1 | 200,000 / 199,997 / 199,996 / 199,993 / 199,998 | 0, -3, -4, -7, -2 |

With sync 1 the capacity was **never exceeded**; the few missing tokens are ones consumed but never acknowledged
(timeouts during the failover). With async the unreplicated tail was 0-1 tokens in these five kills, because the
replicas were within about a millisecond of the primary; this *does not show async is safe*: the deterministic
test `WithAsyncReplicationTheOverAdmissionIsExactlyTheUnreplicatedTail` shows the over-admission equals the tail
whenever there is one.

## 5. Reproducing

```bash
cmake -S . -B build -G Ninja && cmake --build build
./build/distcache_tests --gtest_filter='TokenBucket*:RateLimiter*'
python3 bench/harness/bench_ratelimit.py --suite all --repeats 5 --out results/myrl   # or throughput | sweep | failover
./build/distcache_loadgen --port 6380 --rl-capacity 100 --rl-rate 50 --keyspace 1 --pipeline 16   # by hand: a hot bucket
```
