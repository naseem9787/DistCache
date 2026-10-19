<!-- This file is one section of the original single-page README, moved here unchanged apart from heading levels and link paths. Phase 8 onward were written as separate documents. -->

# Phase 8: replication and automatic failover

Design document with the safety arguments: [`docs/design/replication.md`](../../docs/design/replication.md).
Raw benchmark data and reports: [`docs/benchmarks/phase8/`](../../docs/benchmarks/phase8/).

## 1. Summary

* A shard is **one primary and N replicas**. Every mutation is appended to an in-memory,
  byte-budgeted **replication log** with consecutive offsets, inside the same critical section that
  applies it to the engine, so the log order *is* the apply order. Replicas pull it over their own
  TCP port, apply it with an offset gate (duplicates skipped, gaps force a resync) and acknowledge.
* A replica that falls behind or reconnects continues from its offset (**partial resync**); if the
  log no longer reaches back far enough it takes a **fuzzy snapshot + replay** (**full resync**)
  that provably converges because every logical record is idempotent (absolute TTL deadlines).
* Two acknowledgement policies: **async** (reply when the primary has applied/logged it) and
  **`--repl-sync N`** (hold the reply until the WAL is durable *and* N replicas have acknowledged the
  record). With `sync N` and R replicas, failover cannot lose an acknowledged write (§4).
* **Automatic failover by the router**, which acts as the coordinator: it grants the primary a short
  **write lease**, detects failure, waits until the old lease *cannot* still be alive, promotes the
  most advanced eligible replica under a new **epoch**, re-points the other replicas, and demotes the
  old primary when it returns. When anything is ambiguous the shard **fails closed** (refuses
  reads and writes) instead of guessing.
* Phases 1-7 are unchanged: replication is off unless `--repl-port`/`--replicaof` is given, and
  with no replicated shard configured the router behaves as in Phase 7.

## 2. Architecture and data flow

```
 client ──► router (distcache_router)                      coordinator thread in the router:
              │  ring: key → SHARD NAME (never changes       probe ROLE every probe-ms, renew LEASE,
              │        on failover)                          decide failover, PROMOTE / REPLICAOF
              ▼  shard name → current primary address
         ┌─────────── shard "a" ───────────────────────────────────────┐
         │  PRIMARY  (epoch e, lease)                                   │
         │   command ─► engine lock ─► Engine.apply                     │
         │                  │            └► ObserverChain               │
         │                  │                ├► WAL (Phase 5)           │
         │                  │                └► ReplicationLog.append   │  offset k, same critical section
         │   reply held until: WAL durable  AND  (sync N) N acks ≥ k    │
         │                  AND lease still valid  (release_ok)         │
         │        │ repl port (RESP arrays): R k payload / PING k       │
         │        ▼                                  ▲ ACK k            │
         │  REPLICA 1 … REPLICA n   (read-only, -READONLY on writes)    │
         │   applier: offset gate ─► apply_logical_record ─► engine     │
         └──────────────────────────────────────────────────────────────┘
```

Wire protocol on the replication port: `SYNC replid next epoch replica_id` is answered by
`CONTINUE replid epoch primary_offset` (partial) or `FULLSYNC replid epoch S`, a fuzzy snapshot
(`SET …` records) and `SNAPEND`, and then the stream `R offset payload` and `PING offset`; the
replica answers `ACK offset` (when drained, every 64 records or 20 ms).

Control surface on a node: `ROLE`, `LEASE epoch ms`, `PROMOTE epoch`, `REPLICAOF host port epoch`,
`INFO` (section `# Replication`). On the router: `SHARD STATUS`, `SHARD EVENTS`.

Node flags: `--repl-port`, `--replicaof HOST:PORT` (the *replication* port of the primary),
`--repl-backlog-mb` (default 64), `--repl-sync N`, `--repl-sync-timeout-ms`, `--repl-require-lease`.
Router flags: `--replica SHARD=HOST:PORT` (repeatable), `--lease-ms` (2000), `--probe-ms` (500),
`--probe-timeout-ms`, `--fail-after` (3), `--guard-ms` (500), `--sync-replicas N`, `--state-file`.

## 3. Invariants worth being able to recite

1. **Order = apply order.** The log append happens under the engine lock (lock order engine → WAL →
   log mutex). A reader can never see a mutation whose record does not exist yet.
2. **Offsets are consecutive per lineage (`replid`)**, so "replica has offset k" means "replica has
   exactly records 1..k" (prefix property). Everything about promotion relies on this.
3. **Apply is idempotent.** `SET … PXAT` and `PEXPIREAT` carry absolute deadlines; a record applied
   twice or out of a fuzzy snapshot's overlap gives the same state. The same function
   (`apply_logical_record`) serves WAL recovery and replicas.
4. **Writers never block on replication** (the log trims its oldest records; a replica that needs a
   trimmed record is cut off and resynchronises). Under `sync N` writers wait only for *their* ack.
5. **At most one primary can accept writes at a time, as far as bounded clock-rate drift and message
   delay allow**: a primary writes only while its lease (checked again when the reply is released)
   is valid, the router promotes only after `now ≥ t_ack + L(1+ε) + G`, and a replica that has seen
   a higher epoch makes a stale primary reject it and fence itself.
6. **Promotion picks the highest offset among ≥ R−N+1 reachable, synced, same-lineage replicas** so
   that the promoted replica is guaranteed to hold every write acknowledged under `sync N`
   (pigeonhole proof in the design doc, §1).
7. **A returning old primary is always demoted and fully resynchronised**; its suffix is discarded
   and its WAL contents are superseded by a `FLUSHALL` record, so it cannot resurrect old data.

## 4. Guarantees by acknowledgement policy

| | async (default) | `--repl-sync N` |
|---|---|---|
| Reply sent when | primary applied + logged (and WAL durable per `--fsync`) | …and N replicas acknowledged the record |
| Failover loses an acknowledged write? | **yes, possibly** (the unreplicated tail) | **no**, if the coordinator can reach ≥ R−N+1 eligible replicas; otherwise no promotion |
| Durability of an acknowledged write | WAL policy of the primary | N replicas' **memory** (not their disk) in addition |
| Ack wait timeout | n/a | closes the connection; the outcome of that write is unknown to the client |
| Availability | promotes with one survivor | refuses to promote below R−N+1 survivors |

Other behaviour: a replica serves reads (possibly stale, `-LOADING` while resynchronising);
`-READONLY` on writes to a replica; `-NOREPLICAS` when `sync N` cannot be satisfied; an
unavailable shard refuses both reads and writes; TTLs survive replication and promotion (absolute
deadlines).

## 5. Failure scenarios and what happens

| Scenario | Behaviour | Tested by |
|---|---|---|
| Replica disconnects / restarts | partial resync from its offset; full resync if the log was trimmed past it | `test_replication` |
| Duplicate or reordered delivery | offset gate: duplicate skipped, gap → resync | `test_replication` (fault injection) |
| Replica too slow | cut off when the backlog budget overflows, then full resync; writers unaffected | `test_repl_log`, benchmarks §6.3 |
| Primary `kill -9`, async | promote best replica after lease expiry; unreplicated tail lost | `test_failover_router`, benchmark §6.4 |
| Primary `kill -9`, `sync 1` | no acknowledged write lost under concurrent load | `test_failover_router` |
| Not enough eligible replicas | no promotion, shard unavailable (reads and writes refused) | `test_failover_router` |
| Primary frozen (SIGSTOP), woken (SIGCONT) after promotion | fenced by its expired lease / the higher epoch; cannot accept writes | `test_failover_e2e` (real processes) |
| Old primary restarts from its WAL | demoted, full resync, old data does not resurrect | `test_failover_e2e` |
| Router restarts | learns the epoch from the nodes, waits out any lease it may have granted; `--state-file` remembers the highest epoch | `test_failover_router`, `test_failover_e2e` |
| Failure during Phase 7 migration | migration steps re-read the shard's primary; failed steps are counted, keys stay | `test_failover_router` (rebalance after failover; no deterministic mid-migration kill) |

## 6. Benchmarks

### 6.1 Method and environment

* Machine: i5-11260H (6 cores / 12 threads, hyper-threads are CPU pairs 2k/2k+1), 3.7 GB RAM,
  WSL2 (kernel 6.18), g++ 15.2 `-O3`, loopback network. **Everything runs on one machine**, so the
  network adds no latency and the replicas compete with the primary for the same chip.
* CPU pinning: primary (4 worker threads) on CPUs 0-3, replicas and router on CPUs 4-5 (one
  physical core), load generators on 6-11.
* Closed-loop load generator (`distcache_loadgen`), 50 clients, 64-byte values, 100k keys, uniform;
  8 s measured after 1.5 s warm-up. **5 repetitions**, run in a shuffled order inside one session;
  a fresh process set per run. Medians reported with min..max. The machine health probe passed
  before and after (`env.json`). Latency percentiles are the load generator's, from a closed loop,
  so they understate queueing delay that an open-loop client would see (coordinated omission).
* Every overhead table contains a **single-node control** from the same session; compare rows only
  within one table.
* Reproduce: `python3 bench/harness/bench_repl.py --suite all --repeats 5 --out docs/benchmarks/phase8`
  (about 1 h). Suites: `overhead sweeps lag failover resync`. The first full run produced the
  report in `docs/benchmarks/phase8/report.md`; because two of its lag runs revealed replica
  cut-offs the harness had not recorded, the lag and resync suites were **re-run with the
  counters added** (`lag-rerun/`, `resync-rerun/`). Both versions are kept.

### 6.2 What replication costs on the write path

Median req/s (min..max), p99 latency, 50 clients, direct to the primary:

| topology | SET, pipeline 1 | SET, pipeline 16 | 80 % GET, pipeline 16 |
|---|---|---|---|
| single node (control) | 458.7k, p99 262 µs | 2,331.6k, p99 754 µs | 2,275.6k, p99 770 µs |
| replication log only, 0 replicas | 416.7k (−9 %) | 1,115.5k (−52 %), p99 1,540 µs | 1,929.1k (−15 %) |
| async, 1 replica | 264.5k (−42 %), p99 950 µs | 653.5k (−72 %), p99 2,818 µs; 642-917k | 1,115.6k (−51 %) |
| async, 2 replicas | 194.5k (−58 %) | 508.4k (−78 %) | 870.9k (−62 %) |
| sync 1, 1 replica | 137.7k (−70 %), p50 328 µs | 644.3k (−72 %) | 1,081.4k (−52 %) |
| sync 1, 2 replicas | 125.0k (−73 %) | 506.3k (−78 %) | 881.2k (−61 %) |
| sync 2, 2 replicas | 106.3k (−77 %), p50 434 µs | 506.0k (−78 %) | 855.3k (−62 %) |

Sweeps (SET only; full tables in the report): at pipeline 1 and 1-256 clients, async-1 reaches
about 56 % of single-node throughput at 50 clients and `sync 1` about 30 %; by pipeline 64 both
replicated modes plateau at about 705k req/s against 3.17M for a single node.

What can be said, and what cannot:

* **Measured:** the price of the acknowledgement policy is visible only when the pipeline is
  shallow: at pipeline 1 `sync 1` costs 48 % of async-1's throughput (p50 125 → 328 µs, the extra
  round trip to the replica). At pipeline 16 `sync 1` and async are the same (644k vs 654k): the
  pipeline of replication, not the wait, is the limit. Adding a second replica costs ~22 % more.
* **Measured, surprising:** the replication *log alone* (no replica attached) costs 52 % of
  pipelined write throughput. **Hypothesis, not profiled:** the log append and its single mutex
  sit inside the engine critical section and serialise the four worker threads. This is the largest
  avoidable cost in the design (a per-shard or batched log would be the fix).
* **Measured:** with a replica attached, primary CPU is ~3.0 of 4 cores and the replica ~1 core.
  **Hypothesis:** the sender thread competes with the workers for the primary's four pinned CPUs,
  and the replica's apply loop (~650-700k records/s on its core) caps what async can sustain.
* **One outlier is real and kept:** one of five async-1 pipeline-16 runs reached 917k req/s
  because the replica had been cut off 7 times (backlog overflow) and the primary was
  effectively running unreplicated. That is why that cell has CV 15 %.
* Throughput numbers are for a loopback, single-machine setup; with a real network, `sync` would
  cost more.

### 6.3 Replication lag under sustained load (async, 1 replica, 15 s runs, 5 repetitions)

Primary-side `lag_records` sampled every 200 ms (the primary's view is the authoritative one; the
replica's own `lag_ms` stays 0 because it can only compare against what it has already received).

| pipeline | replica readers | writers req/s | lag, records: median / p99 / max | replica cut off (runs) |
|---:|---|---:|---|---:|
| 1 | no | 259k | 325 / 2,020 / 2,663 | 0 of 5 |
| 1 | yes | 231k | 536 / 2,596 / 2,780 | 0 of 5 |
| 16 | no | 636k | 485 / 10,768 / 32,475 | 0 of 5 |
| 16 | yes | 886k | 387,191 / 14.7M / 14.9M | **5 of 5** |
| 64 | no | 694k | 53,196 / 64,143 / 14.0M | 1 of 5 |
| 64 | yes | 1,016k | 403,834 / 17.1M / 17.3M | **5 of 5** |

(Source: `lag-rerun/`; the first full run, `report.md`, shows the same pattern.) "Readers" is a
second load generator of 20 connections doing pipelined GETs on the replica, 100 % hits.

* A replica doing nothing but applying keeps up at pipeline 1 (hundreds of records of lag) and at
  pipeline 16 (a few hundred median, ~10k p99). At pipeline 64 it runs ~50k records behind in
  steady state; one run exceeded the 64 MiB backlog and was cut off.
* **A replica that also serves heavy reads cannot keep up with a heavily pipelined primary**: the
  reads and the apply loop share its two worker threads; it falls behind until the backlog budget
  overflows, is cut, and fully resynchronises, repeatedly. The 14-17M "max lag" values are the
  samples taken just after such a cut (the new link has acknowledged nothing yet), not a 14M-record
  queue (the drain after load took 0.4 s). The writer throughput *rises* with readers because a cut
  replica stops costing the primary anything. Takeaway: read scaling on replicas is real
  (~690-730k read req/s alongside the writes) but it is paid for with replica freshness, and the
  system recovers by resynchronisation rather than by slowing the primary.
* Not measured: lag across a real network; lag with `sync N` (writers wait, so lag stays bounded by
  the number of in-flight batches).

### 6.4 Failover time (kill -9 of the primary under steady write traffic through the router)

5 kills per configuration, 3-node shard (1 primary + 2 replicas), one write about every 2 ms, then
the old primary is restarted empty on the same ports.

| configuration | writes possible again, median [min..max] | acked writes | acked writes missing | old primary back as synced replica |
|---|---|---:|---:|---:|
| lease 2000 / probe 500 ms, async | 2.62 s [2.61..2.64] | 6,602 | 0 | 0.51 s |
| lease 2000 / probe 500 ms, sync 1 | 2.59 s [2.58..2.61] | 6,260 | 0 | 0.51 s |
| lease 500 / probe 100 ms, async | 0.69 s [0.66..0.71] | 6,605 | 0 | 0.17 s |
| lease 500 / probe 100 ms, sync 1 | 0.67 s [0.65..0.69] | 6,261 | 0 | 0.17 s |

* The unavailability window is almost exactly the **lease wait** `L(1+ε)+G` (2.1 s + 0.5 s;
  0.525 s + 0.1 s) plus detection; it is the price of not allowing two primaries. Shrinking the
  lease shrinks the window and raises the risk that a slow renewal looks like a failure.
* **Zero acknowledged writes were lost in all 20 kills, but this does not demonstrate that async is
  safe**: the writer is slow and the replicas were always caught up. Async *can* lose the
  unreplicated tail; this benchmark did not provoke it. `sync 1` loss-freedom under heavy load is
  covered by `RouterFailover.SyncOneNeverLosesAnAcknowledgedWriteAcrossAFailoverUnderLoad`.

### 6.5 Resynchronisation

| kind | size | median [min..max] | rate |
|---|---|---|---|
| full resync (empty replica joins) | 100k keys | 0.06-0.08 s | ~1.2-1.5M keys/s |
| | 500k keys | 0.33-0.37 s | ~1.3-1.5M keys/s |
| | 1M keys | 0.69-0.72 s | ~1.4M keys/s |
| link dropped under sustained writes (`REPLICAOF` to the same primary) | - | ONLINE again within the 50 ms sampling interval, **partial** resync (1), no full sync needed for the drop itself | - |

(Two repeats of the resync suite, `report.md` and `resync-rerun/report.md`. In both, later
backlog overflows under the 50-client writer cut the replica again in some runs, which is why
some runs also count full syncs.)

## 7. Limitations and remaining risks

* **The router is the single coordinator and a single point of failure.** If it dies, no failover
  happens: primaries keep serving until their leases lapse, then refuse writes (the shard goes
  read-only). It is not Raft; there is no quorum of coordinators.
* **Split-brain is prevented only under assumptions**: bounded clock *rate* drift (ε), bounded
  message delay (D) and bounded process pause (G) between a lease check and its reply. A pause
  longer than G at the wrong instant can produce **one** stale acknowledged write under async; under
  `sync N` the epoch fence closes that gap. Network partitions that isolate the router from only
  *some* members are not tested.
* **Async loses acknowledged writes on failover**; `sync N` is durability in the memory of N
  processes, not on their disks.
* **Replica reads are stale-able** and a replica serving heavy reads can fall far enough behind to
  be cut off (§6.3). Replica-side lag measurement is blind; the primary's view is authoritative.
* **The replication log costs 52 % of pipelined write throughput on its own** (§6.2), and the
  replicated modes plateau near 700k req/s on this machine.
* **Absolute TTL deadlines assume NTP-synchronised wall clocks** between nodes.
* **Failover during a Phase 7 migration** is not tested deterministically; the design keeps keys
  where they are and counts the errors.
* Benchmarks are single-machine, loopback, closed-loop; real networks change the cost of `sync`.
* The three real-process failover tests skip under the sanitizer builds.

## 8. Tests

* New test files: `test_repl_log` (17), `test_replication` (17, including a property test of
  random fault schedules), `test_node_replication` (5), `test_sync_replication` (10),
  `test_failover_node` (11), `test_failover_router` (9), `test_failover_e2e` (3, real processes).
* Whole suite: **255 tests** in the Release build (254 pass, 1 skipped by design); under ASan+UBSan
  251 pass, 4 skipped (3 process tests + 1 by design), 0 reports; under TSan 251 pass, 4 skipped, 0 reports. The process tests skip themselves when `distcache`
  is not next to the test binary (and under the sanitizer builds).
* Every implementation step was followed by deliberate breakages in a scratch copy to confirm the
  tests catch them (offset gate, ordering, sync-N rule, promotion rule, lease wait, epoch memory,
  demotion, availability, lease bound: all caught).

```bash
cmake -S . -B build -G Ninja && cmake --build build
./build/distcache_tests                                   # everything, ~2-4 minutes
./build/distcache_tests --gtest_filter='Repl*:Replication*:SyncRepl*:Failover*:RouterFailover*:FailoverE2E*'
# sanitizers (a 4 GB machine needs -j1):
cmake -S . -B build-tsan -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer" -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build-tsan -j1 --target distcache_tests && ./build-tsan/distcache_tests
```

## 9. Reproducing the demo and the benchmarks

```bash
scripts/demo_replication.sh        # 1 primary + 2 replicas behind the router, kill -9, failover, old primary returns
python3 bench/harness/bench_repl.py --suite overhead --repeats 5 --out results/myrepl   # or sweeps|lag|failover|resync|all
scripts/health_probe.sh            # run before benchmarking: is the machine in its normal state?
# by hand:
./build/distcache --port 7001 --repl-port 7011 --repl-require-lease --repl-sync 1 &
./build/distcache --port 7002 --repl-port 7012 --repl-require-lease --repl-sync 1 --replicaof 127.0.0.1:7011 &
./build/distcache_router --port 6390 --node a=127.0.0.1:7001 --replica a=127.0.0.1:7002 --sync-replicas 1
redis-cli -p 6390 SHARD STATUS
```

## 10. Where to read the code

```
docs/design/replication.md      the proofs: sync-N rule, lease fencing, ordering, full resync
src/repl_log.*                  the log: offsets, lineage, trimming, read_from, append_replicated
src/record_apply.*              apply_logical_record (shared by WAL recovery and replicas)
src/observer_chain.hpp          fan-out WAL then replication log, under the engine lock
src/replication.*               ReplicationPrimary (sender threads, snapshot) and ReplicationReplica (applier, gate)
src/node_replication.*          roles, epochs, leases, ROLE/LEASE/PROMOTE/REPLICAOF, write/read gating
src/commands.cpp, src/server.cpp  command gating; held replies released on WAL durable + replica acks + lease
src/coordinator.*               failover decision: probes, lease wait, eligibility, promotion, reconcile, epoch file
src/router.*                    shard→primary routing, availability, SHARD STATUS/EVENTS, update_route
bench/harness/bench_repl.py     all Phase 8 benchmarks
scripts/demo_replication.sh
tests/test_repl_log.cpp test_replication.cpp test_node_replication.cpp test_sync_replication.cpp
tests/test_failover_node.cpp test_failover_router.cpp test_failover_e2e.cpp
docs/benchmarks/phase8/         raw.jsonl, env.json, report.md, lag-rerun/, resync-rerun/, charts
```
