# Phase 10: integration, reliability and release

Goal: a reproducible, demonstrable distributed cache, not another pile of features. The method was: inspect
the finished system, break it with realistic combinations of the earlier phases, fix what it found before
adding anything, then package it and measure it once more.

## 1. What was found, and what was done about it

| # | finding | found by | status |
|---|---|---|---|
| 0 | **Acknowledged writes were lost** (a Phase 8 defect, the most serious finding). With `--repl-sync 1` and `--fsync everysec`, a primary that is `kill -9`'d and restarted *before* the router has promoted anyone came back at the same epoch with a shorter WAL (its in-memory tail died with the process). The coordinator re-adopted it, the replicas, which held the acknowledged tail, reconnected and resynchronised **down** to it. In a deterministic real-process test 92 of 1,800 acknowledged writes were lost; in chaos seed 5 every rate-limit bucket was granted 532 tokens against a capacity of 500 and one replica ended 5,435 keys short. | chaos harness (one failing run in 8, not reproducible in 4 reruns), then reproduced on demand by `FailoverE2E.AnAcknowledgedWriteSurvives...` | **fixed** by three rules: the coordinator treats a primary whose lineage changed as failed and promotes a replica, unless the node reports it fsyncs before acknowledging (`ROLE` field 11, `--fsync always`); a node under `--repl-require-lease` serves `SYNC` only while it holds a lease (`-ERR NOTVOUCHED`); and it refuses reads without one (`-NOLEASE`). Mutations that remove the first or second rule make the test fail; the third has its own test. Consequence, documented: a restarted non-fsync primary is never re-adopted, so with too few eligible replicas the shard stays closed |
| 1 | **A shard could wedge forever with every node back.** A promoted primary that crashed while no successor could be elected (`--repl-sync 1` with two replicas needs both) restarted at epoch 0; the coordinator saw a stale node, would not adopt it, and could not promote a replica that was still resynchronising. | chaos harness (seeds 2 and 3 failed to heal) | **fixed** (re-adoption applies only to nodes that fsync before acknowledging, see 0): a promoted primary stores its epoch next to its WAL (`primary.epoch`); regression test `FailoverE2E.ACrashedPrimaryThatRestarts...` and the mutation that removes the store reproduces the wedge; a restarted *older* primary is still demoted (`...StillDemotedNotAdopted`) |
| 2 | A failover in the middle of a live migration stranded keys on the old owner (unreachable cache misses; a rate-limit bucket among them silently reset to full). Phase 7 had documented migration as "not resumable". | writing the failover-during-migration test | **fixed**: a migration that ends with errors stays *pending* (the migrating route is kept so reads still find the keys, other topology changes are refused); `RING RESUME` finishes it, `RING ABORT` gives up. Test `Integration.AFailoverInTheMiddleOfALiveMigration...`; removing the fix strands keys |
| 3 | `--port abc` meant port 0 (a random port), `--threads 0`, `--lease-ms 1` etc. were accepted silently. | review | **fixed**: every numeric flag validated with a message and exit status 2 |
| 4 | Every listener bound `0.0.0.0` while node control commands (`PROMOTE`, `REPLICAOF`, `LEASE`) have no authentication. | review | **mitigated**: `--bind`, default `127.0.0.1` for the binaries; documented as *no authentication* (not fixed) |
| 5 | My own `scripts/cluster.sh` restarted replicas with `--replicaof <initial primary>`: after a failover that node may itself be a replica, and the two follow each other forever. | chaos harness | **fixed in the script and compose file**: nodes start plain, the router assigns roles; documented in `docs/configuration.md` |
| 6 | The chaos harness itself had two bugs: it counted error replies as "resurrected" keys, and treated a restarted but not yet resynchronised node as healthy (so it killed the next node too early, a legitimate "not enough eligible replicas"). | reading the failure output | **fixed in the harness**; the earlier failing runs are not a bug of the system beyond finding 1 |
| 7 | A Phase 9 bug: a clock-fault repair of a rate-limiter bucket was not stored on a denial, freezing the bucket forever. | Phase 9 tests | fixed in Phase 9 |

Two further changes were *measured and rejected*, which is also a result:

* **Replication-log mutex (section 4).** Profiling confirmed the Phase 8 hypothesis. One optimisation was tried,
  helped the case nobody runs and hurt the case that matters, and was reverted.
* The sampled-profile route did not work: over half the samples were unresolved library/kernel time; the
  decisive evidence came from timing the mutex itself and from a standalone microbenchmark.

## 2. Operability

* **Configuration**: flags only, all validated; reference in [`docs/configuration.md`](../configuration.md).
* **Startup / shutdown**: one line per component at startup (recovery summary, replication role, version, bind
  address, port). `SIGTERM`/`SIGINT` stop clients, stop replication, flush and fsync the WAL, print
  `stopped cleanly`, exit 0.
* **Health and diagnostics**: `scripts/healthcheck.sh PORT` (liveness + replica link + router shard health), `INFO`
  sections `# Server` (version, pid, uptime), `# Stats` (incl. `rl_*`), `# Persistence`, `# Replication`; the
  router's `INFO`, `SHARD STATUS` and `SHARD EVENTS`; `RING INFO` shows `migrating` / `migration_pending`.
* **Reproducible local multi-node setup**: `scripts/cluster.sh up|status|health|kill|start|logs|down|clean`
  (2 shards x 3 nodes + router, WAL, sync replication). **Tested.**
* **Docker**: `Dockerfile` + `docker-compose.yml` for the same topology with health checks. **Not run**: the
  development machine has no Docker. The compose file parses; nothing else is claimed.
* **End-to-end demo**: `scripts/demo_full.sh` (routing, replication, rate limiting, `kill -9` failover, rejoin,
  live rebalancing); its output is saved in [`docs/demo-output.txt`](../demo-output.txt). In that run writes to
  the failed shard worked again after 1.3-1.4 s and the rate-limit bucket that was empty before the crash was
  still empty after it.

## 3. Testing

* New tests this phase: 5 (three real-process failover/WAL/TTL/rate-limit tests, two migration-repair tests).
  Whole suite: **302 tests: 301 pass, 1 skipped by design (a recovery test that needs a particular eviction history), in the Release build**.
* **Chaos** (`bench/harness/chaos.py`): a real cluster (2 shards x 3 nodes, WAL on, `--repl-sync 1`, one router),
  four writers (SET, SET EX, DEL) and two rate-limit clients through the router, while it injects, at random and one
  failure per shard at a time (a restarted node counts as down until resynchronised): `kill -9` of a primary or a
  replica, `SIGSTOP`/`SIGCONT` of a primary, `kill -9` + restart of the router. Afterwards it verifies against a
  model of everything the cluster *acknowledged*: every live key readable with the right value, every acknowledged
  delete still deleted, no rate-limit bucket over its capacity, a sane TTL, exactly one primary per shard, replicas
  converged. Errors during the chaos are expected and counted; an unacknowledged write makes no promise.

| run | seconds asked | seconds taken | faults injected | live acknowledged keys verified | acknowledged deletes verified | client errors (expected) | result |
|---|---:|---:|---:|---:|---:|---:|---|
| seed 1 | 90 | 92 | 11 | 130,300 | 13,063 | 5,652 | pass |
| seed 2 | 90 | 95 | 13 | 125,209 | 12,570 | 6,349 | pass |
| seed 3 | 90 | 93 | 9 | 132,758 | 13,422 | 8,315 | pass |
| seed 4 | 90 | 94 | 15 | 124,925 | 12,682 | 15,447 | pass |
| seed 5 | 90 | 93 | 13 | 116,724 | 11,747 | 11,204 | pass |
| seed 6 | 90 | 94 | 14 | 124,261 | 12,540 | 15,012 | pass |
| seed 7 | 90 | 96 | 14 | 133,543 | 13,367 | 6,074 | pass |
| seed 8 | 90 | 93 | 12 | 127,150 | 12,866 | 8,742 | pass |
| seed 5, repeat 1 | 90 | **4,980** | 2 | 10,503 | 1,049 | 267 | host suspended ~83 min; buckets at 504 = 500 + 0.001 x 4,980 (exact); see notes |
| seed 5, repeat 2 | 90 | 95 | 13 | 113,455 | 11,377 | 9,606 | pass |
| seed 5, repeat 3 | 90 | 92 | 13 | 116,466 | 11,717 | 9,333 | pass |

Failing runs that came *before* the fixes are kept and explained, not deleted: two runs of seed 5 on earlier code (one of 32,527 s because the
host slept overnight; one with a single key missing that was most likely an unknown-outcome `DEL` the harness mishandled), the runs where seeds 2 and 3
failed to heal (finding 1, JSON overwritten by their reruns), and the deterministic test that reproduced the lost writes. Details and honest
uncertainty: [`chaos-notes.md`](../benchmarks/final/chaos-notes.md).

* **Sanitizers**: ASan+UBSan and TSan each run the same 302 tests on the final code: 301 pass, 1 skipped by design, **0 sanitizer reports** (`docs/benchmarks/final/sanitizers.txt`). The sanitized `distcache` is built next to the test binary, so the six real-process failover tests run too, with sanitized node processes and report files enabled (none were written).
* **Mutation checks**: the epoch persistence and the migration repair were each validated by removing them
  (the tests then fail in the way the chaos run failed); Phase 9's ten and Phase 8's seventeen stand.

## 4. The replication log: investigated, not optimised

The Phase 8 benchmark showed the log alone (no replica) costing about half of pipelined write throughput, with the
mutex as the suspect. Phase 10 measured instead of guessing
(`scripts/profile_repl_log.sh`, `bench/bench_repl_log.cpp`, outputs in `docs/benchmarks/final/`):

| measurement | result |
|---|---|
| `ReplicationLog::on_set` alone, 1 thread | 173 ns per record (5.8M records/s) |
| same, 2 threads / 4 threads | **2.7M / 2.6M records/s in total** (740 / 1,540 ns per record per thread): the log collapses under contention instead of scaling |
| inside the engine, no replication vs log only (diagnostics build, 4 threads, pipeline 16, SET only, 3 interleaved rounds) | shard-lock hold 640-670 ns -> 2,250-2,290 ns per write; throughput 1.87-1.98M -> 0.87-0.89M writes/s |
| of the added ~1.6 us per write | payload encoding 1.4 us, waiting for the log mutex 0.6 us, holding it 0.55 us (the encode figure is inflated by contention on the allocator and caches; one thread encodes in ~100 ns) |
| with a replica attached | mutex wait 1.7 us per write; throughput ~0.55M writes/s |

Reading: the hypothesis was right in substance (one shared structure, cache-line ping-pong, a ceiling near 2.5M
records/s) and wrong in detail (the lock itself is the smaller part). Two experiments:

1. **Free the trimmed payloads after unlocking.** +10 % in isolation, +14 % end to end with the log only, **no gain
   with a replica attached and about 10x more async replica lag at pipeline 16** (median ~41,000 records instead
   of 1,000-17,000; the faster writers starve the sender thread). Lag is the tail an async failover loses.
   **Reverted** (A/B in `repl-log-ab.txt`; the session run with the change is kept in
   `replication-with-eviction-change/`).
2. **Smaller sender batches** (256 / 32 / 8 KiB): no effect on throughput with a replica (560-575k writes/s).
   Reverted to the default.

What would actually fix it is a lock-free sequenced ring (offsets from a `fetch_add`, slots published
in order). That re-opens the Phase 8 arguments about the consecutive-prefix property and snapshot offsets, so it
was **not** attempted in a release phase.

## 5. Final benchmarks

The finished code, one session (health probe healthy before and after; a first launch was aborted because the probe reported a degraded
machine right after the sanitizer builds, see `docs/benchmarks/final/session-notes.txt`). `bench/harness/bench_repl.py --suite all --repeats 5`,
295 runs, none failed; full tables in [`docs/benchmarks/final/replication/report.md`](../benchmarks/final/replication/report.md), raw data next to it.

| | single node | log only | async +1 replica | async +2 | sync 1, +1 | sync 1, +2 | sync 2, +2 |
|---|---:|---:|---:|---:|---:|---:|---:|
| SET, pipeline 1 (req/s) | 449.1k | 399.0k (-11 %) | 237.7k (-47 %) | 177.2k (-61 %) | 130.3k (-71 %) | 118.8k (-74 %) | 99.9k (-78 %) |
| SET, pipeline 16 | 2,118k | 1,012k (-52 %) | 594k (-72 %) | 471k (-78 %) | 587k (-72 %) | 465k (-78 %) | 464k (-78 %) |
| 80 % GET, pipeline 16 | 2,135k | 1,774k (-17 %) | 1,006k (-53 %) | 801k (-62 %) | 991k (-54 %) | 794k (-63 %) | 779k (-64 %) |

* **Latency (SET, pipeline 1, p50 / p99):** single 88 / 287 us; async +1 135 / 1,081 us; `sync 1` +1 336 / 1,016 us. The price of the acknowledgement policy is
  visible at shallow pipelines (`sync 1` is 45 % below async at pipeline 1) and vanishes at pipeline 16 (587k vs 594k): the replication pipeline, not
  the ack wait, is the limit.
* **Replication lag** (async, +1 replica, primary's view, SET): median 228 records at pipeline 1, 3,237 at pipeline 16, 55,539 at pipeline 64. With a
  second load generator reading from the replica the replica **cannot keep up** (cut off by the backlog budget and resynchronised in 5 of 5 runs at pipeline
  16 and 64); the "max lag" figures of 13-18M records there are samples taken right after such a cut, not a real queue.
* **Failover** (`kill -9` of the primary under write traffic, 5 kills per row): writes possible again after **2.61 s** (lease 2 s / probe 500 ms, async), 2.59 s
  (`sync 1`), **0.67 s** / 0.65 s with lease 500 ms / probe 100 ms; the old primary is a synchronised replica again after 0.17-0.51 s; **0 of 25,542
  acknowledged writes missing**. The window is the lease wait `L(1+eps)+guard` plus detection - the price of never allowing two primaries.
* **Resynchronisation:** a full resync moves ~1.3-1.5M keys/s (1M keys in 0.79 s); a dropped link under sustained writes is back ONLINE in 0.05-0.15 s
  (the 5 "full" counts there come from later backlog overflows, not from the drop).
* **Recovery:** the WAL/snapshot recovery numbers of Phase 6 (1M keys in 1.2 s from the log, 0.64 s from a snapshot) were not re-measured; nothing in
  Phase 7-10 touches that path.
* **Rate limiter regression check** (`ratelimit-regression/`, 3 repeats, single node): allow 137.5k / 462.6k (Phase 9: 132.5k / 475.9k), deny
  159k / 828k (153k / 839k), hot key 189k / 1,019k (180k / 1,047k) req/s at pipeline 1 / 16: within the run-to-run spread, and all bound checks held.
* **Compared with the earlier Phase 8 session** the numbers agree within a few percent (single node 2.12M vs 2.33M req/s at pipeline 16, a slower
  session; log only -52 % vs -52 %; failover 2.6 s vs 2.6 s).
* **Noisy cells are kept:** async +1 replica 80 % GET (CV 8.7 %, range 803k-1,037k), `sync 1` +1 80 % GET (CV 5.5 %).

What the numbers do not say: this is one machine over loopback, so a real network makes `sync N` cost more; the load generators and the replicas share the chip with the
primary; the closed-loop clients understate queueing delay.

## 6. Not done / not verified

* Docker Compose was never run (no Docker available). The tested equivalent is `scripts/cluster.sh`.
* Network partitions other than "node unreachable by the router" are untested.
* The chaos harness runs 90-second sessions on one machine over loopback; it found a real defect and passes
  after the fix, but it is evidence, not a proof.
* No authentication/TLS; the router is a single coordinator and a throughput ceiling; async still loses its tail.
  The complete list is [`docs/design/failure-model.md`](../design/failure-model.md).
