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
  Whole suite: **@@TESTS@@**.
* **Chaos** (`bench/harness/chaos.py`): a real cluster (2 shards x 3 nodes, WAL on, `--repl-sync 1`, one router),
  four writers (SET, SET EX, DEL) and two rate-limit clients through the router, while it injects, at random and one
  failure per shard at a time (a restarted node counts as down until resynchronised): `kill -9` of a primary or a
  replica, `SIGSTOP`/`SIGCONT` of a primary, `kill -9` + restart of the router. Afterwards it verifies against a
  model of everything the cluster *acknowledged*: every live key readable with the right value, every acknowledged
  delete still deleted, no rate-limit bucket over its capacity, a sane TTL, exactly one primary per shard, replicas
  converged. Errors during the chaos are expected and counted; an unacknowledged write makes no promise.

@@CHAOS_TABLE@@

* **Sanitizers**: @@SAN@@
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

@@FINAL_BENCH@@

## 6. Not done / not verified

* Docker Compose was never run (no Docker available). The tested equivalent is `scripts/cluster.sh`.
* Network partitions other than "node unreachable by the router" are untested.
* The chaos harness runs 90-second sessions on one machine over loopback; it found a real defect and passes
  after the fix, but it is evidence, not a proof.
* No authentication/TLS; the router is a single coordinator and a throughput ceiling; async still loses its tail.
  The complete list is [`docs/design/failure-model.md`](../design/failure-model.md).
