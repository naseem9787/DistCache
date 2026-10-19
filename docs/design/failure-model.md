# Failure model: what DistCache handles, refuses, and cannot handle

DistCache is a **single-coordinator, lease-fenced primary/replica system**. It is *not* consensus (no Raft or
Paxos), and it does not claim zero data loss or production-grade split-brain prevention. This document lists
each failure, what the system does, and which test or measurement backs the claim. Detailed arguments:
`replication.md` (Phase 8) and `rate-limiter.md` (Phase 9).

## 1. Handled (with evidence)

| failure | behaviour | evidence |
|---|---|---|
| a replica disconnects and reconnects | partial resync from its offset; full resync only if the log was trimmed past it | `test_replication`, benchmark `resync` |
| duplicated / dropped / reordered replication records | offset gate: duplicates skipped, gaps force a resync | `test_replication` (fault injection, property test) |
| a replica too slow for the backlog | cut off and fully resynchronised; writers never blocked | `test_repl_log`, lag benchmark |
| primary `kill -9`, async | best replica promoted after the old lease must have expired; the **unreplicated tail is lost** | `test_failover_router`, failover benchmark |
| primary `kill -9`, `--repl-sync N` | **no acknowledged write is lost** if the coordinator can reach at least R-N+1 eligible replicas | proof in `replication.md` §1, `SyncOneNeverLosesAnAcknowledgedWriteAcrossAFailoverUnderLoad`, `bench/harness/chaos.py` |
| primary frozen (SIGSTOP) and woken after replacement | its lease expired, so it cannot acknowledge writes; it is demoted and resynchronised | `FailoverE2E.AFrozenPrimary...` |
| old primary restarted from its WAL | demoted, full resync, its stale data is discarded (and the WAL gets a `FLUSHALL` so it cannot resurrect) | `FailoverE2E.APrimaryRestartedFromItsWal...`, `...TtlsDeletesAndRateLimiterState...` |
| the primary is killed and restarted BEFORE the router promotes anyone | the restarted node holds no lease, serves neither replicas nor reads, and (unless it fsyncs before acknowledging) is not re-adopted: a replica that holds the acknowledged tail is promoted. *This lost acknowledged writes before Phase 10* (found by the chaos harness) | `FailoverE2E.AnAcknowledgedWriteSurvivesAPrimaryThatIsKilledAndRestartedBeforeAnyPromotion`, `Integration.ALeaselessPrimary...`, `bench/harness/chaos.py` |
| not enough eligible replicas | no promotion; the shard is **unavailable** (reads and writes refused) | `WithTooFewEligibleReplicas...` |
| the router restarts | learns epochs from the nodes, waits out any lease it may have granted; `--state-file` remembers the highest epoch so a stale primary is not adopted | `ARestartedRouterLearnsTheEpoch...`, `ARestartedCoordinatorRefuses...` |
| a failover during a live migration | failed steps are counted; the migration stays **pending** (reads fall back to the old owner) and `RING RESUME` finishes it | `Integration.AFailoverInTheMiddleOfALiveMigration...` |
| TTL across failover / WAL restart | deadlines are absolute; expired keys stay expired, live ones keep their TTL | `...TtlsDeletesAndRateLimiterState...`, `RouterFailover.ADeadPrimary...` |
| rate-limiter state across failover | exact with `--repl-sync`; bounded by the unreplicated tail with async | `RateLimiterFailover.*` |
| random kills, freezes and router restarts under load | every acknowledged write survives, no resurrection, no limit exceeded, one primary per shard | `bench/harness/chaos.py` (results in `docs/benchmarks/final/`) |
| bad command line | rejected with a message, exit 2 (a typo in `--port` no longer means "random port") | `scripts` / manual |

## 2. Refused on purpose (fail closed)

* A node with `--repl-require-lease` and no valid lease refuses writes (`-ERR ... lease`).
* A replica refuses writes (`-READONLY`) and, while resynchronising, reads (`-LOADING`).
* `--repl-sync N` with fewer than N synchronised replicas refuses writes (`-NOREPLICAS`).
* An unavailable shard refuses **reads too** (`-ERR shard 'x' is unavailable`): with an ambiguous state even a read could be stale.
* A router that remembers a newer epoch than any reachable member refuses to adopt them.
* A topology change while a migration is pending (`-ERR a migration is pending`).

## 3. Cases the current design CANNOT safely handle

1. **The router is the only coordinator.** If it dies, no failover happens: primaries keep serving until their
   leases lapse, then refuse writes (the shard goes unavailable for writes). There is no coordinator quorum.
   Mitigation: restart it (state is recovered from the nodes and `--state-file`). Not mitigated: a router that is
   alive but partitioned from some members, which is **untested**.
2. **Split-brain prevention rests on timing assumptions**: bounded clock *rate* drift (ε, default 5 %), bounded
   message delay (D) and bounded process pause (G, default 500 ms) between a primary's lease check and its reply.
   A pause longer than G at exactly the wrong instant, or a clock running more than ε fast or slow, can let a
   fenced primary acknowledge **one** stale write under async replication. Under `--repl-sync N` the epoch fence
   closes this gap. Wall-clock *jumps* do not matter for the lease (it is measured as an interval), but they do
   for absolute TTLs and for rate-limiter refill (see below).
3. **Async replication loses the unreplicated tail** on failover. Use `--repl-sync N` for no-loss; the price is
   latency at shallow pipelines (benchmarks).
4. **`--repl-sync N` is memory on N other processes, not disk.** If all N replicas and the primary lose power
   together, acknowledged writes are lost unless `--fsync always` was also set on the survivor that is promoted.
5. **A sync-N write that times out is of unknown outcome** (the connection is closed). For a rate-limiter call
   the token may or may not be consumed (never granted twice).
6. **A restarted primary that does not fsync before acknowledging is never re-adopted.** If at the same time too few
   replicas are eligible, the shard stays unavailable until replicas return; there is no operator override, and a
   restarted *replica* returns as an unassigned master that nothing promotes. (Run `--fsync always` on nodes where
   this matters: such a primary is re-adopted because its WAL holds everything it acknowledged.)
   **More simultaneous failures than the policy tolerates** (primary + any replica with R=2, N=1) make the shard
   unavailable until a node returns. The system will not promote on a guess; it also will not heal by itself.
7. **Clocks.** Absolute TTL deadlines and rate-limiter refill assume the machines' wall clocks agree (NTP). A
   node whose clock is ahead after a failover expires keys early / refills buckets once (bounded by one
   capacity); one that is behind expires keys late / refills late (stricter).
8. **Eviction resets rate-limit buckets** (`--max-keys`). A hard limit and LRU eviction do not mix.
9. **Network partitions** other than "node unreachable by the router" are not modelled or tested: asymmetric
   partitions, a primary that can reach the router but not its replicas (it simply fails sync writes), a router
   that reaches replicas but not the primary (fails over after the lease expires; safe by the fencing rules above).
10. **Replica reads can be stale** and a replica that serves heavy reads while the primary is pipelined hard can
    fall far enough behind to be cut off and resynchronised repeatedly (measured in the Phase 8 benchmarks).
11. **No authentication or encryption.** Node control commands (`PROMOTE`, `REPLICAOF`, `LEASE`) are ordinary
    commands on the client port; the default bind is 127.0.0.1 for that reason. Do not expose the ports.
12. **Full resync is a fuzzy snapshot**: a replica serves `-LOADING` while it runs; a snapshot that outlasts the
    backlog budget is retried.
13. **Multi-key atomicity across shards** does not exist (`DEL a b` is independent deletes). A rate-limit
    bucket is one key on one shard.
14. **A migration is not atomic with a crash of the router itself**: if the router dies mid-migration, keys are
    split between old and new owners; restart it with the old node list and re-run the migration.
15. **Resource limits**: the router runs one thread per client connection; the coordinator probes sequentially.
    Neither is built for thousands of shards or connections.
