<!-- This file is one section of the original single-page README, moved here unchanged apart from heading levels and link paths. Phase 8 onward were written as separate documents. -->

# Phase 5: durability (write-ahead log, snapshots, crash recovery)

```bash
./build/distcache --dir ./data --fsync everysec          # recover from ./data, then log to it
redis-cli -p 6380 SET session:1 abc EX 3600
redis-cli -p 6380 SAVE                                   # snapshot + compact the log
redis-cli -p 6380 INFO                                   # "# Persistence" section: fsync policy, log size, snapshots
```

Persistence is off unless `--dir` is given. Flags: `--dir PATH`, `--fsync always|everysec|no`
(default `everysec`), `--snapshot-mb N` (compact when a log segment passes N MiB, default 64,
0 = never).

## 1. Design

```
 client SET ──► worker ──► engine.set()  ── under the shard lock ──┐
                                                                    │ observer callback (same critical section)
                                                                    ▼
                                           encode record ► Wal buffer (one memcpy, short mutex)
                                                                    │
                          flusher thread: write() every <=10 ms, fdatasync per policy
                                                                    ▼
   data dir:   LOCK   wal-0000000007.log   wal-0000000008.log   snapshot-0000000007.dcs
```

**Record format.** `[u32 length][u32 CRC-32][payload]`, little endian, in files that start with
a magic string. The payload is a RESP command, so recovery reuses the Phase 1 parser. The CRC is
what separates a complete record from a *torn* one: a crash can leave the last `write()` half on
disk. Recovery reads until the first record whose length or checksum is wrong and ignores the
rest of that file.

**What is logged:** `SET key value [PXAT unix_ms]`, `DEL`, `PEXPIREAT`, `PERSIST`. Reads and
expirations are not logged. Evictions are logged as `DEL`.

Design decisions, and the failure each one prevents:

| Decision | Failure it prevents |
|---|---|
| **Absolute deadlines.** `SET k v EX 60` is logged as `PXAT <wall-clock ms>` | The engine's clock is `steady_clock`, which is meaningless after a restart. A relative TTL replayed later would give keys a fresh lease (or the wrong one). Downtime must count against the TTL |
| **Append to the log inside the engine's lock** (observer callback) | Two threads `SET` the same key. If logging happened after the lock was released, the log could say A then B while the engine applied B then A, and recovery would end with a different value than the live server had. Tested with 8 threads on 20 keys: recovered state equals live state exactly |
| **Evictions logged as `DEL`** | Reads are not logged, so replay's LRU order differs from the live one. Without the `DEL`, replay evicts a different key (a test reproduces this: set A,B,C, read A, set D; live evicts B, naive replay evicts A) |
| **An expired `SET` on replay also deletes the key** | An older value of the key would be resurrected |
| **Snapshot = rotate the log, then walk the engine** | Described below |
| **New log segment on every start** | A torn tail in the previous segment can never swallow new writes |
| **Atomic snapshot publish** (write temp, `fdatasync`, `rename`, fsync the directory) and a `SNAPEND count` trailer | A crash mid-snapshot leaving a half-written file that recovery would trust |
| **Directory lock** (`flock`) | Two servers appending to the same log |
| **Fail-stop on I/O error** (abort) | After a failed `fdatasync` the kernel may have dropped the dirty pages, so a retry that "succeeds" would silently lose data, and acknowledging more writes would be lying. (During benchmarking the server aborted when `/tmp`, a 1.9 GB RAM-backed tmpfs, filled up. I did not capture its message, so I cannot say whether that was this path or plain memory exhaustion, since a full tmpfs also consumes RAM) |

**Fsync policies**

| Policy | A client is answered when | Survives process crash | Survives power loss |
|---|---|---|---|
| `always` | the record has been `fdatasync`ed (group commit: concurrent writers share one fsync) | everything acknowledged | everything acknowledged |
| `everysec` | the engine applied it. `write()` within about 10 ms, `fdatasync` once a second | all but the last ~10 ms | all but the last ~1 s |
| `no` | the engine applied it. `write()` within about 10 ms, no fsync until clean shutdown | all but the last ~10 ms | whatever the OS had flushed |

**Snapshots are fuzzy, and that is correct.** The walk visits shards one at a time while writes
continue, so the file is not a point-in-time image. It does not need to be: step 1 rotates the
log to segment R *before* the walk, and recovery loads the snapshot and then replays segments
`>= R`. Every logged operation is an idempotent absolute-state operation (set a value, delete,
set a deadline), so re-applying records whose effect the snapshot already contains converges to
the same final state. Segments `< R` are deleted only after the snapshot is durable.
(Tested with 6 writer threads running while a snapshot thread loops every 5 ms.)

**Lock order.** engine (shard) lock → `Wal::mu_`, taken by the observer. `Wal::io_mu_` is never
held while taking an engine lock. The flusher takes `io_mu_` then `mu_` briefly; appenders only
ever take `mu_`, so a slow fsync lets the in-memory buffer grow instead of blocking writers.

## 2. Tests

145 tests in total (Phase 4 had 84): 144 pass and 1 is skipped on purpose (the eviction-order
scenario cannot apply to per-shard LRU).

| Area | What is checked |
|---|---|
| Record format | CRC-32 against the standard check value. **Torn file at every byte offset**: exactly the complete records come back, `Corrupt` unless the cut is on a record boundary. **Every single-bit flip** in a record is detected. Wrong magic and a garbage 4 GiB length are rejected without allocating |
| WAL writer | 8 threads x 500 appends all intact and in per-thread order. Under `always`, the record is in the file when `commit()` returns (verified by reading the file from outside). Group commit never does more fsyncs than records. Policy `no` does no fsync until close. Rotation seals segments. Refuses to overwrite an existing segment |
| Recovery, run for all 3 engines | Every operation type survives a restart. **Deadlines are absolute across downtime** (10 s TTL, 4 s down, 6 s left). A key overwritten with an expired value does not resurrect the old value. Evictions replay. Torn tails at every byte of the last two records give a prefix of the writes, and the server keeps working and later writes are not lost. A flipped bit in the middle stops replay there with a warning |
| Snapshots | Snapshot + later writes recovers. Repeated snapshots leave one snapshot and no stale segments. Deadlines survive a snapshot. Incomplete snapshots, a count mismatch and a leftover temp file are ignored (not trusted) |
| Concurrency | 8 writers on 20 keys with set / TTL set / del / expire / persist, then `close`, then recover into a fresh engine: **equal to the live final state**, for global and sharded engines, `always` and `everysec`, with and without a capacity. Snapshots taken while 6 writers run still recover the final state |
| Server, end to end | Under `fsync=always` over real TCP: every acknowledged write is in a copy of the data directory taken while the server is still running with no shutdown |
| Operations | A second process cannot open the same directory. Automatic snapshot compacts a large log. `SAVE` and `INFO` |

**Do the tests actually catch bugs?** I deliberately broke the implementation in 7 ways, one at
a time, and ran the suite:

| Injected defect | Caught by |
|---|---|
| relative TTL written to the log as if absolute | 11 tests |
| CRC never checked | 4 tests (bit-flip and mid-file corruption) |
| expired `SET` does not delete the old value | 3 tests |
| `PERSIST` not logged | 7 tests |
| a torn header treated as a clean end of file | the every-byte torn-tail test |
| `fsync=always` never waits | the on-disk-at-commit test |
| **evictions not logged** | **not caught at first (0 failures)** |

The last one was a real gap: my eviction test replayed the same insertions with the same
capacity, so it passed even without the logged `DEL`s. Eviction logging only matters when read
history differs, so I added a test for exactly that scenario, and it now fails under that
defect. All 7 are caught.

Sanitizers on the final code (3 shuffled repeats each of 144 passing tests): AddressSanitizer
(+Leak), UndefinedBehaviorSanitizer, ThreadSanitizer: **0 findings**. Release: 10 shuffled
repeats, all pass.

## 3. Crash test (real `kill -9`)

`scripts/crash_test.py` starts the real server, writes from 4 connections while recording every
write the server *acknowledged*, sends `SIGKILL` at a random moment (0.3-1.2 s), restarts, and
looks up every acknowledged key. 25 kills per row, data on a real ext4 disk, sharded engine:

| policy | acknowledged writes | **lost** | worst single kill | holes | wrong values |
|---|---|---|---|---|---|
| `always` | 7,011 | **0** | 0 | 0 | 0 |
| `everysec` | 219,689 | 1,702 | 219 | 0 | 0 |
| `no` | 213,497 | 1,360 | 124 | 0 | 0 |
| `always`, snapshot every 1 MiB (kills land around snapshots) | 6,186 | **0** | 0 | 0 | 0 |
| `everysec`, snapshot every 1 MiB | 237,726 | 1,467 | 174 | 0 | 0 |

* **`always` never lost an acknowledged write.** The sample is small (13k writes) because
  `always` is slow here (section 4).
* `everysec` and `no` lose a bounded tail on a process kill: at most a couple of hundred writes,
  matching the ~10 ms flush interval at tens of thousands of writes per second. Under a plain
  process crash they behave the same; they differ only on power loss.
* **"holes" is 0 everywhere:** whenever something was lost it was a suffix. A write is never
  missing while a later one survived, which is what a log prefix should give.
* **Limit:** `SIGKILL` kills the process but the operating system survives, so data already
  handed to `write()` is not lost. This measures the process-crash window. It cannot simulate
  power loss. That guarantee rests on `fdatasync` and is covered by the unit tests, not by this
  script.

## 4. What durability costs (measured)

> **Superseded by Phase 6.** These numbers were taken with `redis-benchmark` in a different
> machine state, before the log-path optimizations and before group commit was made
> asynchronous. The bullet about the weak group commit and the unprofiled overhead is
> addressed (and quantified) in Phase 6 §5.6 and §6; use those tables.

SET, 50 clients, 64-byte values, 4 workers, sharded engine, data on ext4. Median of 3 runs of at
least 6.5 s each (`always`: at least 55 s each); throughput in thousands of requests/second
[min-max], latency in ms p50 / p99.

| config | pipeline 1 | pipeline 16 |
|---|---|---|
| no persistence | 322.9k [292-333] 0.087 / 0.471 | 1713.3k [1408-1846] 0.407 / 1.127 |
| `--fsync no` | 349.9k [235-353] 0.079 / 0.367 | 542.4k [419-861] 0.663 / 11.167 |
| `--fsync everysec` | 221.8k [220-268] 0.087 / 0.855 | 590.1k [490-772] 0.671 / 7.135 |
| `--fsync always` | **0.4k** [0.3-0.4] 119.9 / 495.1 | **5.0k** [4.5-5.2] 141.8 / 368.1 |

Group commit under `always`: 2.0 log records per fsync at pipeline 1, 32.0 at pipeline 16.

What this says, and what it does not:

* **`always` is very expensive on this machine: about 0.4k writes/s unpipelined.** Dividing
  throughput by records-per-fsync gives roughly 200 fsyncs/s, so one `fdatasync` here costs
  about 5-6 ms (a virtualized disk; a real SSD with a power-loss-protected cache is far faster,
  and one without such a cache is not). Absolute numbers are specific to this environment.
* **The group commit is weaker than it could be.** Each worker thread *blocks* while its writes
  become durable, so at most 4 writers are ever waiting and the fsync is shared by about 2.
  Replies are not deferred, because the worker's event loop cannot continue while it waits. The
  fix is to park the reply and keep serving other connections until the flusher publishes the
  durable sequence number, which would let all 50 clients share one fsync. Not implemented.
* **Logging alone costs about 3x at high write rates**, with no fsync involved: at pipeline 16,
  `no` and `everysec` reach about a third of the no-persistence throughput. At pipeline 1 the
  network dominates and the difference is within the run-to-run noise (the `no` row is not
  really faster than no persistence; its range overlaps).
  I fixed two defects that were doubling this (a buffer that regrew from empty under the
  lock; several temporary allocations per record, inside the shard lock), which took it from
  about 300k to about 540-590k. What remains has **not been profiled** (no profiler was
  available). The likely causes are the single shared log buffer and mutex, which put one
  serialization point back across all shards, and encoding inside the shard lock. Both are
  inherent to this design and are the first things to try next.
* At pipeline 16, p99 latency is 11.2 ms (`no`) and 7.1 ms (`everysec`) versus 1.1 ms without
  persistence. I did not isolate the cause; the flusher thread competing with the workers for
  the same CPUs, and writers queuing on the single log buffer, are the candidates.

## 5. Recovery time (1M `SET` commands, 632k distinct keys)

| source | size on disk | time to start serving |
|---|---|---|
| log only (1,000,000 records replayed) | 109 MiB | 1.07 s, 1.11 s, 1.17 s |
| snapshot (compacted by `SAVE`) | 69 MiB | 0.57 s, 0.58 s, 0.62 s |

After every recovery the key count equalled the pre-shutdown count. The snapshot is about 2x
faster to load and about 40% smaller because overwritten values are gone. (Snapshot load
verifies the file first and applies it second, so it reads it twice; it is a simple way to
never apply a damaged snapshot.)

## 6. Known tradeoffs and limitations

* **Reads are not logged**, so LRU recency is not preserved across a restart. Recovery keeps the
  right *set* of keys, in an arbitrary recency order.
* **The snapshot copies a shard (or, in `global` mode, the whole dataset) while holding the
  lock**, then writes outside it. That is a pause and a temporary memory spike proportional to
  the shard. Redis avoids it with `fork` and copy-on-write; this design has no equivalent.
* **No backpressure on the log buffer.** If the disk cannot keep up with the writers, the
  in-memory buffer grows.
* **A write or fsync error aborts the process** (by design, see the table). A production system
  might instead fail writes with an error and keep serving reads.
* **Only the newest valid snapshot is used.** A damaged *segment* in the middle stops replay of
  that segment and everything after it in that file is lost (with a warning); later segments are
  still replayed.
* **Wall-clock jumps** (NTP steps, manual changes) shift logged deadlines.
* **`everysec` / `no` can lose acknowledged writes on a crash.** That is their definition. Only
  `always` promises otherwise, and only to the extent that `fdatasync` is honest on the storage
  below it.
* **Unit tests write to `/tmp`**, which on this machine is a RAM-backed tmpfs where fsync does
  nothing. They verify logic (ordering, torn tails, recovery), not fsync latency. The benchmarks
  and the crash test use a real disk.
* Replication, authentication, and the rest of Phase 6+ are not part of this phase.

## 7. Files

```
src/wal.*            record framing + CRC, RecordReader, Wal writer (group commit, rotation)
src/persistence.*    recovery, observer (logging), snapshots, directory lock, SAVE/INFO hooks
src/engine.hpp       + MutationObserver, for_each
src/store.hpp        + observer callbacks (set/del/expire/persist/eviction), for_each
src/synced_store.hpp + observer forwarding, per-shard copy-then-visit iteration
tests/test_wal.cpp, tests/test_persistence.cpp, + end-to-end durability test in test_server.cpp
scripts/crash_test.py, bench_fsync.sh, summarize_fsync.py, bench_recovery.sh
docs/benchmarks/     crash-test.txt, fsync.csv, fsync-summary.txt, recovery.txt
```
