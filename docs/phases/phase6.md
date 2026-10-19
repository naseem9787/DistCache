<!-- This file is one section of the original single-page README, moved here unchanged apart from heading levels and link paths. Phase 8 onward were written as separate documents. -->

# Phase 6: benchmarking, and what durability really costs

Phases 4 and 5 produced numbers with one-off scripts and a tool (`redis-benchmark`) that cannot
mix reads and writes. This phase replaces them with a reproducible harness, measures every
feature built so far (TTL, LRU, concurrency, each WAL policy, recovery), investigates the
remaining WAL overhead, and makes the optimizations the evidence supports. **Everything below is
a measurement on one machine in one virtualized environment** (next section). Numbers from
different sessions are not mixed unless a table says so.

## 1. Summary

| question | answer (final binary, this machine) | where |
|---|---|---|
| Unpersisted throughput, 50 clients, 4 workers | ~440k req/s unpipelined (p99 0.3 ms), ~2.0M req/s at pipeline 16 (p99 ~1 ms) | §5.1-5.3 |
| Does the sharded design pay off? | 3.6x over one global mutex at 4 workers, pipeline 16; 4 -> 8 workers adds a further 1.4x | §5.3 |
| What does a TTL cost? | **-36 %** throughput on `SET ... PX` (deadline index), measured and profiled | §5.4 |
| What does LRU eviction cost? | -7 % at a 10 % hit rate, within noise at 50 % hit rate | §5.5 |
| What does logging cost? | `fsync no` / `everysec`: **-29 % to -35 %** on pipelined SETs (Phase 5 build: -43 % to -55 %); -5 % to -6 % on an 80 % GET mix | §5.6, §6.2 |
| What does `fsync always` cost? | 10.5-13x faster than Phase 5 on the same disk (deferred acknowledgement), still 8x (pipelined) to 30x (unpipelined) below `everysec`; safe: replies leave only after `fdatasync` | §5.6, §6.5 |
| Recovery: log replay vs snapshot | 1M keys: 1.2 s vs 0.64 s; after heavy overwrites (6.3M records): 12.1 s vs 0.96 s | §5.7 |
| Where does the remaining WAL overhead go? | CPU, not disk, in the steady state: encoding, log-lock hand-off, a longer shard-lock hold, and ~0.7 us per request that I could not attribute. Rare multi-second stalls exist but I could not pin them on the disk | §6 |

## 2. Environment and how to read the numbers

| | |
|---|---|
| CPU | 11th Gen Intel Core i5-11260H, 6 cores / 12 threads, 2.6 GHz base (hypervisor: Microsoft) |
| Memory | 3,776 MiB visible to the VM |
| OS | WSL2 (Ubuntu) on Windows 11, kernel 6.18.33.2-microsoft-standard-WSL2 |
| Compiler / build | g++ 15.2.0, CMake + Ninja, `Release`: `-O3 -DNDEBUG -std=gnu++17` |
| Disk | ext4 on a virtual disk, mounted with `discard`. `/tmp` is tmpfs (fsync is a no-op there; benchmarks that persist use `~/.distcache-data`) |
| Pinning | server on CPUs 0-5 (`taskset`; 0 and 1 are hyperthread siblings), load generator on CPUs 6-11 |
| Defaults | sharded engine, 4 workers, 64 shards, 50 clients, 64-byte values, 100k uniform keys, 80 % GET / 20 % SET, 1.5 s warm-up (discarded) + 8 s measured |

What this environment does to the results:

* **It is a VM on a laptop.** The disk, page cache and CPU are shared with Windows. Across the
  sessions of this phase, page-cache write speed ranged from ~4 GB/s to 190 MB/s, and the same
  code measured 193k req/s under `fsync always` in one session and 65k req/s in a later one (I did
  not isolate whether that was the disk or general host contention). The *ratios* between
  configurations measured in one session were stable; absolute numbers of persistent
  configurations were not.
* **So every comparison is interleaved and same-session**, and `scripts/health_probe.sh` records
  whether the machine is in a representative state (page-cache write speed, CRC speed, dirty
  pages) before and after a session. Two sessions were found degraded (page-cache writes 187 MB/s,
  process RSS 2-3x normal) and the probe's thresholds were derived from them; their data is kept,
  labelled `DEGRADED-SESSION`, and **not used for any conclusion**.
* **The probe is necessary, not sufficient.** One profiling session (`prof2`) passed it (3.1 GB/s
  tmpfs, 1.4 GB/s ext4, CRC 49 ns) and still ran every variant ~4x slower than the clean one
  (§6.3); another (`prof4`) was flagged at the start, narrowly (1.44 GB/s vs the 1.5 threshold),
  and I should have stopped there. A no-persistence control in every session is what caught
  both. Before trusting a session, check that its no-persistence rows are near their usual values
  (about 2.0-2.2M req/s for SET only, pipeline 16).
* **Closed-loop clients** (see below): latency percentiles during a server stall are
  understated compared with a fixed-arrival-rate test.
* No other machine, no real SSD with power-loss protection, no NIC: loopback only. Absolute
  numbers will differ elsewhere; the *shape* (what costs what, and why) is what transfers.

## 3. Harness architecture

```
scripts/bench.sh ──► bench/harness/run_suite.py ──► (per variant, per repeat, interleaved)
                        │  starts   taskset -c 0-5 build/distcache ...     (the binary under test)
                        │  runs     taskset -c 6-11 distcache_loadgen ...  (closed-loop RESP client)
                        │  reads    INFO before/after, /proc/<pid>/task/*/stat (CPU per thread)
                        └► results/<run>/raw.jsonl + env.json
                                   │
                  bench/harness/report.py ──► report.md, charts/*.svg
                                   (median, min..max, CV, p50/p95/p99, Mann-Whitney U, run-to-run table)

bench/loadgen.cpp       closed-loop, duration-based, per-request latency histogram (1.6 % relative error)
bench/bench_wal.cpp     component microbenchmarks: crc, encode, append contention, disk write, whole engine+log
bench/bench_engine.cpp  in-process engine throughput (no sockets), TTL arg
bench/harness/bench_recovery.py   recovery time, log replay vs snapshot
scripts/crash_test.py   kill -9 audit of acknowledged writes
src/diag.* (distcache_diag build)  nanosecond timers + in-process sampling profiler
bench/harness/symbolize.py        profile -> per-thread self / inclusive tables
```

Design decisions, each of which came from a mistake or a doubt:

* **Own load generator.** `redis-benchmark` cannot mix GET and SET, is request-count based
  (short runs are quantized: an earlier number in this project was wrong because of that) and
  its latency definition is undocumented. `distcache_loadgen` is ~560 lines, duration-based, and
  defines latency as *reply read time minus the time its batch started being written*.
* **Validated against two independent references.** `scripts/validate_loadgen.sh` runs both tools on the
  same server (`docs/benchmarks/phase6/loadgen-validation.txt`, 3 runs each): `distcache_loadgen`
  reports ~20 % more throughput than `redis-benchmark` at pipeline 1 (419-433k vs 343-355k) and
  similar or higher at pipeline 16 (1.68-1.97M vs 1.19-1.70M, where `redis-benchmark` itself varied
  by 40 %), with similar latencies. I did not investigate the gap (a leaner client is the
  likely reason), so no cross-tool absolute comparison is made anywhere in this document.
  `scripts/validate_counts.sh` checks the request count against the server's own WAL record
  counter: they agree to within the requests in flight at the stop (50 of 1,117,250 at
  pipeline 1).
* **Interleaving.** A repeat is a round in which every variant (and every binary of a persistent
  variant) runs once, in a seeded random order; round *r+1* starts only after round *r* is
  complete. Slow drift of the machine therefore hits all of them equally instead of favouring
  whichever ran first.
* **Settling.** `--settle N` runs `sync` and sleeps between runs: without it, dirty pages from
  the previous persistent run were being written back during the next run's measurement.
* **CPU pinning** separates server and load generator; per-thread CPU is recorded for every run.
* **Statistics.** Medians with min..max and coefficient of variation (CV). Binary comparisons
  use a two-sided Mann-Whitney U test on per-run throughput (no normality assumption; with
  n = 5-8 per side it is a rough guide, and I say "not significant" when p > 0.05).
* **Reproducibility is measured, not asserted:** the full suite was run twice independently
  (runs B and C); the report lists the difference between the two for every configuration.

## 4. Experiments

Seven experiments (E1-E7) plus the attribution suite W1, defined in `bench/harness/run_suite.py`
(`--list` prints them):

| id | varies | held fixed |
|---|---|---|
| E1 | GET/SET mix (SET only, 80 % GET, 50 % GET, GET only) x pipeline 1 / 16 | no persistence |
| E2 | clients (1, 4, 16, 50, 64, 256) x pipeline 1 / 16 | 80 % GET |
| E3 | pipeline depth 1, 2, 4, 16, 64 | 50 clients |
| E4 | single thread / global mutex / sharded, 1-8 workers | pipeline 16, 80 % GET |
| E5 | no TTL / TTL 60 s (never fires) / TTL 200 ms (constant expiry) | pipeline 16, 50 % GET |
| E6 | capacity unlimited / 200k / 50k / 10k (keyspace 100k), uniform vs Zipf 0.99 | pipeline 16, 50 % GET |
| E7 | no persistence / `fsync no` / `everysec` / `always` x SET only / 80 % GET x pipeline 1 / 16, **final vs Phase 5 build** | |
| W1 | what the log writes to (nothing / tmpfs / discarded / disk), diagnostics build | SET only, pipeline 16 |

## 5. Results

Main tables: `docs/benchmarks/phase6/report-main.md` (runs B and C pooled, 6 runs per
configuration, final binary). `req/s` = median; latency = median of per-run p50 / p99.

### 5.1 Workload mix (E1)

| workload | pipeline 1 | pipeline 16 |
|---|---|---|
| GET only | 431k, p50 90 us, p99 303 us | 1,949k, p50 348 us, p99 1.18 ms |
| 80 % GET | 352k (CV 31 %), p99 627 us | 2,093k (CV 41 %), p99 934 us |
| 50 % GET | 423k | 2,052k |
| SET only | 444k, p50 88 us, p99 283 us | 1,976k (CV 22 %), p99 1.20 ms |

Without persistence a SET is not measurably more expensive than a GET at this scale: the
differences are inside the run-to-run noise (CV 4-41 %). The server spends about half its CPU in
kernel network I/O (see the profile in §6.4), which is the same for both.

### 5.2 Client count and pipeline depth (E2, E3)

![throughput by client count](../../docs/benchmarks/phase6/charts/E2-throughput.svg)
![throughput by pipeline depth](../../docs/benchmarks/phase6/charts/E3-throughput.svg)

| clients | pipeline 1 req/s (p50 / p99) | pipeline 16 req/s (p50 / p99) |
|---|---|---|
| 1 | 18.3k (48 / 108 us) | 232k (59 / 123 us) |
| 4 | 62.9k (58 / 105 us) | 778k (73 / 148 us) |
| 16 | 292k (44 / 121 us) | 1,914k (106 / 360 us) |
| 50 | 436k (90 / 287 us) | 1,965k (356 us / 1.04 ms) |
| 256 | 475k (446 us / 1.36 ms) | 2,052k (1.90 / 4.26 ms) |

| pipeline | req/s | p50 | p99 |
|---|---|---|---|
| 1 | 435k | 90 us | 291 us |
| 2 | 752k | 111 us | 344 us |
| 4 | 1,185k | 148 us | 397 us |
| 16 | 1,976k | 352 us | 1.02 ms |
| 64 | 2,720k | 1.02 ms | 2.92 ms |

* Throughput saturates at about 16 clients (pipeline 16) or 50-64 clients (pipeline 1). Past
  that, extra clients only add queueing: 256 clients at pipeline 1 deliver the same throughput as
  64 with 4x the median latency (Little's law).
* Pipelining is the largest single lever: 6.3x throughput from depth 1 to 64, paid for with
  11x median latency. A request in a batch of 16 waits for its batch.
* One unpipelined client is bound by the loopback round trip (~54 us per request, 18k/s).

### 5.3 Concurrency design (E4)

![concurrency designs](../../docs/benchmarks/phase6/charts/E4-throughput.svg)

| design | req/s | p99 | server CPU (cores) |
|---|---|---|---|
| single thread, no locks | 506k (CV 27 %) | 3.18 ms | 1.0 |
| global mutex, 4 workers | 544k | 3.38 ms | 2.5 |
| sharded, 1 worker | 489k | 3.28 ms | 1.0 |
| sharded, 2 workers | 1,025k (CV 28 %) | 1.80 ms | 2.0 |
| sharded, 4 workers | 1,941k | 1.08 ms | 3.8 |
| sharded, 8 workers | 2,720k | 1.20 ms | 5.3 |

* A global mutex on four workers is **no faster than one thread without locks** (+7 %, inside
  the noise): the critical section is most of the request, so the workers queue on the mutex.
* Sharding recovers the parallelism: 3.6x over the global mutex at 4 workers, scaling to 2.7M at
  8 workers (the server has 6 CPUs, so 8 workers share them; the gain over 4 is 1.4x, not 2x).
* The sharded engine on one worker costs the same as the unsynchronized one (489k vs 506k, within
  noise): the uncontended lock is cheap.

### 5.4 TTL (E5)

| | req/s | p99 |
|---|---|---|
| no TTL | 2,084k | 1.01 ms |
| `SET ... PX 60000` (never expires during the run) | 1,325k (**-36 %**) | 1.36 ms |
| `SET ... PX 200` (constant expiry churn) | 1,300k (-38 %) | 1.49 ms |

A TTL costs about a third of the throughput even when nothing ever expires, so the cost is in
*recording* the deadline, not in expiring. Confirmed three ways:

* **In-process** (`bench_engine`, no sockets, 4 threads): 4.87M -> 2.37M ops/s with a 60 s TTL
  (`docs/benchmarks/phase6/ttl-microbench.txt`, 3 pairs: -44 % to -52 %).
* **Sampling profile** (`docs/benchmarks/phase6/profiles/profile-ttl-60000.txt`, one run, SET only,
  pipeline 16): 36 % of worker CPU samples are inside the red-black tree of deadlines
  (`std::set<pair<long,string>>`): `drop_expiry` (erasing the old deadline on every overwrite)
  25 %, insert 11 %. In the same profile without a TTL the tree does not appear.
* **Shard-lock hold time** (diagnostics build, same two runs, 1.37M -> 0.72M req/s): 0.89 us ->
  2.65 us per SET, which also lengthens the time other workers wait on the shard.

Cause (supported by the profile, not separately tested): every overwrite of a key that has a
deadline erases a node from, and inserts a node into, an ordered set that stores a copy of the
key string. That is an O(log n) pointer chase plus a node allocation and free per write.
**This is the cheapest large win still on the table** (an intrusive heap / timing wheel storing a
handle in the entry); I did not do it in this phase.

### 5.5 LRU eviction (E6)

| capacity (keyspace 100k, 50 % GET) | req/s | vs unlimited | hit rate |
|---|---|---|---|
| unlimited | 2,057k | | |
| 200k (never evicts) | 2,179k | +6 % (noise, CV 28 %) | |
| 50k, uniform | 2,067k | +0 % | 50 % |
| 10k, uniform | 1,906k | -7 % | 10 % |
| 50k, Zipf 0.99 | 2,619k | +27 % | 92 % |

The O(1) list-based LRU is cheap: the busiest case (90 % of requests miss, ~90 % of SETs evict)
costs 7 %. The Zipf row is faster most likely because 92 % of requests hit a small hot set that
stays in the CPU cache (not tested separately); it says the *workload* is cheaper, not that
eviction is free. Cost of the sharded approximation:
see Phase 4 §7b (`bench_lru_accuracy`).

### 5.6 Durability policy (E7)

![E7 throughput](../../docs/benchmarks/phase6/charts/E7-throughput.svg)

Final binary, 50 clients, 6 runs each (B + C pooled). Phase 5 build for reference in the last
column. "vs none" is the change against no persistence in the same suite.

| workload | none | `fsync no` | `everysec` | `always` | `always`, Phase 5 build |
|---|---|---|---|---|---|
| SET only, pipeline 1 | 445k | 402k (-10 %) | 421k (-5 %) | **14.3k** (-97 %), p50 3.4 ms | 1.2k, p50 38 ms |
| SET only, pipeline 16 | 2,219k | 1,475k (-34 %, CV 51 %) | 1,568k (-29 %, CV 8.5 %) | **193k** (-91 %), p50 3.8 ms | 18.4k, p50 40 ms |
| 80 % GET, pipeline 1 | 397k | 394k (-1 %) | 413k (+4 %) | **62k** (-84 %) | 5.3k |
| 80 % GET, pipeline 16 | 2,127k | 2,026k (-5 %) | 1,997k (-6 %) | **218k** (-90 %), p50 3.6 ms | 19.2k |

Reading it:

* **Policies `no` and `everysec` are indistinguishable in throughput** (as designed: they
  differ in *when* the OS is asked to flush, which the flusher thread does off the request path).
* **Logging a write costs ~30-35 % of pipelined write throughput and ~5 % of a read-heavy mix**:
  only SETs, DELs and TTL changes are logged.
* **`always` is bounded by `fdatasync`, not by the server.** At pipeline 16 up to 800 in-flight
  requests (50 clients x 16) can share one fsync; throughput = (requests per fsync) x (fsyncs
  per second). The server
  uses 0.3-0.6 cores doing it. That is why it is ~8x below `everysec` pipelined (~30x
  unpipelined, where far fewer requests share each fsync) and why its absolute value tracks the
  machine (193k in sessions B/C, 65k in a later session, same code).
* **A/B for `always`**, 6 interleaved runs each, final vs Phase 5 build, p = 0.005 for every row
  (`docs/benchmarks/phase6/report-main.md` and `report-always-later-session.md`):

  | | final | Phase 5 | ratio |
  |---|---|---|---|
  | B+C session: SET P1 / SET P16 / 80 % GET P16 | 14.3k / 193k / 218k | 1.2k / 18.4k / 19.2k | 11.9x / 10.5x / 11.3x |
  | later session: SET P1 / SET P16 / 80 % GET P16 | 4.1k / 64.7k / 71.7k | 349 / 5.4k / 5.5k | 11.7x / 12.0x / 13.0x |

  The ratio held at 10.5-13x while the absolute numbers moved 3x.

### 5.7 Recovery: log replay vs snapshot

Session `recovery2` (healthy), 5 repeats, 64-byte values, page cache warm (a cold start from a
disk would add read time proportional to the sizes). "Unique" = every key written once.
"Churn" = keys overwritten repeatedly for 5 s, so the log is much longer than the data;
**record counts differ between churn rows because the load is time-based, compare the time per
record, not the rows.**

| keys | history | log records | log size | log-only recovery | snapshot size | snapshot recovery | speedup |
|---:|---|---:|---:|---|---:|---|---:|
| 100k | unique | 100,000 | 11 MB | 0.10 s (0.10..0.12) | 11 MB | 0.08 s | 1.3x |
| 500k | unique | 500,000 | 57 MB | 0.76 s (0.51..0.91) | 57 MB | 0.33 s | 2.3x |
| 1M | unique | 1,000,000 | 113 MB | 1.17 s (1.08..1.25) | 113 MB | 0.64 s (0.59..0.72) | 1.8x |
| 100k | churn | 6,825,632 | 771 MB | 7.00 s (6.04..7.84) | 11 MB | 0.08 s | 83x |
| 500k | churn | 5,711,104 | 645 MB | 8.33 s (6.74..9.66) | 57 MB | 0.47 s | 17.6x |
| 1M | churn | 6,309,056 | 713 MB | 12.11 s (10.18..13.03) | 113 MB | 0.96 s (0.95..1.11) | 12.6x |

* Log replay costs 1.0-1.9 us per record (rising with dataset size, probably cache misses; not
  tested); snapshot load costs ~0.6-0.8 us per *live key*. **Replay time scales with history,
  snapshot time with data size**: with heavy overwrites the snapshot is 13-83x faster.
* `SAVE` itself took 0.09 s (100k keys) to 1.27 s (1M keys + 6.3M record log).
* The key count after every restart equalled the count before shutdown (30 recovery pairs).
* The log-only churn rows are the worst case (`--snapshot-mb 0`: no automatic compaction). With
  the default `--snapshot-mb 64` the log is compacted before it can reach these sizes.
* An earlier recovery session (`recovery-DEGRADED-SESSION.md`) gave the same shape with wider
  ranges (5-20 s for churn); it is kept and not used.

## 6. The WAL overhead investigation

Starting point (Phase 5 build, SET only, pipeline 16): `fsync no` ran 43-50 % below no
persistence with **no fsync involved**, and the cause was not known. `perf` and `valgrind` are not
installed in this WSL2 environment (`gdb` and `gprof` are; `gprof` needs an instrumented build and
sees neither kernel time nor lock waits), so two instruments were built into a separate
diagnostics build (`distcache_diag`, `-DDISTCACHE_DIAGNOSTICS`; the production binary has none of
it):

1. **Nanosecond timers** around encode, log-lock wait, log-lock hold, shard-lock wait and hold,
   flusher swap / write / fsync, exposed through `INFO`.
2. **An in-process sampling profiler** (`ITIMER_PROF` + `backtrace`, `DISTCACHE_PROFILE=file`),
   symbolized offline (`bench/harness/symbolize.py`). **Limitations:** it samples on-CPU time
   only (a blocked thread is invisible; the timers cover waiting); time inside the kernel is
   attributed to the user function that made the syscall (shown as `[unresolved library/kernel]`);
   it perturbs the program slightly, so use it for
   shares, not for absolute speed; and the timers themselves cost ~0.1 us per request
   (`clock_gettime` is 4 % of samples in the diagnostics build, 0 in production).

### 6.1 Hypotheses and verdicts

| # | hypothesis | experiment (changing one thing) | verdict |
|---|---|---|---|
| H1 | Writing to the disk is the cost | W1: same workload, log to disk / tmpfs / **flusher discards the data** | **Falsified for the steady state.** 1,080k / 1,136k / 1,113k, all within 5 %, CV 1-1.5 % (`report-wal-attribution.md`). Nothing in the write path matters when the data is thrown away |
| H2 | The log buffer regrows (allocation) under the lock | counter of capacity changes + peak size | **Falsified** for the steady state: 2-3 growth events per run, 1.3-2.3 MiB peak. (The Phase 5 fixes for double buffering and `RecordBuilder` already removed the real instance.) *But see 6.3* |
| H3 | Encoding + CRC inside the shard lock is expensive | timer around encode; microbench of `crc32` and `encode` | **Supported, and fixed.** CRC of a 110-byte record: 193 ns -> 38 ns (slicing-by-8, equivalence-tested against the reference). Timed per-record encode: ~480-500 ns (W1-a) -> ~250 ns (W1-b), different sessions so indicative |
| H4 | The single log mutex serializes all workers | timer: wait for and hold of the log lock; `bench_wal append` thread scaling | **Partly supported.** Held ~65 ns per append but average wait 280-350 ns with `std::mutex` (W1-b); 1 -> 8 appending threads drops 10.3M/s -> 5.7M/s per process (`bench_wal`, final build). Replacing `std::mutex` by a spin lock gained +13.5 to +14.3 % (p = 0.002, 12 repeats); the log-lock wait in the one clean profiled run with the spin lock was 94-106 ns |
| H5 | Striping the log buffer per shard removes H4 | built and A/B tested, 5 repeats | **Rejected on semantics, and no consistent gain.** SET P16 vs the unstriped step 2: `fsync no` -11 %, `everysec` +37 % (the unstriped run had CV 59 %), 80 % GET about equal (`report-ab-stripes.md`). A crash can leave a newer write while an older one is lost (breaks the prefix-of-history property; 2 existing torn-tail tests failed). Reverted; reasoning kept in `src/wal.hpp` |
| H6 | The flusher steals CPU from workers | per-thread CPU | **Falsified** in the steady state: 0.02-0.18 cores (5-7 % of CPU samples; ~98 % of it inside `write()`) |
| H7 | Logging lengthens the shard-lock hold, which makes workers queue | shard hold timer | **Supported:** average hold 0.61 us (no log) -> 1.57-1.69 us (log); wait 0.18 -> 0.39-0.41 us (W1-b) |

### 6.2 What the optimizations are, and the evidence for each

One change at a time; the four binaries were benchmarked interleaved in the same session.

| step | change | component-level evidence | end-to-end, SET only, P16, `fsync no` / `everysec` (8 repeats, interleaved, healthy session) |
|---|---|---|---|
| 0 | Phase 5 behaviour | | 1,128k / 1,161k (no persistence: 2,043k; -45 % / -43 %) |
| 1 | **Deferred acknowledgement** for `always` (§6.5) | `always`: 10.5-13x, p = 0.005 | 1,122k (-0.5 %, p = 1.0) / 1,114k (-4.1 %, p = 0.43): no effect on other policies, as expected |
| 2 | **Slicing-by-8 CRC-32** (same polynomial, same on-disk format) | 193 -> 38 ns per record | 1,291k (+14.5 %, p = 0.066) / 1,216k (+4.7 %, p = 0.19): **not significant at n = 8** |
| 3 | **Spin lock** (test-and-test-and-set, `pause`, `sched_yield` fallback) on the log buffer | wait timer 283-347 ns (mutex build) vs 94-106 ns (one clean spin-build run); mechanism not isolated | **1,339k (+18.8 %, p = 0.066) / 1,322k (+13.9 %, p = 0.001)**; independent 12-repeat A/B: +13.5 % / +14.3 %, p = 0.002 |

Net effect on the overhead of logging a pipelined write: **-45 % / -43 % -> -34 % / -35 %**
relative to no persistence. Honest reading: the CRC step is a large, certain change at component
level but is a small share of a request, so its end-to-end gain is within noise here; the spin
lock is the one change with a significant end-to-end effect on the unsynchronized policies. A
first attempt at this ladder ran in a degraded session
(`report-ladder-DEGRADED-SESSION.md`) and could resolve nothing; it was re-run (command line in
`results/ladder2/env.json`) after the health probe was added. The spin lock's mechanism is not isolated: my hypothesis is
that a contended `std::mutex` pays for futex wake and park system calls that a ~65 ns critical
section does not justify.

![ladder](../../docs/benchmarks/phase6/charts/ladder-throughput.svg)

Intermediate binaries (steps 1-3) were built from working-tree states and are not separate
commits, so the ladder itself cannot be re-run from git. What can be re-run is Phase 5 vs the
final tree: `scripts/build_baseline.sh` builds the Phase 5 commit (`cdd855d`) and `scripts/bench.sh`
compares the two (the frozen "baseline" binary used above was verified equivalent to that build:
`results/report-equiv`, 975k vs 980k `everysec`, 5.1k vs 4.7k `always`, both within noise).

### 6.3 Stalls: what I can and cannot say

`fsync no` / `everysec`, SET only, P16 had CVs of 35-51 % in several suite runs (min 375k vs
median 1,470k). To see what happens inside such a dip I ran the W1 profile three times
(`docs/benchmarks/phase6/profiles/raw-prof{2,3,4}.jsonl`, diagnostics counters, one run of each
variant per session). Throughput of the **no-persistence control** in the same session is what
tells the sessions apart:

| session | no-persistence control | log to disk | longest `write()` | longest log-lock wait | peak log buffer |
|---|---|---|---|---|---|
| prof3 | 1,308k req/s, p99 2.0 ms | 1,087k, p99 2.0 ms | 84 ms | 7.5 ms | 3.6 MiB |
| prof4 | **702k**, p99 17 ms | 283k, p99 42 ms | **1,615 ms** | 441 ms | 25 MiB |
| prof2 | **323k**, p99 28 ms | 199k, p99 51 ms | **1,645 ms** | **2,106 ms** | 32 MiB |

The health probe passed at the start of prof2 and prof3 (tmpfs 3.1-3.2 GB/s, ext4 1.4-2.0 GB/s,
CRC 44-49 ns) and flagged prof4 narrowly (tmpfs 1.44 GB/s against a threshold of 1.5); it passed
at the end of all three. The prof4 warning scrolled past in a `tail` and I ran on anyway; the
data is quoted only as a degraded episode.

What this shows:

* **In prof2 and prof4 the whole machine was slow**: the same build, doing no I/O at all, ran 2-4x
  slower with tail latencies 10x worse, in every variant including no persistence, tmpfs and the
  discard sink. So whatever happened there is **not specific to the disk** and the two runs are
  not representative of the system. I cannot say whether the host was busy (it is a laptop
  VM) or something else; I did not find out.
* **On top of that, only the real-disk variant showed `write()` calls of ~1.6 s, a log buffer
  that grew to 25-32 MiB (the log has no backpressure) and a worker waiting up to 2.1 s for the log
  lock.** The tmpfs variant's longest `write()` in the same sessions was 54-91 ms (18 ms in prof3). So the disk did
  contribute, in a session in which everything else was also slow.
* **While the lock holder was stalled, the spin lock burned CPU:** in prof2, 53 % of worker CPU
  samples were in `sched_yield` (`profile-wal-disk-STALL-run-a.txt`) against 0.9 % in the clean
  run. A mutex would have slept instead. This is the cost side of step 3 of the ladder.
* The profile of a stalled session says what the *fallback path* costs, not what normal logging
  costs. **All "where does the CPU go" statements below use prof3 only.**

I could not show that these events explain the variance of the pooled suite runs: the production
binary has no stall counters, and the suite sessions were checked by RSS and the control rows,
not per run. It is the most plausible explanation (rare, large dips; RSS 90-230 MiB in the one
degraded session), not an established one. Practical rule: judge a session by its control rows.

### 6.4 Where the CPU goes (clean session, prof3)

SET only, pipeline 16, `fsync no`, 4 workers, final build with diagnostics. Shares of worker
thread CPU samples; "no log" is the same build without persistence. **Profile runs are for
shares, not throughput:** with the sampler and timers on, no persistence ran at 1.31M (not the
2.0-2.2M of the production build), so the 17 % gap to the logging run (1.09M) is not the
production overhead (34-35 %, §6.2).

| | no log | log to disk |
|---|---|---|
| in the kernel / syscalls (`recv`, `send`, `epoll`) | 51 % | 47 % |
| of which reply `write()` (inclusive) | 38 % | 34 % |
| `ShardedStore::set` (inclusive) | 40 % | 47 % |
| hash table + LRU list | ~27 % | ~19 % |
| `Persistence::on_set` (inclusive: encode + CRC + append) | - | **10.7 %** |
| of which `Wal::append_framed` | - | 7.9 % |
| `crc32` self | - | 1.8 % |
| `SpinMutex::lock` + `sched_yield` | - | 2.4 % |
| flusher thread (share of all samples) | - | 5.7 % (98 % in `write()`) |

The timers of the same two runs: shard-lock hold 0.99 us (no log) -> 1.41 us (log); encode 0.31 us,
log-lock wait 0.11 us, log-lock hold 0.07 us.

Reading it, with the unattributed part kept visible: in the step-2 build (W1-b, throughput
-47 % vs no persistence, server CPU per request ~1.9 us -> ~3.3 us) the shard-lock hold grew by
~1.0 us, of which the timers account for ~0.65 us (encode 0.25 + log-lock wait 0.28-0.35 +
log-lock hold 0.07; the timers themselves cost ~0.1 us). **About 0.7 us of CPU per request is
not attributed by either instrument.** Candidates (hypotheses, none tested): cache lines of the
shared log buffer, sequence counter and lock bouncing between four cores; the longer hold
causing more waiting on the shard mutex; the extra memory traffic of writing the record. Telling
them apart needs hardware counters (not available here) or the experiment listed under
"Remaining bottlenecks".

### 6.5 Group commit and `fsync=always`: how the ack is held

Phase 5 held the *worker thread* until its writes were durable, so at most 4 writers shared an
fsync. Now a reply that depends on write N is parked on the connection and released only when
`durable_seq >= N`; the worker keeps serving other connections, so all concurrently pending
writes share one `fdatasync`.

* **Invariant:** no reply that depends on write N is in a connection's output buffer unless
  the log's `durable_seq` is >= N (`gate()` / `settle()` in `src/server.cpp`). Replies queued
  behind a held reply on the same connection are held too, so order is preserved.
* **No lost wake-up:** the flusher publishes `durable_seq` (atomic) and wakes every worker's
  eventfd; the worker re-reads `durable_seq` *after* parking, so a publication between the
  check and the park is seen.
* **Verification:**
  * `tests/test_deferred.cpp`, 9 tests, with an artificial `fdatasync` delay so a premature
    reply is observable (at the moment a client receives a reply the log's durable sequence
    already covers its record), liveness (a worker waiting for an fsync still serves other
    connections), many connections sharing fsyncs, pipelined order, protocol error after a held
    write, client disconnect while parked, other policies never hold, shutdown with parked
    replies.
  * **Mutation checks**, re-run on the final tree: four deliberate breakages of the gate in a
    scratch copy, each must fail the suite. (1) release when `seq <= durable + 1000000`:
    5 of 9 fail; (2) gate never holds: 5 fail; (3) `settle()` assumes everything durable: 5 fail;
    (4) later replies may overtake a held one: the ordering test fails. Unmodified: 9/9 pass.
  * **`kill -9` crash test on the final binary** (`docs/benchmarks/phase6/crash-test-final.txt`):
    `always`: 0 of 20,110 acknowledged writes lost over 25 rounds (and 0 of 21,867 with a 1 MiB
    snapshot threshold, so kills land around snapshots); no holes, no corruption in any policy.
    `everysec` lost 1,795 acknowledged writes (worst round 137) and `no` 1,516, as defined.
    (`SIGKILL` leaves the OS alive: this measures the process-crash window, not power loss.)

### 6.6 Test and sanitizer status of the final tree

`distcache_tests` (store, WAL, persistence/recovery, concurrency, end-to-end server tests and the
9 deferred-ack tests): **154 pass, 1 skipped** (a logged-eviction scenario that
needs one global LRU order and so does not apply to the sharded engine, skipped by design) in a
plain Release build, under **AddressSanitizer + UBSan**, and under **ThreadSanitizer**, with no
sanitizer reports (`docs/benchmarks/phase6/sanitizers.txt`: exit codes, summary lines and the
number of instrumented symbols in each binary). The tree is the one that produced the benchmarked
binary (`build/distcache` has the same hash as the frozen `distcache_final` used in the suites).
Note that the test suite writes to tmpfs, where `fsync` is a no-op: it verifies ordering,
torn-tail handling and the acknowledgement gate, not `fsync` latency (the crash test does).

## 7. Variability and reproducibility

* **Within a session:** CV of a configuration is typically 2-25 %; pipelined persistent
  configurations are worse (up to 51 %, §6.3).
* **Between two independent full-suite runs (B and C), same binary, same machine:** across 67
  configurations the median difference of medians was **6.8 %**, the 90th percentile 18.1 % and
  the worst 43.4 %. The table is in `report-main.md`. Differences below ~10-20 % between
  configurations measured once should be treated as noise; the conclusions above rest on larger
  effects or on tests with p-values.
* **Which effects reproduced everywhere:** `always` 10.5-13x (two sessions, p = 0.005); sharded
  vs global mutex; TTL cost; pipelining; recovery shape.
* **Which did not reach significance:** the CRC step end to end (p = 0.066 / 0.19), SET-only P16
  `fsync no` pooled B + C (+32 %, p = 0.378, CV 35-51 %).
* **Not comparable:** anything measured in a `DEGRADED-SESSION` file or in the stalled profile
  sessions (§6.3); Phase 5's README numbers (`redis-benchmark`, a different machine state)
  against these.
* **What I would still not claim:** that the absolute numbers hold on other hardware; that the
  CRC step helps end to end; that the TTL fix would recover the full 36 % (the profile says the
  tree is 36 % of worker CPU, which bounds the gain from above, not below).

## 8. Remaining bottlenecks and known limitations

1. **Syscalls: ~50 % of worker CPU** is in kernel network I/O in every configuration. Fewer
   syscalls (batching replies across connections, `io_uring`) is the next ceiling for
   throughput, independent of persistence.
2. **TTL deadline index** (§5.4): 36 % of throughput. An intrusive heap or timing wheel is the
   fix; measured, not implemented.
3. **WAL: ~0.7 us of CPU per request is not attributed** (§6.4). Untested next step: build the
   record *before* taking the shard lock (the encoded bytes depend only on the command and its
   absolute deadline), shortening the hold and moving the CRC out of the critical section. Needs
   an engine API change; its benefit is a hypothesis.
4. **No backpressure on the log buffer, and the spin lock burns CPU during stalls** (§6.3). A
   bounded buffer with a blocking (or `futex`-parking) slow path would turn a multi-second
   stall into a bounded wait for writers. Observed only in degraded sessions; not a steady-state
   cost.
5. **One log lock** remains a serialization point at high core counts (10.3M -> 5.7M appends/s
   from 1 to 8 threads in `bench_wal`); striping was rejected for correctness (§6.1 H5).
6. **`always` is bounded by the disk's `fdatasync` latency**: group commit shares it across all
   pending writes but cannot shorten it. Real hardware will differ from this VM.
7. **Environment:** a laptop VM, 6 cores, shared with the load generator and the host OS; no
   power-loss testing; closed-loop clients understate tail latency during stalls; 8 s runs
   cannot show long-term effects such as snapshot-induced pauses (the snapshot copies a shard
   under its lock, Phase 5 §6).
8. **The intermediate ladder binaries are not in git** (§6.2).

## 9. Reproducing

```bash
# 0. build everything, then check the machine is representative
cmake -S . -B build -G Ninja && cmake --build build
scripts/health_probe.sh                    # exit 3 = degraded: results will not match

# 1. the full suite twice + report + recovery (about 70-80 minutes); --quick is ~5 minutes
scripts/build_baseline.sh                  # Phase 5 commit -> build/distcache_phase5
scripts/bench.sh [--quick]                 # results/run-*/, results/report/report.md, results/recovery/

# 2. one experiment, any binaries (interleaved), any number of repeats
python3 bench/harness/run_suite.py --suite E7 --repeats 8 --settle 4 \
    --binary final=build/distcache --binary baseline=build/distcache_phase5 --out results/mine
python3 bench/harness/report.py results/mine/raw.jsonl --out results/report-mine

# 3. component microbenchmarks (CRC, encode, append contention, disk, engine+log)
scripts/microbench_wal.sh docs/benchmarks/phase6/wal-microbench-mine.txt
python3 bench/harness/summarize_micro.py docs/benchmarks/phase6/wal-microbench-mine.txt

# 4. attribution and profile (diagnostics build)
cmake --build build --target distcache_diag
python3 bench/harness/run_suite.py --suite W1 --binary diag=build/distcache_diag --repeats 3 \
    --profile-dir results/prof --out results/w1
python3 bench/harness/symbolize.py results/prof/<profile>.txt build/distcache_diag --top 20

# 5. recovery, crash safety, load-generator validation
python3 bench/harness/bench_recovery.py --sizes 100000,500000,1000000 --repeats 5 --out results/recovery
scripts/crash_test.py --policy always --rounds 25
scripts/validate_loadgen.sh && scripts/validate_counts.sh
```

Expect the same *ratios* and the same ordering, not the same absolute numbers, on a different
machine; run `health_probe.sh` first and keep sessions comparable.

Raw data of every session of this phase, with an index saying which sessions were healthy, is in
`results/` (`results/README.md`). The rendered reports, charts, symbolized profiles and
microbenchmark outputs are in `docs/benchmarks/phase6/`.

## 10. Files added or changed in this phase

```
bench/loadgen.cpp                 closed-loop RESP load generator
bench/bench_wal.cpp               component microbenchmarks (crc, encode, append, write, engine)
bench/harness/run_suite.py        experiment definitions + orchestration (pinning, interleave, settle, health)
bench/harness/report.py           tables, SVG charts, Mann-Whitney U, run-to-run variability
bench/harness/bench_recovery.py   log-replay vs snapshot recovery
bench/harness/symbolize.py        sampling-profile aggregation
bench/harness/summarize_micro.py  microbenchmark summary
scripts/bench.sh, build_baseline.sh, health_probe.sh, microbench_wal.sh, validate_*.sh
src/diag.*                        diagnostics timers + sampling profiler (distcache_diag only)
src/wal.*                         slicing-by-8 CRC, SpinMutex log lock, durable-sequence publication
src/server.*, persistence.*, commands.*   deferred acknowledgement (gate/settle, durable listeners)
tests/test_deferred.cpp           9 tests for the acknowledgement invariant
results/                          raw data of every session (README.md = index)
docs/benchmarks/phase6/           reports, charts, profiles, microbenchmarks, crash test, validation
```
