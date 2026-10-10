# DistCache

A Redis-compatible in-memory cache written from scratch in C++17: RESP protocol over TCP, TTL
expiration, LRU eviction, and a multi-threaded server with two interchangeable locking designs
(one global mutex, or sharded locks) that are tested under ThreadSanitizer and benchmarked.

You can talk to it with the real `redis-cli`.

```bash
cmake -S . -B build -G Ninja && cmake --build build
./build/distcache --mode sharded --threads 4          # listens on 6380
redis-cli -p 6380 SET session:1 abc EX 60
redis-cli -p 6380 GET session:1
redis-cli -p 6380 INFO
```

| Phase | Status |
|---|---|
| 1. TCP + RESP server, `SET`/`GET`/`DEL` | done |
| 2. TTL: lazy + active expiration | done |
| 3. LRU eviction, O(1) | done |
| **4. Concurrency: worker threads, global lock, sharded locks** | **done (this document)** |
| **5. Durability: write-ahead log, snapshots, crash recovery, fsync policies** | **done** (see "Phase 5" below) |
| 6-10. Benchmark harness, consistent hashing, replication, rate limiter, failure testing | planned |

Commands: `PING ECHO SET(EX|PX) GET DEL EXPIRE PEXPIRE TTL PTTL PERSIST DBSIZE INFO SAVE`.

```
distcache [--port N] [--threads N] [--mode single|global|sharded] [--shards N] [--max-keys N]
          [--dir PATH [--fsync always|everysec|no] [--snapshot-mb N]]
```

---

## Phase 4: concurrency

### 1. What is shared, and what had to change

Before Phase 4 the server was one thread running one epoll loop, so nothing was shared. Once
several threads serve clients, this state is touched by more than one thread:

| Shared mutable state | Mutated by | Notes |
|---|---|---|
| hash map `key -> {value, deadline, lru position}` | set, del, **get** (lazy expiry), sweep, eviction | |
| LRU list | **every get**, set, del, sweep, eviction | the list is relinked on reads |
| deadline index (`std::set<(deadline,key)>`) | set, expire, persist, del, sweep, eviction | |
| `evicted_` / `expired_` counters | get (lazy expiry), sweep, eviction | read by `INFO` |
| connection table (fd -> buffers) | the owning worker only | made private per worker, so it needs no lock |

One logical key lives in **three** structures at once (map, LRU list, deadline index). They must
change together. A thread that observes them half-updated can follow a dangling list pointer or
erase the wrong deadline entry. That defines the critical section: **one engine operation = one
lock, held from start to finish.**

### 2. Why `GET` needs synchronization (it is not a read)

```cpp
std::optional<std::string> get(const std::string& key) {
    auto it = live(key);          // may ERASE the key if its TTL passed  -> writes map, LRU, index
    if (it == map_.end()) return std::nullopt;
    touch(it);                    // relinks the LRU list node             -> writes the list
    return it->second.value;
}
```

A cache with LRU turns every read into a write. Two concurrent `get`s would both splice the same
doubly linked list. That is why this project uses a plain `std::mutex` and **not** a
`std::shared_mutex`: a reader/writer lock only helps when readers really are read-only, and here
they never are. (Making reads truly read-only would mean approximating LRU, e.g. sampling or
CLOCK, which is the design Redis chose. That is a possible later experiment.)

### 3. The race, reproduced

`tests/race_demo.cpp` runs a mixed workload from 8 threads against the **unsynchronized** `Store`
(the Phase 3 engine logic, now behind the `Engine` interface, with no locks added), built with `-fsanitize=thread`:

```bash
cmake -S . -B /tmp/build-tsan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=thread -g -O1"
cmake --build /tmp/build-tsan --target race_demo
/tmp/build-tsan/race_demo unsafe 8 3000
```

Measured result (full log in `docs/tsan-unsafe.txt`):

* **103 ThreadSanitizer data-race reports**, on `Store::live`, `sweep`, `drop_expiry`, `set`,
  `erase`, `get` and `touch`. That is every structure above.
* The process then **crashed with a segmentation fault** inside `drop_expiry`
  (`SUMMARY: ThreadSanitizer: SEGV ... store.hpp:202`): concurrent inserts and erases had
  corrupted the deadline index's tree.

First report, abridged:

```
WARNING: ThreadSanitizer: data race
  Read of size 8 by thread T2:   Store::sweep()    store.hpp:123   (expiries_.empty())
  Previous write of size 8 by T1: Store::set_deadline()  store.hpp:198   (expiries_.emplace)
```

The number of reports before the crash varies from run to run (103 in the logged run, 23 in a
later re-run on the final tree); it is the crash point that changes, not the outcome.

The same workload through the two safe engines: **0 reports**, structures consistent
(`docs/tsan-global.txt`, `docs/tsan-sharded.txt`).

### 4. Architecture

```
                    +-------------------- Server ---------------------+
 clients --TCP-->   | acceptor thread: accept(), round-robin handoff  |
                    |   |            |            |            |       |
                    |   v            v            v            v       |
                    | worker 0     worker 1     worker 2     worker 3  |   each worker = 1 thread
                    | own epoll    own epoll    own epoll    own epoll |   + its own epoll
                    | own conns    own conns    own conns    own conns |   + its OWN connections
                    +----+-----------+------------+------------+-------+
                         |  execute(Engine&, args)  -- the ONLY shared state is the Engine
                         v
                  +--------------- Engine (interface) ----------------+
                  |  Store            no locks (1 thread only)         |
                  |  GlobalLockStore  1 mutex -> 1 Store               |
                  |  ShardedStore     N x (mutex -> Store)             |
                  +----------------------------------------------------+
```

* A connection belongs to **one worker for life**, so its buffers need no lock. The acceptor
  hands a new socket to a worker through a mutex-protected queue plus an `eventfd` that wakes the
  worker's `epoll_wait`.
* Workers never block on a slow client: sockets are non-blocking and epoll reports readiness.
  Pipelined commands are parsed from the buffer and answered in order.
* Active expiration: every worker calls `engine.sweep()` about every 100 ms. The engine serializes
  it. In `single` mode there is exactly one worker, so the unsynchronized store is still safe.
* The command layer (`execute`) works on the `Engine` interface, so the locking strategy is chosen
  at startup with `--mode`.

### 5. Locking strategies

**Global lock** (`GlobalLockStore`): one mutex around one `Store`. Strictly correct, exact global
LRU order, exact stats. Every operation on every key queues on the same lock.

**Sharded** (`ShardedStore`): `hash(key) % N` picks a shard. Each shard is a complete `Store`
(map, LRU list, deadline index, counters) behind its own mutex, so operations on different shards
run in parallel. Shards are `alignas(64)` so neighbouring locks do not share a cache line.

Deadlock avoidance: **no method ever holds two locks.** Each takes one lock, does its work, and
releases it. With no nesting there is no lock-order cycle to break. Values are copied out before
unlocking so no reference outlives the lock.

What shard-level locking can and cannot do:

| Operation | Lock needed | Why |
|---|---|---|
| `SET GET DEL TTL EXPIRE PERSIST` (single key) | its one shard | map, LRU list, deadline index for a key all live in that shard |
| LRU eviction | its one shard | LRU is **per shard**: a shard evicts its own oldest key. Capacity is split `ceil(max_keys/N)` per shard |
| `sweep()` | one shard at a time, in turn | starts at a rotating shard so a busy shard 0 cannot consume the whole budget |
| `INFO`, `DBSIZE`, `size()` | one shard at a time | each term is exact, but the **sum is not an atomic snapshot** under concurrent writes |
| `DEL a b c` (multi-key) | one shard per key | **not atomic as a group** (same as Redis Cluster) |

The cost: LRU becomes **approximate**. A hot shard can evict a key that is more recent than a
cold shard's oldest. Strict global LRU would need one shared list that every `GET` locks, which
is exactly the bottleneck sharding removes. This is measured below.

### 6. Tests

`distcache_tests`: **84 tests** (the Phase 1-3 tests are unchanged and still pass).

| Area | Tests |
|---|---|
| Concurrent `GET` storm (8 threads x 10k, every value checked, LRU list stays valid) | global + sharded |
| Concurrent `SET`/`GET`/`DEL` on 64 overlapping keys (no torn or foreign values) | global + sharded |
| Disjoint keys end in exactly the expected state | global + sharded |
| Concurrent expiration: writers, readers, `expire`, `persist`, a sweeper thread | global + sharded |
| Concurrent LRU updates: an observer thread asserts `size() <= capacity` at every instant | global + sharded |
| Concurrent eviction: `size + evicted == inserted` exactly | global + sharded |
| Mixed workload with `INFO`/`DBSIZE`/`sweep` running alongside | global + sharded |
| Real server, real TCP clients: 16 clients on own keys, 12 clients on one hot key, 400-command pipeline, byte-at-a-time command, connection churn, active expiry with no traffic, abrupt disconnect, malformed input | single + global + sharded |
| Sharded design: sweep budget across shards, capacity division, same key -> same shard | unit |
| Fuzz: 20,000 random ops, `consistent()` checked after **every** op | unit |

`consistent()` verifies that the map, LRU list and deadline index agree (sizes match, every list
node points back at its entry, every TTL has exactly one index entry). It runs after every
concurrent test, so a lost update shows up as a failure and not as a silent leak.

Results (84 tests each; sanitizer builds were re-run on the final tree, 3 shuffled repeats each, after an
earlier pass of 5-8 repeats):

| Build | Result |
|---|---|
| Release | 84/84 pass (and 30 consecutive shuffled repeats) |
| AddressSanitizer (+LeakSanitizer) | 84/84 pass, 0 reports (5 repeats) |
| UndefinedBehaviorSanitizer (`-fno-sanitize-recover`) | 84/84 pass, 0 reports |
| **ThreadSanitizer** | **84/84 pass, 0 reports** (8 shuffled repeats) |

### 7. Benchmarks

Environment: WSL2 Ubuntu on Windows 11, 12 logical CPUs, 3.7 GB RAM, g++ 15.2 `-O3`. Client and
server share the machine. **Treat absolute numbers as indicative, not portable**: this is a
virtualized laptop-class environment and run-to-run variance is large (ranges are shown).
Raw data: `docs/benchmarks/`. Reproduce with `scripts/bench_engine.sh` and `scripts/bench_net.sh`.

#### 7a. In-process: lock contention in isolation (`bench_engine`, no sockets)

100k keys, 64-byte values, 3 s runs, median of 3 runs (ops/second, M = millions). Sharded uses 16
shards in this table.

90% GET / 10% SET. Unsynchronized single-thread baseline: **2.16M**

| threads | global mutex | sharded (16) | sharded / global |
|---|---|---|---|
| 1 | 1.97M | 2.02M | 1.03x |
| 2 | 0.65M | 1.92M | 2.94x |
| 4 | 0.56M | 2.34M | 4.16x |
| 8 | 0.56M | 3.42M [2.56-4.11] | 6.08x |
| 12 | 0.57M | 2.51M [2.22-2.85] | 4.39x |

50% GET / 50% SET. Baseline: **1.54M**

| threads | global mutex | sharded (16) | sharded / global |
|---|---|---|---|
| 1 | 1.61M | 1.90M | 1.19x |
| 2 | 0.64M | 2.00M | 3.12x |
| 4 | 0.70M | 3.00M | 4.28x |
| 8 | 0.72M | 4.26M | 5.92x |
| 12 | 0.66M | 4.99M | 7.58x |

What this shows:

* **The global lock gets slower when threads are added**: 2.0M ops/s with 1 thread falls to
  0.65M with 2 and stays near 0.6M. Threads spend their time contending for the one lock and
  bouncing its cache line between cores (lock convoy), not working.
* **Sharding recovers the parallelism**, up to roughly 6x the global lock at 8 threads (90% GET).
* Scaling is **not linear** (8 threads reach about 1.6x the single-thread baseline at 16 shards,
  not 8x). Random access over 100k keys is memory-bound, and the 16-shard run is noisy.
* Uncontended cost of locking: 1 thread sharded 2.02M vs unsynchronized 2.16M, about 7%.

Shard count, 8 threads, 90% GET (median [min-max]):

| shards | 1 | 2 | 4 | 8 | 16 | 64 | 256 |
|---|---|---|---|---|---|---|---|
| ops/s | 0.81M | 1.35M | 2.14M | 3.06M | 3.42M [2.56-4.11] | 4.43M | 9.22M |

A second sweep taken later (3 runs each, ops/s) was noisy: 64 shards 5.1-7.1M; 256 shards
8.6-9.0M; 1024 shards 3.4-11.5M; 4096 shards 5.7-10.1M. The consistent finding is that **more
shards help until contention is gone (roughly 256 here), then plateau**. Caveat on the table:
the 1-shard row (0.81M) is structurally the same as the global mutex (0.56M in the earlier
table); the gap is a measure of the environment's run-to-run noise at this contention level, so
do not read fine differences between neighbouring cells.

#### 7b. LRU accuracy: what sharding costs (`bench_lru_accuracy`)

Cyclic access over a working set of W keys on a store with capacity C. An exact LRU keeps all
W <= C keys (100% hits). Hit rate after warm-up:

C = 1,000

| working set | 1 shard | 4 | 16 | 64 | 256 |
|---|---|---|---|---|---|
| 50% of C | 100.0 | 100.0 | 100.0 | 100.0 | 83.2 |
| 80% of C | 100.0 | 100.0 | 100.0 | 86.4 | 62.6 |
| 95% of C | 100.0 | 73.3 | 78.7 | 54.8 | 50.1 |

C = 100,000

| working set | 1 shard | 4 | 16 | 64 | 256 |
|---|---|---|---|---|---|
| 50% of C | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 |
| 80% of C | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 |
| 95% of C | 100.0 | 100.0 | 100.0 | 100.0 | 83.9 |

Keys hash unevenly, so some shards receive more keys than their slice holds and thrash, even
though the store as a whole has room. The damage depends on **keys per shard**, not shard count.
So the server applies a guard: with `--max-keys`, it uses at most `max_keys / 1024` shards
(at least one), and prints a note when it reduces your `--shards`. The default is 64 shards when
no capacity is set (no eviction, so no accuracy cost).

#### 7c. End to end over TCP (`redis-benchmark`)

Real server, loopback, 64-byte values, random keys from a 100k keyspace, `redis-benchmark
--threads 4`. 3 runs per cell; table shows median throughput in thousands of requests/second
with [min-max], and p50 / p99 latency in milliseconds. `global-4w` and `sharded-4w` use 4 worker
threads; `single` is the unsynchronized engine on one worker thread. Every run in the 10+ client
rows lasted at least 4.5 s (a first pass with shorter pipelined runs was discarded: the tool's
time resolution is too coarse below a couple of seconds; see `scripts/bench_net.sh`).

GET

| clients, pipeline | single | global-4w | sharded-4w |
|---|---|---|---|
| 1, 1 | 16.0k 0.055/0.135 | 13.8k 0.055/0.207 | 14.2k 0.055/0.167 |
| 10, 1 | 59.0k [44-62] 0.127/0.407 | 166.5k [167-176] 0.039/0.175 | 168.3k [121-200] 0.039/0.159 |
| 50, 1 | 69.4k [64-72] 0.535/3.047 | 292.1k [248-300] 0.103/0.455 | 299.9k [225-307] 0.079/0.599 |
| 200, 1 | 66.5k [61-84] 1.687/11.351 | 278.6k [184-324] 0.511/2.735 | 342.2k [315-352] 0.303/1.255 |
| **50, 16** | **464.6k** [427-484] 1.343/11.407 | **423.0k** [403-435] 1.455/10.495 | **1390.3k** [1291-1775] 0.407/1.887 |

SET

| clients, pipeline | single | global-4w | sharded-4w |
|---|---|---|---|
| 1, 1 | 13.8k 0.055/0.183 | 13.8k 0.055/0.183 | 14.8k 0.055/0.167 |
| 10, 1 | 64.0k [64-69] 0.119/0.359 | 134.3k [114-158] 0.039/0.239 | 171.2k [153-200] 0.039/0.175 |
| 50, 1 | 80.0k [53-89] 0.535/1.391 | 272.4k [189-279] 0.103/0.583 | 278.6k [238-299] 0.087/0.695 |
| 200, 1 | 75.6k [73-80] 1.639/8.607 | 278.5k [226-343] 0.511/2.543 | 332.5k [238-352] 0.311/1.767 |
| **50, 16** | **478.6k** [406-584] 1.223/11.095 | **479.4k** [444-510] 1.359/8.775 | **1292.4k** [1256-1409] 0.407/5.863 |

What this shows, and what it does not:

* Worker threads (either locking design) give **about 2-4x** the throughput of one thread once
  there are 10+ clients (2.1x for SET at 10 clients, 4.2x for GET at 50), and cut p50 latency (50 clients: 0.535 ms single vs about 0.08-0.10 ms),
  because the single thread is the bottleneck.
* **Without pipelining, global and sharded are nearly tied.** Each request costs a socket
  read/write syscall pair, which dwarfs the microseconds spent under the lock, so lock
  contention is hidden. With 1 client all three are equal (about 14-16k): that is network round
  trip, not the cache.
* **With pipelining (16 commands per round trip), the locking design decides the outcome.**
  Syscall cost is amortized, the lock becomes the hot spot, and:
  `global-4w` performs about the same as the single thread (**4 threads bought nothing**),
  while `sharded-4w` reaches about **3x** both. This agrees with the in-process result.
* p99 is high for `single` and `global` under pipelining (8.8-11.4 ms) versus 1.9-5.9 ms for
  sharded: the queue behind the lock shows up in the tail.
* Caveats: `redis-benchmark` runs on the same 12 CPUs as the server and may itself be the limit
  at the highest rates. Throughput varies a lot between runs (ranges above). This is loopback,
  not a real network. No claim is made about absolute speed versus real Redis.

### 8. Known tradeoffs and limitations

* **Sharded LRU is approximate** (section 5, 7b). Strict global LRU needs a global lock.
* **`INFO` / `DBSIZE` under sharding are not atomic snapshots** while writes are in flight.
* **Multi-key `DEL` is not atomic** across shards.
* **No `MULTI`/transactions, no blocking commands.** Not needed yet; they would need cross-shard
  coordination.
* **Connections are balanced round-robin at accept time**, not by load. One very heavy connection
  pins its worker.
* **A worker's loop is not preemptible**: a huge pipelined batch from one client delays other
  clients on the same worker.
* **Active expiration is a periodic sweep driven by worker loops**, up to 1000 keys per call; a
  mass expiry drains over several ticks. Lazy expiration keeps reads correct meanwhile.
* **Hash-sharding does not fix a single hot key**: all traffic for one key still hits one lock.
* **Deadlines use `steady_clock`**, which is meaningless after a restart. Phase 5 (WAL) must log
  wall-clock deadlines.
* Memory is bounded by key count (`--max-keys`), not bytes.
* Linux only (epoll, eventfd).

### 9. Request flow for a concurrent request

`SET session:123 abc EX 60` arriving while 3 other clients are active:

1. The **acceptor** thread accepted this client earlier, picked worker 2 (round robin), pushed the
   socket onto worker 2's pending queue and wrote its `eventfd`. Worker 2 woke, registered the fd
   in its own epoll, and owns it from now on.
2. Worker 2's `epoll_wait` reports the fd readable. It `read()`s into the connection's input
   buffer (private to worker 2, no lock).
3. `parse_command` decodes RESP from the buffer, possibly several pipelined commands, or returns
   `Incomplete` and waits for more bytes.
4. `execute(engine, args)` dispatches to `cmd_set`, which parses `EX 60` into 60000 ms.
5. `engine.set(...)`. In sharded mode: `std::hash("session:123") % 64` selects shard 17, then
   `lock_guard` on **shard 17's mutex** (other workers using other shards keep running).
   Inside the lock, `Store::set` updates the hash map, links the key at the LRU front, evicts the
   shard's LRU key if the shard is full, and inserts `(now+60000, key)` into the deadline index.
   The lock is released as the guard leaves scope.
6. The reply `+OK\r\n` is appended to the connection's output buffer and written. If the socket
   buffer is full, worker 2 registers `EPOLLOUT` and finishes later without blocking.
7. About every 100 ms each worker calls `engine.sweep()`, which locks one shard at a time and
   removes expired keys from all three structures.

### 10. Layout

```
src/engine.hpp        Engine interface (thread-safety contract)
src/store.hpp         unsynchronized engine: hash map + LRU list + deadline index
src/synced_store.hpp  GlobalLockStore and ShardedStore
src/commands.*        command dispatch (works on any Engine)
src/resp.*            RESP parser and encoders
src/server.*          acceptor + worker threads, epoll, eventfd handoff
src/main.cpp          CLI
src/wal.*, persistence.*  write-ahead log, snapshots, recovery (Phase 5)
tests/                unit, concurrency and end-to-end tests; race_demo.cpp
bench/                bench_engine (locks), bench_lru_accuracy
scripts/              benchmark drivers and summarizers
docs/                 TSan logs and raw benchmark data
```

---

## Phase 5: durability (write-ahead log, snapshots, crash recovery)

```bash
./build/distcache --dir ./data --fsync everysec          # recover from ./data, then log to it
redis-cli -p 6380 SET session:1 abc EX 3600
redis-cli -p 6380 SAVE                                   # snapshot + compact the log
redis-cli -p 6380 INFO                                   # "# Persistence" section: fsync policy, log size, snapshots
```

Persistence is off unless `--dir` is given. Flags: `--dir PATH`, `--fsync always|everysec|no`
(default `everysec`), `--snapshot-mb N` (compact when a log segment passes N MiB, default 64,
0 = never).

### 1. Design

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

### 2. Tests

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

### 3. Crash test (real `kill -9`)

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

### 4. What durability costs (measured)

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

### 5. Recovery time (1M `SET` commands, 632k distinct keys)

| source | size on disk | time to start serving |
|---|---|---|
| log only (1,000,000 records replayed) | 109 MiB | 1.07 s, 1.11 s, 1.17 s |
| snapshot (compacted by `SAVE`) | 69 MiB | 0.57 s, 0.58 s, 0.62 s |

After every recovery the key count equalled the pre-shutdown count. The snapshot is about 2x
faster to load and about 40% smaller because overwritten values are gone. (Snapshot load
verifies the file first and applies it second, so it reads it twice; it is a simple way to
never apply a damaged snapshot.)

### 6. Known tradeoffs and limitations

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

### 7. Files

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
