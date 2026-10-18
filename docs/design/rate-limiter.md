# Distributed rate limiter: design (Phase 9)

Source: `src/rate_limiter.{hpp,cpp}` (algorithm and commands), `Engine::update` in `src/engine.hpp`,
`src/store.hpp`, `src/synced_store.hpp` (atomic read-modify-write), the `RL.*` cases in
`src/commands.cpp` and `src/router.cpp`. Tests: `tests/test_rate_limiter.cpp`,
`tests/test_rate_limiter_cluster.cpp`.

## 1. The interface

```
RL.ACQUIRE key capacity rate [cost]    take `cost` tokens if they are available      (a write)
RL.PEEK    key capacity rate [cost]    same answer, nothing is consumed or created   (a read)
```

* `capacity`: integer 1 .. 1,000,000,000 tokens (the burst size).
* `rate`: tokens per second, decimal with at most three decimals, 0.001 .. 1,000,000.
* `cost`: integer 1 .. capacity (default 1). A cost above the capacity can never succeed, so it is
  an error (`-ERR cost exceeds capacity`), not a denial.
* Reply, always an array of four integers:

| # | field | meaning |
|---|---|---|
| 1 | `allowed` | 1 = the tokens were taken, 0 = refused |
| 2 | `remaining` | whole tokens left after the decision |
| 3 | `retry_after_ms` | when denied: milliseconds until `cost` tokens will exist; `-1` when allowed |
| 4 | `reset_after_ms` | milliseconds until the bucket is full again |

Errors are ordinary RESP errors: wrong arity or bad numbers (`-ERR ...`), a key that holds a normal
value (`-WRONGTYPE ...`), a replica (`-READONLY`), a node without a lease (`-ERR ... lease`), a shard
the router has made unavailable (`-ERR shard 'x' is unavailable`), `-NOREPLICAS`, `-LOADING`.
**An error is neither "allowed" nor "denied": the caller must choose fail-open or fail-closed.**

The parameters come with every call (the client owns the policy; the server stores only the
state). Changing them is immediate: a lowered capacity clamps the stored tokens.

## 2. The algorithm (token bucket)

State per key: `tokens` (micro-tokens) and `last_ms` (wall clock). On a call at time `now`:

```
tokens = min(capacity, tokens + (now - last_ms) * rate)      only if now > last_ms; then last_ms = now
allowed iff tokens >= cost
allowed:  tokens -= cost; store (tokens, last_ms) with TTL = time until the bucket is full
denied:   store nothing
missing key == a full bucket
```

* **Exact arithmetic.** One token is 1,000,000 micro-tokens. A rate of `r` tokens/s is `1000*r`
  micro-tokens per millisecond, an integer because `r` has at most three decimals. No floating point
  anywhere, so there is no drift: polling every millisecond for an hour at 1 token/s grants exactly
  3,601 tokens (test `NoDriftOverManySmallSteps`).
* **Capacity** is a hard limit on any burst; **refill** is continuous.
* **A denial writes nothing** (so denied traffic costs no log record and no replication), with one
  exception (§4, clock repair).
* **Idle buckets disappear.** The stored key carries a TTL equal to the time the bucket needs to
  become full; when it expires, "missing" and "stored" mean the same thing. Memory is proportional to
  *recently active* buckets, not to every key ever seen.
* **The safety invariant** (checked by a randomised property test, exactly, in integers): for any
  schedule of calls the tokens granted in `[t0, t]` never exceed `capacity + rate * (t - t0)`.

## 3. Concurrency: why callers cannot collectively exceed the limit

The whole read-modify-write runs inside `Engine::update`, which holds the key's shard lock (one
mutex per shard, or the one global mutex). Two callers on the same key are serialised; there is no
window between "read tokens" and "store tokens" for another caller to slip into. The wall clock is
read **inside** that critical section, so clock readings are ordered like the updates they belong
to and `last_ms` can never go backwards within one node.

Tests: 16 threads against one key with a frozen clock are granted *exactly* the capacity (not one
more, not one fewer) on both the sharded and the global-lock engines; with a running clock the
total never exceeds `capacity + rate * elapsed`; 64 keys are each exact. A deliberately non-atomic
version (get, decide, set) fails these tests.

## 4. Clocks

The state holds **wall-clock** milliseconds, because after a failover a different machine's clock
reads it. Rules (all tested with injected time):

| situation | behaviour | direction |
|---|---|---|
| clock goes backwards | no refill; `last_ms` never moves back | stricter |
| stored `last_ms` slightly in the future (a failover onto a machine whose clock is behind) | no refill until our clock catches up | stricter (it costs the caller at most the skew) |
| stored `last_ms` more than one full-refill period in the future | treated as a clock fault: `last_ms := now`, and the repair is **stored even by a denied call** (otherwise every denied call would restart the repair and the bucket would stay frozen) | resumes service |
| clock jumps forward | refill is capped at capacity, so a jump can grant at most **one extra capacity** once | more permissive, bounded |
| huge elapsed values | handled without overflow (the refill is capped before multiplying) | |

A forward jump or a failover onto a machine whose clock runs ahead is the one way to be more
permissive than the ideal, and it is bounded by one capacity per bucket per event.

## 5. Where the state lives, and what that gives for free

A bucket is an ordinary string key `RL1 <micro-tokens> <last_ms>`. So:

* **Sharding.** The router sends `RL.*` by key like `GET`/`SET`. A bucket lives on exactly one shard;
  use a hash tag (`{tenant}:api`) to co-locate several. There is no cross-shard limit: *one bucket
  is one key*.
* **WAL.** The write is logged through the same `MutationObserver` as `SET`, as the **resulting
  state with an absolute deadline**, so replay is idempotent and a restart continues the refill
  (test: 6 tokens left at the crash, down for 2 s, 8 after recovery).
* **Replication.** The record the replicas receive is the state, not the command. Applying it
  twice, out of order within the offset gate, or out of a fuzzy snapshot is harmless (test with
  duplicated delivery: byte-identical state).
* **Sync replication.** An allowed answer is a write, so under `--repl-sync N` it is held back
  until N replicas have it (test: with the replica stalled the reply is withheld and the connection
  is closed on timeout). Denials are reads and are answered at once.
* **Live migration.** Buckets are moved like any key. During a migration the router sends `RL.*` to
  whichever node holds the bucket, and creates a missing bucket on the **new** owner (otherwise it
  would be orphaned on the old owner when the migration ends; the deliberate breakage is caught by
  `BucketsFirstTouchedWhileAMigrationRunsAreNotOrphanedOnTheOldOwner`).

## 6. Consistency and what happens on failure

| event | guarantee |
|---|---|
| normal operation | **Linearizable per bucket**: every `RL.ACQUIRE` is applied to the one primary, in a single total order, under a lock. Exact. |
| `RL.PEEK` on a replica | **Stale, advisory.** It shows the replica's state, which lags the primary by the replication lag. Never use it to *grant*. (Test: a lagging replica shows tokens while the primary denies.) |
| `RL.ACQUIRE` on a replica | refused (`-READONLY`). Replicas never hand out tokens. |
| primary without a valid lease | refuses (Phase 8 fencing); no tokens are granted by a fenced or expired primary |
| failover, **`--repl-sync N`** | **no consumed token is forgotten**: every allowed answer a client received was held until N replicas had it, and promotion picks a replica that has it (Phase 8 proof). The new primary admits exactly the remaining tokens (test: 30 consumed + the rest = exactly 50). |
| failover, **async** | the unreplicated tail is forgotten, so the new primary may **admit up to that many tokens twice**. Tested exactly: over-admission == tokens consumed in the unreplicated tail (bounded by replication lag x admission rate, measured in the benchmark). It is always bounded by the capacity, because a bucket never holds more than `capacity`. |
| failover onto a machine with a skewed clock | §4: behind = stricter, ahead = at most one extra capacity |
| old primary returns | demoted and fully resynchronised (Phase 8); it never grants tokens again from its stale state |
| partition / no safe primary | the router marks the shard **unavailable**: `RL.*` return `-ERR shard 'x' is unavailable`. The limiter fails *closed at the server*; the client decides whether to fail open (let the request through) or closed. |
| reply lost / timeout under sync N | the token **was consumed** but the caller was never told: a timeout burns a token (over-restrictive, the safe direction) |
| `--max-keys` eviction | an evicted bucket is a full bucket: **evictions reset limits**. Size `--max-keys` for the number of simultaneously active buckets, or do not combine a hard limit with eviction. |
| a normal key with that name already exists | `-WRONGTYPE`, the key is untouched |

### What this does not give

* No limit **across** keys or shards (a per-tenant limit must be one key; a global limit is one hot
  key on one shard, which is also its throughput ceiling: §7).
* No exactly-once for the *caller*: a retry after a timeout consumes another token.
* Not a sliding-log limiter: a token bucket allows a burst up to `capacity` at any moment, which is
  the documented behaviour, not a bug.
* With async replication, no protection of the unreplicated tail at failover (use `--repl-sync`).
* The accuracy of refill across a failover depends on the machines' wall clocks agreeing (NTP).

## 7. Cost

Measured in `docs/benchmarks/phase9/report.md`: an allowed call is a write (WAL record, replication
record), a denied call is a read. A hot key is serialised by its shard lock, so its throughput is
the single-lock rate; many keys scale with shards, threads and nodes.
