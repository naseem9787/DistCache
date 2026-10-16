# Replication and failover: design (Phase 8)

Status: the design is fixed here before the code is written; sections are marked with the build
step that implements them. Anything the design cannot guarantee is listed in "What is NOT
guaranteed".

## 0. Vocabulary

* **Shard**: one entry of the consistent-hash ring (Phase 7). Identified by NAME. Backed by a
  *replica group*: one primary and R replicas. R = the number of replicas the router has
  CONFIGURED for the shard, whether or not they are currently connected (the primary is not
  counted in R).
* **offset**: position of a record in the primary's replication log (1, 2, 3, ...).
* **replid**: random id of a log lineage (a process start or a promotion starts a new one).
* **epoch**: per-shard integer, bumped by the router on every promotion. Higher epoch wins.
* **ONLINE replica**: has finished its initial synchronisation (snapshot installed, applying the
  live stream in order) on the current lineage. A replica that is connecting, loading a
  snapshot, or has lost its stream is SYNCING and is never counted or promoted.
* **sync N**: the write policy "acknowledge a client write only after N replicas confirmed they
  applied it".

## 1. Why `sync N` loses no acknowledged write (proof)  [step 4 + 6]

Facts the code maintains:

* **F1 (prefix)** A replica applies records strictly in offset order (`offset == applied+1`,
  duplicates skipped, gaps refused). So "applied offset = j" means it applied every record <= j
  of the lineage.
* **F2 (ack = applied)** A replica sends `ACK j` only after applying j. The primary replies to
  the client for a write at offset o only when N distinct ONLINE replicas have acked some j >= o.
  Call that set A(o); |A(o)| >= N, and by F1 every member holds record o (and everything before).
* **F3 (promotion rule)** The router promotes only after collecting status from the replicas it
  can currently reach *and that are ONLINE on the failed primary's lineage*. Call that set U.
  It promotes only if `|U| >= R - N + 1`, and then promotes the member of U with the highest
  applied offset.

**Claim.** Every write acknowledged to a client under sync N is present on the promoted replica.

**Proof.** U and A(o) are both subsets of the R configured replicas. If they were disjoint,
R >= |U| + |A(o)| >= (R - N + 1) + N = R + 1, a contradiction. So some replica x is in
U ∩ A(o). By F2 x applied o; by F1 x's applied offset is >= o. The promoted replica has the
maximum applied offset in U, so it is >= x's, so by F1 it holds o. This holds for every
acknowledged o simultaneously because "maximum" is a single choice. ∎

What the proof needs, and how each case behaves:

* **The primary crashes before the router learns the last acknowledged offset.** The rule never
  uses the primary's view. It uses only the counts and offsets the *replicas* report. Writes the
  primary applied but never got N acks for were never acknowledged; they may or may not survive
  (the promoted replica may have received some), which is allowed: the client saw no success.
* **Replicas disconnected or lagging.** A disconnected replica is simply not in U (it still
  counts in R, so unreachable replicas make the rule stricter, never looser). A lagging replica is
  in U with a smaller offset and is never the maximum if some other member of U holds o. Lag
  cannot break the proof; it can only make the promoted replica the one that is further ahead.
* **A replica loses its memory after acking (process restart without state).** It comes back
  on a new lineage (SYNCING), so it is not in U. It also no longer holds o, but the claim only
  used members of U ∩ A(o), which are replicas that still report the lineage and the offset.
  The cost is availability: the rule may refuse to promote (below). It is never a safety hole.
* **Membership changes.** A replica added to the group raises R by one and cannot appear in any
  existing A(o) (it held nothing when o was acknowledged); the threshold rises with R, so the
  intersection argument still holds. Removing a replica is only allowed through the router and
  lowers R only if it keeps `R - N + 1 <= |U|` achievable; otherwise sync writes stop first.
* **Only the primary's local WAL fsync policy decides durability on the primary itself.** `sync N`
  means "in the memory of N other processes". If the primary and all N acking replicas lose
  power together, the write is lost unless those processes use `--fsync always`.
* **Fewer than N ONLINE replicas** (writes cannot be acknowledged): the primary refuses writes
  (`-NOREPLICAS`) instead of acknowledging weaker ones.
* **Under `async` there is NO such guarantee.** A promoted replica may lack acknowledged writes;
  how many is measured in the benchmarks (replication lag), not promised.

If `|U| < R - N + 1` the router does **not** promote (section 3): it would not be able to prove
that the promoted replica holds every acknowledged write.

## 2. Lease fencing  [step 5 + 6]

Goal: when the router promotes a replacement, the old primary must be *unable to acknowledge a
write* from then on, even if it is alive and still reachable by some clients.

### Parameters (all configurable; defaults)

| name | default | meaning |
|---|---|---|
| L | 2000 ms | lease length |
| renewal | L/4 | the router renews every 500 ms |
| D | 500 ms | request timeout: a lease message that is not answered in D is treated as "lost, and possibly delivered late" |
| eps | 5 % | assumed bound on relative clock RATE error between any two machines |
| G | 500 ms | guard band: scheduling / processing delay between a node's lease check and its reply |
| F | 3 | consecutive failed probes before a primary is declared failed |

### Clock assumptions

Only **rates** of monotonic clocks (`CLOCK_MONOTONIC`) are assumed: each party measures
durations on its own monotonic clock, differing from real time by at most the factor (1 +/- eps).
No clock synchronisation and no wall-clock time is used for leases. Also assumed: a lease
message is either delivered within D or never (the router closes the connection when a request
times out). Violations of these assumptions are listed under "not guaranteed".

### Node side: what a primary checks

The router sends `LEASE <epoch> <L>`. The node, on receipt at its monotonic time r, accepts it
only if `<epoch>` equals the node's current epoch, and sets
`lease_expiry = r + L * (1 - eps)` (it **shortens** its own lease).
Before executing any write command a primary performs, in this order:

1. role is PRIMARY (otherwise `-READONLY`);
2. the node is not fenced (a higher epoch was observed: `-READONLY`);
3. `monotonic_now < lease_expiry` (otherwise `-READONLY lease expired`);
4. under sync N: at least N ONLINE replicas are connected (otherwise `-NOREPLICAS`).

Under sync N a held reply is released only if check 3 **still holds at release time**; otherwise
the connection is closed (outcome unknown to the client). A node started under a router
(`--require-lease`) has no lease at start-up, hence refuses writes until the router grants one.
A standalone node (no router) does not require a lease.

### Router side: when promotion is allowed

For each grant the router records `t_ack` = its monotonic time when the node's reply arrived
(or, if no reply arrived, `t_send + D`). The node received the message at some time <= t_ack,
so under the rate assumption the node's lease cannot outlive

    safe_expiry = t_ack + L * (1 + eps) + G.

The router promotes a replacement only when `now >= safe_expiry` for the old primary's last
grant, AND the old primary has failed F consecutive probes. Until then the shard is
"failing over": writes get an error, reads may be served from replicas.

* **Long VM pause or SIGSTOP of the *primary*.** On resume its next command finds
  `monotonic_now >= lease_expiry` (CLOCK_MONOTONIC counts the pause on a stopped process;
  across a host suspend all clocks pause together) and is refused. The one residual window is
  a pause *between* check 3 and sending the reply that exceeds G; under async that single write
  could be acknowledged after the new primary exists. Under sync N the check at release time and
  the epoch fence on replicas (they reject a stream from a lower epoch) close it.
* **Long pause or restart of the *router*.** The router never acts on stale observations: after a
  gap longer than L/2 in its control loop it discards its view and needs F fresh failed probes
  *after* the gap before declaring anything failed, so a pause cannot by itself promote a healthy
  primary (and a primary whose lease lapsed during the pause simply gets a fresh grant: same
  epoch, no data lost). After a router **restart** it knows no grants, so it sets
  `safe_expiry = start + L*(1+eps) + G` for every node (the previous router instance may have
  granted a lease just before dying), learns the epoch as the maximum reported by the nodes, and
  only then may act. If it cannot identify exactly one primary at the highest epoch it **fails
  closed**: the shard accepts no writes through the router.
* **Fail closed**, concretely: no unique primary at the highest epoch; fewer than `R-N+1`
  eligible replicas; a replica whose lineage differs from the failed primary's; any
  unanswered status request; a grant whose outcome is unknown. In all of these the router does
  not promote and the shard refuses writes.
* **Two routers** are not supported (they would both grant leases); the same single-coordinator
  assumption as Phase 7's migration.

## 3. Mutation ordering and the observer  [step 1, implemented]

Verified in `src/store.hpp`: `Store::set` mutates the map, then calls `observer_->on_set(...)`,
all inside the engine's critical section (shard mutex, or the global mutex). Eviction victims
are deleted and reported (`on_del`) by `make_room()` *before* the new key's `on_set`, in the same
section. The lock is released only after the observer returned.

The observer slot now holds an `ObserverChain` of `[Persistence (WAL), ReplicationLog]`, called
in that order, each a short, infallible append:

* The WAL aborts the process on an I/O error (Phase 5 policy); the replication log cannot fail
  (a byte budget trims the oldest records instead, invariant I3).
* Lock order: engine lock -> WAL buffer lock -> replication-log mutex. The log mutex is never held
  while calling into the engine or the network.
* Consequence (invariant I2, tested by `AMutationIsNeverVisibleBeforeItsRecordExists`): another
  thread can observe a mutation only after the engine lock is released, and by then the mutation
  already has its WAL record AND its replication offset. There is no state in which a mutation is
  visible but has no offset, and per-key offset order equals the engine's order for that key
  (`ReplayingTheLogReproducesThePrimary` replays a log produced by four concurrent writers with
  evictions, TTLs, EXPIRE and PERSIST into an empty engine and compares the result).
* A client is acknowledged only after the chain ran (the reply is produced after `execute`
  returns), so an acknowledged write always has an offset. A crash between the WAL append and the
  replication append loses only an unacknowledged write on the primary's side; replicas never see
  a record the primary's WAL lacks the other way round because the WAL is appended first.
* Not replicated, on purpose: reads (LRU recency) and lazy/active expiry. Replicas apply the
  absolute deadline themselves, so a key disappears on every node at the same wall-clock time up
  to clock skew (documented), and promotion preserves TTLs exactly. Evictions ARE replicated (as
  `DEL`) and replicas never evict on their own, otherwise their LRU order (which differs, reads
  are not replicated) would pick different victims.

## 4. Full resynchronisation  [step 2]

Request: the replica connects to the primary's replication port and sends
`SYNC <replid> <next_offset> <epoch>`.

* **Partial resync.** If `replid` is the primary's current lineage (or the lineage it was
  promoted from, with `next_offset-1 <= promotion offset`) and `next_offset` is still in the
  backlog (`read_from` returns Ok), the primary just streams records from `next_offset`.
* **Full resync** otherwise (unknown lineage, offset trimmed away, replica ahead of the primary,
  fresh replica):
  1. Under the replication-log mutex the primary reads `S = last_offset`. Every record <= S was
     appended after its mutation was applied (invariant I2), so the engine already reflects all
     records <= S.
  2. It sends `FULLSYNC <replid> <epoch> <S>` and then walks the engine with `for_each` (a copy of
     one shard at a time, not a point-in-time snapshot across shards), sending each live entry as
     a `SET key value [PXAT abs]`. Concurrent writes keep going; they are appended to the log at
     offsets > S as usual.
  3. It sends `SNAPEND` and then streams records from offset S+1.
  The replica marks itself SYNCING, **clears its engine** (logging a `FLUSHALL` record in its own
  WAL so recovery agrees), applies the snapshot entries, and on `SNAPEND` sets `applied = S`; from
  then on it accepts only `offset == applied+1`. It becomes ONLINE when it has applied the stream
  up to the primary's last known offset (catch-up), and is never counted for acks or promotion
  before that.
* **Why this converges (no loss, no duplicate, no gap).** For a key k, the snapshot value v_k
  is the state of k at some moment tau_k that is not earlier than the moment S was read, so it
  already includes every record of k with offset <= S. Records of k with offset > S are all
  streamed, in order, after the snapshot; applying them (absolute, idempotent operations)
  leaves the state of the last one regardless of whether v_k already included some of them. If k
  has no record after S, v_k is final. No record > S can be skipped (the backlog still holds
  S+1.. unless it was trimmed; then the primary aborts this resync with an error and the replica
  retries, nothing is half-applied that matters because the replica is SYNCING). No record is
  applied twice by the offset gate. During the replay a key may temporarily show an *older*
  value than the snapshot's; this is invisible because the replica is SYNCING (reads are
  refused) and, once ONLINE, "applied = j" implies every key is at least as new as its state at j.
* **The snapshot is a fuzzy copy, like Phase 5's**: correctness comes from idempotent absolute
  records plus replay from the recorded boundary, not from freezing the engine.
* **Backpressure**: a full resync that outlasts the backlog budget fails and is retried; the
  budget (`--repl-backlog-mb`) is the knob. Writers are never slowed.

## 5. Promotion, recovery of an old primary, and routing  [steps 5-6]

* **Choice**: the eligible replica with the highest applied offset (ties: lowest name). Eligible =
  ONLINE on the failed primary's lineage, reachable, and the quorum rule of section 1 satisfied
  (under async: at least one eligible replica and, optionally, lag below `--failover-max-lag`).
* **Promotion**: router sends `PROMOTE <new_epoch>`; the node becomes PRIMARY with a new
  replid, keeps its log (offsets continue), and starts accepting the other replicas. The router
  then points the remaining replicas at it (`REPLICAOF`); replicas whose applied offset is
  within the new primary's history continue by partial resync, a replica that is *ahead* of it
  (it applied records the new primary lacks) does a full resync and thereby discards that suffix.
* **No safe replica**: the shard refuses writes (`-ERR shard unavailable`), reads continue from
  replicas that are ONLINE. Nothing is promoted on a guess.
* **Old primary returns**: it carries a lower epoch. On first contact with the router (or with any
  node reporting a higher epoch) it demotes itself to replica and **always full-resyncs**, so its
  possibly divergent suffix is discarded: data never flows from an old primary to a newer one.
  Its own WAL contents are superseded (the `FLUSHALL` record) and cannot resurrect.
* **Routing**: the ring identifies a shard by NAME; only the shard's primary address changes, so no
  key moves. The router sends writes to the current primary only.
* **Migration (Phase 7) during failover**: migration steps address the shard's current primary.
  If it fails mid-step the step is counted in `errors=` and the key stays where it was; failover
  proceeds independently; the migrating route keeps working through the new primary.

## 6. What is NOT guaranteed (so nobody has to discover it)

* `async` loses acknowledged writes on failover (bounded by lag, measured).
* The router is a single point of failure: if it dies, no failover happens (primaries keep
  serving until their leases expire and then refuse writes: the shard becomes read-only rather
  than risk two primaries).
* Safety of fencing rests on the clock-RATE and message-delay assumptions of section 2. A
  violation (a >G pause between a lease check and its reply on async, a message delayed beyond D)
  can produce one stale acknowledged write, not unbounded divergence, and cannot happen under
  sync N because of the epoch fence.
* Absolute TTL deadlines assume the machines' wall clocks agree to within the tolerated TTL
  skew (NTP).
* `sync N` durability is memory on N processes, not disk.
* No quorum/consensus: this is single-coordinator failover, not Raft.
