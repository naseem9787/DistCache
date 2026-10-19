# Configuration reference

DistCache is configured with command-line flags only (no config file, no environment variables). Every
numeric flag is validated: `--port abc` or `--threads 0` is an error with a message and exit status 2,
never a silent default. `--help` prints the list.

## `distcache` (a cache node)

| flag | default | meaning |
|---|---|---|
| `--port N` | 6380 | client port (0 = pick a free port, printed at startup) |
| `--bind ADDR` | 127.0.0.1 | IPv4 address of the client **and** replication ports. `0.0.0.0` accepts other machines. **There is no authentication**: anyone who can connect can `PROMOTE`/`REPLICAOF` a node or read everything. |
| `--threads N` | 4 | worker threads (1-256) |
| `--mode M` | sharded | `single` (1 thread, no locks), `global` (one mutex), `sharded` (one mutex per shard) |
| `--shards N` | 64 | shards in sharded mode |
| `--max-keys N` | 0 | LRU capacity (0 = unlimited). **Evicting a rate-limiter bucket resets that limit.** |
| `--dir PATH` | off | enable persistence (WAL + snapshots) in PATH; recovery runs before the port opens |
| `--fsync P` | everysec | `always` (reply after fdatasync), `everysec`, `no`. Needs `--dir`. **It also decides failover policy**: only a node with `--fsync always` is re-adopted as primary after a restart; otherwise its replicas are promoted instead (see `docs/design/failure-model.md`). |
| `--snapshot-mb N` | 64 | compact the log with a snapshot when a segment passes N MiB (0 = off) |
| `--repl-port N` | off | serve replicas on this port (this node can be a replication primary). A replica keeps it for its own promotion. |
| `--replicaof H:P` | off | be a read-only replica of the primary whose **replication** port is H:P |
| `--repl-backlog-mb N` | 64 | in-memory replication log budget; a replica that falls further behind is cut off and fully resynchronised |
| `--repl-sync N` | 0 | acknowledge a write only after N replicas applied it (0 = async). While fewer than N are in sync: `-NOREPLICAS`. |
| `--repl-sync-timeout-ms N` | 1000 | if the N acks do not arrive, **close the connection** (the outcome of that write is unknown) |
| `--repl-require-lease` | off | refuse writes unless the router granted a valid write lease. **Always use this under `distcache_router`**: it is what fences an old primary. Without a valid lease the node also refuses reads (`-NOLEASE`) and refuses to serve replicas (`-ERR NOTVOUCHED`). |

Startup prints one line per component (recovery summary, replication role, `distcache VERSION listening on
ADDR:PORT`). `SIGTERM`/`SIGINT` stop accepting clients, stop replication, flush and fsync the log, print
`stopped cleanly` and exit 0. `kill -9` is always safe for data that was acknowledged under `--fsync always`
(or `--repl-sync N` with a surviving replica); see `docs/design/failure-model.md`.

## `distcache_router`

| flag | default | meaning |
|---|---|---|
| `--port N` | 6390 | client port |
| `--bind ADDR` | 127.0.0.1 | IPv4 address to listen on (no authentication) |
| `--node NAME=HOST:PORT[@WEIGHT]` | - | a shard (repeatable). HOST may be a name. |
| `--replica NAME=HOST:PORT` | - | a replica (client port) of shard NAME (repeatable). A shard with replicas is watched and failed over. |
| `--vnodes N` | 160 | ring points per unit of weight |
| `--timeout-ms N` | 3000 | connect/send/receive timeout towards nodes |
| `--lease-ms N` | 2000 | write-lease length L. Failover takes about `L*1.05 + guard` after the last renewal. |
| `--probe-ms N` | 500 | how often every member is probed and leases are renewed (keep it at most L/4) |
| `--probe-timeout-ms N` | 500 | a probe that takes longer counts as missed |
| `--fail-after N` | 3 | consecutive missed probes before a primary is declared failed |
| `--guard-ms N` | 500 | extra wait before promoting, for the clock-rate and message-delay assumptions |
| `--sync-replicas N` | 0 | must equal the nodes' `--repl-sync`: promotion then needs at least R-N+1 eligible replicas |
| `--state-file PATH` | off | remembers the highest epoch per shard, so a restarted router cannot adopt a stale primary. **Use it.** |

At runtime: `RING INFO`, `RING LOOKUP key`, `RING ADD name host port [weight] [MIGRATE]`, `RING REMOVE name [MIGRATE]`,
`RING RESUME` (finish a migration that had errors), `RING ABORT` (give up on it), `SHARD STATUS`, `SHARD EVENTS`, `INFO`.

## Commands (nodes and router)

`PING ECHO SET(EX|PX) GET DEL EXPIRE PEXPIRE TTL PTTL PERSIST DBSIZE KEYS INFO SAVE` and the rate limiter
`RL.ACQUIRE key capacity rate [cost]`, `RL.PEEK key capacity rate [cost]` (see `docs/design/rate-limiter.md`).
Node control commands, used by the router (and by hand in the demos): `ROLE`, `LEASE epoch ms`, `PROMOTE epoch`,
`REPLICAOF host port epoch`.

## Health and diagnostics

* **Liveness:** `redis-cli -p PORT PING` answers `PONG`.
* **Readiness:** `scripts/healthcheck.sh PORT` exits 0 when the process answers and, if it is a replica, its link is
  up and synchronised; for the router, when `SHARD STATUS` shows no unhealthy shard. The Docker image uses it as `HEALTHCHECK`.
* **`INFO`** has sections `# Server` (version, pid, uptime), `# Concurrency`, `# Stats` (keys, evictions, expirations,
  `rl_allowed`/`rl_denied`/`rl_errors`), `# Persistence` (WAL records/fsyncs, recovery time) and `# Replication`
  (role, epoch, lease remaining, offsets, per-replica state and lag, resync counters). The router's `INFO` shows the
  ring, `migrating`/`migration_pending`, forwarded commands and backend errors, and the shard states.
* **`SHARD EVENTS`** is the coordinator's timeline (failure detected, promoted, re-pointed, demoted, recovered).
