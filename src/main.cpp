// distcache: command-line entry point.
//
//   distcache [--port N] [--threads N] [--mode single|global|sharded]
//             [--shards N] [--max-keys N]
//
// Old positional form still works: distcache <port> [max_keys]
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include "diag.hpp"
#include "node_replication.hpp"
#include "observer_chain.hpp"
#include "persistence.hpp"
#include "server.hpp"
#include "store.hpp"
#include "synced_store.hpp"

namespace {

volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

void usage() {
    std::puts(
        "usage: distcache [options]\n"
        "  --port N        listen port (default 6380)\n"
        "  --threads N     worker threads (default 4; forced to 1 in single mode)\n"
        "  --mode M        single | global | sharded   (default sharded)\n"
        "                    single  : one thread, no locks (baseline)\n"
        "                    global  : worker threads sharing one mutex\n"
        "                    sharded : worker threads, one mutex per shard\n"
        "  --shards N      number of shards in sharded mode (default 64)\n"
        "  --max-keys N    capacity before LRU eviction (default 0 = unlimited)\n"
        "  --dir PATH      enable persistence (write-ahead log + snapshots) in PATH\n"
        "  --fsync P       always | everysec | no   (default everysec; needs --dir)\n"
        "                    always   : reply only after fdatasync: no acknowledged write is lost\n"
        "                    everysec : fdatasync once a second: a power cut can lose ~1 s\n"
        "                    no       : leave flushing to the OS\n"
        "  --snapshot-mb N compact the log with a snapshot when a segment passes N MiB (default 64, 0 = off)\n"
        "  --repl-port N   serve replicas on this port (this node is a replication primary)\n"
        "  --replicaof H:P be a read-only replica of the primary whose --repl-port is H:P\n"
        "  --repl-backlog-mb N  replication backlog budget in MiB (default 64)\n"
        "  --repl-sync N   acknowledge a write only after N replicas applied it (default 0 = async);\n"
        "                  refuses writes (-NOREPLICAS) while fewer than N replicas are in sync\n"
        "  --repl-sync-timeout-ms N  close the connection if the N acks do not arrive in time (default 1000)");
}

}  // namespace

int main(int argc, char** argv) {
    int port = 6380, threads = 4;
    size_t shards = 64, max_keys = 0;
    std::string mode = "sharded";
    std::string dir;
    FsyncPolicy fsync_policy = FsyncPolicy::EverySec;
    uint64_t snapshot_mb = 64;
    NodeReplicationOptions ro;
    bool want_repl = false;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) { std::fprintf(stderr, "missing value for %s\n", a.c_str()); std::exit(2); }
            return argv[++i];
        };
        if (a == "--port") port = std::atoi(next());
        else if (a == "--threads") threads = std::atoi(next());
        else if (a == "--mode") mode = next();
        else if (a == "--shards") shards = static_cast<size_t>(std::atoll(next()));
        else if (a == "--max-keys") max_keys = static_cast<size_t>(std::atoll(next()));
        else if (a == "--dir") dir = next();
        else if (a == "--snapshot-mb") snapshot_mb = static_cast<uint64_t>(std::atoll(next()));
        else if (a == "--repl-port") { ro.repl_port = std::atoi(next()); want_repl = true; }
        else if (a == "--repl-sync") ro.sync_replicas = std::atoi(next());
        else if (a == "--repl-sync-timeout-ms") ro.sync_timeout_ms = std::atoi(next());
        else if (a == "--repl-backlog-mb") ro.backlog_bytes = static_cast<size_t>(std::atoll(next())) << 20;
        else if (a == "--replicaof") {
            const std::string spec = next();
            const size_t colon = spec.rfind(':');
            if (colon == std::string::npos || colon == 0) { std::fprintf(stderr, "--replicaof wants HOST:PORT\n"); return 2; }
            ro.replicaof_host = spec.substr(0, colon);
            ro.replicaof_port = std::atoi(spec.c_str() + colon + 1);
            want_repl = true;
        }
        else if (a == "--fsync") {
            if (!parse_fsync_policy(next(), fsync_policy)) {
                std::fprintf(stderr, "--fsync must be always, everysec or no\n");
                return 2;
            }
        }
        else if (a == "--help" || a == "-h") { usage(); return 0; }
        else if (a[0] != '-' && i == 1) port = std::atoi(a.c_str());                          // legacy
        else if (a[0] != '-' && i == 2) max_keys = static_cast<size_t>(std::atoll(a.c_str()));  // legacy
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); usage(); return 2; }
    }

    std::unique_ptr<Engine> engine;
    if (mode == "single") {
        if (threads != 1) std::fprintf(stderr, "note: single mode is unsynchronized, using 1 worker thread\n");
        threads = 1;
        engine = std::make_unique<Store>(Store::real_now_ms, max_keys);
    } else if (mode == "global") {
        engine = std::make_unique<GlobalLockStore>(Store::real_now_ms, max_keys);
    } else if (mode == "sharded") {
        // Each shard runs its own LRU over its slice of the capacity. With few keys per shard
        // that approximation gets poor (docs/benchmarks/lru-accuracy.txt), so when a capacity
        // is set, keep at least kMinKeysPerShard keys per shard.
        constexpr size_t kMinKeysPerShard = 1024;
        if (max_keys > 0) {
            size_t allowed = std::max<size_t>(1, max_keys / kMinKeysPerShard);
            if (shards > allowed) {
                std::fprintf(stderr, "note: --max-keys %zu allows at most %zu shards (>= %zu keys each); using %zu\n",
                             max_keys, allowed, kMinKeysPerShard, allowed);
                shards = allowed;
            }
        }
        engine = std::make_unique<ShardedStore>(shards, Store::real_now_ms, max_keys);
    } else {
        std::fprintf(stderr, "unknown mode '%s'\n", mode.c_str());
        usage();
        return 2;
    }

#ifdef DISTCACHE_DIAGNOSTICS
    // DISTCACHE_PROFILE=<file> [DISTCACHE_PROFILE_HZ=250]: in-process sampling profiler. The output
    // is symbolized offline by bench/harness/symbolize.py. Not available in the normal build.
    if (const char* p = std::getenv("DISTCACHE_PROFILE")) {
        const char* hz = std::getenv("DISTCACHE_PROFILE_HZ");
        diag::profiler_start(p, hz ? std::atoi(hz) : 250);
    }
#endif
    std::signal(SIGPIPE, SIG_IGN);  // writing to a closed socket returns EPIPE instead of killing us
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    // Persistence must come up BEFORE the server accepts clients: recovery fills the engine,
    // and the log must be attached before the first new write.
    std::unique_ptr<Persistence> persistence;
    std::string err;
    if (!dir.empty()) {
        PersistenceOptions po;
        po.dir = dir;
        po.fsync = fsync_policy;
        po.snapshot_wal_bytes = snapshot_mb << 20;
        persistence = std::make_unique<Persistence>(*engine, po);
        RecoveryStats rs;
        if (!persistence->open(&rs, &err)) { std::fprintf(stderr, "persistence: %s\n", err.c_str()); return 1; }
        std::printf("recovered: snapshot=%s (%llu keys), %llu log segments, %llu records replayed, "
                    "%llu already-expired skipped, %zu keys live\n",
                    rs.snapshot_loaded ? "yes" : "no", static_cast<unsigned long long>(rs.snapshot_keys),
                    static_cast<unsigned long long>(rs.segments_replayed),
                    static_cast<unsigned long long>(rs.records_replayed),
                    static_cast<unsigned long long>(rs.expired_skipped), engine->size());
        for (const auto& w : rs.warnings) std::fprintf(stderr, "recovery warning: %s\n", w.c_str());
    }

    // Replication (optional). The observer chain is built after persistence opened (open() installs
    // itself as the engine observer; the chain replaces that single slot) and before any thread
    // can touch the engine. Without --repl-port / --replicaof nothing here exists and the hook
    // pointer stays the plain Persistence, exactly as before Phase 8.
    std::unique_ptr<ReplicationService> repl;
    std::unique_ptr<NodeHooks> node_hooks;
    ObserverChain chain;
    CommandHooks* hooks = persistence.get();
    if (want_repl) {
        repl = std::make_unique<ReplicationService>(*engine, ro);
        if (persistence) chain.add(persistence.get());
        chain.add(repl->observer());
        engine->set_observer(&chain);
        if (!repl->start(&err)) { std::fprintf(stderr, "replication: %s\n", err.c_str()); return 1; }
        node_hooks = std::make_unique<NodeHooks>(persistence.get(), repl.get());
        hooks = node_hooks.get();
        if (repl->role() == ReplicationService::Role::Replica)
            std::printf("replica of %s:%d\n", ro.replicaof_host.c_str(), ro.replicaof_port);
        else
            std::printf("replication primary, replication port %d\n", repl->repl_port());
    }

    Server server(*engine, ServerOptions{port, threads, hooks});
    if (!server.start(&err)) { std::fprintf(stderr, "failed to start: %s\n", err.c_str()); return 1; }
    std::printf("distcache listening on port %d (mode=%s, workers=%d, shards=%zu, max_keys=%zu)\n",
                server.port(), engine->mode(), std::max(1, threads), engine->shards(), engine->max_keys());
    std::fflush(stdout);

    while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    server.stop();                         // stop accepting and serving first...
    if (repl) repl->stop();
    if (persistence) persistence->close(); // ...then flush and fsync the log
#ifdef DISTCACHE_DIAGNOSTICS
    diag::profiler_stop();
#endif
    return 0;
}
