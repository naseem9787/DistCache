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
        "  --snapshot-mb N compact the log with a snapshot when a segment passes N MiB (default 64, 0 = off)");
}

}  // namespace

int main(int argc, char** argv) {
    int port = 6380, threads = 4;
    size_t shards = 64, max_keys = 0;
    std::string mode = "sharded";
    std::string dir;
    FsyncPolicy fsync_policy = FsyncPolicy::EverySec;
    uint64_t snapshot_mb = 64;

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

    Server server(*engine, ServerOptions{port, threads, persistence.get()});
    if (!server.start(&err)) { std::fprintf(stderr, "failed to start: %s\n", err.c_str()); return 1; }
    std::printf("distcache listening on port %d (mode=%s, workers=%d, shards=%zu, max_keys=%zu)\n",
                server.port(), engine->mode(), std::max(1, threads), engine->shards(), engine->max_keys());
    std::fflush(stdout);

    while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    server.stop();                         // stop accepting and serving first...
    if (persistence) persistence->close(); // ...then flush and fsync the log
    return 0;
}
