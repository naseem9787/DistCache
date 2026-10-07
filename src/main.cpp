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
        "  --max-keys N    capacity before LRU eviction (default 0 = unlimited)");
}

}  // namespace

int main(int argc, char** argv) {
    int port = 6380, threads = 4;
    size_t shards = 64, max_keys = 0;
    std::string mode = "sharded";

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

    Server server(*engine, ServerOptions{port, threads});
    std::string err;
    if (!server.start(&err)) { std::fprintf(stderr, "failed to start: %s\n", err.c_str()); return 1; }
    std::printf("distcache listening on port %d (mode=%s, workers=%d, shards=%zu, max_keys=%zu)\n",
                server.port(), engine->mode(), std::max(1, threads), engine->shards(), engine->max_keys());
    std::fflush(stdout);

    while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    server.stop();
    return 0;
}
