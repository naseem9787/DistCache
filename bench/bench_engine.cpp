// In-process engine benchmark: N threads call the Engine directly, no sockets.
// This isolates the cost of the locking strategy from network/syscall overhead.
//
//   bench_engine <none|global|sharded> <threads> [seconds=3] [keyspace=100000] [read_pct=90] [shards=16]
//
// "none" is the unsynchronized Store and only makes sense with 1 thread (baseline).
// Prints one CSV line: engine,threads,shards,seconds,total_ops,ops_per_sec
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "store.hpp"
#include "synced_store.hpp"

namespace {
struct alignas(64) Counter { uint64_t ops = 0; };

inline uint64_t xorshift(uint64_t& s) {
    s ^= s << 13; s ^= s >> 7; s ^= s << 17;
    return s;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "usage: bench_engine <none|global|sharded> <threads> [seconds] [keyspace] [read_pct] [shards]\n"); return 2; }
    std::string kind = argv[1];
    int threads = std::atoi(argv[2]);
    double seconds = argc > 3 ? std::atof(argv[3]) : 3.0;
    size_t keyspace = argc > 4 ? static_cast<size_t>(std::atoll(argv[4])) : 100000;
    int read_pct = argc > 5 ? std::atoi(argv[5]) : 90;
    size_t shards = argc > 6 ? static_cast<size_t>(std::atoll(argv[6])) : 16;

    std::unique_ptr<Engine> e;
    if (kind == "none") {
        if (threads != 1) { std::fprintf(stderr, "'none' is unsynchronized: use 1 thread\n"); return 2; }
        e = std::make_unique<Store>();
    } else if (kind == "global") e = std::make_unique<GlobalLockStore>();
    else if (kind == "sharded") e = std::make_unique<ShardedStore>(shards);
    else { std::fprintf(stderr, "unknown engine\n"); return 2; }

    // Pre-build keys so string formatting doesn't dominate the measurement.
    std::vector<std::string> keys(keyspace);
    for (size_t i = 0; i < keyspace; ++i) keys[i] = "key:" + std::to_string(i);
    const std::string value(64, 'x');
    for (const auto& k : keys) e->set(k, value);

    std::atomic<bool> go{false}, stop{false};
    std::vector<Counter> counters(static_cast<size_t>(threads));
    std::vector<std::thread> ts;
    for (int t = 0; t < threads; ++t) {
        ts.emplace_back([&, t] {
            uint64_t rng = 0x9E3779B97F4A7C15ULL + static_cast<uint64_t>(t) * 7919;
            uint64_t ops = 0;
            while (!go.load(std::memory_order_acquire)) {}
            while (!stop.load(std::memory_order_relaxed)) {
                const std::string& k = keys[xorshift(rng) % keyspace];
                if (static_cast<int>(xorshift(rng) % 100) < read_pct) e->get(k);
                else e->set(k, value);
                ++ops;
            }
            counters[static_cast<size_t>(t)].ops = ops;
        });
    }
    auto t0 = std::chrono::steady_clock::now();
    go = true;
    std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
    stop = true;
    for (auto& th : ts) th.join();
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    uint64_t total = 0;
    for (auto& c : counters) total += c.ops;
    std::printf("%s,%d,%zu,%.2f,%llu,%.0f\n", kind.c_str(), threads, e->shards(), elapsed,
                static_cast<unsigned long long>(total), static_cast<double>(total) / elapsed);
    return e->consistent() ? 0 : 1;
}
