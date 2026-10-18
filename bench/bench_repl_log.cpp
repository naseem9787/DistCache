// Microbenchmark of the replication log alone (no engine, no sockets): how long does one ReplicationLog::on_set
// take as the number of writer threads grows? Separates "the work" (1 thread) from contention (more threads).
//   bench_repl_log [seconds] [value_bytes] [budget_mb]
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "repl_log.hpp"

int main(int argc, char** argv) {
    const double secs = argc > 1 ? std::atof(argv[1]) : 3;
    const size_t vbytes = argc > 2 ? static_cast<size_t>(std::atoll(argv[2])) : 64;
    const size_t budget_mb = argc > 3 ? static_cast<size_t>(std::atoll(argv[3])) : 64;
    const std::string value(vbytes, 'x');
    std::printf("on_set with a %zu-byte value, backlog budget %zu MiB (trimming active once full)\n", vbytes, budget_mb);
    std::printf("%8s %14s %14s\n", "threads", "total ops/s", "ns per op per thread");
    for (int threads : {1, 2, 4}) {
        ReplicationLog log(ReplicationLog::Options{budget_mb << 20, {}});
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> total{0};
        std::vector<std::thread> ts;
        const auto t0 = std::chrono::steady_clock::now();
        for (int t = 0; t < threads; ++t)
            ts.emplace_back([&, t] {
                uint64_t n = 0;
                char key[32];
                while (!stop.load(std::memory_order_relaxed)) {
                    std::snprintf(key, sizeof key, "key:%010llu", static_cast<unsigned long long>((n * 7919 + t) % 100000));
                    log.on_set(key, value, std::nullopt);
                    ++n;
                }
                total += n;
            });
        std::this_thread::sleep_for(std::chrono::duration<double>(secs));
        stop = true;
        for (auto& th : ts) th.join();
        const double el = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        std::printf("%8d %14.0f %14.0f\n", threads, total / el, el * 1e9 * threads / static_cast<double>(total));
    }
}
