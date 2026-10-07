// Reproducible demonstration of the Phase 3 race conditions.
//
//   race_demo unsafe  [threads] [ops]   plain Store shared between threads (BUG on purpose)
//   race_demo global  [threads] [ops]   same workload through GlobalLockStore
//   race_demo sharded [threads] [ops]   same workload through ShardedStore
//
// Build with -fsanitize=thread and run "unsafe": ThreadSanitizer reports data races on the
// hash map, the LRU list and the deadline index. Run "global"/"sharded": no reports.
// (Without TSan "unsafe" may also crash or corrupt data - that is the point.)
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "store.hpp"
#include "synced_store.hpp"

int main(int argc, char** argv) {
    std::string kind = argc > 1 ? argv[1] : "unsafe";
    int threads = argc > 2 ? std::atoi(argv[2]) : 8;
    int ops = argc > 3 ? std::atoi(argv[3]) : 20000;

    std::unique_ptr<Engine> e;
    if (kind == "unsafe") e = std::make_unique<Store>(Store::real_now_ms, 256);
    else if (kind == "global") e = std::make_unique<GlobalLockStore>(Store::real_now_ms, 256);
    else if (kind == "sharded") e = std::make_unique<ShardedStore>(16, Store::real_now_ms, 256);
    else { std::fprintf(stderr, "usage: race_demo unsafe|global|sharded [threads] [ops]\n"); return 2; }

    std::printf("engine=%s threads=%d ops/thread=%d\n", kind.c_str(), threads, ops);
    std::fflush(stdout);

    std::vector<std::thread> ts;
    for (int t = 0; t < threads; ++t) {
        ts.emplace_back([&, t] {
            std::mt19937 rng(t);
            for (int i = 0; i < ops; ++i) {
                std::string key = "k" + std::to_string(rng() % 1000);
                switch (rng() % 10) {
                    case 0: case 1: case 2: e->set(key, "v"); break;
                    case 3: e->set(key, "v", static_cast<int64_t>(1 + rng() % 20)); break;  // TTL -> deadline index
                    case 4: case 5: case 6: case 7: e->get(key); break;  // "read" that reorders the LRU list
                    case 8: e->del(key); break;
                    default: e->sweep(8);
                }
            }
        });
    }
    for (auto& th : ts) th.join();

    std::printf("done: size=%zu evicted=%llu expired=%llu structures_consistent=%s\n", e->size(),
                static_cast<unsigned long long>(e->evicted_keys()),
                static_cast<unsigned long long>(e->expired_keys()), e->consistent() ? "yes" : "NO");
    return e->consistent() ? 0 : 1;
}
