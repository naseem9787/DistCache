// Measures what per-shard (approximate) LRU costs compared with one global LRU.
//
// Workload: cycle through a working set of W distinct keys, repeatedly, on a store whose
// capacity is `cap`. If W <= cap an exact global LRU keeps every key: 100% hits after
// warm-up. A sharded store divides cap among N shards; keys hash unevenly, so some shards
// receive more keys than their slice holds and (with cyclic access) thrash. The hit rate
// drops below 100% even though the whole store has room: that is the price of sharding.
//
//   bench_lru_accuracy [capacity=1000]
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "synced_store.hpp"

namespace {
double hit_rate(Engine& e, size_t working_set, int passes) {
    std::vector<std::string> keys(working_set);
    for (size_t i = 0; i < working_set; ++i) keys[i] = "key:" + std::to_string(i);
    for (const auto& k : keys) e.set(k, "v");  // warm-up pass
    size_t hits = 0, total = 0;
    for (int p = 0; p < passes; ++p) {
        for (const auto& k : keys) {
            ++total;
            if (e.get(k)) ++hits; else e.set(k, "v");  // cache-aside: miss -> load -> store
        }
    }
    return 100.0 * static_cast<double>(hits) / static_cast<double>(total);
}
}  // namespace

int main(int argc, char** argv) {
    size_t cap = argc > 1 ? static_cast<size_t>(std::atoll(argv[1])) : 1000;
    std::printf("capacity=%zu, cyclic access, hit rate %% after warm-up (100 = exact LRU)\n", cap);
    std::printf("%-22s", "working set / shards");
    const size_t shard_counts[] = {1, 4, 16, 64, 256};
    for (size_t s : shard_counts) std::printf("%8zu", s);
    std::printf("\n");
    for (double frac : {0.50, 0.80, 0.95}) {
        size_t w = static_cast<size_t>(frac * static_cast<double>(cap));
        std::printf("W=%4zu (%3.0f%% of cap)  ", w, frac * 100);
        for (size_t s : shard_counts) {
            ShardedStore store(s, Store::real_now_ms, cap);
            std::printf("%8.1f", hit_rate(store, w, 20));
        }
        std::printf("\n");
    }
    std::printf("(effective capacity per column: ");
    for (size_t s : shard_counts) std::printf("%zu ", ShardedStore(s, Store::real_now_ms, cap).max_keys());
    std::printf(")\n");
}
