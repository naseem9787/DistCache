// Concurrency tests for the thread-safe engines (GlobalLockStore, ShardedStore).
// They check two kinds of things:
//   * values: a GET must never return a torn or foreign value
//   * structure: after the storm, consistent() proves map / LRU list / deadline index agree
// They are most valuable under ThreadSanitizer (see README), which also flags any
// unsynchronized access that happens to produce the right answer by luck.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "commands.hpp"
#include "store.hpp"
#include "synced_store.hpp"

namespace {

constexpr int kThreads = 8;

std::unique_ptr<Engine> make_engine(const std::string& kind, size_t cap = 0,
                                    Store::Clock clock = Store::real_now_ms) {
    if (kind == "global") return std::make_unique<GlobalLockStore>(clock, cap);
    return std::make_unique<ShardedStore>(8, clock, cap);
}

void run_threads(int n, const std::function<void(int)>& body) {
    std::vector<std::thread> ts;
    for (int t = 0; t < n; ++t) ts.emplace_back(body, t);
    for (auto& th : ts) th.join();
}

std::string key_of(int i) { return "k" + std::to_string(i); }

}  // namespace

class EngineConcurrency : public ::testing::TestWithParam<std::string> {};
INSTANTIATE_TEST_SUITE_P(Engines, EngineConcurrency, ::testing::Values("global", "sharded"),
                         [](const auto& info) { return info.param; });

TEST_P(EngineConcurrency, ManyConcurrentGets) {
    auto e = make_engine(GetParam());
    constexpr int kKeys = 2000;
    for (int i = 0; i < kKeys; ++i) e->set(key_of(i), "val-" + key_of(i));

    std::atomic<int> wrong{0};
    run_threads(kThreads, [&](int t) {
        std::mt19937 rng(t);
        for (int i = 0; i < 10000; ++i) {
            int k = static_cast<int>(rng() % kKeys);
            auto v = e->get(key_of(k));
            if (!v || *v != "val-" + key_of(k)) ++wrong;
        }
    });
    EXPECT_EQ(wrong.load(), 0);
    EXPECT_EQ(e->size(), static_cast<size_t>(kKeys));  // GETs never lose or duplicate keys
    EXPECT_TRUE(e->consistent());                      // GET rewires the LRU list: it must stay valid
}

TEST_P(EngineConcurrency, ConcurrentSetGetDelOnOverlappingKeys) {
    auto e = make_engine(GetParam());
    constexpr int kKeys = 64;  // few keys + many threads = maximum contention
    std::atomic<int> torn{0};
    run_threads(kThreads, [&](int t) {
        std::mt19937 rng(100 + t);
        for (int i = 0; i < 10000; ++i) {
            int k = static_cast<int>(rng() % kKeys);
            std::string key = key_of(k);
            switch (rng() % 3) {
                case 0: e->set(key, key + "|t" + std::to_string(t)); break;
                case 1: {
                    auto v = e->get(key);
                    if (v && v->compare(0, key.size() + 1, key + "|") != 0) ++torn;  // value belongs to this key
                    break;
                }
                default: e->del(key);
            }
        }
    });
    EXPECT_EQ(torn.load(), 0);
    EXPECT_LE(e->size(), static_cast<size_t>(kKeys));
    EXPECT_TRUE(e->consistent());
}

TEST_P(EngineConcurrency, DisjointKeysEndInExactlyTheExpectedState) {
    auto e = make_engine(GetParam());
    constexpr int kPer = 500;
    run_threads(kThreads, [&](int t) {
        for (int i = 0; i < kPer; ++i) e->set("t" + std::to_string(t) + ":" + std::to_string(i), "v" + std::to_string(i));
        for (int i = 0; i < kPer; i += 2) EXPECT_TRUE(e->del("t" + std::to_string(t) + ":" + std::to_string(i)));
    });
    EXPECT_EQ(e->size(), static_cast<size_t>(kThreads * kPer / 2));
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kPer; ++i) {
            auto v = e->get("t" + std::to_string(t) + ":" + std::to_string(i));
            if (i % 2 == 0) EXPECT_FALSE(v);
            else EXPECT_EQ(v, "v" + std::to_string(i));
        }
    }
    EXPECT_TRUE(e->consistent());
}

TEST_P(EngineConcurrency, ConcurrentExpiration) {
    auto e = make_engine(GetParam());
    constexpr int kKeys = 300;
    for (int i = 0; i < 50; ++i) e->set("perm" + std::to_string(i), "keep");  // never expires

    std::atomic<bool> stop{false};
    std::thread sweeper([&] {
        while (!stop) { e->sweep(); std::this_thread::yield(); }
    });
    run_threads(kThreads, [&](int t) {
        std::mt19937 rng(7 + t);
        for (int i = 0; i < 5000; ++i) {
            std::string key = "tmp" + std::to_string(rng() % kKeys);
            switch (rng() % 5) {
                case 0: e->set(key, "v", static_cast<int64_t>(1 + rng() % 5)); break;   // 1-5 ms TTL
                case 1: e->get(key); break;                                              // lazy expiry path
                case 2: e->ttl_ms(key); break;
                case 3: e->expire(key, 1 + rng() % 5); break;
                default: e->persist(key);
            }
        }
    });
    stop = true;
    sweeper.join();

    // Anything left either has no TTL (persisted) or has a short one; drain what's due.
    for (int i = 0; i < kKeys; ++i) e->del("tmp" + std::to_string(i));
    EXPECT_EQ(e->size(), 50u);
    for (int i = 0; i < 50; ++i) EXPECT_EQ(e->get("perm" + std::to_string(i)), "keep");
    EXPECT_GT(e->expired_keys(), 0u);
    EXPECT_TRUE(e->consistent());
}

TEST_P(EngineConcurrency, ConcurrentLruUpdatesRespectCapacity) {
    auto e = make_engine(GetParam(), 96);
    const size_t cap = e->max_keys();
    ASSERT_GT(cap, 0u);

    std::atomic<bool> stop{false};
    std::atomic<size_t> max_seen{0};
    std::thread observer([&] {  // capacity must hold at every instant, not just at the end
        while (!stop) {
            size_t s = e->size();
            size_t prev = max_seen.load();
            while (s > prev && !max_seen.compare_exchange_weak(prev, s)) {}
        }
    });
    run_threads(kThreads, [&](int t) {
        std::mt19937 rng(31 + t);
        for (int i = 0; i < 8000; ++i) {
            std::string key = key_of(static_cast<int>(rng() % 1000));  // 1000 keys >> capacity
            if (rng() % 2) e->set(key, "v"); else e->get(key);          // GET reorders the LRU list
        }
    });
    stop = true;
    observer.join();
    EXPECT_LE(max_seen.load(), cap);
    EXPECT_LE(e->size(), cap);
    EXPECT_TRUE(e->consistent());
}

TEST_P(EngineConcurrency, ConcurrentEvictionAccountingIsExact) {
    auto e = make_engine(GetParam(), 96);
    constexpr int kPer = 2000;
    run_threads(kThreads, [&](int t) {
        for (int i = 0; i < kPer; ++i) e->set("u" + std::to_string(t) + "-" + std::to_string(i), "v");
    });
    // Every insert used a brand-new key, so each one is either still stored or was evicted.
    EXPECT_EQ(e->size() + e->evicted_keys(), static_cast<uint64_t>(kThreads) * kPer);
    EXPECT_LE(e->size(), e->max_keys());
    EXPECT_TRUE(e->consistent());
}

TEST_P(EngineConcurrency, MixedWorkloadWithStatsReaders) {
    auto e = make_engine(GetParam(), 200);
    std::atomic<bool> stop{false};
    std::atomic<int> bad_info{0};
    std::thread stats([&] {  // INFO / DBSIZE running alongside all the writers
        while (!stop) {
            std::string info = execute(*e, {"INFO"});
            if (info.find("evicted_keys:") == std::string::npos) ++bad_info;
            execute(*e, {"DBSIZE"});
        }
    });
    std::thread sweeper([&] { while (!stop) { e->sweep(); std::this_thread::yield(); } });

    run_threads(kThreads, [&](int t) {
        std::mt19937 rng(900 + t);
        for (int i = 0; i < 6000; ++i) {
            std::string key = key_of(static_cast<int>(rng() % 600));
            switch (rng() % 8) {
                case 0: case 1: e->set(key, key + "|x"); break;
                case 2: e->set(key, key + "|x", static_cast<int64_t>(1 + rng() % 10)); break;
                case 3: case 4: case 5: e->get(key); break;
                case 6: e->del(key); break;
                default: e->expire(key, 1 + rng() % 10);
            }
        }
    });
    stop = true;
    stats.join();
    sweeper.join();
    EXPECT_EQ(bad_info.load(), 0);
    EXPECT_LE(e->size(), e->max_keys());
    EXPECT_TRUE(e->consistent());
}

// --- single-threaded checks of the sharded design decisions ---------------

TEST(ShardedStoreDesign, SweepHonoursItsBudgetAcrossShards) {
    int64_t t = 1000;
    ShardedStore s(8, [&] { return t; });
    for (int i = 0; i < 200; ++i) s.set(key_of(i), "v", 100);
    t += 500;
    EXPECT_EQ(s.sweep(30), 30u);
    EXPECT_EQ(s.sweep(30), 30u);
    size_t rest = 0, got;
    while ((got = s.sweep(50)) > 0) rest += got;
    EXPECT_EQ(60u + rest, 200u);
    EXPECT_EQ(s.size(), 0u);
    EXPECT_EQ(s.expired_keys(), 200u);
}

TEST(ShardedStoreDesign, CapacityIsDividedAndNeverZeroPerShard) {
    ShardedStore few_keys(16, Store::real_now_ms, 3);  // more shards than capacity
    EXPECT_EQ(few_keys.shards(), 3u);                  // clamped so each shard holds >= 1
    EXPECT_EQ(few_keys.max_keys(), 3u);
    ShardedStore rounded(8, Store::real_now_ms, 100);  // 100/8 rounds up to 13 per shard
    EXPECT_EQ(rounded.max_keys(), 104u);
    for (int i = 0; i < 1000; ++i) rounded.set(key_of(i), "v");
    EXPECT_LE(rounded.size(), rounded.max_keys());
    EXPECT_TRUE(rounded.consistent());
}

TEST(ShardedStoreDesign, SameKeyAlwaysLandsInTheSameShard) {
    ShardedStore s(8);
    for (int i = 0; i < 500; ++i) s.set(key_of(i), "a");
    for (int i = 0; i < 500; ++i) s.set(key_of(i), "b");  // overwrite must find the old entry
    EXPECT_EQ(s.size(), 500u);
    for (int i = 0; i < 500; ++i) EXPECT_EQ(s.get(key_of(i)), "b");
}

// Random single-thread operations with a moving fake clock, checking that all three
// internal structures agree after EVERY step. This is the cheapest way to catch a
// forgotten drop_expiry()/lru erase before concurrency makes it hard to reproduce.
TEST(StoreFuzz, StructuresStayConsistentAfterEveryOperation) {
    int64_t t = 1000;
    Store s([&] { return t; }, 20);
    std::mt19937 rng(12345);
    for (int i = 0; i < 20000; ++i) {
        std::string key = key_of(static_cast<int>(rng() % 50));
        switch (rng() % 8) {
            case 0: s.set(key, "v"); break;
            case 1: s.set(key, "v", static_cast<int64_t>(1 + rng() % 50)); break;
            case 2: s.get(key); break;
            case 3: s.del(key); break;
            case 4: s.expire(key, static_cast<int64_t>(rng() % 50) - 5); break;
            case 5: s.persist(key); break;
            case 6: s.sweep(static_cast<size_t>(1 + rng() % 5)); break;
            default: t += static_cast<int64_t>(rng() % 20);
        }
        ASSERT_TRUE(s.consistent()) << "inconsistent after op " << i;
    }
}
