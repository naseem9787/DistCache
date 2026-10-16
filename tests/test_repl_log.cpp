// Replication log, observer chain and the shared record-apply function (Phase 8, step 1).
// The tests are organised around the invariants I1-I4 documented in repl_log.hpp.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "observer_chain.hpp"
#include "record_apply.hpp"
#include "repl_log.hpp"
#include "resp.hpp"
#include "store.hpp"
#include "synced_store.hpp"

namespace {

constexpr int64_t kWall = 1'000'000;  // fixed "wall clock" for exact-bytes tests

ReplicationLog::Options fixed_wall(size_t max_bytes = 64u << 20) {
    ReplicationLog::Options o;
    o.max_bytes = max_bytes;
    o.wall_ms = [] { return kWall; };
    return o;
}

std::vector<std::string> args_of(const std::string& payload) {
    ParseResult r = parse_command(payload);
    EXPECT_EQ(r.status, ParseStatus::Ok);
    EXPECT_EQ(r.consumed, payload.size());
    return r.args;
}

std::vector<ReplRecord> all_records(const ReplicationLog& log) {
    std::vector<ReplRecord> v;
    EXPECT_EQ(log.read_from(log.first_offset(), SIZE_MAX, &v), ReplicationLog::ReadStatus::Ok);
    return v;
}

int64_t real_wall() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

struct State {
    std::string value;
    bool has_ttl;
    int64_t ttl;
    bool operator==(const State& o) const { return value == o.value && has_ttl == o.has_ttl; }
};

std::map<std::string, State> dump(const Engine& e) {
    std::map<std::string, State> m;
    e.for_each([&](const std::string& k, const std::string& v, int64_t ttl) { m[k] = State{v, ttl >= 0, ttl}; });
    return m;
}

}  // namespace

// ---- encoding -------------------------------------------------------------------------------

TEST(ReplLog, EncodesEveryMutationKindExactly) {
    ReplicationLog log(fixed_wall());
    log.on_set("k", "v", std::nullopt);
    log.on_set("k", "v", 500);
    log.on_del("k");
    log.on_expire("k", 2000);
    log.on_persist("k");
    auto recs = all_records(log);
    ASSERT_EQ(recs.size(), 5u);
    EXPECT_EQ(recs[0].payload, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    EXPECT_EQ(recs[1].payload, "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$4\r\nPXAT\r\n$7\r\n1000500\r\n");  // absolute deadline
    EXPECT_EQ(recs[2].payload, "*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n");
    EXPECT_EQ(recs[3].payload, "*3\r\n$9\r\nPEXPIREAT\r\n$1\r\nk\r\n$7\r\n1002000\r\n");
    EXPECT_EQ(recs[4].payload, "*2\r\n$7\r\nPERSIST\r\n$1\r\nk\r\n");
}

TEST(ReplLog, BinarySafeKeysAndValues) {
    ReplicationLog log(fixed_wall());
    const std::string key("a\r\nb", 4), val("x\0y\r\n", 5);
    log.on_set(key, val, std::nullopt);
    auto a = args_of(all_records(log)[0].payload);
    ASSERT_EQ(a.size(), 3u);
    EXPECT_EQ(a[1], key);
    EXPECT_EQ(a[2], val);
}

// ---- I1: offsets -------------------------------------------------------------------------------

TEST(ReplLog, OffsetsAreConsecutiveFromOne) {
    ReplicationLog log(fixed_wall());
    EXPECT_EQ(log.last_offset(), 0u);
    EXPECT_EQ(log.first_offset(), 1u);  // empty: the next offset to be assigned
    for (int i = 0; i < 100; ++i) log.on_del("k" + std::to_string(i));
    EXPECT_EQ(log.last_offset(), 100u);
    auto recs = all_records(log);
    ASSERT_EQ(recs.size(), 100u);
    for (size_t i = 0; i < recs.size(); ++i) EXPECT_EQ(recs[i].offset, i + 1);
}

TEST(ReplLog, ConcurrentAppendersGetDistinctConsecutiveOffsets) {
    ReplicationLog log(fixed_wall(1u << 30));
    constexpr int kThreads = 4, kEach = 5000;
    std::atomic<bool> done{false};
    std::atomic<int> violations{0};
    std::thread consumer([&] {  // reads while the producers run
        uint64_t next = 1;
        while (!done || next <= log.last_offset()) {
            std::vector<ReplRecord> v;
            if (log.read_from(next, 4096, &v) != ReplicationLog::ReadStatus::Ok) { ++violations; return; }
            for (auto& r : v) {
                if (r.offset != next) ++violations;
                ++next;
            }
            if (v.empty()) log.wait_beyond(next - 1, 5);
        }
    });
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&, t] { for (int i = 0; i < kEach; ++i) log.on_del("t" + std::to_string(t) + ":" + std::to_string(i)); });
    for (auto& th : ts) th.join();
    done = true;
    consumer.join();
    EXPECT_EQ(violations.load(), 0);
    EXPECT_EQ(log.last_offset(), static_cast<uint64_t>(kThreads * kEach));
    EXPECT_EQ(log.stats().appended, static_cast<uint64_t>(kThreads * kEach));
}

// ---- I3: bounded memory, slow consumers are cut off, writers never block --------------------------

TEST(ReplLog, ByteBudgetDropsOldestAndReportsTooOld) {
    ReplicationLog log(fixed_wall(2000));
    for (int i = 0; i < 200; ++i) log.on_del("key-number-" + std::to_string(i));
    const auto st = log.stats();
    EXPECT_GT(st.trimmed, 0u);
    EXPECT_LE(st.buffered_bytes, 2000u + 200);  // budget, plus at most the newest record's slack
    EXPECT_EQ(log.last_offset(), 200u);
    const uint64_t first = log.first_offset();
    EXPECT_GT(first, 1u);

    std::vector<ReplRecord> v;
    EXPECT_EQ(log.read_from(1, SIZE_MAX, &v), ReplicationLog::ReadStatus::TooOld);
    EXPECT_TRUE(v.empty());
    EXPECT_EQ(log.read_from(first - 1, SIZE_MAX, &v), ReplicationLog::ReadStatus::TooOld);
    EXPECT_EQ(log.read_from(first, SIZE_MAX, &v), ReplicationLog::ReadStatus::Ok);
    ASSERT_EQ(v.size(), 200u - (first - 1));
    for (size_t i = 0; i < v.size(); ++i) EXPECT_EQ(v[i].offset, first + i);  // what remains is still gapless
}

TEST(ReplLog, ReadBoundariesCaughtUpAndFuture) {
    ReplicationLog log(fixed_wall());
    for (int i = 0; i < 10; ++i) log.on_del("k");
    std::vector<ReplRecord> v;
    EXPECT_EQ(log.read_from(11, SIZE_MAX, &v), ReplicationLog::ReadStatus::Ok);  // caught up
    EXPECT_TRUE(v.empty());
    EXPECT_EQ(log.read_from(12, SIZE_MAX, &v), ReplicationLog::ReadStatus::Future);  // claims to be ahead
    EXPECT_EQ(log.read_from(10, SIZE_MAX, &v), ReplicationLog::ReadStatus::Ok);
    ASSERT_EQ(v.size(), 1u);
    EXPECT_EQ(v[0].offset, 10u);
}

TEST(ReplLog, ReadHonoursByteLimitButAlwaysMakesProgress) {
    ReplicationLog log(fixed_wall());
    for (int i = 0; i < 20; ++i) log.on_set("k" + std::to_string(i), std::string(100, 'x'), std::nullopt);
    std::vector<ReplRecord> v;
    ASSERT_EQ(log.read_from(1, 1, &v), ReplicationLog::ReadStatus::Ok);  // limit smaller than one record
    EXPECT_EQ(v.size(), 1u);                                              // still returns one
    v.clear();
    ASSERT_EQ(log.read_from(1, 500, &v), ReplicationLog::ReadStatus::Ok);
    EXPECT_GT(v.size(), 1u);
    EXPECT_LT(v.size(), 20u);
}

// ---- I4: the replica-side copy ------------------------------------------------------------------

TEST(ReplLog, ReplicatedAppendIgnoresDuplicatesAndRefusesGaps) {
    ReplicationLog log(fixed_wall());
    log.reset("lineage-A", 0);
    using S = ReplicationLog::AppendStatus;
    EXPECT_EQ(log.append_replicated(1, "p1"), S::Appended);
    EXPECT_EQ(log.append_replicated(2, "p2"), S::Appended);
    EXPECT_EQ(log.append_replicated(2, "p2"), S::Duplicate);   // redelivery: ignored
    EXPECT_EQ(log.append_replicated(1, "p1"), S::Duplicate);
    EXPECT_EQ(log.append_replicated(4, "p4"), S::Gap);         // 3 is missing: refused, state unchanged
    EXPECT_EQ(log.last_offset(), 2u);
    EXPECT_EQ(log.append_replicated(3, "p3"), S::Appended);
    auto recs = all_records(log);
    ASSERT_EQ(recs.size(), 3u);
    EXPECT_EQ(recs[2].payload, "p3");
}

TEST(ReplLog, ResetStartsANewLineage) {
    ReplicationLog log(fixed_wall());
    for (int i = 0; i < 5; ++i) log.on_del("k");
    const std::string old = log.replid();
    log.reset("fresh-lineage", 100);
    EXPECT_EQ(log.replid(), "fresh-lineage");
    EXPECT_NE(log.replid(), old);
    EXPECT_EQ(log.last_offset(), 100u);
    EXPECT_EQ(log.first_offset(), 101u);
    std::vector<ReplRecord> v;
    EXPECT_EQ(log.read_from(3, SIZE_MAX, &v), ReplicationLog::ReadStatus::TooOld);
    log.on_del("x");
    EXPECT_EQ(log.last_offset(), 101u);
    EXPECT_EQ(ReplicationLog::new_replid().size(), 40u);
    EXPECT_NE(ReplicationLog::new_replid(), ReplicationLog::new_replid());
}

// ---- waiting ----------------------------------------------------------------------------------------

TEST(ReplLog, WaitBeyondWakesOnAppendTimesOutAndWakesOnClose) {
    ReplicationLog log(fixed_wall());
    EXPECT_FALSE(log.wait_beyond(0, 30));  // nothing yet: times out

    std::thread t([&] { std::this_thread::sleep_for(std::chrono::milliseconds(50)); log.on_del("k"); });
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_TRUE(log.wait_beyond(0, 5000));
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::milliseconds(2500));
    t.join();
    EXPECT_TRUE(log.wait_beyond(0, 0));  // already beyond: returns at once

    std::atomic<bool> woke{false};
    std::thread w([&] { log.wait_beyond(1, 10000); woke = true; });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(woke);
    log.close();
    w.join();
    EXPECT_TRUE(woke);
}

// ---- I2: the observer chain and visibility ---------------------------------------------------------------

TEST(ObserverChain, DeliversToEveryMemberInOrder) {
    ReplicationLog a(fixed_wall()), b(fixed_wall());
    ObserverChain chain;
    chain.add(&a);
    chain.add(nullptr);  // ignored
    chain.add(&b);
    Store engine;
    engine.set_observer(&chain);
    engine.set("x", "1");
    engine.set("x", "2", 1000);
    engine.expire("x", 5000);
    engine.persist("x");
    engine.del("x");
    ASSERT_EQ(a.last_offset(), 5u);
    ASSERT_EQ(b.last_offset(), 5u);
    auto ra = all_records(a), rb = all_records(b);
    for (size_t i = 0; i < ra.size(); ++i) EXPECT_EQ(ra[i].payload, rb[i].payload);
}

// If a client can read a value from the engine, the record that produced it is ALREADY in the
// log. (The observer runs inside the engine's critical section, before the lock is released.)
TEST(ObserverChain, AMutationIsNeverVisibleBeforeItsRecordExists) {
    ReplicationLog log(ReplicationLog::Options{1u << 30, {}});
    ObserverChain chain;
    chain.add(&log);
    ShardedStore engine(8);
    engine.set_observer(&chain);
    constexpr int kWriters = 4;
    std::atomic<bool> done{false};
    std::atomic<int> violations{0}, checks{0};
    std::vector<std::thread> ws;
    for (int w = 0; w < kWriters; ++w)
        ws.emplace_back([&, w] {  // keep writing until the reader has made enough observations
            for (int i = 1; !done && i <= 30000; ++i) {
                engine.set("w" + std::to_string(w), std::to_string(i));
                if (i % 8 == 0) std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        });
    std::thread reader([&] {
        std::mt19937 rng(7);
        while (!done) {
            const int w = static_cast<int>(rng() % kWriters);
            auto v = engine.get("w" + std::to_string(w));
            if (!v) continue;
            bool found = false;  // is there a SET w<w> <v> record already? (look at the recent tail)
            std::vector<ReplRecord> tail;
            const uint64_t last = log.last_offset();
            log.read_from(last > 20000 ? last - 20000 : log.first_offset(), SIZE_MAX, &tail);
            for (auto& r : tail) {
                auto a = args_of(r.payload);
                if (a.size() == 3 && a[0] == "SET" && a[1] == "w" + std::to_string(w) && a[2] == *v) { found = true; break; }
            }
            if (!found) ++violations;
            ++checks;
            if (checks >= 150) done = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    reader.join();
    for (auto& t : ws) t.join();
    EXPECT_GT(checks.load(), 20);
    EXPECT_EQ(violations.load(), 0);
}

// ---- the core property: replaying the log reproduces the engine --------------------------------------------

// Concurrent writers hit a small key space on a capacity-limited sharded engine (so evictions
// happen), with TTLs, EXPIRE and PERSIST mixed in. Afterwards, applying the log's records in
// offset order to an empty engine must give exactly the primary's contents. This is what makes
// "replica = primary" true: per-key order is the engine's order, evictions arrive as DELs.
TEST(ReplicationProperty, ReplayingTheLogReproducesThePrimary) {
    ReplicationLog log(ReplicationLog::Options{1u << 30, {}});
    ObserverChain chain;
    chain.add(&log);
    ShardedStore primary(8, Store::real_now_ms, /*max_keys=*/150);
    primary.set_observer(&chain);

    constexpr int kThreads = 4, kOps = 15000, kKeys = 400;
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t) {
        ts.emplace_back([&, t] {
            std::mt19937 rng(100 + t);
            for (int i = 0; i < kOps; ++i) {
                const std::string k = "k" + std::to_string(rng() % kKeys);
                switch (rng() % 8) {
                    case 0: case 1: case 2: primary.set(k, "v" + std::to_string(rng() % 1000)); break;
                    case 3: primary.set(k, "t" + std::to_string(rng() % 1000), 60'000); break;
                    case 4: primary.del(k); break;
                    case 5: primary.expire(k, 90'000); break;
                    case 6: primary.persist(k); break;
                    default: primary.get(k); break;  // reads are not logged but reorder the LRU
                }
            }
        });
    }
    for (auto& th : ts) th.join();
    ASSERT_GT(primary.evicted_keys(), 0u);  // the scenario really exercised eviction

    Store replica;  // no capacity limit: evictions must come from the log, not from the replica
    uint64_t expect = 1;
    for (auto& r : all_records(log)) {
        ASSERT_EQ(r.offset, expect++);
        ASSERT_EQ(apply_logical_record(replica, args_of(r.payload), real_wall()), ApplyResult::Applied);
    }
    EXPECT_EQ(expect - 1, log.last_offset());

    const auto a = dump(primary), b = dump(replica);
    ASSERT_EQ(a.size(), b.size());
    for (auto& kv : a) {
        auto it = b.find(kv.first);
        ASSERT_NE(it, b.end()) << kv.first << " missing on the replica";
        EXPECT_EQ(kv.second.value, it->second.value) << kv.first;
        EXPECT_EQ(kv.second.has_ttl, it->second.has_ttl) << kv.first;
        if (kv.second.has_ttl) {
            EXPECT_LT(std::llabs(kv.second.ttl - it->second.ttl), 3000) << kv.first;
        }
    }
    EXPECT_TRUE(replica.consistent());
}

// Idempotence: applying the whole stream a second time leaves the same state.
TEST(ReplicationProperty, ApplyingTheStreamTwiceIsHarmless) {
    ReplicationLog log(ReplicationLog::Options{1u << 30, {}});
    Store primary;
    primary.set_observer(&log);
    std::mt19937 rng(5);
    for (int i = 0; i < 4000; ++i) {
        const std::string k = "k" + std::to_string(rng() % 50);
        switch (rng() % 5) {
            case 0: case 1: primary.set(k, std::to_string(i)); break;
            case 2: primary.set(k, std::to_string(i), 60'000); break;
            case 3: primary.del(k); break;
            default: primary.expire(k, 80'000); break;
        }
    }
    Store once, twice;
    for (auto& r : all_records(log)) apply_logical_record(once, args_of(r.payload), real_wall());
    for (int pass = 0; pass < 2; ++pass)
        for (auto& r : all_records(log)) apply_logical_record(twice, args_of(r.payload), real_wall());
    const auto a = dump(once), b = dump(twice), p = dump(primary);
    EXPECT_EQ(a.size(), b.size());
    EXPECT_EQ(a.size(), p.size());
    for (auto& kv : a) {
        ASSERT_TRUE(b.count(kv.first));
        EXPECT_TRUE(kv.second == b.at(kv.first)) << kv.first;
        EXPECT_TRUE(kv.second == p.at(kv.first)) << kv.first;
    }
}

// ---- apply_logical_record ------------------------------------------------------------------------------

TEST(ApplyLogicalRecord, TtlIsRelativeToTheApplyingClockAndExpiredDeadlinesDelete) {
    Store s;
    const int64_t now = 5'000'000;
    EXPECT_EQ(apply_logical_record(s, {"SET", "a", "1", "PXAT", std::to_string(now + 10'000)}, now), ApplyResult::Applied);
    EXPECT_NEAR(static_cast<double>(s.ttl_ms("a")), 10'000.0, 50.0);
    EXPECT_EQ(apply_logical_record(s, {"SET", "a", "2", "PXAT", std::to_string(now - 1)}, now), ApplyResult::ExpiredSkipped);
    EXPECT_EQ(s.get("a"), std::nullopt);  // the older value must not survive an already-dead newer one
    EXPECT_EQ(apply_logical_record(s, {"SET", "b", "x"}, now), ApplyResult::Applied);
    EXPECT_EQ(apply_logical_record(s, {"PEXPIREAT", "b", std::to_string(now + 7000)}, now), ApplyResult::Applied);
    EXPECT_NEAR(static_cast<double>(s.ttl_ms("b")), 7000.0, 50.0);
    EXPECT_EQ(apply_logical_record(s, {"PERSIST", "b"}, now), ApplyResult::Applied);
    EXPECT_EQ(s.ttl_ms("b"), -1);
    EXPECT_EQ(apply_logical_record(s, {"DEL", "b"}, now), ApplyResult::Applied);
    EXPECT_EQ(s.get("b"), std::nullopt);
}

TEST(ApplyLogicalRecord, RejectsMalformedRecords) {
    Store s;
    EXPECT_EQ(apply_logical_record(s, {}, 0), ApplyResult::Invalid);
    EXPECT_EQ(apply_logical_record(s, {"FLUSHALL"}, 0), ApplyResult::Invalid);
    EXPECT_EQ(apply_logical_record(s, {"SET", "a"}, 0), ApplyResult::Invalid);
    EXPECT_EQ(apply_logical_record(s, {"SET", "a", "1", "PXAT", "notanumber"}, 0), ApplyResult::Invalid);
    EXPECT_EQ(apply_logical_record(s, {"DEL"}, 0), ApplyResult::Invalid);
    EXPECT_EQ(s.size(), 0u);
}
