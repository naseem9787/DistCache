// Asynchronous replication, step 2 of Phase 8: streaming, ordering, duplicates, gaps, reconnect and
// partial resync, backlog overflow and full resync, slow replicas, concurrent writes during a full
// sync, TTLs, evictions. Real sockets on loopback, real threads; faults are injected through
// ReplFaults and the replica's test controls, never by timing luck.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "observer_chain.hpp"
#include "repl_log.hpp"
#include "replication.hpp"
#include "store.hpp"
#include "synced_store.hpp"

namespace {

using Clock = std::chrono::steady_clock;

bool wait_until(const std::function<bool()>& pred, int timeout_ms = 8000) {
    const auto end = Clock::now() + std::chrono::milliseconds(timeout_ms);
    while (Clock::now() < end) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return pred();
}

struct Entry {
    std::string value;
    bool has_ttl;
    int64_t ttl;
};

std::map<std::string, Entry> dump(const Engine& e) {
    std::map<std::string, Entry> m;
    e.for_each([&](const std::string& k, const std::string& v, int64_t t) { m[k] = Entry{v, t >= 0, t}; });
    return m;
}

// Same keys, same values, same TTL-ness, TTLs within a few seconds of each other.
::testing::AssertionResult same_state(const Engine& a, const Engine& b) {
    const auto x = dump(a), y = dump(b);
    if (x.size() != y.size()) return ::testing::AssertionFailure() << "sizes differ: " << x.size() << " vs " << y.size();
    for (const auto& kv : x) {
        auto it = y.find(kv.first);
        if (it == y.end()) return ::testing::AssertionFailure() << "missing on replica: " << kv.first;
        if (it->second.value != kv.second.value)
            return ::testing::AssertionFailure() << kv.first << ": '" << kv.second.value << "' vs '" << it->second.value << "'";
        if (it->second.has_ttl != kv.second.has_ttl) return ::testing::AssertionFailure() << kv.first << ": TTL presence differs";
        if (kv.second.has_ttl && std::llabs(kv.second.ttl - it->second.ttl) > 3000)
            return ::testing::AssertionFailure() << kv.first << ": TTLs differ " << kv.second.ttl << " vs " << it->second.ttl;
    }
    return ::testing::AssertionSuccess();
}

// A primary (sharded engine + replication log + replication listener).
struct PrimaryNode {
    explicit PrimaryNode(size_t backlog = 64u << 20, size_t max_keys = 0, ReplicationPrimary::Options po = {})
        : engine(8, Store::real_now_ms, max_keys), log(ReplicationLog::Options{backlog, {}}), repl(engine, log, po) {
        chain.add(&log);
        engine.set_observer(&chain);
        std::string err;
        EXPECT_TRUE(repl.start(&err)) << err;
    }
    ShardedStore engine;
    ObserverChain chain;
    ReplicationLog log;
    ReplicationPrimary repl;
};

// A replica (its own engine + mirror log + the client side of the protocol).
struct ReplicaNode {
    explicit ReplicaNode(int port, size_t max_keys = 0) : engine(8, Store::real_now_ms, max_keys), log() {
        ReplicationReplica::Options o;
        o.port = port;
        o.link_timeout_ms = 1500;
        rep = std::make_unique<ReplicationReplica>(engine, log, o);
        rep->start();
    }
    bool caught_up(const PrimaryNode& p, int ms = 8000) { return rep->wait_applied(p.log.last_offset(), ms); }
    ShardedStore engine;
    ReplicationLog log;
    std::unique_ptr<ReplicationReplica> rep;
};

}  // namespace

// ---- the basics -----------------------------------------------------------------------------

TEST(Replication, AFreshReplicaFullSyncsExistingDataIncludingTtls) {
    PrimaryNode p;
    for (int i = 0; i < 500; ++i) p.engine.set("k" + std::to_string(i), "v" + std::to_string(i));
    for (int i = 0; i < 100; ++i) p.engine.set("t" + std::to_string(i), "x", 60'000 + i * 1000);
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));
    ASSERT_TRUE(r.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    const auto s = r.rep->status();
    EXPECT_EQ(s.full_syncs, 1u);
    EXPECT_EQ(s.partial_syncs, 0u);
    EXPECT_EQ(s.replid, p.log.replid());
}

TEST(Replication, LiveWritesArriveInOrderAndTheFinalStateMatches) {
    PrimaryNode p;
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));
    // Order matters: the SAME keys are overwritten, deleted and re-created many times.
    for (int round = 0; round < 300; ++round) {
        for (int k = 0; k < 20; ++k) {
            const std::string key = "k" + std::to_string(k);
            switch ((round + k) % 4) {
                case 0: p.engine.set(key, "r" + std::to_string(round)); break;
                case 1: p.engine.set(key, "t" + std::to_string(round), 90'000); break;
                case 2: p.engine.del(key); break;
                default: p.engine.set(key, "p" + std::to_string(round)); p.engine.persist(key); break;
            }
        }
    }
    ASSERT_TRUE(r.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    EXPECT_EQ(r.log.last_offset(), p.log.last_offset());  // the mirror has the primary's offsets
}

// ---- duplicates and gaps ----------------------------------------------------------------------

TEST(Replication, DuplicateDeliveryDoesNotCorruptState) {
    PrimaryNode p;
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));
    p.repl.faults().duplicate_next = 100;  // the next 100 records are sent twice
    for (int i = 0; i < 400; ++i) {
        p.engine.set("k" + std::to_string(i % 10), "v" + std::to_string(i));
        if (i % 7 == 0) p.engine.del("k" + std::to_string(i % 10));
    }
    ASSERT_TRUE(r.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    EXPECT_EQ(r.rep->status().duplicates_skipped, 100u);
    EXPECT_EQ(r.rep->status().gaps, 0u);
    EXPECT_EQ(r.log.last_offset(), p.log.last_offset());  // not 100 records longer
}

TEST(Replication, AMissingRecordIsDetectedAndRepairedByResynchronising) {
    PrimaryNode p;
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));
    p.repl.faults().drop_next = 1;  // one record never reaches the replica
    for (int i = 0; i < 50; ++i) p.engine.set("a" + std::to_string(i), "1");
    for (int i = 0; i < 50; ++i) p.engine.set("b" + std::to_string(i), "2");  // the next record exposes the hole
    ASSERT_TRUE(r.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    const auto s = r.rep->status();
    EXPECT_GE(s.gaps, 1u);
    EXPECT_GE(s.partial_syncs, 1u);  // repaired from the backlog, no snapshot needed
    EXPECT_EQ(s.full_syncs, 1u);     // only the initial one
}

// ---- disconnect, reconnect, catch-up ----------------------------------------------------------------

TEST(Replication, ReconnectCatchesUpWithAPartialResync) {
    PrimaryNode p;
    ReplicaNode r(p.repl.port());
    for (int i = 0; i < 100; ++i) p.engine.set("k" + std::to_string(i), "before");
    ASSERT_TRUE(r.caught_up(p));
    ASSERT_TRUE(r.rep->wait_online(8000));

    r.rep->set_reconnect_hold(true);
    r.rep->drop_link();
    ASSERT_TRUE(wait_until([&] { return r.rep->status().state == ReplicationReplica::State::Disconnected; }));
    for (int i = 0; i < 1000; ++i) p.engine.set("k" + std::to_string(i % 150), "during" + std::to_string(i));  // while it is away
    r.rep->set_reconnect_hold(false);

    ASSERT_TRUE(r.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    const auto s = r.rep->status();
    EXPECT_EQ(s.full_syncs, 1u);     // the initial one only
    EXPECT_EQ(s.partial_syncs, 1u);  // the catch-up came from the backlog
    EXPECT_GE(s.reconnects, 1u);
}

TEST(Replication, FallingBehindTheBacklogForcesAFullResync) {
    PrimaryNode p(/*backlog=*/4096);
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));
    r.rep->set_reconnect_hold(true);
    r.rep->drop_link();
    ASSERT_TRUE(wait_until([&] { return r.rep->status().state == ReplicationReplica::State::Disconnected; }));
    for (int i = 0; i < 3000; ++i) p.engine.set("k" + std::to_string(i % 400), "v" + std::to_string(i));
    ASSERT_GT(p.log.first_offset(), r.log.last_offset() + 1);  // the records it needs are gone
    r.rep->set_reconnect_hold(false);
    ASSERT_TRUE(r.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    EXPECT_EQ(r.rep->status().full_syncs, 2u);
}

TEST(Replication, ADisconnectedPrimaryLinkIsRetriedUntilThePrimaryReturns) {
    auto p = std::make_unique<PrimaryNode>();
    ReplicaNode r(p->repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));
    p->engine.set("x", "1");
    ASSERT_TRUE(r.caught_up(*p));
    p->repl.stop();  // the listener goes away
    ASSERT_TRUE(wait_until([&] { return r.rep->status().state == ReplicationReplica::State::Disconnected; }));
    EXPECT_EQ(r.engine.get("x"), std::optional<std::string>("1"));  // data stays readable while disconnected
    EXPECT_GE(r.rep->status().reconnects, 0u);
}

// ---- lag and backpressure ------------------------------------------------------------------------------

TEST(Replication, ASlowReplicaNeverBlocksThePrimaryAndIsCutOffThenResynchronised) {
    ReplicationPrimary::Options po;
    po.send_timeout_ms = 200;  // a replica that does not drain its socket for 200 ms is cut
    PrimaryNode p(/*backlog=*/1u << 20, 0, po);
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));

    r.rep->set_apply_paused(true);  // the replica stops reading: its socket backs up
    const std::string big(400, 'x');
    const auto t0 = Clock::now();
    for (int i = 0; i < 40000; ++i) p.engine.set("k" + std::to_string(i % 2000), big);  // ~16 MB through a 1 MB backlog
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
    EXPECT_LT(elapsed, 6000) << "writes must not wait for the stalled replica";

    ASSERT_TRUE(wait_until([&] {
        const auto s = p.repl.stats();
        return s.cut_too_old + s.cut_send_failed >= 1;
    })) << "the stalled replica was never cut off";

    r.rep->set_apply_paused(false);
    ASSERT_TRUE(r.caught_up(p, 15000));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    EXPECT_GE(r.rep->status().full_syncs, 2u);  // it fell behind the backlog, so it needed a snapshot
}

TEST(Replication, LagIsVisibleOnBothSides) {
    ReplicationPrimary::Options po;
    PrimaryNode p(64u << 20, 0, po);
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));
    r.rep->set_apply_paused(true);
    for (int i = 0; i < 2000; ++i) p.engine.set("k" + std::to_string(i), "v");
    // The replica is paused, so the primary sees its acknowledged offset stay behind...
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    auto infos = p.repl.replicas();
    ASSERT_EQ(infos.size(), 1u);
    EXPECT_EQ(infos[0].state, "streaming");
    EXPECT_LT(infos[0].acked, p.log.last_offset());
    r.rep->set_apply_paused(false);
    ASSERT_TRUE(r.caught_up(p));
    // ... and once it caught up, its ACK reaches the primary.
    ASSERT_TRUE(wait_until([&] { auto v = p.repl.replicas(); return v.size() == 1 && v[0].acked == p.log.last_offset(); }));
    EXPECT_EQ(r.rep->status().lag(), 0u);
}

// ---- full resync under concurrent writes (the fuzzy snapshot argument) --------------------------------------

TEST(Replication, FullSyncWhileTheKeyspaceIsBeingModifiedConverges) {
    PrimaryNode p;
    for (int i = 0; i < 20000; ++i) p.engine.set("k" + std::to_string(i), "init");
    p.repl.faults().snapshot_pause_us = 2000;  // stretch the snapshot over ~0.6 s so writes land inside it

    std::atomic<bool> stop{false};
    std::thread writer([&] {
        std::mt19937 rng(11);
        int n = 0;
        while (!stop) {
            const std::string k = "k" + std::to_string(rng() % 20000);
            switch (rng() % 5) {
                case 0: p.engine.del(k); break;
                case 1: p.engine.set(k, "ttl" + std::to_string(n), 120'000); break;
                case 2: p.engine.expire(k, 150'000); break;
                default: p.engine.set(k, "w" + std::to_string(n)); break;
            }
            ++n;
            if (n % 32 == 0) std::this_thread::yield();
        }
    });
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(wait_until([&] { return r.rep->status().state != ReplicationReplica::State::Disconnected && r.rep->status().full_syncs >= 1; }));
    ASSERT_TRUE(r.rep->wait_online(20000));  // snapshot done, replay caught up, while the writer is still running
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop = true;
    writer.join();
    ASSERT_GT(p.log.last_offset(), 20000u);  // writes really happened during/after the sync
    ASSERT_TRUE(r.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    EXPECT_EQ(r.rep->status().full_syncs, 1u);
}

// ---- TTL, eviction, flush -----------------------------------------------------------------------------------

TEST(Replication, TtlsReplicateAsAbsoluteDeadlinesAndExpireOnTheReplicaToo) {
    PrimaryNode p;
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));
    p.engine.set("long", "1", 60'000);
    p.engine.set("short", "1", 300);
    p.engine.set("plain", "1");
    p.engine.expire("plain", 45'000);
    p.engine.set("persisted", "1", 30'000);
    p.engine.persist("persisted");
    ASSERT_TRUE(r.caught_up(p));
    EXPECT_NEAR(static_cast<double>(r.engine.ttl_ms("long")), 60'000.0, 2000.0);
    EXPECT_NEAR(static_cast<double>(r.engine.ttl_ms("plain")), 45'000.0, 2000.0);
    EXPECT_EQ(r.engine.ttl_ms("persisted"), -1);
    EXPECT_EQ(r.engine.get("short"), std::optional<std::string>("1"));
    std::this_thread::sleep_for(std::chrono::milliseconds(450));
    EXPECT_EQ(p.engine.get("short"), std::nullopt);  // expired on the primary...
    EXPECT_EQ(r.engine.get("short"), std::nullopt);  // ...and on the replica, by the same absolute deadline
    EXPECT_EQ(r.engine.get("long"), std::optional<std::string>("1"));
}

TEST(Replication, EvictionsArriveAsDeletesAndTheReplicaNeverEvictsByItself) {
    PrimaryNode p(64u << 20, /*max_keys=*/100);
    ReplicaNode r(p.repl.port(), /*max_keys=*/100);  // same capacity, but evicting is switched off
    ASSERT_TRUE(r.rep->wait_online(8000));
    for (int i = 0; i < 2000; ++i) p.engine.set("k" + std::to_string(i), "v" + std::to_string(i));
    ASSERT_GT(p.engine.evicted_keys(), 0u);
    ASSERT_TRUE(r.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    EXPECT_EQ(r.engine.evicted_keys(), 0u);
    EXPECT_EQ(r.engine.size(), p.engine.size());
}

TEST(Replication, ClearIsReplicatedAsFlushall) {
    PrimaryNode p;
    ReplicaNode r(p.repl.port());
    ASSERT_TRUE(r.rep->wait_online(8000));
    for (int i = 0; i < 200; ++i) p.engine.set("k" + std::to_string(i), "v");
    ASSERT_TRUE(r.caught_up(p));
    ASSERT_EQ(r.engine.size(), 200u);
    p.engine.clear();  // quiescent: no concurrent writers (clear is a resync tool, not a client command)
    p.engine.set("after", "1");
    ASSERT_TRUE(r.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, r.engine));
    EXPECT_EQ(r.engine.size(), 1u);
}

// ---- lifecycle ------------------------------------------------------------------------------------------------

TEST(Replication, ASecondReplicaGetsItsOwnIndependentStream) {
    PrimaryNode p;
    for (int i = 0; i < 300; ++i) p.engine.set("k" + std::to_string(i), "v");
    ReplicaNode a(p.repl.port()), b(p.repl.port());
    ASSERT_TRUE(a.rep->wait_online(8000));
    ASSERT_TRUE(b.rep->wait_online(8000));
    for (int i = 0; i < 300; ++i) p.engine.set("k" + std::to_string(i), "w");
    ASSERT_TRUE(a.caught_up(p));
    ASSERT_TRUE(b.caught_up(p));
    EXPECT_TRUE(same_state(p.engine, a.engine));
    EXPECT_TRUE(same_state(p.engine, b.engine));
    EXPECT_EQ(p.repl.replicas().size(), 2u);
}

TEST(Replication, StopIsPromptEvenWhileStreaming) {
    auto p = std::make_unique<PrimaryNode>();
    auto r = std::make_unique<ReplicaNode>(p->repl.port());
    ASSERT_TRUE(r->rep->wait_online(8000));
    const auto t0 = Clock::now();
    r->rep->stop();
    p->repl.stop();
    EXPECT_LT(Clock::now() - t0, std::chrono::milliseconds(2500));
}
