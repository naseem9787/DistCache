// The rate limiter in a cluster (Phase 9): replication, replica lag, failover, clock skew between
// machines, partitions, WAL recovery, routing and live migration. Nodes are in-process but speak
// real TCP; every machine "sees" the same fake wall clock unless a test deliberately moves it.
//
// Rates are tiny (0.001-0.5 tokens/s) wherever a test runs for seconds of REAL time: a bucket's
// key expires when the bucket would be full, and the engines here count TTLs in real time.
#include <gtest/gtest.h>

#include <stdlib.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "node_replication.hpp"
#include "observer_chain.hpp"
#include "persistence.hpp"
#include "rate_limiter.hpp"
#include "router.hpp"
#include "server.hpp"
#include "synced_store.hpp"
#include "test_client.hpp"

namespace {

using Clock = std::chrono::steady_clock;

bool wait_until(const std::function<bool()>& pred, int timeout_ms = 20000) {
    const auto end = Clock::now() + std::chrono::milliseconds(timeout_ms);
    while (Clock::now() < end) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

// The wall clock every node's limiter reads. Frozen unless a test moves it.
struct Wall {
    std::atomic<int64_t> t{1'700'000'000'000LL};
    Wall() { ratelimit::set_clock([this] { return t.load(); }); }
    ~Wall() { ratelimit::set_clock(nullptr); }
};

struct Node {
    explicit Node(NodeReplicationOptions o) : engine(8), svc(engine, std::move(o)), hooks(nullptr, &svc),
                                              server(engine, ServerOptions{0, 2, &hooks}) {
        chain.add(svc.observer());
        engine.set_observer(&chain);
        std::string err;
        EXPECT_TRUE(svc.start(&err)) << err;
        EXPECT_TRUE(server.start(&err)) << err;
    }
    ~Node() {
        server.stop();
        svc.stop();
    }
    int port() const { return server.port(); }
    ShardedStore engine;
    ReplicationService svc;
    ObserverChain chain;
    NodeHooks hooks;
    Server server;
};

NodeReplicationOptions primary_opts(int sync_n = 0, bool lease = false) {
    NodeReplicationOptions o;
    o.repl_port = 0;
    o.require_lease = lease;
    o.sync_replicas = sync_n;
    o.sync_timeout_ms = sync_n ? 1500 : 5000;
    o.log_events = false;
    return o;
}
NodeReplicationOptions replica_of(Node& p, int sync_n = 0, bool lease = false) {
    NodeReplicationOptions o = primary_opts(sync_n, lease);
    o.replicaof_host = "127.0.0.1";
    o.replicaof_port = p.svc.repl_port();
    return o;
}
CoordinatorOptions fast_failover(int sync_n) {
    CoordinatorOptions c;
    c.lease_ms = 500;
    c.probe_ms = 100;
    c.probe_timeout_ms = 200;
    c.fail_after = 3;
    c.guard_ms = 100;
    c.eps = 0.05;
    c.sync_replicas = sync_n;
    c.log_events = false;
    return c;
}

struct ShardNodes {
    ShardNodes(int replicas, int sync_n) {
        primary = std::make_unique<Node>(primary_opts(sync_n, true));
        for (int i = 0; i < replicas; ++i) repl.push_back(std::make_unique<Node>(replica_of(*primary, sync_n, true)));
    }
    RouterNode spec(const std::string& name) const {
        RouterNode n;
        n.name = name;
        n.host = "127.0.0.1";
        n.port = primary->port();
        for (auto& r : repl) n.replicas.push_back({"127.0.0.1", r->port()});
        return n;
    }
    std::unique_ptr<Node> primary;
    std::vector<std::unique_ptr<Node>> repl;
};

std::unique_ptr<Router> make_router(std::vector<RouterNode> nodes, int sync_n) {
    RouterOptions ro;
    ro.port = 0;
    ro.io_timeout_ms = 2500;
    ro.failover = fast_failover(sync_n);
    auto r = std::make_unique<Router>(std::move(nodes), ro);
    std::string err;
    EXPECT_TRUE(r->start(&err)) << err;
    return r;
}
Coordinator::ShardInfo shard_info(Router& r, const std::string& name) {
    for (auto& s : r.coordinator()->info()) if (s.shard == name) return s;
    return {};
}
bool healthy(Router& r, const std::string& name) { return shard_info(r, name).state == "healthy"; }
bool caught_up(Node& replica, Node& primary) {
    return replica.svc.replica() && replica.svc.replica()->status().applied >= primary.svc.log().last_offset();
}

// RL.ACQUIRE / RL.PEEK -> {allowed, remaining, retry_after_ms, reset_after_ms}; {} plus *err on an error reply.
std::vector<long long> acquire(Client& c, const std::string& key, const char* cap, const char* rate, std::string* err = nullptr) {
    return c.cmd_ints({"RL.ACQUIRE", key, cap, rate}, err);
}
std::vector<long long> peek(Client& c, const std::string& key, const char* cap, const char* rate, std::string* err = nullptr) {
    return c.cmd_ints({"RL.PEEK", key, cap, rate}, err);
}
bool allowed(const std::vector<long long>& r) { return r.size() == 4 && r[0] == 1; }

// Like acquire(), but an ERROR reply is retried for up to 10 s and only a definite answer (allowed or
// denied) is returned. Right after a failover a sync-N shard can answer -NOREPLICAS until the other
// replica has re-attached to the new primary: refusing is correct, so callers simply try again.
std::vector<long long> acquire_retry(Client& c, const std::string& key, const char* cap, const char* rate) {
    const auto end = Clock::now() + std::chrono::seconds(10);
    for (;;) {
        std::string err;
        auto r = acquire(c, key, cap, rate, &err);
        if (!r.empty() || Clock::now() > end) return r;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

// Takes tokens through `c` until the limiter definitely says no. Returns how many it granted.
int drain(Client& c, const std::string& key, const char* cap, const char* rate, int safety = 1000) {
    int n = 0;
    while (n < safety && allowed(acquire_retry(c, key, cap, rate))) ++n;
    return n;
}

}  // namespace

// ---------------------------------------------------------------------------------------------
// Replication of bucket state
// ---------------------------------------------------------------------------------------------

TEST(RateLimiterCluster, TheBucketReplicatesAndReplicasAnswerPeeksButRefuseToConsume) {
    Wall wall;
    Node p(primary_opts());
    Node r(replica_of(p));
    Client cp(p.port()), cr(r.port());
    for (int i = 0; i < 3; ++i) ASSERT_TRUE(allowed(acquire(cp, "k", "5", "1")));
    ASSERT_TRUE(wait_until([&] { return caught_up(r, p); }));
    auto v = peek(cr, "k", "5", "1");
    ASSERT_EQ(v.size(), 4u);
    EXPECT_EQ(v[0], 1);
    EXPECT_EQ(v[1], 2);                                       // exactly the primary's remaining
    EXPECT_EQ(*r.engine.get("k"), *p.engine.get("k"));        // byte-identical state
    std::string err;
    EXPECT_TRUE(acquire(cr, "k", "5", "1", &err).empty());
    EXPECT_EQ(err.rfind("-READONLY", 0), 0u) << err;          // a replica never hands out tokens
    EXPECT_EQ(*r.engine.get("k"), *p.engine.get("k"));
    EXPECT_GT(r.engine.ttl_ms("k"), 0);                       // the idle-expiry TTL travelled with it
}

TEST(RateLimiterCluster, DuplicatedDeliveryOfBucketUpdatesIsHarmless) {
    Wall wall;
    Node p(primary_opts());
    Node r(replica_of(p));
    Client cp(p.port());
    ASSERT_TRUE(wait_until([&] { return p.svc.primary() && p.svc.primary()->in_sync_replicas(1500) == 1; }));
    p.svc.primary()->faults().duplicate_next = 6;
    for (int i = 0; i < 6; ++i) ASSERT_TRUE(allowed(acquire(cp, "dup", "10", "1")));
    ASSERT_TRUE(wait_until([&] { return caught_up(r, p); }));
    EXPECT_EQ(*r.engine.get("dup"), *p.engine.get("dup"));    // a duplicate must not consume a token twice
    EXPECT_GE(r.svc.replica()->status().duplicates_skipped, 1u);
}

TEST(RateLimiterCluster, ReplicaLagMakesAPeekStaleButNeverTheAuthoritativeAnswer) {
    Wall wall;
    Node p(primary_opts());
    Node r(replica_of(p));
    Client cp(p.port()), cr(r.port());
    ASSERT_TRUE(allowed(acquire(cp, "lag", "3", "0.001")));
    ASSERT_TRUE(wait_until([&] { return caught_up(r, p); }));
    r.svc.replica()->set_apply_paused(true);                  // the replica stops applying: lag
    ASSERT_TRUE(allowed(acquire(cp, "lag", "3", "0.001")));
    ASSERT_TRUE(allowed(acquire(cp, "lag", "3", "0.001")));
    EXPECT_FALSE(allowed(acquire(cp, "lag", "3", "0.001")));  // the primary: exhausted, denies
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto stale = peek(cr, "lag", "3", "0.001");
    ASSERT_EQ(stale.size(), 4u);
    EXPECT_EQ(stale[0], 1) << "a lagging replica still shows tokens: PEEK on a replica is advisory";
    EXPECT_EQ(stale[1], 2);
    r.svc.replica()->set_apply_paused(false);                 // catch up: the replica converges
    ASSERT_TRUE(wait_until([&] { return caught_up(r, p); }));
    EXPECT_FALSE(allowed(peek(cr, "lag", "3", "0.001")));
    EXPECT_EQ(*r.engine.get("lag"), *p.engine.get("lag"));
}

TEST(RateLimiterCluster, AnAllowedAnswerIsNotAnnouncedUntilTheReplicaHasItUnderSyncReplication) {
    Wall wall;
    NodeReplicationOptions po = primary_opts(1);
    po.sync_timeout_ms = 600;
    Node p(po);
    Node r(replica_of(p, 1));
    ASSERT_TRUE(wait_until([&] { return p.svc.primary() && p.svc.primary()->in_sync_replicas(1500) == 1; }));
    {
        Client c(p.port());
        EXPECT_TRUE(allowed(acquire(c, "s", "5", "0.001")));   // replica healthy: acknowledged normally
    }
    r.svc.replica()->set_apply_paused(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    Client slow(p.port());
    std::string err;
    EXPECT_TRUE(acquire(slow, "s", "5", "0.001", &err).empty());
    EXPECT_EQ(err, "<closed>") << "the allowed decision must be held back, then the connection closed on timeout";
    // The token WAS consumed on the primary even though the caller was never told: a timeout burns a
    // token (over-restrictive, the safe direction). Documented in the README.
    r.svc.replica()->set_apply_paused(false);
    Client c2(p.port());
    auto v = peek(c2, "s", "5", "0.001");
    ASSERT_EQ(v.size(), 4u);
    EXPECT_EQ(v[1], 3);
}

TEST(RateLimiterCluster, ABucketSurvivesWalRecoveryAndKeepsRefillingAcrossTheDowntime) {
    namespace fs = std::filesystem;
    char tmpl[] = "/tmp/distcache-rltest-XXXXXX";
    const std::string dir = mkdtemp(tmpl);
    int64_t steady = 1000, wall_ms = 1'700'000'000'000LL;
    ratelimit::set_clock([&] { return wall_ms; });
    auto open = [&](std::unique_ptr<ShardedStore>& e, std::unique_ptr<Persistence>& p) {
        e = std::make_unique<ShardedStore>(4, [&] { return steady; });
        PersistenceOptions o;
        o.dir = dir;
        o.fsync = FsyncPolicy::No;
        o.wall_ms = [&] { return wall_ms; };
        p = std::make_unique<Persistence>(*e, o);
        RecoveryStats rs;
        std::string err;
        ASSERT_TRUE(p->open(&rs, &err)) << err;
    };
    {
        std::unique_ptr<ShardedStore> e;
        std::unique_ptr<Persistence> p;
        open(e, p);
        for (int i = 0; i < 4; ++i) ASSERT_NE(execute(*e, {"RL.ACQUIRE", "wal", "10", "1"}, nullptr).find(":1"), std::string::npos);
        p->close();
    }
    steady += 2000;       // the server was down for two seconds
    wall_ms += 2000;
    std::unique_ptr<ShardedStore> e;
    std::unique_ptr<Persistence> p;
    open(e, p);
    EXPECT_EQ(execute(*e, {"RL.PEEK", "wal", "10", "1"}, nullptr), "*4\r\n:1\r\n:8\r\n:-1\r\n:2000\r\n")
        << "6 tokens left at the crash + 2 s of refill at 1 token/s";
    p->close();
    ratelimit::set_clock(nullptr);
    std::error_code ec;
    fs::remove_all(dir, ec);
}

// ---------------------------------------------------------------------------------------------
// Failover
// ---------------------------------------------------------------------------------------------

TEST(RateLimiterFailover, WithSyncReplicationNoConsumedTokenIsEverForgottenByAFailover) {
    Wall wall;
    ShardNodes s(2, 1);
    auto router = make_router({s.spec("a")}, 1);
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    ASSERT_TRUE(wait_until([&] { return s.primary->svc.primary()->in_sync_replicas(1500) == 2; }));
    Client c(router->port());
    int before = 0;
    for (; before < 30; ++before) ASSERT_TRUE(allowed(acquire(c, "api:u1", "50", "0.001"))) << before;
    s.primary.reset();                                         // crash
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    Client c2(router->port());
    const int after = drain(c2, "api:u1", "50", "0.001");
    EXPECT_EQ(before + after, 50) << "capacity 50: every acknowledged token must still count after the failover";
}

TEST(RateLimiterFailover, WithAsyncReplicationTheOverAdmissionIsExactlyTheUnreplicatedTail) {
    Wall wall;
    ShardNodes s(1, 0);
    auto router = make_router({s.spec("a")}, 0);
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    Client c(router->port());
    int admitted = 0;
    for (int i = 0; i < 20; ++i) admitted += allowed(acquire(c, "api:u2", "50", "0.001"));
    ASSERT_TRUE(wait_until([&] { return caught_up(*s.repl[0], *s.primary); }));
    s.repl[0]->svc.replica()->set_apply_paused(true);          // from here on, nothing reaches the replica
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    for (int i = 0; i < 10; ++i) admitted += allowed(acquire(c, "api:u2", "50", "0.001"));
    const uint64_t tail = s.primary->svc.log().last_offset() - s.repl[0]->svc.replica()->status().applied;
    ASSERT_EQ(admitted, 30);
    ASSERT_GT(tail, 0u);
    s.primary.reset();                                         // crash with `tail` acknowledged-but-unreplicated tokens
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    Client c2(router->port());
    admitted += drain(c2, "api:u2", "50", "0.001");
    // The documented bound: tokens granted <= capacity + (tokens consumed in the unreplicated tail).
    EXPECT_EQ(static_cast<uint64_t>(admitted), 50 + tail);
}

TEST(RateLimiterFailover, ANewPrimaryWhoseClockIsBehindGrantsNoRefillForTheSkew) {
    Wall wall;
    ShardNodes s(2, 1);                                        // two replicas: one is left to acknowledge after the failover
    auto router = make_router({s.spec("a")}, 1);
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    ASSERT_TRUE(wait_until([&] { return s.primary->svc.primary()->in_sync_replicas(1500) == 2; }));
    Client c(router->port());
    EXPECT_EQ(drain(c, "skew", "5", "0.5"), 5);                // 1 token per 2000 ms
    const int64_t t_last = wall.t.load();
    s.primary.reset();
    wall.t = t_last - 300;                                     // the replica's machine runs 300 ms behind
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    Client c2(router->port());
    auto r = acquire_retry(c2, "skew", "5", "0.5");
    ASSERT_EQ(r.size(), 4u);
    EXPECT_EQ(r[0], 0);
    wall.t = t_last + 1999;
    r = acquire_retry(c2, "skew", "5", "0.5");
    ASSERT_EQ(r.size(), 4u);
    EXPECT_EQ(r[0], 0) << "the skew must not be credited as elapsed time";
    wall.t = t_last + 2000;
    r = acquire_retry(c2, "skew", "5", "0.5");
    ASSERT_EQ(r.size(), 4u);
    EXPECT_EQ(r[0], 1);
}

TEST(RateLimiterFailover, ANewPrimaryWhoseClockIsFarAheadCanGrantAtMostOneExtraCapacity) {
    Wall wall;
    ShardNodes s(2, 1);
    auto router = make_router({s.spec("a")}, 1);
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    ASSERT_TRUE(wait_until([&] { return s.primary->svc.primary()->in_sync_replicas(1500) == 2; }));
    Client c(router->port());
    int admitted = drain(c, "ahead", "5", "0.5");
    EXPECT_EQ(admitted, 5);
    s.primary.reset();
    wall.t += 24LL * 3600 * 1000;                              // the new primary's clock is a day ahead
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    Client c2(router->port());
    admitted += drain(c2, "ahead", "5", "0.5");
    EXPECT_EQ(admitted, 10) << "documented: a forward clock jump refills the bucket once, never more than capacity";
}

TEST(RateLimiterFailover, AShardWithoutASafePrimaryRefusesToAnswerInsteadOfGuessing) {
    Wall wall;
    ShardNodes s(2, 1);                                        // R=2, sync 1: promotion needs both replicas
    auto router = make_router({s.spec("a")}, 1);
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    ASSERT_TRUE(wait_until([&] { return s.primary->svc.primary()->in_sync_replicas(1500) == 2; }));
    Client c(router->port());
    ASSERT_TRUE(allowed(acquire(c, "part", "3", "1")));
    s.primary.reset();
    s.repl[1].reset();
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").state == "unavailable"; }));
    Client c2(router->port());
    std::string err;
    EXPECT_TRUE(acquire(c2, "part", "3", "1", &err).empty());
    EXPECT_EQ(err.rfind("-ERR shard 'a' is unavailable", 0), 0u) << err;   // neither "allowed" nor "denied": the caller decides
    err.clear();
    EXPECT_TRUE(peek(c2, "part", "3", "1", &err).empty());
    EXPECT_EQ(err.rfind("-ERR shard 'a' is unavailable", 0), 0u) << err;
}

TEST(RateLimiterFailover, APrimaryWithoutAValidLeaseHandsOutNoTokens) {
    Wall wall;
    Node p(primary_opts(0, /*lease=*/true));                   // no coordinator ever granted it a lease
    Client c(p.port());
    std::string err;
    EXPECT_TRUE(acquire(c, "fenced", "5", "1", &err).empty());
    EXPECT_EQ(err[0], '-') << err;
    EXPECT_EQ(p.engine.size(), 0u);
}

// ---------------------------------------------------------------------------------------------
// Routing across shards and live rebalancing
// ---------------------------------------------------------------------------------------------

namespace {
RouterNode plain_spec(const std::string& name, Node& n) {
    RouterNode r;
    r.name = name;
    r.host = "127.0.0.1";
    r.port = n.port();
    return r;
}
}  // namespace

TEST(RateLimiterRouting, BucketsSpreadOverShardsAndEveryBucketIsExact) {
    Wall wall;
    Node a(primary_opts()), b(primary_opts()), d(primary_opts());
    RouterOptions ro;
    ro.port = 0;
    Router router({plain_spec("a", a), plain_spec("b", b), plain_spec("c", d)}, ro);
    ASSERT_TRUE(router.start(nullptr));
    Client c(router.port());
    constexpr int kKeys = 300;
    for (int k = 0; k < kKeys; ++k) {
        const std::string key = "tenant:" + std::to_string(k);
        EXPECT_EQ(drain(c, key, "3", "0.001"), 3) << key;
    }
    EXPECT_GT(a.engine.size(), 30u);
    EXPECT_GT(b.engine.size(), 30u);
    EXPECT_GT(d.engine.size(), 30u);
    EXPECT_EQ(a.engine.size() + b.engine.size() + d.engine.size(), static_cast<size_t>(kKeys));
    // a pipelined burst across shards keeps reply order: 4 attempts on one key => 1,1,1,0
    Client p(router.port());
    p.send_raw(Client::encode({"RL.ACQUIRE", "burst", "3", "0.001"}) + Client::encode({"RL.ACQUIRE", "burst", "3", "0.001"}) +
               Client::encode({"RL.ACQUIRE", "burst", "3", "0.001"}) + Client::encode({"RL.ACQUIRE", "burst", "3", "0.001"}));
    int got = 0;
    for (int i = 0; i < 4; ++i) {
        std::string head = p.read_reply();   // "*4"
        ASSERT_EQ(head, "*4");
        const std::string allowed_flag = p.read_reply();
        for (int j = 0; j < 3; ++j) p.read_reply();
        if (allowed_flag == ":1") ++got;
        EXPECT_EQ(allowed_flag, i < 3 ? ":1" : ":0") << i;
    }
    EXPECT_EQ(got, 3);
}

TEST(RateLimiterRouting, BucketsSurviveLiveMigrationAndConcurrentCallersNeverExceedTheLimit) {
    Wall wall;
    Node a(primary_opts()), b(primary_opts()), fresh(primary_opts());
    RouterOptions ro;
    ro.port = 0;
    Router router({plain_spec("a", a), plain_spec("b", b)}, ro);
    ASSERT_TRUE(router.start(nullptr));
    constexpr int kKeys = 150, kCap = 6;
    {
        Client c(router.port());
        for (int k = 0; k < kKeys; ++k)
            for (int i = 0; i < 2; ++i) ASSERT_TRUE(allowed(acquire(c, "m" + std::to_string(k), "6", "0.001")));
    }
    std::vector<std::atomic<int>> granted(kKeys);
    for (auto& g : granted) g = 2;                              // the two taken above
    std::atomic<bool> go{true};
    std::vector<std::thread> workers;
    for (int t = 0; t < 4; ++t)
        workers.emplace_back([&] {
            Client c(router.port());
            for (int pass = 0; pass < 12 && go; ++pass)
                for (int k = 0; k < kKeys; ++k)
                    if (allowed(acquire(c, "m" + std::to_string(k), "6", "0.001"))) granted[k]++;
        });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    {
        Client admin(router.port());
        const std::string r = admin.cmd({"RING", "ADD", "n3", "127.0.0.1", std::to_string(fresh.port()), "MIGRATE"});
        EXPECT_EQ(r.rfind("+", 0), 0u) << r;
    }
    for (auto& w : workers) w.join();
    Client c(router.port());
    for (int k = 0; k < kKeys; ++k) granted[k] += drain(c, "m" + std::to_string(k), "6", "0.001");
    int wrong = 0;
    for (int k = 0; k < kKeys; ++k) if (granted[k] != kCap) ++wrong;
    EXPECT_EQ(wrong, 0) << "every bucket must grant exactly its capacity in total, wherever it lived at the time";
    EXPECT_GT(fresh.engine.size(), 10u) << "the new node must really have received buckets";
}

TEST(RateLimiterRouting, BucketsFirstTouchedWhileAMigrationRunsAreNotOrphanedOnTheOldOwner) {
    Wall wall;
    Node a(primary_opts()), b(primary_opts()), fresh(primary_opts());
    RouterOptions ro;
    ro.port = 0;
    Router router({plain_spec("a", a), plain_spec("b", b)}, ro);
    ASSERT_TRUE(router.start(nullptr));
    {   // plenty of ordinary data, so the migration takes long enough for new buckets to appear inside it
        Client c(router.port());
        std::string batch;
        for (int i = 0; i < 40000; ++i) {
            batch += Client::encode({"SET", "filler:" + std::to_string(i), "x"});
            if (i % 500 == 499) { c.send_raw(batch); batch.clear(); for (int j = 0; j < 500; ++j) c.read_reply(); }
        }
    }
    constexpr int kKeys = 200, kCap = 6;
    std::vector<std::atomic<int>> granted(kKeys);
    std::atomic<bool> migrating{true};
    std::atomic<long> during{0};
    std::thread admin([&] {
        Client c(router.port());
        const std::string r = c.cmd({"RING", "ADD", "n3", "127.0.0.1", std::to_string(fresh.port()), "MIGRATE"});
        EXPECT_EQ(r.rfind("+", 0), 0u) << r;
        migrating = false;
    });
    std::vector<std::thread> workers;
    for (int t = 0; t < 3; ++t)
        workers.emplace_back([&, t] {
            Client c(router.port());
            // each worker walks the keys in a different order, only buckets that do not exist yet are created here
            for (int k = t; migrating; k = (k + 1) % kKeys) {
                if (allowed(acquire(c, "fresh" + std::to_string(k), "6", "0.001"))) granted[k]++;
                ++during;
            }
        });
    admin.join();
    for (auto& w : workers) w.join();
    ASSERT_GT(during.load(), 100) << "the migration finished before any bucket was touched; make it longer";
    Client c(router.port());
    for (int k = 0; k < kKeys; ++k) granted[k] += drain(c, "fresh" + std::to_string(k), "6", "0.001");
    int wrong = 0;
    for (int k = 0; k < kKeys; ++k) if (granted[k] != kCap) ++wrong;
    EXPECT_EQ(wrong, 0) << "a bucket created mid-migration must live on its NEW owner, or it resets when the migration ends";
}
