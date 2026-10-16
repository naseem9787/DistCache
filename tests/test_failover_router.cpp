// Automatic failover through the router (Phase 8, step 6), with in-process nodes: the router's
// coordinator watches replicated shards, keeps their primaries' leases alive, and promotes a
// replica when a primary stops answering. A "crash" is destroying the node object (its sockets
// close, as for a killed process). Process-level scenarios (SIGSTOP split brain, WAL restart,
// stale-state refusal) are in test_failover_e2e.cpp.
//
// Timings are scaled down (lease 500 ms, probe 100 ms) so a failover takes about a second;
// the rules are the same ones the defaults (lease 2 s, probe 500 ms) use.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "node_replication.hpp"
#include "observer_chain.hpp"
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

struct Node {
    explicit Node(NodeReplicationOptions o) : engine(8), svc(engine, std::move(o)), hooks(nullptr, &svc), server(engine, ServerOptions{0, 2, &hooks}) {
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

NodeReplicationOptions node_opts(int sync_n) {
    NodeReplicationOptions o;
    o.repl_port = 0;
    o.require_lease = true;
    o.sync_replicas = sync_n;
    o.sync_timeout_ms = 1500;
    o.log_events = false;
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

// One replicated shard: a primary and `replicas` replicas, all lease-gated.
struct ShardNodes {
    ShardNodes(int replicas, int sync_n) {
        primary = std::make_unique<Node>(node_opts(sync_n));
        for (int i = 0; i < replicas; ++i) {
            auto o = node_opts(sync_n);
            o.replicaof_host = "127.0.0.1";
            o.replicaof_port = primary->svc.repl_port();
            repl.push_back(std::make_unique<Node>(o));
        }
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

std::unique_ptr<Router> make_router(std::vector<RouterNode> nodes, int sync_n, const std::string& state_file = "") {
    RouterOptions ro;
    ro.port = 0;
    ro.io_timeout_ms = 2000;
    ro.failover = fast_failover(sync_n);
    ro.failover.state_file = state_file;
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

}  // namespace

TEST(RouterFailover, AShardBecomesAvailableOnlyAfterTheCoordinatorHasConfirmedItsPrimary) {
    ShardNodes s(1, 0);
    auto router = make_router({s.spec("a")}, 0);
    Client c(router->port());
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    EXPECT_EQ(c.cmd({"SET", "k", "v"}), "+OK");  // the lease was granted: writes work
    EXPECT_EQ(c.cmd({"GET", "k"}), "v");
    const std::string status = c.cmd({"SHARD", "STATUS"});
    EXPECT_NE(status.find("shard:a state=healthy"), std::string::npos) << status;
    EXPECT_NE(status.find("epoch=0"), std::string::npos);
    // a router with no replicated shard has nothing to report
    Node lone(NodeReplicationOptions{});
    auto r2 = std::make_unique<Router>(std::vector<RouterNode>{RouterNode{"x", "127.0.0.1", lone.port(), 1, {}, true}}, RouterOptions{0});
    ASSERT_TRUE(r2->start(nullptr));
    Client c2(r2->port());
    EXPECT_EQ(c2.cmd({"SHARD", "STATUS"}).rfind("-ERR no replicated shards", 0), 0u);
}

TEST(RouterFailover, ADeadPrimaryIsReplacedByTheMostAdvancedReplicaAndNothingIsLost) {
    ShardNodes s(2, 0);
    auto router = make_router({s.spec("a")}, 0);
    Client c(router->port());
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    for (int i = 0; i < 200; ++i) ASSERT_EQ(c.cmd({"SET", "k" + std::to_string(i), "v" + std::to_string(i)}), "+OK");
    ASSERT_EQ(c.cmd({"SET", "ttl", "x", "EX", "100"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return caught_up(*s.repl[0], *s.primary) && caught_up(*s.repl[1], *s.primary); }));

    const auto t0 = Clock::now();
    s.primary.reset();  // the primary crashes
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }))
        << router->coordinator()->status_text();
    const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
    EXPECT_LT(took, 8000);
    // The coordinator must not promote before the old primary's lease can no longer be alive:
    // lease 500 ms * 1.05 + guard 100 ms = 625 ms after the last grant, which was at most one probe
    // period (100 ms) before the crash. Promoting right after the 3rd missed probe (~300 ms) is the bug.
    EXPECT_GE(took, 500) << "promoted before the old lease could have expired";

    const auto info = shard_info(*router, "a");
    EXPECT_EQ(info.epoch, 1);
    EXPECT_EQ(info.primary, "a#1");  // equal offsets: the lowest name wins
    Client c2(router->port());
    for (int i = 0; i < 200; ++i) ASSERT_EQ(c2.cmd({"GET", "k" + std::to_string(i)}), "v" + std::to_string(i)) << i;
    const long long ttl = std::stoll(c2.cmd({"TTL", "ttl"}).substr(1));
    EXPECT_GT(ttl, 80);  // the absolute deadline survived the failover
    EXPECT_LE(ttl, 100);
    EXPECT_EQ(c2.cmd({"SET", "after", "failover"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return s.repl[1]->engine.get("after") == std::optional<std::string>("failover"); }))
        << "the other replica must follow the new primary";
    EXPECT_NE(c2.cmd({"RING", "INFO"}).find("addr=127.0.0.1:" + std::to_string(s.repl[0]->port())), std::string::npos);
    const std::string events = c2.cmd({"SHARD", "EVENTS"});
    EXPECT_NE(events.find("PRIMARY FAILED"), std::string::npos) << events;
    EXPECT_NE(events.find("PROMOTED a#1 to epoch 1"), std::string::npos) << events;
}

TEST(RouterFailover, ThePromotedReplicaIsTheMostAdvancedOneAndKeepsEverythingItHad) {
    ShardNodes s(2, 0);
    auto router = make_router({s.spec("a")}, 0);
    Client c(router->port());
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    for (int i = 0; i < 50; ++i) ASSERT_EQ(c.cmd({"SET", "base" + std::to_string(i), "v"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return caught_up(*s.repl[0], *s.primary) && caught_up(*s.repl[1], *s.primary); }));
    s.repl[0]->svc.replica()->set_apply_paused(true);  // a#1 stops receiving; a#2 keeps going
    for (int i = 0; i < 50; ++i) ASSERT_EQ(c.cmd({"SET", "late" + std::to_string(i), "v"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return caught_up(*s.repl[1], *s.primary); }));
    s.primary.reset();
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    EXPECT_EQ(shard_info(*router, "a").primary, "a#2") << "the behind replica must not be chosen";
    Client c2(router->port());
    for (int i = 0; i < 50; ++i) ASSERT_EQ(c2.cmd({"GET", "late" + std::to_string(i)}), "v") << i;
}

TEST(RouterFailover, TheShardRefusesTrafficWhileItFailsOverInsteadOfGuessing) {
    ShardNodes s(1, 0);
    auto router = make_router({s.spec("a")}, 0);
    Client c(router->port());
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    ASSERT_EQ(c.cmd({"SET", "k", "v"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return caught_up(*s.repl[0], *s.primary); }));
    s.primary.reset();
    bool saw_unavailable = false;
    const auto end = Clock::now() + std::chrono::seconds(20);
    while (Clock::now() < end) {
        Client probe(router->port());
        const std::string r = probe.cmd({"GET", "k"});
        if (r.rfind("-ERR shard 'a' is unavailable", 0) == 0) saw_unavailable = true;
        if (r == "v") break;  // served again by the new primary
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(saw_unavailable) << "reads must be refused (not forwarded to a node we no longer trust) during the failover";
    EXPECT_EQ(shard_info(*router, "a").failovers, 1u);
}

TEST(RouterFailover, SyncOneNeverLosesAnAcknowledgedWriteAcrossAFailoverUnderLoad) {
    ShardNodes s(2, 1);
    auto router = make_router({s.spec("a")}, 1);
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    ASSERT_TRUE(wait_until([&] { return s.primary->svc.primary()->in_sync_replicas(1500) == 2; }));

    std::mutex mu;
    std::vector<std::pair<std::string, std::string>> acked;
    std::atomic<bool> stop{false};
    std::atomic<long> errors{0}, successes{0};
    std::vector<std::thread> writers;
    for (int t = 0; t < 4; ++t) {
        writers.emplace_back([&, t] {
            std::unique_ptr<Client> c;
            for (int i = 0; !stop; ++i) {
                if (!c || !c->ok()) c = std::make_unique<Client>(router->port());
                const std::string k = "w" + std::to_string(t) + ":" + std::to_string(i), v = "v" + std::to_string(i);
                const std::string r = c->cmd({"SET", k, v});
                if (r == "+OK") {
                    std::lock_guard<std::mutex> g(mu);
                    acked.emplace_back(k, v);  // the client was told: this write must survive
                    ++successes;
                } else {
                    ++errors;  // refused, unreachable, or the connection closed (outcome unknown): no promise made
                    c.reset();
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    const long before = successes.load();
    ASSERT_GT(before, 20);
    s.primary.reset();  // crash in the middle of traffic
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    const long at_failover = successes.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // traffic continues on the new primary
    stop = true;
    for (auto& w : writers) w.join();
    EXPECT_GT(successes.load(), at_failover) << "writes must succeed again after the failover";
    EXPECT_GT(errors.load(), 0) << "there must have been an unavailability window";

    Client check(router->port());
    size_t missing = 0;
    for (auto& kv : acked)
        if (check.cmd({"GET", kv.first}) != kv.second) ++missing;
    EXPECT_EQ(missing, 0u) << missing << " of " << acked.size() << " acknowledged writes were lost";
    EXPECT_GT(acked.size(), 50u);
}

TEST(RouterFailover, WithTooFewEligibleReplicasNothingIsPromotedAndTheShardStaysClosed) {
    // R = 2 replicas, sync 1: the rule needs R-N+1 = 2 eligible replicas, because an acknowledged
    // write may live on only ONE of them and we cannot tell which one survived.
    ShardNodes s(2, 1);
    auto router = make_router({s.spec("a")}, 1);
    Client c(router->port());
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    ASSERT_TRUE(wait_until([&] { return s.primary->svc.primary()->in_sync_replicas(1500) == 2; }));
    ASSERT_EQ(c.cmd({"SET", "k", "v"}), "+OK");
    s.primary.reset();
    s.repl[1].reset();  // and one replica is gone too
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").state == "unavailable"; })) << router->coordinator()->status_text();
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));  // plenty of rounds in which a careless coordinator would promote
    const auto info = shard_info(*router, "a");
    EXPECT_EQ(info.failovers, 0u);
    EXPECT_EQ(info.state, "unavailable");
    EXPECT_NE(info.reason.find("NOT PROMOTING"), std::string::npos) << info.reason;
    EXPECT_EQ(s.repl[0]->svc.role(), ReplicationService::Role::Replica);  // untouched
    Client c2(router->port());
    EXPECT_EQ(c2.cmd({"SET", "x", "1"}).rfind("-ERR shard 'a' is unavailable", 0), 0u);
    EXPECT_EQ(c2.cmd({"GET", "k"}).rfind("-ERR shard 'a' is unavailable", 0), 0u);
}

TEST(RouterFailover, WithoutSyncReplicationOneSurvivingReplicaIsEnough) {
    ShardNodes s(2, 0);
    auto router = make_router({s.spec("a")}, 0);
    Client c(router->port());
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    ASSERT_EQ(c.cmd({"SET", "k", "v"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return caught_up(*s.repl[0], *s.primary) && caught_up(*s.repl[1], *s.primary); }));
    s.primary.reset();
    s.repl[1].reset();
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    Client c2(router->port());
    EXPECT_EQ(c2.cmd({"GET", "k"}), "v");
}

TEST(RouterFailover, ARestartedRouterLearnsTheEpochFromTheNodesAndKeepsServing) {
    ShardNodes s(1, 0);
    auto router = make_router({s.spec("a")}, 0);
    {
        Client c(router->port());
        ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
        ASSERT_EQ(c.cmd({"SET", "k", "v"}), "+OK");
        ASSERT_TRUE(wait_until([&] { return caught_up(*s.repl[0], *s.primary); }));
    }
    s.primary.reset();
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    const RouterNode original_config = [&] {  // the router is restarted with its ORIGINAL configuration: primary = the dead node
        RouterNode n;
        n.name = "a"; n.host = "127.0.0.1"; n.port = 1; n.replicas = {{"127.0.0.1", s.repl[0]->port()}};
        return n;
    }();
    router.reset();

    // The new coordinator has no memory. It must find the promoted node by itself, not promote again.
    auto router2 = make_router({original_config}, 0);
    ASSERT_TRUE(wait_until([&] { return healthy(*router2, "a"); })) << router2->coordinator()->status_text();
    EXPECT_EQ(shard_info(*router2, "a").failovers, 0u);
    EXPECT_EQ(shard_info(*router2, "a").epoch, 1);
    Client c(router2->port());
    EXPECT_EQ(c.cmd({"GET", "k"}), "v");
    EXPECT_EQ(c.cmd({"SET", "k2", "v2"}), "+OK");
}

TEST(RouterFailover, OtherShardsAreUnaffectedAndRebalancingWorksAfterAFailover) {
    ShardNodes a(1, 0);
    Node b(NodeReplicationOptions{});  // a plain, unreplicated shard
    RouterNode bn;
    bn.name = "b"; bn.host = "127.0.0.1"; bn.port = b.port();
    auto router = make_router({a.spec("a"), bn}, 0);
    Client c(router->port());
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    constexpr int kKeys = 600;
    for (int i = 0; i < kKeys; ++i) ASSERT_EQ(c.cmd({"SET", "k" + std::to_string(i), "v" + std::to_string(i)}), "+OK");
    ASSERT_TRUE(wait_until([&] { return caught_up(*a.repl[0], *a.primary); }));

    // keys that live on shard b keep working THROUGHOUT the failover of shard a
    std::vector<std::string> b_keys;
    for (int i = 0; i < kKeys; ++i) {
        const std::string k = "k" + std::to_string(i);
        if (c.cmd({"RING", "LOOKUP", k}) == "b") b_keys.push_back(k);
    }
    ASSERT_GT(b_keys.size(), 100u);
    std::atomic<bool> b_failed{false}, done{false};
    std::thread reader([&] {
        Client rc(router->port());
        size_t i = 0;
        while (!done) {
            const std::string& k = b_keys[i++ % b_keys.size()];
            if (rc.cmd({"GET", k}) != "v" + k.substr(1)) b_failed = true;
        }
    });
    a.primary.reset();
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    done = true;
    reader.join();
    EXPECT_FALSE(b_failed) << "shard b must not notice shard a's failover";

    for (int i = 0; i < kKeys; ++i) ASSERT_EQ(c.cmd({"GET", "k" + std::to_string(i)}), "v" + std::to_string(i));

    // Phase 7 rebalancing after the failover: a third shard joins with migration; the migration talks to
    // shard a's NEW primary (the route was updated), and no key is lost.
    Node fresh(NodeReplicationOptions{});
    const std::string reply = c.cmd({"RING", "ADD", "c", "127.0.0.1", std::to_string(fresh.port()), "MIGRATE"});
    ASSERT_EQ(reply.rfind("+OK", 0), 0u) << reply;
    EXPECT_NE(reply.find("errors=0"), std::string::npos) << reply;
    for (int i = 0; i < kKeys; ++i) ASSERT_EQ(c.cmd({"GET", "k" + std::to_string(i)}), "v" + std::to_string(i)) << "k" << i;
    EXPECT_EQ(c.cmd({"DBSIZE"}), ":" + std::to_string(kKeys));
}
