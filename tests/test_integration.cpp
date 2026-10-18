// Cross-phase integration tests (Phase 10): the combinations earlier phases tested only separately.
//   * a failover in the middle of a Phase 7 live migration (replication + failover + rebalancing),
//     and the resumable-migration repair that makes it safe (RING RESUME).
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
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

bool wait_until(const std::function<bool()>& pred, int timeout_ms = 30000) {
    const auto end = Clock::now() + std::chrono::milliseconds(timeout_ms);
    while (Clock::now() < end) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

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

NodeReplicationOptions opts(int sync_n, bool lease) {
    NodeReplicationOptions o;
    o.repl_port = 0;
    o.require_lease = lease;
    o.sync_replicas = sync_n;
    o.sync_timeout_ms = 1500;
    o.log_events = false;
    return o;
}

struct ShardNodes {
    ShardNodes(int replicas, int sync_n) {
        primary = std::make_unique<Node>(opts(sync_n, true));
        for (int i = 0; i < replicas; ++i) {
            auto o = opts(sync_n, true);
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

RouterNode plain_spec(const std::string& name, Node& n) {
    RouterNode r;
    r.name = name;
    r.host = "127.0.0.1";
    r.port = n.port();
    return r;
}

std::unique_ptr<Router> make_router(std::vector<RouterNode> nodes, int sync_n) {
    RouterOptions ro;
    ro.port = 0;
    ro.io_timeout_ms = 2500;
    ro.failover.lease_ms = 500;
    ro.failover.probe_ms = 100;
    ro.failover.probe_timeout_ms = 200;
    ro.failover.fail_after = 3;
    ro.failover.guard_ms = 100;
    ro.failover.sync_replicas = sync_n;
    ro.failover.log_events = false;
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

long long errors_in(const std::string& reply) {  // "+OK scanned=.. moved=.. errors=N [PENDING...]"
    const auto p = reply.find("errors=");
    return p == std::string::npos ? -1 : std::atoll(reply.c_str() + p + 7);
}

}  // namespace

TEST(Integration, AFailoverInTheMiddleOfALiveMigrationLosesNothingAndResumeFinishesTheJob) {
    ShardNodes a(2, 1);                       // a replicated shard: primary + 2 replicas, --repl-sync 1
    Node b(opts(0, false)), fresh(opts(0, false));
    auto router = make_router({a.spec("a"), plain_spec("b", b)}, 1);
    ASSERT_TRUE(wait_until([&] { return healthy(*router, "a"); }));
    ASSERT_TRUE(wait_until([&] { return a.primary->svc.primary()->in_sync_replicas(1500) == 2; }));

    constexpr int kKeys = 40000;
    {   // every key is acknowledged by the cluster (sync 1 for shard a) before the trouble starts
        Client c(router->port());
        std::string batch;
        for (int i = 0; i < kKeys; ++i) {
            if (i % 100 == 0) batch += Client::encode({"SET", "k" + std::to_string(i), "v" + std::to_string(i), "EX", "3600"});
            else batch += Client::encode({"SET", "k" + std::to_string(i), "v" + std::to_string(i)});
            if (i % 500 == 499) {
                c.send_raw(batch);
                batch.clear();
                for (int j = 0; j < 500; ++j) ASSERT_EQ(c.read_reply(), "+OK");
            }
        }
    }

    std::string migrate_reply;
    std::thread admin([&] {
        Client c(router->port());
        migrate_reply = c.cmd({"RING", "ADD", "c", "127.0.0.1", std::to_string(fresh.port()), "MIGRATE"});
    });
    {   // wait until the migration is really running, then crash the primary of shard a
        Client probe(router->port());
        ASSERT_TRUE(wait_until([&] { return probe.cmd({"RING", "INFO"}).find("migrating:yes") != std::string::npos; }, 5000));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    a.primary.reset();
    admin.join();
    ASSERT_EQ(migrate_reply.rfind("+OK", 0), 0u) << migrate_reply;
    ASSERT_GT(errors_in(migrate_reply), 0) << "the crash landed after the migration had finished: " << migrate_reply;
    EXPECT_TRUE(router->migration_pending()) << "a migration with errors must stay pending";
    EXPECT_NE(migrate_reply.find("PENDING"), std::string::npos) << migrate_reply;

    // While pending, nothing may be refused for the wrong reason or answered wrongly: another topology
    // change is refused, and once the shard is back every key is still found (old-owner fallback).
    {
        Client c(router->port());
        EXPECT_EQ(c.cmd({"RING", "REMOVE", "b"}).rfind("-ERR a migration is pending", 0), 0u);
    }
    ASSERT_TRUE(wait_until([&] { return shard_info(*router, "a").failovers == 1 && healthy(*router, "a"); }));
    {
        Client c(router->port());
        for (int i = 0; i < kKeys; i += 7) ASSERT_EQ(c.cmd({"GET", "k" + std::to_string(i)}), "v" + std::to_string(i)) << "k" << i;
    }

    // Repair: resume until no error remains.
    std::string reply;
    Client admin2(router->port());
    for (int attempt = 0; attempt < 10; ++attempt) {
        reply = admin2.cmd({"RING", "RESUME"});
        ASSERT_EQ(reply.rfind("+OK", 0), 0u) << reply;
        if (errors_in(reply) == 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    EXPECT_EQ(errors_in(reply), 0) << reply;
    EXPECT_FALSE(router->migration_pending());
    EXPECT_EQ(admin2.cmd({"RING", "RESUME"}).rfind("-ERR no migration is pending", 0), 0u);

    Client c(router->port());
    int bad = 0;
    for (int i = 0; i < kKeys; ++i)
        if (c.cmd({"GET", "k" + std::to_string(i)}) != "v" + std::to_string(i)) ++bad;
    EXPECT_EQ(bad, 0) << "keys lost or wrong after the failover + resumed migration";
    EXPECT_EQ(c.cmd({"DBSIZE"}), ":" + std::to_string(kKeys)) << "a key exists twice or not at all";
    const long long ttl_ms = std::stoll(c.cmd({"PTTL", "k100"}).substr(1));
    EXPECT_GT(ttl_ms, 3'500'000);             // the TTL travelled with the key through the migration
    EXPECT_LE(ttl_ms, 3'600'000);
    EXPECT_GT(fresh.engine.size(), 1000u) << "the new node must have received its share";
    // and the topology can change again
    EXPECT_EQ(admin2.cmd({"RING", "REMOVE", "c", "MIGRATE"}).rfind("+OK", 0), 0u);
}

TEST(Integration, AnAbortedMigrationAcceptsTheLossAndUnblocksTopologyChanges) {
    Node a(opts(0, false)), b(opts(0, false)), fresh(opts(0, false));
    RouterOptions ro;
    ro.port = 0;
    Router router({plain_spec("a", a), plain_spec("b", b)}, ro);
    ASSERT_TRUE(router.start(nullptr));
    Client c(router.port());
    for (int i = 0; i < 2000; ++i) ASSERT_EQ(c.cmd({"SET", "k" + std::to_string(i), "v"}), "+OK");
    // the destination is unreachable (port 1): every copy fails, the migration is left pending
    const std::string r = c.cmd({"RING", "ADD", "dead", "127.0.0.1", "1", "MIGRATE"});
    ASSERT_EQ(r.rfind("+OK", 0), 0u) << r;
    EXPECT_GT(errors_in(r), 0) << r;
    EXPECT_TRUE(router.migration_pending());
    EXPECT_EQ(c.cmd({"RING", "RESUME"}).find("PENDING") != std::string::npos, true);   // still failing: still pending
    EXPECT_EQ(c.cmd({"RING", "ABORT"}), "+OK");
    EXPECT_FALSE(router.migration_pending());
    EXPECT_EQ(c.cmd({"RING", "REMOVE", "dead"}), "+OK");                                // the ring can be repaired now
    EXPECT_EQ(c.cmd({"RING", "ABORT"}).rfind("-ERR no migration is pending", 0), 0u);
}
