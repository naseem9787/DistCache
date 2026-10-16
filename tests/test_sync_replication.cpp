// Synchronous replication ("sync N"), Phase 8 step 4. Real nodes, real client ports.
//
// The guarantee under test: when a client receives +OK for a write on a node running sync N,
// at least N distinct replicas have ALREADY applied that write. The tests check it at the only
// moment that matters, immediately after the reply, with no waiting.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "node_replication.hpp"
#include "observer_chain.hpp"
#include "server.hpp"
#include "synced_store.hpp"
#include "test_client.hpp"

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

NodeReplicationOptions sync_primary(int n, int timeout_ms = 3000) {
    NodeReplicationOptions o;
    o.repl_port = 0;
    o.sync_replicas = n;
    o.sync_timeout_ms = timeout_ms;
    return o;
}
NodeReplicationOptions replica_of(const Node& p, const std::string& id = "") {
    NodeReplicationOptions o;
    o.replicaof_host = "127.0.0.1";
    o.replicaof_port = p.svc.repl_port();
    o.replica_id = id;
    return o;
}
bool online(Node& r) { return r.svc.replica()->status().state == ReplicationReplica::State::Online; }

// Runs `fn` on another thread and reports whether it finished within `ms`.
struct Async {
    explicit Async(std::function<void()> fn) : th([this, fn] { fn(); done = true; }) {}
    ~Async() { if (th.joinable()) th.join(); }
    bool finished_within(int ms) {
        wait_until([&] { return done.load(); }, ms);
        return done.load();
    }
    std::atomic<bool> done{false};
    std::thread th;
};

}  // namespace

TEST(SyncReplication, AnAcknowledgedWriteIsAlreadyOnTheReplica) {
    Node primary(sync_primary(1));
    Node replica(replica_of(primary));
    ASSERT_TRUE(wait_until([&] { return online(replica); }));
    ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->in_sync_replicas(1500) == 1; }));
    Client c(primary.port());
    int missing = 0;
    for (int i = 0; i < 300; ++i) {
        const std::string k = "k" + std::to_string(i), v = "v" + std::to_string(i);
        ASSERT_EQ(c.cmd({"SET", k, v}), "+OK");
        if (replica.engine.get(k) != std::optional<std::string>(v)) ++missing;  // NO waiting: the reply is the proof
        if (i % 5 == 0) {
            ASSERT_EQ(c.cmd({"DEL", k}), ":1");
            if (replica.engine.get(k).has_value()) ++missing;
        }
    }
    EXPECT_EQ(missing, 0);
}

TEST(SyncReplication, TheReplyWaitsWhileTheReplicaHasNotApplied) {
    Node primary(sync_primary(1));
    Node replica(replica_of(primary));
    ASSERT_TRUE(wait_until([&] { return online(replica); }));
    ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->in_sync_replicas(1500) == 1; }));

    replica.svc.replica()->set_apply_paused(true);  // it will not apply, hence not ACK
    std::string reply;
    Async a([&] { Client c(primary.port()); reply = c.cmd({"SET", "k", "v"}); });
    EXPECT_FALSE(a.finished_within(400)) << "the reply was sent before any replica applied the write";
    EXPECT_EQ(primary.engine.get("k"), std::optional<std::string>("v"));  // applied locally, not yet acknowledged
    replica.svc.replica()->set_apply_paused(false);
    ASSERT_TRUE(a.finished_within(5000));
    EXPECT_EQ(reply, "+OK");
    EXPECT_EQ(replica.engine.get("k"), std::optional<std::string>("v"));
}

TEST(SyncReplication, ReadsAreNeverDelayed) {
    Node primary(sync_primary(1));
    Node replica(replica_of(primary));
    ASSERT_TRUE(wait_until([&] { return online(replica); }));
    Client c(primary.port());
    ASSERT_EQ(c.cmd({"SET", "k", "v"}), "+OK");
    replica.svc.replica()->set_apply_paused(true);
    std::string r;
    Async a([&] { Client c2(primary.port()); r = c2.cmd({"GET", "k"}); });
    EXPECT_TRUE(a.finished_within(300));
    EXPECT_EQ(r, "v");
    replica.svc.replica()->set_apply_paused(false);
}

TEST(SyncReplication, WritesAreRefusedWhenTooFewReplicasAreInSync) {
    Node primary(sync_primary(1));
    Client c(primary.port());
    const std::string r = c.cmd({"SET", "k", "v"});
    EXPECT_EQ(r.rfind("-NOREPLICAS", 0), 0u) << r;
    EXPECT_EQ(primary.engine.get("k"), std::nullopt);  // refused BEFORE touching the data
    EXPECT_EQ(c.cmd({"GET", "k"}), "(nil)");           // reads still work

    Node replica(replica_of(primary));
    ASSERT_TRUE(wait_until([&] { return online(replica); }));
    ASSERT_TRUE(wait_until([&] { return c.cmd({"SET", "k", "v"}) == "+OK"; }));

    replica.svc.stop();  // replica goes away: writes are refused again once it no longer counts
    ASSERT_TRUE(wait_until([&] {
        Client fresh(primary.port());  // a write accepted in the meantime is held until the timeout and closes its connection
        return fresh.cmd({"SET", "k2", "v"}).rfind("-NOREPLICAS", 0) == 0;
    }, 15000));
}

TEST(SyncReplication, WhenTheAcksNeverArriveTheConnectionIsClosedButTheWriteWasApplied) {
    Node primary(sync_primary(1, /*timeout_ms=*/300));
    Node replica(replica_of(primary));
    ASSERT_TRUE(wait_until([&] { return online(replica); }));
    ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->in_sync_replicas(1500) == 1; }));
    replica.svc.replica()->set_apply_paused(true);
    Client c(primary.port());
    const auto t0 = Clock::now();
    EXPECT_EQ(c.cmd({"SET", "k", "v"}), "<closed>");  // no +OK, no error: the client does not know
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
    EXPECT_GE(ms, 250);
    EXPECT_LT(ms, 3000);
    EXPECT_EQ(primary.engine.get("k"), std::optional<std::string>("v"));  // the outcome the client cannot see
    replica.svc.replica()->set_apply_paused(false);
}

TEST(SyncReplication, SyncTwoNeedsBothReplicasAndSyncOneNeedsOnlyTheFasterOne) {
    {
        Node primary(sync_primary(2));
        Node r1(replica_of(primary)), r2(replica_of(primary));
        ASSERT_TRUE(wait_until([&] { return online(r1) && online(r2); }));
        ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->in_sync_replicas(1500) == 2; }));
        r2.svc.replica()->set_apply_paused(true);
        std::string reply;
        Async a([&] { Client c(primary.port()); reply = c.cmd({"SET", "k", "v"}); });
        EXPECT_FALSE(a.finished_within(400)) << "sync 2 must wait for BOTH replicas";
        r2.svc.replica()->set_apply_paused(false);
        ASSERT_TRUE(a.finished_within(5000));
        EXPECT_EQ(reply, "+OK");
        EXPECT_EQ(r1.engine.get("k"), std::optional<std::string>("v"));
        EXPECT_EQ(r2.engine.get("k"), std::optional<std::string>("v"));
    }
    {
        Node primary(sync_primary(1));
        Node r1(replica_of(primary)), r2(replica_of(primary));
        ASSERT_TRUE(wait_until([&] { return online(r1) && online(r2); }));
        ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->in_sync_replicas(1500) == 2; }));
        r2.svc.replica()->set_apply_paused(true);  // one replica stalls; the other is enough for sync 1
        Client c(primary.port());
        for (int i = 0; i < 50; ++i) {
            ASSERT_EQ(c.cmd({"SET", "k" + std::to_string(i), "v"}), "+OK");
            ASSERT_EQ(r1.engine.get("k" + std::to_string(i)), std::optional<std::string>("v"));  // it was r1 that acked
        }
        r2.svc.replica()->set_apply_paused(false);
    }
}

TEST(SyncReplication, APipelinedBatchIsAcknowledgedInOrderAfterItsLastWrite) {
    Node primary(sync_primary(1));
    Node replica(replica_of(primary));
    ASSERT_TRUE(wait_until([&] { return online(replica); }));
    ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->in_sync_replicas(1500) == 1; }));
    Client c(primary.port());
    std::string batch;
    std::vector<std::string> expect;
    for (int i = 0; i < 100; ++i) {
        batch += Client::encode({"SET", "k" + std::to_string(i), "v" + std::to_string(i)});
        expect.push_back("+OK");
        batch += Client::encode({"GET", "k" + std::to_string(i)});
        expect.push_back("v" + std::to_string(i));
    }
    batch += Client::encode({"PING"});
    expect.push_back("+PONG");
    c.send_raw(batch);
    for (size_t i = 0; i < expect.size(); ++i) ASSERT_EQ(c.read_reply(), expect[i]) << "reply #" << i;
    // when the last reply of the batch is in, the whole batch is on the replica
    for (int i = 0; i < 100; ++i) EXPECT_EQ(replica.engine.get("k" + std::to_string(i)), std::optional<std::string>("v" + std::to_string(i)));
}

TEST(SyncReplication, ManyConcurrentWritersAreAllAcknowledgedAfterReplication) {
    Node primary(sync_primary(1));
    Node replica(replica_of(primary));
    ASSERT_TRUE(wait_until([&] { return online(replica); }));
    ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->in_sync_replicas(1500) == 1; }));
    std::atomic<int> missing{0}, failures{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < 6; ++t) {
        ts.emplace_back([&, t] {
            Client c(primary.port());
            for (int i = 0; i < 150; ++i) {
                const std::string k = "t" + std::to_string(t) + ":" + std::to_string(i);
                if (c.cmd({"SET", k, "v"}) != "+OK") { ++failures; return; }
                if (replica.engine.get(k) != std::optional<std::string>("v")) ++missing;
            }
        });
    }
    for (auto& th : ts) th.join();
    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(missing.load(), 0);
}

// Two links from the SAME replica process (it reconnected and the old link has not been noticed
// yet) must count once. Otherwise "N distinct replicas" would be a lie and the promotion proof
// (which needs N distinct holders) would not apply.
TEST(SyncReplication, TheSameReplicaOnTwoLinksCountsOnce) {
    Node primary(sync_primary(2));
    Node a(replica_of(primary, "same-process")), b(replica_of(primary, "same-process"));
    ASSERT_TRUE(wait_until([&] { return online(a) && online(b); }));
    ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->replicas().size() == 2; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));  // acks flowing on both links
    EXPECT_EQ(primary.svc.primary()->in_sync_replicas(1500), 1u);
    Client c(primary.port());
    EXPECT_EQ(c.cmd({"SET", "k", "v"}).rfind("-NOREPLICAS", 0), 0u);  // sync 2 with one distinct replica: refused
}

TEST(SyncReplication, InfoShowsThePolicyAndTheQuorumOffset) {
    Node primary(sync_primary(1));
    Node replica(replica_of(primary));
    ASSERT_TRUE(wait_until([&] { return online(replica); }));
    ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->in_sync_replicas(1500) == 1; }));
    Client c(primary.port());
    for (int i = 0; i < 10; ++i) c.cmd({"SET", "k" + std::to_string(i), "v"});
    const std::string info = c.cmd({"INFO"});
    EXPECT_NE(info.find("sync_replicas:1"), std::string::npos);
    EXPECT_NE(info.find("in_sync_replicas:1"), std::string::npos);
    EXPECT_NE(info.find("acked_by_quorum:10"), std::string::npos);
}
