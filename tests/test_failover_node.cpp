// Node-side failover mechanics (Phase 8, step 5): epochs, write leases, promotion, re-pointing,
// demotion, fencing. The coordinator (router, step 6) is played by the test, which sends the same
// control commands (ROLE / LEASE / PROMOTE / REPLICAOF) the router will.
//
// A "crash" is simulated by destroying the node object: its client port and replication port
// close, which is what a killed process does to its peers (the full process-kill version runs in
// the end-to-end failover tests of step 6).
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

NodeReplicationOptions primary_opts() {
    NodeReplicationOptions o;
    o.repl_port = 0;
    o.log_events = false;
    return o;
}
NodeReplicationOptions replica_opts(int primary_repl_port) {
    NodeReplicationOptions o;
    o.repl_port = 0;  // where it will serve replicas after a promotion
    o.replicaof_host = "127.0.0.1";
    o.replicaof_port = primary_repl_port;
    o.log_events = false;
    return o;
}
bool synced(Node& r) { return r.svc.replica() && r.svc.replica()->readable(); }
bool caught_up(Node& r, Node& p) {
    return r.svc.replica() && r.svc.replica()->status().applied >= p.svc.log().last_offset();
}
std::string field(const std::vector<std::string>& role, size_t i) { return i < role.size() ? role[i] : "?"; }

// ROLE reply layout: role, epoch, replid, offset, fenced, lease_ms, link, repl_port, lag, in_sync
enum { kRole, kEpoch, kReplid, kOffset, kFenced, kLease, kLink, kReplPort, kLag, kInSync };

struct Async {
    explicit Async(std::function<void()> fn) : th([this, fn] { fn(); done = true; }) {}
    ~Async() { if (th.joinable()) th.join(); }
    bool finished_within(int ms) { wait_until([&] { return done.load(); }, ms); return done.load(); }
    std::atomic<bool> done{false};
    std::thread th;
};

}  // namespace

// ---- leases ----------------------------------------------------------------------------------------

TEST(Failover, WithoutALeaseAWriteIsRefusedAndALeaseIsTrustedForLessThanItsLength) {
    auto o = primary_opts();
    o.require_lease = true;
    Node n(o);
    Client c(n.port());
    const std::string no_lease = "-READONLY No valid write lease (not granted or expired).";
    EXPECT_EQ(c.cmd({"SET", "k", "v"}), no_lease);          // fail closed from the first instant
    EXPECT_EQ(c.cmd({"GET", "k"}), "(nil)");                // reads are not affected
    EXPECT_EQ(c.cmd({"LEASE", "0", "1000"}), "+OK");
    EXPECT_EQ(c.cmd({"SET", "k", "v"}), "+OK");
    const long long rem = std::stoll(field(c.cmd_array({"ROLE"}), kLease));
    EXPECT_LE(rem, 950);  // 1000 ms granted, 5 % shaved off by the node
    EXPECT_GT(rem, 800);
    EXPECT_EQ(c.cmd({"LEASE", "7", "1000"}).rfind("-ERR EPOCH 0", 0), 0u);  // a lease for another epoch is not accepted
    EXPECT_EQ(c.cmd({"LEASE", "0", "300"}), "+OK");                          // renewal can also shorten
    ASSERT_TRUE(wait_until([&] { return c.cmd({"SET", "k", "v2"}) == no_lease; }, 2000)) << "the lease never lapsed";
    EXPECT_EQ(c.cmd({"LEASE", "0", "5000"}), "+OK");
    EXPECT_EQ(c.cmd({"SET", "k", "v3"}), "+OK");
    EXPECT_EQ(c.cmd({"LEASE", "0", "0"}), "+OK");  // revocation
    EXPECT_EQ(c.cmd({"SET", "k", "v4"}), no_lease);
}

TEST(Failover, AReplyHeldForReplicasIsNotAcknowledgedIfTheLeaseLapsedMeanwhile) {
    auto po = primary_opts();
    po.require_lease = true;
    po.sync_replicas = 1;
    po.sync_timeout_ms = 10000;
    Node primary(po);
    Node replica(replica_opts(primary.svc.repl_port()));
    ASSERT_TRUE(wait_until([&] { return synced(replica); }));
    ASSERT_TRUE(wait_until([&] { return primary.svc.primary()->in_sync_replicas(1500) == 1; }));
    Client admin(primary.port());

    // Control case: the lease is renewed before the acks arrive -> the write is acknowledged.
    ASSERT_EQ(admin.cmd({"LEASE", "0", "400"}), "+OK");
    replica.svc.replica()->set_apply_paused(true);
    std::string r1;
    Async a1([&] { Client c(primary.port()); r1 = c.cmd({"SET", "k1", "v"}); });
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    ASSERT_EQ(admin.cmd({"LEASE", "0", "5000"}), "+OK");
    replica.svc.replica()->set_apply_paused(false);
    ASSERT_TRUE(a1.finished_within(5000));
    EXPECT_EQ(r1, "+OK");

    // The real case: the acks only arrive after the lease has expired.
    ASSERT_EQ(admin.cmd({"LEASE", "0", "400"}), "+OK");
    replica.svc.replica()->set_apply_paused(true);
    std::string r2;
    Async a2([&] { Client c(primary.port()); r2 = c.cmd({"SET", "k2", "v"}); });
    ASSERT_TRUE(wait_until([&] { return primary.svc.lease_remaining_ms() <= 0; }, 3000));
    replica.svc.replica()->set_apply_paused(false);  // now the replica applies and acks...
    ASSERT_TRUE(a2.finished_within(5000));
    EXPECT_EQ(r2, "<closed>") << "...but the node must not acknowledge after its lease lapsed";
    EXPECT_EQ(primary.engine.get("k2"), std::optional<std::string>("v"));  // applied; the client just cannot know
}

// ---- promotion ---------------------------------------------------------------------------------------

TEST(Failover, AnUpToDateReplicaIsPromotedAndKeepsTheDataTtlsAndOffsets) {
    auto p = std::make_unique<Node>(primary_opts());
    Node r(replica_opts(p->svc.repl_port()));
    {
        Client c(p->port());
        for (int i = 0; i < 100; ++i) ASSERT_EQ(c.cmd({"SET", "k" + std::to_string(i), "v" + std::to_string(i)}), "+OK");
        ASSERT_EQ(c.cmd({"SET", "ttl", "x", "EX", "60"}), "+OK");
    }
    ASSERT_TRUE(wait_until([&] { return caught_up(r, *p); }));
    const uint64_t old_offset = p->svc.log().last_offset();
    const std::string old_replid = p->svc.log().replid();
    p.reset();  // the primary crashes

    Client cr(r.port());
    EXPECT_EQ(cr.cmd({"SET", "x", "1"}).rfind("-READONLY", 0), 0u);  // still a replica
    const std::string reply = cr.cmd({"PROMOTE", "1"});
    ASSERT_EQ(reply.rfind("+OK ", 0), 0u) << reply;
    EXPECT_NE(reply.find(std::to_string(old_offset)), std::string::npos) << "the promotion offset is where the old primary left off";

    auto role = cr.cmd_array({"ROLE"});
    EXPECT_EQ(field(role, kRole), "master");
    EXPECT_EQ(field(role, kEpoch), "1");
    EXPECT_NE(field(role, kReplid), old_replid);  // a new lineage
    EXPECT_EQ(field(role, kOffset), std::to_string(old_offset));

    EXPECT_EQ(cr.cmd({"GET", "k42"}), "v42");
    EXPECT_EQ(cr.cmd({"DBSIZE"}), ":101");
    const long long ttl = std::stoll(cr.cmd({"TTL", "ttl"}).substr(1));
    EXPECT_GT(ttl, 50);  // the absolute deadline survived the promotion
    EXPECT_LE(ttl, 60);
    EXPECT_EQ(cr.cmd({"SET", "after", "promotion"}), "+OK");  // writes accepted
    EXPECT_EQ(std::stoull(field(cr.cmd_array({"ROLE"}), kOffset)), old_offset + 1);  // numbering continues
}

TEST(Failover, PromotionIsRefusedWhenItWouldBeUnsafeOrMeaningless) {
    // A replica that never finished synchronising has no valid dataset to serve.
    {
        auto p = std::make_unique<Node>(primary_opts());
        for (int i = 0; i < 20000; ++i) p->engine.set("k" + std::to_string(i), "v");
        p->svc.primary()->faults().snapshot_pause_us = 2000;  // a ~0.6 s snapshot
        Node r(replica_opts(p->svc.repl_port()));
        Client c(r.port());
        ASSERT_TRUE(wait_until([&] { return r.svc.replica()->status().full_syncs >= 1; }));
        EXPECT_FALSE(synced(r));
        EXPECT_EQ(c.cmd({"PROMOTE", "1"}).rfind("-ERR not promotable", 0), 0u);
        ASSERT_TRUE(wait_until([&] { return synced(r); }, 15000));
        EXPECT_EQ(c.cmd({"PROMOTE", "1"}).rfind("+OK", 0), 0u);  // fine once it is synchronised
    }
    // The epoch must increase; a primary cannot be promoted; a node needs somewhere to serve replicas.
    {
        Node p(primary_opts());
        Node r(replica_opts(p.svc.repl_port()));
        ASSERT_TRUE(wait_until([&] { return synced(r); }));
        Client cp(p.port()), cr(r.port());
        EXPECT_EQ(cr.cmd({"PROMOTE", "0"}).rfind("-ERR epoch 0 is not higher", 0), 0u);
        EXPECT_EQ(cp.cmd({"PROMOTE", "5"}), "-ERR already a primary");
        auto no_port = replica_opts(p.svc.repl_port());
        no_port.repl_port = -1;
        Node r2(no_port);
        ASSERT_TRUE(wait_until([&] { return synced(r2); }));
        Client c2(r2.port());
        EXPECT_EQ(c2.cmd({"PROMOTE", "1"}).rfind("-ERR this node has no --repl-port", 0), 0u);
        EXPECT_EQ(c2.cmd({"SET", "x", "1"}).rfind("-READONLY", 0), 0u);  // refusal changed nothing
    }
}

TEST(Failover, AFreshlyPromotedNodeHasNoLeaseUntilTheCoordinatorGrantsOne) {
    auto p = std::make_unique<Node>(primary_opts());
    auto ro = replica_opts(p->svc.repl_port());
    ro.require_lease = true;
    Node r(ro);
    ASSERT_TRUE(wait_until([&] { return synced(r); }));
    p.reset();
    Client c(r.port());
    ASSERT_EQ(c.cmd({"PROMOTE", "1"}).rfind("+OK", 0), 0u);
    EXPECT_EQ(c.cmd({"SET", "k", "v"}), "-READONLY No valid write lease (not granted or expired).");
    EXPECT_EQ(c.cmd({"LEASE", "0", "1000"}).rfind("-ERR EPOCH 1", 0), 0u);  // a lease for the OLD epoch is useless
    EXPECT_EQ(c.cmd({"LEASE", "1", "1000"}), "+OK");
    EXPECT_EQ(c.cmd({"SET", "k", "v"}), "+OK");
}

// ---- following the new primary -----------------------------------------------------------------------------

TEST(Failover, AnotherReplicaFollowsTheNewPrimaryWithoutAFullResync) {
    auto p = std::make_unique<Node>(primary_opts());
    Node r1(replica_opts(p->svc.repl_port())), r2(replica_opts(p->svc.repl_port()));
    for (int i = 0; i < 200; ++i) p->engine.set("k" + std::to_string(i), "v");
    ASSERT_TRUE(wait_until([&] { return caught_up(r1, *p) && caught_up(r2, *p); }));
    p.reset();

    Client c1(r1.port()), c2(r2.port());
    const std::string reply = c1.cmd({"PROMOTE", "1"});
    ASSERT_EQ(reply.rfind("+OK ", 0), 0u) << reply;
    ASSERT_EQ(c2.cmd({"REPLICAOF", "127.0.0.1", std::to_string(r1.svc.repl_port()), "1"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return synced(r2) && r2.svc.replica()->status().state == ReplicationReplica::State::Online; }));
    const auto st = r2.svc.replica()->status();
    EXPECT_EQ(st.partial_syncs, 1u);  // it continued from its position: it was a prefix of the new primary's history
    EXPECT_EQ(st.full_syncs, 0u);
    EXPECT_EQ(st.replid, r1.svc.log().replid());  // and adopted the new lineage

    ASSERT_EQ(c1.cmd({"SET", "new", "write"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return c2.cmd({"GET", "new"}) == "write"; }));
    EXPECT_EQ(c2.cmd({"DBSIZE"}), ":201");
}

TEST(Failover, AReplicaAheadOfTheNewPrimaryIsFullyResynchronisedAndLosesTheExtraWrites) {
    auto p = std::make_unique<Node>(primary_opts());
    Node behind(replica_opts(p->svc.repl_port())), ahead(replica_opts(p->svc.repl_port()));
    for (int i = 0; i < 100; ++i) p->engine.set("base" + std::to_string(i), "v");
    ASSERT_TRUE(wait_until([&] { return caught_up(behind, *p) && caught_up(ahead, *p); }));
    behind.svc.replica()->set_apply_paused(true);                      // `behind` stops receiving...
    for (int i = 0; i < 50; ++i) p->engine.set("extra" + std::to_string(i), "v");  // ...while `ahead` gets 50 more
    ASSERT_TRUE(wait_until([&] { return caught_up(ahead, *p); }));
    p.reset();

    Client cb(behind.port()), ca(ahead.port());
    ASSERT_EQ(cb.cmd({"PROMOTE", "1"}).rfind("+OK ", 0), 0u);  // the stale one becomes primary: no extra* there
    EXPECT_EQ(cb.cmd({"GET", "extra1"}), "(nil)");
    // The new primary keeps taking writes BEFORE the other replica attaches, so that its offsets
    // 101..180 now hold different records than the ones `ahead` applied at offsets 101..150. A
    // naive "continue from my offset" would splice the two histories together silently.
    for (int i = 0; i < 80; ++i) ASSERT_EQ(cb.cmd({"SET", "new" + std::to_string(i), "v"}), "+OK");
    ASSERT_EQ(ca.cmd({"REPLICAOF", "127.0.0.1", std::to_string(behind.svc.repl_port()), "1"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return synced(ahead) && ahead.svc.replica()->status().applied >= behind.svc.log().last_offset(); }, 15000));
    const auto st = ahead.svc.replica()->status();
    EXPECT_EQ(st.full_syncs, 1u);     // it had records the new primary lacks: it must start over
    EXPECT_EQ(st.partial_syncs, 0u);
    EXPECT_EQ(ca.cmd({"GET", "extra1"}), "(nil)");  // the divergent suffix is gone
    EXPECT_EQ(ca.cmd({"GET", "new79"}), "v");       // and the new primary's history is complete
    EXPECT_EQ(ca.cmd({"DBSIZE"}), ":180");
    EXPECT_EQ(cb.cmd({"DBSIZE"}), ":180");
}

// ---- the old primary comes back ------------------------------------------------------------------------------------

TEST(Failover, ADemotedPrimaryDiscardsItsUnreplicatedWritesAndCannotWriteAnymore) {
    Node old_primary(primary_opts());
    Node r(replica_opts(old_primary.svc.repl_port()));
    Client c_old(old_primary.port());
    for (int i = 0; i < 50; ++i) ASSERT_EQ(c_old.cmd({"SET", "base" + std::to_string(i), "v"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return caught_up(r, old_primary); }));

    // The old primary is cut off from the replica but is still alive and still takes writes.
    r.svc.replica()->set_reconnect_hold(true);
    r.svc.replica()->drop_link();
    ASSERT_TRUE(wait_until([&] { return r.svc.replica()->status().state == ReplicationReplica::State::Disconnected; }));
    for (int i = 0; i < 30; ++i) ASSERT_EQ(c_old.cmd({"SET", "unreplicated" + std::to_string(i), "v"}), "+OK");  // acknowledged (async!) and doomed

    // Failover: the replica is promoted. The old primary does not know yet.
    Client c_new(r.port());
    const std::string reply = c_new.cmd({"PROMOTE", "1"});
    ASSERT_EQ(reply.rfind("+OK ", 0), 0u) << reply;
    ASSERT_EQ(c_new.cmd({"SET", "newer", "value"}), "+OK");
    EXPECT_EQ(c_old.cmd({"SET", "split-brain", "1"}), "+OK");  // two primaries exist right now: the window the lease closes

    // The old primary is told to follow the new one (what the coordinator does when it comes back).
    ASSERT_EQ(c_old.cmd({"REPLICAOF", "127.0.0.1", std::to_string(r.svc.repl_port()), "1"}), "+OK");
    EXPECT_EQ(c_old.cmd({"SET", "late", "1"}).rfind("-READONLY", 0), 0u);  // an existing connection can no longer write
    ASSERT_TRUE(wait_until([&] { return synced(old_primary); }, 15000));
    ASSERT_TRUE(wait_until([&] { return old_primary.svc.replica()->status().applied >= r.svc.log().last_offset(); }));

    EXPECT_EQ(old_primary.svc.replica()->status().full_syncs, 1u);  // always a full resync
    EXPECT_EQ(c_old.cmd({"GET", "unreplicated3"}), "(nil)");       // the doomed writes are gone...
    EXPECT_EQ(c_old.cmd({"GET", "split-brain"}), "(nil)");
    EXPECT_EQ(c_old.cmd({"GET", "newer"}), "value");               // ...and it has the new primary's data
    EXPECT_EQ(c_old.cmd({"DBSIZE"}), ":51");                       // 50 base + newer
    auto role = c_old.cmd_array({"ROLE"});
    EXPECT_EQ(field(role, kRole), "slave");
    EXPECT_EQ(field(role, kEpoch), "1");
    ASSERT_EQ(c_new.cmd({"SET", "after", "demotion"}), "+OK");  // it follows the new primary from now on
    ASSERT_TRUE(wait_until([&] { return c_old.cmd({"GET", "after"}) == "demotion"; }));
}

TEST(Failover, ARepointRequestWithAnOlderEpochIsRefused) {
    Node p(primary_opts());
    Node r(replica_opts(p.svc.repl_port()));
    ASSERT_TRUE(wait_until([&] { return synced(r); }));
    Client c(r.port());
    ASSERT_EQ(c.cmd({"REPLICAOF", "127.0.0.1", std::to_string(p.svc.repl_port()), "3"}), "+OK");  // epoch 3 accepted
    EXPECT_EQ(c.cmd({"REPLICAOF", "127.0.0.1", std::to_string(p.svc.repl_port()), "2"}).rfind("-ERR epoch 2 is older than 3", 0), 0u);
    EXPECT_EQ(field(c.cmd_array({"ROLE"}), kEpoch), "3");
}

// ---- split brain: the data plane fences a stale primary --------------------------------------------------------------------

TEST(Failover, AReplicaThatHasSeenANewerEpochFencesAStalePrimary) {
    Node stale_primary(primary_opts());  // epoch 0, does not know it was replaced
    Client c(stale_primary.port());
    ASSERT_EQ(c.cmd({"SET", "k", "v"}), "+OK");

    // A replica that already follows epoch 1 (it was re-pointed to the new primary) is misdirected to the old one.
    auto xo = replica_opts(stale_primary.svc.repl_port());
    xo.initial_epoch = 1;
    Node x(xo);
    ASSERT_TRUE(wait_until([&] { return stale_primary.svc.fenced(); })) << "the stale primary was not fenced";

    EXPECT_EQ(c.cmd({"SET", "k2", "v"}), "-READONLY This node was fenced by a newer epoch and waits to be demoted.");
    EXPECT_EQ(field(c.cmd_array({"ROLE"}), kFenced), "1");
    EXPECT_EQ(c.cmd({"LEASE", "0", "1000"}).rfind("-ERR FENCED", 0), 0u);  // a fenced node takes no lease
    EXPECT_EQ(c.cmd({"GET", "k"}), "v");                                    // reads continue
    EXPECT_FALSE(x.svc.replica()->readable()) << "the replica must not load data from a primary of an older epoch";
    EXPECT_EQ(x.engine.size(), 0u);

    // The coordinator then demotes it for good.
    Node real_primary(primary_opts());
    ASSERT_EQ(c.cmd({"REPLICAOF", "127.0.0.1", std::to_string(real_primary.svc.repl_port()), "1"}), "+OK");
    EXPECT_FALSE(stale_primary.svc.fenced());  // fencing is cleared by the demotion; it is a replica now
    EXPECT_EQ(c.cmd({"SET", "k3", "v"}).rfind("-READONLY You can't write", 0), 0u);
}

TEST(Failover, InfoAndRoleReportTheTransitions) {
    auto p = std::make_unique<Node>(primary_opts());
    Node r(replica_opts(p->svc.repl_port()));
    ASSERT_TRUE(wait_until([&] { return synced(r); }));
    p.reset();
    Client c(r.port());
    ASSERT_EQ(c.cmd({"PROMOTE", "4"}).rfind("+OK", 0), 0u);
    const std::string info = c.cmd({"INFO"});
    EXPECT_NE(info.find("epoch:4"), std::string::npos);
    EXPECT_NE(info.find("role:master"), std::string::npos);
    EXPECT_NE(info.find("role_transitions:1"), std::string::npos);
    EXPECT_NE(info.find("fenced:no"), std::string::npos);
}
