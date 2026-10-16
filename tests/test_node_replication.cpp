// Replication inside real server nodes (Phase 8, step 3): client port + replication port, the
// READONLY / LOADING gates, and the INFO replication section.
#include <gtest/gtest.h>

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

bool wait_until(const std::function<bool()>& pred, int timeout_ms = 8000) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < end) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return pred();
}

// A node as main.cpp assembles it (without persistence).
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
    return o;
}
NodeReplicationOptions replica_opts(int repl_port) {
    NodeReplicationOptions o;
    o.replicaof_host = "127.0.0.1";
    o.replicaof_port = repl_port;
    return o;
}

// Value of "name:<number>" in an INFO text; -1 if absent.
long long info_num(const std::string& info, const std::string& name) {
    const size_t p = info.find(name + ":");
    if (p == std::string::npos) return -1;
    return std::atoll(info.c_str() + p + name.size() + 1);
}
// Value of "key=<number>" inside the replica0 line of a primary's INFO.
long long replica_field(const std::string& info, const std::string& key) {
    const size_t line = info.find("replica0:");
    if (line == std::string::npos) return -1;
    const size_t p = info.find(key + "=", line);
    if (p == std::string::npos) return -1;
    return std::atoll(info.c_str() + p + key.size() + 1);
}

}  // namespace

TEST(NodeReplication, AWriteToThePrimaryIsReadableFromTheReplicaOverTheClientPort) {
    Node primary(primary_opts());
    Node replica(replica_opts(primary.svc.repl_port()));
    Client cp(primary.port()), cr(replica.port());
    ASSERT_TRUE(cp.ok() && cr.ok());
    ASSERT_TRUE(wait_until([&] { return replica.svc.replica()->status().state == ReplicationReplica::State::Online; }));

    EXPECT_EQ(cp.cmd({"SET", "name", "Naseem"}), "+OK");
    EXPECT_EQ(cp.cmd({"SET", "temp", "1", "EX", "100"}), "+OK");
    ASSERT_TRUE(wait_until([&] { return cr.cmd({"GET", "name"}) == "Naseem"; }));
    EXPECT_EQ(cr.cmd({"GET", "temp"}), "1");
    const long long ttl = std::stoll(cr.cmd({"TTL", "temp"}).substr(1));
    EXPECT_GT(ttl, 90);
    EXPECT_EQ(cp.cmd({"DEL", "name"}), ":1");
    ASSERT_TRUE(wait_until([&] { return cr.cmd({"GET", "name"}) == "(nil)"; }));
    EXPECT_EQ(cr.cmd({"DBSIZE"}), ":1");
}

TEST(NodeReplication, AReplicaRefusesEveryWriteButStillAnswersReadsAndAdminCommands) {
    Node primary(primary_opts());
    Node replica(replica_opts(primary.svc.repl_port()));
    Client cp(primary.port()), cr(replica.port());
    cp.cmd({"SET", "k", "v"});
    ASSERT_TRUE(wait_until([&] { return cr.cmd({"GET", "k"}) == "v"; }));

    const std::string ro = "-READONLY You can't write against a read only replica.";
    EXPECT_EQ(cr.cmd({"SET", "k", "other"}), ro);
    EXPECT_EQ(cr.cmd({"DEL", "k"}), ro);
    EXPECT_EQ(cr.cmd({"EXPIRE", "k", "10"}), ro);
    EXPECT_EQ(cr.cmd({"PEXPIRE", "k", "10"}), ro);
    EXPECT_EQ(cr.cmd({"PERSIST", "k"}), ro);
    EXPECT_EQ(cr.cmd({"GET", "k"}), "v");  // nothing changed
    EXPECT_EQ(cr.cmd({"PING"}), "+PONG");
    EXPECT_EQ(cr.cmd_array({"KEYS", "*"}), std::vector<std::string>({"k"}));  // reads are allowed
    // a pipelined batch mixing allowed and refused commands keeps its reply order
    cr.send_raw(Client::encode({"GET", "k"}) + Client::encode({"SET", "x", "1"}) + Client::encode({"PING"}));
    EXPECT_EQ(cr.read_reply(), "v");
    EXPECT_EQ(cr.read_reply(), ro);
    EXPECT_EQ(cr.read_reply(), "+PONG");
}

TEST(NodeReplication, AReplicaAnswersLoadingWhileItsSnapshotIsBeingInstalled) {
    Node primary(primary_opts());
    for (int i = 0; i < 20000; ++i) primary.engine.set("k" + std::to_string(i), "v");
    primary.svc.primary()->faults().snapshot_pause_us = 2000;  // ~0.6 s of snapshot
    Node replica(replica_opts(primary.svc.repl_port()));
    Client cr(replica.port());
    bool saw_loading = false;
    std::string last;
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (std::chrono::steady_clock::now() < end) {
        last = cr.cmd({"GET", "k1"});
        if (last.rfind("-LOADING", 0) == 0) saw_loading = true;
        else if (last == "v") break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_TRUE(saw_loading) << "never saw -LOADING";
    EXPECT_EQ(last, "v");
    EXPECT_EQ(cr.cmd({"DBSIZE"}), ":20000");
}

TEST(NodeReplication, InfoShowsRolesOffsetsAndLagOnBothSides) {
    Node primary(primary_opts());
    Node replica(replica_opts(primary.svc.repl_port()));
    Client cp(primary.port()), cr(replica.port());
    ASSERT_TRUE(wait_until([&] { return replica.svc.replica()->status().state == ReplicationReplica::State::Online; }));
    for (int i = 0; i < 100; ++i) cp.cmd({"SET", "k" + std::to_string(i), "v"});
    ASSERT_TRUE(wait_until([&] { return info_num(cr.cmd({"INFO"}), "repl_offset") == 100; }));

    std::string pi = cp.cmd({"INFO"}), ri = cr.cmd({"INFO"});
    EXPECT_NE(pi.find("role:master"), std::string::npos);
    EXPECT_EQ(info_num(pi, "repl_offset"), 100);
    EXPECT_EQ(info_num(pi, "connected_replicas"), 1);
    EXPECT_NE(pi.find("replica0:addr="), std::string::npos);
    EXPECT_NE(pi.find("state=streaming"), std::string::npos);
    EXPECT_NE(ri.find("role:slave"), std::string::npos);
    EXPECT_NE(ri.find("master_link_status:up"), std::string::npos);
    EXPECT_NE(ri.find("replica_state:online"), std::string::npos);
    EXPECT_EQ(info_num(ri, "lag_records"), 0);
    EXPECT_EQ(info_num(ri, "full_syncs"), 1);

    // acks flow back: the primary learns the replica's position
    ASSERT_TRUE(wait_until([&] { return replica_field(cp.cmd({"INFO"}), "acked") == 100; }));
    EXPECT_EQ(replica_field(cp.cmd({"INFO"}), "lag_records"), 0);

    // a replica that stops applying: the PRIMARY reports the growing lag
    replica.svc.replica()->set_apply_paused(true);
    for (int i = 0; i < 500; ++i) cp.cmd({"SET", "more" + std::to_string(i), "v"});
    ASSERT_TRUE(wait_until([&] { return replica_field(cp.cmd({"INFO"}), "lag_records") >= 400; }));
    pi = cp.cmd({"INFO"});
    EXPECT_EQ(info_num(pi, "repl_offset"), 600);
    EXPECT_GT(replica_field(pi, "ack_age_ms"), -1);

    // ... and once it resumes, both sides report zero again
    replica.svc.replica()->set_apply_paused(false);
    ASSERT_TRUE(wait_until([&] { return info_num(cr.cmd({"INFO"}), "repl_offset") == 600; }));
    ASSERT_TRUE(wait_until([&] { return replica_field(cp.cmd({"INFO"}), "lag_records") == 0; }));
    EXPECT_EQ(info_num(cr.cmd({"INFO"}), "lag_ms"), 0);
}

TEST(NodeReplication, ANodeWithoutReplicationOptionsKeepsTheOriginalBehaviour) {
    ShardedStore engine(4);
    Server server(engine, ServerOptions{0, 2, nullptr});  // exactly how Phases 1-7 start
    std::string err;
    ASSERT_TRUE(server.start(&err)) << err;
    Client c(server.port());
    EXPECT_EQ(c.cmd({"SET", "a", "1"}), "+OK");
    EXPECT_EQ(c.cmd({"GET", "a"}), "1");
    EXPECT_EQ(c.cmd({"INFO"}).find("# Replication"), std::string::npos);
    server.stop();
}
