// Router end-to-end: real nodes (Server + ShardedStore on ephemeral ports), a real Router in
// front of them, real TCP clients. Covers placement, pipelining across nodes, multi-node
// commands, failure of one node, and live rebalancing (with and without key migration),
// including migration while clients keep reading and writing.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "hash.hpp"
#include "router.hpp"
#include "server.hpp"
#include "synced_store.hpp"
#include "test_client.hpp"

namespace {

struct TestNode {
    std::string name;
    std::unique_ptr<ShardedStore> engine;
    std::unique_ptr<Server> server;
    int port() const { return server->port(); }
};

std::unique_ptr<TestNode> make_node(const std::string& name) {
    auto n = std::make_unique<TestNode>();
    n->name = name;
    n->engine = std::make_unique<ShardedStore>(8);
    n->server = std::make_unique<Server>(*n->engine, ServerOptions{0, 2});
    std::string err;
    EXPECT_TRUE(n->server->start(&err)) << err;
    return n;
}

class Cluster {
public:
    explicit Cluster(int nodes, RouterOptions ro = [] { RouterOptions o; o.port = 0; o.io_timeout_ms = 2000; return o; }()) {
        for (int i = 0; i < nodes; ++i) nodes_.push_back(make_node("n" + std::to_string(i)));
        std::vector<RouterNode> rn;
        for (auto& n : nodes_) rn.push_back({n->name, "127.0.0.1", n->port(), 1});
        router_ = std::make_unique<Router>(rn, ro);
        std::string err;
        EXPECT_TRUE(router_->start(&err)) << err;
    }
    ~Cluster() {
        router_->stop();
        for (auto& n : nodes_) n->server->stop();
    }
    TestNode& node(size_t i) { return *nodes_[i]; }
    TestNode* by_name(const std::string& name) {
        for (auto& n : nodes_) if (n->name == name) return n.get();
        return nullptr;
    }
    TestNode& add_node(const std::string& name) {
        nodes_.push_back(make_node(name));
        return *nodes_.back();
    }
    Router& router() { return *router_; }
    int rport() const { return router_->port(); }
    size_t size() const { return nodes_.size(); }

private:
    std::vector<std::unique_ptr<TestNode>> nodes_;
    std::unique_ptr<Router> router_;
};

// How many of `keys` are present directly on `node` (bypassing the router).
size_t present_on(TestNode& node, const std::vector<std::string>& keys) {
    Client c(node.port());
    size_t n = 0;
    for (const auto& k : keys) if (c.cmd({"GET", k}) != "(nil)") ++n;
    return n;
}

std::string key_of(int i) { return "key:" + std::to_string(i); }

}  // namespace

TEST(Router, EachKeyIsStoredOnItsRingOwnerAndNowhereElse) {
    Cluster cl(3);
    Client c(cl.rport());
    ASSERT_TRUE(c.ok());
    constexpr int kKeys = 600;
    for (int i = 0; i < kKeys; ++i) ASSERT_EQ(c.cmd({"SET", key_of(i), "v" + std::to_string(i)}), "+OK");
    for (int i = 0; i < kKeys; ++i) ASSERT_EQ(c.cmd({"GET", key_of(i)}), "v" + std::to_string(i));

    std::map<std::string, int> per_node;
    for (int i = 0; i < kKeys; ++i) {
        const std::string owner = cl.router().owner_of(key_of(i));
        ++per_node[owner];
        for (size_t n = 0; n < cl.size(); ++n) {
            Client direct(cl.node(n).port());
            const std::string got = direct.cmd({"GET", key_of(i)});
            if (cl.node(n).name == owner) ASSERT_EQ(got, "v" + std::to_string(i)) << key_of(i);
            else ASSERT_EQ(got, "(nil)") << key_of(i) << " leaked to " << cl.node(n).name;
        }
    }
    EXPECT_EQ(per_node.size(), 3u);                       // all three nodes got keys
    for (auto& kv : per_node) EXPECT_GT(kv.second, 100);  // roughly a third each
}

TEST(Router, PipelinedBatchSpanningNodesKeepsReplyOrder) {
    Cluster cl(3);
    Client c(cl.rport());
    std::string batch;
    std::vector<std::string> expect;
    for (int i = 0; i < 300; ++i) {
        const std::string k = key_of(i), v = "val" + std::to_string(i);
        batch += Client::encode({"SET", k, v});
        expect.push_back("+OK");
        batch += Client::encode({"GET", k});
        expect.push_back(v);
        if (i % 7 == 0) { batch += Client::encode({"PING"}); expect.push_back("+PONG"); }
        if (i % 11 == 0) { batch += Client::encode({"DEL", k}); expect.push_back(":1"); }
        if (i % 11 == 0) { batch += Client::encode({"GET", k}); expect.push_back("(nil)"); }
    }
    c.send_raw(batch);  // one write: the router sees it as one pipeline
    for (size_t i = 0; i < expect.size(); ++i) ASSERT_EQ(c.read_reply(), expect[i]) << "reply #" << i;
}

TEST(Router, MultiNodeCommands) {
    Cluster cl(3);
    Client c(cl.rport());
    for (int i = 0; i < 90; ++i) ASSERT_EQ(c.cmd({"SET", key_of(i), "x"}), "+OK");
    EXPECT_EQ(c.cmd({"DBSIZE"}), ":90");
    auto keys = c.cmd_array({"KEYS", "key:*"});
    EXPECT_EQ(keys.size(), 90u);
    EXPECT_EQ(std::set<std::string>(keys.begin(), keys.end()).size(), 90u);
    EXPECT_EQ(c.cmd_array({"KEYS", "key:1?"}).size(), 10u);  // key:10 .. key:19
    // one DEL naming keys on different nodes
    EXPECT_EQ(c.cmd({"DEL", key_of(1), key_of(2), key_of(3), key_of(4), "no-such-key"}), ":4");
    EXPECT_EQ(c.cmd({"DBSIZE"}), ":86");
    EXPECT_EQ(c.cmd({"PING"}), "+PONG");
    EXPECT_EQ(c.cmd({"ECHO", "hi"}), "hi");
}

TEST(Router, TtlCommandsFollowTheKey) {
    Cluster cl(3);
    Client c(cl.rport());
    for (int i = 0; i < 30; ++i) {
        const std::string k = "ttl:" + std::to_string(i);
        ASSERT_EQ(c.cmd({"SET", k, "v", "EX", "100"}), "+OK");
        const long long ttl = std::stoll(c.cmd({"TTL", k}).substr(1));
        EXPECT_GT(ttl, 90);
        EXPECT_LE(ttl, 100);
        EXPECT_EQ(c.cmd({"PERSIST", k}), ":1");
        EXPECT_EQ(c.cmd({"TTL", k}), ":-1");
        EXPECT_EQ(c.cmd({"PEXPIRE", k, "50000"}), ":1");
        const long long pttl = std::stoll(c.cmd({"PTTL", k}).substr(1));
        EXPECT_GT(pttl, 40000);
    }
    EXPECT_EQ(c.cmd({"TTL", "never-set"}), ":-2");
}

TEST(Router, HashTagKeepsRelatedKeysTogether) {
    Cluster cl(4);
    Client c(cl.rport());
    for (int u = 0; u < 40; ++u) {
        const std::string tag = "{u" + std::to_string(u) + "}";
        const std::string a = tag + ":profile", b = tag + ":cart";
        ASSERT_EQ(c.cmd({"SET", a, "1"}), "+OK");
        ASSERT_EQ(c.cmd({"SET", b, "2"}), "+OK");
        EXPECT_EQ(cl.router().owner_of(a), cl.router().owner_of(b));
    }
}

TEST(Router, ArgumentErrorsAndUnknownCommandsAreAnsweredLocally) {
    Cluster cl(2);
    Client c(cl.rport());
    EXPECT_EQ(c.cmd({"GET"}), "-ERR wrong number of arguments for 'get' command");
    EXPECT_EQ(c.cmd({"SET", "k"}), "-ERR wrong number of arguments for 'set' command");
    EXPECT_EQ(c.cmd({"NOPE"}), "-ERR unknown command 'NOPE'");
    EXPECT_EQ(c.cmd({"SET", "k", "v", "BOGUS"}), "-ERR syntax error");  // validated by the node
    EXPECT_EQ(c.cmd({"PING"}), "+PONG");
}

TEST(Router, RingInfoAndLookup) {
    Cluster cl(3);
    Client c(cl.rport());
    const std::string info = c.cmd({"RING", "INFO"});
    EXPECT_NE(info.find("nodes:3"), std::string::npos);
    EXPECT_NE(info.find("node:n0 "), std::string::npos);
    EXPECT_NE(info.find("migrating:no"), std::string::npos);
    for (int i = 0; i < 20; ++i)
        EXPECT_EQ(c.cmd({"RING", "LOOKUP", key_of(i)}), cl.router().owner_of(key_of(i)));
}

TEST(Router, AnUnreachableNodeFailsOnlyItsOwnKeys) {
    Cluster cl(3);
    Client c(cl.rport());
    for (int i = 0; i < 90; ++i) ASSERT_EQ(c.cmd({"SET", key_of(i), "v"}), "+OK");
    cl.node(1).server->stop();  // node n1 goes away
    int errors = 0, ok = 0;
    for (int i = 0; i < 90; ++i) {
        const std::string r = c.cmd({"GET", key_of(i)});
        if (cl.router().owner_of(key_of(i)) == "n1") {
            EXPECT_EQ(r.rfind("-ERR node 'n1' unreachable", 0), 0u) << r;
            ++errors;
        } else {
            EXPECT_EQ(r, "v");
            ++ok;
        }
    }
    EXPECT_GT(errors, 0);
    EXPECT_GT(ok, 0);
    // a pipelined batch mixing live and dead nodes: replies still line up
    std::string batch;
    for (int i = 0; i < 40; ++i) batch += Client::encode({"GET", key_of(i)});
    c.send_raw(batch);
    for (int i = 0; i < 40; ++i) {
        const std::string r = c.read_reply();
        if (cl.router().owner_of(key_of(i)) == "n1") EXPECT_EQ(r.rfind("-ERR node 'n1'", 0), 0u);
        else EXPECT_EQ(r, "v");
    }
}

// ---- rebalancing ------------------------------------------------------------------------------

TEST(RouterRebalance, AddingANodeWithoutMigrationLosesAboutOneNthOfTheKeys) {
    Cluster cl(3);
    Client c(cl.rport());
    constexpr int kKeys = 3000;
    for (int i = 0; i < kKeys; ++i) ASSERT_EQ(c.cmd({"SET", key_of(i), "v"}), "+OK");

    TestNode& fresh = cl.add_node("n3");
    ASSERT_EQ(c.cmd({"RING", "ADD", "n3", "127.0.0.1", std::to_string(fresh.port())}), "+OK");

    int hits = 0;
    for (int i = 0; i < kKeys; ++i) if (c.cmd({"GET", key_of(i)}) == "v") ++hits;
    const double lost = 1.0 - static_cast<double>(hits) / kKeys;
    // ~1/4 of the keys now belong to n3, which has none of them: they miss. Not ~3/4.
    EXPECT_GT(lost, 0.25 * 0.7);
    EXPECT_LT(lost, 0.25 * 1.3);
    // and the remaining three quarters are still served from their old nodes untouched
    EXPECT_EQ(present_on(fresh, [&] { std::vector<std::string> v; for (int i = 0; i < kKeys; ++i) v.push_back(key_of(i)); return v; }()), 0u);
}

TEST(RouterRebalance, AddingANodeWithMigrationKeepsEveryKeyAndMovesOnlyOneNth) {
    Cluster cl(3);
    Client c(cl.rport());
    constexpr int kKeys = 3000;
    std::vector<std::string> keys;
    for (int i = 0; i < kKeys; ++i) {
        keys.push_back(key_of(i));
        // every 5th key carries a TTL that must survive the move
        if (i % 5 == 0) ASSERT_EQ(c.cmd({"SET", key_of(i), "v" + std::to_string(i), "EX", "500"}), "+OK");
        else ASSERT_EQ(c.cmd({"SET", key_of(i), "v" + std::to_string(i)}), "+OK");
    }
    TestNode& fresh = cl.add_node("n3");
    const std::string reply = c.cmd({"RING", "ADD", "n3", "127.0.0.1", std::to_string(fresh.port()), "MIGRATE"});
    ASSERT_EQ(reply.rfind("+OK", 0), 0u) << reply;
    EXPECT_NE(reply.find("errors=0"), std::string::npos) << reply;

    for (int i = 0; i < kKeys; ++i) ASSERT_EQ(c.cmd({"GET", key_of(i)}), "v" + std::to_string(i)) << key_of(i);
    EXPECT_EQ(c.cmd({"DBSIZE"}), ":" + std::to_string(kKeys));  // nothing duplicated, nothing lost

    // placement is exactly the new ring's: each key lives on its owner and nowhere else
    for (int i = 0; i < kKeys; i += 7) {
        const std::string owner = cl.router().owner_of(key_of(i));
        for (size_t n = 0; n < cl.size(); ++n) {
            Client d(cl.node(n).port());
            const std::string got = d.cmd({"GET", key_of(i)});
            if (cl.node(n).name == owner) ASSERT_EQ(got, "v" + std::to_string(i));
            else ASSERT_EQ(got, "(nil)") << key_of(i) << " is also on " << cl.node(n).name;
        }
    }
    const size_t on_new = present_on(fresh, keys);
    EXPECT_GT(on_new, kKeys * 0.25 * 0.7);
    EXPECT_LT(on_new, kKeys * 0.25 * 1.3);
    // TTLs survived (moved keys included): still about 500 s, never "no expiry" or expired
    int checked = 0;
    for (int i = 0; i < kKeys; i += 5) {
        const long long ttl = std::stoll(c.cmd({"TTL", key_of(i)}).substr(1));
        ASSERT_GT(ttl, 400) << key_of(i);
        ASSERT_LE(ttl, 500);
        ++checked;
    }
    EXPECT_EQ(checked, kKeys / 5);
    EXPECT_NE(c.cmd({"RING", "INFO"}).find("migrating:no"), std::string::npos);
}

TEST(RouterRebalance, RemovingANodeWithMigrationEmptiesItAndKeepsEveryKey) {
    Cluster cl(4);
    Client c(cl.rport());
    constexpr int kKeys = 2000;
    for (int i = 0; i < kKeys; ++i) ASSERT_EQ(c.cmd({"SET", key_of(i), "v" + std::to_string(i)}), "+OK");
    const std::string reply = c.cmd({"RING", "REMOVE", "n2", "MIGRATE"});
    ASSERT_EQ(reply.rfind("+OK", 0), 0u) << reply;
    EXPECT_NE(reply.find("errors=0"), std::string::npos) << reply;
    for (int i = 0; i < kKeys; ++i) ASSERT_EQ(c.cmd({"GET", key_of(i)}), "v" + std::to_string(i));
    Client direct(cl.node(2).port());
    EXPECT_EQ(direct.cmd({"DBSIZE"}), ":0");
    EXPECT_EQ(c.cmd({"DBSIZE"}), ":" + std::to_string(kKeys));
    EXPECT_EQ(c.cmd({"RING", "REMOVE", "n2"}).rfind("-ERR", 0), 0u);  // already gone
}

TEST(RouterRebalance, RejectsBadTopologyCommands) {
    Cluster cl(2);
    Client c(cl.rport());
    EXPECT_EQ(c.cmd({"RING", "ADD", "n0", "127.0.0.1", "1234"}).rfind("-ERR", 0), 0u);   // duplicate name
    EXPECT_EQ(c.cmd({"RING", "ADD", "x", "127.0.0.1", "0"}).rfind("-ERR", 0), 0u);       // bad port
    EXPECT_EQ(c.cmd({"RING", "REMOVE", "nope"}).rfind("-ERR", 0), 0u);
    EXPECT_EQ(c.cmd({"RING", "WHAT"}).rfind("-ERR", 0), 0u);
}

// The important one: clients keep reading and writing while nodes are added and removed
// with migration. Each client thread owns a disjoint set of keys and checks every reply against
// its own model, so any stale read, lost write or resurrected delete is caught immediately,
// including the ones that happen in the middle of a migration step.
TEST(RouterRebalance, ClientsSeeConsistentDataWhileNodesAreAddedAndRemoved) {
    Cluster cl(3);
    {
        Client c(cl.rport());
        for (int i = 0; i < 4000; ++i) ASSERT_EQ(c.cmd({"SET", key_of(i), "init"}), "+OK");
    }
    constexpr int kThreads = 4;
    std::atomic<bool> stop{false};
    std::atomic<int> failures{0};
    std::mutex msg_mu;
    std::string first_failure;
    auto fail = [&](const std::string& m) {
        if (failures.fetch_add(1) == 0) {
            std::lock_guard<std::mutex> lock(msg_mu);
            first_failure = m;
        }
    };
    std::vector<std::map<int, std::optional<std::string>>> models(kThreads);
    std::vector<std::thread> workers;
    std::atomic<long> ops{0};
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t] {
            Client c(cl.rport());
            if (!c.ok()) { fail("connect"); return; }
            std::mt19937 rng(1234 + t);
            auto& model = models[t];
            // thread t owns keys i with i % kThreads == t
            for (int i = t; i < 4000; i += kThreads) model[i] = "init";
            std::vector<int> mine;
            for (auto& kv : model) mine.push_back(kv.first);
            int counter = 0;
            while (!stop) {
                const int k = mine[rng() % mine.size()];
                const std::string key = key_of(k);
                const int op = static_cast<int>(rng() % 10);
                if (op < 4) {  // SET
                    const std::string v = "t" + std::to_string(t) + "-" + std::to_string(++counter);
                    if (c.cmd({"SET", key, v}) != "+OK") { fail("SET " + key); return; }
                    model[k] = v;
                } else if (op < 5) {  // DEL
                    const std::string r = c.cmd({"DEL", key});
                    const std::string want = model[k] ? ":1" : ":0";
                    if (r != want) { fail("DEL " + key + " got " + r + " want " + want); return; }
                    model[k] = std::nullopt;
                } else {  // GET
                    const std::string r = c.cmd({"GET", key});
                    const std::string want = model[k] ? *model[k] : "(nil)";
                    if (r != want) { fail("GET " + key + " got '" + r + "' want '" + want + "'"); return; }
                }
                ops.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // topology changes while the clients run
    Client admin(cl.rport());
    ASSERT_TRUE(admin.ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    TestNode& n3 = cl.add_node("n3");
    std::string r = admin.cmd({"RING", "ADD", "n3", "127.0.0.1", std::to_string(n3.port()), "MIGRATE"});
    EXPECT_EQ(r.rfind("+OK", 0), 0u) << r;
    EXPECT_NE(r.find("errors=0"), std::string::npos) << r;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    TestNode& n4 = cl.add_node("n4");
    r = admin.cmd({"RING", "ADD", "n4", "127.0.0.1", std::to_string(n4.port()), "MIGRATE"});
    EXPECT_EQ(r.rfind("+OK", 0), 0u) << r;
    EXPECT_NE(r.find("errors=0"), std::string::npos) << r;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    r = admin.cmd({"RING", "REMOVE", "n0", "MIGRATE"});
    EXPECT_EQ(r.rfind("+OK", 0), 0u) << r;
    EXPECT_NE(r.find("errors=0"), std::string::npos) << r;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    stop = true;
    for (auto& w : workers) w.join();
    ASSERT_EQ(failures.load(), 0) << first_failure;
    EXPECT_GT(ops.load(), 200);

    // final state: every key matches its owner's model and is stored only where the ring says
    Client c(cl.rport());
    size_t live = 0;
    for (int t = 0; t < kThreads; ++t) {
        for (auto& kv : models[t]) {
            const std::string key = key_of(kv.first);
            const std::string got = c.cmd({"GET", key});
            const std::string want = kv.second ? *kv.second : "(nil)";
            ASSERT_EQ(got, want) << key;
            if (kv.second) ++live;
            const std::string owner = cl.router().owner_of(key);
            for (size_t n = 0; n < cl.size(); ++n) {
                Client d(cl.node(n).port());
                const std::string g = d.cmd({"GET", key});
                if (kv.second && cl.node(n).name == owner) ASSERT_EQ(g, want) << key << " missing on owner " << owner;
                else ASSERT_EQ(g, "(nil)") << key << " found on " << cl.node(n).name << " (owner " << owner << ")";
            }
        }
    }
    EXPECT_EQ(c.cmd({"DBSIZE"}), ":" + std::to_string(live));
    Client gone(cl.node(0).port());
    EXPECT_EQ(gone.cmd({"DBSIZE"}), ":0");
}
