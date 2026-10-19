// End-to-end failover with REAL server processes (Phase 8, step 6): the scenarios an in-process
// node cannot model.
//   * SIGSTOP: a primary that is alive but silent. After it is replaced and resumed it must be
//     unable to acknowledge a write (its lease lapsed) and must be demoted, not trusted.
//   * a primary restarted from its WAL after a failover must not resurrect writes the new
//     primary deleted.
//   * a restarted coordinator that can only reach a stale primary must refuse to adopt it.
// The router/coordinator runs in the test process; the nodes are `distcache` processes found next
// to this test binary (the sanitizer builds do not have it, so these tests skip there).
#include <gtest/gtest.h>

#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <netinet/in.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "router.hpp"
#include "test_client.hpp"

namespace {

using Clock = std::chrono::steady_clock;
namespace fs = std::filesystem;

bool wait_until(const std::function<bool()>& pred, int timeout_ms = 25000) {
    const auto end = Clock::now() + std::chrono::milliseconds(timeout_ms);
    while (Clock::now() < end) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

std::string server_binary() {
    char buf[4096];
    const ssize_t n = readlink("/proc/self/exe", buf, sizeof buf - 1);
    if (n <= 0) return "";
    buf[n] = 0;
    const std::string p = (fs::path(buf).parent_path() / "distcache").string();
    return fs::exists(p) ? p : "";
}

int free_port() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    socklen_t len = sizeof a;
    getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    const int port = ntohs(a.sin_port);
    close(fd);
    return port;
}

class Proc {
public:
    Proc() = default;
    Proc(const Proc&) = delete;
    Proc& operator=(const Proc&) = delete;
    ~Proc() { stop(); }
    void start(const std::vector<std::string>& args, const std::string& log) {
        pid_ = fork();
        if (pid_ == 0) {
            int fd = open(log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            dup2(fd, 1);
            dup2(fd, 2);
            std::vector<char*> argv;
            for (auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
            argv.push_back(nullptr);
            execv(argv[0], argv.data());
            _exit(127);
        }
    }
    void signal(int sig) { if (pid_ > 0) kill(pid_, sig); }
    void stop() {
        if (pid_ <= 0) return;
        kill(pid_, SIGCONT);
        kill(pid_, SIGKILL);
        int st;
        waitpid(pid_, &st, 0);
        pid_ = -1;
    }
    bool running() const { return pid_ > 0; }

private:
    pid_t pid_ = -1;
};

// One server process: ports are fixed so it can be killed and restarted at the same address.
struct NodeProc {
    int port = free_port(), repl_port = free_port();
    std::string dir;  // empty = no persistence
    std::string fsync_policy = "always";
    Proc proc;
    std::string log;

    void start(const std::string& exe, int primary_repl_port /*0 = start as primary*/, int sync_n = 0) {
        std::vector<std::string> a = {exe, "--port", std::to_string(port), "--threads", "2", "--repl-port", std::to_string(repl_port),
                                      "--repl-require-lease"};
        if (primary_repl_port > 0) { a.push_back("--replicaof"); a.push_back("127.0.0.1:" + std::to_string(primary_repl_port)); }
        if (sync_n > 0) { a.push_back("--repl-sync"); a.push_back(std::to_string(sync_n)); a.push_back("--repl-sync-timeout-ms"); a.push_back("1500"); }
        if (!dir.empty()) { a.push_back("--dir"); a.push_back(dir); a.push_back("--fsync"); a.push_back(fsync_policy); }
        proc.start(a, log);
        ASSERT_TRUE(wait_until([&] { Client c(port); return c.ok() && c.cmd({"PING"}) == "+PONG"; }, 8000)) << "node did not start";
    }
    std::vector<std::string> role() { Client c(port); return c.ok() ? c.cmd_array({"ROLE"}) : std::vector<std::string>{}; }
    // ROLE layout: role, epoch, replid, offset, fenced, lease_ms, link, repl_port, lag, in_sync
    std::string role_name() { auto r = role(); return r.size() > 0 ? r[0] : "?"; }
    long long epoch() { auto r = role(); return r.size() > 1 ? std::atoll(r[1].c_str()) : -1; }
    bool synced_replica() { auto r = role(); return r.size() > 6 && r[0] == "slave" && r[6] == "synced"; }
    std::string get(const std::string& k) { Client c(port); return c.ok() ? c.cmd({"GET", k}) : "<down>"; }
};

struct Env {
    std::string exe = server_binary();
    fs::path root = fs::temp_directory_path() / ("dc-e2e-" + std::to_string(getpid()) + "-" + std::to_string(rand()));
    Env() { fs::create_directories(root); }
    ~Env() { std::error_code ec; fs::remove_all(root, ec); }
};

CoordinatorOptions fast_failover(int sync_n, const std::string& state_file = "") {
    CoordinatorOptions c;
    c.lease_ms = 500;
    c.probe_ms = 100;
    c.probe_timeout_ms = 200;
    c.fail_after = 3;
    c.guard_ms = 100;
    c.sync_replicas = sync_n;
    c.log_events = false;
    c.state_file = state_file;
    return c;
}

std::unique_ptr<Router> make_router(NodeProc& primary, std::vector<NodeProc*> replicas, int sync_n, const std::string& state_file = "") {
    RouterNode n;
    n.name = "a";
    n.host = "127.0.0.1";
    n.port = primary.port;
    for (auto* r : replicas) n.replicas.push_back({"127.0.0.1", r->port});
    RouterOptions ro;
    ro.port = 0;
    ro.io_timeout_ms = 2000;
    ro.failover = fast_failover(sync_n, state_file);
    auto r = std::make_unique<Router>(std::vector<RouterNode>{n}, ro);
    std::string err;
    EXPECT_TRUE(r->start(&err)) << err;
    return r;
}

Coordinator::ShardInfo info_of(Router& r) { auto v = r.coordinator()->info(); return v.empty() ? Coordinator::ShardInfo{} : v[0]; }
bool healthy(Router& r) { return info_of(r).state == "healthy"; }

#define REQUIRE_SERVER_BINARY(env) \
    if ((env).exe.empty()) GTEST_SKIP() << "build/distcache not next to the test binary (sanitizer build)"

}  // namespace

// The classic split brain: the primary is not dead, it is frozen. The coordinator cannot tell the
// difference, replaces it (only after its lease must have expired), and the old one wakes up.
TEST(FailoverE2E, AFrozenPrimaryThatWakesUpCannotAcknowledgeWritesAndIsDemoted) {
    Env env;
    REQUIRE_SERVER_BINARY(env);
    NodeProc p, r1;
    p.log = (env.root / "p.log").string();
    r1.log = (env.root / "r1.log").string();
    p.start(env.exe, 0);
    r1.start(env.exe, p.repl_port);
    auto router = make_router(p, {&r1}, 0);
    ASSERT_TRUE(wait_until([&] { return healthy(*router); })) << router->coordinator()->status_text();
    Client c(router->port());
    for (int i = 0; i < 100; ++i) ASSERT_EQ(c.cmd({"SET", "k" + std::to_string(i), "v"}), "+OK");
    ASSERT_TRUE(wait_until([&] { auto a = p.role(), b = r1.role(); return a.size() > 3 && b.size() > 3 && a[3] == b[3]; }));

    Client direct_before(p.port);  // a client that talks to the old primary directly, bypassing the router
    ASSERT_EQ(direct_before.cmd({"SET", "direct", "1"}), "+OK");  // while it holds a lease, the old primary accepts writes

    p.proc.signal(SIGSTOP);  // frozen: connects succeed (kernel), nothing answers
    ASSERT_TRUE(wait_until([&] { return info_of(*router).failovers == 1 && healthy(*router); })) << router->coordinator()->status_text();
    Client c2(router->port());
    ASSERT_EQ(c2.cmd({"SET", "after", "failover"}), "+OK");  // the new primary takes writes
    EXPECT_EQ(info_of(*router).epoch, 1);

    p.proc.signal(SIGCONT);  // the old primary wakes up believing it is still in charge
    // It may not acknowledge anything: its lease expired long ago. Both an old and a fresh connection are refused.
    const std::string r_old = direct_before.cmd({"SET", "zombie", "1"});
    EXPECT_TRUE(r_old.rfind("-READONLY", 0) == 0 || r_old == "<closed>") << "the zombie acknowledged a write: " << r_old;
    {
        Client fresh(p.port);
        if (fresh.ok()) {
            const std::string r_new = fresh.cmd({"SET", "zombie2", "1"});
            EXPECT_EQ(r_new.rfind("-READONLY", 0), 0u) << r_new;
        }
    }
    // The coordinator then demotes it; after resynchronising it IS the new primary's replica.
    ASSERT_TRUE(wait_until([&] { return p.synced_replica() && p.epoch() == 1; })) << router->coordinator()->status_text();
    EXPECT_EQ(p.get("after"), "failover");
    EXPECT_EQ(p.get("zombie"), "(nil)");
    EXPECT_EQ(p.get("zombie2"), "(nil)");
    {
        Client fresh(p.port);
        EXPECT_EQ(fresh.cmd({"SET", "late", "1"}).rfind("-READONLY", 0), 0u);  // a replica now
    }
    EXPECT_NE(c2.cmd({"SHARD", "EVENTS"}).find("demoting a#0"), std::string::npos);
}

// A primary that crashed with its WAL on disk comes back after the new primary deleted a key.
TEST(FailoverE2E, APrimaryRestartedFromItsWalDoesNotResurrectWhatTheNewPrimaryDeleted) {
    Env env;
    REQUIRE_SERVER_BINARY(env);
    NodeProc p, r1;
    p.dir = (env.root / "p-data").string();
    fs::create_directories(p.dir);
    p.log = (env.root / "p.log").string();
    r1.log = (env.root / "r1.log").string();
    p.start(env.exe, 0);
    r1.start(env.exe, p.repl_port);
    auto router = make_router(p, {&r1}, 0);
    ASSERT_TRUE(wait_until([&] { return healthy(*router); }));
    Client c(router->port());
    for (int i = 0; i < 100; ++i) ASSERT_EQ(c.cmd({"SET", "k" + std::to_string(i), "v" + std::to_string(i)}), "+OK");
    ASSERT_TRUE(wait_until([&] { auto a = p.role(), b = r1.role(); return a.size() > 3 && b.size() > 3 && a[3] == b[3]; }));

    p.proc.signal(SIGKILL);
    p.proc.stop();
    ASSERT_TRUE(wait_until([&] { return info_of(*router).failovers == 1 && healthy(*router); })) << router->coordinator()->status_text();
    Client c2(router->port());
    ASSERT_EQ(c2.cmd({"DEL", "k5"}), ":1");           // the new primary deletes a key the old one still has in its WAL
    ASSERT_EQ(c2.cmd({"SET", "k200", "new"}), "+OK");

    p.start(env.exe, 0);  // the old primary restarts: it recovers its WAL (k5 alive) and thinks it is a primary
    ASSERT_TRUE(wait_until([&] { return p.synced_replica() && p.epoch() == 1; })) << router->coordinator()->status_text();
    EXPECT_EQ(p.get("k5"), "(nil)") << "stale data from the WAL leaked into the demoted node";
    EXPECT_EQ(p.get("k200"), "new");
    EXPECT_EQ(p.get("k6"), "v6");

    // And its own log now agrees: restart it again as a plain standalone node and read what it recovers.
    p.proc.signal(SIGKILL);
    p.proc.stop();
    NodeProc plain;
    plain.log = (env.root / "plain.log").string();
    plain.dir = p.dir;
    std::vector<std::string> args = {env.exe, "--port", std::to_string(plain.port), "--threads", "2", "--dir", plain.dir, "--fsync", "always"};
    plain.proc.start(args, plain.log);
    ASSERT_TRUE(wait_until([&] { Client x(plain.port); return x.ok() && x.cmd({"PING"}) == "+PONG"; }, 8000));
    EXPECT_EQ(plain.get("k5"), "(nil)") << "the demotion's FLUSHALL must be in the WAL, or recovery resurrects k5";
    EXPECT_EQ(plain.get("k200"), "new");
    EXPECT_EQ(plain.get("k6"), "v6");
}

// The coordinator restarts while the only reachable node is a stale old primary.
TEST(FailoverE2E, ARestartedCoordinatorRefusesToAdoptAStalePrimaryWhenItRemembersANewerEpoch) {
    Env env;
    REQUIRE_SERVER_BINARY(env);
    NodeProc p, r1;
    p.log = (env.root / "p.log").string();
    r1.log = (env.root / "r1.log").string();
    const std::string state = (env.root / "router.state").string();
    p.start(env.exe, 0);
    r1.start(env.exe, p.repl_port);
    auto router = make_router(p, {&r1}, 0, state);
    ASSERT_TRUE(wait_until([&] { return healthy(*router); }));
    Client c(router->port());
    ASSERT_EQ(c.cmd({"SET", "k", "acknowledged"}), "+OK");
    ASSERT_TRUE(wait_until([&] { auto a = p.role(), b = r1.role(); return a.size() > 3 && b.size() > 3 && a[3] == b[3]; }));
    p.proc.signal(SIGKILL);
    p.proc.stop();
    ASSERT_TRUE(wait_until([&] { return info_of(*router).failovers == 1 && healthy(*router); }));
    ASSERT_EQ(c.cmd({"SET", "k", "newer"}), "+OK");  // acknowledged by epoch 1

    // Disaster: the new primary dies too, the coordinator restarts, and the OLD primary comes back (empty).
    router.reset();
    r1.proc.signal(SIGKILL);
    r1.proc.stop();
    p.start(env.exe, 0);  // epoch 0, claims to be a primary
    auto router2 = make_router(p, {&r1}, 0, state);
    ASSERT_TRUE(wait_until([&] { return info_of(*router2).state == "unavailable"; })) << router2->coordinator()->status_text();
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    EXPECT_EQ(info_of(*router2).state, "unavailable");
    EXPECT_NE(info_of(*router2).reason.find("refusing to adopt stale members"), std::string::npos) << info_of(*router2).reason;
    Client c2(router2->port());
    EXPECT_EQ(c2.cmd({"GET", "k"}).rfind("-ERR shard 'a' is unavailable", 0), 0u);  // not even a stale read
    Client direct(p.port);
    EXPECT_EQ(direct.cmd({"SET", "x", "1"}).rfind("-READONLY", 0), 0u);                // and it was never granted a lease

    // Without the state file the same restart WOULD adopt the stale node: that is what the file prevents.
    router2.reset();
    auto router3 = make_router(p, {&r1}, 0);
    ASSERT_TRUE(wait_until([&] { return healthy(*router3); }));
    EXPECT_EQ(info_of(*router3).epoch, 0);
}

// TTLs, deletes and rate-limiter buckets across a failover, then a WAL restart of the old primary
// (replication + failover + TTL + persistence + the rate limiter, in real processes).
TEST(FailoverE2E, TtlsDeletesAndRateLimiterStateSurviveAFailoverAndAWalRestartOfTheOldPrimary) {
    Env env;
    REQUIRE_SERVER_BINARY(env);
    NodeProc p, r1, r2;
    p.dir = (env.root / "p-data").string();
    fs::create_directories(p.dir);
    p.log = (env.root / "p.log").string();
    r1.log = (env.root / "r1.log").string();
    r2.log = (env.root / "r2.log").string();
    p.start(env.exe, 0, 1);
    r1.start(env.exe, p.repl_port, 1);
    r2.start(env.exe, p.repl_port, 1);
    auto router = make_router(p, {&r1, &r2}, 1);
    ASSERT_TRUE(wait_until([&] { return healthy(*router); })) << router->coordinator()->status_text();
    Client c(router->port());
    ASSERT_TRUE(wait_until([&] { return c.cmd({"SET", "probe", "1"}) == "+OK"; }));   // sync 1: both replicas attached
    ASSERT_EQ(c.cmd({"SET", "short", "s", "PX", "1500"}), "+OK");
    ASSERT_EQ(c.cmd({"SET", "long", "l", "EX", "3600"}), "+OK");
    ASSERT_EQ(c.cmd({"SET", "gone", "g"}), "+OK");
    ASSERT_EQ(c.cmd({"DEL", "gone"}), ":1");
    for (int i = 0; i < 3; ++i) {
        auto r = c.cmd_ints({"RL.ACQUIRE", "bucket", "5", "0.001"});
        ASSERT_EQ(r.size(), 4u);
        ASSERT_EQ(r[0], 1);
    }
    ASSERT_TRUE(wait_until([&] {
        auto a = p.role(), b = r1.role(), d = r2.role();
        return a.size() > 3 && b.size() > 3 && d.size() > 3 && a[3] == b[3] && a[3] == d[3];
    }));

    p.proc.signal(SIGKILL);
    p.proc.stop();
    ASSERT_TRUE(wait_until([&] { return info_of(*router).failovers == 1 && healthy(*router); })) << router->coordinator()->status_text();
    std::this_thread::sleep_for(std::chrono::milliseconds(1700));   // "short" (1500 ms) is now past its deadline

    Client c2(router->port());
    EXPECT_EQ(c2.cmd({"GET", "short"}), "(nil)") << "an expired key came back after the failover";
    EXPECT_EQ(c2.cmd({"GET", "gone"}), "(nil)");
    EXPECT_EQ(c2.cmd({"GET", "long"}), "l");
    const long long ttl = std::stoll(c2.cmd({"TTL", "long"}).substr(1));
    EXPECT_GT(ttl, 3500);
    EXPECT_LE(ttl, 3600);
    int allowed = 0;   // capacity 5, three already taken before the crash: exactly two more
    for (int i = 0; i < 4; ++i) {
        std::vector<long long> r;
        for (int tries = 0; tries < 50 && r.empty(); ++tries) {   // -NOREPLICAS until the other replica re-attaches
            r = c2.cmd_ints({"RL.ACQUIRE", "bucket", "5", "0.001"});
            if (r.empty()) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_EQ(r.size(), 4u);
        allowed += static_cast<int>(r[0]);
    }
    EXPECT_EQ(allowed, 2) << "the failover forgot or invented rate-limit tokens";

    p.start(env.exe, 0, 1);   // the old primary restarts from its WAL, which still has short, gone (live) and the old bucket
    ASSERT_TRUE(wait_until([&] { return p.synced_replica() && p.epoch() == 1; })) << router->coordinator()->status_text();
    EXPECT_EQ(p.get("short"), "(nil)");
    EXPECT_EQ(p.get("gone"), "(nil)");
    EXPECT_EQ(p.get("long"), "l");
    Client direct(p.port);
    auto peek = direct.cmd_ints({"RL.PEEK", "bucket", "5", "0.001"});
    ASSERT_EQ(peek.size(), 4u);
    EXPECT_EQ(peek[0], 0) << "the demoted node must show the new primary's bucket (empty), not its stale WAL copy";
    EXPECT_EQ(peek[1], 0);
}

// Found by bench/harness/chaos.py: the promoted primary crashes while the shard cannot elect a successor
// (sync 1 with two replicas needs both of them). When it restarts it must be recognised as the latest
// primary of its epoch (the epoch is stored next to its WAL), otherwise the shard is wedged forever even
// though every node is back.
TEST(FailoverE2E, ACrashedPrimaryThatRestartsIsReadoptedAtItsOwnEpochInsteadOfWedgingTheShard) {
    Env env;
    REQUIRE_SERVER_BINARY(env);
    NodeProc p, r1, r2;
    for (auto* n : {&p, &r1, &r2}) {
        n->dir = (env.root / ("data-" + std::to_string(n->port))).string();
        fs::create_directories(n->dir);
    }
    p.log = (env.root / "p.log").string();
    r1.log = (env.root / "r1.log").string();
    r2.log = (env.root / "r2.log").string();
    p.start(env.exe, 0, 1);
    r1.start(env.exe, p.repl_port, 1);
    r2.start(env.exe, p.repl_port, 1);
    auto router = make_router(p, {&r1, &r2}, 1);
    ASSERT_TRUE(wait_until([&] { return healthy(*router); })) << router->coordinator()->status_text();
    Client c(router->port());
    ASSERT_TRUE(wait_until([&] { return r1.synced_replica() && r2.synced_replica(); })) << "both replicas must be in sync before the primary is killed";
    ASSERT_TRUE(wait_until([&] { return c.cmd({"SET", "probe", "1"}) == "+OK"; }));
    for (int i = 0; i < 200; ++i) ASSERT_EQ(c.cmd({"SET", "k" + std::to_string(i), "v" + std::to_string(i)}), "+OK");

    // 1. first failover: p dies, r1 becomes the primary (epoch 1); p comes back as a replica
    p.proc.signal(SIGKILL);
    p.proc.stop();
    ASSERT_TRUE(wait_until([&] { return info_of(*router).failovers == 1 && healthy(*router); })) << router->coordinator()->status_text();
    p.start(env.exe, 0, 1);
    ASSERT_TRUE(wait_until([&] { return p.synced_replica() && r2.synced_replica(); })) << router->coordinator()->status_text();
    Client c1(router->port());
    ASSERT_TRUE(wait_until([&] { return c1.cmd({"SET", "after-first", "1"}) == "+OK"; }));

    // 2. a replica is lost and then the primary too: with R=2, N=1 nobody may be promoted
    r2.proc.signal(SIGKILL);
    r2.proc.stop();
    r1.proc.signal(SIGKILL);
    r1.proc.stop();
    ASSERT_TRUE(wait_until([&] { return info_of(*router).state == "unavailable"; })) << router->coordinator()->status_text();
    EXPECT_NE(info_of(*router).reason.find("NOT PROMOTING"), std::string::npos) << info_of(*router).reason;

    // 3. the primary restarts from its WAL (as a plain node): it reports the epoch it was promoted at
    r1.start(env.exe, 0, 1);
    EXPECT_EQ(r1.epoch(), 1) << "the restarted primary lost its epoch";
    ASSERT_TRUE(wait_until([&] { return healthy(*router); }, 15000)) << "wedged: " << router->coordinator()->status_text();
    EXPECT_EQ(info_of(*router).epoch, 1);
    EXPECT_EQ(info_of(*router).failovers, 1u) << "re-adoption must not be a second failover";
    Client c2(router->port());
    for (int i = 0; i < 200; ++i) ASSERT_EQ(c2.cmd({"GET", "k" + std::to_string(i)}), "v" + std::to_string(i)) << i;
    EXPECT_EQ(c2.cmd({"GET", "after-first"}), "1");
    EXPECT_TRUE(wait_until([&] { return c2.cmd({"SET", "after-restart", "1"}) == "+OK"; }));   // p (still a replica) acknowledges
    r2.start(env.exe, 0, 1);
    ASSERT_TRUE(wait_until([&] { return r2.synced_replica() && p.synced_replica(); })) << router->coordinator()->status_text();
}

// The same restart must NOT be adopted when a newer epoch already exists.
TEST(FailoverE2E, ARestartedFormerPrimaryAtAnOlderEpochIsStillDemotedNotAdopted) {
    Env env;
    REQUIRE_SERVER_BINARY(env);
    NodeProc p, r1;
    p.dir = (env.root / "data-p").string();
    r1.dir = (env.root / "data-r1").string();
    fs::create_directories(p.dir);
    fs::create_directories(r1.dir);
    p.log = (env.root / "p.log").string();
    r1.log = (env.root / "r1.log").string();
    p.start(env.exe, 0);
    r1.start(env.exe, p.repl_port);
    auto router = make_router(p, {&r1}, 0);
    ASSERT_TRUE(wait_until([&] { return healthy(*router); }));
    Client c(router->port());
    ASSERT_EQ(c.cmd({"SET", "k", "v1"}), "+OK");
    ASSERT_TRUE(wait_until([&] { auto a = p.role(), b = r1.role(); return a.size() > 3 && b.size() > 3 && a[3] == b[3]; }));
    p.proc.signal(SIGKILL);
    p.proc.stop();
    ASSERT_TRUE(wait_until([&] { return info_of(*router).failovers == 1 && healthy(*router); }));   // r1 primary at epoch 1
    ASSERT_EQ(c.cmd({"SET", "k", "v2"}), "+OK");
    p.start(env.exe, 0);   // epoch 0: older than the shard's epoch 1
    ASSERT_TRUE(wait_until([&] { return p.synced_replica() && p.epoch() == 1; })) << router->coordinator()->status_text();
    EXPECT_EQ(p.get("k"), "v2");
    EXPECT_EQ(info_of(*router).primary, "a#1");
}

// Suspected hole in the epoch-persistence fix: the primary is killed and comes back BEFORE the coordinator has
// promoted anyone, so it is re-adopted at its own epoch with whatever its WAL holds. With --fsync everysec the WAL
// tail is still in the process when it dies, but the replicas have it (sync 1). If the restarted primary is
// re-adopted, the replicas resynchronise DOWN to it and acknowledged writes are lost.
TEST(FailoverE2E, AnAcknowledgedWriteSurvivesAPrimaryThatIsKilledAndRestartedBeforeAnyPromotion) {
    Env env;
    REQUIRE_SERVER_BINARY(env);
    int lost_total = 0, acked_total = 0;
    for (int round = 0; round < 6; ++round) {
        NodeProc p, r1, r2;
        for (auto* n : {&p, &r1, &r2}) {
            n->dir = (env.root / ("r" + std::to_string(round) + "-" + std::to_string(n->port))).string();
            n->fsync_policy = "everysec";
            fs::create_directories(n->dir);
        }
        p.log = (env.root / "p.log").string();
        r1.log = (env.root / "r1.log").string();
        r2.log = (env.root / "r2.log").string();
        p.start(env.exe, 0, 1);
        r1.start(env.exe, p.repl_port, 1);
        r2.start(env.exe, p.repl_port, 1);
        auto router = make_router(p, {&r1, &r2}, 1);
        ASSERT_TRUE(wait_until([&] { return healthy(*router) && r1.synced_replica() && r2.synced_replica(); }));
        Client c(router->port());
        std::vector<std::string> acked;
        for (int i = 0; i < 300; ++i) {
            const std::string k = "r" + std::to_string(round) + "k" + std::to_string(i);
            if (c.cmd({"SET", k, "v"}) == "+OK") acked.push_back(k);
        }
        p.proc.signal(SIGKILL);   // immediately after the last acknowledgement
        p.proc.stop();
        p.start(env.exe, 0, 1);   // and back before the coordinator can have promoted anybody (lease 500 ms)
        // The coordinator must notice that the primary PROCESS was replaced (new lineage, no fsync before acks) and
        // promote a replica, which still holds the tail; the restarted node is demoted and resynchronises.
        ASSERT_TRUE(wait_until([&] { return info_of(*router).failovers >= 1 && healthy(*router); }, 25000)) << router->coordinator()->status_text();
        wait_until([&] { return p.synced_replica() && (r1.synced_replica() || r1.role_name() == "master") && (r2.synced_replica() || r2.role_name() == "master"); }, 10000);
        Client c2(router->port());
        int lost = 0;
        for (auto& k : acked) {
            std::string v;
            for (int t = 0; t < 50; ++t) { v = c2.cmd({"GET", k}); if (v.rfind("-", 0) != 0) break; std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
            if (v != "v") ++lost;
        }
        lost_total += lost;
        acked_total += static_cast<int>(acked.size());
        std::fprintf(stderr, "round %d: %zu acknowledged, %d lost, failovers=%llu epoch=%lld\n", round, acked.size(), lost,
                     static_cast<unsigned long long>(info_of(*router).failovers), static_cast<long long>(info_of(*router).epoch));
    }
    EXPECT_EQ(lost_total, 0) << lost_total << " of " << acked_total << " acknowledged writes lost";
}

// The documented consequence of the fix above: a restarted primary that does NOT fsync before acknowledging is never
// re-adopted. When no successor can be elected either, the shard stays closed (it will not risk acknowledged writes).
TEST(FailoverE2E, ARestartedPrimaryThatDoesNotFsyncBeforeAcknowledgingIsNotReadoptedAndTheShardStaysClosed) {
    Env env;
    REQUIRE_SERVER_BINARY(env);
    NodeProc p, r1, r2;
    for (auto* n : {&p, &r1, &r2}) {
        n->dir = (env.root / ("data-" + std::to_string(n->port))).string();
        n->fsync_policy = "everysec";
        fs::create_directories(n->dir);
    }
    p.log = (env.root / "p.log").string();
    r1.log = (env.root / "r1.log").string();
    r2.log = (env.root / "r2.log").string();
    p.start(env.exe, 0, 1);
    r1.start(env.exe, p.repl_port, 1);
    r2.start(env.exe, p.repl_port, 1);
    auto router = make_router(p, {&r1, &r2}, 1);
    ASSERT_TRUE(wait_until([&] { return healthy(*router) && r1.synced_replica() && r2.synced_replica(); }));
    Client c(router->port());
    for (int i = 0; i < 100; ++i) ASSERT_EQ(c.cmd({"SET", "k" + std::to_string(i), "v"}), "+OK");
    p.proc.signal(SIGKILL);
    p.proc.stop();
    ASSERT_TRUE(wait_until([&] { return info_of(*router).failovers == 1 && healthy(*router); }));   // r1 or r2 promoted (epoch 1)
    p.start(env.exe, 0, 1);   // the old primary returns and is demoted
    // now the promoted primary dies together with the other replica: nobody can be elected
    NodeProc* promoted = r1.role_name() == "master" ? &r1 : &r2;
    NodeProc* other = promoted == &r1 ? &r2 : &r1;
    ASSERT_TRUE(wait_until([&] { return other->synced_replica() && p.synced_replica(); }));
    other->proc.signal(SIGKILL);
    other->proc.stop();
    promoted->proc.signal(SIGKILL);
    promoted->proc.stop();
    ASSERT_TRUE(wait_until([&] { return info_of(*router).state == "unavailable"; }));
    promoted->start(env.exe, 0, 1);   // back, but it acknowledged writes it had not fsynced
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    EXPECT_NE(info_of(*router).state, "healthy") << "a non-durable restarted primary must not be re-adopted";
    Client direct(promoted->port);
    EXPECT_EQ(direct.cmd({"SET", "x", "1"}).rfind("-READONLY", 0), 0u);    // and it was never given a lease
    EXPECT_EQ(direct.cmd({"GET", "k5"}).rfind("-NOLEASE", 0), 0u);        // nor does it serve reads
}
