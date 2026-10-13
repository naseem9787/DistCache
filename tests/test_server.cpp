// End-to-end tests: a real Server on an ephemeral port, driven by real TCP clients,
// for each engine mode. Runs under ThreadSanitizer too, so the server's thread
// hand-off (acceptor -> worker) and the engine locking are checked together.
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <stdlib.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "persistence.hpp"
#include "server.hpp"
#include "test_client.hpp"
#include "store.hpp"
#include "synced_store.hpp"

namespace {

struct Mode { std::string name; int workers; };

}  // namespace

class ServerE2E : public ::testing::TestWithParam<Mode> {
protected:
    void SetUp() override {
        const Mode& m = GetParam();
        if (m.name == "single") engine_ = std::make_unique<Store>();
        else if (m.name == "global") engine_ = std::make_unique<GlobalLockStore>();
        else engine_ = std::make_unique<ShardedStore>(8);
        server_ = std::make_unique<Server>(*engine_, ServerOptions{0, m.workers});
        std::string err;
        ASSERT_TRUE(server_->start(&err)) << err;
    }
    void TearDown() override { server_->stop(); }
    int port() const { return server_->port(); }

    std::unique_ptr<Engine> engine_;
    std::unique_ptr<Server> server_;
};

INSTANTIATE_TEST_SUITE_P(Modes, ServerE2E,
                         ::testing::Values(Mode{"single", 1}, Mode{"global", 4}, Mode{"sharded", 4}),
                         [](const auto& info) { return info.param.name; });

TEST_P(ServerE2E, BasicCommands) {
    Client c(port());
    ASSERT_TRUE(c.ok());
    EXPECT_EQ(c.cmd({"PING"}), "+PONG");
    EXPECT_EQ(c.cmd({"SET", "name", "Naseem"}), "+OK");
    EXPECT_EQ(c.cmd({"GET", "name"}), "Naseem");
    EXPECT_EQ(c.cmd({"DEL", "name"}), ":1");
    EXPECT_EQ(c.cmd({"GET", "name"}), "(nil)");
}

TEST_P(ServerE2E, ManyClientsOwnKeys) {
    constexpr int kClients = 16, kOps = 300;
    std::atomic<int> failures{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < kClients; ++t) {
        ts.emplace_back([&, t] {
            Client c(port());
            if (!c.ok()) { ++failures; return; }
            for (int i = 0; i < kOps; ++i) {
                std::string k = "c" + std::to_string(t) + ":" + std::to_string(i), v = "v" + std::to_string(i * t);
                if (c.cmd({"SET", k, v}) != "+OK") ++failures;
                if (c.cmd({"GET", k}) != v) ++failures;
            }
        });
    }
    for (auto& th : ts) th.join();
    server_->stop();  // joins the workers: after this it is safe to inspect even the unlocked engine
    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(engine_->size(), static_cast<size_t>(kClients * kOps));
    EXPECT_TRUE(engine_->consistent());
}

TEST_P(ServerE2E, SharedHotKeyNeverReturnsGarbage) {
    constexpr int kClients = 12, kOps = 300;
    std::atomic<int> bad{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < kClients; ++t) {
        ts.emplace_back([&, t] {
            Client c(port());
            for (int i = 0; i < kOps; ++i) {
                c.cmd({"SET", "hot", "v" + std::to_string(t) + "-" + std::to_string(i)});
                std::string v = c.cmd({"GET", "hot"});
                if (v.empty() || v[0] != 'v') ++bad;  // always some client's complete value
            }
        });
    }
    for (auto& th : ts) th.join();
    server_->stop();
    EXPECT_EQ(bad.load(), 0);
    EXPECT_TRUE(engine_->consistent());
}

TEST_P(ServerE2E, PipelinedCommandsAnswerInOrder) {
    Client c(port());
    std::string batch;
    for (int i = 0; i < 200; ++i) batch += Client::encode({"SET", "p" + std::to_string(i), std::to_string(i)});
    for (int i = 0; i < 200; ++i) batch += Client::encode({"GET", "p" + std::to_string(i)});
    c.send_raw(batch);  // 400 commands in one write
    for (int i = 0; i < 200; ++i) ASSERT_EQ(c.read_reply(), "+OK");
    for (int i = 0; i < 200; ++i) ASSERT_EQ(c.read_reply(), std::to_string(i));
}

TEST_P(ServerE2E, CommandSplitAcrossManySmallWrites) {
    Client c(port());
    std::string wire = Client::encode({"SET", "slow", "value"});
    for (char ch : wire) {
        c.send_raw(std::string(1, ch));  // one byte at a time: parser must say "Incomplete"
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    EXPECT_EQ(c.read_reply(), "+OK");
    EXPECT_EQ(c.cmd({"GET", "slow"}), "value");
}

TEST_P(ServerE2E, ConnectionChurn) {
    std::atomic<int> failures{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < 8; ++t) {
        ts.emplace_back([&, t] {
            for (int i = 0; i < 50; ++i) {
                Client c(port());  // connect, one round trip, disconnect
                std::string k = "churn" + std::to_string(t) + "-" + std::to_string(i);
                if (!c.ok() || c.cmd({"SET", k, "x"}) != "+OK" || c.cmd({"GET", k}) != "x") ++failures;
            }
        });
    }
    for (auto& th : ts) th.join();
    EXPECT_EQ(failures.load(), 0);
}

TEST_P(ServerE2E, ActiveExpirationRunsWithNoClientTraffic) {
    {
        Client c(port());
        for (int i = 0; i < 200; ++i) ASSERT_EQ(c.cmd({"SET", "e" + std::to_string(i), "v", "PX", "50"}), "+OK");
    }  // client leaves: nobody will GET these keys, only the background sweep can remove them
    Client probe(port());
    bool drained = false;
    for (int i = 0; i < 40 && !drained; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        drained = probe.cmd({"DBSIZE"}) == ":0";  // asked over the network: DBSIZE does not touch the keys
    }
    EXPECT_TRUE(drained);
    server_->stop();
    EXPECT_EQ(engine_->expired_keys(), 200u);
}

TEST_P(ServerE2E, AbruptDisconnectAndMalformedInputDoNotAffectOthers) {
    {
        Client half(port());
        half.send_raw("*3\r\n$3\r\nSET\r\n$1\r\nk");  // incomplete command, then hang up
    }
    {
        Client bad(port());
        bad.send_raw("*x\r\n");
        EXPECT_EQ(bad.read_reply().substr(0, 4), "-ERR");
        EXPECT_EQ(bad.read_reply(), "<closed>");  // server drops a client that breaks the protocol
    }
    Client good(port());
    EXPECT_EQ(good.cmd({"PING"}), "+PONG");
}

TEST_P(ServerE2E, InfoReportsTheActiveMode) {
    Client c(port());
    std::string info = c.cmd({"INFO"});
    EXPECT_NE(info.find("mode:" + GetParam().name), std::string::npos) << info;
}

TEST(ServerLifecycle, StopWithOpenConnectionsDoesNotHang) {
    ShardedStore engine(4);
    Server server(engine, ServerOptions{0, 4});
    ASSERT_TRUE(server.start());
    Client a(server.port()), b(server.port());
    EXPECT_EQ(a.cmd({"PING"}), "+PONG");
    server.stop();
    server.stop();  // idempotent
    EXPECT_EQ(b.cmd({"PING"}), "<closed>");
}

// The durability promise, end to end: under fsync=always, every write a client saw acknowledged
// is already on disk. We copy the data directory WHILE the server is still running, with no
// clean shutdown (this is what the disk looks like after a crash) and recover from the copy.
TEST(ServerPersistence, AcknowledgedWritesAreOnDiskUnderFsyncAlways) {
    char tmpl[] = "/tmp/distcache-srv-XXXXXX";
    const std::string dir = mkdtemp(tmpl);
    const std::string crash_image = dir + "-crash";
    ShardedStore engine(4);
    PersistenceOptions po;
    po.dir = dir;
    po.fsync = FsyncPolicy::Always;
    po.snapshot_wal_bytes = 0;
    Persistence persistence(engine, po);
    std::string err;
    ASSERT_TRUE(persistence.open(nullptr, &err)) << err;
    Server server(engine, ServerOptions{0, 4, &persistence});
    ASSERT_TRUE(server.start(&err)) << err;

    constexpr int kClients = 8, kPer = 40;
    std::atomic<int> failures{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < kClients; ++t)
        ts.emplace_back([&, t] {
            Client c(server.port());
            for (int i = 0; i < kPer; ++i)
                if (c.cmd({"SET", "d" + std::to_string(t) + ":" + std::to_string(i), "v" + std::to_string(i)}) != "+OK") ++failures;
        });
    for (auto& th : ts) th.join();
    ASSERT_EQ(failures.load(), 0);

    std::filesystem::create_directories(crash_image);
    for (auto& e : std::filesystem::directory_iterator(dir))
        if (e.path().filename() != "LOCK") std::filesystem::copy_file(e.path(), crash_image + "/" + e.path().filename().string());

    ShardedStore recovered(4);
    PersistenceOptions po2 = po;
    po2.dir = crash_image;
    Persistence p2(recovered, po2);
    RecoveryStats rs;
    ASSERT_TRUE(p2.open(&rs, &err)) << err;
    EXPECT_EQ(recovered.size(), static_cast<size_t>(kClients * kPer));  // nothing acknowledged was lost
    for (int t = 0; t < kClients; ++t)
        for (int i = 0; i < kPer; ++i)
            ASSERT_EQ(recovered.get("d" + std::to_string(t) + ":" + std::to_string(i)), "v" + std::to_string(i));
    EXPECT_EQ(rs.damaged_files, 0u);
    p2.close();

    server.stop();
    persistence.close();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::remove_all(crash_image, ec);
}
