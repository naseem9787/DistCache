// Deferred acknowledgement under fsync=always.
//
// Property under test (the safety invariant): a reply that depends on a logged write is never
// sent before that write's log record has been fdatasync'ed. Liveness property: a worker that
// is waiting for an fsync still serves every other connection, and one fsync covers the pending
// writes of ALL connections.
//
// The log has a test hook that sleeps before each fdatasync. That widens the window between
// "written" and "durable" so a premature reply is observable: at the moment a client receives
// its reply, the log's durable sequence number must already cover its record.
#include <gtest/gtest.h>

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
#include "synced_store.hpp"
#include "test_client.hpp"

namespace {

using Clock = std::chrono::steady_clock;
using Ms = std::chrono::milliseconds;

double ms_since(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// A server with fsync=always whose every fdatasync is artificially delayed.
struct Rig {
    std::string dir;
    ShardedStore engine{8};
    std::unique_ptr<Persistence> persistence;
    std::unique_ptr<Server> server;

    Rig(int workers, int delay_ms, FsyncPolicy policy = FsyncPolicy::Always) {
        char tmpl[] = "/tmp/distcache-deferred-XXXXXX";
        dir = mkdtemp(tmpl);
        PersistenceOptions po;
        po.dir = dir;
        po.fsync = policy;
        po.snapshot_wal_bytes = 0;
        persistence = std::make_unique<Persistence>(engine, po);
        std::string err;
        EXPECT_TRUE(persistence->open(nullptr, &err)) << err;
        persistence->set_sync_delay_for_testing(Ms(delay_ms));
        server = std::make_unique<Server>(engine, ServerOptions{0, workers, persistence.get()});
        EXPECT_TRUE(server->start(&err)) << err;
    }
    ~Rig() {
        server->stop();
        persistence->close();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
    int port() const { return server->port(); }
    WalStats wal() const { return persistence->stats().wal; }
};

}  // namespace

TEST(DeferredAck, ReplyIsNotSentBeforeItsRecordIsDurable) {
    Rig r(/*workers=*/2, /*delay_ms=*/200);
    Client c(r.port());
    auto t0 = Clock::now();
    EXPECT_EQ(c.cmd({"SET", "k", "v"}), "+OK");
    const double took = ms_since(t0);
    EXPECT_GE(took, 180.0) << "reply arrived before the (delayed) fdatasync could have finished";
    // At the instant the reply arrived, the log must already cover the record that produced it.
    WalStats w = r.wal();
    EXPECT_GE(w.last_seq, 1u);
    EXPECT_GE(w.durable_seq, w.last_seq);
    EXPECT_GE(w.fsyncs, 1u);
}

TEST(DeferredAck, AWorkerWaitingForAnFsyncStillServesOtherConnections) {
    Rig r(/*workers=*/1, /*delay_ms=*/300);  // ONE worker owns both connections
    Client writer(r.port()), reader(r.port());
    ASSERT_EQ(reader.cmd({"PING"}), "+PONG");
    writer.send_raw(Client::encode({"SET", "k", "v"}));  // now waiting ~300 ms for its fsync
    std::this_thread::sleep_for(Ms(30));                 // let the worker execute it

    auto t0 = Clock::now();
    EXPECT_EQ(reader.cmd({"PING"}), "+PONG");
    EXPECT_EQ(reader.cmd({"GET", "nothing"}), "(nil)");
    const double served = ms_since(t0);
    EXPECT_LT(served, 150.0) << "the worker was blocked behind another connection's fsync";

    auto t1 = Clock::now();
    EXPECT_EQ(writer.read_reply(), "+OK");
    EXPECT_GE(ms_since(t1) + served + 30.0, 280.0);       // and the write still waited for its fsync
    EXPECT_GE(r.wal().durable_seq, r.wal().last_seq);
}

TEST(DeferredAck, PipelinedRepliesStayInOrderAndAllWaitForTheWrite) {
    Rig r(1, 150);
    Client c(r.port());
    std::string batch;
    batch += Client::encode({"SET", "a", "1"});
    batch += Client::encode({"GET", "a"});       // read AFTER a write in the same batch
    batch += Client::encode({"PING"});
    batch += Client::encode({"SET", "b", "2"});
    batch += Client::encode({"GET", "b"});
    auto t0 = Clock::now();
    c.send_raw(batch);
    EXPECT_EQ(c.read_reply(), "+OK");
    EXPECT_GE(ms_since(t0), 130.0);              // even the first reply waited for the batch's fsync
    EXPECT_EQ(c.read_reply(), "1");
    EXPECT_EQ(c.read_reply(), "+PONG");
    EXPECT_EQ(c.read_reply(), "+OK");
    EXPECT_EQ(c.read_reply(), "2");
}

TEST(DeferredAck, LaterBatchWithNoWritesIsHeldBehindAnEarlierWaitingBatch) {
    Rig r(1, 250);
    Client c(r.port());
    c.send_raw(Client::encode({"SET", "k", "v"}));  // batch 1: waits for fsync
    std::this_thread::sleep_for(Ms(30));
    auto t0 = Clock::now();
    c.send_raw(Client::encode({"PING"}));           // batch 2: no write, but replies must stay ordered
    EXPECT_EQ(c.read_reply(), "+OK");
    EXPECT_EQ(c.read_reply(), "+PONG");
    EXPECT_GE(ms_since(t0), 150.0);                 // the PING reply could not overtake the SET reply
}

TEST(DeferredAck, ManyConnectionsShareFsyncs) {
    Rig r(/*workers=*/1, /*delay_ms=*/60);
    constexpr int kClients = 40;
    std::atomic<int> failures{0};
    auto t0 = Clock::now();
    std::vector<std::thread> ts;
    for (int i = 0; i < kClients; ++i)
        ts.emplace_back([&, i] {
            Client c(r.port());
            if (c.cmd({"SET", "k" + std::to_string(i), "v"}) != "+OK") ++failures;
        });
    for (auto& t : ts) t.join();
    const double took = ms_since(t0);
    EXPECT_EQ(failures.load(), 0);
    // One worker and a 60 ms fsync: acknowledging 40 writes ONE fsync at a time would take
    // 40 x 60 = 2400 ms. Sharing fsyncs across connections must be several times faster.
    EXPECT_LT(took, 1200.0);
    EXPECT_LE(r.wal().fsyncs, 12u) << "40 concurrent writes should need far fewer than 40 fsyncs";
    RecordProperty("fsyncs_for_40_writes", static_cast<int>(r.wal().fsyncs));
}

TEST(DeferredAck, ProtocolErrorAfterAHeldWriteDeliversBothRepliesThenCloses) {
    Rig r(1, 150);
    Client c(r.port());
    c.send_raw(Client::encode({"SET", "k", "v"}) + "*x\r\n");  // valid write, then garbage
    auto t0 = Clock::now();
    EXPECT_EQ(c.read_reply(), "+OK");
    EXPECT_GE(ms_since(t0), 130.0);
    EXPECT_EQ(c.read_reply().substr(0, 4), "-ERR");
    EXPECT_EQ(c.read_reply(), "<closed>");
}

TEST(DeferredAck, ClientThatDisconnectsWhileWaitingDoesNotBreakTheServer) {
    Rig r(1, 150);
    {
        Client c(r.port());
        c.send_raw(Client::encode({"SET", "ghost", "v"}));
        std::this_thread::sleep_for(Ms(20));
    }  // hang up while the reply is parked
    Client other(r.port());
    EXPECT_EQ(other.cmd({"PING"}), "+PONG");
    for (int i = 0; i < 50 && r.wal().durable_seq < 1; ++i) std::this_thread::sleep_for(Ms(20));
    EXPECT_GE(r.wal().durable_seq, 1u);               // the write itself was still made durable
    EXPECT_EQ(other.cmd({"GET", "ghost"}), "v");
}

TEST(DeferredAck, PoliciesOtherThanAlwaysNeverHoldReplies) {
    Rig r(1, 400, FsyncPolicy::EverySec);  // the sync delay applies only when an fsync happens
    Client c(r.port());
    auto t0 = Clock::now();
    EXPECT_EQ(c.cmd({"SET", "k", "v"}), "+OK");
    EXPECT_LT(ms_since(t0), 150.0);
}

TEST(DeferredAck, ShutdownWithParkedRepliesDoesNotHang) {
    auto r = std::make_unique<Rig>(1, 500);
    Client c(r->port());
    c.send_raw(Client::encode({"SET", "k", "v"}));
    std::this_thread::sleep_for(Ms(30));
    auto t0 = Clock::now();
    r.reset();  // stop server (listeners removed first), close persistence
    EXPECT_LT(ms_since(t0), 3000.0);
}
