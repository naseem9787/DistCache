// Crash-recovery tests: write through a persistent engine, "crash" (or close), recover into a
// fresh engine, and compare. Clocks are injected so deadlines are exact and nothing sleeps.
#include <gtest/gtest.h>

#include <stdlib.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "commands.hpp"
#include "persistence.hpp"
#include "store.hpp"
#include "synced_store.hpp"
#include "wal.hpp"

namespace fs = std::filesystem;

namespace {

struct TempDir {
    std::string path;
    TempDir() {
        char t[] = "/tmp/distcache-ptest-XXXXXX";
        path = mkdtemp(t);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

// Two fake clocks: `steady` drives the engine (TTL countdown), `wall` is unix time for the log.
struct Clocks {
    int64_t steady = 1000;
    int64_t wall = 1'700'000'000'000;
    Store::Clock steady_fn() { return [this] { return steady; }; }
    std::function<int64_t()> wall_fn() { return [this] { return wall; }; }
    // "The server was down for `ms`": both clocks move, then a fresh process starts.
    void downtime(int64_t ms) { steady += ms; wall += ms; }
};

std::unique_ptr<Engine> make_engine(const std::string& kind, Clocks& c, size_t cap = 0) {
    if (kind == "single") return std::make_unique<Store>(c.steady_fn(), cap);
    if (kind == "global") return std::make_unique<GlobalLockStore>(c.steady_fn(), cap);
    return std::make_unique<ShardedStore>(4, c.steady_fn(), cap);
}

using Dump = std::map<std::string, std::pair<std::string, int64_t>>;  // key -> (value, remaining ttl)
Dump dump(const Engine& e) {
    Dump d;
    e.for_each([&](const std::string& k, const std::string& v, int64_t ttl) { d[k] = {v, ttl}; });
    return d;
}

PersistenceOptions opts(const std::string& dir, Clocks& c, FsyncPolicy p = FsyncPolicy::No, uint64_t snap = 0) {
    PersistenceOptions o;
    o.dir = dir;
    o.fsync = p;
    o.snapshot_wal_bytes = snap;  // 0 = no automatic snapshots (tests call snapshot() themselves)
    o.wall_ms = c.wall_fn();
    return o;
}

// One "server process": an engine plus its persistence, recovered from `dir`.
struct Node {
    std::unique_ptr<Engine> engine;
    std::unique_ptr<Persistence> p;
    RecoveryStats rs;
    bool open(const std::string& kind, const std::string& dir, Clocks& c, FsyncPolicy pol = FsyncPolicy::No,
              size_t cap = 0, uint64_t snap = 0, std::string* err = nullptr) {
        engine = make_engine(kind, c, cap);
        p = std::make_unique<Persistence>(*engine, opts(dir, c, pol, snap));
        std::string e;
        bool ok = p->open(&rs, &e);
        if (err) *err = e;
        return ok;
    }
};

std::string newest_segment(const std::string& dir) {
    auto ids = list_ids(dir, "wal-", ".log");
    return wal_path(dir, ids.back());
}
std::string slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f), {});
}
void spit(const std::string& p, const std::string& s) {
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f.write(s.data(), static_cast<std::streamsize>(s.size()));
}
void copy_dir(const std::string& from, const std::string& to) {  // like copying the disk after a crash
    fs::create_directories(to);
    for (auto& e : fs::directory_iterator(from))
        if (e.path().filename() != "LOCK") fs::copy_file(e.path(), to + "/" + e.path().filename().string());
}

}  // namespace

class Recovery : public ::testing::TestWithParam<std::string> {};
INSTANTIATE_TEST_SUITE_P(Engines, Recovery, ::testing::Values("single", "global", "sharded"),
                         [](const auto& i) { return i.param; });

TEST_P(Recovery, EveryOperationTypeSurvivesARestart) {
    TempDir d; Clocks c; Dump before;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        Engine& e = *n.engine;
        e.set("a", "1"); e.set("a", "2");              // overwrite
        e.set("b", "x"); e.del("b");                   // deleted
        e.set("c", "v", 60'000);                       // with TTL
        e.set("d", "v", 60'000); e.persist("d");       // TTL removed again
        e.set("e", "v"); e.expire("e", 90'000);        // TTL added later
        e.set("f", std::string("bin\r\n\0ary", 9));    // binary-safe value
        e.set("g", ""); e.del("zzz");                  // empty value; delete of a missing key
        before = dump(e);
        n.p->close();
    }
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_EQ(dump(*n.engine), before);
    EXPECT_TRUE(n.engine->consistent());
    EXPECT_EQ(n.rs.damaged_files, 0u);
}

TEST_P(Recovery, DeadlinesAreAbsoluteSoDowntimeCountsAgainstTtl) {
    TempDir d; Clocks c;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        n.engine->set("long", "v", 10'000);
        n.engine->set("short", "v", 3'000);
        n.engine->set("forever", "v");
        n.p->close();
    }
    c.downtime(4'000);  // the process was off for 4 s
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_FALSE(n.engine->get("short"));             // expired while down: must NOT come back
    EXPECT_EQ(n.engine->get("long"), "v");
    EXPECT_EQ(n.engine->ttl_ms("long"), 6'000);       // 10 s - 4 s: not reset to 10 s
    EXPECT_EQ(n.engine->ttl_ms("forever"), -1);
    EXPECT_EQ(n.rs.expired_skipped, 1u);
}

TEST_P(Recovery, AKeyOverwrittenWithAnExpiredValueDoesNotResurrectTheOldValue) {
    TempDir d; Clocks c;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        n.engine->set("k", "old");
        n.engine->set("k", "new", 1'000);  // newest value expires during downtime
        n.p->close();
    }
    c.downtime(5'000);
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_FALSE(n.engine->get("k"));  // not "old"
}

TEST_P(Recovery, EvictionsAreLoggedSoTheRecoveredKeySetMatches) {
    TempDir d; Clocks c; Dump before;
    const size_t cap = GetParam() == "sharded" ? 8 : 5;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c, FsyncPolicy::No, cap));
        for (int i = 0; i < 40; ++i) n.engine->set("k" + std::to_string(i), "v" + std::to_string(i));
        EXPECT_GT(n.engine->evicted_keys(), 0u);
        before = dump(*n.engine);
        n.p->close();
    }
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c, FsyncPolicy::No, cap));
    EXPECT_EQ(dump(*n.engine), before);
    EXPECT_LE(n.engine->size(), n.engine->max_keys());
}

// Why evictions must be logged: READS are not logged, so on replay the LRU order differs from
// what the live server had. Without the logged DEL, replay would evict the wrong key.
TEST_P(Recovery, LoggedEvictionsMakeRecoveryIndependentOfReadHistory) {
    if (GetParam() == "sharded") GTEST_SKIP() << "LRU is per shard; this scenario needs one global order";
    TempDir d; Clocks c;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c, FsyncPolicy::No, /*cap=*/3));
        Engine& e = *n.engine;
        e.set("A", "1"); e.set("B", "2"); e.set("C", "3");
        ASSERT_TRUE(e.get("A"));      // A becomes most recent: B is now the LRU key
        e.set("D", "4");              // live server evicts B
        ASSERT_FALSE(e.get("B"));
        ASSERT_TRUE(e.get("A"));
        n.p->close();
    }
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c, FsyncPolicy::No, 3));
    // a naive replay (SET A,B,C,D with no memory of the GET) would have evicted A instead
    EXPECT_TRUE(n.engine->get("A"));
    EXPECT_FALSE(n.engine->get("B"));
    EXPECT_TRUE(n.engine->get("C"));
    EXPECT_TRUE(n.engine->get("D"));
}

TEST_P(Recovery, ANewSessionKeepsAppendingAndRecoversEverything) {
    TempDir d; Clocks c;
    for (int session = 0; session < 3; ++session) {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        EXPECT_EQ(n.engine->size(), static_cast<size_t>(session * 10));
        for (int i = 0; i < 10; ++i) n.engine->set("s" + std::to_string(session) + "k" + std::to_string(i), "v");
        n.p->close();
    }
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_EQ(n.engine->size(), 30u);
    EXPECT_EQ(list_ids(d.path, "wal-", ".log").size(), 4u);  // a fresh segment per start
}

TEST_P(Recovery, TornLogTailAtEveryByteGivesAPrefixOfTheWrites) {
    TempDir src; Clocks c;
    constexpr int kN = 8;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), src.path, c));
        for (int i = 0; i < kN; ++i) n.engine->set("k" + std::to_string(i), "value-" + std::to_string(i));
        n.p->close();
    }
    const std::string seg = newest_segment(src.path);
    const std::string full = slurp(seg);
    // Cut at EVERY byte inside the last two records (each open does real fsyncs, so cutting
    // inside all of them would be slow; the byte-level sweep over a whole file is in test_wal).
    std::vector<size_t> ends;
    {
        RecordReader r(seg, kWalMagic);
        std::string p;
        while (r.next(p) == RecordReader::Status::Record) ends.push_back(static_cast<size_t>(r.good_offset()));
    }
    ASSERT_EQ(ends.size(), static_cast<size_t>(kN));
    size_t last_prefix = 0;
    for (size_t cut = ends[kN - 3]; cut <= full.size(); ++cut) {
        TempDir work;
        copy_dir(src.path, work.path);
        spit(work.path + "/" + fs::path(seg).filename().string(), full.substr(0, cut));
        Node n; ASSERT_TRUE(n.open(GetParam(), work.path, c)) << "cut " << cut;
        // recovered keys must be exactly k0..k(j-1) for some j, with correct values
        size_t j = n.engine->size();
        for (size_t i = 0; i < j; ++i) ASSERT_EQ(n.engine->get("k" + std::to_string(i)), "value-" + std::to_string(i));
        ASSERT_LE(j, static_cast<size_t>(kN));
        ASSERT_GE(j, static_cast<size_t>(kN - 3)) << "the untouched earlier records must always survive";
        ASSERT_GE(j, last_prefix) << "a longer file must never recover fewer records";
        last_prefix = j;
        n.p->close();
    }
    EXPECT_EQ(last_prefix, static_cast<size_t>(kN));  // the uncut file recovers everything
}

TEST_P(Recovery, AfterATornTailTheServerKeepsWorkingAndLaterWritesAreNotLost) {
    TempDir d; Clocks c;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        for (int i = 0; i < 5; ++i) n.engine->set("old" + std::to_string(i), "v");
        n.p->close();
    }
    const std::string seg = newest_segment(d.path);
    std::string full = slurp(seg);
    spit(seg, full.substr(0, full.size() - 3));  // chop the last record in half
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        EXPECT_EQ(n.rs.damaged_files, 1u);
        EXPECT_EQ(n.engine->size(), 4u);
        n.engine->set("after-crash", "yes");  // goes to a NEW segment, past the damaged one
        n.p->close();
    }
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_EQ(n.engine->get("after-crash"), "yes");
    EXPECT_EQ(n.engine->size(), 5u);  // 4 survivors + the new one
}

TEST_P(Recovery, CorruptionInTheMiddleStopsReplayThereAndSaysSo) {
    TempDir d; Clocks c;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        for (int i = 0; i < 10; ++i) n.engine->set("k" + std::to_string(i), "value");
        n.p->close();
    }
    const std::string seg = newest_segment(d.path);
    std::string data = slurp(seg);
    data[data.size() / 2] = static_cast<char>(data[data.size() / 2] ^ 0x40);  // one flipped bit
    spit(seg, data);
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_EQ(n.rs.damaged_files, 1u);
    ASSERT_FALSE(n.rs.warnings.empty());
    EXPECT_LT(n.engine->size(), 10u);   // later records are not trusted
    EXPECT_GT(n.engine->size(), 0u);    // earlier ones are
    for (size_t i = 0; i < n.engine->size(); ++i) EXPECT_TRUE(n.engine->get("k" + std::to_string(i)));  // prefix only
}

TEST_P(Recovery, SnapshotPlusLaterWritesRecovers) {
    TempDir d; Clocks c; Dump before;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        Engine& e = *n.engine;
        for (int i = 0; i < 50; ++i) e.set("k" + std::to_string(i), "v" + std::to_string(i), i % 2 ? std::optional<int64_t>(80'000) : std::nullopt);
        std::string err;
        ASSERT_TRUE(n.p->snapshot(&err)) << err;
        e.set("after1", "x"); e.del("k3"); e.set("k4", "changed"); e.expire("k5", 5'000);
        before = dump(e);
        n.p->close();
    }
    // compaction really happened: one snapshot, and no log segment older than it
    auto snaps = list_ids(d.path, "snapshot-", ".dcs");
    ASSERT_EQ(snaps.size(), 1u);
    for (uint64_t id : list_ids(d.path, "wal-", ".log")) EXPECT_GE(id, snaps[0]);

    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_TRUE(n.rs.snapshot_loaded);
    EXPECT_EQ(n.rs.snapshot_keys, 50u);
    EXPECT_EQ(dump(*n.engine), before);
}

TEST_P(Recovery, RepeatedSnapshotsKeepOnlyTheLatestAndStayCorrect) {
    TempDir d; Clocks c; Dump before;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        std::string err;
        for (int round = 0; round < 4; ++round) {
            for (int i = 0; i < 20; ++i) n.engine->set("r" + std::to_string(round) + "k" + std::to_string(i), "v");
            ASSERT_TRUE(n.p->snapshot(&err)) << err;
        }
        n.engine->del("r0k0");
        before = dump(*n.engine);
        n.p->close();
    }
    EXPECT_EQ(list_ids(d.path, "snapshot-", ".dcs").size(), 1u);
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_EQ(dump(*n.engine), before);
}

TEST_P(Recovery, SnapshotKeepsAbsoluteDeadlines) {
    TempDir d; Clocks c;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        n.engine->set("k", "v", 10'000);
        c.steady += 2'000; c.wall += 2'000;   // 2 s pass while running
        std::string err;
        ASSERT_TRUE(n.p->snapshot(&err));
        n.p->close();
    }
    c.downtime(3'000);
    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_EQ(n.engine->ttl_ms("k"), 5'000);  // 10 s - 2 s running - 3 s down
}

TEST_P(Recovery, IncompleteOrDamagedSnapshotsAreIgnoredNotTrusted) {
    TempDir d; Clocks c;
    {
        Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
        for (int i = 0; i < 5; ++i) n.engine->set("k" + std::to_string(i), "v");
        n.p->close();
    }
    // a "newer" snapshot that was cut off before its SNAPEND marker (crash mid-write)
    std::string partial(kSnapshotMagic, kMagicLen);
    partial += frame_record("*3\r\n$3\r\nSET\r\n$5\r\nghost\r\n$1\r\nx\r\n");
    spit(snapshot_path(d.path, 900), partial);
    // and one whose count disagrees with its contents
    std::string wrong_count(kSnapshotMagic, kMagicLen);
    wrong_count += frame_record("*3\r\n$3\r\nSET\r\n$6\r\nghost2\r\n$1\r\nx\r\n");
    wrong_count += frame_record("*2\r\n$7\r\nSNAPEND\r\n$2\r\n99\r\n");
    spit(snapshot_path(d.path, 901), wrong_count);
    spit(d.path + "/snapshot.tmp", "half written");

    Node n; ASSERT_TRUE(n.open(GetParam(), d.path, c));
    EXPECT_FALSE(n.rs.snapshot_loaded);
    EXPECT_EQ(n.rs.damaged_files, 2u);
    EXPECT_EQ(n.engine->size(), 5u);                 // came from the log
    EXPECT_FALSE(n.engine->get("ghost"));
    EXPECT_FALSE(n.engine->get("ghost2"));
    EXPECT_FALSE(fs::exists(d.path + "/snapshot.tmp"));
}

// The central correctness claim of the design: under concurrency, the log order for each key
// equals the engine's apply order, so replay reproduces the live state EXACTLY.
class RecoveryConcurrent : public ::testing::TestWithParam<std::tuple<std::string, FsyncPolicy, size_t>> {};
INSTANTIATE_TEST_SUITE_P(
    Matrix, RecoveryConcurrent,
    ::testing::Values(std::make_tuple("global", FsyncPolicy::EverySec, size_t{0}),
                      std::make_tuple("sharded", FsyncPolicy::EverySec, size_t{0}),
                      std::make_tuple("sharded", FsyncPolicy::Always, size_t{0}),
                      std::make_tuple("global", FsyncPolicy::Always, size_t{12}),
                      std::make_tuple("sharded", FsyncPolicy::EverySec, size_t{12})),
    [](const auto& i) {
        return std::get<0>(i.param) + "_" + to_string(std::get<1>(i.param)) + "_cap" + std::to_string(std::get<2>(i.param));
    });

TEST_P(RecoveryConcurrent, ConcurrentWritersRecoverToExactlyTheLiveFinalState) {
    const std::string kind = std::get<0>(GetParam());   // plain locals: C++17 lambdas can't capture structured bindings
    const FsyncPolicy policy = std::get<1>(GetParam());
    const size_t cap = std::get<2>(GetParam());
    TempDir d; Clocks c; Dump live;
    {
        Node n; ASSERT_TRUE(n.open(kind, d.path, c, policy, cap));
        Engine& e = *n.engine;
        std::vector<std::thread> ts;
        for (int t = 0; t < 8; ++t)
            ts.emplace_back([&, t] {
                std::mt19937 rng(40 + t);
                for (int i = 0; i < 600; ++i) {
                    std::string k = "k" + std::to_string(rng() % 20);  // 20 keys, 8 writers: constant conflicts
                    switch (rng() % 6) {
                        case 0: case 1: e.set(k, "t" + std::to_string(t) + "-" + std::to_string(i)); break;
                        case 2: e.set(k, "ttl-" + std::to_string(t), 1'000'000 + static_cast<int64_t>(rng() % 5)); break;
                        case 3: e.del(k); break;
                        case 4: e.expire(k, 2'000'000 + static_cast<int64_t>(rng() % 5)); break;
                        default: e.persist(k);
                    }
                    if (policy == FsyncPolicy::Always && i % 25 == 0) n.p->commit();
                }
                n.p->commit();
            });
        for (auto& th : ts) th.join();
        live = dump(e);
        n.p->close();
    }
    Node n; ASSERT_TRUE(n.open(kind, d.path, c, policy, cap));
    EXPECT_EQ(dump(*n.engine), live);
    EXPECT_TRUE(n.engine->consistent());
}

TEST(PersistenceConcurrent, SnapshotsTakenWhileWritersRunStillRecoverTheFinalState) {
    TempDir d; Clocks c; Dump live;
    {
        Node n; ASSERT_TRUE(n.open("sharded", d.path, c, FsyncPolicy::EverySec));
        Engine& e = *n.engine;
        std::atomic<bool> stop{false};
        std::thread snapper([&] {
            std::string err;
            while (!stop) { EXPECT_TRUE(n.p->snapshot(&err)) << err; std::this_thread::sleep_for(std::chrono::milliseconds(5)); }
        });
        std::vector<std::thread> ts;
        for (int t = 0; t < 6; ++t)
            ts.emplace_back([&, t] {
                std::mt19937 rng(7 + t);
                for (int i = 0; i < 1500; ++i) {
                    std::string k = "k" + std::to_string(rng() % 40);
                    switch (rng() % 4) {
                        case 0: case 1: e.set(k, "v" + std::to_string(t) + "." + std::to_string(i)); break;
                        case 2: e.del(k); break;
                        default: e.expire(k, 3'000'000);
                    }
                }
            });
        for (auto& th : ts) th.join();
        stop = true;
        snapper.join();
        live = dump(e);
        n.p->close();
    }
    Node n; ASSERT_TRUE(n.open("sharded", d.path, c));
    EXPECT_EQ(dump(*n.engine), live);  // fuzzy snapshots + idempotent log replay converge
    EXPECT_EQ(list_ids(d.path, "snapshot-", ".dcs").size(), 1u);
}

TEST(PersistenceBasics, SecondProcessCannotOpenTheSameDirectory) {
    TempDir d; Clocks c;
    Node a; ASSERT_TRUE(a.open("single", d.path, c));
    Node b; std::string err;
    EXPECT_FALSE(b.open("single", d.path, c, FsyncPolicy::No, 0, 0, &err));
    EXPECT_NE(err.find("in use"), std::string::npos) << err;
    a.p->close();
    Node again; EXPECT_TRUE(again.open("single", d.path, c));  // released on close
}

TEST(PersistenceBasics, AutomaticSnapshotCompactsALargeLog) {
    TempDir d; Clocks c;
    Node n; ASSERT_TRUE(n.open("global", d.path, c, FsyncPolicy::No, 0, /*snap=*/4000));
    for (int i = 0; i < 400; ++i) n.engine->set("key" + std::to_string(i), std::string(40, 'x'));
    for (int i = 0; i < 60 && n.p->stats().snapshots_taken == 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_GE(n.p->stats().snapshots_taken, 1u);
    n.p->close();
    Node r; ASSERT_TRUE(r.open("global", d.path, c));
    EXPECT_EQ(r.engine->size(), 400u);
}

TEST(PersistenceBasics, SaveAndInfoCommands) {
    TempDir d; Clocks c;
    Node n; ASSERT_TRUE(n.open("sharded", d.path, c, FsyncPolicy::EverySec));
    EXPECT_EQ(execute(*n.engine, {"SET", "k", "v"}, n.p.get()), "+OK\r\n");
    EXPECT_EQ(execute(*n.engine, {"SAVE"}, n.p.get()), "+OK\r\n");
    std::string info = execute(*n.engine, {"INFO"}, n.p.get());
    EXPECT_NE(info.find("fsync:everysec"), std::string::npos) << info;
    EXPECT_NE(info.find("snapshots_taken:1"), std::string::npos) << info;
    EXPECT_NE(execute(*n.engine, {"SAVE"}).find("-ERR persistence is not enabled"), std::string::npos);
    EXPECT_NE(execute(*n.engine, {"INFO"}).find("enabled:no"), std::string::npos);
}
