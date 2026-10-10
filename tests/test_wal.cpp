// Tests for the on-disk log format and the WAL writer.
#include <gtest/gtest.h>

#include <stdlib.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include "wal.hpp"

namespace fs = std::filesystem;

namespace {

struct TempDir {
    std::string path;
    TempDir() {
        char t[] = "/tmp/distcache-test-XXXXXX";
        path = mkdtemp(t);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

std::string read_file(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f), {});
}
void write_file(const std::string& p, const std::string& data) {
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f.write(data.data(), static_cast<std::streamsize>(data.size()));
}

std::vector<std::string> read_all(const std::string& path, RecordReader::Status* final_status = nullptr,
                                  const char* magic = kWalMagic) {
    RecordReader r(path, magic);
    std::vector<std::string> out;
    std::string p;
    RecordReader::Status s;
    while ((s = r.next(p)) == RecordReader::Status::Record) out.push_back(p);
    if (final_status) *final_status = s;
    return out;
}

// A log file image with `n` records of varied length, plus each record's end offset.
std::string make_log(int n, std::vector<size_t>* ends, std::vector<std::string>* payloads) {
    std::string data(kWalMagic, kMagicLen);
    for (int i = 0; i < n; ++i) {
        std::string payload = "record-" + std::to_string(i) + std::string(static_cast<size_t>(i * 3), 'x');
        data += frame_record(payload);
        if (ends) ends->push_back(data.size());
        if (payloads) payloads->push_back(payload);
    }
    return data;
}

}  // namespace

TEST(Crc32, MatchesTheStandardCheckValue) {
    EXPECT_EQ(crc32("123456789", 9), 0xCBF43926u);  // the canonical CRC-32 test vector
    EXPECT_EQ(crc32("", 0), 0u);
}

TEST(RecordFormat, RoundTrip) {
    TempDir d;
    std::vector<std::string> payloads;
    write_file(d.path + "/f", make_log(10, nullptr, &payloads));
    RecordReader::Status s;
    EXPECT_EQ(read_all(d.path + "/f", &s), payloads);
    EXPECT_EQ(s, RecordReader::Status::End);
}

TEST(RecordFormat, EmptyLogIsJustAnEnd) {
    TempDir d;
    write_file(d.path + "/f", std::string(kWalMagic, kMagicLen));
    RecordReader::Status s;
    EXPECT_TRUE(read_all(d.path + "/f", &s).empty());
    EXPECT_EQ(s, RecordReader::Status::End);
}

// The crash scenario the CRC exists for: the file ends anywhere inside the last record.
// At EVERY possible cut point the reader must return exactly the records that are complete,
// and report Corrupt (never crash, never invent a record) unless the cut is on a boundary.
TEST(RecordFormat, TornTailAtEveryByteOffset) {
    TempDir d;
    std::vector<size_t> ends;
    std::vector<std::string> payloads;
    const std::string full = make_log(6, &ends, &payloads);
    for (size_t cut = kMagicLen; cut <= full.size(); ++cut) {
        write_file(d.path + "/f", full.substr(0, cut));
        RecordReader::Status s;
        auto got = read_all(d.path + "/f", &s);
        size_t complete = 0;
        bool on_boundary = (cut == kMagicLen);
        for (size_t e : ends) { if (e <= cut) ++complete; if (e == cut) on_boundary = true; }
        ASSERT_EQ(got.size(), complete) << "cut at byte " << cut;
        for (size_t i = 0; i < got.size(); ++i) ASSERT_EQ(got[i], payloads[i]);
        ASSERT_EQ(s, on_boundary ? RecordReader::Status::End : RecordReader::Status::Corrupt) << "cut at " << cut;
    }
}

TEST(RecordFormat, AnySingleBitFlipInARecordIsDetected) {
    TempDir d;
    std::vector<size_t> ends;
    const std::string full = make_log(5, &ends, nullptr);
    const size_t begin = ends[1], end = ends[2];  // damage the third record
    for (size_t pos = begin; pos < end; ++pos) {
        for (int bit : {0, 3, 7}) {
            std::string bad = full;
            bad[pos] = static_cast<char>(bad[pos] ^ (1 << bit));
            write_file(d.path + "/f", bad);
            RecordReader::Status s;
            auto got = read_all(d.path + "/f", &s);
            ASSERT_EQ(got.size(), 2u) << "flip at byte " << pos << " bit " << bit;  // only records 0 and 1 survive
            ASSERT_EQ(s, RecordReader::Status::Corrupt);
        }
    }
}

TEST(RecordFormat, WrongMagicAndHugeLengthAreRejected) {
    TempDir d;
    write_file(d.path + "/f", "NOTALOG!" + frame_record("x"));
    RecordReader::Status s;
    EXPECT_TRUE(read_all(d.path + "/f", &s).empty());
    EXPECT_EQ(s, RecordReader::Status::Corrupt);

    std::string huge(kWalMagic, kMagicLen);
    huge += std::string("\xff\xff\xff\xff\0\0\0\0", 8);  // claims a 4 GiB payload: must not be allocated
    write_file(d.path + "/g", huge);
    EXPECT_TRUE(read_all(d.path + "/g", &s).empty());
    EXPECT_EQ(s, RecordReader::Status::Corrupt);

    RecordReader missing(d.path + "/nope", kWalMagic);
    EXPECT_FALSE(missing.opened());
}

TEST(WalWriter, ConcurrentAppendsAreAllWrittenIntactAndSequenced) {
    TempDir d;
    Wal wal(d.path, FsyncPolicy::EverySec);
    std::string err;
    ASSERT_TRUE(wal.open(1, &err)) << err;
    wal.start();
    constexpr int kThreads = 8, kPer = 500;
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&, t] {
            for (int i = 0; i < kPer; ++i) wal.append("t" + std::to_string(t) + "-" + std::to_string(i));
        });
    for (auto& th : ts) th.join();
    wal.stop();

    auto got = read_all(wal_path(d.path, 1));
    EXPECT_EQ(got.size(), static_cast<size_t>(kThreads * kPer));
    // each thread's own records appear in its own order
    std::vector<int> next(kThreads, 0);
    for (const auto& p : got) {
        int t = std::stoi(p.substr(1, p.find('-') - 1)), i = std::stoi(p.substr(p.find('-') + 1));
        EXPECT_EQ(i, next[static_cast<size_t>(t)]++);
    }
    EXPECT_EQ(wal.stats().records, static_cast<uint64_t>(kThreads * kPer));
}

TEST(WalWriter, AlwaysPolicyHasTheRecordOnDiskWhenCommitReturns) {
    TempDir d;
    Wal wal(d.path, FsyncPolicy::Always);
    std::string err;
    ASSERT_TRUE(wal.open(1, &err));
    wal.start();
    for (int i = 0; i < 20; ++i) {
        wal.append("rec" + std::to_string(i));
        wal.commit();
        // read the file from outside the writer, without stopping it: the record must be there
        auto got = read_all(wal_path(d.path, 1));
        ASSERT_EQ(got.size(), static_cast<size_t>(i + 1));
        EXPECT_EQ(got.back(), "rec" + std::to_string(i));
    }
    EXPECT_GE(wal.stats().fsyncs, 1u);
    wal.stop();
}

TEST(WalWriter, GroupCommitSharesFsyncsBetweenConcurrentWriters) {
    TempDir d;
    Wal wal(d.path, FsyncPolicy::Always);
    std::string err;
    ASSERT_TRUE(wal.open(1, &err));
    wal.start();
    constexpr int kThreads = 8, kPer = 100;
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&] {
            for (int i = 0; i < kPer; ++i) { wal.append("x"); wal.commit(); }
        });
    for (auto& th : ts) th.join();
    auto st = wal.stats();
    EXPECT_EQ(st.records, static_cast<uint64_t>(kThreads * kPer));
    EXPECT_GE(st.fsyncs, 1u);
    EXPECT_LE(st.fsyncs, st.records);  // never more fsyncs than commits; usually far fewer (batched)
    RecordProperty("fsyncs", static_cast<int>(st.fsyncs));
    RecordProperty("records", static_cast<int>(st.records));
    wal.stop();
}

TEST(WalWriter, NoPolicyDoesNotFsyncUntilClose) {
    TempDir d;
    Wal wal(d.path, FsyncPolicy::No);
    std::string err;
    ASSERT_TRUE(wal.open(1, &err));
    wal.start();
    for (int i = 0; i < 100; ++i) { wal.append("r"); wal.commit(); }  // commit is a no-op for this policy
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    EXPECT_EQ(wal.stats().fsyncs, 0u);
    wal.stop();
    EXPECT_GE(wal.stats().fsyncs, 1u);  // a clean shutdown still syncs
    EXPECT_EQ(read_all(wal_path(d.path, 1)).size(), 100u);
}

TEST(WalWriter, RotateSealsTheOldSegmentAndStartsANewOne) {
    TempDir d;
    Wal wal(d.path, FsyncPolicy::EverySec);
    std::string err;
    ASSERT_TRUE(wal.open(5, &err));
    wal.start();
    wal.append("a"); wal.append("b");
    uint64_t id = 0;
    ASSERT_TRUE(wal.rotate(&id, &err)) << err;
    EXPECT_EQ(id, 6u);
    wal.append("c");
    wal.stop();
    EXPECT_EQ(read_all(wal_path(d.path, 5)), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(read_all(wal_path(d.path, 6)), (std::vector<std::string>{"c"}));
    EXPECT_EQ(list_ids(d.path, "wal-", ".log"), (std::vector<uint64_t>{5, 6}));
}

TEST(WalWriter, RefusesToOverwriteAnExistingSegment) {
    TempDir d;
    write_file(wal_path(d.path, 1), "precious");
    Wal wal(d.path, FsyncPolicy::No);
    std::string err;
    EXPECT_FALSE(wal.open(1, &err));
    EXPECT_EQ(read_file(wal_path(d.path, 1)), "precious");
}
