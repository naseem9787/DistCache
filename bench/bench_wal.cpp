// bench_wal: isolates the cost of each step of a logged write, one component at a time.
//
// This is how the logging overhead is attributed without an external profiler. Every test
// exercises PRODUCTION code (the same RecordBuilder, Wal, Persistence and engines the server
// uses), with no network in the way. Output: one CSV line per test.
//
//   bench_wal crc    <bytes> <iterations>
//   bench_wal encode <value_bytes> <iterations>
//   bench_wal append <threads> <records_per_thread> <dir> <always|everysec|no>
//   bench_wal write  <dir> <chunk_kb> <total_mb> <sync: 0 none | 1 fdatasync at end | 2 after every chunk>
//   bench_wal engine <single|global|sharded> <threads> <ops_per_thread> <none|no|everysec> <dir>
//
// Fixed operation counts (not durations) keep the in-memory log buffer bounded: with no network,
// producers can outrun any disk, and this log (by design, see README) has no backpressure.
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "persistence.hpp"
#include "store.hpp"
#include "synced_store.hpp"
#include "wal.hpp"

namespace {

using Clock = std::chrono::steady_clock;

double secs_since(Clock::time_point t0) {
    return std::chrono::duration<double>(Clock::now() - t0).count();
}

struct Barrier {  // all workers start together
    std::atomic<bool> go{false};
    void wait() { while (!go.load(std::memory_order_acquire)) std::this_thread::yield(); }
};

std::string unique_dir(const std::string& root) {
    std::string tmpl = root + "/benchwal-XXXXXX";
    std::vector<char> b(tmpl.begin(), tmpl.end());
    b.push_back('\0');
    return mkdtemp(b.data());
}

void rm_rf(const std::string& d) {
    std::string cmd = "rm -rf '" + d + "'";
    int r = std::system(cmd.c_str());
    (void)r;
}

int cmd_crc(size_t bytes, size_t iters) {
    std::string buf(bytes, 'x');
    for (size_t i = 0; i < bytes; ++i) buf[i] = static_cast<char>(i * 31 + 7);
    uint32_t sink = 0;
    auto t0 = Clock::now();
    for (size_t i = 0; i < iters; ++i) sink ^= crc32(buf.data(), bytes);
    double s = secs_since(t0);
    std::printf("crc,bytes=%zu,iters=%zu,ns_per_call=%.1f,MB_per_s=%.0f,sink=%u\n", bytes, iters, s * 1e9 / iters,
                static_cast<double>(bytes) * iters / s / 1e6, sink);
    return 0;
}

int cmd_encode(size_t value_bytes, size_t iters) {
    RecordBuilder rb;
    const std::string key = "key:0000012345";
    const std::string value(value_bytes, 'v');
    size_t sink = 0;
    auto t0 = Clock::now();
    for (size_t i = 0; i < iters; ++i) {
        rb.begin(3);
        rb.arg("SET");
        rb.arg(key);
        rb.arg(value);
        sink += rb.finish().size();
    }
    double s = secs_since(t0);
    std::printf("encode,value_bytes=%zu,iters=%zu,ns_per_record=%.1f,record_bytes=%zu\n", value_bytes, iters,
                s * 1e9 / iters, sink / iters);
    return 0;
}

int cmd_append(int threads, size_t per_thread, const std::string& root, const std::string& policy_s) {
    FsyncPolicy policy;
    if (!parse_fsync_policy(policy_s, policy)) { std::fprintf(stderr, "bad policy\n"); return 2; }
    std::string dir = unique_dir(root);
    Wal wal(dir, policy);
    std::string err;
    if (!wal.open(1, &err)) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }
    wal.start();
    RecordBuilder proto;
    proto.begin(3);
    proto.arg("SET"); proto.arg("key:0000012345"); proto.arg(std::string(64, 'v'));
    const std::string framed(proto.finish());

    Barrier b;
    std::vector<std::thread> ts;
    for (int t = 0; t < threads; ++t)
        ts.emplace_back([&] {
            b.wait();
            for (size_t i = 0; i < per_thread; ++i) wal.append_framed(framed);
        });
    auto t0 = Clock::now();
    b.go = true;
    for (auto& th : ts) th.join();
    double append_s = secs_since(t0);
    wal.stop();  // drains and fsyncs: the time to here is the "all data written" time
    double total_s = secs_since(t0);
    const double n = static_cast<double>(per_thread) * threads;
    std::printf("append,threads=%d,records_per_thread=%zu,dir=%s,policy=%s,record_bytes=%zu,"
                "append_phase_s=%.3f,records_per_s=%.0f,ns_per_append_per_thread=%.0f,drain_total_s=%.3f,MB_written=%.0f\n",
                threads, per_thread, root.c_str(), policy_s.c_str(), framed.size(), append_s, n / append_s,
                append_s * 1e9 / per_thread, total_s, n * static_cast<double>(framed.size()) / 1e6);
    rm_rf(dir);
    return 0;
}

int cmd_write(const std::string& root, size_t chunk_kb, size_t total_mb, int sync_mode) {
    std::string dir = unique_dir(root);
    std::string path = dir + "/f";
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { std::perror("open"); return 1; }
    std::string chunk(chunk_kb * 1024, 'w');
    const size_t chunks = total_mb * 1024 / chunk_kb;
    auto t0 = Clock::now();
    for (size_t i = 0; i < chunks; ++i) {
        size_t off = 0;
        while (off < chunk.size()) {
            ssize_t w = ::write(fd, chunk.data() + off, chunk.size() - off);
            if (w < 0) { std::perror("write"); return 1; }
            off += static_cast<size_t>(w);
        }
        if (sync_mode == 2) fdatasync(fd);
    }
    double write_s = secs_since(t0);
    if (sync_mode >= 1) fdatasync(fd);
    double total_s = secs_since(t0);
    ::close(fd);
    std::printf("write,dir=%s,chunk_kb=%zu,total_mb=%zu,sync=%d,write_phase_s=%.3f,MB_per_s_write_only=%.0f,"
                "total_s=%.3f,MB_per_s_incl_sync=%.0f\n",
                root.c_str(), chunk_kb, total_mb, sync_mode, write_s, static_cast<double>(total_mb) / write_s, total_s,
                static_cast<double>(total_mb) / total_s);
    rm_rf(dir);
    return 0;
}

int cmd_engine(const std::string& mode, int threads, size_t per_thread, const std::string& persist,
               const std::string& root) {
    std::unique_ptr<Engine> engine;
    if (mode == "single") engine = std::make_unique<Store>();
    else if (mode == "global") engine = std::make_unique<GlobalLockStore>();
    else engine = std::make_unique<ShardedStore>(64);
    if (mode == "single" && threads != 1) { std::fprintf(stderr, "single is unsynchronized: 1 thread\n"); return 2; }

    std::string dir;
    std::unique_ptr<Persistence> p;
    if (persist != "none") {
        FsyncPolicy pol;
        if (!parse_fsync_policy(persist, pol)) { std::fprintf(stderr, "bad policy\n"); return 2; }
        dir = unique_dir(root);
        PersistenceOptions po;
        po.dir = dir;
        po.fsync = pol;
        po.snapshot_wal_bytes = 0;
        p = std::make_unique<Persistence>(*engine, po);
        std::string err;
        if (!p->open(nullptr, &err)) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }
    }

    const size_t keyspace = 100000;
    std::vector<std::string> keys(keyspace);
    for (size_t i = 0; i < keyspace; ++i) keys[i] = "key:" + std::to_string(i);
    const std::string value(64, 'x');

    Barrier b;
    std::vector<std::thread> ts;
    for (int t = 0; t < threads; ++t)
        ts.emplace_back([&, t] {
            std::mt19937_64 rng(static_cast<uint64_t>(t) + 1);
            b.wait();
            for (size_t i = 0; i < per_thread; ++i) engine->set(keys[rng() % keyspace], value);
        });
    auto t0 = Clock::now();
    b.go = true;
    for (auto& th : ts) th.join();
    double s = secs_since(t0);
    uint64_t records = 0;
    if (p) {
        records = p->stats().wal.records;
        p->close();
    }
    const double n = static_cast<double>(per_thread) * threads;
    std::printf("engine,mode=%s,threads=%d,ops_per_thread=%zu,persist=%s,dir=%s,seconds=%.3f,ops_per_s=%.0f,"
                "ns_per_op_per_thread=%.0f,wal_records=%llu\n",
                mode.c_str(), threads, per_thread, persist.c_str(), root.c_str(), s, n / s, s * 1e9 / per_thread,
                static_cast<unsigned long long>(records));
    if (!dir.empty()) rm_rf(dir);
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    auto arg = [&](int i) -> std::string { return i < argc ? argv[i] : std::string(); };
    const std::string t = arg(1);
    if (t == "crc" && argc >= 4) return cmd_crc(std::stoul(arg(2)), std::stoul(arg(3)));
    if (t == "encode" && argc >= 4) return cmd_encode(std::stoul(arg(2)), std::stoul(arg(3)));
    if (t == "append" && argc >= 6) return cmd_append(std::stoi(arg(2)), std::stoul(arg(3)), arg(4), arg(5));
    if (t == "write" && argc >= 6) return cmd_write(arg(2), std::stoul(arg(3)), std::stoul(arg(4)), std::stoi(arg(5)));
    if (t == "engine" && argc >= 7) return cmd_engine(arg(2), std::stoi(arg(3)), std::stoul(arg(4)), arg(5), arg(6));
    std::fputs("usage: see the header comment of bench/bench_wal.cpp\n", stderr);
    return 2;
}
