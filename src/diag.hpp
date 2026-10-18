#pragma once
// Diagnostics for performance investigation. EVERYTHING here compiles to nothing unless the
// translation unit is built with -DDISTCACHE_DIAGNOSTICS (the `distcache_diag` target), so the
// production server carries no timing overhead and no extra code paths.
//
// Why this exists: no external profiler (perf, valgrind) is available on the development
// machine and ptrace is restricted, so the logging path is investigated with
//   1. these nanosecond timers around the specific steps of a logged write,
//   2. an in-process sampling profiler (profiler.cpp),
//   3. isolated component microbenchmarks (bench/bench_wal.cpp).
// Timers are not free (two clock reads per scope, roughly 40 ns), so the diag build is for
// attribution; it must not be used for headline throughput numbers.
#include <cstdint>
#include <string>

#ifdef DISTCACHE_DIAGNOSTICS
#include <atomic>
#include <chrono>

namespace diag {

struct Counter {
    std::atomic<uint64_t> ns{0};
    std::atomic<uint64_t> n{0};
    std::atomic<uint64_t> max_ns{0};   // the single worst observation: stalls hide in averages
    void add(uint64_t d) {
        ns.fetch_add(d, std::memory_order_relaxed);
        n.fetch_add(1, std::memory_order_relaxed);
        uint64_t m = max_ns.load(std::memory_order_relaxed);
        while (d > m && !max_ns.compare_exchange_weak(m, d, std::memory_order_relaxed)) {}
    }
};

struct Counters {
    Counter encode;         // Persistence observer: build the framed record (RESP encode + CRC)
    Counter append_wait;    // Wal::append_framed: time BLOCKED acquiring the log buffer mutex
    Counter append_locked;  // Wal::append_framed: time holding that mutex (copy into the buffer)
    Counter flush_swap;     // flusher: holding the buffer mutex to swap buffers
    Counter flush_write;    // flusher: write() to the file
    Counter flush_sync;     // flusher: fdatasync()
    Counter shard_wait;     // engine: waiting to acquire a shard lock
    Counter shard_hold;     // engine: holding a shard lock for one operation (includes logging)
    Counter repl_encode;    // ReplicationLog::on_set: build the record payload
    Counter repl_wait;      // ReplicationLog::append_local: time BLOCKED acquiring the log mutex
    Counter repl_locked;    // ReplicationLog::append_local: time holding it (push + trim + notify)
    std::atomic<uint64_t> flush_cycles{0};
    std::atomic<uint64_t> flush_bytes{0};
    std::atomic<uint64_t> buf_grow_events{0};   // log buffer had to reallocate while appenders held the mutex
    std::atomic<uint64_t> buf_peak_bytes{0};    // largest the pending-log buffer ever got
};

Counters& c();

inline uint64_t now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}

class Scoped {
public:
    explicit Scoped(Counter& k) : k_(k), t0_(now_ns()) {}
    ~Scoped() { k_.add(now_ns() - t0_); }
private:
    Counter& k_;
    uint64_t t0_;
};

std::string report();   // "key:value\r\n..." lines for INFO
bool sink_is_null();    // env DISTCACHE_WAL_SINK=null: flusher discards data instead of write()

void profiler_start(const std::string& out_path, int hz);
void profiler_stop();
void tag_thread(const char* name);

}  // namespace diag

#define DC_SCOPED(field) ::diag::Scoped _dc_scoped_##field(::diag::c().field)
#define DC_TAG_THREAD(name) ::diag::tag_thread(name)
#else
#define DC_SCOPED(field) ((void)0)
#define DC_TAG_THREAD(name) ((void)0)
#endif
