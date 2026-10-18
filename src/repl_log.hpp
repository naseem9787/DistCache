#pragma once
// The replication log: an ordered, bounded, in-memory sequence of logical mutation records.
//
//   offset   1, 2, 3, ...  assigned in the order records are appended; this is THE order a
//            replica applies them in. Offsets are a property of a lineage (replid): a new
//            lineage (fresh process, promotion) starts a new numbering.
//   payload  the RESP-encoded logical command (see record_apply.hpp), the same shape the WAL
//            stores, with absolute deadlines.
//
// INVARIANTS (replication correctness rests on these; each has a test in test_repl_log.cpp):
//   I1  Offsets are consecutive: record k+1 is appended right after record k, no holes.
//   I2  The log is appended from inside the engine's critical section (it is a MutationObserver),
//       AFTER the engine applied the mutation and BEFORE the lock is released. So a mutation can
//       be observed by a client only if its record already has an offset, and two mutations of
//       one key get offsets in the order the engine applied them.
//   I3  append can not fail and never blocks on a consumer: when the byte budget is exceeded the
//       OLDEST records are dropped. A consumer that needs a dropped record gets TooOld and must
//       resynchronise from a snapshot; the writer is never slowed by a slow replica.
//   I4  A replica's copy (append_replicated) accepts only offset == last+1: duplicates are
//       ignored, gaps are refused. Replaying the same stream twice leaves the same log.
//
// Lock order: engine lock -> (WAL buffer lock) -> this log's mutex. The mutex is never held
// while calling into the engine, the network or a callback.
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "engine.hpp"

struct ReplRecord {
    uint64_t offset = 0;
    std::string payload;  // RESP-encoded logical command
};

class ReplicationLog : public MutationObserver {
public:
    struct Options {
        size_t max_bytes = 64u << 20;            // backlog budget (payload bytes + bookkeeping)
        std::function<int64_t()> wall_ms;        // unix milliseconds; default: system clock
    };
    ReplicationLog();  // default options (a defaulted `Options = {}` argument does not compile with NSDMIs)
    explicit ReplicationLog(Options opts);
    ReplicationLog(const ReplicationLog&) = delete;
    ReplicationLog& operator=(const ReplicationLog&) = delete;

    // MutationObserver: encode and append. Called inside the engine's critical section.
    void on_set(const std::string& key, const std::string& value, std::optional<int64_t> ttl_ms) override;
    void on_del(const std::string& key) override;
    void on_expire(const std::string& key, int64_t ttl_ms) override;
    void on_persist(const std::string& key) override;
    void on_flush() override;

    // Lineage. A random 40-hex id; reset() starts a new lineage whose next record is last_offset+1.
    static std::string new_replid();
    std::string replid() const;
    void reset(std::string replid, uint64_t last_offset);
    // Promotion: keep every record and the offset numbering, but start a new lineage id. The old
    // lineage and the offset at which it ended are remembered, so a replica that followed the old
    // primary up to some offset <= that point can continue WITHOUT a full resync (previous_lineage).
    void promote_lineage(std::string new_replid);
    // A replica adopting the primary's (new) lineage id after a partial resync across a promotion.
    void set_replid(std::string replid);
    struct Lineage {
        std::string id;        // empty if there was no promotion
        uint64_t end_offset = 0;
    };
    Lineage previous_lineage() const;

    uint64_t last_offset() const { return last_.load(std::memory_order_acquire); }
    // The offset of the calling thread's most recent LOCAL append (observer callback), cleared on
    // read; 0 if none. The server asks for it after executing a batch to learn which offset the
    // batch's replies depend on (sync N).
    static uint64_t take_thread_offset();
    uint64_t first_offset() const;  // oldest retained offset; last_offset()+1 when empty

    // Replica side: mirror a record received from the primary under its original offset.
    enum class AppendStatus { Appended, Duplicate, Gap };
    AppendStatus append_replicated(uint64_t offset, std::string payload);

    // Consumer side: copy records with offset >= from (at most ~max_bytes of payload, at least one
    // record when available) into *out. Ok with an empty *out means "caught up".
    enum class ReadStatus { Ok, TooOld, Future };
    ReadStatus read_from(uint64_t from, size_t max_bytes, std::vector<ReplRecord>* out) const;

    // Block until last_offset() > after, the timeout passes or close() is called.
    // Returns true when new records are available.
    bool wait_beyond(uint64_t after, int timeout_ms);
    void close();  // wake every waiter permanently (shutdown)

    struct Stats {
        uint64_t appended = 0;       // records ever appended to this log object
        uint64_t trimmed = 0;        // records dropped to stay within the byte budget
        uint64_t buffered_records = 0;
        uint64_t buffered_bytes = 0;
    };
    Stats stats() const;

private:
    static constexpr size_t kRecordOverhead = 48;  // deque node + string header, counted against the budget

    // NOTE: trimmed payloads are freed here, under the mutex. Freeing them after unlocking was tried in Phase 10
    // and reverted: +14 % writer throughput without replicas, but ~10x more replica lag (docs/benchmarks/final/repl-log-ab.txt).
    uint64_t push_locked(std::string payload, uint64_t offset);
    void append_local(std::string payload);
    int64_t wall() const { return opts_.wall_ms(); }

    Options opts_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<ReplRecord> q_;
    std::string replid_;
    Lineage prev_;
    size_t bytes_ = 0;
    uint64_t appended_ = 0, trimmed_ = 0;
    int waiters_ = 0;
    bool closed_ = false;
    std::atomic<uint64_t> last_{0};
};
