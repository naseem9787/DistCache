#pragma once
// Write-ahead log: an append-only sequence of checksummed records, split into segments.
//
// On-disk record (little endian):
//
//     +-----------+-----------+------------------------------+
//     | u32 len   | u32 crc32 | payload (len bytes)          |
//     +-----------+-----------+------------------------------+
//
// The payload is a RESP-encoded command, so recovery reuses the Phase 1 parser. The CRC is
// what lets recovery tell a complete record from a *torn* one: after a crash the last
// write() may have reached the disk only partially (or in the wrong order), and the file
// then ends in half a record or in garbage. Recovery stops at the first record whose length
// or checksum is wrong and ignores everything after it.
//
// Each file starts with a magic string, so a random file is never mistaken for a log.
//
// DURABILITY (the fsync policy) - when is a write considered safe?
//   always   : a client is answered only after the record has been fdatasync'ed. No
//              acknowledged write is lost on a crash or power failure. Slowest. Concurrent
//              writers share one fsync (group commit), see Wal::wait_durable.
//   everysec : records are write()n to the OS within ~10 ms but fdatasync'ed once a second.
//              A process crash loses nothing the OS already has; a power failure or kernel
//              crash can lose about the last second.
//   no       : never fsync (except on close). The OS flushes when it wants, typically within
//              ~30 s. Fastest. A process crash still loses only what was not yet write()n.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

enum class FsyncPolicy { Always, EverySec, No };
bool parse_fsync_policy(const std::string& s, FsyncPolicy& out);
const char* to_string(FsyncPolicy p);

uint32_t crc32(const void* data, size_t n);

constexpr char kWalMagic[] = "DCWAL001";       // 8 bytes
constexpr char kSnapshotMagic[] = "DCSNP001";  // 8 bytes
constexpr size_t kMagicLen = 8;
constexpr uint32_t kMaxRecordBytes = 512u * 1024 * 1024;

// header + payload, ready to append to a file
std::string frame_record(std::string_view payload);

// Builds one framed record ([len][crc][RESP command]) in a reusable buffer, with no
// allocation once warm. This is the hot-path encoder used for every logged write; keep one
// per thread (it is not thread-safe). Kept here so benchmarks measure the production code.
class RecordBuilder {
public:
    void begin(size_t argc);           // start a command with `argc` arguments
    void arg(std::string_view a);      // append one bulk-string argument
    std::string_view finish();         // fill in the header; valid until the next begin()
private:
    std::string buf_;
};

// Sequentially reads records from a log or snapshot file.
class RecordReader {
public:
    enum class Status {
        Record,   // `payload` holds the next record
        End,      // clean end of file exactly at a record boundary
        Corrupt,  // torn or damaged record (or bad magic): stop reading this file here
    };
    RecordReader(const std::string& path, const char* magic);
    ~RecordReader();
    RecordReader(const RecordReader&) = delete;
    RecordReader& operator=(const RecordReader&) = delete;

    bool opened() const { return f_ != nullptr; }
    Status next(std::string& payload);
    uint64_t good_offset() const { return good_offset_; }  // bytes up to the end of the last good record

private:
    FILE* f_ = nullptr;
    bool bad_magic_ = false;
    uint64_t good_offset_ = 0;
};

// File naming and discovery in a data directory.
std::string wal_path(const std::string& dir, uint64_t id);
std::string snapshot_path(const std::string& dir, uint64_t id);
// ids found in `dir` for "wal-<id>.log" / "snapshot-<id>.dcs", ascending
std::vector<uint64_t> list_ids(const std::string& dir, const char* prefix, const char* suffix);
bool fsync_dir(const std::string& dir);

// A small spin lock for critical sections of a few dozen nanoseconds (here: one memcpy and an
// increment). A contended std::mutex parks the loser in the kernel (futex), which costs far more
// than the section it protects; this one spins briefly, then yields the CPU so a preempted holder
// can finish. It is only for such tiny sections: never hold it across a syscall or an allocation
// you cannot bound. Satisfies BasicLockable, so std::lock_guard / unique_lock work with it.
class SpinMutex {
public:
    void lock() {
        for (unsigned spins = 0;;) {
            if (!flag_.exchange(true, std::memory_order_acquire)) return;
            while (flag_.load(std::memory_order_relaxed)) {
                if (++spins < 128) {
#if defined(__x86_64__) || defined(__i386__)
                    __builtin_ia32_pause();
#endif
                } else {
                    std::this_thread::yield();
                }
            }
        }
    }
    void unlock() { flag_.store(false, std::memory_order_release); }

private:
    std::atomic<bool> flag_{false};
};

struct WalStats {
    uint64_t records = 0;
    uint64_t bytes = 0;     // bytes handed to write()
    uint64_t fsyncs = 0;
    uint64_t segment_id = 0;
    uint64_t segment_bytes = 0;
    uint64_t last_seq = 0;      // sequence number of the newest record appended
    uint64_t durable_seq = 0;   // newest record known to be fdatasync'ed (maintained under policy Always)
};

// The writer. append() is cheap (copy into a buffer under a short mutex) and may be called
// from any thread, including while an engine lock is held. A background flusher thread does
// the write()/fdatasync() calls.
//
// ONE buffer, ONE mutex, on purpose. The log is a total order of the appends, so after a crash
// the surviving log is always a PREFIX of the write history: no write is ever missing while a
// later one survived. A striped variant (one buffer per shard, no shared mutex) was built and
// measured in Phase 6 and REJECTED: the flusher then writes each stripe as a block, so a torn
// tail can lose an older write while keeping a newer one, and it showed no consistent
// end-to-end gain (see docs/phases/phase6.md, "Optimization ladder"). Do not reintroduce it without a way to
// keep the total order.
//
// Locks: buf_mu_ (a spin lock) guards the in-memory buffer and the record count: it is the
// only lock append() takes under fsync=no/everysec. mu_ guards the durability bookkeeping and
// the flusher's wake-up (append() takes it only under fsync=always). io_mu_ guards the file
// descriptor (taken by the flusher and by rotate). Order is io_mu_ -> buf_mu_ and io_mu_ -> mu_;
// buf_mu_ and mu_ are never held together. append() never takes io_mu_, so a slow fsync never
// blocks appenders, it just lets the buffer grow.
//
// I/O errors are FAIL-STOP: if write() or fdatasync() fails the process aborts. After a
// failed fsync the kernel may have dropped the dirty pages, so a retry that "succeeds"
// would silently lose data; continuing to acknowledge writes would be lying.
class Wal {
public:
    Wal(std::string dir, FsyncPolicy policy);
    ~Wal();

    bool open(uint64_t segment_id, std::string* err);  // creates wal-<id>.log (must not exist)
    void start();                                      // launches the flusher thread
    void stop();                                       // flush + fsync + join (idempotent)

    // Queues a record; returns its sequence number (1, 2, 3, ...).
    // Also remembers it as this thread's most recent record, see commit().
    uint64_t append(std::string_view payload);
    // Same, for a record already framed by the caller (header + payload, see frame_record).
    // This is the hot path: the caller encodes straight into its own reusable buffer.
    uint64_t append_framed(std::string_view framed);

    // Blocks until the calling thread's most recent append() is durable (policy Always only;
    // returns immediately otherwise or when the thread has nothing pending).
    void commit();

    // Non-blocking alternative to commit(), used by the server so a worker can keep serving
    // other connections while a write waits for its fsync:
    //   take_pending_seq(): this thread's most recent append (and forget it). Returns 0 when no
    //                       waiting is required: nothing pending, or a policy other than Always.
    //   durable_seq():      newest record already fdatasync'ed. A reply that depends on record N
    //                       may be sent once durable_seq() >= N.
    //   set_durable_callback(): invoked by the flusher thread each time durable_seq advances
    //                       (set before start()). It must be cheap and must not call into the Wal.
    uint64_t take_pending_seq();
    uint64_t durable_seq() const { return durable_atomic_.load(std::memory_order_acquire); }
    void set_durable_callback(std::function<void()> cb) { on_durable_ = std::move(cb); }

    // TEST ONLY: sleep before every fdatasync so tests can observe the window between "written"
    // and "durable". Has no effect on correctness, only timing.
    void set_sync_delay_for_testing(std::chrono::milliseconds d) {
        sync_delay_ms_.store(d.count(), std::memory_order_relaxed);
    }

    // Seals the current segment (flush + fsync) and starts wal-<id+1>.log. Returns the new id.
    bool rotate(uint64_t* new_id, std::string* err);

    WalStats stats() const;
    FsyncPolicy policy() const { return policy_; }

private:
    void flusher_loop();
    void flush_locked_io(bool force_sync);  // requires io_mu_ held
    void write_all(const char* data, size_t n);
    bool open_segment(uint64_t id, std::string* err);

    const std::string dir_;
    const FsyncPolicy policy_;

    alignas(64) mutable SpinMutex buf_mu_;    // buf_ + last_seq_ (own cache line: appenders spin on it)
    std::string buf_;
    uint64_t last_seq_ = 0;                   // also the number of records appended: counted here,
                                              // under buf_mu_, instead of a separate hot atomic

    alignas(64) mutable std::mutex mu_;       // durability bookkeeping + flusher wake-up (mutable: stats() is const)
    std::condition_variable cv_work_;
    std::condition_variable cv_durable_;
    uint64_t durable_seq_ = 0;
    bool stop_ = false;
    bool work_pending_ = false;               // fsync=always: an append happened since the flusher last looked

    mutable std::mutex io_mu_;                // fd + segment state
    std::string io_buf_;                      // double buffering: swapped with buf_ and reused, so
                                              // neither side ever regrows from empty under mu_
    int fd_ = -1;
    uint64_t segment_id_ = 0;
    uint64_t segment_bytes_ = 0;
    int64_t last_sync_ms_ = 0;
    bool unsynced_ = false;

    std::atomic<uint64_t> durable_atomic_{0};  // mirror of durable_seq_ for lock-free reads
    std::atomic<int64_t> sync_delay_ms_{0};
    std::function<void()> on_durable_;
    std::atomic<uint64_t> bytes_{0};
    std::atomic<uint64_t> fsyncs_{0};
    std::thread flusher_;
    bool started_ = false;
};
