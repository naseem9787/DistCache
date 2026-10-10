#pragma once
// Durability for an Engine: write-ahead log + snapshots + crash recovery.
//
//   <dir>/LOCK                    held (flock) by the running server: no two servers per dir
//   <dir>/wal-0000000007.log      log segments; ids only grow
//   <dir>/snapshot-0000000007.dcs "everything before wal segment 7, plus a bit more"
//
// LIFECYCLE
//   open():   recover (load newest valid snapshot, replay the log segments >= its id),
//             then start a NEW segment and begin logging. Recovery is done with the
//             observer detached so replay is not logged a second time.
//   running:  every logical mutation is appended to the log while the engine's lock is
//             held (MutationObserver contract), so for any one key, log order == the order
//             the engine applied the changes. Replay therefore ends in the same state.
//   snapshot: (SAVE, or automatically when a segment gets big)
//               1. rotate the log: new records now go to segment R
//               2. walk the engine and write every live key to snapshot-R.tmp
//               3. fdatasync, atomically rename to snapshot-R.dcs, fsync the directory
//               4. delete log segments < R and older snapshots
//             The walk is FUZZY (not a point-in-time view, shards are visited one at a
//             time while writes continue). That is still correct: every record is an
//             idempotent absolute-state operation (SET/DEL/PEXPIREAT/PERSIST), so
//             replaying segment R on top of a snapshot that already contains some of
//             its effects converges to the same final state.
//   close():  flush + fsync the log and release the directory.
//
// WHAT IS LOGGED: SET (with an ABSOLUTE wall-clock deadline PXAT, because the engine's
// own clock is steady_clock, which means nothing after a restart), DEL, PEXPIREAT, PERSIST.
// Evictions are logged as DEL. Reads and expirations are not logged.
//
// LOCK ORDER: engine lock -> Wal::mu_ (append, from the observer callbacks). Wal's io_mu_
// is never taken while holding an engine lock. snapshot() takes no engine lock while it
// holds io_mu_ (rotate finishes first). No path takes an engine lock after a Wal lock.
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "commands.hpp"
#include "engine.hpp"
#include "wal.hpp"

struct PersistenceOptions {
    std::string dir;
    FsyncPolicy fsync = FsyncPolicy::EverySec;
    uint64_t snapshot_wal_bytes = 64ull << 20;  // auto-snapshot when a segment exceeds this (0 = never)
    std::function<int64_t()> wall_ms;           // unix time in ms; default: system_clock (tests inject a fake)
};

struct RecoveryStats {
    bool snapshot_loaded = false;
    uint64_t snapshot_id = 0;
    uint64_t snapshot_keys = 0;
    uint64_t segments_replayed = 0;
    uint64_t records_replayed = 0;
    uint64_t expired_skipped = 0;    // logged keys whose deadline had already passed
    uint64_t damaged_files = 0;      // files where replay stopped early (torn tail or corruption)
    std::vector<std::string> warnings;
};

class Persistence : public MutationObserver, public CommandHooks {
public:
    Persistence(Engine& engine, PersistenceOptions opts);
    ~Persistence() override;

    // Locks the directory, recovers into the engine, opens a new log segment, attaches
    // itself as the engine's observer and starts the background threads.
    bool open(RecoveryStats* recovery, std::string* error);
    void close();  // idempotent

    // MutationObserver (called under the engine's lock)
    void on_set(const std::string& key, const std::string& value, std::optional<int64_t> ttl_ms) override;
    void on_del(const std::string& key) override;
    void on_expire(const std::string& key, int64_t ttl_ms) override;
    void on_persist(const std::string& key) override;

    // CommandHooks
    bool save(std::string* error) override { return snapshot(error); }
    std::string info() override;
    void commit() override { if (wal_) wal_->commit(); }

    bool snapshot(std::string* error);

    struct Stats {
        WalStats wal;
        uint64_t snapshots_taken = 0;
        uint64_t last_snapshot_id = 0;
        uint64_t last_snapshot_keys = 0;
        uint64_t last_snapshot_ms = 0;
    };
    Stats stats() const;

private:
    bool recover(RecoveryStats* st, std::string* error);
    bool verify_snapshot(const std::string& path, uint64_t* keys) const;
    bool apply_record(const std::string& payload, RecoveryStats* st);
    int64_t wall() const { return opts_.wall_ms(); }
    void snapshot_loop();

    Engine& engine_;
    PersistenceOptions opts_;
    std::unique_ptr<Wal> wal_;
    int lock_fd_ = -1;
    bool open_ = false;

    std::mutex snap_mu_;  // one snapshot at a time
    mutable std::mutex stats_mu_;
    uint64_t snapshots_taken_ = 0, last_snapshot_id_ = 0, last_snapshot_keys_ = 0, last_snapshot_ms_ = 0;

    std::thread snap_thread_;
    std::mutex stop_mu_;
    std::condition_variable stop_cv_;
    bool stopping_ = false;
};
