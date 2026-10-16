#pragma once
// Primary -> replica log shipping (asynchronous). Step 2 of Phase 8; see docs/design/replication.md.
//
// WIRE PROTOCOL (RESP arrays over a dedicated TCP port, separate from the client port so the
// replication link can be faulted, firewalled or measured on its own):
//
//   replica -> primary   SYNC <replid|?> <next_offset> <epoch>
//   primary -> replica   CONTINUE <replid> <epoch> <primary_last_offset>      partial resync, or
//                        FULLSYNC <replid> <epoch> <S>                        full resync:
//                          SET key value [PXAT ms] ...   (a fuzzy snapshot, one entry per array)
//                          SNAPEND <count>
//                        then, in both cases, forever:
//                          R <offset> <payload>          one log record (payload = RESP command)
//                          PING <primary_last_offset>    idle heartbeat (every ping_ms)
//   replica -> primary   ACK <applied_offset>            whenever it has applied everything it
//                                                        had buffered, and on every PING
//   an error is a plain RESP error line, then the primary closes the link.
//
// Roles of the numbers: offsets order the stream; the replica applies a record only when
// offset == applied+1 (duplicates are skipped, a gap drops the link and re-synchronises);
// `replid` says which numbering the offsets belong to; `epoch` is carried for the failover
// fencing added in later steps.
//
// Backpressure: the primary NEVER waits for a replica on the write path. Each replica has its own
// sender thread reading from the shared log at its own pace. A replica that cannot keep up either
// (a) stalls the TCP connection, the send times out and the link is cut, or (b) falls behind the
// log's byte budget (ReadStatus::TooOld) and is cut. Either way it reconnects and resumes with a
// partial resync if its position is still in the backlog, else a full resync.
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "engine.hpp"
#include "repl_log.hpp"

// Deterministic fault injection (used by tests and the benchmarks; all off by default).
struct ReplFaults {
    std::atomic<int> drop_next{0};          // do not send the next N records (creates a gap)
    std::atomic<int> duplicate_next{0};     // send the next N records twice
    std::atomic<int> send_delay_ms{0};      // sleep before every batch (a slow link)
    std::atomic<int> snapshot_pause_us{0};  // sleep after every 64 snapshot entries (widens the full-sync window)
};

class ReplicationPrimary {
public:
    struct Options {
        int port = 0;                       // 0 = ephemeral (see port())
        size_t batch_bytes = 256u << 10;    // max payload bytes per network batch
        int send_timeout_ms = 5000;         // a replica that does not drain its socket this long is cut
        int ping_ms = 100;                  // heartbeat interval when idle
        int handshake_timeout_ms = 5000;
        std::function<int64_t()> wall_ms;   // unix ms for snapshot deadlines; default system clock
    };
    ReplicationPrimary(Engine& engine, ReplicationLog& log);
    ReplicationPrimary(Engine& engine, ReplicationLog& log, Options opts);
    ~ReplicationPrimary();
    ReplicationPrimary(const ReplicationPrimary&) = delete;
    ReplicationPrimary& operator=(const ReplicationPrimary&) = delete;

    bool start(std::string* error = nullptr);
    void stop();
    int port() const { return port_; }

    void set_epoch(int64_t e) { epoch_.store(e); }
    ReplFaults& faults() { return faults_; }
    void disconnect_replicas();  // cut every replica link now (they reconnect)

    struct ReplicaInfo {
        std::string peer;
        std::string state;          // "full-sync" | "streaming"
        uint64_t sent = 0;          // next offset this replica will be sent, minus 1
        uint64_t acked = 0;         // highest offset it confirmed applying
        int64_t last_ack_ms_ago = -1;
    };
    std::vector<ReplicaInfo> replicas() const;

    struct Stats {
        uint64_t links = 0, full_syncs = 0, partial_syncs = 0;
        uint64_t cut_too_old = 0;     // fell behind the backlog
        uint64_t cut_send_failed = 0; // could not be written to (slow or dead)
    };
    Stats stats() const;

private:
    struct Link;
    void accept_loop();
    void serve(std::shared_ptr<Link> link);
    bool stream_snapshot(Link& l, uint64_t* S);
    bool stream_records(Link& l, uint64_t cursor);

    Engine& engine_;
    ReplicationLog& log_;
    Options opts_;
    int listen_fd_ = -1;
    int port_ = 0;
    std::atomic<bool> running_{false};
    std::atomic<int64_t> epoch_{0};
    std::atomic<uint64_t> kill_generation_{0};
    ReplFaults faults_;
    std::thread acceptor_;

    mutable std::mutex mu_;
    std::vector<std::shared_ptr<Link>> links_;
    std::atomic<uint64_t> n_links_{0}, n_full_{0}, n_partial_{0}, n_too_old_{0}, n_send_failed_{0};
};

class ReplicationReplica {
public:
    struct Options {
        std::string host = "127.0.0.1";
        int port = 0;
        int link_timeout_ms = 2000;       // connect / read timeout: no frame (not even a PING) this long = link dead
        int reconnect_min_ms = 20;
        int reconnect_max_ms = 500;
        std::function<int64_t()> wall_ms;
    };
    // `log` is the replica's MIRROR of the primary's log (same offsets), used for resynchronisation
    // and, after a promotion, to serve other replicas. Eviction is switched off on `engine`.
    ReplicationReplica(Engine& engine, ReplicationLog& log, Options opts);
    ~ReplicationReplica();
    ReplicationReplica(const ReplicationReplica&) = delete;
    ReplicationReplica& operator=(const ReplicationReplica&) = delete;

    void start();
    void stop();

    enum class State { Disconnected, Syncing, Online };
    struct Status {
        State state = State::Disconnected;
        uint64_t applied = 0;           // highest offset applied (== mirror log last offset)
        uint64_t primary_offset = 0;    // highest primary offset seen (handshake, records, pings)
        std::string replid;
        uint64_t full_syncs = 0, partial_syncs = 0, reconnects = 0;
        uint64_t records_applied = 0, duplicates_skipped = 0, gaps = 0, invalid = 0;
        int64_t behind_ms = 0;          // how long the replica has been continuously behind (0 = caught up)
        uint64_t lag() const { return primary_offset > applied ? primary_offset - applied : 0; }
    };
    Status status() const;
    bool wait_applied(uint64_t offset, int timeout_ms) const;
    bool wait_online(int timeout_ms) const;
    // False until the first synchronisation finished, and from the moment a full resync wipes
    // the engine until its snapshot is installed. Reads are refused (-LOADING) while false.
    // A replica that merely lost its link keeps serving its (stale) data.
    bool readable() const { return readable_.load(std::memory_order_acquire); }

    // Fault injection / test control.
    void set_apply_paused(bool paused) { paused_ = paused; }       // stop reading: the link backs up
    void set_reconnect_hold(bool hold) { hold_reconnect_ = hold; } // do not reconnect while true
    void drop_link();                                              // close the current connection now

private:
    void run();
    bool session();
    int64_t wall() const { return opts_.wall_ms(); }

    Engine& engine_;
    ReplicationLog& log_;
    Options opts_;
    std::thread thread_;
    std::atomic<bool> stop_{false}, paused_{false}, hold_reconnect_{false};
    std::atomic<int> cur_fd_{-1};
    std::atomic<bool> readable_{false};
    int64_t behind_since_ms_ = 0;  // guarded by mu_; steady-clock ms, 0 = not behind

    void refresh_locked();  // recompute state / behind_since from the offsets; caller holds mu_

    mutable std::mutex mu_;  // guards status_ and replid
    Status status_;
    int64_t epoch_ = 0;
};
