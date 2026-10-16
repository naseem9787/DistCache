#pragma once
// The failover coordinator (Phase 8, step 6): one thread inside the router that watches every
// replicated shard, keeps its primary's write lease alive, detects a failed primary and promotes
// a replacement SAFELY. The rules (and the reasons for them) are in docs/design/replication.md;
// the short version of what this file implements:
//
//   every probe_ms, per shard
//     1. ROLE every member               -> who is reachable, which role/epoch/offset they report
//     2. LEASE the primary               -> liveness proof AND the permission to write; the reply
//                                           time bounds how long the primary can still believe it
//                                           has the lease:  safe_expiry = t_ack + L*(1+eps) + G
//     3. the primary has failed when fail_after probes in a row got no answer, or it answers but
//        is no longer the primary of the shard's epoch (it restarted, was fenced, lost its role)
//     4. NEVER promote before now >= safe_expiry: until then the old primary may still be
//        acknowledging writes
//     5. eligible replicas: reachable, replica role, synced, same lineage as the failed primary,
//        following the current epoch. Promote only if there are at least R-N+1 of them
//        (R = replicas configured, N = sync replicas; N=0 needs one). Pick the highest offset.
//     6. PROMOTE epoch+1, publish the new primary address to the router, re-point the others
//     7. every round: any member that claims to be a primary but is not THE primary, or follows an
//        older epoch, is told REPLICAOF <primary> <epoch> (that is how an old primary that comes
//        back is demoted, and why it then resynchronises from scratch)
//
// FAIL CLOSED: whenever the state is ambiguous (no unique primary at the highest epoch, an
// unanswered request, too few eligible replicas, a lease whose fate is unknown) nothing is
// promoted and the shard simply stays unwritable.
//
// A router restart is safe because the coordinator learns epochs from the nodes and, having no
// memory of the leases its previous incarnation granted, behaves as if one had just been granted
// to every primary at start-up (safe_expiry = start + L*(1+eps) + G).
#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct CoordinatorOptions {
    int lease_ms = 2000;          // L: length of each lease grant
    int probe_ms = 500;           // renewal / probe period (L/4)
    int probe_timeout_ms = 500;   // D: connect / reply timeout towards a node; a lease message is assumed
                                  //    to be delivered within D or never
    int fail_after = 3;           // F: consecutive failed probes before a primary is declared failed
    int guard_ms = 500;           // G: guard band added to the lease bound
    double eps = 0.05;            // assumed relative clock-rate error between any two machines
    int sync_replicas = 0;        // N: must equal the --repl-sync the shard's nodes run with
    bool log_events = true;       // events on stderr (always kept in memory for SHARD EVENTS)
    // File holding, per shard, the HIGHEST epoch ever used (rewritten atomically on every change).
    // It makes a coordinator restart safe in one more case: if only an OLD primary is reachable
    // (the newer one died too), its epoch is lower than the file's, so it is not adopted as the
    // primary; the shard stays unavailable instead of silently resurrecting stale data.
    // Empty = no persistence (tests, or when you accept that risk).
    std::string state_file;
};

struct MemberSpec {
    std::string name;  // e.g. "a#0"
    std::string host;
    int port = 0;      // client port (ROLE / LEASE / PROMOTE / REPLICAOF are sent here)
};

struct ShardSpec {
    std::string shard;                // the ring node name
    std::vector<MemberSpec> members;  // members[0] is the designated primary at first start
};

class Coordinator {
public:
    using PrimaryChanged = std::function<void(const std::string& shard, const std::string& host, int port)>;
    // Told whenever a shard becomes usable (true) or must be refused (false): during a failover, when its
    // state is ambiguous, or when no safe replica exists. The router refuses ALL traffic to a shard
    // that is not available (reads too: a primary we no longer trust may serve stale data).
    using AvailabilityChanged = std::function<void(const std::string& shard, bool available)>;
    Coordinator(std::vector<ShardSpec> shards, CoordinatorOptions opts, PrimaryChanged on_primary_changed,
                AvailabilityChanged on_availability_changed = nullptr);
    ~Coordinator();
    Coordinator(const Coordinator&) = delete;
    Coordinator& operator=(const Coordinator&) = delete;

    void start();
    void stop();

    struct MemberInfo {
        std::string name, endpoint, role;
        bool reachable = false;
        bool synced = false;
        int64_t epoch = 0;
        uint64_t offset = 0;
    };
    struct ShardInfo {
        std::string shard, state, reason, primary;
        int64_t epoch = 0;
        uint64_t failovers = 0;
        int failures = 0;
        int64_t safe_expiry_in_ms = 0;  // > 0: the old primary's lease may still be alive
        std::vector<MemberInfo> members;
    };
    std::vector<ShardInfo> info() const;
    std::string status_text() const;
    std::vector<std::string> events() const;  // oldest first

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
