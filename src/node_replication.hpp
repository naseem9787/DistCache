#pragma once
// Replication as a part of a running node: owns the replication log, the primary listener or the
// replica client, decides whether client commands are allowed, changes role at run time and
// reports status. (Phase 8, steps 3-5; see docs/design/replication.md.)
//
// Wiring in main.cpp / tests:
//     Persistence p(...); p.open(...);                 // attaches itself as the engine observer
//     ReplicationService repl(engine, opts);
//     ObserverChain chain; chain.add(&p); chain.add(repl.observer());
//     engine.set_observer(&chain);                     // before any thread uses the engine
//     repl.start();
//     NodeHooks hooks(&p, &repl);  Server server(engine, {port, workers, &hooks});
//
// The replication log sits behind a GATE. On a primary the gate is open: every client mutation
// is appended with the next offset. On a replica it is closed: the replica's mirror log is fed by
// the applier with the PRIMARY's offsets, and letting the observer append as well would number
// the same mutation twice. Promotion opens the gate, demotion closes it.
//
// ROLE CHANGES are driven by the coordinator (the router, step 6) through four control commands
// served on the normal client port:
//     ROLE                               status of this node (role, epoch, lease, offsets)
//     LEASE <epoch> <ms>                 grant/renew the write lease (primary only)
//     PROMOTE <new_epoch>                replica -> primary
//     REPLICAOF <host> <port> <epoch>    primary -> replica of host:port, or re-point a replica
// Every one of them is checked against the node's EPOCH: a command carrying a lower epoch than
// the node has seen is refused. Epochs only ever increase.
//
// WRITE PERMISSION (checked before every write command, see write_allowed):
//   1. role is PRIMARY            2. not fenced (no replica has shown us a newer epoch)
//   3. if a lease is required: now < lease_expiry (a lease is granted for L ms by LEASE, and the
//      node trusts only L*(1-eps), shorter than the router assumes)
//   4. under sync N: at least N replicas in sync.
// A reply that was HELD (sync N / fsync=always) is re-checked against the lease when it is about to
// be released (release_ok); if the lease lapsed meanwhile the connection is closed instead.
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "commands.hpp"
#include "engine.hpp"
#include "persistence.hpp"
#include "repl_log.hpp"
#include "replication.hpp"

// Forwards mutations to `target` only while enabled. One relaxed atomic load on the write path.
class GatedObserver final : public MutationObserver {
public:
    GatedObserver(MutationObserver* target, bool enabled) : target_(target), enabled_(enabled) {}
    void set_enabled(bool e) { enabled_.store(e, std::memory_order_release); }
    bool enabled() const { return enabled_.load(std::memory_order_acquire); }

    void on_set(const std::string& k, const std::string& v, std::optional<int64_t> ttl) override { if (enabled()) target_->on_set(k, v, ttl); }
    void on_del(const std::string& k) override { if (enabled()) target_->on_del(k); }
    void on_expire(const std::string& k, int64_t ttl) override { if (enabled()) target_->on_expire(k, ttl); }
    void on_persist(const std::string& k) override { if (enabled()) target_->on_persist(k); }
    void on_flush() override { if (enabled()) target_->on_flush(); }

private:
    MutationObserver* target_;
    std::atomic<bool> enabled_;
};

struct NodeReplicationOptions {
    int repl_port = -1;               // >= 0: serve replicas on this port (0 = ephemeral). A replica keeps it for its promotion.
    std::string replicaof_host;       // non-empty: start as a replica of this primary's replication port
    int replicaof_port = 0;
    size_t backlog_bytes = 64u << 20; // replication log budget
    int send_timeout_ms = 5000;
    int link_timeout_ms = 2000;
    std::string replica_id;           // identity of this process as a replica (random if empty; tests set it)
    // Acknowledgement policy. 0 = asynchronous (reply after the local apply). N >= 1 = sync N: a
    // write is acknowledged only after N distinct in-sync replicas confirmed applying it; with
    // fewer than N in sync, writes are refused (-NOREPLICAS); if the confirmations do not arrive
    // within sync_timeout_ms the connection is closed (outcome unknown to the client).
    int sync_replicas = 0;
    int sync_timeout_ms = 1000;
    int max_ack_age_ms = 1500;        // a replica silent this long is not "in sync"
    // Failover.
    bool require_lease = false;       // true under a router: no lease, no writes (fail closed)
    double lease_eps = 0.05;          // the node trusts a lease for L*(1-eps)
    int64_t initial_epoch = 0;
    bool log_events = true;           // role transitions on stderr
};

class ReplicationService {
public:
    ReplicationService(Engine& engine, NodeReplicationOptions opts);
    ~ReplicationService();

    MutationObserver* observer() { return &gate_; }
    bool start(std::string* error);
    void stop();  // idempotent

    enum class Role { Primary = 0, Replica = 1 };
    Role role() const { return static_cast<Role>(role_.load(std::memory_order_acquire)); }
    int64_t epoch() const { return epoch_.load(std::memory_order_acquire); }
    bool fenced() const { return fenced_.load(std::memory_order_acquire); }
    int repl_port() const;  // -1 if this node is not currently serving replicas
    int64_t lease_remaining_ms() const;  // <= 0: no valid lease

    // CommandHooks plumbing. Empty *error when allowed.
    bool write_allowed(std::string* error) const;
    bool read_allowed(std::string* error) const;
    bool release_ok() const;  // may a held reply be released now? (lease still valid)
    std::string info() const;  // the "# Replication" section
    std::string control(const std::vector<std::string>& args, bool* handled);

    // sync N support
    uint64_t take_repl_pending();
    uint64_t repl_acked() const;
    int repl_timeout_ms() const { return opts_.sync_timeout_ms; }
    int add_ack_listener(std::function<void()> fn);
    void remove_ack_listener(int id);

    // Role changes (what the control commands call). All return false with *err set on refusal.
    bool grant_lease(int64_t epoch, int64_t ms, std::string* err);
    bool promote(int64_t new_epoch, std::string* reply_info, std::string* err);
    bool replicate_from(const std::string& host, int port, int64_t epoch, std::string* err);

    ReplicationLog& log() { return log_; }
    ReplicationPrimary* primary() { return std::atomic_load(&primary_).get(); }
    ReplicationReplica* replica() { return std::atomic_load(&replica_).get(); }

private:
    bool start_primary(std::string* err);
    void start_replica(const std::string& host, int port);
    void event(const std::string& msg) const;

    Engine& engine_;
    NodeReplicationOptions opts_;
    ReplicationLog log_;
    GatedObserver gate_;
    std::string rid_;

    std::atomic<int> role_{0};
    std::atomic<int64_t> epoch_{0};
    std::atomic<bool> fenced_{false};
    std::atomic<int64_t> lease_expiry_ms_{0};  // steady-clock ms; 0 = no lease
    std::atomic<uint64_t> transitions_{0};
    std::string last_host_;
    int last_port_ = 0;

    std::mutex ctl_mu_;                        // one role change at a time
    std::shared_ptr<ReplicationPrimary> primary_;  // accessed with std::atomic_load / atomic_store
    std::shared_ptr<ReplicationReplica> replica_;

    std::mutex lmu_;
    std::map<int, std::function<void()>> ack_listeners_;
    int next_listener_ = 1;
    void notify_ack_listeners();
};

// CommandHooks for a node that has replication and possibly persistence. Persistence calls are
// forwarded unchanged (so the durable-before-reply machinery of Phase 6 keeps working).
class NodeHooks final : public CommandHooks {
public:
    NodeHooks(Persistence* persistence, ReplicationService* replication) : p_(persistence), r_(replication) {}

    bool save(std::string* error) override;
    std::string info() override;
    void commit() override { if (p_) p_->commit(); }
    uint64_t take_pending() override { return p_ ? p_->take_pending() : 0; }
    uint64_t durable() override { return p_ ? p_->durable() : 0; }
    int add_durable_listener(std::function<void()> fn) override;
    void remove_durable_listener(int id) override;
    uint64_t take_repl_pending() override { return r_ ? r_->take_repl_pending() : 0; }
    uint64_t repl_acked() override { return r_ ? r_->repl_acked() : UINT64_MAX; }
    int repl_timeout_ms() override { return r_ ? r_->repl_timeout_ms() : 0; }

    bool gates_commands() const override { return r_ != nullptr; }
    bool write_allowed(std::string* error) override { return r_ ? r_->write_allowed(error) : true; }
    bool read_allowed(std::string* error) override { return r_ ? r_->read_allowed(error) : true; }
    bool release_ok() override { return r_ ? r_->release_ok() : true; }
    std::string control(const std::vector<std::string>& args, bool* handled) override;

private:
    struct Ids { int p = 0; int r = 0; };
    Persistence* p_;
    ReplicationService* r_;
    std::mutex mu_;
    std::map<int, Ids> ids_;
    int next_id_ = 1;
};
