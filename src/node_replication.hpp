#pragma once
// Replication as a part of a running node: owns the replication log, the primary listener or the
// replica client, decides whether client commands are allowed, and reports status in INFO.
// (Phase 8, step 3.) The pieces it assembles live in repl_log.hpp and replication.hpp.
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
// the same mutation twice. (Promotion, a later step, closes/opens the gate.)
#include <atomic>
#include <memory>
#include <string>

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
    int repl_port = -1;               // >= 0: serve replicas on this port (0 = ephemeral); -1: not a primary
    std::string replicaof_host;       // non-empty: be a replica of this primary's replication port
    int replicaof_port = 0;
    size_t backlog_bytes = 64u << 20; // replication log budget
    int send_timeout_ms = 5000;
    int link_timeout_ms = 2000;
};

class ReplicationService {
public:
    ReplicationService(Engine& engine, NodeReplicationOptions opts);
    ~ReplicationService();

    MutationObserver* observer() { return &gate_; }
    bool start(std::string* error);
    void stop();  // idempotent

    enum class Role { Primary, Replica };
    Role role() const { return opts_.replicaof_host.empty() ? Role::Primary : Role::Replica; }
    int repl_port() const { return primary_ ? primary_->port() : -1; }

    // Command gating (CommandHooks::write_allowed / read_allowed). Empty *error when allowed.
    bool write_allowed(std::string* error) const;
    bool read_allowed(std::string* error) const;
    std::string info() const;  // the "# Replication" section

    ReplicationLog& log() { return log_; }
    ReplicationPrimary* primary() { return primary_.get(); }
    ReplicationReplica* replica() { return replica_.get(); }

private:
    Engine& engine_;
    NodeReplicationOptions opts_;
    ReplicationLog log_;
    GatedObserver gate_;
    std::unique_ptr<ReplicationPrimary> primary_;
    std::unique_ptr<ReplicationReplica> replica_;
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
    int add_durable_listener(std::function<void()> fn) override { return p_ ? p_->add_durable_listener(std::move(fn)) : 0; }
    void remove_durable_listener(int id) override { if (p_) p_->remove_durable_listener(id); }

    bool gates_commands() const override { return r_ != nullptr; }
    bool write_allowed(std::string* error) override { return r_ ? r_->write_allowed(error) : true; }
    bool read_allowed(std::string* error) override { return r_ ? r_->read_allowed(error) : true; }

private:
    Persistence* p_;
    ReplicationService* r_;
};
