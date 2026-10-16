#include "node_replication.hpp"

#include <algorithm>
#include <sstream>

ReplicationService::ReplicationService(Engine& engine, NodeReplicationOptions opts)
    : engine_(engine),
      opts_(std::move(opts)),
      log_(ReplicationLog::Options{opts_.backlog_bytes, {}}),
      gate_(&log_, /*enabled=*/opts_.replicaof_host.empty()) {}

ReplicationService::~ReplicationService() { stop(); }

bool ReplicationService::start(std::string* error) {
    if (role() == Role::Replica) {
        ReplicationReplica::Options o;
        o.host = opts_.replicaof_host;
        o.port = opts_.replicaof_port;
        o.link_timeout_ms = opts_.link_timeout_ms;
        o.replica_id = opts_.replica_id;
        replica_ = std::make_unique<ReplicationReplica>(engine_, log_, o);
        replica_->start();
        return true;
    }
    if (opts_.repl_port >= 0) {
        ReplicationPrimary::Options o;
        o.port = opts_.repl_port;
        o.send_timeout_ms = opts_.send_timeout_ms;
        primary_ = std::make_unique<ReplicationPrimary>(engine_, log_, o);
        primary_->set_ack_callback([this] { notify_ack_listeners(); });
        if (!primary_->start(error)) { primary_.reset(); return false; }
    }
    return true;
}

void ReplicationService::stop() {
    if (replica_) replica_->stop();
    if (primary_) primary_->stop();
    log_.close();
}

bool ReplicationService::write_allowed(std::string* error) const {
    if (role() == Role::Replica) {
        *error = "READONLY You can't write against a read only replica.";
        return false;
    }
    if (opts_.sync_replicas > 0) {  // sync N: refuse up front rather than accept a write nobody can confirm
        const size_t have = primary_ ? primary_->in_sync_replicas(opts_.max_ack_age_ms) : 0;
        if (have < static_cast<size_t>(opts_.sync_replicas)) {
            *error = "NOREPLICAS Not enough in-sync replicas to write (need " + std::to_string(opts_.sync_replicas) +
                     ", have " + std::to_string(have) + ")";
            return false;
        }
    }
    return true;
}

uint64_t ReplicationService::take_repl_pending() {
    const uint64_t o = ReplicationLog::take_thread_offset();  // always clear: it must not leak into the next batch
    return opts_.sync_replicas > 0 ? o : 0;
}

uint64_t ReplicationService::repl_acked() const {
    if (opts_.sync_replicas <= 0 || !primary_) return UINT64_MAX;
    return primary_->acked_by(static_cast<size_t>(opts_.sync_replicas), opts_.max_ack_age_ms);
}

int ReplicationService::add_ack_listener(std::function<void()> fn) {
    std::lock_guard<std::mutex> g(lmu_);
    const int id = next_listener_++;
    ack_listeners_[id] = std::move(fn);
    return id;
}

void ReplicationService::remove_ack_listener(int id) {
    std::lock_guard<std::mutex> g(lmu_);
    ack_listeners_.erase(id);
}

void ReplicationService::notify_ack_listeners() {
    std::lock_guard<std::mutex> g(lmu_);
    for (auto& kv : ack_listeners_) kv.second();  // each just writes an eventfd
}

bool ReplicationService::read_allowed(std::string* error) const {
    if (role() == Role::Replica && replica_ && !replica_->readable()) {
        *error = "LOADING the replica is synchronizing with its primary";
        return false;
    }
    return true;
}

std::string ReplicationService::info() const {
    std::ostringstream o;
    o << "# Replication\r\n";
    if (role() == Role::Replica) {
        ReplicationReplica::Status s{};
        if (replica_) s = replica_->status();
        const char* st = s.state == ReplicationReplica::State::Online ? "online"
                         : s.state == ReplicationReplica::State::Syncing ? "syncing" : "disconnected";
        o << "role:slave\r\n"
          << "master_host:" << opts_.replicaof_host << "\r\n"
          << "master_port:" << opts_.replicaof_port << "\r\n"
          << "master_link_status:" << (s.state == ReplicationReplica::State::Disconnected ? "down" : "up") << "\r\n"
          << "replica_state:" << st << "\r\n"
          << "replica_readable:" << (replica_ && replica_->readable() ? "yes" : "no") << "\r\n"
          << "repl_id:" << s.replid << "\r\n"
          << "repl_offset:" << s.applied << "\r\n"
          << "master_offset_seen:" << s.primary_offset << "\r\n"
          << "lag_records:" << s.lag() << "\r\n"
          << "lag_ms:" << s.behind_ms << "\r\n"
          << "full_syncs:" << s.full_syncs << "\r\n"
          << "partial_syncs:" << s.partial_syncs << "\r\n"
          << "reconnects:" << s.reconnects << "\r\n"
          << "records_applied:" << s.records_applied << "\r\n"
          << "duplicates_skipped:" << s.duplicates_skipped << "\r\n"
          << "gaps:" << s.gaps << "\r\n";
        return o.str();
    }
    const auto ls = log_.stats();
    o << "role:master\r\n"
      << "repl_id:" << log_.replid() << "\r\n"
      << "repl_offset:" << log_.last_offset() << "\r\n"
      << "repl_enabled:" << (primary_ ? "yes" : "no") << "\r\n"
      << "sync_replicas:" << opts_.sync_replicas << "\r\n";
    if (primary_) {
        o << "in_sync_replicas:" << primary_->in_sync_replicas(opts_.max_ack_age_ms) << "\r\n";
        if (opts_.sync_replicas > 0) o << "acked_by_quorum:" << repl_acked() << "\r\n";
        o << "repl_port:" << primary_->port() << "\r\n"
          << "backlog_first_offset:" << log_.first_offset() << "\r\n"
          << "backlog_records:" << ls.buffered_records << "\r\n"
          << "backlog_bytes:" << ls.buffered_bytes << "\r\n"
          << "backlog_trimmed_records:" << ls.trimmed << "\r\n";
        const auto reps = primary_->replicas();
        o << "connected_replicas:" << reps.size() << "\r\n";
        const uint64_t last = log_.last_offset();
        for (size_t i = 0; i < reps.size(); ++i) {
            o << "replica" << i << ":addr=" << reps[i].peer << ",state=" << reps[i].state << ",sent=" << reps[i].sent
              << ",acked=" << reps[i].acked << ",lag_records=" << (last > reps[i].acked ? last - reps[i].acked : 0)
              << ",ack_age_ms=" << reps[i].last_ack_ms_ago << "\r\n";
        }
        const auto st = primary_->stats();
        o << "full_syncs:" << st.full_syncs << "\r\n"
          << "partial_syncs:" << st.partial_syncs << "\r\n"
          << "replicas_cut_too_old:" << st.cut_too_old << "\r\n"
          << "replicas_cut_send_failed:" << st.cut_send_failed << "\r\n";
    }
    return o.str();
}

// --- NodeHooks ---

int NodeHooks::add_durable_listener(std::function<void()> fn) {
    Ids ids;
    if (p_) ids.p = p_->add_durable_listener(fn);
    if (r_) ids.r = r_->add_ack_listener(std::move(fn));
    std::lock_guard<std::mutex> g(mu_);
    const int id = next_id_++;
    ids_[id] = ids;
    return id;
}

void NodeHooks::remove_durable_listener(int id) {
    Ids ids;
    {
        std::lock_guard<std::mutex> g(mu_);
        auto it = ids_.find(id);
        if (it == ids_.end()) return;
        ids = it->second;
        ids_.erase(it);
    }
    if (p_) p_->remove_durable_listener(ids.p);
    if (r_) r_->remove_ack_listener(ids.r);
}

bool NodeHooks::save(std::string* error) {
    if (!p_) {
        if (error) *error = "persistence is not enabled (start with --dir)";
        return false;
    }
    return p_->save(error);
}

std::string NodeHooks::info() {
    std::string s = p_ ? p_->info() : std::string("# Persistence\r\nenabled:no\r\n");
    if (r_) s += r_->info();
    return s;
}
